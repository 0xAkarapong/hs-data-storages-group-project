import datetime as dt

from sqlalchemy import func, select

from db import BusinessError, lock, retry_on_conflict, tx
from models import Payment, PaymentDirection, PaymentStatus, Plan, SubStatus, Subscription, User


@retry_on_conflict()
def purchase_subscription(user_id: int, plan_id: int, payment_method: str,
                          idempotency_key: str, months: int = 1) -> dict:
    """Buy a plan: SUBSCRIPTION + PAYMENT are written in one transaction, or neither is.
    Concurrency:
      • idempotency_key UNIQUE → a retried/duplicated request never double-charges.
      • Row lock on the user + partial unique index → never two active subscriptions."""
    with tx() as s:
        user = s.scalar(lock(select(User).where(User.user_id == user_id)))
        if not user:
            raise BusinessError("no such user")

        # Re-check after acquiring the lock so a request that waited for an
        # identical purchase observes the payment committed by the winner.
        existing = s.scalar(select(Payment).where(Payment.idempotency_key == idempotency_key))
        if existing:
            return {"subscription_id": existing.subscription_id,
                    "payment_id": existing.payment_id, "replayed": True}

        plan = s.scalar(select(Plan).where(Plan.plan_id == plan_id))
        if not plan:
            raise BusinessError("no such plan")

        active = s.scalar(select(func.count()).select_from(Subscription).where(
            Subscription.user_id == user_id, Subscription.status == SubStatus.active))
        if active:
            raise BusinessError("user already has an active subscription")

        today = dt.date.today()
        sub = Subscription(user_id=user_id, plan_id=plan_id, status=SubStatus.active,
                           start_date=today, end_date=today + dt.timedelta(days=30 * months))
        s.add(sub)
        s.flush()

        pay = Payment(user_id=user_id, subscription_id=sub.subscription_id,
                      amount=plan.monthly_price * months, currency="THB",
                      status=PaymentStatus.captured, direction=PaymentDirection.debit,
                      payment_method=payment_method, idempotency_key=idempotency_key)
        s.add(pay)
        s.flush()
        return {"subscription_id": sub.subscription_id, "payment_id": pay.payment_id, "replayed": False}

import datetime as dt
import hashlib
import os
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from db import retry_on_conflict, tx
from models import (Bet, BetStatus, OddsSnapshot, Outcome, OutcomeStatus, Payment,
                    PaymentDirection, PaymentStatus, Plan, SportsEvent, EventStatus,
                    StreamingSession, SubStatus, Subscription, User)

MAX_ODDS_AGE = dt.timedelta(seconds=30)


class BusinessError(Exception):
    """Rule violation — caller's fault, never retried."""


def _hash_password(pw: str) -> str:
    salt = os.urandom(16)
    return f"pbkdf2$100000${salt.hex()}${hashlib.pbkdf2_hmac('sha256', pw.encode(), salt, 100_000).hex()}"


def _lock(stmt):
    """FOR UPDATE on PostgreSQL; harmless no-op on SQLite (BEGIN IMMEDIATE already serializes)."""
    return stmt.with_for_update()


# ── OP 1 ───────────────────────────────────────────────────────────────────────
@retry_on_conflict()
def register_user(email: str, password: str, display_name: str) -> int:
    """Atomic sign-up. Concurrency: the UNIQUE index on users.email is the arbiter —
    two simultaneous sign-ups with the same email cannot both win, no check-then-act race."""
    email = email.strip().lower()
    try:
        with tx() as s:
            u = User(email=email, password_hash=_hash_password(password), display_name=display_name)
            s.add(u)
            s.flush()
            return u.user_id
    except IntegrityError as exc:
        raise BusinessError(f"email already registered: {email}") from exc


# ── OP 2 ───────────────────────────────────────────────────────────────────────
@retry_on_conflict()
def purchase_subscription(user_id: int, plan_id: int, payment_method: str,
                          idempotency_key: str, months: int = 1) -> dict:
    """Buy a plan: SUBSCRIPTION + PAYMENT are written in one transaction, or neither is.
    Concurrency:
      • idempotency_key UNIQUE → a retried/duplicated request never double-charges.
      • Row lock on the user + partial unique index → never two active subscriptions."""
    with tx() as s:
        existing = s.scalar(select(Payment).where(Payment.idempotency_key == idempotency_key))
        if existing:                                    # replay of a request we already processed
            return {"subscription_id": existing.subscription_id,
                    "payment_id": existing.payment_id, "replayed": True}

        user = s.scalar(_lock(select(User).where(User.user_id == user_id)))
        if not user:
            raise BusinessError("no such user")
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
                      amount=plan.monthly_price * months, currency="USD",
                      status=PaymentStatus.captured, direction=PaymentDirection.debit,
                      payment_method=payment_method, idempotency_key=idempotency_key)
        s.add(pay)
        s.flush()
        return {"subscription_id": sub.subscription_id, "payment_id": pay.payment_id, "replayed": False}


# ── OP 3 ───────────────────────────────────────────────────────────────────────
@retry_on_conflict()
def start_streaming_session(user_id: int, sports_event_id: int) -> int:
    """"Book a seat": open a stream only if the plan's concurrent-stream cap allows it.
    Concurrency: the subscription row is locked FOR UPDATE *before* counting open
    sessions, so the count-then-insert sequence is serialized per subscription.
    Without that lock, N threads all read "1 of 2 used" and all insert."""
    with tx() as s:
        sub = s.scalar(_lock(
            select(Subscription).where(Subscription.user_id == user_id,
                                       Subscription.status == SubStatus.active)))
        if not sub:
            raise BusinessError("no active subscription")
        if sub.end_date and sub.end_date < dt.date.today():
            raise BusinessError("subscription expired")

        plan = s.scalar(select(Plan).where(Plan.plan_id == sub.plan_id))
        event = s.scalar(select(SportsEvent).where(SportsEvent.sports_event_id == sports_event_id))
        if not event or event.status not in (EventStatus.scheduled, EventStatus.live):
            raise BusinessError("event not streamable")

        open_streams = s.scalar(select(func.count()).select_from(StreamingSession).where(
            StreamingSession.subscription_id == sub.subscription_id,
            StreamingSession.ended_at.is_(None)))
        if open_streams >= plan.max_concurrent_streams:
            raise BusinessError(f"concurrent stream limit reached ({plan.max_concurrent_streams})")

        sess = StreamingSession(user_id=user_id, sports_event_id=sports_event_id,
                                subscription_id=sub.subscription_id, started_at=dt.datetime.now(dt.timezone.utc))
        s.add(sess)
        s.flush()
        return sess.session_id


# ── OP 4 ───────────────────────────────────────────────────────────────────────
@retry_on_conflict()
def place_bet(user_id: int, outcome_id: int, stake: Decimal,
              streaming_session_id: int | None, idempotency_key: str) -> dict:
    """Place a wager at a locked-in price. BET + stake PAYMENT commit together.
    Concurrency:
      • Lock the OUTCOME row first → a settlement running concurrently must wait,
        so a bet can never land on an already-settled outcome.
      • Re-read the latest ODDS_SNAPSHOT *inside* the transaction and reject stale
        prices → no betting on an odds value the pricing feed has already replaced.
      • idempotency_key UNIQUE → double-click / client retry charges once."""
    stake = Decimal(stake).quantize(Decimal("0.01"))
    if stake <= 0:
        raise BusinessError("stake must be positive")

    with tx() as s:
        prior = s.scalar(select(Payment).where(Payment.idempotency_key == idempotency_key))
        if prior:
            return {"bet_id": prior.bet_id, "payment_id": prior.payment_id, "replayed": True}

        if not s.scalar(_lock(select(User).where(User.user_id == user_id))):
            raise BusinessError("no such user")

        outcome = s.scalar(_lock(select(Outcome).where(Outcome.outcome_id == outcome_id)))
        if not outcome or outcome.status != OutcomeStatus.open:
            raise BusinessError("outcome is not open")

        event = s.scalar(_lock(select(SportsEvent)
                               .where(SportsEvent.sports_event_id == outcome.sports_event_id)))
        if event.status not in (EventStatus.scheduled, EventStatus.live):
            raise BusinessError("market closed for this event")

        snap = s.scalar(select(OddsSnapshot)
                        .where(OddsSnapshot.outcome_id == outcome_id)
                        .order_by(OddsSnapshot.captured_at.desc()).limit(1))
        if not snap:
            raise BusinessError("no price available")
        captured_at = snap.captured_at
        if captured_at.tzinfo is None:
            captured_at = captured_at.replace(tzinfo=dt.timezone.utc)
        if dt.datetime.now(dt.timezone.utc) - captured_at > MAX_ODDS_AGE:
            raise BusinessError("price is stale, refresh and retry")

        if streaming_session_id is not None:
            sess = s.scalar(select(StreamingSession)
                            .where(StreamingSession.session_id == streaming_session_id))
            if not sess or sess.user_id != user_id or sess.ended_at is not None:
                raise BusinessError("invalid or closed streaming session")

        bet = Bet(user_id=user_id, snapshot_id=snap.snapshot_id,
                  streaming_session_id=streaming_session_id, amount_staked=stake,
                  status=BetStatus.pending, placed_at=dt.datetime.now(dt.timezone.utc))
        s.add(bet)
        s.flush()

        pay = Payment(user_id=user_id, bet_id=bet.bet_id, amount=stake, currency="USD",
                      status=PaymentStatus.captured, direction=PaymentDirection.debit,
                      payment_method="wallet", idempotency_key=idempotency_key)
        s.add(pay)
        s.flush()
        return {"bet_id": bet.bet_id, "payment_id": pay.payment_id,
                "locked_price": snap.price, "replayed": False}


# ── OP 5 ───────────────────────────────────────────────────────────────────────
@retry_on_conflict()
def settle_outcome(outcome_id: int, won: bool) -> dict:
    """Settle a market: flip the outcome, resolve every pending bet, write payouts.
    Concurrency:
      • Same lock order as place_bet (outcome first) → no deadlock, and in-flight
        bets either commit before settlement or are rejected after it.
      • Idempotent: if the outcome is already settled we return early, so a duplicated
        settlement message cannot pay a winner twice."""
    with tx() as s:
        outcome = s.scalar(_lock(select(Outcome).where(Outcome.outcome_id == outcome_id)))
        if not outcome:
            raise BusinessError("no such outcome")
        if outcome.status != OutcomeStatus.open:
            return {"settled": 0, "already_settled": True}

        outcome.status = OutcomeStatus.won if won else OutcomeStatus.lost

        rows = s.execute(
            _lock(select(Bet, OddsSnapshot.price)
                  .join(OddsSnapshot, Bet.snapshot_id == OddsSnapshot.snapshot_id)
                  .where(OddsSnapshot.outcome_id == outcome_id,
                         Bet.status == BetStatus.pending))
        ).all()

        paid = Decimal("0.00")
        for bet, price in rows:
            if won:
                bet.status = BetStatus.won
                payout = (bet.amount_staked * price).quantize(Decimal("0.01"))
                paid += payout
                s.add(Payment(user_id=bet.user_id, bet_id=bet.bet_id, amount=payout,
                              currency="USD", status=PaymentStatus.captured,
                              direction=PaymentDirection.credit, payment_method="wallet",
                              idempotency_key=f"payout:{bet.bet_id}"))
            else:
                bet.status = BetStatus.lost
        return {"settled": len(rows), "total_paid": paid, "already_settled": False}


# ── bonus: releases a "seat" ───────────────────────────────────────────────────
@retry_on_conflict()
def end_streaming_session(session_id: int) -> bool:
    with tx() as s:
        sess = s.scalar(_lock(select(StreamingSession)
                              .where(StreamingSession.session_id == session_id)))
        if not sess or sess.ended_at is not None:
            return False
        sess.ended_at = dt.datetime.now(dt.timezone.utc)
        return True

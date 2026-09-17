import datetime as dt
from decimal import Decimal

from sqlalchemy import select

from db import BusinessError, lock, retry_on_conflict, tx
from models import (Bet, BetStatus, EventStatus, OddsSnapshot, Outcome, OutcomeStatus, Payment,
                    PaymentDirection, PaymentStatus, SportsEvent, StreamingSession, User)

MAX_ODDS_AGE = dt.timedelta(seconds=30)


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

        if not s.scalar(lock(select(User).where(User.user_id == user_id))):
            raise BusinessError("no such user")

        outcome = s.scalar(lock(select(Outcome).where(Outcome.outcome_id == outcome_id)))
        if not outcome or outcome.status != OutcomeStatus.open:
            raise BusinessError("outcome is not open")

        event = s.scalar(lock(select(SportsEvent)
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

        pay = Payment(user_id=user_id, bet_id=bet.bet_id, amount=stake, currency="THB",
                      status=PaymentStatus.captured, direction=PaymentDirection.debit,
                      payment_method="wallet", idempotency_key=idempotency_key)
        s.add(pay)
        s.flush()
        return {"bet_id": bet.bet_id, "payment_id": pay.payment_id,
                "locked_price": snap.price, "replayed": False}


@retry_on_conflict()
def settle_outcome(outcome_id: int, won: bool) -> dict:
    """Settle a market: flip the outcome, resolve every pending bet, write payouts.
    Concurrency:
      • Same lock order as place_bet (outcome first) → no deadlock, and in-flight
        bets either commit before settlement or are rejected after it.
      • Idempotent: if the outcome is already settled we return early, so a duplicated
        settlement message cannot pay a winner twice."""
    with tx() as s:
        outcome = s.scalar(lock(select(Outcome).where(Outcome.outcome_id == outcome_id)))
        if not outcome:
            raise BusinessError("no such outcome")
        if outcome.status != OutcomeStatus.open:
            return {"settled": 0, "already_settled": True}

        event = s.scalar(lock(select(SportsEvent).where(SportsEvent.sports_event_id == outcome.sports_event_id)))
        if event.status == EventStatus.cancelled:
            return {"settled": 0, "already_settled": True, "event_cancelled": True}

        outcome.status = OutcomeStatus.won if won else OutcomeStatus.lost

        rows = s.execute(
            lock(select(Bet, OddsSnapshot.price)
                  .join(OddsSnapshot, Bet.snapshot_id == OddsSnapshot.snapshot_id)
                  .where(OddsSnapshot.outcome_id == outcome_id,
                         Bet.status == BetStatus.pending))
        ).all()

        paid = Decimal("0.00")
        for bet, price in rows:
            if won:
                bet.status = BetStatus.won

                # TODO: fix this logic to use the locked-in price from the OddsSnapshot, not the current price
                payout = (bet.amount_staked * price).quantize(Decimal("0.01"))

                paid += payout
                s.add(Payment(user_id=bet.user_id, bet_id=bet.bet_id, amount=payout,
                              currency="THB", status=PaymentStatus.captured,
                              direction=PaymentDirection.credit, payment_method="wallet",
                              idempotency_key=f"payout:{bet.bet_id}"))
            else:
                bet.status = BetStatus.lost
        return {"settled": len(rows), "total_paid": paid, "already_settled": False}


@retry_on_conflict()
def cancel_event(sports_event_id: int) -> dict:
    """Cancel a SportsEvent: void every still-pending bet on its outcomes and refund the stake.
    Concurrency:
      • Lock outcomes before the event — same order as place_bet (outcome, then event) —
        so a bet racing a cancellation either commits first or sees a closed event, never both.
      • Idempotent: already-cancelled is a no-op; already-finished is rejected outright."""
    with tx() as s:
        outcomes = s.execute(lock(
            select(Outcome).where(Outcome.sports_event_id == sports_event_id)
                            .order_by(Outcome.outcome_id))).scalars().all()
        event = s.scalar(lock(select(SportsEvent).where(SportsEvent.sports_event_id == sports_event_id)))
        if not event:
            raise BusinessError("no such event")
        if event.status == EventStatus.cancelled:
            return {"cancelled_now": False, "already_cancelled": True, "voided": 0, "refunded": Decimal("0.00")}
        if event.status == EventStatus.finished:
            raise BusinessError("event already finished, cannot cancel")

        event.status = EventStatus.cancelled
        outcome_ids = [o.outcome_id for o in outcomes]

        rows = s.execute(
            lock(select(Bet).join(OddsSnapshot, Bet.snapshot_id == OddsSnapshot.snapshot_id)
                  .where(OddsSnapshot.outcome_id.in_(outcome_ids), Bet.status == BetStatus.pending))
        ).scalars().all()

        refunded = Decimal("0.00")
        for bet in rows:
            bet.status = BetStatus.voided
            refunded += bet.amount_staked
            s.add(Payment(user_id=bet.user_id, bet_id=bet.bet_id, amount=bet.amount_staked,
                          currency="THB", status=PaymentStatus.captured, direction=PaymentDirection.credit,
                          payment_method="wallet", idempotency_key=f"void:{bet.bet_id}"))
        return {"cancelled_now": True, "already_cancelled": False, "voided": len(rows), "refunded": refunded}

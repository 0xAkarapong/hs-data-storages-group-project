from __future__ import annotations

import datetime as dt
import enum
from decimal import Decimal

from sqlalchemy import (
    CheckConstraint, Date, DateTime, Enum, ForeignKey, Index, Numeric,
    String, UniqueConstraint, func, text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from dotenv import load_dotenv
import os

load_dotenv()

MONEY = Numeric(12, 2)
ODDS = Numeric(8, 3)
SCHEMA_NAME = os.getenv("SCHEMA_NAME")


class Base(DeclarativeBase):
    pass


class EventStatus(str, enum.Enum):
    scheduled = "scheduled"
    live = "live"
    finished = "finished"
    cancelled = "cancelled"


class OutcomeStatus(str, enum.Enum):
    open = "open"
    won = "won"
    lost = "lost"


class BetStatus(str, enum.Enum):
    pending = "pending"
    won = "won"
    lost = "lost"
    voided = "voided"


class SubStatus(str, enum.Enum):
    active = "active"
    cancelled = "cancelled"
    expired = "expired"


class PaymentStatus(str, enum.Enum):
    pending = "pending"
    captured = "captured"
    failed = "failed"
    refunded = "refunded"


class PaymentDirection(str, enum.Enum):
    debit = "debit"
    credit = "credit"


class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": SCHEMA_NAME}
    
    user_id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    display_name: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    subscriptions: Mapped[list["Subscription"]] = relationship(back_populates="user")
    bets: Mapped[list["Bet"]] = relationship(back_populates="user")
    sessions: Mapped[list["StreamingSession"]] = relationship(back_populates="user")


class Plan(Base):
    __tablename__ = "plans"

    plan_id: Mapped[int] = mapped_column(primary_key=True)
    plan_name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    monthly_price: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    max_concurrent_streams: Mapped[int] = mapped_column(nullable=False, default=1)

    __table_args__ = (
        CheckConstraint("monthly_price >= 0", name="ck_plan_price_nonneg"),
        CheckConstraint("max_concurrent_streams > 0", name="ck_plan_streams_pos"),
        {"schema": SCHEMA_NAME},
    )


class Subscription(Base):
    __tablename__ = "subscriptions"

    subscription_id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA_NAME}.users.user_id", ondelete="CASCADE"), nullable=False)
    plan_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA_NAME}.plans.plan_id"), nullable=False)
    status: Mapped[SubStatus] = mapped_column(Enum(SubStatus, name="sub_status", inherit_schema=True), nullable=False)
    start_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    end_date: Mapped[dt.date | None] = mapped_column(Date)

    user: Mapped[User] = relationship(back_populates="subscriptions")
    plan: Mapped[Plan] = relationship()
    __table_args__ = (
        CheckConstraint("end_date IS NULL OR end_date >= start_date", name="ck_sub_dates"),
        # One active subscription per user, enforced by the database (PostgreSQL).
        Index("uq_one_active_sub_per_user", "user_id",
              unique=True, postgresql_where=text("status = 'active'")),
        {"schema": SCHEMA_NAME},
    )


class SportsEvent(Base):
    __tablename__ = "sports_events"
    __table_args__ = {"schema": SCHEMA_NAME}

    sports_event_id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    sport_type: Mapped[str] = mapped_column(String(50), nullable=False)
    start_time: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[EventStatus] = mapped_column(Enum(EventStatus, name="event_status", inherit_schema=True),
                                                nullable=False, default=EventStatus.scheduled)
    outcomes: Mapped[list["Outcome"]] = relationship(back_populates="event")


class Outcome(Base):
    __tablename__ = "outcomes"
    outcome_id: Mapped[int] = mapped_column(primary_key=True)
    sports_event_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA_NAME}.sports_events.sports_event_id"), nullable=False)
    description: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[OutcomeStatus] = mapped_column(Enum(OutcomeStatus, name="outcome_status", inherit_schema=True),
                                                  nullable=False, default=OutcomeStatus.open)
    event: Mapped[SportsEvent] = relationship(back_populates="outcomes")
    snapshots: Mapped[list["OddsSnapshot"]] = relationship(back_populates="outcome")
    __table_args__ = (
        UniqueConstraint("sports_event_id", "description", name="uq_outcome_desc"),
        {"schema": SCHEMA_NAME},
    )


class OddsSnapshot(Base):
    __tablename__ = "odds_snapshots"
    snapshot_id: Mapped[int] = mapped_column(primary_key=True)
    outcome_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA_NAME}.outcomes.outcome_id"), nullable=False)
    captured_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    price: Mapped[Decimal] = mapped_column(ODDS, nullable=False)

    outcome: Mapped[Outcome] = relationship(back_populates="snapshots")
    __table_args__ = (
        UniqueConstraint("outcome_id", "captured_at", name="uq_snapshot_outcome_time"),  # your UK
        CheckConstraint("price > 1", name="ck_price_gt_one"),
        Index("ix_snapshot_latest", "outcome_id", "captured_at"),
        {"schema": SCHEMA_NAME},
    )


class StreamingSession(Base):
    __tablename__ = "streaming_sessions"
    session_id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA_NAME}.users.user_id"), nullable=False)
    sports_event_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA_NAME}.sports_events.sports_event_id"), nullable=False)
    subscription_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA_NAME}.subscriptions.subscription_id"), nullable=False)
    started_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    ended_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))

    user: Mapped[User] = relationship(back_populates="sessions")
    __table_args__ = (
        CheckConstraint("ended_at IS NULL OR ended_at >= started_at", name="ck_session_times"),
        Index("ix_session_open", "subscription_id", "ended_at"),
        {"schema": SCHEMA_NAME},
    )


class Bet(Base):
    __tablename__ = "bets"
    bet_id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA_NAME}.users.user_id"), nullable=False)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA_NAME}.odds_snapshots.snapshot_id"), nullable=False)
    streaming_session_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA_NAME}.streaming_sessions.session_id"))
    amount_staked: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    status: Mapped[BetStatus] = mapped_column(Enum(BetStatus, name="bet_status", inherit_schema=True),
                                              nullable=False, default=BetStatus.pending)
    placed_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    user: Mapped[User] = relationship(back_populates="bets")
    snapshot: Mapped[OddsSnapshot] = relationship()
    __table_args__ = (
        CheckConstraint("amount_staked > 0", name="ck_stake_pos"),
        Index("ix_bet_pending", "snapshot_id", "status"),
        {"schema": SCHEMA_NAME},
    )


class Payment(Base):
    __tablename__ = "payments"
    payment_id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA_NAME}.users.user_id"), nullable=False)
    subscription_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA_NAME}.subscriptions.subscription_id"))
    bet_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA_NAME}.bets.bet_id"))
    amount: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="THB")
    status: Mapped[PaymentStatus] = mapped_column(Enum(PaymentStatus, name="payment_status", inherit_schema=True), nullable=False)
    direction: Mapped[PaymentDirection] = mapped_column(Enum(PaymentDirection, name="payment_direction", inherit_schema=True),
                                                        nullable=False, default=PaymentDirection.debit)
    payment_method: Mapped[str] = mapped_column(String(30), nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(80), unique=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_payment_amount_pos"),
        # A payment settles exactly one thing: a subscription OR a bet.
        CheckConstraint(
            "(subscription_id IS NOT NULL AND bet_id IS NULL) OR "
            "(subscription_id IS NULL AND bet_id IS NOT NULL)",
            name="ck_payment_xor_target"),
        {"schema": SCHEMA_NAME},
    )

from __future__ import annotations

from sqlalchemy import JSON, DateTime, Integer, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class RawBitrixDeal(Base):
    __tablename__ = "raw_bitrix_deals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    bitrix_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(1024), default="")
    stage_id: Mapped[str] = mapped_column(String(128), default="")
    company_id: Mapped[str] = mapped_column(String(64), default="")
    contact_id: Mapped[str] = mapped_column(String(64), default="")
    opportunity: Mapped[str] = mapped_column(String(64), default="")
    currency_id: Mapped[str] = mapped_column(String(32), default="")
    comments: Mapped[str] = mapped_column(Text, default="")
    raw_payload: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Opportunity(Base):
    __tablename__ = "opportunities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_deal_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(1024), default="")
    stage_id: Mapped[str] = mapped_column(String(128), default="")
    company_id: Mapped[str] = mapped_column(String(64), default="")
    contact_id: Mapped[str] = mapped_column(String(64), default="")
    opportunity_value: Mapped[str] = mapped_column(String(64), default="")
    currency_id: Mapped[str] = mapped_column(String(32), default="")
    last_comment: Mapped[str] = mapped_column(Text, default="")
    next_step: Mapped[str] = mapped_column(Text, default="")
    priority_score: Mapped[str] = mapped_column(String(32), default="0")
    state_code: Mapped[str] = mapped_column(String(128), default="raw_new")
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OpportunityAction(Base):
    __tablename__ = "opportunity_actions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[int] = mapped_column(Integer, index=True)
    action_code: Mapped[str] = mapped_column(String(128), index=True)
    action_name: Mapped[str] = mapped_column(String(255), default="")
    target_role: Mapped[str] = mapped_column(String(128), default="")
    reason: Mapped[str] = mapped_column(Text, default="")
    priority: Mapped[str] = mapped_column(String(32), default="normal")
    status: Mapped[str] = mapped_column(String(64), default="open")
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OpportunityExplainability(Base):
    __tablename__ = "opportunity_explainability"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[int] = mapped_column(Integer, index=True)
    action_id: Mapped[int] = mapped_column(Integer, index=True)
    summary: Mapped[str] = mapped_column(Text, default="")
    why_important: Mapped[str] = mapped_column(Text, default="")
    triggered_signals: Mapped[str] = mapped_column(Text, default="")
    recommended_action_reason: Mapped[str] = mapped_column(Text, default="")
    risk_if_ignored: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OpportunityActionFeedback(Base):
    __tablename__ = "opportunity_action_feedback"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    action_id: Mapped[int] = mapped_column(Integer, index=True)
    shown_to_role: Mapped[str] = mapped_column(String(128), default="")
    decision: Mapped[str] = mapped_column(String(64), default="")
    rejection_reason: Mapped[str] = mapped_column(Text, default="")
    executed: Mapped[str] = mapped_column(String(16), default="no")
    outcome_note: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

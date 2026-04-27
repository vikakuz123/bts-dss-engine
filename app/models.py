from __future__ import annotations

from sqlalchemy import Boolean, Float, JSON, DateTime, Integer, String, Text, func
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


class BusinessPolicy(Base):
    __tablename__ = "business_policies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    policy_code: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    policy_name: Mapped[str] = mapped_column(String(255), default="")
    value: Mapped[dict] = mapped_column(JSON, default=dict)
    description: Mapped[str] = mapped_column(Text, default="")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class DictionaryItem(Base):
    __tablename__ = "dictionary_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dictionary_code: Mapped[str] = mapped_column(String(128), index=True)
    item_code: Mapped[str] = mapped_column(String(128), index=True)
    name: Mapped[str] = mapped_column(String(512), default="")
    normalized_name: Mapped[str] = mapped_column(String(512), default="")
    aliases: Mapped[list] = mapped_column(JSON, default=list)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class EntityResolutionRecord(Base):
    __tablename__ = "entity_resolution_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    entity_type: Mapped[str] = mapped_column(String(128), index=True)
    raw_value: Mapped[str] = mapped_column(Text, default="")
    normalized_value: Mapped[str] = mapped_column(Text, default="")
    confidence_score: Mapped[float] = mapped_column(Float, default=0.0)
    resolved_entity_id: Mapped[str] = mapped_column(String(128), default="")
    resolution_reason: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class CommunicationEvent(Base):
    __tablename__ = "communication_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(128), default="bitrix24", index=True)
    source_event_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    source_entity_type: Mapped[str] = mapped_column(String(128), default="")
    source_entity_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    event_type: Mapped[str] = mapped_column(String(128), default="")
    channel: Mapped[str] = mapped_column(String(128), default="")
    occurred_at: Mapped[str] = mapped_column(String(64), default="")
    text: Mapped[str] = mapped_column(Text, default="")
    transcript_ref: Mapped[str] = mapped_column(Text, default="")
    raw_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    processing_status: Mapped[str] = mapped_column(String(64), default="new", index=True)
    error_message: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class CommunicationExtraction(Base):
    __tablename__ = "communication_extractions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    communication_event_id: Mapped[int] = mapped_column(Integer, index=True)
    extracted_json: Mapped[dict] = mapped_column(JSON, default=dict)
    confidence_score: Mapped[float] = mapped_column(Float, default=0.0)
    extractor_version: Mapped[str] = mapped_column(String(64), default="rules_v1")
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OpportunityUnit(Base):
    __tablename__ = "opportunity_units"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    bitrix_deal_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    client_name: Mapped[str] = mapped_column(String(512), default="")
    client_entity_id: Mapped[str] = mapped_column(String(128), default="")
    contact_name: Mapped[str] = mapped_column(String(512), default="")
    contact_entity_id: Mapped[str] = mapped_column(String(128), default="")
    object_name: Mapped[str] = mapped_column(String(512), default="")
    object_entity_id: Mapped[str] = mapped_column(String(128), default="")
    object_address: Mapped[str] = mapped_column(Text, default="")
    equipment_type: Mapped[str] = mapped_column(String(255), default="")
    equipment_model: Mapped[str] = mapped_column(String(255), default="")
    need_window: Mapped[str] = mapped_column(String(255), default="")
    rental_duration: Mapped[str] = mapped_column(String(255), default="")
    commercial_scenario: Mapped[str] = mapped_column(String(128), default="unknown")
    decision_access_status: Mapped[str] = mapped_column(String(128), default="unknown")
    risk_level: Mapped[str] = mapped_column(String(64), default="unknown")
    economic_value: Mapped[float] = mapped_column(Float, default=0.0)
    margin_estimate: Mapped[float] = mapped_column(Float, default=0.0)
    next_step: Mapped[str] = mapped_column(Text, default="")
    owner_manager_id: Mapped[str] = mapped_column(String(128), default="")
    data_quality_score: Mapped[float] = mapped_column(Float, default=0.0)
    raw_context: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OpportunityScore(Base):
    __tablename__ = "opportunity_scores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_unit_id: Mapped[int] = mapped_column(Integer, index=True)
    need_score: Mapped[float] = mapped_column(Float, default=0.0)
    time_score: Mapped[float] = mapped_column(Float, default=0.0)
    spec_score: Mapped[float] = mapped_column(Float, default=0.0)
    access_score: Mapped[float] = mapped_column(Float, default=0.0)
    money_score: Mapped[float] = mapped_column(Float, default=0.0)
    fit_score: Mapped[float] = mapped_column(Float, default=0.0)
    pclose: Mapped[float] = mapped_column(Float, default=0.0)
    econ_value: Mapped[float] = mapped_column(Float, default=0.0)
    urgency: Mapped[float] = mapped_column(Float, default=0.0)
    actionability: Mapped[float] = mapped_column(Float, default=0.0)
    strategy_weight: Mapped[float] = mapped_column(Float, default=1.0)
    priority_score: Mapped[float] = mapped_column(Float, default=0.0)
    blocked_reason: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OpportunityState(Base):
    __tablename__ = "opportunity_states"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_unit_id: Mapped[int] = mapped_column(Integer, index=True)
    state_code: Mapped[str] = mapped_column(String(128), index=True)
    state_name: Mapped[str] = mapped_column(String(255), default="")
    confidence_score: Mapped[float] = mapped_column(Float, default=0.0)
    reason: Mapped[str] = mapped_column(Text, default="")
    evidence_json: Mapped[dict] = mapped_column(JSON, default=dict)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ActionTemplate(Base):
    __tablename__ = "action_templates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    action_code: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    action_name: Mapped[str] = mapped_column(String(255), default="")
    action_type: Mapped[str] = mapped_column(String(128), default="")
    target_role: Mapped[str] = mapped_column(String(128), default="")
    description: Mapped[str] = mapped_column(Text, default="")
    trigger_conditions: Mapped[dict] = mapped_column(JSON, default=dict)
    expected_effect: Mapped[str] = mapped_column(Text, default="")
    deadline_sla_minutes: Mapped[int] = mapped_column(Integer, default=0)
    escalation_rule: Mapped[str] = mapped_column(Text, default="")
    requires_approval: Mapped[bool] = mapped_column(Boolean, default=False)
    owner_visible: Mapped[bool] = mapped_column(Boolean, default=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class DecisionRecommendation(Base):
    __tablename__ = "decision_recommendations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_unit_id: Mapped[int] = mapped_column(Integer, index=True)
    action_template_id: Mapped[int] = mapped_column(Integer, index=True)
    action_code: Mapped[str] = mapped_column(String(128), index=True)
    target_role: Mapped[str] = mapped_column(String(128), default="")
    owner_id: Mapped[str] = mapped_column(String(128), default="")
    deadline_at: Mapped[str] = mapped_column(String(64), default="")
    requires_escalation: Mapped[bool] = mapped_column(Boolean, default=False)
    escalation_role: Mapped[str] = mapped_column(String(128), default="")
    status: Mapped[str] = mapped_column(String(64), default="open", index=True)
    explainability_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class RecommendationFeedback(Base):
    __tablename__ = "recommendation_feedback"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    recommendation_id: Mapped[int] = mapped_column(Integer, index=True)
    shown_to_user_id: Mapped[str] = mapped_column(String(128), default="")
    shown_to_role: Mapped[str] = mapped_column(String(128), default="")
    was_shown: Mapped[bool] = mapped_column(Boolean, default=True)
    decision: Mapped[str] = mapped_column(String(64), default="")
    rejection_reason: Mapped[str] = mapped_column(Text, default="")
    was_executed: Mapped[bool] = mapped_column(Boolean, default=False)
    deal_outcome: Mapped[str] = mapped_column(String(64), default="")
    effect_1d: Mapped[str] = mapped_column(Text, default="")
    effect_3d: Mapped[str] = mapped_column(Text, default="")
    effect_7d: Mapped[str] = mapped_column(Text, default="")
    effect_30d: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(128), index=True)
    entity_type: Mapped[str] = mapped_column(String(128), default="")
    entity_id: Mapped[str] = mapped_column(String(128), default="")
    status: Mapped[str] = mapped_column(String(64), default="ok")
    message: Mapped[str] = mapped_column(Text, default="")
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

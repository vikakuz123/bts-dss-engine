from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import (
    Base,
    Opportunity,
    OpportunityAction,
    OpportunityActionFeedback,
    OpportunityExplainability,
    RawBitrixDeal,
)


def normalize_database_url(database_url: str) -> str:
    if database_url.startswith("postgresql://"):
        return database_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return database_url


def build_engine(settings: Settings) -> Engine:
    return create_engine(normalize_database_url(settings.database_url), pool_pre_ping=True)


def check_database(engine: Engine) -> dict[str, object]:
    with engine.connect() as connection:
        current_database = connection.execute(text("select current_database()")).scalar_one()
        current_user = connection.execute(text("select current_user")).scalar_one()

    return {
        "database": current_database,
        "user": current_user,
    }


def create_tables(engine: Engine) -> list[str]:
    Base.metadata.create_all(engine)
    return sorted(Base.metadata.tables.keys())


def list_raw_bitrix_deals(engine: Engine, limit: int = 20) -> list[dict[str, object]]:
    with Session(engine) as session:
        deals = (
            session.query(RawBitrixDeal)
            .order_by(RawBitrixDeal.id.desc())
            .limit(limit)
            .all()
        )

    return [
        {
            "id": deal.id,
            "bitrix_id": deal.bitrix_id,
            "title": deal.title,
            "stage_id": deal.stage_id,
            "company_id": deal.company_id,
            "contact_id": deal.contact_id,
            "opportunity": deal.opportunity,
            "currency_id": deal.currency_id,
            "comments": deal.comments,
            "created_at": deal.created_at.isoformat() if deal.created_at else None,
        }
        for deal in deals
    ]


def upsert_raw_bitrix_deals(engine: Engine, records: list[dict]) -> dict[str, int]:
    inserted = 0
    updated = 0

    with Session(engine) as session:
        for record in records:
            bitrix_id = str(record.get("ID", "")).strip()
            if not bitrix_id:
                continue

            existing = session.query(RawBitrixDeal).filter_by(bitrix_id=bitrix_id).one_or_none()
            payload = {
                "title": str(record.get("TITLE", "") or ""),
                "stage_id": str(record.get("STAGE_ID", "") or ""),
                "company_id": str(record.get("COMPANY_ID", "") or ""),
                "contact_id": str(record.get("CONTACT_ID", "") or ""),
                "opportunity": str(record.get("OPPORTUNITY", "") or ""),
                "currency_id": str(record.get("CURRENCY_ID", "") or ""),
                "comments": str(record.get("COMMENTS", "") or ""),
                "raw_payload": record,
            }

            if existing is None:
                session.add(
                    RawBitrixDeal(
                        bitrix_id=bitrix_id,
                        **payload,
                    )
                )
                inserted += 1
            else:
                for key, value in payload.items():
                    setattr(existing, key, value)
                updated += 1

        session.commit()

    return {
        "inserted": inserted,
        "updated": updated,
        "total": inserted + updated,
    }


def build_opportunities_from_raw_deals(engine: Engine) -> dict[str, int]:
    inserted = 0
    updated = 0

    with Session(engine) as session:
        raw_deals = session.query(RawBitrixDeal).order_by(RawBitrixDeal.id.asc()).all()

        for raw_deal in raw_deals:
            existing = (
                session.query(Opportunity)
                .filter_by(source_deal_id=raw_deal.bitrix_id)
                .one_or_none()
            )

            payload = {
                "title": raw_deal.title,
                "stage_id": raw_deal.stage_id,
                "company_id": raw_deal.company_id,
                "contact_id": raw_deal.contact_id,
                "opportunity_value": raw_deal.opportunity,
                "currency_id": raw_deal.currency_id,
                "last_comment": raw_deal.comments,
                "next_step": "",
                "priority_score": "0",
                "state_code": "raw_new",
            }

            if existing is None:
                session.add(
                    Opportunity(
                        source_deal_id=raw_deal.bitrix_id,
                        **payload,
                    )
                )
                inserted += 1
            else:
                for key, value in payload.items():
                    setattr(existing, key, value)
                updated += 1

        session.commit()

    return {
        "inserted": inserted,
        "updated": updated,
        "total": inserted + updated,
    }


def list_opportunities(engine: Engine, limit: int = 20) -> list[dict[str, object]]:
    with Session(engine) as session:
        items = (
            session.query(Opportunity)
            .order_by(Opportunity.id.desc())
            .limit(limit)
            .all()
        )

    return [
        {
            "id": item.id,
            "source_deal_id": item.source_deal_id,
            "title": item.title,
            "stage_id": item.stage_id,
            "company_id": item.company_id,
            "contact_id": item.contact_id,
            "opportunity_value": item.opportunity_value,
            "currency_id": item.currency_id,
            "last_comment": item.last_comment,
            "next_step": item.next_step,
            "priority_score": item.priority_score,
            "state_code": item.state_code,
            "created_at": item.created_at.isoformat() if item.created_at else None,
        }
        for item in items
    ]


def recompute_opportunity_states(engine: Engine) -> dict[str, int]:
    updated = 0

    with Session(engine) as session:
        items = session.query(Opportunity).order_by(Opportunity.id.asc()).all()

        for item in items:
            new_state_code = item.state_code

            if item.stage_id == "NEW" and (item.next_step or "").strip():
                new_state_code = "in_progress"
            elif item.stage_id == "NEW" and not (item.next_step or "").strip():
                new_state_code = "needs_attention"
            elif item.stage_id == "NEW":
                new_state_code = "raw_new"

            if item.state_code != new_state_code:
                item.state_code = new_state_code
                updated += 1

        session.commit()

    return {
        "updated": updated,
    }


def build_actions_from_opportunities(engine: Engine) -> dict[str, int]:
    inserted = 0
    updated = 0

    with Session(engine) as session:
        items = session.query(Opportunity).order_by(Opportunity.id.asc()).all()

        for item in items:
            if item.state_code == "needs_attention":
                action_code = "follow_up_manager"
                payload = {
                    "action_name": "Follow up with manager",
                    "target_role": "manager",
                    "reason": "Opportunity is new and has no next step",
                    "priority": "high",
                    "status": "open",
                }
            elif item.state_code == "in_progress":
                action_code = "monitor_progress"
                payload = {
                    "action_name": "Monitor progress",
                    "target_role": "manager",
                    "reason": "Opportunity has a next step and should be monitored for execution",
                    "priority": "normal",
                    "status": "open",
                }
            else:
                continue

            existing = (
                session.query(OpportunityAction)
                .filter_by(
                    opportunity_id=item.id,
                    action_code=action_code,
                )
                .one_or_none()
            )

            if existing is None:
                session.add(
                    OpportunityAction(
                        opportunity_id=item.id,
                        action_code=action_code,
                        **payload,
                    )
                )
                inserted += 1
            else:
                for key, value in payload.items():
                    setattr(existing, key, value)
                updated += 1

        session.commit()

    return {
        "inserted": inserted,
        "updated": updated,
        "total": inserted + updated,
    }


def list_opportunity_actions(engine: Engine, limit: int = 20) -> list[dict[str, object]]:
    with Session(engine) as session:
        items = (
            session.query(OpportunityAction)
            .order_by(OpportunityAction.id.desc())
            .limit(limit)
            .all()
        )

    return [
        {
            "id": item.id,
            "opportunity_id": item.opportunity_id,
            "action_code": item.action_code,
            "action_name": item.action_name,
            "target_role": item.target_role,
            "reason": item.reason,
            "priority": item.priority,
            "status": item.status,
            "created_at": item.created_at.isoformat() if item.created_at else None,
        }
        for item in items
    ]


def build_explainability_from_actions(engine: Engine) -> dict[str, int]:
    inserted = 0
    updated = 0

    with Session(engine) as session:
        actions = session.query(OpportunityAction).order_by(OpportunityAction.id.asc()).all()

        for action in actions:
            opportunity = session.query(Opportunity).filter_by(id=action.opportunity_id).one_or_none()
            if opportunity is None:
                continue

            existing = (
                session.query(OpportunityExplainability)
                .filter_by(action_id=action.id)
                .one_or_none()
            )

            if action.action_code == "monitor_progress":
                summary = (
                    f"Opportunity '{opportunity.title}' is in progress because it already has "
                    f"a planned next step and remains in stage '{opportunity.stage_id}'."
                )
                why_important = "The opportunity is active and should be monitored so the planned next step is executed."
                recommended_action_reason = (
                    "The system recommends monitor_progress because the next step already exists "
                    "and the manager should control execution rather than create a new action from scratch."
                )
                risk_if_ignored = (
                    "If progress is not monitored, the planned next step may be missed and the opportunity "
                    "can fall back into a stagnant state."
                )
            else:
                summary = (
                    f"Opportunity '{opportunity.title}' requires attention because it is still in "
                    f"stage '{opportunity.stage_id}' and has no next step."
                )
                why_important = "The opportunity is new and currently has no recorded next action."
                recommended_action_reason = (
                    "The system recommends follow_up_manager because the manager needs to move "
                    "the opportunity forward from a new state into an active next step."
                )
                risk_if_ignored = (
                    "If the opportunity is ignored, it may remain without movement and lose priority "
                    "or commercial momentum."
                )

            payload = {
                "summary": summary,
                "why_important": why_important,
                "triggered_signals": (
                    f"state_code={opportunity.state_code}; action_code={action.action_code}; stage_id={opportunity.stage_id}; "
                    f"next_step_empty={not bool((opportunity.next_step or '').strip())}"
                ),
                "recommended_action_reason": recommended_action_reason,
                "risk_if_ignored": risk_if_ignored,
            }

            if existing is None:
                session.add(
                    OpportunityExplainability(
                        opportunity_id=opportunity.id,
                        action_id=action.id,
                        **payload,
                    )
                )
                inserted += 1
            else:
                for key, value in payload.items():
                    setattr(existing, key, value)
                updated += 1

        session.commit()

    return {
        "inserted": inserted,
        "updated": updated,
        "total": inserted + updated,
    }


def list_explainability(engine: Engine, limit: int = 20) -> list[dict[str, object]]:
    with Session(engine) as session:
        items = (
            session.query(OpportunityExplainability)
            .order_by(OpportunityExplainability.id.desc())
            .limit(limit)
            .all()
        )

    return [
        {
            "id": item.id,
            "opportunity_id": item.opportunity_id,
            "action_id": item.action_id,
            "summary": item.summary,
            "why_important": item.why_important,
            "triggered_signals": item.triggered_signals,
            "recommended_action_reason": item.recommended_action_reason,
            "risk_if_ignored": item.risk_if_ignored,
            "created_at": item.created_at.isoformat() if item.created_at else None,
        }
        for item in items
    ]


def create_action_feedback(
    engine: Engine,
    action_id: int,
    shown_to_role: str,
    decision: str,
    rejection_reason: str,
    executed: str,
    outcome_note: str,
) -> dict[str, object]:
    with Session(engine) as session:
        feedback = OpportunityActionFeedback(
            action_id=action_id,
            shown_to_role=shown_to_role,
            decision=decision,
            rejection_reason=rejection_reason,
            executed=executed,
            outcome_note=outcome_note,
        )
        session.add(feedback)
        session.commit()
        session.refresh(feedback)

    return {
        "id": feedback.id,
        "action_id": feedback.action_id,
        "shown_to_role": feedback.shown_to_role,
        "decision": feedback.decision,
        "rejection_reason": feedback.rejection_reason,
        "executed": feedback.executed,
        "outcome_note": feedback.outcome_note,
        "created_at": feedback.created_at.isoformat() if feedback.created_at else None,
    }


def list_action_feedback(engine: Engine, limit: int = 20) -> list[dict[str, object]]:
    with Session(engine) as session:
        items = (
            session.query(OpportunityActionFeedback)
            .order_by(OpportunityActionFeedback.id.desc())
            .limit(limit)
            .all()
        )

    return [
        {
            "id": item.id,
            "action_id": item.action_id,
            "shown_to_role": item.shown_to_role,
            "decision": item.decision,
            "rejection_reason": item.rejection_reason,
            "executed": item.executed,
            "outcome_note": item.outcome_note,
            "created_at": item.created_at.isoformat() if item.created_at else None,
        }
        for item in items
    ]


def update_opportunity_next_step(engine: Engine, opportunity_id: int, next_step: str) -> dict[str, object] | None:
    with Session(engine) as session:
        item = session.query(Opportunity).filter_by(id=opportunity_id).one_or_none()
        if item is None:
            return None

        item.next_step = next_step
        session.commit()
        session.refresh(item)

    return {
        "id": item.id,
        "source_deal_id": item.source_deal_id,
        "title": item.title,
        "stage_id": item.stage_id,
        "next_step": item.next_step,
        "state_code": item.state_code,
        "priority_score": item.priority_score,
    }


def recompute_opportunity_priority_scores(engine: Engine) -> dict[str, int]:
    updated = 0

    with Session(engine) as session:
        items = session.query(Opportunity).order_by(Opportunity.id.asc()).all()

        for item in items:
            score = 0

            if item.state_code == "needs_attention":
                score += 70
            elif item.state_code == "in_progress":
                score += 40

            if item.stage_id == "NEW":
                score += 20

            if not (item.next_step or "").strip():
                score += 10
            else:
                score -= 15

            if not (item.company_id or "").strip():
                score -= 5

            if score < 0:
                score = 0

            new_priority_score = str(score)
            if item.priority_score != new_priority_score:
                item.priority_score = new_priority_score
                updated += 1

        session.commit()

    return {
        "updated": updated,
    }

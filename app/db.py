from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any

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


def _as_text(value: object) -> str:
    return str(value or "").strip()


def _parse_amount(value: object) -> float:
    raw = _as_text(value).replace(" ", "").replace(",", ".")
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _contains_any(value: object, needles: tuple[str, ...]) -> bool:
    haystack = _as_text(value).upper()
    return any(needle in haystack for needle in needles)


def _age_in_days(created_at: datetime | None) -> int:
    if created_at is None:
        return 0
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    return max(0, int((datetime.now(timezone.utc) - created_at).total_seconds() // 86400))


def _to_percent(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100, 2)


def _stage_rank(stage_id: object) -> tuple[int, str]:
    stage = _as_text(stage_id).upper()
    hints = [
        ("NEW", 10),
        ("QUAL", 20),
        ("CONTACT", 30),
        ("MEET", 40),
        ("PREPAR", 50),
        ("PROPOSAL", 60),
        ("QUOTE", 65),
        ("NEGOT", 70),
        ("FINAL", 80),
        ("WON", 90),
        ("SUCCESS", 90),
        ("LOSE", 95),
        ("FAIL", 95),
        ("CANCEL", 95),
    ]
    for needle, rank in hints:
        if needle in stage:
            return rank, stage
    return 50, stage


def _stage_label(stage_id: object) -> str:
    stage = _as_text(stage_id)
    if not stage:
        return "Unknown"
    return stage.replace("_", " ").replace(":", " / ")


def _split_reason_tokens(raw: object) -> list[str]:
    value = _as_text(raw)
    if not value:
        return []
    parts = re.split(r"[,\n;|/]+", value)
    return [part.strip(" .:-") for part in parts if part.strip(" .:-")]


def _extract_failure_reasons(raw_payload: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    preferred_keys: list[str] = []
    fallback_keys: list[str] = []

    for key in raw_payload:
        upper_key = key.upper()
        if any(token in upper_key for token in ("LOSS", "FAIL", "REJECT", "CANCEL", "REASON")):
            if upper_key.startswith("UF_CRM"):
                preferred_keys.append(key)
            else:
                fallback_keys.append(key)

    for key in preferred_keys + fallback_keys:
        reasons.extend(_split_reason_tokens(raw_payload.get(key)))

    unique_reasons: list[str] = []
    seen: set[str] = set()
    for reason in reasons:
        normalized = reason.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_reasons.append(reason)

    if unique_reasons:
        return unique_reasons[:4]

    comments = _split_reason_tokens(raw_payload.get("COMMENTS"))
    if comments:
        return comments[:1]
    return ["Unspecified loss reason"]


def _deal_is_won(raw_payload: dict[str, Any]) -> bool:
    stage_id = _as_text(raw_payload.get("STAGE_ID")).upper()
    semantic = _as_text(raw_payload.get("STAGE_SEMANTIC_ID")).upper()
    won_flag = _as_text(raw_payload.get("WON")).upper()
    return semantic == "S" or won_flag in {"Y", "1", "TRUE"} or _contains_any(stage_id, ("WON", "SUCCESS"))


def _deal_is_lost(raw_payload: dict[str, Any]) -> bool:
    stage_id = _as_text(raw_payload.get("STAGE_ID")).upper()
    semantic = _as_text(raw_payload.get("STAGE_SEMANTIC_ID")).upper()
    closed_flag = _as_text(raw_payload.get("CLOSED")).upper()
    if semantic == "F":
        return True
    if closed_flag in {"Y", "1", "TRUE"} and not _deal_is_won(raw_payload):
        return True
    return _contains_any(stage_id, ("LOSE", "FAIL", "CANCEL", "REJECT"))


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


def list_raw_bitrix_deals_for_analytics(engine: Engine) -> list[dict[str, Any]]:
    with Session(engine) as session:
        deals = session.query(RawBitrixDeal).order_by(RawBitrixDeal.id.asc()).all()

    items: list[dict[str, Any]] = []
    for deal in deals:
        raw_payload = deal.raw_payload or {}
        items.append(
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
                "raw_payload": raw_payload,
                "created_at": deal.created_at.isoformat() if deal.created_at else None,
            }
        )

    return items


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
            "age_days": _age_in_days(item.created_at),
            "created_at": item.created_at.isoformat() if item.created_at else None,
        }
        for item in items
    ]


def recompute_opportunity_states(engine: Engine) -> dict[str, int]:
    updated = 0

    with Session(engine) as session:
        items = session.query(Opportunity).order_by(Opportunity.id.asc()).all()

        for item in items:
            stage = _as_text(item.stage_id).upper()
            next_step_present = bool(_as_text(item.next_step))
            has_comment = bool(_as_text(item.last_comment))
            amount = _parse_amount(item.opportunity_value)

            if _contains_any(stage, ("WON", "SUCCESS", "CLOSE", "CLOSED")):
                new_state_code = "closed"
            elif _contains_any(stage, ("LOSE", "FAILED", "CANCEL")):
                new_state_code = "lost"
            elif not _as_text(item.company_id) and not _as_text(item.contact_id):
                new_state_code = "missing_data"
            elif not next_step_present and not has_comment:
                new_state_code = "stalled"
            elif not next_step_present:
                new_state_code = "needs_attention"
            elif _contains_any(stage, ("PROPOSAL", "PREPAR", "QUOTE", "PRESENT")):
                new_state_code = "proposal_pending"
            elif amount >= 100000:
                new_state_code = "high_value_in_progress"
            else:
                new_state_code = "in_progress"

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
            desired_actions: list[tuple[str, dict[str, str]]] = []
            amount = _parse_amount(item.opportunity_value)

            if item.state_code == "missing_data":
                desired_actions.append(
                    (
                        "request_missing_data",
                        {
                            "action_name": "Запросить недостающие данные CRM",
                            "target_role": "sales_ops",
                            "reason": "У сделки отсутствует привязка к компании или контакту, поэтому ее нельзя безопасно двигать дальше.",
                            "priority": "high",
                            "status": "open",
                        },
                    )
                )
            if item.state_code in {"needs_attention", "stalled"}:
                desired_actions.append(
                    (
                        "follow_up_manager",
                        {
                            "action_name": "Связаться с менеджером",
                            "target_role": "manager",
                            "reason": "У сделки нет понятного следующего шага, поэтому нужен ответственный менеджер, чтобы сдвинуть ее вперед.",
                            "priority": "high",
                            "status": "open",
                        },
                    )
                )
            if item.state_code == "stalled":
                desired_actions.append(
                    (
                        "revive_client_contact",
                        {
                            "action_name": "Возобновить контакт с клиентом",
                            "target_role": "manager",
                            "reason": "Следующий шаг не определен и свежего полезного контекста нет, поэтому сделка рискует остыть.",
                            "priority": "high",
                            "status": "open",
                        },
                    )
                )
            if item.state_code in {"in_progress", "high_value_in_progress"}:
                desired_actions.append(
                    (
                        "monitor_progress",
                        {
                            "action_name": "Контролировать продвижение",
                            "target_role": "manager",
                            "reason": "У сделки уже есть следующий шаг, поэтому сейчас важно контролировать его выполнение.",
                            "priority": "normal",
                            "status": "open",
                        },
                    )
                )
            if item.state_code in {"proposal_pending", "high_value_in_progress"}:
                desired_actions.append(
                    (
                        "prepare_offer",
                        {
                            "action_name": "Подготовить коммерческое предложение",
                            "target_role": "manager",
                            "reason": "Сделка близка к этапу предложения и требует конкретного оффера или презентации.",
                            "priority": "high" if amount >= 100000 else "normal",
                            "status": "open",
                        },
                    )
                )
            if _as_text(item.contact_id) and not _as_text(item.next_step):
                desired_actions.append(
                    (
                        "schedule_client_call",
                        {
                            "action_name": "Запланировать звонок клиенту",
                            "target_role": "manager",
                            "reason": "Контакт уже есть, но конкретное следующее действие по сделке все еще не определено.",
                            "priority": "normal",
                            "status": "open",
                        },
                    )
                )
            if amount >= 250000:
                desired_actions.append(
                    (
                        "escalate_to_supervisor",
                        {
                            "action_name": "Эскалировать руководителю",
                            "target_role": "supervisor",
                            "reason": "Сделка с высокой ценностью требует контроля со стороны руководителя для снижения риска исполнения.",
                            "priority": "high",
                            "status": "open",
                        },
                    )
                )

            existing_items = (
                session.query(OpportunityAction)
                .filter_by(opportunity_id=item.id)
                .all()
            )
            existing_by_code = {existing.action_code: existing for existing in existing_items}
            desired_codes = {action_code for action_code, _ in desired_actions}

            for action_code, payload in desired_actions:
                existing = existing_by_code.get(action_code)
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

            for existing in existing_items:
                if existing.action_code not in desired_codes and existing.status not in {"done", "rejected"}:
                    existing.status = "closed"
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

            explainability_map: dict[str, dict[str, str]] = {
                "monitor_progress": {
                    "summary": (
                        f"У сделки '{opportunity.title}' уже есть следующий шаг, и она остается активной "
                        f"на этапе '{opportunity.stage_id}'."
                    ),
                    "why_important": "Сейчас риск исполнения важнее риска планирования, поэтому критично именно доведение действия до результата.",
                    "recommended_action_reason": "Система рекомендует контролировать выполнение, а не перепланировать, потому что конкретный следующий шаг уже задан.",
                    "risk_if_ignored": "Сделка может незаметно застопориться, если выполнение запланированного действия никто не проверит.",
                },
                "follow_up_manager": {
                    "summary": (
                        f"У сделки '{opportunity.title}' нет надежно определенного следующего шага на этапе '{opportunity.stage_id}'."
                    ),
                    "why_important": "Без следующего действия, закрепленного за ответственным, даже перспективная сделка быстро теряет импульс.",
                    "recommended_action_reason": "Система рекомендует подключение менеджера, чтобы вернуть ответственность за сделку и определить следующий шаг.",
                    "risk_if_ignored": "Сделка может оставаться открытой в CRM, но фактически перестать двигаться.",
                },
                "request_missing_data": {
                    "summary": (
                        f"У сделки '{opportunity.title}' отсутствуют критически важные CRM-данные, например привязка к компании или контакту."
                    ),
                    "why_important": "Неполные данные блокируют маршрутизацию, коммуникацию и корректную отчетность.",
                    "recommended_action_reason": "Система рекомендует сначала заполнить недостающие CRM-данные, а уже потом переходить к более сложному планированию действий.",
                    "risk_if_ignored": "Команда может работать не с тем контактом или потерять сделку из-за пробелов в данных и отчетности.",
                },
                "prepare_offer": {
                    "summary": (
                        f"Сделка '{opportunity.title}' по своему текущему этапу близка к подготовке предложения."
                    ),
                    "why_important": "Сделки на стадии предложения конвертируются лучше, когда коммерческое предложение готовится без задержек.",
                    "recommended_action_reason": "Система рекомендует подготовить предложение, потому что текущий этап показывает, что клиент уже находится в процессе оценки вариантов.",
                    "risk_if_ignored": "Медленная реакция может подтолкнуть клиента к другому варианту или заморозить переговоры.",
                },
                "schedule_client_call": {
                    "summary": (
                        f"У сделки '{opportunity.title}' уже есть контакт, но по ней все еще не определен конкретный следующий шаг."
                    ),
                    "why_important": "Наличие готового контакта снижает барьер для действия, а быстрый звонок часто является самым простым способом вернуть импульс сделке.",
                    "recommended_action_reason": "Система рекомендует запланировать звонок клиенту, потому что коммуникацию можно начать немедленно.",
                    "risk_if_ignored": "Сделка может простаивать, хотя данных для действия уже достаточно.",
                },
                "revive_client_contact": {
                    "summary": (
                        f"Сделка '{opportunity.title}' выглядит остывшей, потому что по ней нет следующего шага и почти нет полезного актуального контекста."
                    ),
                    "why_important": "Остывшие сделки быстро теряют шансы и часто требуют отдельного действия для реактивации.",
                    "recommended_action_reason": "Система рекомендует реактивацию контакта, потому что пассивное ожидание вряд ли улучшит ситуацию.",
                    "risk_if_ignored": "Сделка может фактически умереть, продолжая при этом засорять активную воронку.",
                },
                "escalate_to_supervisor": {
                    "summary": (
                        f"Сделка '{opportunity.title}' имеет высокую ценность и требует внимания руководителя."
                    ),
                    "why_important": "Крупные сделки дают больший потенциал, но и несут больше риска, поэтому усиленный контроль здесь оправдан.",
                    "recommended_action_reason": "Система рекомендует эскалацию, чтобы улучшить координацию и снизить вероятность ошибок исполнения.",
                    "risk_if_ignored": "Ценная сделка может сорваться до того, как кто-то вовремя заметит растущий риск.",
                },
            }
            details = explainability_map.get(
                action.action_code,
                {
                    "summary": f"Для сделки '{opportunity.title}' было сформировано действие '{action.action_name}'.",
                    "why_important": "Система обнаружила паттерн, который требует внимания.",
                    "recommended_action_reason": "Это действие лучше всего соответствует текущему состоянию сделки.",
                    "risk_if_ignored": "Сделка может потерять импульс или корректность прохождения по воронке.",
                },
            )

            payload = {
                "summary": details["summary"],
                "why_important": details["why_important"],
                "triggered_signals": (
                    f"state_code={opportunity.state_code}; action_code={action.action_code}; stage_id={opportunity.stage_id}; "
                    f"next_step_empty={not bool((opportunity.next_step or '').strip())}; "
                    f"amount={_parse_amount(opportunity.opportunity_value)}; age_days={_age_in_days(opportunity.created_at)}"
                ),
                "recommended_action_reason": details["recommended_action_reason"],
                "risk_if_ignored": details["risk_if_ignored"],
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
        action = session.query(OpportunityAction).filter_by(id=action_id).one_or_none()
        if action is None:
            raise RuntimeError(f"Action {action_id} not found")

        feedback = OpportunityActionFeedback(
            action_id=action_id,
            shown_to_role=shown_to_role,
            decision=decision,
            rejection_reason=rejection_reason,
            executed=executed,
            outcome_note=outcome_note,
        )
        session.add(feedback)
        if executed == "yes":
            action.status = "done"
        elif decision == "accepted":
            action.status = "accepted"
        elif decision == "postponed":
            action.status = "postponed"
        elif decision == "rejected":
            action.status = "rejected"
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


def update_action_status(engine: Engine, action_id: int, status: str) -> dict[str, object] | None:
    with Session(engine) as session:
        item = session.query(OpportunityAction).filter_by(id=action_id).one_or_none()
        if item is None:
            return None

        item.status = status
        session.commit()
        session.refresh(item)

    return {
        "id": item.id,
        "opportunity_id": item.opportunity_id,
        "action_code": item.action_code,
        "action_name": item.action_name,
        "status": item.status,
        "priority": item.priority,
    }


def recompute_opportunity_priority_scores(engine: Engine) -> dict[str, int]:
    updated = 0

    with Session(engine) as session:
        items = session.query(Opportunity).order_by(Opportunity.id.asc()).all()

        for item in items:
            score = 0

            amount = _parse_amount(item.opportunity_value)
            age_days = _age_in_days(item.created_at)
            state_weights = {
                "missing_data": 85,
                "stalled": 82,
                "needs_attention": 76,
                "proposal_pending": 66,
                "high_value_in_progress": 62,
                "in_progress": 44,
                "closed": 0,
                "lost": 0,
            }
            score += state_weights.get(item.state_code, 25)

            if _contains_any(item.stage_id, ("NEW",)):
                score += 10
            if _contains_any(item.stage_id, ("PROPOSAL", "QUOTE", "PREPAR", "PRESENT")):
                score += 12

            if amount >= 500000:
                score += 30
            elif amount >= 250000:
                score += 22
            elif amount >= 100000:
                score += 14
            elif amount > 0:
                score += 6

            if not _as_text(item.next_step):
                score += 18
            else:
                score -= 10

            if not _as_text(item.company_id):
                score += 8
            if not _as_text(item.contact_id):
                score += 7
            if not _as_text(item.last_comment):
                score += 10
            if age_days >= 30:
                score += 12
            elif age_days >= 14:
                score += 7
            elif age_days >= 7:
                score += 3

            feedback_summary = (
                session.query(
                    OpportunityActionFeedback.decision,
                    text("count(*)"),
                )
                .join(OpportunityAction, OpportunityAction.id == OpportunityActionFeedback.action_id)
                .filter(OpportunityAction.opportunity_id == item.id)
                .group_by(OpportunityActionFeedback.decision)
                .all()
            )
            feedback_counts = {decision: count for decision, count in feedback_summary}
            score += int(feedback_counts.get("rejected", 0)) * 8
            score += int(feedback_counts.get("postponed", 0)) * 5
            score -= int(feedback_counts.get("accepted", 0)) * 4

            if score < 0:
                score = 0
            if score > 100:
                score = 100

            new_priority_score = str(score)
            if item.priority_score != new_priority_score:
                item.priority_score = new_priority_score
                updated += 1

        session.commit()

    return {
        "updated": updated,
    }


def build_dashboard_analytics(
    raw_deals: list[dict[str, Any]],
    opportunities: list[dict[str, Any]],
    actions: list[dict[str, Any]],
    feedback_items: list[dict[str, Any]],
) -> dict[str, Any]:
    high_priority = sum(1 for item in opportunities if int(item.get("priority_score") or 0) >= 80)
    without_next_step = sum(1 for item in opportunities if not _as_text(item.get("next_step")))
    avg_priority = round(
        sum(int(item.get("priority_score") or 0) for item in opportunities) / len(opportunities),
        1,
    ) if opportunities else 0.0

    stage_counts: dict[str, int] = {}
    for item in opportunities:
        stage = _as_text(item.get("stage_id")) or "unknown"
        stage_counts[stage] = stage_counts.get(stage, 0) + 1

    feedback_counts: dict[str, int] = {}
    for item in feedback_items:
        decision = _as_text(item.get("decision")) or "unknown"
        feedback_counts[decision] = feedback_counts.get(decision, 0) + 1

    action_status_counts: dict[str, int] = {}
    for item in actions:
        status = _as_text(item.get("status")) or "unknown"
        action_status_counts[status] = action_status_counts.get(status, 0) + 1

    funnel = build_funnel_analytics(raw_deals)

    return {
        "high_priority": high_priority,
        "without_next_step": without_next_step,
        "avg_priority": avg_priority,
        "stage_counts": dict(sorted(stage_counts.items(), key=lambda pair: (-pair[1], pair[0]))[:8]),
        "feedback_counts": dict(sorted(feedback_counts.items(), key=lambda pair: (-pair[1], pair[0]))),
        "action_status_counts": dict(sorted(action_status_counts.items(), key=lambda pair: (-pair[1], pair[0]))),
        "funnel": funnel,
    }


def build_funnel_analytics(raw_deals: list[dict[str, Any]]) -> dict[str, Any]:
    if not raw_deals:
        return {
            "initial_count": 0,
            "won_count": 0,
            "lost_count": 0,
            "active_count": 0,
            "overall_success_conversion_pct": 0.0,
            "stages": [],
            "failure_reasons": [],
            "stage_failure_breakdown": [],
        }

    stage_map: dict[str, dict[str, Any]] = {}
    weighted_reason_counts: dict[str, float] = {}
    stage_reason_counts: dict[str, dict[str, float]] = {}
    won_count = 0
    lost_count = 0
    active_count = 0

    for deal in raw_deals:
        raw_payload = dict(deal.get("raw_payload") or {})
        stage_id = _as_text(raw_payload.get("STAGE_ID") or deal.get("stage_id")) or "UNKNOWN"
        stage_entry = stage_map.setdefault(
            stage_id,
            {
                "stage_id": stage_id,
                "label": _stage_label(stage_id),
                "count": 0,
                "active_count": 0,
                "won_count": 0,
                "lost_count": 0,
            },
        )
        stage_entry["count"] += 1

        if _deal_is_won(raw_payload):
            stage_entry["won_count"] += 1
            won_count += 1
        elif _deal_is_lost(raw_payload):
            stage_entry["lost_count"] += 1
            lost_count += 1
            reasons = _extract_failure_reasons(raw_payload)
            weight = round(1 / max(len(reasons), 1), 4)
            stage_reason_bucket = stage_reason_counts.setdefault(stage_id, {})
            for reason in reasons:
                weighted_reason_counts[reason] = weighted_reason_counts.get(reason, 0.0) + weight
                stage_reason_bucket[reason] = stage_reason_bucket.get(reason, 0.0) + weight
        else:
            stage_entry["active_count"] += 1
            active_count += 1

    ordered_stages = sorted(stage_map.values(), key=lambda item: _stage_rank(item["stage_id"]))
    initial_count = max(int(ordered_stages[0]["count"]), 1)
    previous_count = 0
    stages: list[dict[str, Any]] = []

    for item in ordered_stages:
        count = int(item["count"])
        stages.append(
            {
                **item,
                "conversion_from_start_pct": _to_percent(count, initial_count),
                "conversion_from_previous_pct": _to_percent(count, previous_count or initial_count),
            }
        )
        previous_count = count

    failure_reasons = [
        {
            "reason": reason,
            "weighted_deals": round(weight, 2),
            "share_of_lost_pct": _to_percent(weight, float(lost_count)),
        }
        for reason, weight in sorted(weighted_reason_counts.items(), key=lambda pair: (-pair[1], pair[0].lower()))
    ]

    stage_failure_breakdown = []
    for stage in ordered_stages:
        stage_id = stage["stage_id"]
        stage_lost_count = int(stage["lost_count"])
        stage_reasons = stage_reason_counts.get(stage_id, {})
        stage_failure_breakdown.append(
            {
                "stage_id": stage_id,
                "label": _stage_label(stage_id),
                "lost_count": stage_lost_count,
                "share_of_lost_pct": _to_percent(stage_lost_count, lost_count),
                "failure_reasons": [
                    {
                        "reason": reason,
                        "weighted_deals": round(weight, 2),
                        "share_within_stage_losses_pct": _to_percent(weight, float(stage_lost_count)),
                    }
                    for reason, weight in sorted(stage_reasons.items(), key=lambda pair: (-pair[1], pair[0].lower()))
                ],
            }
        )

    return {
        "initial_count": len(raw_deals),
        "won_count": won_count,
        "lost_count": lost_count,
        "active_count": active_count,
        "overall_success_conversion_pct": _to_percent(won_count, len(raw_deals)),
        "stages": stages,
        "failure_reasons": failure_reasons,
        "stage_failure_breakdown": stage_failure_breakdown,
    }

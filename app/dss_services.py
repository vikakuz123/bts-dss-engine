from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.db import _as_text, _parse_amount
from app.models import (
    ActionTemplate,
    AuditLog,
    CommunicationEvent,
    CommunicationExtraction,
    DecisionRecommendation,
    EntityResolutionRecord,
    Opportunity,
    OpportunityScore,
    OpportunityState,
    OpportunityUnit,
    RecommendationFeedback,
)


REQUIRED_OPPORTUNITY_FIELDS = (
    "client_name",
    "object_name",
    "equipment_type",
    "need_window",
    "rental_duration",
    "next_step",
    "owner_manager_id",
)

MATURITY_MARKERS = (
    "договор",
    "кп",
    "коммерческое",
    "счет",
    "оплата",
    "готовы",
    "подписать",
)

URGENCY_MARKERS = (
    "срочно",
    "сегодня",
    "завтра",
    "утром",
    "сейчас",
    "горит",
    "немедленно",
)

SPEC_MARKERS = (
    "тонн",
    "метр",
    "стрела",
    "ковш",
    "вылет",
    "смен",
    "час",
    "jcb",
    "manitou",
    "liebherr",
)

SUBRENT_MARKERS = (
    "субаренда",
    "партнер",
    "найти технику",
    "нет своей",
)

COMPETITOR_MARKERS = (
    "конкурент",
    "дешевле",
    "у других",
    "предложили",
)

DEBT_MARKERS = (
    "debt",
    "overdue",
    "blacklist",
    "дебиторк",
    "долг",
    "просроч",
    "РґРµР±РёС‚РѕСЂРє",
    "РґРѕР»Рі",
    "РїСЂРѕСЃСЂРѕС‡",
)

NEGATIVE_MARGIN_MARKERS = (
    "negative margin",
    "below margin",
    "ниже марж",
    "минус",
    "дорогая субаренда",
    "РЅРёР¶Рµ РјР°СЂР¶",
    "РјРёРЅСѓСЃ",
    "РґРѕСЂРѕРіР°СЏ СЃСѓР±Р°СЂРµРЅРґР°",
)

OWN_EQUIPMENT_MARKERS = (
    "own equipment",
    "available fleet",
    "своя техник",
    "наша техник",
    "есть в парке",
    "СЃРІРѕСЏ С‚РµС…РЅРёРє",
    "РЅР°С€Р° С‚РµС…РЅРёРє",
    "РµСЃС‚СЊ РІ РїР°СЂРєРµ",
)

CROSS_SELL_MARKERS = (
    "cross sell",
    "additional equipment",
    "дополнительн",
    "соседн",
    "кросс",
    "РґРѕРїРѕР»РЅРёС‚РµР»СЊРЅ",
    "СЃРѕСЃРµРґРЅ",
    "РєСЂРѕСЃСЃ",
)

PROMISE_MARKERS = (
    "promise",
    "overdue follow",
    "обещал",
    "обещание",
    "должен был",
    "просрочен",
    "РѕР±РµС‰Р°Р»",
    "РѕР±РµС‰Р°РЅРёРµ",
    "РґРѕР»Р¶РµРЅ Р±С‹Р»",
    "РїСЂРѕСЃСЂРѕС‡РµРЅ",
)


EQUIPMENT_ALIASES = {
    "excavator_loader": (
        "экскаватор-погрузчик",
        "экскаватор погрузчик",
        "jcb",
        "jcb 3cx",
        "3cx",
    ),
    "truck_crane": ("автокран", "кран", "truck crane"),
    "manipulator": ("манипулятор", "кму"),
    "loader": ("погрузчик", "фронтальный погрузчик"),
    "aerial_platform": ("автовышка", "вышка", "агп"),
    "telehandler": ("телескопический погрузчик", "manitou", "маниту"),
    "excavator": ("экскаватор", "гусеничный экскаватор", "колесный экскаватор"),
}

ENTITY_STOP_WORDS = (
    "срочно",
    "завтра",
    "сегодня",
    "послезавтра",
    "нужен",
    "нужна",
    "нужно",
    "требуется",
    "просит",
    "ждет",
)


def _repair_text(value: object) -> str:
    text = _as_text(value)
    candidates = [text]
    for encoding in ("cp1251", "latin1"):
        try:
            candidates.append(text.encode(encoding).decode("utf-8"))
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    repaired = min(candidates, key=lambda item: item.count("Р") + item.count("С") + item.count("�"))
    return repaired.replace("ё", "е").replace("Ё", "Е")


def _canon_text(value: object) -> str:
    return _repair_text(value).lower()


def _clean_extracted_value(value: object) -> str:
    text = _repair_text(value)
    text = re.split(
        r"\b(?:нужен|нужна|нужно|требуется|срочно|завтра|сегодня|контакт|лпр|адрес)\b",
        text,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    text = re.sub(r"[\"'`]+", "", text)
    text = re.sub(r"\s+", " ", text).strip(" .,:;()[]")
    return text[:120]


def _first_match(patterns: tuple[str, ...], text: str, flags: int = re.IGNORECASE) -> str:
    for pattern in patterns:
        match = re.search(pattern, text, flags=flags)
        if match:
            value = next((group for group in match.groups() if group), "")
            return _clean_extracted_value(value)
    return ""


def _normalize_equipment_type(value: object) -> str:
    lower = _canon_text(value)
    for normalized, aliases in EQUIPMENT_ALIASES.items():
        if any(alias in lower for alias in aliases):
            return normalized
    return _normalize_name(value)


def _normalize_name(value: object) -> str:
    text = _as_text(value).lower()
    text = text.replace("ё", "е")
    text = re.sub(r"[\"'`.,;:()]+", " ", text)
    text = re.sub(r"\b(ооо|ао|пао|ип|зао)\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _contains_marker(text: str, markers: tuple[str, ...]) -> bool:
    lower = text.lower().replace("ё", "е")
    return any(marker in lower for marker in markers)


def _score_from_markers(text: str, markers: tuple[str, ...], base: float = 20.0) -> float:
    if not text:
        return 0.0
    hits = sum(1 for marker in markers if marker in text.lower().replace("ё", "е"))
    return min(100.0, base + hits * 22.0)


def _normalize_name(value: object) -> str:
    text = _canon_text(value)
    text = re.sub(r"[\"'`.,;:()]+", " ", text)
    text = re.sub(r"\b(ооо|оао|ао|пао|ип|зао|llc|ltd|inc)\b", " ", text)
    text = re.sub(r"\b(?:%s)\b" % "|".join(ENTITY_STOP_WORDS), " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _contains_marker(text: str, markers: tuple[str, ...]) -> bool:
    lower = _canon_text(text)
    return any(_canon_text(marker) in lower for marker in markers)


def _score_from_markers(text: str, markers: tuple[str, ...], base: float = 20.0) -> float:
    if not text:
        return 0.0
    lower = _canon_text(text)
    hits = sum(1 for marker in markers if _canon_text(marker) in lower)
    return min(100.0, base + hits * 22.0)


def _context_text(unit: OpportunityUnit) -> str:
    raw_context = unit.raw_context or {}
    extracted = raw_context.get("extracted") if isinstance(raw_context, dict) else {}
    extracted_text = " ".join(str(value) for value in (extracted or {}).values())
    return " ".join(
        [
            _as_text(raw_context.get("source_text") if isinstance(raw_context, dict) else ""),
            extracted_text,
            unit.client_name,
            unit.contact_name,
            unit.object_name,
            unit.object_address,
            unit.equipment_type,
            unit.equipment_model,
            unit.need_window,
            unit.rental_duration,
            unit.commercial_scenario,
            unit.risk_level,
            unit.next_step,
        ]
    )


def _deadline(minutes: int) -> str:
    if minutes <= 0:
        return ""
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()


def _latest_score(session: Session, opportunity_unit_id: int) -> OpportunityScore | None:
    return (
        session.query(OpportunityScore)
        .filter_by(opportunity_unit_id=opportunity_unit_id)
        .order_by(OpportunityScore.id.desc())
        .first()
    )


def _active_states(session: Session, opportunity_unit_id: int) -> list[OpportunityState]:
    return (
        session.query(OpportunityState)
        .filter_by(opportunity_unit_id=opportunity_unit_id, active=True)
        .order_by(OpportunityState.id.desc())
        .all()
    )


def serialize_opportunity_unit(item: OpportunityUnit) -> dict[str, Any]:
    return {
        "id": item.id,
        "bitrix_deal_id": item.bitrix_deal_id,
        "client_name": item.client_name,
        "client_entity_id": item.client_entity_id,
        "contact_name": item.contact_name,
        "contact_entity_id": item.contact_entity_id,
        "object_name": item.object_name,
        "object_entity_id": item.object_entity_id,
        "object_address": item.object_address,
        "equipment_type": item.equipment_type,
        "equipment_model": item.equipment_model,
        "need_window": item.need_window,
        "rental_duration": item.rental_duration,
        "commercial_scenario": item.commercial_scenario,
        "decision_access_status": item.decision_access_status,
        "risk_level": item.risk_level,
        "economic_value": item.economic_value,
        "margin_estimate": item.margin_estimate,
        "next_step": item.next_step,
        "owner_manager_id": item.owner_manager_id,
        "data_quality_score": item.data_quality_score,
        "raw_context": item.raw_context or {},
        "created_at": item.created_at.isoformat() if item.created_at else None,
    }


def serialize_score(score: OpportunityScore | None) -> dict[str, Any] | None:
    if score is None:
        return None
    return {
        "need_score": score.need_score,
        "time_score": score.time_score,
        "spec_score": score.spec_score,
        "access_score": score.access_score,
        "money_score": score.money_score,
        "fit_score": score.fit_score,
        "pclose": score.pclose,
        "econ_value": score.econ_value,
        "urgency": score.urgency,
        "actionability": score.actionability,
        "strategy_weight": score.strategy_weight,
        "priority_score": score.priority_score,
        "blocked_reason": score.blocked_reason,
        "created_at": score.created_at.isoformat() if score.created_at else None,
    }


def serialize_state(state: OpportunityState) -> dict[str, Any]:
    return {
        "state_code": state.state_code,
        "state_name": state.state_name,
        "confidence_score": state.confidence_score,
        "reason": state.reason,
        "evidence": state.evidence_json or {},
        "created_at": state.created_at.isoformat() if state.created_at else None,
    }


def serialize_recommendation(item: DecisionRecommendation) -> dict[str, Any]:
    return {
        "id": item.id,
        "opportunity_unit_id": item.opportunity_unit_id,
        "action_template_id": item.action_template_id,
        "action_code": item.action_code,
        "target_role": item.target_role,
        "owner_id": item.owner_id,
        "deadline_at": item.deadline_at,
        "requires_escalation": item.requires_escalation,
        "escalation_role": item.escalation_role,
        "status": item.status,
        "explainability": item.explainability_json or {},
        "created_at": item.created_at.isoformat() if item.created_at else None,
    }


def seed_action_templates(engine: Engine) -> dict[str, int]:
    templates = [
        ("call_15_min", "Позвонить клиенту в течение 15 минут", "sales_call", "manager", 15, False),
        ("send_offer", "Отправить коммерческое предложение", "send_offer", "manager", 60, False),
        ("send_contract", "Отправить договор", "send_contract", "manager", 120, False),
        ("clarify_specs", "Уточнить технические параметры", "clarify_specs", "manager", 180, False),
        ("clarify_object_access", "Уточнить объект и условия заезда", "clarify_object_access", "manager", 180, False),
        ("reserve_own_equipment", "Зарезервировать свою технику", "reserve_own_equipment", "logistics", 60, False),
        ("request_subrent", "Передать запрос в субаренду", "request_subrent", "logistics", 60, False),
        ("reprice_deal", "Пересчитать ставку и маржу", "reprice_deal", "rop", 240, True),
        ("cross_sell_offer", "Предложить соседнюю технику на объект", "cross_sell_offer", "manager", 480, False),
        ("owner_escalation", "Эскалировать руководителю", "owner_escalation", "owner", 30, True),
        ("stop_deal", "Остановить работу по низкоприоритетной сделке", "stop_deal", "manager", 0, False),
        ("debt_control", "Проверить условия оплаты и дебиторку", "debt_control", "rop", 120, True),
        ("competitor_attack", "Запустить сценарий атаки объекта конкурента", "competitor_attack", "rop", 240, True),
        ("follow_up_reminder", "Вернуть просроченный follow-up в работу", "follow_up_reminder", "manager", 30, False),
    ]

    inserted = 0
    with Session(engine) as session:
        for code, name, action_type, role, sla, approval in templates:
            existing = session.query(ActionTemplate).filter_by(action_code=code).one_or_none()
            payload = {
                "action_name": name,
                "action_type": action_type,
                "target_role": role,
                "description": name,
                "trigger_conditions": {"version": "mvp_v1"},
                "expected_effect": "Сократить потерю горячей сделки и повысить управляемость следующего шага.",
                "deadline_sla_minutes": sla,
                "escalation_rule": "Эскалировать РОПу при просрочке SLA или экономическом ограничении.",
                "requires_approval": approval,
                "owner_visible": role in {"rop", "owner"},
                "active": True,
            }
            if existing is None:
                session.add(ActionTemplate(action_code=code, **payload))
                inserted += 1
            else:
                for key, value in payload.items():
                    setattr(existing, key, value)
        session.commit()
    return {"inserted": inserted, "total": len(templates)}


def ingest_bitrix_event(engine: Engine, payload: dict[str, Any]) -> dict[str, Any]:
    source_event_id = _as_text(payload.get("event_id") or payload.get("ID") or payload.get("id"))
    text = _as_text(payload.get("text") or payload.get("comment") or payload.get("COMMENTS") or payload.get("summary"))
    with Session(engine) as session:
        event = CommunicationEvent(
            source="bitrix24",
            source_event_id=source_event_id,
            source_entity_type=_as_text(payload.get("entity_type") or payload.get("ENTITY_TYPE") or "deal"),
            source_entity_id=_as_text(payload.get("entity_id") or payload.get("ENTITY_ID") or payload.get("deal_id")),
            event_type=_as_text(payload.get("event_type") or payload.get("type") or "timeline"),
            channel=_as_text(payload.get("channel") or "bitrix24"),
            occurred_at=_as_text(payload.get("occurred_at") or payload.get("DATE_CREATE")),
            text=text,
            transcript_ref=_as_text(payload.get("transcript_ref")),
            raw_payload=payload,
            processing_status="new",
        )
        session.add(event)
        session.add(AuditLog(event_type="bitrix_event_ingested", entity_type="CommunicationEvent", payload=payload))
        session.commit()
        session.refresh(event)
        return {"id": event.id, "processing_status": event.processing_status}


def extract_entities_from_text(text: str) -> dict[str, Any]:
    clean = _as_text(text)
    lower = clean.lower().replace("ё", "е")
    equipment_patterns = {
        "экскаватор-погрузчик": ("jcb", "экскаватор погрузчик", "экскаватор-погрузчик"),
        "автокран": ("автокран", "кран"),
        "манипулятор": ("манипулятор",),
        "погрузчик": ("погрузчик", "фронтальный"),
        "автовышка": ("автовышка", "вышка"),
    }
    equipment_type = ""
    for normalized, aliases in equipment_patterns.items():
        if any(alias in lower for alias in aliases):
            equipment_type = normalized
            break

    model_match = re.search(r"\b(jcb\s*3cx|jcb|manitou|liebherr|xcmg|zoomlion)\b", lower)
    date_match = re.search(r"\b(сегодня|завтра|послезавтра|с понедельника|на неделю|на месяц)\b", lower)
    duration_match = re.search(r"\b(\d+\s*(?:дн|день|дня|дней|смен|час|часа|часов|месяц|месяца))\b", lower)
    address_match = re.search(r"(?:объект|адрес|на объекте|в районе)\s+([^.;,\n]{3,80})", clean, flags=re.IGNORECASE)

    extracted = {
        "client_company": "",
        "contact_person": "",
        "project_object": address_match.group(1).strip() if address_match else "",
        "address_or_geo": address_match.group(1).strip() if address_match else "",
        "equipment_type": equipment_type,
        "equipment_model": model_match.group(1).upper() if model_match else "",
        "time_window": date_match.group(1) if date_match else "",
        "urgency": "high" if _contains_marker(clean, URGENCY_MARKERS) else "unknown",
        "rental_duration": duration_match.group(1) if duration_match else "",
        "work_conditions": "",
        "technical_parameters": [marker for marker in SPEC_MARKERS if marker in lower],
        "price_context": "mentioned" if any(word in lower for word in ("цена", "ставка", "дорого", "дешевле", "счет")) else "",
        "competitor_mention": _contains_marker(clean, COMPETITOR_MARKERS),
        "own_or_subrent_signal": "subrent" if _contains_marker(clean, SUBRENT_MARKERS) else "unknown",
        "contract_readiness": _contains_marker(clean, ("договор", "подписать")),
        "payment_readiness": _contains_marker(clean, ("оплата", "счет", "предоплата")),
        "risk_reasons": [
            reason
            for reason, has_signal in {
                "debt_risk": _contains_marker(clean, DEBT_MARKERS),
                "negative_margin_risk": _contains_marker(clean, NEGATIVE_MARGIN_MARKERS),
            }.items()
            if has_signal
        ],
        "next_client_step": "ждет КП" if "кп" in lower else "",
        "manager_promise": "mentioned" if _contains_marker(clean, PROMISE_MARKERS) else "",
        "next_touch_date": date_match.group(1) if date_match else "",
    }
    signal_count = sum(bool(value) for value in extracted.values() if not isinstance(value, list))
    confidence = min(0.95, 0.25 + signal_count * 0.06)
    return {"extracted": extracted, "confidence_score": round(confidence, 2), "extractor_version": "rules_v1"}


def extract_entities_from_text(text: str) -> dict[str, Any]:
    clean = _repair_text(text)
    lower = _canon_text(clean)

    company = _first_match(
        (
            r"(?:компания|клиент|заказчик)\s+([A-ZА-ЯЁ0-9\"' .\-]{3,80})",
            r"\b((?:ООО|АО|ПАО|ИП|ЗАО)\s+[A-ZА-ЯЁ0-9\"' .\-]{2,80})",
            r"(?:company|client|customer)\s+([A-ZA-Z0-9\"' .\-]{3,80})",
        ),
        clean,
    )
    contact = _first_match(
        (
            r"(?:контакт|лпр|прораб|инженер|менеджер)\s+([A-ZА-ЯЁ][A-Za-zА-Яа-яЁё\-]+(?:\s+[A-ZА-ЯЁ][A-Za-zА-Яа-яЁё\-]+){0,2})",
            r"(?:звонить|связаться)\s+([A-ZА-ЯЁ][A-Za-zА-Яа-яЁё\-]+(?:\s+[A-ZА-ЯЁ][A-Za-zА-Яа-яЁё\-]+){0,2})",
        ),
        clean,
    )
    project_object = _first_match(
        (
            r"(?:объект|стройка|площадка|жк|бц)\s+([^.;\n]{3,90})",
            r"(?:на объекте)\s+([^.;\n]{3,90})",
        ),
        clean,
    )
    address = _first_match(
        (
            r"(?:адрес|по адресу|ул\.?|улица|шоссе|проспект|район)\s+([^.;\n]{3,100})",
            r"\b((?:ул\.?|улица|шоссе|проспект|пр-т|район)\s+[^.;\n]{3,100})",
        ),
        clean,
    )

    equipment_type = ""
    for normalized, aliases in EQUIPMENT_ALIASES.items():
        if any(alias in lower for alias in aliases):
            equipment_type = normalized
            break

    model_match = re.search(
        r"\b(jcb\s*3cx|jcb|manitou|маниту|liebherr|xcmg|zoomlion|като|клинцы|ивановец|камаз)\b",
        lower,
    )
    window_match = re.search(
        r"\b(сегодня|завтра|послезавтра|с понедельника|с утра|к утру|на неделю|на месяц|\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?)\b",
        lower,
    )
    duration_match = re.search(
        r"\b(\d+\s*(?:дн|день|дня|дней|смен|смены|час|часа|часов|недел[яюи]|месяц|месяца|мес))\b",
        lower,
    )
    tech_params = sorted(
        set(
            re.findall(
                r"\b\d+(?:[,.]\d+)?\s*(?:тонн|т|м|метр|метра|метров|м3|куб|смен|час|часа|часов)\b",
                lower,
            )
        )
    )
    work_conditions = _first_match(
        (
            r"(?:условия|работа|грунт|плечо|высота|стрела|вылет)\s+([^.;\n]{3,90})",
        ),
        clean,
    )
    next_step = _first_match(
        (
            r"(?:следующий шаг|клиент просит|ждет|ожидает)\s+([^.;\n]{3,90})",
            r"(?:нужно|надо)\s+(позвонить|отправить кп|выставить счет|подготовить договор|уточнить[^.;\n]{0,70})",
        ),
        clean,
    )
    next_touch = _first_match(
        (
            r"(?:следующее касание|перезвонить|созвон)\s+([^.;\n]{3,60})",
        ),
        clean,
    )

    own_signal = "unknown"
    if _contains_marker(clean, SUBRENT_MARKERS) or any(word in lower for word in ("субаренда", "партнер", "нет своей")):
        own_signal = "subrent"
    elif _contains_marker(clean, OWN_EQUIPMENT_MARKERS) or any(word in lower for word in ("своя техника", "есть в парке", "наш парк")):
        own_signal = "own_equipment"

    risk_reasons = [
        reason
        for reason, has_signal in {
            "debt_risk": _contains_marker(clean, DEBT_MARKERS) or any(word in lower for word in ("дебитор", "долг", "просроч", "черный список")),
            "negative_margin_risk": _contains_marker(clean, NEGATIVE_MARGIN_MARKERS) or any(word in lower for word in ("ниже марж", "минус", "убыт", "дорогая субаренда")),
            "unclear_specs": not equipment_type and any(word in lower for word in ("техника", "машина", "нужно")),
        }.items()
        if has_signal
    ]

    extracted = {
        "client_company": company,
        "contact_person": contact,
        "project_object": project_object,
        "address_or_geo": address,
        "equipment_type": equipment_type,
        "equipment_model": model_match.group(1).upper() if model_match else "",
        "time_window": window_match.group(1) if window_match else "",
        "urgency": "high" if (_contains_marker(clean, URGENCY_MARKERS) or any(word in lower for word in ("срочно", "горит", "сегодня", "завтра"))) else "unknown",
        "rental_duration": duration_match.group(1) if duration_match else "",
        "work_conditions": work_conditions,
        "technical_parameters": tech_params or [marker for marker in SPEC_MARKERS if _canon_text(marker) in lower],
        "price_context": "mentioned" if any(word in lower for word in ("цена", "ставка", "руб", "₽", "дорого", "дешевле", "счет", "price", "rate")) else "",
        "competitor_mention": _contains_marker(clean, COMPETITOR_MARKERS) or any(word in lower for word in ("конкурент", "другие дали", "дешевле")),
        "own_or_subrent_signal": own_signal,
        "contract_readiness": any(word in lower for word in ("договор", "подписать", "готовы подписать")),
        "payment_readiness": any(word in lower for word in ("оплата", "счет", "предоплата", "готовы оплатить")),
        "risk_reasons": risk_reasons,
        "next_client_step": next_step or ("ждет КП" if "кп" in lower else ""),
        "manager_promise": "mentioned" if (_contains_marker(clean, PROMISE_MARKERS) or any(word in lower for word in ("обещал", "должен был", "просрочил"))) else "",
        "next_touch_date": next_touch or (window_match.group(1) if window_match else ""),
    }
    scalar_signals = sum(bool(value) for value in extracted.values() if not isinstance(value, list))
    list_signals = sum(1 for value in extracted.values() if isinstance(value, list) and value)
    confidence = min(0.96, 0.2 + scalar_signals * 0.055 + list_signals * 0.04)
    return {"extracted": extracted, "confidence_score": round(confidence, 2), "extractor_version": "rules_v2"}


def extract_event_entities(engine: Engine, event_id: int) -> dict[str, Any] | None:
    with Session(engine) as session:
        event = session.query(CommunicationEvent).filter_by(id=event_id).one_or_none()
        if event is None:
            return None
        result = extract_entities_from_text(event.text)
        extraction = CommunicationExtraction(
            communication_event_id=event.id,
            extracted_json=result["extracted"],
            confidence_score=result["confidence_score"],
            extractor_version=result["extractor_version"],
        )
        event.processing_status = "extracted"
        session.add(extraction)
        session.commit()
        session.refresh(extraction)
        return {
            "id": extraction.id,
            "communication_event_id": extraction.communication_event_id,
            **result,
        }


def create_entity_resolution(engine: Engine, entity_type: str, raw_value: str) -> dict[str, Any]:
    normalized = _normalize_name(raw_value)
    confidence = 0.92 if normalized and len(normalized) >= 4 else 0.35
    resolved_entity_id = f"{entity_type}:{normalized}" if normalized else ""
    with Session(engine) as session:
        record = EntityResolutionRecord(
            entity_type=entity_type,
            raw_value=raw_value,
            normalized_value=normalized,
            confidence_score=confidence,
            resolved_entity_id=resolved_entity_id,
            resolution_reason="Нормализация MVP: очистка юр. форм, пунктуации и регистра.",
        )
        session.add(record)
        session.commit()
        session.refresh(record)
        return {
            "id": record.id,
            "entity_type": record.entity_type,
            "raw_value": record.raw_value,
            "normalized_value": record.normalized_value,
            "confidence_score": record.confidence_score,
            "resolved_entity_id": record.resolved_entity_id,
        }


def create_entity_resolution(engine: Engine, entity_type: str, raw_value: str) -> dict[str, Any]:
    normalized = _normalize_equipment_type(raw_value) if entity_type == "equipment_type" else _normalize_name(raw_value)
    if entity_type in {"company", "client_company"}:
        normalized = re.sub(r"\b(строительная|компания|клиент|заказчик)\b", " ", normalized)
    elif entity_type in {"project_object", "object"}:
        normalized = re.sub(r"\b(объект|стройка|площадка)\b", " ", normalized)
    elif entity_type == "competitor":
        normalized = re.sub(r"\b(конкурент|дал|дали|предложил|предложили)\b", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()

    confidence = 0.35
    if normalized:
        confidence = 0.72
        if len(normalized) >= 4:
            confidence += 0.12
        if entity_type == "equipment_type" and normalized in EQUIPMENT_ALIASES:
            confidence += 0.1
        if raw_value != normalized:
            confidence += 0.04
    confidence = min(0.96, confidence)
    resolved_entity_id = f"{entity_type}:{normalized}" if normalized else ""

    with Session(engine) as session:
        record = EntityResolutionRecord(
            entity_type=entity_type,
            raw_value=_repair_text(raw_value),
            normalized_value=normalized,
            confidence_score=confidence,
            resolved_entity_id=resolved_entity_id,
            resolution_reason="MVP normalization v2: text repair, legal-form cleanup, type-specific aliases.",
        )
        session.add(record)
        session.commit()
        session.refresh(record)
        return {
            "id": record.id,
            "entity_type": record.entity_type,
            "raw_value": record.raw_value,
            "normalized_value": record.normalized_value,
            "confidence_score": record.confidence_score,
            "resolved_entity_id": record.resolved_entity_id,
        }


def build_opportunity_units(engine: Engine) -> dict[str, int]:
    inserted = 0
    updated = 0
    with Session(engine) as session:
        opportunities = session.query(Opportunity).order_by(Opportunity.id.asc()).all()
        for opportunity in opportunities:
            existing = session.query(OpportunityUnit).filter_by(bitrix_deal_id=opportunity.source_deal_id).one_or_none()
            context_text = " ".join([opportunity.title or "", opportunity.last_comment or "", opportunity.next_step or ""])
            extracted = extract_entities_from_text(context_text)["extracted"]
            risk_reasons = extracted.get("risk_reasons") or []
            commercial_scenario = extracted.get("own_or_subrent_signal") or "unknown"
            if commercial_scenario == "unknown" and _contains_marker(context_text, OWN_EQUIPMENT_MARKERS):
                commercial_scenario = "own_equipment"
            risk_level = "debt_block" if "debt_risk" in risk_reasons else "unknown"
            margin_estimate = -1.0 if "negative_margin_risk" in risk_reasons else 0.0
            required_present = {
                "client_name": bool(opportunity.company_id or extracted.get("client_company")),
                "object_name": bool(extracted.get("project_object")),
                "equipment_type": bool(extracted.get("equipment_type")),
                "need_window": bool(extracted.get("time_window")),
                "rental_duration": bool(extracted.get("rental_duration")),
                "next_step": bool(opportunity.next_step or extracted.get("next_client_step")),
                "owner_manager_id": bool((opportunity.raw_context if hasattr(opportunity, "raw_context") else "") or ""),
            }
            data_quality_score = round((sum(required_present.values()) / len(required_present)) * 100, 1)
            payload = {
                "client_name": opportunity.company_id or extracted.get("client_company") or "",
                "client_entity_id": f"company:{_normalize_name(opportunity.company_id or extracted.get('client_company'))}" if (opportunity.company_id or extracted.get("client_company")) else "",
                "contact_name": opportunity.contact_id or extracted.get("contact_person") or "",
                "contact_entity_id": f"person:{_normalize_name(opportunity.contact_id or extracted.get('contact_person'))}" if (opportunity.contact_id or extracted.get("contact_person")) else "",
                "object_name": extracted.get("project_object") or "",
                "object_entity_id": f"object:{_normalize_name(extracted.get('project_object'))}" if extracted.get("project_object") else "",
                "object_address": extracted.get("address_or_geo") or "",
                "equipment_type": extracted.get("equipment_type") or "",
                "equipment_model": extracted.get("equipment_model") or "",
                "need_window": extracted.get("time_window") or "",
                "rental_duration": extracted.get("rental_duration") or "",
                "commercial_scenario": commercial_scenario,
                "decision_access_status": "contact_known" if (opportunity.contact_id or extracted.get("contact_person")) else "unknown",
                "risk_level": risk_level,
                "economic_value": _parse_amount(opportunity.opportunity_value),
                "margin_estimate": margin_estimate,
                "next_step": opportunity.next_step or extracted.get("next_client_step") or "",
                "owner_manager_id": "",
                "data_quality_score": data_quality_score,
                "raw_context": {"source_opportunity_id": opportunity.id, "source_text": context_text, "extracted": extracted},
            }
            if existing is None:
                session.add(OpportunityUnit(bitrix_deal_id=opportunity.source_deal_id, **payload))
                inserted += 1
            else:
                for key, value in payload.items():
                    setattr(existing, key, value)
                updated += 1
        session.commit()
    return {"inserted": inserted, "updated": updated, "total": inserted + updated}


def compute_opportunity_unit_scores(engine: Engine) -> dict[str, int]:
    created = 0
    with Session(engine) as session:
        units = session.query(OpportunityUnit).order_by(OpportunityUnit.id.asc()).all()
        for unit in units:
            text = _context_text(unit)
            need = 80.0 if unit.equipment_type or unit.object_name else 35.0
            time_score = 90.0 if unit.need_window in {"сегодня", "завтра"} else _score_from_markers(text, URGENCY_MARKERS, 25.0)
            spec = 85.0 if unit.equipment_type and (unit.equipment_model or unit.rental_duration) else 45.0 if unit.equipment_type else 20.0
            access = 70.0 if unit.contact_entity_id else 30.0
            money = 80.0 if unit.economic_value > 0 or _contains_marker(text, MATURITY_MARKERS) else 35.0
            fit = 85.0 if unit.commercial_scenario == "own_equipment" else 45.0 if unit.commercial_scenario == "subrent" else 65.0 if unit.equipment_type else 35.0
            actionability = 85.0 if unit.next_step or unit.contact_entity_id else 45.0
            pclose = round((need + spec + access + money) / 400, 3)
            econ = min(100.0, max(10.0, unit.economic_value / 5000)) if unit.economic_value else 35.0
            urgency = time_score / 100
            strategy_weight = 1.25 if unit.commercial_scenario == "own_equipment" else 0.9 if unit.commercial_scenario == "subrent" else 1.0
            priority = pclose * econ * urgency * (fit / 100) * (actionability / 100) * strategy_weight
            priority_score = round(min(100.0, priority), 1)
            blocked_reason = ""
            if unit.risk_level == "debt_block":
                priority_score = min(priority_score, 25.0)
                blocked_reason = "Клиент заблокирован по дебиторке."
            if unit.margin_estimate < 0:
                priority_score = min(priority_score, 30.0)
                blocked_reason = "Отрицательная маржа."
            if unit.commercial_scenario == "subrent" and priority_score >= 70:
                priority_score = min(priority_score, 69.0)
                blocked_reason = blocked_reason or "Нужна проверка субаренды и маржи."
            session.add(
                OpportunityScore(
                    opportunity_unit_id=unit.id,
                    need_score=need,
                    time_score=time_score,
                    spec_score=spec,
                    access_score=access,
                    money_score=money,
                    fit_score=fit,
                    pclose=pclose,
                    econ_value=econ,
                    urgency=urgency,
                    actionability=actionability / 100,
                    strategy_weight=strategy_weight,
                    priority_score=priority_score,
                    blocked_reason=blocked_reason,
                )
            )
            created += 1
        session.commit()
    return {"created": created}


def compute_opportunity_unit_states(engine: Engine) -> dict[str, int]:
    created = 0
    with Session(engine) as session:
        units = session.query(OpportunityUnit).order_by(OpportunityUnit.id.asc()).all()
        for unit in units:
            session.query(OpportunityState).filter_by(opportunity_unit_id=unit.id, active=True).update({"active": False})
            score = _latest_score(session, unit.id)
            if score is None:
                continue
            states: list[tuple[str, str, float, str]] = []
            if score.need_score >= 70 and score.time_score >= 70 and score.money_score >= 65:
                states.append(("hot_urgent", "Горячая и срочная", 0.9, "Высокие Need/Time/Money индексы."))
            if score.need_score >= 70 and score.fit_score >= 70:
                states.append(("hot_own_equipment_fit", "Горячая, своя техника подходит", 0.82, "Высокие Need и Fit."))
            if score.need_score >= 70 and score.fit_score < 50:
                states.append(("hot_subrent_only", "Горячая, только субаренда", 0.78, "Потребность высокая, fit под свой парк низкий."))
            if score.need_score < 45 and score.spec_score < 45 and score.time_score < 50:
                states.append(("noisy_low_priority", "Сделка шумовая", 0.75, "Низкие Need/Spec/Time индексы."))
            if score.spec_score < 50:
                states.append(("unclear_technical_spec", "Техспецификация неясна", 0.8, "Недостаточно техники, модели или условий работы."))
            if not unit.next_step:
                states.append(("hot_unprocessed" if score.need_score >= 70 else "missing_next_step", "Нет следующего шага", 0.86, "В Opportunity Unit не заполнен следующий шаг."))
            if score.blocked_reason:
                states.append(("economic_or_debt_block", "Экономический или дебиторский блок", 0.92, score.blocked_reason))
            text = _context_text(unit)
            if unit.commercial_scenario == "subrent" and score.need_score >= 70:
                states.append(("subrent_margin_review", "Subrent margin review", 0.82, "Subrent deal requires margin and partner availability check."))
            if _contains_marker(text, COMPETITOR_MARKERS):
                states.append(("competitor_on_object", "Competitor on object", 0.78, "Communication contains competitor signal."))
            if _contains_marker(text, CROSS_SELL_MARKERS) and unit.object_entity_id:
                states.append(("cross_sell_open", "Cross-sell opportunity", 0.72, "Object context suggests adjacent equipment demand."))
            if _contains_marker(text, PROMISE_MARKERS) and not unit.next_step:
                states.append(("manager_promise_overdue", "Manager promise overdue", 0.8, "There is a promise signal but no next step is recorded."))
            for code, name, confidence, reason in states:
                session.add(
                    OpportunityState(
                        opportunity_unit_id=unit.id,
                        state_code=code,
                        state_name=name,
                        confidence_score=confidence,
                        reason=reason,
                        evidence_json=serialize_score(score) or {},
                        active=True,
                    )
                )
                created += 1
        session.commit()
    return {"created": created}


def build_decision_recommendations(engine: Engine) -> dict[str, int]:
    seed_action_templates(engine)
    created = 0
    with Session(engine) as session:
        units = session.query(OpportunityUnit).order_by(OpportunityUnit.id.asc()).all()
        templates = {item.action_code: item for item in session.query(ActionTemplate).filter_by(active=True).all()}
        state_to_action = {
            "hot_urgent": "call_15_min",
            "hot_own_equipment_fit": "reserve_own_equipment",
            "hot_subrent_only": "request_subrent",
            "hot_unprocessed": "follow_up_reminder",
            "noisy_low_priority": "stop_deal",
            "unclear_technical_spec": "clarify_specs",
            "economic_or_debt_block": "owner_escalation",
            "missing_next_step": "follow_up_reminder",
            "subrent_margin_review": "reprice_deal",
            "competitor_on_object": "competitor_attack",
            "cross_sell_open": "cross_sell_offer",
            "manager_promise_overdue": "follow_up_reminder",
        }
        for unit in units:
            states = _active_states(session, unit.id)
            score = _latest_score(session, unit.id)
            for state in states:
                action_code = state_to_action.get(state.state_code)
                template = templates.get(action_code or "")
                if template is None:
                    continue
                existing = (
                    session.query(DecisionRecommendation)
                    .filter_by(opportunity_unit_id=unit.id, action_code=template.action_code, status="open")
                    .one_or_none()
                )
                explainability = {
                    "why_important": f"Состояние: {state.state_name}. {state.reason}",
                    "triggered_signals": state.evidence_json,
                    "similar_cases": [],
                    "recommended_action_reason": template.description,
                    "risk_if_ignored": "Сделка может потерять импульс, нарушить SLA или уйти к конкуренту.",
                    "s0_alignment": {
                        "own_equipment_priority": unit.commercial_scenario == "own_equipment",
                        "margin_control": unit.margin_estimate >= 0,
                        "debt_control": unit.risk_level != "debt_block",
                        "noise_reduction": state.state_code != "noisy_low_priority",
                    },
                }
                if existing is None:
                    session.add(
                        DecisionRecommendation(
                            opportunity_unit_id=unit.id,
                            action_template_id=template.id,
                            action_code=template.action_code,
                            target_role=template.target_role,
                            owner_id=unit.owner_manager_id,
                            deadline_at=_deadline(template.deadline_sla_minutes),
                            requires_escalation=template.requires_approval or state.state_code in {"hot_unprocessed", "economic_or_debt_block", "subrent_margin_review", "competitor_on_object", "manager_promise_overdue"},
                            escalation_role="rop" if template.target_role != "owner" else "owner",
                            status="open",
                            explainability_json=explainability,
                        )
                    )
                    created += 1
                elif score is not None:
                    existing.explainability_json = explainability
        session.commit()
    return {"created": created}


def run_decision_pipeline(engine: Engine) -> dict[str, Any]:
    return {
        "action_templates": seed_action_templates(engine),
        "opportunity_units": build_opportunity_units(engine),
        "scores": compute_opportunity_unit_scores(engine),
        "states": compute_opportunity_unit_states(engine),
        "recommendations": build_decision_recommendations(engine),
    }


def get_opportunity_state(engine: Engine, opportunity_unit_id: int) -> dict[str, Any] | None:
    with Session(engine) as session:
        unit = session.query(OpportunityUnit).filter_by(id=opportunity_unit_id).one_or_none()
        if unit is None:
            return None
        return {
            "opportunity_unit": serialize_opportunity_unit(unit),
            "scores": serialize_score(_latest_score(session, unit.id)),
            "states": [serialize_state(item) for item in _active_states(session, unit.id)],
        }


def get_opportunity_decision(engine: Engine, opportunity_unit_id: int) -> dict[str, Any] | None:
    with Session(engine) as session:
        unit = session.query(OpportunityUnit).filter_by(id=opportunity_unit_id).one_or_none()
        if unit is None:
            return None
        recommendations = (
            session.query(DecisionRecommendation)
            .filter_by(opportunity_unit_id=unit.id)
            .order_by(DecisionRecommendation.id.desc())
            .limit(20)
            .all()
        )
        return {
            "opportunity_unit": serialize_opportunity_unit(unit),
            "recommendations": [serialize_recommendation(item) for item in recommendations],
        }


def get_object_graph(engine: Engine, object_id: str) -> dict[str, Any]:
    with Session(engine) as session:
        units = (
            session.query(OpportunityUnit)
            .filter((OpportunityUnit.object_entity_id == object_id) | (OpportunityUnit.object_name == object_id))
            .limit(50)
            .all()
        )
        nodes: dict[str, dict[str, Any]] = {}
        edges: list[dict[str, str]] = []
        for unit in units:
            opp_id = f"opportunity:{unit.id}"
            nodes[opp_id] = {"id": opp_id, "type": "Opportunity", "label": unit.bitrix_deal_id}
            if unit.client_entity_id:
                nodes[unit.client_entity_id] = {"id": unit.client_entity_id, "type": "Company", "label": unit.client_name}
                edges.append({"source": opp_id, "target": unit.client_entity_id, "type": "FOR_COMPANY"})
            if unit.object_entity_id:
                nodes[unit.object_entity_id] = {"id": unit.object_entity_id, "type": "ProjectObject", "label": unit.object_name}
                edges.append({"source": opp_id, "target": unit.object_entity_id, "type": "FOR_OBJECT"})
            if unit.equipment_type:
                equipment_id = f"equipment_type:{_normalize_name(unit.equipment_type)}"
                nodes[equipment_id] = {"id": equipment_id, "type": "EquipmentType", "label": unit.equipment_type}
                edges.append({"source": opp_id, "target": equipment_id, "type": "NEEDS"})
        return {"nodes": list(nodes.values()), "edges": edges}


def get_role_dashboard(engine: Engine, role: str) -> dict[str, Any]:
    with Session(engine) as session:
        query = session.query(DecisionRecommendation).order_by(DecisionRecommendation.id.desc()).limit(200)
        recommendations = query.all()
        if role == "manager":
            filtered = [item for item in recommendations if item.target_role == "manager"]
        elif role == "rop":
            filtered = [item for item in recommendations if item.requires_escalation or item.target_role == "rop"]
        elif role == "logistics":
            filtered = [item for item in recommendations if item.target_role == "logistics"]
        elif role == "owner":
            filtered = [item for item in recommendations if item.requires_escalation or item.target_role == "owner"]
        else:
            filtered = recommendations
        return {
            "role": role,
            "count": len(filtered),
            "items": [serialize_recommendation(item) for item in filtered[:50]],
        }


def create_recommendation_feedback(engine: Engine, recommendation_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    with Session(engine) as session:
        recommendation = session.query(DecisionRecommendation).filter_by(id=recommendation_id).one_or_none()
        if recommendation is None:
            return None
        feedback = RecommendationFeedback(
            recommendation_id=recommendation_id,
            shown_to_user_id=_as_text(payload.get("shown_to_user_id")),
            shown_to_role=_as_text(payload.get("shown_to_role")),
            was_shown=bool(payload.get("was_shown", True)),
            decision=_as_text(payload.get("decision")),
            rejection_reason=_as_text(payload.get("rejection_reason")),
            was_executed=bool(payload.get("was_executed", False)),
            deal_outcome=_as_text(payload.get("deal_outcome")),
            effect_1d=_as_text(payload.get("effect_1d")),
            effect_3d=_as_text(payload.get("effect_3d")),
            effect_7d=_as_text(payload.get("effect_7d")),
            effect_30d=_as_text(payload.get("effect_30d")),
        )
        if feedback.was_executed:
            recommendation.status = "done"
        elif feedback.decision in {"accepted", "rejected", "postponed"}:
            recommendation.status = feedback.decision
        session.add(feedback)
        session.commit()
        session.refresh(feedback)
        return {
            "id": feedback.id,
            "recommendation_id": feedback.recommendation_id,
            "decision": feedback.decision,
            "was_executed": feedback.was_executed,
            "deal_outcome": feedback.deal_outcome,
        }


def vector_collection_contracts() -> list[dict[str, Any]]:
    collections = (
        "deal_events_vectors",
        "won_deals_vectors",
        "lost_deals_vectors",
        "competitor_mentions_vectors",
        "contact_person_vectors",
        "object_history_vectors",
    )
    return [
        {
            "collection": name,
            "required_payload": [
                "opportunity_unit_id",
                "bitrix_deal_id",
                "equipment_type",
                "owner_manager_id",
                "stage_id",
                "event_type",
                "created_at",
            ],
        }
        for name in collections
    ]

from __future__ import annotations

from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from pydantic import BaseModel
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

from app.config import load_settings
from app.db import (
    build_dashboard_analytics,
    build_engine,
    build_actions_from_opportunities,
    build_explainability_from_actions,
    build_opportunities_from_raw_deals,
    check_database,
    create_action_feedback,
    create_tables,
    list_action_feedback,
    list_explainability,
    list_opportunity_actions,
    list_opportunities,
    list_raw_bitrix_deals,
    list_raw_bitrix_deals_for_analytics,
    recompute_opportunity_priority_scores,
    recompute_opportunity_states,
    update_action_status,
    update_opportunity_next_step,
    upsert_raw_bitrix_deals,
)
from app.qdrant_service import build_qdrant_client
from sync_bitrix_to_qdrant import fetch_bitrix_entities

settings = load_settings()
app = FastAPI(title=settings.app_name)
engine = build_engine(settings)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


class FeedbackCreateRequest(BaseModel):
    action_id: int
    shown_to_role: str = "manager"
    decision: str
    rejection_reason: str = ""
    executed: str = "no"
    outcome_note: str = ""


class OpportunityNextStepUpdateRequest(BaseModel):
    next_step: str


@app.on_event("startup")
def ensure_database_schema() -> None:
    create_tables(engine)


def _safe_int(value: object) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0


def _priority_band(score: int) -> str:
    if score >= 80:
        return "high"
    if score >= 50:
        return "medium"
    return "low"


def build_dashboard_view_model(filters: dict[str, str]) -> dict[str, object]:
    raw_deals = list_raw_bitrix_deals_for_analytics(engine)
    opportunities = list_opportunities(engine, limit=200)
    actions = list_opportunity_actions(engine, limit=200)
    explainability_items = list_explainability(engine, limit=200)
    feedback_items = list_action_feedback(engine, limit=200)

    actions_by_opportunity: dict[int, list[dict[str, object]]] = {}
    for action in actions:
        actions_by_opportunity.setdefault(int(action["opportunity_id"]), []).append(action)

    explainability_by_action = {
        int(item["action_id"]): item
        for item in explainability_items
    }

    feedback_count_by_action: dict[int, int] = {}
    for item in feedback_items:
        action_id = int(item["action_id"])
        feedback_count_by_action[action_id] = feedback_count_by_action.get(action_id, 0) + 1

    rows: list[dict[str, object]] = []
    for opportunity in opportunities:
        opportunity_actions = actions_by_opportunity.get(int(opportunity["id"]), [])
        enriched_actions = []
        for action in opportunity_actions:
            enriched_actions.append(
                {
                    **action,
                    "explainability": explainability_by_action.get(int(action["id"])),
                    "feedback_count": feedback_count_by_action.get(int(action["id"]), 0),
                }
            )

        rows.append(
            {
                **opportunity,
                "priority_score_value": _safe_int(opportunity.get("priority_score")),
                "priority_band": _priority_band(_safe_int(opportunity.get("priority_score"))),
                "actions": enriched_actions,
            }
        )

    search = filters.get("search", "").strip().lower()
    state_filter = filters.get("state", "").strip().lower()
    stage_filter = filters.get("stage", "").strip().lower()
    priority_filter = filters.get("priority", "").strip().lower()
    next_step_filter = filters.get("next_step", "").strip().lower()

    filtered_rows = []
    for row in rows:
        title = str(row.get("title") or "").lower()
        source_deal_id = str(row.get("source_deal_id") or "").lower()
        company_id = str(row.get("company_id") or "").lower()
        last_comment = str(row.get("last_comment") or "").lower()

        if search and not any(search in value for value in (title, source_deal_id, company_id, last_comment)):
            continue
        if state_filter and str(row.get("state_code") or "").lower() != state_filter:
            continue
        if stage_filter and str(row.get("stage_id") or "").lower() != stage_filter:
            continue
        if priority_filter and str(row.get("priority_band") or "").lower() != priority_filter:
            continue
        if next_step_filter == "missing" and str(row.get("next_step") or "").strip():
            continue
        if next_step_filter == "present" and not str(row.get("next_step") or "").strip():
            continue

        filtered_rows.append(row)

    rows = filtered_rows
    rows.sort(
        key=lambda item: (
            -int(item["priority_score_value"]),
            str(item.get("title") or "").lower(),
        )
    )

    state_counts: dict[str, int] = {}
    for row in rows:
        state = str(row.get("state_code") or "unknown")
        state_counts[state] = state_counts.get(state, 0) + 1

    available_states = sorted({str(item.get("state_code") or "") for item in opportunities if item.get("state_code")})
    available_stages = sorted({str(item.get("stage_id") or "") for item in opportunities if item.get("stage_id")})

    return {
        "opportunities": rows,
        "metrics": {
            "raw_deals": len(raw_deals),
            "opportunities": len(rows),
            "actions": len(actions),
            "explainability": len(explainability_items),
            "feedback": len(feedback_items),
        },
        "state_counts": state_counts,
        "analytics": build_dashboard_analytics(raw_deals, opportunities, actions, feedback_items),
        "available_states": available_states,
        "available_stages": available_stages,
        "filters": filters,
    }


def build_interface_feature_overview() -> list[dict[str, str]]:
    return [
        {
            "title": "Мониторинг сделок",
            "description": "Просматривайте карточки сделок, текущие этапы, приоритеты, суммы, комментарии и план следующих шагов в одном окне.",
        },
        {
            "title": "Аналитика воронки продаж",
            "description": "Отслеживайте количество сделок по этапам, конверсию от начального объема, конверсию от предыдущего этапа и итоговую успешность.",
        },
        {
            "title": "Анализ потерь",
            "description": "Разбирайте проваленные сделки по этапам и по взвешенным причинам отказа, чтобы понимать, где и почему теряются сделки.",
        },
        {
            "title": "Рабочий контур тестирования",
            "description": "Обновляйте данные Bitrix, пересчитывайте аналитику, отправляйте обратную связь и проверяйте поведение интерфейса прямо из дашборда.",
        },
    ]


def build_ui_label_maps() -> dict[str, dict[str, str]]:
    return {
        "priority": {
            "high": "высокий",
            "medium": "средний",
            "low": "низкий",
        },
        "action_status": {
            "open": "открыто",
            "accepted": "принято",
            "postponed": "отложено",
            "done": "выполнено",
            "rejected": "отклонено",
        },
        "decision": {
            "accepted": "принято",
            "rejected": "отклонено",
            "postponed": "отложено",
        },
        "executed": {
            "yes": "да",
            "no": "нет",
        },
        "state": {
            "new": "новая",
            "active": "в работе",
            "won": "успешная",
            "lost": "провалена",
            "stalled": "зависла",
            "unknown": "неизвестно",
        },
    }


@app.get("/")
def root() -> dict[str, str]:
    return {
        "name": settings.app_name,
        "env": settings.app_env,
        "status": "ok",
    }


@app.get("/dashboard")
def dashboard(request: Request):
    filters = {
        "search": str(request.query_params.get("search", "")),
        "state": str(request.query_params.get("state", "")),
        "stage": str(request.query_params.get("stage", "")),
        "priority": str(request.query_params.get("priority", "")),
        "next_step": str(request.query_params.get("next_step", "")),
    }
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "app_name": settings.app_name,
            "app_env": settings.app_env,
            "view": build_dashboard_view_model(filters),
            "feature_overview": build_interface_feature_overview(),
            "ui_labels": build_ui_label_maps(),
            "testing_access_url": str(request.url_for("dashboard")),
            "health_url": str(request.url_for("health")),
            "analytics_url": str(request.url_for("get_analytics_summary")),
        },
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "service": settings.app_name,
    }


@app.get("/health/db")
def health_db() -> dict[str, object]:
    db_info = check_database(engine)
    return {
        "status": "ok",
        "driver": "postgresql",
        **db_info,
    }


@app.post("/setup/db")
def setup_db() -> dict[str, object]:
    tables = create_tables(engine)
    return {
        "status": "ok",
        "tables": tables,
    }


@app.get("/raw/deals")
def get_raw_deals(limit: int = 20) -> dict[str, object]:
    deals = list_raw_bitrix_deals(engine, limit=limit)
    return {
        "status": "ok",
        "count": len(deals),
        "items": deals,
    }


@app.post("/ingest/bitrix/deals")
def ingest_bitrix_deals() -> dict[str, object]:
    deal_fields = [
        "*",
        "UF_*",
    ]
    records = fetch_bitrix_entities(settings, "crm.deal.list", deal_fields)
    result = upsert_raw_bitrix_deals(engine, records)

    return {
        "status": "ok",
        "fetched": len(records),
        **result,
    }


@app.post("/build/opportunities")
def build_opportunities() -> dict[str, object]:
    result = build_opportunities_from_raw_deals(engine)
    return {
        "status": "ok",
        **result,
    }


@app.get("/opportunities")
def get_opportunities(limit: int = 20) -> dict[str, object]:
    items = list_opportunities(engine, limit=limit)
    return {
        "status": "ok",
        "count": len(items),
        "items": items,
    }


@app.post("/compute/opportunity-states")
def compute_opportunity_states() -> dict[str, object]:
    result = recompute_opportunity_states(engine)
    return {
        "status": "ok",
        **result,
    }


@app.post("/build/actions")
def build_actions() -> dict[str, object]:
    result = build_actions_from_opportunities(engine)
    return {
        "status": "ok",
        **result,
    }


@app.get("/actions")
def get_actions(limit: int = 20) -> dict[str, object]:
    items = list_opportunity_actions(engine, limit=limit)
    return {
        "status": "ok",
        "count": len(items),
        "items": items,
    }


@app.post("/build/explainability")
def build_explainability() -> dict[str, object]:
    result = build_explainability_from_actions(engine)
    return {
        "status": "ok",
        **result,
    }


@app.get("/explainability")
def get_explainability(limit: int = 20) -> dict[str, object]:
    items = list_explainability(engine, limit=limit)
    return {
        "status": "ok",
        "count": len(items),
        "items": items,
    }


@app.get("/analytics/summary")
def get_analytics_summary() -> dict[str, object]:
    raw_deals = list_raw_bitrix_deals_for_analytics(engine)
    opportunities = list_opportunities(engine, limit=500)
    actions = list_opportunity_actions(engine, limit=500)
    feedback_items = list_action_feedback(engine, limit=500)
    return {
        "status": "ok",
        "item": build_dashboard_analytics(raw_deals, opportunities, actions, feedback_items),
    }


@app.post("/feedback/actions")
def create_feedback(payload: FeedbackCreateRequest) -> dict[str, object]:
    item = create_action_feedback(
        engine=engine,
        action_id=payload.action_id,
        shown_to_role=payload.shown_to_role,
        decision=payload.decision,
        rejection_reason=payload.rejection_reason,
        executed=payload.executed,
        outcome_note=payload.outcome_note,
    )
    return {
        "status": "ok",
        "item": item,
    }


@app.get("/feedback/actions")
def get_feedback(limit: int = 20) -> dict[str, object]:
    items = list_action_feedback(engine, limit=limit)
    return {
        "status": "ok",
        "count": len(items),
        "items": items,
    }


@app.patch("/opportunities/{opportunity_id}/next-step")
def patch_opportunity_next_step(
    opportunity_id: int,
    payload: OpportunityNextStepUpdateRequest,
) -> dict[str, object]:
    item = update_opportunity_next_step(
        engine=engine,
        opportunity_id=opportunity_id,
        next_step=payload.next_step,
    )
    if item is None:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    return {
        "status": "ok",
        "item": item,
    }


@app.post("/compute/opportunity-priority")
def compute_opportunity_priority() -> dict[str, object]:
    result = recompute_opportunity_priority_scores(engine)
    return {
        "status": "ok",
        **result,
    }


@app.get("/health/qdrant")
def health_qdrant() -> dict[str, object]:
    client = build_qdrant_client(settings)
    collections = client.get_collections().collections
    names = sorted(collection.name for collection in collections)

    return {
        "status": "ok",
        "qdrant_url": settings.qdrant_url,
        "collection_count": len(names),
        "collections": names,
        "target_collection": settings.qdrant_collection,
    }


@app.post("/dashboard/run-pipeline")
def run_dashboard_pipeline() -> RedirectResponse:
    create_tables(engine)
    result = ingest_bitrix_deals()
    build_opportunities()
    compute_opportunity_states()
    compute_opportunity_priority()
    build_actions()
    build_explainability()

    redirect = RedirectResponse(
        url=f"/dashboard?ingested={result['fetched']}",
        status_code=303,
    )
    return redirect


@app.post("/dashboard/opportunities/{opportunity_id}/next-step")
async def dashboard_update_opportunity_next_step(
    opportunity_id: int,
    request: Request,
) -> RedirectResponse:
    form = await request.form()
    next_step = str(form.get("next_step", "")).strip()
    item = update_opportunity_next_step(
        engine=engine,
        opportunity_id=opportunity_id,
        next_step=next_step,
    )
    if item is None:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    recompute_opportunity_states(engine)
    recompute_opportunity_priority_scores(engine)
    build_actions_from_opportunities(engine)
    build_explainability_from_actions(engine)

    return RedirectResponse(url="/dashboard", status_code=303)


@app.post("/dashboard/actions/{action_id}/feedback")
async def dashboard_create_feedback(
    action_id: int,
    request: Request,
) -> RedirectResponse:
    form = await request.form()
    create_action_feedback(
        engine=engine,
        action_id=action_id,
        shown_to_role=str(form.get("shown_to_role", "manager")),
        decision=str(form.get("decision", "accepted")),
        rejection_reason=str(form.get("rejection_reason", "")),
        executed=str(form.get("executed", "no")),
        outcome_note=str(form.get("outcome_note", "")),
    )
    return RedirectResponse(url="/dashboard", status_code=303)


@app.post("/dashboard/actions/{action_id}/status")
async def dashboard_update_action_status(
    action_id: int,
    request: Request,
) -> RedirectResponse:
    form = await request.form()
    status = str(form.get("status", "open")).strip() or "open"
    item = update_action_status(engine, action_id=action_id, status=status)
    if item is None:
        raise HTTPException(status_code=404, detail="Action not found")
    return RedirectResponse(url="/dashboard", status_code=303)

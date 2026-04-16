from __future__ import annotations

from fastapi import FastAPI
from fastapi import HTTPException
from pydantic import BaseModel

from app.config import load_settings
from app.db import (
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
    recompute_opportunity_priority_scores,
    recompute_opportunity_states,
    update_opportunity_next_step,
    upsert_raw_bitrix_deals,
)
from app.qdrant_service import build_qdrant_client
from sync_bitrix_to_qdrant import fetch_bitrix_entities

settings = load_settings()
app = FastAPI(title=settings.app_name)
engine = build_engine(settings)


class FeedbackCreateRequest(BaseModel):
    action_id: int
    shown_to_role: str = "manager"
    decision: str
    rejection_reason: str = ""
    executed: str = "no"
    outcome_note: str = ""


class OpportunityNextStepUpdateRequest(BaseModel):
    next_step: str


@app.get("/")
def root() -> dict[str, str]:
    return {
        "name": settings.app_name,
        "env": settings.app_env,
        "status": "ok",
    }


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
        "ID",
        "TITLE",
        "TYPE_ID",
        "STAGE_ID",
        "OPPORTUNITY",
        "CURRENCY_ID",
        "COMMENTS",
        "COMPANY_ID",
        "CONTACT_ID",
        "ASSIGNED_BY_ID",
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

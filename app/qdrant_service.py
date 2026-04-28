from __future__ import annotations

import hashlib
import math
import re
import uuid
from dataclasses import dataclass
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.http import models
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.config import Settings
from app.db import _as_text
from app.models import CommunicationEvent, OpportunityUnit, RawBitrixDeal


VECTOR_COLLECTIONS = (
    "deal_events_vectors",
    "won_deals_vectors",
    "lost_deals_vectors",
    "competitor_mentions_vectors",
    "contact_person_vectors",
    "object_history_vectors",
)
VECTOR_SIZE = 384


@dataclass(frozen=True)
class VectorDocument:
    collection: str
    source_type: str
    source_id: str
    document: str
    payload: dict[str, Any]


def build_qdrant_client(settings: Settings) -> QdrantClient:
    return QdrantClient(
        url=settings.qdrant_url,
        api_key=settings.qdrant_api_key,
        check_compatibility=False,
    )


def _point_id(collection: str, source_type: str, source_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"bts-dss:{collection}:{source_type}:{source_id}"))


def _line(label: str, value: object) -> str:
    text = _as_text(value)
    return f"{label}: {text}" if text else ""


def _join_lines(lines: list[str]) -> str:
    return "\n".join(line for line in lines if line)


def _deal_is_won(raw_payload: dict[str, Any], stage_id: str) -> bool:
    semantic = _as_text(raw_payload.get("STAGE_SEMANTIC_ID")).upper()
    won_flag = _as_text(raw_payload.get("WON")).upper()
    normalized_stage = stage_id.upper()
    return semantic == "S" or won_flag in {"Y", "1", "TRUE"} or any(token in normalized_stage for token in ("WON", "SUCCESS"))


def _deal_is_lost(raw_payload: dict[str, Any], stage_id: str) -> bool:
    semantic = _as_text(raw_payload.get("STAGE_SEMANTIC_ID")).upper()
    closed_flag = _as_text(raw_payload.get("CLOSED")).upper()
    normalized_stage = stage_id.upper()
    if semantic == "F":
        return True
    if closed_flag in {"Y", "1", "TRUE"} and not _deal_is_won(raw_payload, stage_id):
        return True
    return any(token in normalized_stage for token in ("LOSE", "FAIL", "CANCEL", "REJECT"))


def _deal_document(deal: RawBitrixDeal, event_type: str) -> VectorDocument:
    raw_payload = dict(deal.raw_payload or {})
    document = _join_lines(
        [
            _line("event_type", event_type),
            _line("bitrix_deal_id", deal.bitrix_id),
            _line("title", deal.title),
            _line("stage_id", deal.stage_id),
            _line("company_id", deal.company_id),
            _line("contact_id", deal.contact_id),
            _line("opportunity", deal.opportunity),
            _line("currency", deal.currency_id),
            _line("comments", deal.comments),
        ]
    )
    return VectorDocument(
        collection="deal_events_vectors",
        source_type="raw_bitrix_deal",
        source_id=deal.bitrix_id,
        document=document,
        payload={
            "opportunity_unit_id": "",
            "bitrix_deal_id": deal.bitrix_id,
            "equipment_type": "",
            "owner_manager_id": _as_text(raw_payload.get("ASSIGNED_BY_ID")),
            "stage_id": deal.stage_id,
            "event_type": event_type,
            "created_at": deal.created_at.isoformat() if deal.created_at else "",
            "entity_type": "deal",
            "source": "bitrix24",
            "title": deal.title,
            "document": document,
            "raw": raw_payload,
        },
    )


def _event_document(event: CommunicationEvent) -> VectorDocument:
    document = _join_lines(
        [
            _line("event_type", event.event_type),
            _line("channel", event.channel),
            _line("source_entity_type", event.source_entity_type),
            _line("source_entity_id", event.source_entity_id),
            _line("occurred_at", event.occurred_at),
            _line("text", event.text),
            _line("transcript_ref", event.transcript_ref),
        ]
    )
    return VectorDocument(
        collection="deal_events_vectors",
        source_type="communication_event",
        source_id=str(event.id),
        document=document,
        payload={
            "opportunity_unit_id": "",
            "bitrix_deal_id": event.source_entity_id,
            "equipment_type": "",
            "owner_manager_id": "",
            "stage_id": "",
            "event_type": event.event_type,
            "created_at": event.created_at.isoformat() if event.created_at else "",
            "entity_type": "communication_event",
            "source": event.source,
            "title": event.source_event_id,
            "document": document,
            "raw": event.raw_payload or {},
        },
    )


def _unit_payload(unit: OpportunityUnit, collection: str, event_type: str, document: str) -> dict[str, Any]:
    return {
        "opportunity_unit_id": unit.id,
        "bitrix_deal_id": unit.bitrix_deal_id,
        "equipment_type": unit.equipment_type,
        "owner_manager_id": unit.owner_manager_id,
        "stage_id": "",
        "event_type": event_type,
        "created_at": unit.created_at.isoformat() if unit.created_at else "",
        "entity_type": collection.replace("_vectors", ""),
        "source": "postgresql",
        "title": unit.object_name or unit.client_name or unit.bitrix_deal_id,
        "document": document,
        "raw": unit.raw_context or {},
    }


def _unit_document(unit: OpportunityUnit, collection: str, event_type: str) -> VectorDocument:
    document = _join_lines(
        [
            _line("event_type", event_type),
            _line("bitrix_deal_id", unit.bitrix_deal_id),
            _line("client", unit.client_name),
            _line("contact", unit.contact_name),
            _line("object", unit.object_name),
            _line("object_address", unit.object_address),
            _line("equipment_type", unit.equipment_type),
            _line("equipment_model", unit.equipment_model),
            _line("need_window", unit.need_window),
            _line("rental_duration", unit.rental_duration),
            _line("commercial_scenario", unit.commercial_scenario),
            _line("risk_level", unit.risk_level),
            _line("next_step", unit.next_step),
        ]
    )
    return VectorDocument(
        collection=collection,
        source_type="opportunity_unit",
        source_id=str(unit.id),
        document=document,
        payload=_unit_payload(unit, collection, event_type, document),
    )


def _build_vector_documents(engine: Engine) -> list[VectorDocument]:
    documents: list[VectorDocument] = []
    with Session(engine) as session:
        for event in session.query(CommunicationEvent).order_by(CommunicationEvent.id.asc()).all():
            documents.append(_event_document(event))
            lower_text = _as_text(event.text).lower()
            if any(marker in lower_text for marker in ("competitor", "конкурент", "дешевле", "у других")):
                competitor_doc = _event_document(event)
                documents.append(
                    VectorDocument(
                        collection="competitor_mentions_vectors",
                        source_type=competitor_doc.source_type,
                        source_id=competitor_doc.source_id,
                        document=competitor_doc.document,
                        payload={**competitor_doc.payload, "entity_type": "competitor_mention"},
                    )
                )

        for deal in session.query(RawBitrixDeal).order_by(RawBitrixDeal.id.asc()).all():
            documents.append(_deal_document(deal, "raw_deal"))
            raw_payload = dict(deal.raw_payload or {})
            if _deal_is_won(raw_payload, deal.stage_id):
                won_doc = _deal_document(deal, "won_deal")
                documents.append(VectorDocument("won_deals_vectors", won_doc.source_type, won_doc.source_id, won_doc.document, won_doc.payload))
            if _deal_is_lost(raw_payload, deal.stage_id):
                lost_doc = _deal_document(deal, "lost_deal")
                documents.append(VectorDocument("lost_deals_vectors", lost_doc.source_type, lost_doc.source_id, lost_doc.document, lost_doc.payload))

        for unit in session.query(OpportunityUnit).order_by(OpportunityUnit.id.asc()).all():
            if unit.contact_entity_id or unit.contact_name:
                documents.append(_unit_document(unit, "contact_person_vectors", "contact_context"))
            if unit.object_entity_id or unit.object_name:
                documents.append(_unit_document(unit, "object_history_vectors", "object_context"))

    return [document for document in documents if document.document.strip()]


def _ensure_collection(client: QdrantClient, collection_name: str, vector_size: int) -> None:
    existing = {collection.name for collection in client.get_collections().collections}
    if collection_name in existing:
        return
    client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(size=vector_size, distance=models.Distance.COSINE),
    )


def _count_collection(client: QdrantClient, collection_name: str) -> int:
    try:
        result = client.count(collection_name=collection_name, exact=True)
    except Exception:
        return 0
    return int(result.count)


def _embed_document(document: str, vector_size: int = VECTOR_SIZE) -> list[float]:
    vector = [0.0] * vector_size
    tokens = re.findall(r"[\wА-Яа-яЁё]+", document.lower())
    for token in tokens:
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
        bucket = int.from_bytes(digest[:4], "big") % vector_size
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        vector[bucket] += sign

    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        return vector
    return [round(value / norm, 6) for value in vector]


def _embed_documents(documents: list[str]) -> list[list[float]]:
    return [_embed_document(document) for document in documents]


def index_dss_vectors(engine: Engine, settings: Settings) -> dict[str, Any]:
    client = build_qdrant_client(settings)
    vector_size = VECTOR_SIZE

    for collection_name in VECTOR_COLLECTIONS:
        _ensure_collection(client, collection_name, vector_size)

    documents = _build_vector_documents(engine)
    grouped: dict[str, list[VectorDocument]] = {name: [] for name in VECTOR_COLLECTIONS}
    for document in documents:
        grouped.setdefault(document.collection, []).append(document)

    indexed: dict[str, int] = {}
    for collection_name, collection_documents in grouped.items():
        indexed[collection_name] = 0
        for start in range(0, len(collection_documents), 50):
            batch = collection_documents[start : start + 50]
            vectors = _embed_documents([item.document for item in batch])
            points = [
                models.PointStruct(
                    id=_point_id(item.collection, item.source_type, item.source_id),
                    vector=vector,
                    payload=item.payload,
                )
                for item, vector in zip(batch, vectors, strict=True)
            ]
            if points:
                client.upsert(collection_name=collection_name, points=points)
                indexed[collection_name] += len(points)

    return {
        "embedding_model": "local_hash_v1",
        "vector_size": vector_size,
        "indexed": indexed,
        "collection_counts": {
            collection_name: _count_collection(client, collection_name)
            for collection_name in VECTOR_COLLECTIONS
        },
        "collections": sorted(collection.name for collection in client.get_collections().collections),
    }

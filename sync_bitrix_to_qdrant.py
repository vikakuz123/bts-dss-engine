from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from fastembed import TextEmbedding
from qdrant_client import QdrantClient
from qdrant_client.http import models


@dataclass
class Settings:
    bitrix_webhook_base: str
    qdrant_url: str
    qdrant_api_key: str | None
    qdrant_collection: str
    embedding_model: str


def load_settings() -> Settings:
    load_dotenv()
    bitrix_webhook_base = os.getenv("BITRIX_WEBHOOK_BASE", "").rstrip("/")
    qdrant_url = os.getenv("QDRANT_URL", "").strip()

    missing = [
        name
        for name, value in {
            "BITRIX_WEBHOOK_BASE": bitrix_webhook_base,
            "QDRANT_URL": qdrant_url,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    parsed_qdrant_url = urlparse(qdrant_url)
    if parsed_qdrant_url.scheme not in {"http", "https"} or not parsed_qdrant_url.netloc:
        raise RuntimeError(
            "QDRANT_URL must be a full URL like "
            "'https://<cluster-id>.<region>.cloud.qdrant.io', not just a cluster id."
        )

    return Settings(
        bitrix_webhook_base=bitrix_webhook_base,
        qdrant_url=qdrant_url,
        qdrant_api_key=os.getenv("QDRANT_API_KEY", "").strip() or None,
        qdrant_collection=os.getenv("QDRANT_COLLECTION", "bitrix_crm").strip(),
        embedding_model=os.getenv(
            "EMBEDDING_MODEL",
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        ).strip(),
    )


def fetch_bitrix_entities(settings: Settings, method_name: str, select_fields: list[str]) -> list[dict]:
    records: list[dict] = []
    start = 0

    while True:
        response = requests.post(
            f"{settings.bitrix_webhook_base}/{method_name}.json",
            json={"select": select_fields, "start": start},
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()

        if "error" in payload:
            raise RuntimeError(f"Bitrix API error for {method_name}: {payload['error_description']}")

        batch = payload.get("result", [])
        records.extend(batch)

        next_start = payload.get("next")
        if next_start is None:
            break
        start = next_start

    return records


def normalize_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ", ".join(normalize_value(item) for item in value if item is not None)
    if isinstance(value, dict):
        return ", ".join(f"{key}: {normalize_value(val)}" for key, val in value.items() if val is not None)
    return str(value).strip()


def build_document(entity_type: str, record: dict) -> str:
    lines = [f"entity_type: {entity_type}"]

    preferred_fields = [
        "ID",
        "TITLE",
        "COMPANY_TITLE",
        "TYPE_ID",
        "STAGE_ID",
        "OPPORTUNITY",
        "CURRENCY_ID",
        "ASSIGNED_BY_ID",
        "CONTACT_ID",
        "COMMENTS",
        "CITY",
        "ADDRESS",
        "ADDRESS_CITY",
        "ADDRESS_REGION",
        "ADDRESS_PROVINCE",
        "ADDRESS_COUNTRY",
    ]

    used = set()
    for field in preferred_fields:
        value = normalize_value(record.get(field))
        if value:
            lines.append(f"{field.lower()}: {value}")
            used.add(field)

    for field, raw_value in sorted(record.items()):
        if field in used:
            continue
        value = normalize_value(raw_value)
        if value:
            lines.append(f"{field.lower()}: {value}")

    return "\n".join(lines)


def chunked(items: list[dict], size: int) -> Iterable[list[dict]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def ensure_collection(client: QdrantClient, collection_name: str, vector_size: int) -> None:
    collections = client.get_collections().collections
    existing_names = {collection.name for collection in collections}
    if collection_name in existing_names:
        return

    client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(size=vector_size, distance=models.Distance.COSINE),
    )


def make_point_id(entity_type: str, entity_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"bitrix:{entity_type}:{entity_id}"))


def count_points(client: QdrantClient, collection_name: str, entity_type: str) -> int:
    total = 0
    offset = None

    while True:
        points, offset = client.scroll(
            collection_name=collection_name,
            limit=100,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )
        total += sum(1 for point in points if (point.payload or {}).get("entity_type") == entity_type)
        if offset is None:
            break

    return total


def upload_entities(
    settings: Settings,
    client: QdrantClient,
    embedding_model: TextEmbedding,
    entity_type: str,
    records: list[dict],
) -> int:
    uploaded = 0

    for batch in chunked(records, size=50):
        documents = [build_document(entity_type, record) for record in batch]
        vectors = [vector.tolist() for vector in embedding_model.embed(documents)]

        if uploaded == 0 and vectors:
            ensure_collection(client, settings.qdrant_collection, len(vectors[0]))

        points = []
        for record, document, vector in zip(batch, documents, vectors, strict=True):
            entity_id = str(record.get("ID", "")).strip()
            if not entity_id:
                continue

            payload = {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "title": record.get("TITLE") or record.get("COMPANY_TITLE") or "",
                "document": document,
                "source": "bitrix24",
                "raw": record,
            }
            points.append(
                models.PointStruct(
                    id=make_point_id(entity_type, entity_id),
                    vector=vector,
                    payload=payload,
                )
            )

        if points:
            client.upsert(collection_name=settings.qdrant_collection, points=points)
            uploaded += len(points)

    return uploaded


def main() -> None:
    settings = load_settings()
    qdrant_client = QdrantClient(
        url=settings.qdrant_url,
        api_key=settings.qdrant_api_key,
        check_compatibility=False,
    )
    embedding_model = TextEmbedding(model_name=settings.embedding_model)

    company_fields = [
        "ID",
        "TITLE",
        "COMPANY_TYPE",
        "COMMENTS",
        "CITY",
        "ADDRESS",
        "ADDRESS_CITY",
        "ADDRESS_REGION",
        "ADDRESS_PROVINCE",
        "ADDRESS_COUNTRY",
        "ASSIGNED_BY_ID",
    ]
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

    companies = fetch_bitrix_entities(settings, "crm.company.list", company_fields)
    deals = fetch_bitrix_entities(settings, "crm.deal.list", deal_fields)

    print(f"Fetched {len(companies)} companies from Bitrix24.")
    print(f"Fetched {len(deals)} deals from Bitrix24.")

    uploaded_companies = upload_entities(settings, qdrant_client, embedding_model, "company", companies)
    uploaded_deals = upload_entities(settings, qdrant_client, embedding_model, "deal", deals)

    qdrant_companies = count_points(qdrant_client, settings.qdrant_collection, "company")
    qdrant_deals = count_points(qdrant_client, settings.qdrant_collection, "deal")

    print(f"Uploaded {uploaded_companies} companies and {uploaded_deals} deals to Qdrant collection '{settings.qdrant_collection}'.")
    print(f"Qdrant now contains {qdrant_companies} companies and {qdrant_deals} deals in '{settings.qdrant_collection}'.")


if __name__ == "__main__":
    main()

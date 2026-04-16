from __future__ import annotations

import argparse
import os
from urllib.parse import urlparse

from dotenv import load_dotenv
from fastembed import TextEmbedding
from qdrant_client import QdrantClient
from qdrant_client.http import models


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Search Bitrix24 data in Qdrant.")
    parser.add_argument("query", help="Text query for semantic search.")
    parser.add_argument("--limit", type=int, default=5, help="Number of results to return.")
    parser.add_argument(
        "--entity-type",
        choices=["company", "deal"],
        help="Optional filter for entity type.",
    )
    return parser.parse_args()


def load_settings() -> dict[str, str | None]:
    load_dotenv()

    qdrant_url = os.getenv("QDRANT_URL", "").strip()
    collection_name = os.getenv("QDRANT_COLLECTION", "bitrix_crm").strip()
    embedding_model = os.getenv(
        "EMBEDDING_MODEL",
        "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
    ).strip()

    missing = [name for name, value in {"QDRANT_URL": qdrant_url}.items() if not value]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    parsed_qdrant_url = urlparse(qdrant_url)
    if parsed_qdrant_url.scheme not in {"http", "https"} or not parsed_qdrant_url.netloc:
        raise RuntimeError(
            "QDRANT_URL must be a full URL like "
            "'https://<cluster-id>.<region>.cloud.qdrant.io', not just a cluster id."
        )

    return {
        "qdrant_url": qdrant_url,
        "qdrant_api_key": os.getenv("QDRANT_API_KEY", "").strip() or None,
        "qdrant_collection": collection_name,
        "embedding_model": embedding_model,
    }


def build_filter(entity_type: str | None) -> models.Filter | None:
    if not entity_type:
        return None

    return models.Filter(
        must=[
            models.FieldCondition(
                key="entity_type",
                match=models.MatchValue(value=entity_type),
            )
        ]
    )


def main() -> None:
    args = parse_args()
    settings = load_settings()

    client = QdrantClient(
        url=settings["qdrant_url"],
        api_key=settings["qdrant_api_key"],
        check_compatibility=False,
    )
    embedding_model = TextEmbedding(model_name=str(settings["embedding_model"]))

    query_vector = next(embedding_model.embed([args.query])).tolist()
    response = client.query_points(
        collection_name=str(settings["qdrant_collection"]),
        query=query_vector,
        query_filter=build_filter(args.entity_type),
        limit=args.limit,
        with_payload=True,
        with_vectors=False,
    )
    results = response.points

    if not results:
        print("No results found.")
        return

    for index, result in enumerate(results, start=1):
        payload = result.payload or {}
        print(f"{index}. [{payload.get('entity_type', 'unknown')}] {payload.get('title', '')}")
        print(f"   id: {payload.get('entity_id', '')}")
        print(f"   score: {result.score:.4f}")
        print(f"   document: {payload.get('document', '')[:300]}")
        print()


if __name__ == "__main__":
    main()

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    app_name: str
    app_env: str
    database_url: str
    bitrix_webhook_base: str
    qdrant_url: str
    qdrant_api_key: str | None
    qdrant_collection: str


def _validate_qdrant_url(qdrant_url: str) -> None:
    parsed = urlparse(qdrant_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise RuntimeError(
            "QDRANT_URL must be a full URL like "
            "'https://<cluster-id>.<region>.cloud.qdrant.io'."
        )


def load_settings() -> Settings:
    load_dotenv()

    database_url = os.getenv("DATABASE_URL", "").strip()
    bitrix_webhook_base = os.getenv("BITRIX_WEBHOOK_BASE", "").rstrip("/")
    qdrant_url = os.getenv("QDRANT_URL", "").strip()
    missing = [
        name
        for name, value in {
            "DATABASE_URL": database_url,
            "BITRIX_WEBHOOK_BASE": bitrix_webhook_base,
            "QDRANT_URL": qdrant_url,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    _validate_qdrant_url(qdrant_url)

    return Settings(
        app_name=os.getenv("APP_NAME", "BTS DSS API").strip() or "BTS DSS API",
        app_env=os.getenv("APP_ENV", "development").strip() or "development",
        database_url=database_url,
        bitrix_webhook_base=bitrix_webhook_base,
        qdrant_url=qdrant_url,
        qdrant_api_key=os.getenv("QDRANT_API_KEY", "").strip() or None,
        qdrant_collection=os.getenv("QDRANT_COLLECTION", "bitrix_crm_v2").strip(),
    )

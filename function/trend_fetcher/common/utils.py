from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from ulid import ULID


def _quote_terms(terms: list[str]) -> str:
    cleaned = [term.strip() for term in terms if term and term.strip()]
    if not cleaned:
        raise ValueError("検索語が空です")
    return " OR ".join(f'"{term}"' for term in cleaned)


def build_query(
    risk_keywords: list[str],
    site_keywords: list[str],
    exclusion_keywords: list[str] | None = None,
) -> str:
    risk_part = f"({_quote_terms(risk_keywords)})"
    site_part = f"({_quote_terms(site_keywords)})" if site_keywords else ""
    base = f"{risk_part} {site_part}".strip()

    exclusions = ""
    if exclusion_keywords:
        excluded = [term.strip() for term in exclusion_keywords if term and term.strip()]
        if excluded:
            exclusions = " " + " ".join(f'-"{term}"' for term in excluded)

    return f"{base} lang:ja -is:retweet{exclusions}".strip()


def generate_s3_key(now: datetime) -> str:
    return f"raw/{now:%Y-%m-%d}/{ULID()}.json"


def serialize_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str, indent=2)

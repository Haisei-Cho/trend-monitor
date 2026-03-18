import json
from datetime import datetime, timezone
from typing import Any

from ulid import ULID


def generate_ulid() -> str:
    """ULIDを生成する。"""
    return str(ULID())


def generate_s3_key(date: datetime | None = None, ulid: str | None = None) -> str:
    """S3キーを日付パーティション形式で生成する。

    形式: raw/{YYYY-MM-DD}/{ULID}.json
    """
    if date is None:
        date = datetime.now(timezone.utc)
    if ulid is None:
        ulid = generate_ulid()
    date_str = date.strftime("%Y-%m-%d")
    return f"raw/{date_str}/{ulid}.json"


def generate_classified_s3_key(
    category_id: str,
    date: datetime | None = None,
    ulid: str | None = None,
) -> str:
    """分類結果用S3キーを生成する。

    形式: classified/{category_id}/{YYYY-MM-DD}/{ULID}.json
    """
    if date is None:
        date = datetime.now(timezone.utc)
    if ulid is None:
        ulid = generate_ulid()
    date_str = date.strftime("%Y-%m-%d")
    return f"classified/{category_id}/{date_str}/{ulid}.json"


def serialize_json(data: Any, ensure_ascii: bool = False) -> str:
    """PythonオブジェクトをJSON文字列に直列化する。"""
    return json.dumps(data, ensure_ascii=ensure_ascii)


X_API_QUERY_MAX_LENGTH = 512  # Basic/Pro tier: 512文字制限


def build_search_suffix(exclude_keywords: list[str] | None = None) -> str:
    """検索クエリの共通サフィックス（lang:ja + 除外ルール）を構築する。"""
    parts = ["lang:ja"]
    if exclude_keywords:
        parts.append("-(" + " OR ".join(exclude_keywords) + ")")
    return " ".join(parts)


def build_official_queries(usernames: list[str]) -> list[str]:
    """
    公式アカウント監視用 from: クエリを構築する。
    512文字を超える場合はアカウントを分割して複数クエリを返す。

    Returns:
        list[str]: 512文字以内のクエリリスト（形式: (from:A OR from:B ...) lang:ja -is:retweet）

    Raises:
        ValueError: usernamesが空の場合
    """
    if not usernames:
        raise ValueError("アカウントリストは少なくとも1つ必要です")

    suffix = "lang:ja -is:retweet"
    items = [f"from:{u}" for u in usernames]

    single = "(" + " OR ".join(items) + ") " + suffix
    if len(single) <= X_API_QUERY_MAX_LENGTH:
        return [single]

    # 分割: "(" + content + ") " + suffix
    overhead = 1 + 2 + len(suffix)  # "(" + ") " = 3文字
    max_content = X_API_QUERY_MAX_LENGTH - overhead

    chunks: list[list[str]] = []
    chunk: list[str] = []
    length = 0

    for item in items:
        added = len(item) if not chunk else len(item) + 4  # " OR " = 4文字
        if length + added > max_content and chunk:
            chunks.append(chunk)
            chunk = [item]
            length = len(item)
        else:
            chunk.append(item)
            length += added

    if chunk:
        chunks.append(chunk)

    return ["(" + " OR ".join(c) + ") " + suffix for c in chunks]


def build_query(
    risk_keywords: list[str],
    site_keywords: list[str],
    exclude_rules: list[str] | None = None,
) -> list[str]:
    """
    X API Search Recent用のクエリ文字列を構築する。
    512文字（Basic/Pro tier制限）を超える場合は拠点KWを分割して複数クエリを返す。

    Returns:
        list[str]: 512文字以内のクエリリスト

    Raises:
        ValueError: リスクキーワードまたは拠点キーワードが空の場合
    """
    if not risk_keywords:
        raise ValueError("リスクキーワードは少なくとも1つ必要です")
    if not site_keywords:
        raise ValueError("拠点キーワードは少なくとも1つ必要です")

    risk_part = "(" + " OR ".join(risk_keywords) + ")"
    suffix = build_search_suffix(exclude_rules)

    # 512文字以内なら分割不要
    site_part = "(" + " OR ".join(site_keywords) + ")"
    single = f"{risk_part} {site_part} {suffix}"
    if len(single) <= X_API_QUERY_MAX_LENGTH:
        return [single]

    # 拠点KWを分割: overhead = risk_part + " (" + site_content + ") " + suffix
    overhead = len(risk_part) + 1 + 1 + 1 + 1 + len(suffix)
    max_content = X_API_QUERY_MAX_LENGTH - overhead

    chunks: list[list[str]] = []
    chunk: list[str] = []
    length = 0

    for kw in site_keywords:
        added = len(kw) if not chunk else len(kw) + 4  # " OR " = 4文字
        if length + added > max_content and chunk:
            chunks.append(chunk)
            chunk = [kw]
            length = len(kw)
        else:
            chunk.append(kw)
            length += added

    if chunk:
        chunks.append(chunk)

    return [f"{risk_part} ({' OR '.join(c)}) {suffix}" for c in chunks]

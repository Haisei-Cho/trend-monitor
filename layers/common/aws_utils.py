"""AWS共通ユーティリティ。

Secrets Manager, S3, DynamoDB など AWS サービスの共通操作を提供する。
"""

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Any

import boto3

from utils import generate_s3_key, serialize_json
from log_utils import setup_logger

logger = setup_logger("aws_utils")

# X API Search Recent で安全に使うAPI層フィルタだけを許可する
SUPPORTED_SEARCH_API_FILTERS = {
    "-is:retweet",
    "-is:reply",
    "-is:quote",
    "-has:links",
    "-has:media",
}

# AWSクライアント（Lambda実行時に1回だけ初期化）
s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")

# 環境変数
BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
SECRET_NAME = os.environ.get("SECRET_NAME", "")

# JST タイムゾーン
JST = timezone(timedelta(hours=9))


def get_bearer_token() -> str:
    """Secrets ManagerからX APIトークンを取得する。"""
    response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    secret_string = response["SecretString"]
    try:
        token = json.loads(secret_string).get("bearer_token", secret_string)
    except json.JSONDecodeError:
        token = secret_string
    if not token:
        raise ValueError("bearer_tokenが見つかりません")
    logger.info("X APIトークン取得完了")
    return token


def get_today_start_time() -> str:
    """当日0時(JST)をUTC ISO8601形式で返す。"""
    today_start = datetime.now(JST).replace(hour=0, minute=0, second=0, microsecond=0)
    return today_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_terms(terms: list[str] | None) -> list[str]:
    """空文字を除去しつつ順序を保って重複排除する。"""
    if not terms:
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for term in terms:
        value = term.strip()
        if not value or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    return normalized


def load_x_api_query_filters(table_name: str) -> tuple[list[str], list[str]]:
    """DynamoDBの除外ルールから除外KWとAPIフィルタを取得する。"""
    exclude_keywords: list[str] = []
    api_filters: list[str] = []

    for item in query_gsi1(table_name, "TYPE#EXCLUSION"):
        exclude_keywords.extend(item.get("keywords", []))
        api_filter = item.get("apiFilter", "").strip()
        if api_filter:
            api_filters.append(api_filter)

    return _normalize_terms(exclude_keywords), _normalize_terms(api_filters)


def filter_supported_search_api_filters(api_filters: list[str] | None) -> list[str]:
    """Search Recentで使えるAPIフィルタだけを残す。"""
    normalized = _normalize_terms(api_filters)
    unsupported = [
        value for value in normalized
        if not value.startswith("-(") and value not in SUPPORTED_SEARCH_API_FILTERS
    ]
    if unsupported:
        logger.warning(
            "Search Recent非対応のAPIフィルタを除外: %s",
            ", ".join(unsupported),
        )

    return [
        value for value in normalized
        if value.startswith("-(") or value in SUPPORTED_SEARCH_API_FILTERS
    ]


def apply_x_api_query_filters(
    base_query: str,
    exclude_keywords: list[str] | None = None,
    api_filters: list[str] | None = None,
    lang: str = "ja",
) -> str:
    """X API検索クエリに共通フィルタを付与する。"""
    query = base_query.strip()
    suffixes: list[str] = []

    lang_filter = f"lang:{lang}"
    if lang_filter not in query:
        suffixes.append(lang_filter)

    normalized_api_filters = filter_supported_search_api_filters(api_filters)
    for api_filter in normalized_api_filters:
        if api_filter not in query:
            suffixes.append(api_filter)

    normalized_keywords = _normalize_terms(exclude_keywords)
    if normalized_keywords:
        exclusion_clause = "-(" + " OR ".join(normalized_keywords) + ")"
        if exclusion_clause not in query:
            suffixes.append(exclusion_clause)

    if not suffixes:
        return query
    return f"{query} {' '.join(suffixes)}"


def save_to_s3(items: list[dict[str, Any]], source: str) -> str:
    """統一フォーマットでS3に保存する。

    Args:
        items: 保存するデータリスト
        source: データソース識別子（"trends_route" / "keyword_route"）

    Returns:
        S3キー
    """
    now = datetime.now(timezone.utc)
    s3_key = generate_s3_key(now)

    data = {
        "fetched_at": now.isoformat(),
        "source": source,
        "item_count": len(items),
        "items": items,
    }

    body = serialize_json(data)
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"S3保存完了: s3://{BUCKET_NAME}/{s3_key}")
    return s3_key


def query_gsi1(table_name: str, gsi1pk: str) -> list[dict]:
    """GSI1PK指定でDynamoDBから全件取得（ページネーション対応）。"""
    table = boto3.resource("dynamodb").Table(table_name)
    items = []
    params = {
        "IndexName": "GSI1",
        "KeyConditionExpression": "GSI1PK = :t",
        "ExpressionAttributeValues": {":t": gsi1pk},
    }
    while True:
        resp = table.query(**params)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        params["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items

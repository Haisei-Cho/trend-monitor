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


def query_sc_gsi1(table_name: str, gsi1pk: str) -> list[dict]:
    """SupplyChainMasterテーブル用GSI1クエリ（小文字キー、ページネーション対応）。"""
    table = boto3.resource("dynamodb").Table(table_name)
    items = []
    params = {
        "IndexName": "GSI1",
        "KeyConditionExpression": "gsi1pk = :t",
        "ExpressionAttributeValues": {":t": gsi1pk},
    }
    while True:
        resp = table.query(**params)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        params["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def query_sc_by_pk(table_name: str, pk: str, sk_prefix: str) -> list[dict]:
    """SupplyChainMasterテーブル用PKクエリ（sk begins_with、ページネーション対応）。"""
    table = boto3.resource("dynamodb").Table(table_name)
    items = []
    params = {
        "KeyConditionExpression": "pk = :pk AND begins_with(sk, :prefix)",
        "ExpressionAttributeValues": {":pk": pk, ":prefix": sk_prefix},
    }
    while True:
        resp = table.query(**params)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        params["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items

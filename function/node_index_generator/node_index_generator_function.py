"""NodeIndexGenerator Lambda関数。

SupplyChainMaster から工場・倉庫・T1/T2サプライヤーを取得し、
related_infra と products を含む JSON を S3 にアップロードする。

トリガー:
  - EventBridge Schedule（日次）
  - 手動 invoke（マスタ更新時）
"""

import json
import os
from datetime import datetime, timezone
from typing import Any

import boto3

from log_utils import setup_logger

logger = setup_logger("node_index_generator")

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")

SC_TABLE_NAME = os.environ.get("SC_TABLE_NAME", "")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
S3_KEY = "config/node_location_index.json"


def _query_all(table, **kwargs) -> list[dict]:
    """ページネーション付きクエリ。"""
    items = []
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def _get_gsi1_items(table, gsi1pk: str) -> list[dict]:
    """GSI1でノード種別を取得。"""
    return _query_all(
        table,
        IndexName="GSI1",
        KeyConditionExpression="gsi1pk = :t",
        ExpressionAttributeValues={":t": gsi1pk},
    )


def _get_relations(table, pk: str, sk_prefix: str) -> list[dict]:
    """PKとSKプレフィックスでリレーションを取得。"""
    return _query_all(
        table,
        KeyConditionExpression="pk = :pk AND begins_with(sk, :prefix)",
        ExpressionAttributeValues={":pk": pk, ":prefix": sk_prefix},
    )


def _build_node_index(table) -> dict:
    """SupplyChainMaster からノードインデックスを構築する。"""
    # ノード取得
    plants = _get_gsi1_items(table, "plant")
    warehouses = _get_gsi1_items(table, "warehouse")
    suppliers = _get_gsi1_items(table, "supplier")

    # Tier算出
    edges: dict[str, list[str]] = {}
    for s in suppliers:
        relations = _get_relations(table, s["pk"], "SUPPLIES_TO#")
        edges[s["pk"]] = [r["to_id"] for r in relations]

    t1 = {sid for sid, targets in edges.items()
           if any(t.startswith("PLT") for t in targets)}
    t2 = {sid for sid, targets in edges.items()
           if any(t in t1 for t in targets) and sid not in t1}

    # 製品マスタ取得
    products = _get_gsi1_items(table, "product")
    product_map = {p["pk"]: p.get("name", "") for p in products}

    nodes = []

    # 工場
    for p in plants:
        produces = _get_relations(table, p["pk"], "PRODUCES#")
        nodes.append({
            "id": p["pk"],
            "name": p.get("name", ""),
            "node_type": "plant",
            "tier": None,
            "location_name": p.get("location_name", ""),
            "related_infra": p.get("related_infra", []),
            "products": [product_map.get(r["product_id"], r["product_id"]) for r in produces],
        })

    # 倉庫
    for w in warehouses:
        produces = _get_relations(table, w["pk"], "PRODUCES#")
        nodes.append({
            "id": w["pk"],
            "name": w.get("name", ""),
            "node_type": "warehouse",
            "tier": None,
            "location_name": w.get("location_name", ""),
            "related_infra": w.get("related_infra", []),
            "products": [product_map.get(r["product_id"], r["product_id"]) for r in produces],
        })

    # サプライヤー（T1/T2のみ）
    for s in suppliers:
        sid = s["pk"]
        tier = "T1" if sid in t1 else "T2" if sid in t2 else None
        if tier is None:
            continue
        produces = _get_relations(table, sid, "PRODUCES#")
        nodes.append({
            "id": sid,
            "name": s.get("name", ""),
            "node_type": "supplier",
            "tier": tier,
            "location_name": s.get("location_name", s.get("region", "")),
            "related_infra": s.get("related_infra", []),
            "products": [product_map.get(r["product_id"], r["product_id"]) for r in produces],
        })

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "node_count": len(nodes),
        "nodes": nodes,
    }


def lambda_handler(event: dict, context: Any) -> dict:
    """ノードインデックスを生成して S3 にアップロードする。"""
    logger.info("ノードインデックス生成開始", extra={"sc_table": SC_TABLE_NAME})

    table = dynamodb.Table(SC_TABLE_NAME)
    index = _build_node_index(table)
    logger.info("ノード取得完了", extra={"node_count": index["node_count"]})

    json_body = json.dumps(index, ensure_ascii=False, indent=2)
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=S3_KEY,
        Body=json_body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("S3アップロード完了", extra={"s3_key": f"s3://{BUCKET_NAME}/{S3_KEY}"})

    return {"node_count": index["node_count"], "s3_key": S3_KEY}
"""EventTable 共通ユーティリティ。

FactChecker / RoadwayFactChecker で共有する
イベント書き込み・重複判定・スコア計算ロジック。
"""

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

from log_utils import setup_logger
from utils import generate_ulid

logger = setup_logger("event_utils")

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

EVENT_TABLE_NAME = os.environ.get("EVENT_TABLE_NAME", "")
event_table = dynamodb.Table(EVENT_TABLE_NAME) if EVENT_TABLE_NAME else None

# ─── 定数 ───

RISK_CATEGORIES = {
    "earthquake": "地震・津波",
    "flood": "風水害",
    "fire": "火災・爆発",
    "traffic": "交通障害",
    "infra": "停電・インフラ障害",
    "labor": "労務・操業リスク",
    "geopolitics": "地政学・貿易",
    "pandemic": "感染症",
}

STATUS_PRIORITY = {"DISMISSED": 0, "WATCHING": 1, "PENDING": 2, "CONFIRMED": 3}

SCORE_JMA = 80
SCORE_ROADWAY = 80
SCORE_NEWS_3PLUS = 50
SCORE_NEWS_1PLUS = 30
SCORE_OFFICIAL = 40

TTL_DAYS = 30
DEDUP_WINDOW_HOURS = 2
DOWNGRADE_THRESHOLD = 30  # confidence が30ポイント以上低下した場合にステータスダウングレード許可


# ─── S3 / ノードインデックス ───

def load_s3_json(bucket: str, key: str) -> dict | None:
    """S3からJSONを読み込む。"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read().decode("utf-8")
        return json.loads(body)
    except Exception as e:
        logger.error(f"S3読込エラー: s3://{bucket}/{key}: {e}")
        return None


def load_node_index(bucket: str) -> dict | None:
    """S3からノードインデックスを読み込む。"""
    return load_s3_json(bucket, "config/node_location_index.json")


# ─── スコア・ステータス判定 ───

def calculate_fact_score(fact_sources: list[dict]) -> int:
    """fact_sources から合計スコアを計算する。"""
    return sum(s.get("score_added", 0) for s in fact_sources)


def compute_final_confidence(ai_confidence: int, fact_score: int) -> int:
    """ai_confidence と fact_score から final_confidence を算出する（加重平均方式）。

    ai_confidence (0-100) と fact_score (0-∞) を 6:4 の加重平均でブレンドする。
    fact_score は100でキャップしてからスケーリングする。
    """
    capped_fact = min(fact_score, 100)
    return int(0.6 * ai_confidence + 0.4 * capped_fact)


def determine_status(final_confidence: int) -> str:
    """final_confidence からステータスを判定する。"""
    if final_confidence >= 80:
        return "CONFIRMED"
    elif final_confidence >= 50:
        return "PENDING"
    elif final_confidence >= 30:
        return "WATCHING"
    else:
        return "DISMISSED"


def determine_risk_level(relevance_score: int, category_id: str) -> int:
    """risk_level を判定する。"""
    critical_categories = {"earthquake", "fire", "traffic", "flood"}
    if relevance_score >= 80 and category_id in critical_categories:
        return 3
    elif relevance_score >= 60:
        return 2
    else:
        return 1


def determine_score_added(source_type: str, data_type: str, fact_data: dict) -> int:
    """ファクトソースの種類からscore_addedを決定する。"""
    if source_type == "jma":
        return SCORE_JMA
    elif source_type == "roadway":
        return SCORE_ROADWAY
    elif source_type == "official":
        return SCORE_OFFICIAL
    elif source_type in ("news", "google_news"):
        articles = fact_data.get("articles", [])
        if len(articles) >= 3:
            return SCORE_NEWS_3PLUS
        else:
            return SCORE_NEWS_1PLUS
    return 0


# ─── ノード構築 ───

def build_related_nodes(matched_node_ids: list[str], node_map: dict, result: dict) -> list[dict]:
    """マッチしたノードIDからrelated_nodesを構築する。"""
    nodes = []
    for nid in matched_node_ids:
        node = node_map.get(nid)
        if node:
            nodes.append({
                "id": nid,
                "name": node.get("name", ""),
                "node_type": node.get("node_type", ""),
                "impact_summary": result.get("impact_summary", ""),
                "relevance_score": result.get("relevance_score", 0),
            })
    return nodes


def extract_matched_text(original_fact: dict, result: dict) -> str:
    """マッチしたテキストを抽出する。"""
    impact = result.get("impact_summary", "")
    if impact:
        return impact[:200]
    if "text" in original_fact:
        return original_fact["text"][:200]
    if "title" in original_fact:
        return original_fact["title"][:200]
    if "road_name" in original_fact:
        parts = [original_fact.get("road_name", ""), original_fact.get("section", ""), original_fact.get("regulation_type", "")]
        return " ".join(p for p in parts if p)[:200]
    return json.dumps(original_fact, ensure_ascii=False)[:200]


# ─── EventTable 書き込み ───

def write_or_update_event(
    *,
    category_id: str,
    related_nodes: list[dict],
    summary: str,
    source_type: str,
    ai_confidence: int | None,
    fact_score: int,
    final_confidence: int,
    status: str,
    risk_level: int,
    fact_sources: list[dict],
    classified_s3_key: str | None,
    raw_s3_key: str | None,
) -> None:
    """EventTable に新規イベントを作成するか、既存イベントを更新する。"""
    node_ids = [n.get("id", "") for n in related_nodes if n.get("id")]
    existing_event = _find_existing_event(category_id, node_ids)

    now = datetime.now(timezone.utc)

    if existing_event:
        _update_existing_event(
            existing_event=existing_event,
            new_fact_sources=fact_sources,
            new_ai_confidence=ai_confidence,
            new_classified_s3_key=classified_s3_key,
            new_summary=summary,
            new_related_nodes=related_nodes,
            now=now,
        )
    else:
        _create_new_event(
            category_id=category_id,
            related_nodes=related_nodes,
            summary=summary,
            source_type=source_type,
            ai_confidence=ai_confidence,
            fact_score=fact_score,
            final_confidence=final_confidence,
            status=status,
            risk_level=risk_level,
            fact_sources=fact_sources,
            classified_s3_key=classified_s3_key,
            raw_s3_key=raw_s3_key,
            now=now,
        )


def _find_existing_event(category_id: str, node_ids: list[str]) -> dict | None:
    """GSI2 Query + filter でDedup対象の既存イベントを検索する。"""
    if not node_ids:
        return None

    now = datetime.now(timezone.utc)
    window_start = (now - timedelta(hours=DEDUP_WINDOW_HOURS)).isoformat()
    now_str = now.isoformat()

    try:
        node_id_set = set(node_ids)
        query_params = {
            "IndexName": "GSI2",
            "KeyConditionExpression": (
                Key("GSI2PK").eq(f"CAT#{category_id}") &
                Key("GSI2SK").between(window_start, now_str)
            ),
        }
        while True:
            resp = event_table.query(**query_params)
            for item in resp.get("Items", []):
                item_node_ids = {n.get("id", "") for n in item.get("related_nodes", [])}
                if item_node_ids & node_id_set:
                    return item
            if "LastEvaluatedKey" not in resp:
                break
            query_params["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    except Exception as e:
        logger.error(f"Dedup検索エラー: {e}")
        return None

    return None


def _update_existing_event(
    *,
    existing_event: dict,
    new_fact_sources: list[dict],
    new_ai_confidence: int | None,
    new_classified_s3_key: str | None,
    new_summary: str,
    new_related_nodes: list[dict],
    now: datetime,
) -> None:
    """既存イベントをUPDATEする（fact_sources追加、スコア再計算、status上昇のみ）。"""
    event_id = existing_event["event_id"]

    existing_sources = existing_event.get("fact_sources", [])
    merged_sources = existing_sources + new_fact_sources

    new_fact_score = sum(s.get("score_added", 0) for s in merged_sources)

    ai_conf = existing_event.get("ai_confidence")
    if new_ai_confidence is not None:
        ai_conf = new_ai_confidence

    if ai_conf is not None:
        new_final_confidence = compute_final_confidence(ai_conf, new_fact_score)
    else:
        new_final_confidence = min(new_fact_score, 100)

    new_status = determine_status(new_final_confidence)
    existing_status = existing_event.get("status", "DISMISSED")
    existing_confidence = existing_event.get("final_confidence", 0)
    confidence_drop = existing_confidence - new_final_confidence

    if confidence_drop > DOWNGRADE_THRESHOLD:
        # 大幅なconfidence低下 → ダウングレード許可
        logger.info(
            f"ステータスダウングレード: EVT#{event_id} "
            f"{existing_status}→{new_status} "
            f"(confidence {existing_confidence}→{new_final_confidence}, drop={confidence_drop})"
        )
    elif STATUS_PRIORITY.get(new_status, 0) < STATUS_PRIORITY.get(existing_status, 0):
        new_status = existing_status

    existing_node_ids = {n.get("id") for n in existing_event.get("related_nodes", [])}
    merged_nodes = list(existing_event.get("related_nodes", []))
    for node in new_related_nodes:
        if node.get("id") not in existing_node_ids:
            merged_nodes.append(node)

    existing_risk_level = existing_event.get("risk_level", 1)
    new_risk_level = existing_risk_level
    if new_fact_sources:
        max_relevance = max((n.get("relevance_score", 0) for n in new_related_nodes), default=0)
        category_id = existing_event.get("category_id", "")
        computed_risk = determine_risk_level(max_relevance, category_id)
        new_risk_level = max(existing_risk_level, computed_risk)

    update_expr = (
        "SET fact_sources = :fs, fact_score = :fscore, "
        "final_confidence = :fc, #st = :status, "
        "GSI1PK = :gsi1pk, "
        "risk_level = :rl, related_nodes = :rn, "
        "updated_at = :ua"
    )
    expr_values: dict[str, Any] = {
        ":fs": merged_sources,
        ":fscore": new_fact_score,
        ":fc": new_final_confidence,
        ":status": new_status,
        ":gsi1pk": f"STATUS#{new_status}",
        ":rl": new_risk_level,
        ":rn": merged_nodes,
        ":ua": now.isoformat(),
    }
    expr_names = {"#st": "status"}

    if new_summary:
        update_expr += ", summary = :summary"
        expr_values[":summary"] = new_summary
    if new_ai_confidence is not None:
        update_expr += ", ai_confidence = :aic"
        expr_values[":aic"] = new_ai_confidence
    if new_classified_s3_key:
        update_expr += ", classified_s3_key = :csk"
        expr_values[":csk"] = new_classified_s3_key

    try:
        event_table.update_item(
            Key={"PK": f"EVT#{event_id}", "SK": "META"},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values,
            ExpressionAttributeNames=expr_names,
        )
        logger.info(f"イベント更新: EVT#{event_id}, status={new_status}, final_confidence={new_final_confidence}")
    except Exception as e:
        logger.error(f"イベント更新エラー: {e}")


def _create_new_event(
    *,
    category_id: str,
    related_nodes: list[dict],
    summary: str,
    source_type: str,
    ai_confidence: int | None,
    fact_score: int,
    final_confidence: int,
    status: str,
    risk_level: int,
    fact_sources: list[dict],
    classified_s3_key: str | None,
    raw_s3_key: str | None,
    now: datetime,
) -> None:
    """新規イベントを EventTable に PUT する。"""
    event_id = generate_ulid()
    ttl = int((now + timedelta(days=TTL_DAYS)).timestamp())

    item: dict[str, Any] = {
        "PK": f"EVT#{event_id}",
        "SK": "META",
        "GSI1PK": f"STATUS#{status}",
        "GSI1SK": now.isoformat(),
        "GSI2PK": f"CAT#{category_id}",
        "GSI2SK": now.isoformat(),
        "event_id": event_id,
        "status": status,
        "category_id": category_id,
        "category_name": RISK_CATEGORIES.get(category_id, ""),
        "summary": summary,
        "source_type": source_type,
        "fact_score": fact_score,
        "final_confidence": final_confidence,
        "risk_level": risk_level,
        "related_nodes": related_nodes,
        "fact_sources": fact_sources,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "reviewed_by": None,
        "ttl": ttl,
    }

    if ai_confidence is not None:
        item["ai_confidence"] = ai_confidence
    if classified_s3_key:
        item["classified_s3_key"] = classified_s3_key
    if raw_s3_key:
        item["raw_s3_key"] = raw_s3_key

    try:
        event_table.put_item(Item=item)
        logger.info(f"イベント作成: EVT#{event_id}, category={category_id}, status={status}")
    except Exception as e:
        logger.error(f"イベント作成エラー: {e}")

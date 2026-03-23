"""ファクトチェック Lambda関数。

2つのトリガーで起動し、ファクトデータとサプライチェーンノードを照合して
リスクイベントを EventTable に書き込む。

トリガー:
    A: S3 classified/ → ファクト照合 → EventTable
    B: S3 facts/ → Stage 1 Haiku → Stage 2 Sonnet → EventTable
"""

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

from event_utils import (
    RISK_CATEGORIES,
    SCORE_JMA,
    SCORE_NEWS_1PLUS,
    SCORE_NEWS_3PLUS,
    SCORE_OFFICIAL,
    SCORE_ROADWAY,
    build_related_nodes,
    calculate_fact_score,
    compute_final_confidence,
    determine_risk_level,
    determine_score_added,
    determine_status,
    extract_matched_text,
    load_node_index,
    load_s3_json,
    write_or_update_event,
)
from fact_matcher import (
    format_node_list,
    invoke_stage1,
    invoke_stage2,
    invoke_stage2_classified,
)
from log_utils import setup_logger

logger = setup_logger("fact_checker")

BUCKET_NAME = os.environ["BUCKET_NAME"]
ROADWAY_TABLE_NAME = os.environ["ROADWAY_TABLE_NAME"]

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# カテゴリ別ファクトソースマッピング（Trigger A で使用）
CATEGORY_FACT_SOURCES = {
    "earthquake": {
        "jma": ["facts/jma/latest/quake_list.json", "facts/jma/latest/tsunami.json"],
        "news": ["facts/news/latest/earthquake.json"],
        "official": True,
    },
    "flood": {
        "jma": ["facts/jma/latest/typhoon.json"],
        "news": ["facts/news/latest/flood.json"],
        "official": True,
    },
    "fire": {
        "news": ["facts/news/latest/fire.json"],
        "official": True,
    },
    "traffic": {
        "roadway": True,
        "news": ["facts/news/latest/traffic.json"],
        "official": True,
    },
    "infra": {
        "news": ["facts/news/latest/infra.json"],
        "official": True,
    },
    "labor": {
        "news": ["facts/news/latest/labor.json"],
    },
    "geopolitics": {
        "news": ["facts/news/latest/geopolitics.json"],
    },
    "pandemic": {
        "news": ["facts/news/latest/pandemic.json"],
    },
}


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda関数エントリーポイント。トリガーを判定して適切な処理を実行する。"""
    if "detail" in event:
        s3_key = event["detail"].get("object", {}).get("key", "")
        if s3_key.startswith("classified/"):
            return handle_trigger_a(event)
        elif s3_key.startswith("facts/"):
            return handle_trigger_b(event)

    logger.warning(f"不明なトリガー: {json.dumps(event, ensure_ascii=False)[:500]}")
    return {"processed": False, "reason": "不明なトリガー"}


# ─── Trigger A: classified 起点 ───

def handle_trigger_a(event: dict) -> dict:
    """Trigger A: classified S3イベントを処理する。

    Stage 1 スキップ → カテゴリ別ファクトソース読込 → Stage 2 Sonnet → スコア計算 → EventTable
    """
    bucket = event["detail"]["bucket"]["name"]
    s3_key = event["detail"]["object"]["key"]
    logger.info(f"Trigger A: {s3_key}")

    # classified イベント読込
    classified = load_s3_json(bucket, s3_key)
    if not classified:
        return {"processed": False, "reason": "classified読込失敗"}

    category_id = classified.get("category_id", "")
    if category_id not in RISK_CATEGORIES:
        logger.warning(f"不明なカテゴリ: {category_id}")
        return {"processed": False, "reason": f"不明なカテゴリ: {category_id}"}

    # ノードインデックス読込
    node_index = load_node_index(bucket)
    if not node_index or not node_index.get("nodes"):
        return {"processed": False, "reason": "ノードインデックスが空"}
    node_list_text = format_node_list(node_index)

    # カテゴリ別ファクトソース読込
    fact_sources_data = _load_fact_sources_for_category(bucket, category_id)

    # Stage 2 Sonnet（classified起点）
    classified_for_ai = {
        "category_id": category_id,
        "summary": classified.get("summary", ""),
        "ai_confidence": classified.get("ai_confidence", 0),
        "related_nodes": classified.get("related_nodes", []),
    }

    try:
        stage2_results = invoke_stage2_classified(classified_for_ai, fact_sources_data, node_list_text)
    except Exception as e:
        logger.error(f"Stage 2 Sonnet エラー: {e}")
        stage2_results = []

    # ノードマップ構築
    node_map = {n["id"]: n for n in node_index.get("nodes", [])}

    # 結果処理 → EventTable
    events_written = 0
    ai_confidence = classified.get("ai_confidence", 0)

    if stage2_results:
        for result in stage2_results:
            matched_node_ids = result.get("matched_node_ids", [])
            if not matched_node_ids:
                continue

            fact_sources = _build_fact_sources_from_stage2(result)
            if not fact_sources and fact_sources_data:
                fact_sources = _build_fact_sources_from_loaded_data(fact_sources_data)
            fact_score = calculate_fact_score(fact_sources)
            final_confidence = compute_final_confidence(ai_confidence, fact_score)
            status = determine_status(final_confidence)
            risk_level = determine_risk_level(result.get("relevance_score", 0), category_id)

            related_nodes = build_related_nodes(matched_node_ids, node_map, result)

            write_or_update_event(
                category_id=category_id,
                related_nodes=related_nodes,
                summary=result.get("impact_summary", classified.get("summary", "")),
                source_type="classified",
                ai_confidence=ai_confidence,
                fact_score=fact_score,
                final_confidence=final_confidence,
                status=status,
                risk_level=risk_level,
                fact_sources=fact_sources,
                classified_s3_key=s3_key,
                raw_s3_key=classified.get("raw_s3_key"),
            )
            events_written += 1
    else:
        # ファクトマッチなし: ai_confidenceのみでイベント作成
        related_nodes_raw = classified.get("related_nodes", [])
        if related_nodes_raw:
            final_confidence = ai_confidence
            status = determine_status(final_confidence)
            risk_level = 1

            write_or_update_event(
                category_id=category_id,
                related_nodes=related_nodes_raw,
                summary=classified.get("summary", ""),
                source_type="classified",
                ai_confidence=ai_confidence,
                fact_score=0,
                final_confidence=final_confidence,
                status=status,
                risk_level=risk_level,
                fact_sources=[],
                classified_s3_key=s3_key,
                raw_s3_key=classified.get("raw_s3_key"),
            )
            events_written += 1

    output = {"trigger": "A", "s3_key": s3_key, "events_written": events_written}
    logger.info(f"Trigger A 完了: {json.dumps(output, ensure_ascii=False)}")
    return output


# ─── Trigger B: facts 起点 ───

def handle_trigger_b(event: dict) -> dict:
    """Trigger B: facts/ S3イベントを処理する。

    データタイプ判定 → Stage 1 Haiku → Stage 2 Sonnet → EventTable
    """
    bucket = event["detail"]["bucket"]["name"]
    s3_key = event["detail"]["object"]["key"]
    logger.info(f"Trigger B: {s3_key}")

    # factデータ読込
    fact_data = load_s3_json(bucket, s3_key)
    if not fact_data:
        return {"processed": False, "reason": "factデータ読込失敗"}

    # データタイプ判定
    source_type, data_type = _determine_fact_type(s3_key)
    facts_for_stage1 = _extract_facts_for_stage1(fact_data, source_type, data_type, s3_key)

    if not facts_for_stage1:
        logger.info("Stage 1 に渡すファクトデータなし")
        return {"trigger": "B", "s3_key": s3_key, "events_written": 0}

    # Stage 1: Haiku フィルタ
    try:
        stage1_results = invoke_stage1(facts_for_stage1)
    except Exception as e:
        logger.error(f"Stage 1 Haiku エラー: {e}")
        return {"trigger": "B", "s3_key": s3_key, "events_written": 0, "error": str(e)}

    passed_facts = [
        facts_for_stage1[r["fact_index"]]
        for r in stage1_results
        if r.get("decision") == "pass" and 0 <= r.get("fact_index", -1) < len(facts_for_stage1)
    ]

    if not passed_facts:
        logger.info(f"Stage 1 全件 skip: {len(facts_for_stage1)}件")
        return {"trigger": "B", "s3_key": s3_key, "stage1_passed": 0, "events_written": 0}

    logger.info(f"Stage 1 pass: {len(passed_facts)}/{len(facts_for_stage1)}件")

    # ノードインデックス読込
    node_index = load_node_index(bucket)
    if not node_index or not node_index.get("nodes"):
        return {"processed": False, "reason": "ノードインデックスが空"}
    node_list_text = format_node_list(node_index)
    node_map = {n["id"]: n for n in node_index.get("nodes", [])}

    # Stage 2: Sonnet ノードマッチ
    for i, f in enumerate(passed_facts):
        f["fact_index"] = i

    try:
        stage2_results = invoke_stage2(passed_facts, node_list_text)
    except Exception as e:
        logger.error(f"Stage 2 Sonnet エラー: {e}")
        return {"trigger": "B", "s3_key": s3_key, "events_written": 0, "error": str(e)}

    # 結果処理 → EventTable
    events_written = 0
    for result in stage2_results:
        matched_node_ids = result.get("matched_node_ids", [])
        if not matched_node_ids:
            continue

        category_id = result.get("category_id", _infer_category(s3_key, source_type))
        if category_id not in RISK_CATEGORIES:
            continue

        fact_index = result.get("fact_index", -1)
        original_fact = passed_facts[fact_index] if 0 <= fact_index < len(passed_facts) else {}

        score_added = determine_score_added(source_type, data_type, fact_data)
        fact_sources = [{
            "source": source_type,
            "data_type": data_type,
            "matched_text": extract_matched_text(original_fact, result),
            "matched_at": datetime.now(timezone.utc).isoformat(),
            "score_added": score_added,
        }]
        fact_score = score_added
        final_confidence = fact_score
        status = determine_status(final_confidence)
        risk_level = determine_risk_level(result.get("relevance_score", 0), category_id)

        related_nodes = build_related_nodes(matched_node_ids, node_map, result)

        write_or_update_event(
            category_id=category_id,
            related_nodes=related_nodes,
            summary=result.get("impact_summary", ""),
            source_type="fact",
            ai_confidence=None,
            fact_score=fact_score,
            final_confidence=final_confidence,
            status=status,
            risk_level=risk_level,
            fact_sources=fact_sources,
            classified_s3_key=None,
            raw_s3_key=None,
        )
        events_written += 1

    output = {
        "trigger": "B",
        "s3_key": s3_key,
        "stage1_passed": len(passed_facts),
        "events_written": events_written,
    }
    logger.info(f"Trigger B 完了: {json.dumps(output, ensure_ascii=False)}")
    return output


# ─── ヘルパー関数 ───

def _load_fact_sources_for_category(bucket: str, category_id: str) -> list[dict]:
    """カテゴリに対応するファクトソースをS3/DynamoDBから読み込む。"""
    sources_config = CATEGORY_FACT_SOURCES.get(category_id, {})
    result = []

    for jma_key in sources_config.get("jma", []):
        data = load_s3_json(bucket, jma_key)
        if data:
            result.append({"source": "jma", "type": jma_key.split("/")[-1].replace(".json", ""), "data": data})

    for news_key in sources_config.get("news", []):
        data = load_s3_json(bucket, news_key)
        if data:
            result.append({"source": "news", "type": "articles", "data": data})

    if sources_config.get("official"):
        official_data = _load_recent_official(bucket)
        if official_data:
            result.append({"source": "official", "type": "tweets", "data": official_data})

    if sources_config.get("roadway"):
        roadway_data = _load_active_roadway()
        if roadway_data:
            result.append({"source": "roadway", "type": "regulations", "data": roadway_data})

    return result


def _load_recent_official(bucket: str) -> dict | None:
    """直近のofficial factsファイルを読み込む。UTC当日と前日を検索する。"""
    now = datetime.now(timezone.utc)
    dates = [now.strftime("%Y-%m-%d"), (now - timedelta(days=1)).strftime("%Y-%m-%d")]
    try:
        all_contents = []
        for date_str in dates:
            prefix = f"facts/official/{date_str}/"
            resp = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
            all_contents.extend(resp.get("Contents", []))
        if not all_contents:
            return None
        latest = sorted(all_contents, key=lambda x: x["Key"], reverse=True)[0]
        return load_s3_json(BUCKET_NAME, latest["Key"])
    except Exception as e:
        logger.warning(f"Official facts読込エラー: {e}")
        return None


def _load_active_roadway() -> dict | None:
    """RoadwayTraffic テーブルからACTIVE規制を読み込む。"""
    try:
        roadway_table = dynamodb.Table(ROADWAY_TABLE_NAME)
        resp = roadway_table.query(
            IndexName="GSI2",
            KeyConditionExpression=Key("GSI2PK").eq("ACTIVE"),
        )
        items = resp.get("Items", [])
        if not items:
            return None
        return {"regulations": items, "count": len(items)}
    except Exception as e:
        logger.warning(f"RoadwayTraffic読込エラー: {e}")
        return None


def _determine_fact_type(s3_key: str) -> tuple[str, str]:
    """S3キーからファクトソースタイプとデータタイプを判定する。"""
    if s3_key.startswith("facts/jma/"):
        data_type = s3_key.split("/")[-1].replace(".json", "")
        return ("jma", data_type)
    elif s3_key.startswith("facts/news/"):
        return ("news", "articles")
    elif s3_key.startswith("facts/official/"):
        return ("official", "tweets")
    return ("unknown", "unknown")


def _extract_facts_for_stage1(fact_data: dict, source_type: str, data_type: str, s3_key: str) -> list[dict]:
    """factデータからStage 1に渡すデータを構築する。"""
    facts = []

    if source_type == "jma":
        raw_data = fact_data.get("raw_data", [])
        for i, item in enumerate(raw_data):
            facts.append({
                "fact_index": i,
                "source": "jma",
                "data_type": data_type,
                "data": item,
            })
    elif source_type == "news":
        articles = fact_data.get("articles", [])
        if articles:
            facts.append({
                "fact_index": 0,
                "source": "news",
                "category": fact_data.get("category", ""),
                "article_count": len(articles),
                "articles": [{"title": a.get("title", ""), "description": a.get("description", "")} for a in articles[:10]],
            })
    elif source_type == "official":
        tweets = fact_data.get("tweets", [])
        if tweets:
            facts.append({
                "fact_index": 0,
                "source": "official",
                "tweets": [{"author": t.get("author_username", ""), "text": t.get("text", "")} for t in tweets[:10]],
            })

    return facts


def _infer_category(s3_key: str, source_type: str) -> str:
    """S3キーとソースタイプからカテゴリを推定する。"""
    if source_type == "jma":
        if "tsunami" in s3_key or "quake" in s3_key:
            return "earthquake"
        elif "typhoon" in s3_key:
            return "flood"
    elif source_type == "news":
        parts = s3_key.split("/")
        if len(parts) >= 4:
            category = parts[-1].replace(".json", "")
            if category in RISK_CATEGORIES:
                return category
    return "unknown"


def _build_fact_sources_from_loaded_data(fact_sources_data: list[dict]) -> list[dict]:
    """渡されたファクトソースデータが存在する場合、プログラム的にfact_sourcesを構築する。"""
    sources = []
    now = datetime.now(timezone.utc).isoformat()

    for fs in fact_sources_data:
        source = fs.get("source", "")
        data = fs.get("data", {})
        score_added = 0
        data_type = fs.get("type", "")

        if source == "jma":
            raw_data = data.get("raw_data", [])
            if raw_data:
                score_added = SCORE_JMA
        elif source == "roadway":
            regulations = data.get("regulations", [])
            if regulations:
                score_added = SCORE_ROADWAY
        elif source == "news":
            articles = data.get("articles", [])
            if articles:
                score_added = SCORE_NEWS_3PLUS if len(articles) >= 3 else SCORE_NEWS_1PLUS
        elif source == "official":
            tweets = data.get("tweets", [])
            if tweets:
                score_added = SCORE_OFFICIAL

        if score_added > 0:
            sources.append({
                "source": source,
                "data_type": data_type,
                "matched_text": "",
                "matched_at": now,
                "score_added": score_added,
            })

    return sources


def _build_fact_sources_from_stage2(result: dict) -> list[dict]:
    """Stage 2（classified起点）の結果からfact_sourcesを構築する。"""
    fact_match_details = result.get("fact_match_details", [])
    sources = []
    now = datetime.now(timezone.utc).isoformat()

    for detail in fact_match_details:
        source = detail.get("source", "")
        score_added = 0
        if source == "jma":
            score_added = SCORE_JMA
        elif source == "roadway":
            score_added = SCORE_ROADWAY
        elif source == "news":
            article_count = detail.get("article_count", 1)
            score_added = SCORE_NEWS_3PLUS if article_count >= 3 else SCORE_NEWS_1PLUS
        elif source == "official":
            score_added = SCORE_OFFICIAL

        sources.append({
            "source": source,
            "data_type": detail.get("data_type", ""),
            "matched_text": detail.get("matched_text", ""),
            "matched_at": now,
            "score_added": score_added,
        })

    return sources

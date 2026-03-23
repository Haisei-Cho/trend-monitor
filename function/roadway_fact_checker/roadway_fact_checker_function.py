"""RoadwayFactChecker Lambda関数。

DynamoDB Streams (RoadwayTraffic INSERT) をトリガーに、
Stage 1 Haiku フィルタ → Stage 2 Sonnet ノードマッチ → EventTable 書き込み。
"""

import json
import os
from datetime import datetime, timezone
from typing import Any

from event_utils import (
    RISK_CATEGORIES,
    SCORE_ROADWAY,
    build_related_nodes,
    determine_risk_level,
    determine_status,
    extract_matched_text,
    load_node_index,
    write_or_update_event,
)
from fact_matcher import format_node_list, invoke_stage1, invoke_stage2
from log_utils import setup_logger

logger = setup_logger("roadway_fact_checker")

BUCKET_NAME = os.environ["BUCKET_NAME"]


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda関数エントリーポイント。

    DynamoDB Streams の INSERT イベントを処理する。
    """
    records = event.get("Records", [])
    insert_records = [r for r in records if r.get("eventName") == "INSERT"]

    if not insert_records:
        return {"events_written": 0, "reason": "INSERTイベントなし"}

    logger.info(f"RoadwayFactChecker開始: {len(insert_records)}件のINSERTレコード")

    # DynamoDB Streams レコードからファクトデータを構築
    facts_for_stage1 = []
    for i, record in enumerate(insert_records):
        new_image = record.get("dynamodb", {}).get("NewImage", {})
        fact = _dynamodb_image_to_fact(new_image, i)
        if fact:
            facts_for_stage1.append(fact)

    if not facts_for_stage1:
        return {"events_written": 0}

    # Stage 1: Haiku フィルタ
    try:
        stage1_results = invoke_stage1(facts_for_stage1)
    except Exception as e:
        logger.error(f"Stage 1 Haiku エラー: {e}")
        return {"events_written": 0, "error": str(e)}

    passed_facts = [
        facts_for_stage1[r["fact_index"]]
        for r in stage1_results
        if r.get("decision") == "pass" and 0 <= r.get("fact_index", -1) < len(facts_for_stage1)
    ]

    if not passed_facts:
        logger.info(f"Stage 1 全件 skip: {len(facts_for_stage1)}件")
        return {"stage1_passed": 0, "events_written": 0}

    logger.info(f"Stage 1 pass: {len(passed_facts)}/{len(facts_for_stage1)}件")

    # ノードインデックス読込
    node_index = load_node_index(BUCKET_NAME)
    if not node_index:
        return {"processed": False, "reason": "ノードインデックス読込失敗"}
    node_list_text = format_node_list(node_index)
    node_map = {n["id"]: n for n in node_index.get("nodes", [])}

    # Stage 2: Sonnet ノードマッチ
    for i, f in enumerate(passed_facts):
        f["fact_index"] = i

    try:
        stage2_results = invoke_stage2(passed_facts, node_list_text)
    except Exception as e:
        logger.error(f"Stage 2 Sonnet エラー: {e}")
        return {"events_written": 0, "error": str(e)}

    # 結果処理 → EventTable
    events_written = 0
    for result in stage2_results:
        matched_node_ids = result.get("matched_node_ids", [])
        if not matched_node_ids:
            continue

        category_id = result.get("category_id", "traffic")
        if category_id not in RISK_CATEGORIES:
            category_id = "traffic"

        fact_index = result.get("fact_index", -1)
        original_fact = passed_facts[fact_index] if 0 <= fact_index < len(passed_facts) else {}

        fact_sources = [{
            "source": "roadway",
            "data_type": "regulation",
            "matched_text": extract_matched_text(original_fact, result),
            "matched_at": datetime.now(timezone.utc).isoformat(),
            "score_added": SCORE_ROADWAY,
        }]
        fact_score = SCORE_ROADWAY
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
        "records_processed": len(insert_records),
        "stage1_passed": len(passed_facts),
        "events_written": events_written,
    }
    logger.info(f"RoadwayFactChecker完了: {json.dumps(output, ensure_ascii=False)}")
    return output


def _dynamodb_image_to_fact(new_image: dict, index: int) -> dict | None:
    """DynamoDB Streams の NewImage をfactデータに変換する。"""
    if not new_image:
        return None

    def unwrap(val: dict) -> Any:
        if "S" in val:
            return val["S"]
        elif "N" in val:
            text = val["N"]
            return int(text) if "." not in text else float(text)
        elif "BOOL" in val:
            return val["BOOL"]
        elif "NULL" in val:
            return None
        elif "L" in val:
            return [unwrap(item) for item in val["L"]]
        elif "M" in val:
            return {k: unwrap(v) for k, v in val["M"].items()}
        return str(val)

    fact = {"fact_index": index, "source": "roadway"}
    for key in ["road_name", "direction", "section", "regulation_type", "cause", "pref_name"]:
        if key in new_image:
            fact[key] = unwrap(new_image[key])

    return fact if len(fact) > 2 else None

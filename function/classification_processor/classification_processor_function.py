"""Lambda for classifying raw trend items into simple supply-chain impact summaries."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote_plus

import boto3

from aws_utils import load_json_from_s3, query_index, save_json_to_s3
from log_utils import setup_logger

logger = setup_logger("classification_processor")

SUPPLY_CHAIN_TABLE_NAME = os.environ.get("SUPPLY_CHAIN_TABLE_NAME", "SupplyChainMaster")
BUCKET_NAME = os.environ["BUCKET_NAME"]
BEDROCK_MODEL_ID = os.environ.get("CLASSIFICATION_MODEL_ID", "jp.anthropic.claude-sonnet-4-6")
AI_BATCH_SIZE = int(os.environ.get("AI_BATCH_SIZE", "8"))
MAX_SAMPLE_TWEETS = int(os.environ.get("MAX_SAMPLE_TWEETS", "5"))
MAX_TWEET_TEXT_LENGTH = int(os.environ.get("MAX_TWEET_TEXT_LENGTH", "220"))
ENABLE_AI_CLASSIFICATION = os.environ.get("ENABLE_AI_CLASSIFICATION", "true").lower() == "true"

bedrock_runtime = boto3.client("bedrock-runtime")

AI_SYSTEM_PROMPT = """You classify supply-chain event records.

Use only the provided event text and supply-chain context.
Prefer conservative scoring when evidence is weak.
Return JSON only.

Return shape:
{
  "results": [
    {
      "id": "item id",
      "time": "ISO8601 or empty string",
      "reason": "short reason",
      "impacted_plants": ["PLT001"],
      "impacted_suppliers": ["SUP001", "SUP101"],
      "classification_code": "交通事故・道路障害",
      "classification_slug": "traffic",
      "score": 0
    }
  ]
}
"""


@dataclass(slots=True)
class RiskCategory:
    code: str
    label: str
    slug: str
    base_severity: int
    keywords: list[str]


RISK_CATEGORIES: dict[str, RiskCategory] = {
    "earthquake": RiskCategory("earthquake", "地震・津波", "earthquake", 45, ["地震", "震度", "津波", "余震", "震源", "マグニチュード"]),
    "flood": RiskCategory("flood", "豪雨・洪水", "flood", 35, ["大雨", "豪雨", "洪水", "浸水", "冠水", "線状降水帯", "土砂", "河川氾濫"]),
    "fire": RiskCategory("fire", "火災・爆発", "fire", 42, ["火災", "爆発", "炎上", "延焼", "煙", "燃焼", "出火"]),
    "traffic": RiskCategory("traffic", "交通事故・道路障害", "traffic", 28, ["事故", "通行止め", "渋滞", "運休", "遅延", "故障", "通行規制", "交通障害", "道路"]),
    "infrastructure": RiskCategory("infrastructure", "インフラ障害", "infrastructure", 33, ["停電", "断水", "通信障害", "システム障害", "ネットワーク障害", "インフラ障害"]),
    "labor": RiskCategory("labor", "労働・操業停止", "labor", 32, ["ストライキ", "操業停止", "休業", "人手不足", "労災", "労働争議"]),
    "geopolitics": RiskCategory("geopolitics", "地政学・規制", "geopolitics", 30, ["制裁", "関税", "禁輸", "紛争", "軍事", "輸出規制", "地政学"]),
    "pandemic": RiskCategory("pandemic", "感染症・衛生", "pandemic", 26, ["感染", "パンデミック", "クラスター", "検疫", "新型", "ウイルス"]),
    "general": RiskCategory("general", "その他リスク", "general", 12, []),
}


@dataclass(slots=True)
class SupplyNode:
    node_id: str
    node_type: str
    name: str
    location_name: str
    region: str
    country: str
    keywords: list[str]
    products: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SupplyChainContext:
    plants: dict[str, SupplyNode]
    warehouses: dict[str, SupplyNode]
    t1_suppliers: dict[str, SupplyNode]
    t2_suppliers: dict[str, SupplyNode]
    plant_to_t1: dict[str, list[str]]
    plant_to_t2: dict[str, list[str]]
    t1_to_plants: dict[str, list[str]]
    t2_to_t1: dict[str, list[str]]
    t2_to_plants: dict[str, list[str]]
    warehouse_to_plants: dict[str, list[str]]
    summary_text: str
    product_names: dict[str, str] = field(default_factory=dict)


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip().lower()


def unique_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def extract_string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, list):
        return [item.strip() for item in value if isinstance(item, str) and item.strip()]
    return []


def build_node_keywords(item: dict[str, Any]) -> list[str]:
    raw_values = [
        item.get("pk", ""),
        item.get("name", ""),
        item.get("location_name", ""),
        item.get("region", ""),
        item.get("country", ""),
        item.get("pref", ""),
        item.get("city", ""),
    ]
    return unique_preserving_order([value.strip() for value in raw_values if isinstance(value, str) and value.strip()])


def get_upstream_node_ids(target_id: str, prefix: str | None = None) -> list[str]:
    edges = query_index(SUPPLY_CHAIN_TABLE_NAME, "GSI2", "gsi2pk", target_id)
    node_ids = [edge.get("pk", "") for edge in edges if edge.get("edge_type") == "supplies_to"]
    if prefix:
        node_ids = [node_id for node_id in node_ids if node_id.startswith(prefix)]
    return unique_preserving_order([node_id for node_id in node_ids if node_id])


def build_product_records() -> tuple[dict[str, str], dict[str, list[str]]]:
    product_names: dict[str, str] = {}
    product_to_plants: dict[str, list[str]] = {}
    for item in query_index(SUPPLY_CHAIN_TABLE_NAME, "GSI1", "gsi1pk", "product"):
        product_id = item.get("pk", "")
        if not product_id:
            continue
        if item.get("name"):
            product_names[product_id] = item["name"]
        plant_ids = extract_string_list(item.get("plant_ids"))
        if plant_ids:
            product_to_plants[product_id] = unique_preserving_order(plant_ids)
    return product_names, product_to_plants


def get_plant_products(item: dict[str, Any], product_names: dict[str, str], product_to_plants: dict[str, list[str]]) -> list[str]:
    plant_id = item.get("pk", "")
    product_ids = unique_preserving_order(
        extract_string_list(item.get("product_ids"))
        + extract_string_list(item.get("products"))
        + extract_string_list(item.get("product_refs"))
    )
    if plant_id:
        for product_id, plant_ids in product_to_plants.items():
            if plant_id in plant_ids:
                product_ids.append(product_id)
    product_labels = unique_preserving_order(
        extract_string_list(item.get("product_names"))
        + extract_string_list(item.get("product_labels"))
    )
    resolved_names = [product_names[product_id] for product_id in unique_preserving_order(product_ids) if product_id in product_names]
    return unique_preserving_order(product_labels + resolved_names)


def build_node_map(node_type: str, product_names: dict[str, str], product_to_plants: dict[str, list[str]]) -> dict[str, SupplyNode]:
    nodes: dict[str, SupplyNode] = {}
    for item in query_index(SUPPLY_CHAIN_TABLE_NAME, "GSI1", "gsi1pk", node_type):
        node_id = item.get("pk", "")
        if not node_id:
            continue
        nodes[node_id] = SupplyNode(
            node_id=node_id,
            node_type=node_type,
            name=item.get("name", node_id),
            location_name=item.get("location_name", ""),
            region=item.get("region", item.get("pref", "")),
            country=item.get("country", ""),
            keywords=build_node_keywords(item),
            products=get_plant_products(item, product_names, product_to_plants) if node_type == "plant" else [],
        )
    return nodes


def build_master_summary(context: SupplyChainContext) -> str:
    lines = ["[Plants]"]
    for node in context.plants.values():
        product_text = ", ".join(node.products) if node.products else "-"
        suppliers_t1 = ", ".join(context.plant_to_t1.get(node.node_id, [])) or "-"
        suppliers_t2 = ", ".join(context.plant_to_t2.get(node.node_id, [])) or "-"
        lines.append(
            f"- {node.node_id}: {node.name} / {node.location_name or node.region} / "
            f"products[{product_text}] / t1[{suppliers_t1}] / t2[{suppliers_t2}]"
        )

    lines.append("[Warehouses]")
    for node in context.warehouses.values():
        plants = ", ".join(context.warehouse_to_plants.get(node.node_id, [])) or "-"
        lines.append(f"- {node.node_id}: {node.name} / {node.location_name or node.region} / plants[{plants}]")

    lines.append("[Tier1 suppliers]")
    for supplier_id, node in context.t1_suppliers.items():
        downstream = ", ".join(context.t1_to_plants.get(supplier_id, [])) or "-"
        lines.append(f"- {supplier_id}: {node.name} / {node.region} / plants[{downstream}]")

    lines.append("[Tier2 suppliers]")
    for supplier_id, node in context.t2_suppliers.items():
        t1_ids = ", ".join(context.t2_to_t1.get(supplier_id, [])) or "-"
        plant_ids = ", ".join(context.t2_to_plants.get(supplier_id, [])) or "-"
        lines.append(f"- {supplier_id}: {node.name} / {node.region} / t1[{t1_ids}] / plants[{plant_ids}]")

    return "\n".join(lines)


def get_master_data() -> SupplyChainContext:
    product_names, product_to_plants = build_product_records()
    plants = build_node_map("plant", product_names, product_to_plants)
    warehouses = build_node_map("warehouse", product_names, product_to_plants)
    suppliers = build_node_map("supplier", product_names, product_to_plants)

    t1_to_plants: dict[str, list[str]] = {}
    plant_to_t1: dict[str, list[str]] = {}
    for plant_id in plants:
        supplier_ids = [supplier_id for supplier_id in get_upstream_node_ids(plant_id, prefix="SUP") if supplier_id in suppliers]
        plant_to_t1[plant_id] = supplier_ids
        for supplier_id in supplier_ids:
            t1_to_plants.setdefault(supplier_id, []).append(plant_id)

    t1_suppliers = {supplier_id: suppliers[supplier_id] for supplier_id in t1_to_plants}

    t2_to_t1: dict[str, list[str]] = {}
    for t1_supplier_id in t1_suppliers:
        for supplier_id in get_upstream_node_ids(t1_supplier_id, prefix="SUP"):
            if supplier_id in suppliers and supplier_id not in t1_suppliers:
                t2_to_t1.setdefault(supplier_id, []).append(t1_supplier_id)

    t2_suppliers = {supplier_id: suppliers[supplier_id] for supplier_id in t2_to_t1}

    t2_to_plants: dict[str, list[str]] = {}
    for supplier_id, downstream_t1 in t2_to_t1.items():
        t2_to_plants[supplier_id] = unique_preserving_order(
            [plant_id for t1_supplier_id in downstream_t1 for plant_id in t1_to_plants.get(t1_supplier_id, [])]
        )

    plant_to_t2: dict[str, list[str]] = {}
    for supplier_id, plant_ids in t2_to_plants.items():
        for plant_id in plant_ids:
            plant_to_t2.setdefault(plant_id, []).append(supplier_id)

    warehouse_to_plants: dict[str, list[str]] = {}
    for warehouse_id in warehouses:
        warehouse_to_plants[warehouse_id] = [
            plant_id
            for plant_id in get_upstream_node_ids(warehouse_id, prefix="PLT")
            if plant_id in plants
        ]

    context = SupplyChainContext(
        plants=plants,
        warehouses=warehouses,
        t1_suppliers=t1_suppliers,
        t2_suppliers=t2_suppliers,
        plant_to_t1={key: unique_preserving_order(value) for key, value in plant_to_t1.items()},
        plant_to_t2={key: unique_preserving_order(value) for key, value in plant_to_t2.items()},
        t1_to_plants={key: unique_preserving_order(value) for key, value in t1_to_plants.items()},
        t2_to_t1={key: unique_preserving_order(value) for key, value in t2_to_t1.items()},
        t2_to_plants={key: unique_preserving_order(value) for key, value in t2_to_plants.items()},
        warehouse_to_plants={key: unique_preserving_order(value) for key, value in warehouse_to_plants.items()},
        summary_text="",
        product_names=product_names,
    )
    context.summary_text = build_master_summary(context)

    logger.info(
        "SupplyChainMaster loaded plants=%d warehouses=%d t1=%d t2=%d products=%d",
        len(plants),
        len(warehouses),
        len(t1_suppliers),
        len(t2_suppliers),
        len(product_names),
    )
    return context


def build_item_text(item: dict[str, Any]) -> str:
    parts = [
        item.get("trend_name", ""),
        item.get("screening_reason", ""),
        item.get("query_used", ""),
        item.get("category_id", ""),
    ]
    for tweet in item.get("sample_tweets", [])[:MAX_SAMPLE_TWEETS]:
        parts.append(tweet.get("text", "")[:MAX_TWEET_TEXT_LENGTH])
    return "\n".join(part for part in parts if part).strip()


def find_keyword_hits(text: str, keywords: list[str]) -> list[str]:
    normalized_text = normalize_text(text)
    hits: list[str] = []
    for keyword in keywords:
        normalized_keyword = normalize_text(keyword)
        if normalized_keyword and normalized_keyword in normalized_text:
            hits.append(keyword)
    return unique_preserving_order(hits)


def match_supply_nodes(text: str, nodes: dict[str, SupplyNode]) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for node in nodes.values():
        location_hits = find_keyword_hits(text, node.keywords)
        product_hits = find_keyword_hits(text, node.products)
        combined_hits = unique_preserving_order(location_hits + product_hits)
        if not combined_hits:
            continue
        matches.append({
            "node_id": node.node_id,
            "node_type": node.node_type,
            "name": node.name,
            "matched_keywords": combined_hits,
            "matched_products": product_hits,
            "score": len(location_hits) * 15 + len(product_hits) * 25,
        })
    return sorted(matches, key=lambda value: value["score"], reverse=True)


def detect_risk_category(text: str, category_hint: str = "") -> dict[str, Any]:
    if category_hint and category_hint in RISK_CATEGORIES:
        category = RISK_CATEGORIES[category_hint]
        matched_keywords = find_keyword_hits(text, category.keywords)
        return {
            "code": category.code,
            "name": category.label,
            "slug": category.slug,
            "matched_keywords": matched_keywords,
            "confidence": min(100, 50 + len(matched_keywords) * 10),
        }

    best = RISK_CATEGORIES["general"]
    best_hits: list[str] = []
    best_score = 0
    for category in RISK_CATEGORIES.values():
        if category.code == "general":
            continue
        hits = find_keyword_hits(text, category.keywords)
        score = len(hits) * 20 + category.base_severity
        if score > best_score:
            best = category
            best_hits = hits
            best_score = score

    if not best_hits:
        best = RISK_CATEGORIES["general"]

    return {
        "code": best.code,
        "name": best.label,
        "slug": best.slug,
        "matched_keywords": best_hits,
        "confidence": min(100, 20 + len(best_hits) * 18 + best.base_severity),
    }


def extract_item_time(item: dict[str, Any]) -> str:
    for tweet in item.get("sample_tweets", []):
        created_at = tweet.get("created_at", "")
        if created_at:
            return created_at
    return item.get("fetched_at", "")


def calculate_authenticity_score(item: dict[str, Any], risk_category: dict[str, Any], mention_score: int) -> int:
    sample_tweets = item.get("sample_tweets", [])[:MAX_SAMPLE_TWEETS]
    tweet_count = int(item.get("tweet_count", 0) or 0)
    unique_authors = len({tweet.get("author_id") for tweet in sample_tweets if tweet.get("author_id")})
    url_count = sum(tweet.get("text", "").count("http") for tweet in sample_tweets)
    keyword_bonus = min(15, len(risk_category["matched_keywords"]) * 5)
    mention_bonus = min(20, mention_score // 5)

    score = (
        20
        + min(20, tweet_count * 2)
        + min(20, len(sample_tweets) * 4)
        + min(15, unique_authors * 5)
        + min(10, url_count * 3)
        + keyword_bonus
        + mention_bonus
    )
    return max(0, min(100, score))


def calculate_impact_score(
    risk_category: dict[str, Any],
    matched_plants: list[dict[str, Any]],
    matched_warehouses: list[dict[str, Any]],
    matched_t1: list[dict[str, Any]],
    matched_t2: list[dict[str, Any]],
    candidate_plants: list[str],
    candidate_t1: list[str],
    candidate_t2: list[str],
) -> int:
    category = RISK_CATEGORIES.get(risk_category["code"], RISK_CATEGORIES["general"])
    score = category.base_severity
    score += min(30, len(matched_plants) * 20)
    score += min(15, len(matched_warehouses) * 10)
    score += min(18, len(matched_t1) * 9)
    score += min(12, len(matched_t2) * 6)
    score += min(10, len(candidate_plants) * 4)
    score += min(8, len(candidate_t1) * 3)
    score += min(6, len(candidate_t2) * 2)
    return max(0, min(100, score))


def format_reason(
    risk_category: dict[str, Any],
    matched_plants: list[dict[str, Any]],
    matched_warehouses: list[dict[str, Any]],
    matched_t1: list[dict[str, Any]],
    matched_t2: list[dict[str, Any]],
) -> str:
    reasons: list[str] = []
    if risk_category["name"]:
        if risk_category["matched_keywords"]:
            reasons.append(f"{risk_category['name']}を示す語句: {', '.join(risk_category['matched_keywords'][:4])}")
        else:
            reasons.append(f"{risk_category['name']}として分類")
    if matched_plants:
        reasons.append(f"工場一致: {', '.join(match['node_id'] for match in matched_plants[:3])}")
    if matched_warehouses:
        reasons.append(f"倉庫一致: {', '.join(match['node_id'] for match in matched_warehouses[:2])}")
    supplier_ids = [match["node_id"] for match in (matched_t1[:2] + matched_t2[:2])]
    if supplier_ids:
        reasons.append(f"供給網一致: {', '.join(supplier_ids)}")
    return " / ".join(reasons) if reasons else "供給網との直接一致は弱く、監視継続レベル"


def build_stage2_result(item: dict[str, Any], text: str, context: SupplyChainContext) -> dict[str, Any]:
    matched_plants = match_supply_nodes(text, context.plants)
    matched_warehouses = match_supply_nodes(text, context.warehouses)
    matched_t1 = match_supply_nodes(text, context.t1_suppliers)
    matched_t2 = match_supply_nodes(text, context.t2_suppliers)
    risk_category = detect_risk_category(text, item.get("category_id", ""))

    candidate_plants = [match["node_id"] for match in matched_plants]
    for warehouse_id in [match["node_id"] for match in matched_warehouses]:
        candidate_plants.extend(context.warehouse_to_plants.get(warehouse_id, []))

    candidate_t1 = [match["node_id"] for match in matched_t1]
    candidate_t2 = [match["node_id"] for match in matched_t2]

    for plant_id in unique_preserving_order(candidate_plants):
        candidate_t1.extend(context.plant_to_t1.get(plant_id, []))
        candidate_t2.extend(context.plant_to_t2.get(plant_id, []))

    for supplier_id in unique_preserving_order(candidate_t1):
        candidate_plants.extend(context.t1_to_plants.get(supplier_id, []))

    for supplier_id in unique_preserving_order(candidate_t2):
        candidate_t1.extend(context.t2_to_t1.get(supplier_id, []))
        candidate_plants.extend(context.t2_to_plants.get(supplier_id, []))

    candidate_plants = unique_preserving_order(candidate_plants)
    candidate_t1 = unique_preserving_order(candidate_t1)
    candidate_t2 = unique_preserving_order(candidate_t2)

    candidate_paths = [
        f"{supplier_id} -> {t1_supplier_id} -> {plant_id}"
        for supplier_id in candidate_t2
        for t1_supplier_id in context.t2_to_t1.get(supplier_id, [])
        for plant_id in context.t1_to_plants.get(t1_supplier_id, [])
        if plant_id in candidate_plants
    ]
    candidate_paths.extend(
        f"{supplier_id} -> {plant_id}"
        for supplier_id in candidate_t1
        for plant_id in context.t1_to_plants.get(supplier_id, [])
        if plant_id in candidate_plants
    )
    candidate_paths = unique_preserving_order(candidate_paths)

    mention_score = min(
        100,
        sum(match["score"] for match in matched_plants[:2])
        + sum(match["score"] for match in matched_warehouses[:2])
        + sum(match["score"] for match in matched_t1[:2])
        + sum(match["score"] for match in matched_t2[:2]),
    )
    authenticity_score = calculate_authenticity_score(item, risk_category, mention_score)
    impact_score = calculate_impact_score(
        risk_category,
        matched_plants,
        matched_warehouses,
        matched_t1,
        matched_t2,
        candidate_plants,
        candidate_t1,
        candidate_t2,
    )
    total_score = min(100, round(authenticity_score * 0.45 + impact_score * 0.55))

    return {
        "status": "ready",
        "time": extract_item_time(item),
        "risk_category": risk_category,
        "matched_plants": matched_plants,
        "matched_warehouses": matched_warehouses,
        "matched_t1_suppliers": matched_t1,
        "matched_t2_suppliers": matched_t2,
        "candidate_plants": candidate_plants,
        "candidate_t1_suppliers": candidate_t1,
        "candidate_t2_suppliers": candidate_t2,
        "candidate_paths": candidate_paths,
        "entity_mention_score": mention_score,
        "authenticity_score": authenticity_score,
        "impact_score": impact_score,
        "heuristic_score": total_score,
        "ai_candidate": ENABLE_AI_CLASSIFICATION and bool(candidate_plants or candidate_t1 or candidate_t2 or candidate_paths),
    }


def build_ai_payload(items: list[dict[str, Any]], context: SupplyChainContext) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for item in items:
        stage2 = item["stage2"]
        payload.append({
            "id": item["classification_id"],
            "trend_name": item.get("trend_name", ""),
            "source": item.get("source", ""),
            "tweet_count": item.get("tweet_count", 0),
            "time": stage2["time"],
            "event_text": build_item_text(item),
            "classification_hint": stage2["risk_category"]["name"],
            "classification_slug_hint": stage2["risk_category"]["slug"],
            "candidate_plants": stage2["candidate_plants"],
            "candidate_t1_suppliers": stage2["candidate_t1_suppliers"],
            "candidate_t2_suppliers": stage2["candidate_t2_suppliers"],
            "candidate_paths": stage2["candidate_paths"][:8],
            "authenticity_score": stage2["authenticity_score"],
            "impact_score": stage2["impact_score"],
        })
    return payload


def invoke_ai_classifier(items: list[dict[str, Any]], context: SupplyChainContext) -> dict[str, dict[str, Any]]:
    if not items or not ENABLE_AI_CLASSIFICATION:
        return {}

    payload = build_ai_payload(items, context)
    response = bedrock_runtime.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4096,
            "system": [
                {"type": "text", "text": AI_SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}},
                {"type": "text", "text": context.summary_text, "cache_control": {"type": "ephemeral"}},
            ],
            "messages": [{
                "role": "user",
                "content": f"Classify these events:\n{json.dumps(payload, ensure_ascii=False)}",
            }],
        }, ensure_ascii=False),
    )

    body = json.loads(response["body"].read())
    text = body["content"][0]["text"].strip()
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError(f"AI response did not contain JSON: {text[:200]}")

    result = json.loads(match.group())
    return {row["id"]: row for row in result.get("results", []) if row.get("id")}


def build_stage3_result(item: dict[str, Any], ai_result: dict[str, Any] | None) -> dict[str, Any]:
    stage2 = item["stage2"]

    base_result = {
        "time": stage2["time"],
        "score": stage2["heuristic_score"],
        "authenticity_score": stage2["authenticity_score"],
        "impact_score": stage2["impact_score"],
        "reason": format_reason(
            stage2["risk_category"],
            stage2["matched_plants"],
            stage2["matched_warehouses"],
            stage2["matched_t1_suppliers"],
            stage2["matched_t2_suppliers"],
        ),
        "used_ai": False,
        "model_id": None,
        "impacted_plants": stage2["candidate_plants"],
        "impacted_suppliers": unique_preserving_order(stage2["candidate_t1_suppliers"] + stage2["candidate_t2_suppliers"]),
        "classification_code": stage2["risk_category"]["name"],
        "classification_slug": stage2["risk_category"]["slug"],
    }

    if ai_result is None:
        return base_result

    return {
        **base_result,
        "time": ai_result.get("time") or base_result["time"],
        "score": int(ai_result.get("score", base_result["score"])),
        "reason": ai_result.get("reason") or base_result["reason"],
        "used_ai": True,
        "model_id": BEDROCK_MODEL_ID,
        "impacted_plants": unique_preserving_order(ai_result.get("impacted_plants", base_result["impacted_plants"])),
        "impacted_suppliers": unique_preserving_order(ai_result.get("impacted_suppliers", base_result["impacted_suppliers"])),
        "classification_code": ai_result.get("classification_code") or base_result["classification_code"],
        "classification_slug": ai_result.get("classification_slug") or base_result["classification_slug"],
    }


def determine_final_label(stage3: dict[str, Any]) -> str:
    if stage3["score"] >= 70:
        return "high"
    if stage3["score"] >= 40:
        return "medium"
    return "low"


def build_processed_key(raw_key: str, classification_slug: str | None = None) -> str:
    if raw_key.startswith("raw/"):
        suffix = raw_key[len("raw/"):]
    else:
        suffix = raw_key
    if classification_slug:
        return f"processed/{classification_slug}/{suffix}"
    return f"processed/{suffix}"


def classify_items(items: list[dict[str, Any]], context: SupplyChainContext) -> tuple[list[dict[str, Any]], dict[str, int]]:
    enriched_items: list[dict[str, Any]] = []
    ai_candidates: list[dict[str, Any]] = []

    for index, item in enumerate(items, 1):
        text = build_item_text(item)
        stage2 = build_stage2_result(item, text, context)
        enriched = {**item, "classification_id": f"item-{index}", "stage2": stage2}
        enriched_items.append(enriched)
        if stage2["ai_candidate"]:
            ai_candidates.append(enriched)

    ai_results: dict[str, dict[str, Any]] = {}
    for start in range(0, len(ai_candidates), AI_BATCH_SIZE):
        batch = ai_candidates[start:start + AI_BATCH_SIZE]
        try:
            ai_results.update(invoke_ai_classifier(batch, context))
        except Exception as exc:
            logger.warning("AI classification failed, fallback to heuristic scoring: %s", exc)

    counts = {"high": 0, "medium": 0, "low": 0}
    for item in enriched_items:
        stage3 = build_stage3_result(item, ai_results.get(item["classification_id"]))
        final_label = determine_final_label(stage3)
        item["stage3"] = stage3
        item["final_label"] = final_label
        counts[final_label] += 1

    return enriched_items, counts


def format_node_refs(node_ids: list[str], nodes: dict[str, SupplyNode]) -> list[str]:
    return [f"{node_id}:{nodes[node_id].name}" for node_id in node_ids if node_id in nodes]


def build_output_item(item: dict[str, Any], context: SupplyChainContext) -> dict[str, Any]:
    supplier_nodes = {**context.t1_suppliers, **context.t2_suppliers}
    stage3 = item["stage3"]
    return {
        "time": stage3.get("time", ""),
        "reason": stage3.get("reason", ""),
        "impacted_plants": format_node_refs(stage3.get("impacted_plants", []), context.plants),
        "impacted_suppliers": format_node_refs(stage3.get("impacted_suppliers", []), supplier_nodes),
        "classification_code": stage3.get("classification_code", ""),
        "score": stage3.get("score", 0),
    }


def group_items_by_classification(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        slug = item["stage3"].get("classification_slug", "general") or "general"
        grouped.setdefault(slug, []).append(item)
    return grouped


def process_s3_record(record: dict[str, Any], context: SupplyChainContext) -> dict[str, Any]:
    bucket = record["s3"]["bucket"]["name"]
    raw_key = unquote_plus(record["s3"]["object"]["key"])

    if not raw_key.endswith(".json") or not raw_key.startswith("raw/"):
        logger.info("Skip non-target S3 key: s3://%s/%s", bucket, raw_key)
        return {"bucket": bucket, "raw_key": raw_key, "skipped": True}

    raw_data = load_json_from_s3(bucket, raw_key)
    raw_items = raw_data.get("items", [])
    for raw_item in raw_items:
        raw_item.setdefault("fetched_at", raw_data.get("fetched_at", ""))

    classified_items, counts = classify_items(raw_items, context)
    grouped_items = group_items_by_classification(classified_items)

    processed_keys: list[str] = []
    for slug, grouped in grouped_items.items():
        processed_key = build_processed_key(raw_key, slug)
        processed_data = {
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "bucket": bucket,
            "raw_key": raw_key,
            "source": raw_data.get("source", ""),
            "fetched_at": raw_data.get("fetched_at", ""),
            "classification": slug,
            "item_count": len(grouped),
            "label_counts": {
                "high": sum(1 for item in grouped if item["final_label"] == "high"),
                "medium": sum(1 for item in grouped if item["final_label"] == "medium"),
                "low": sum(1 for item in grouped if item["final_label"] == "low"),
            },
            "items": [build_output_item(item, context) for item in grouped],
        }
        save_json_to_s3(processed_data, processed_key, bucket_name=bucket)
        processed_keys.append(processed_key)

    logger.info(
        "Classification complete raw=%s processed=%s labels=%s",
        raw_key,
        json.dumps(processed_keys, ensure_ascii=False),
        json.dumps(counts, ensure_ascii=False),
    )
    return {
        "bucket": bucket,
        "raw_key": raw_key,
        "processed_keys": processed_keys,
        "item_count": len(classified_items),
        "label_counts": counts,
    }


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    logger.info("Classification Processor start: %s", json.dumps(event, ensure_ascii=False))
    supply_chain_context = get_master_data()

    results = [process_s3_record(record, supply_chain_context) for record in event.get("Records", [])]
    processed_files = sum(len(result.get("processed_keys", [])) for result in results if not result.get("skipped"))

    output = {
        "processed_records": len(results),
        "processed_files": processed_files,
        "results": results,
        "bucket_name": BUCKET_NAME,
    }
    logger.info("Classification Processor done: %s", json.dumps(output, ensure_ascii=False))
    return output

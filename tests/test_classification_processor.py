# ruff: noqa: I001

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "layers", "common"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "function", "classification_processor"))

os.environ.setdefault("BUCKET_NAME", "test-bucket")
os.environ.setdefault("AWS_DEFAULT_REGION", "ap-northeast-1")
os.environ.setdefault("SUPPLY_CHAIN_TABLE_NAME", "SupplyChainMaster")
os.environ.setdefault("ENABLE_AI_CLASSIFICATION", "true")

from classification_processor_function import (  # noqa: E402
    SupplyChainContext,
    SupplyNode,
    build_processed_key,
    build_stage2_result,
    determine_final_label,
    lambda_handler,
    normalize_score_for_evidence,
)


PLANTS = {
    "PLT008": SupplyNode(
        "PLT008",
        "plant",
        "Sendai Plant",
        "Sendai, Miyagi",
        "Miyagi",
        "Japan",
        ["PLT008", "Sendai Plant", "Sendai", "Miyagi"],
        ["Sensor Module", "Battery Module"],
        ["Battery Module"],
        ["Sensor Module"],
    ),
    "PLT009": SupplyNode(
        "PLT009",
        "plant",
        "Yokohama Plant",
        "Yokohama, Kanagawa",
        "Kanagawa",
        "Japan",
        ["PLT009", "Yokohama Plant", "Yokohama", "Kanagawa"],
        ["Finished Unit", "Motor Unit"],
        ["Finished Unit"],
        ["Motor Unit"],
    ),
}

WAREHOUSES = {
    "WHS005": SupplyNode(
        "WHS005",
        "warehouse",
        "Sendai Warehouse",
        "Sendai, Miyagi",
        "Miyagi",
        "Japan",
        ["WHS005", "Sendai Warehouse", "Sendai", "Miyagi"],
    ),
}

T1_SUPPLIERS = {
    "SUP003": SupplyNode(
        "SUP003",
        "supplier",
        "Iwate Metals",
        "",
        "Iwate",
        "Japan",
        ["SUP003", "Iwate Metals", "Iwate"],
        ["Battery Module", "Raw Resin"],
        ["Battery Module"],
        ["Raw Resin"],
    ),
    "SUP007": SupplyNode(
        "SUP007",
        "supplier",
        "Kanagawa Components",
        "",
        "Kanagawa",
        "Japan",
        ["SUP007", "Kanagawa Components", "Kanagawa"],
        ["Motor Unit"],
        ["Motor Unit"],
        [],
    ),
}

T2_SUPPLIERS = {
    "SUP102": SupplyNode(
        "SUP102",
        "supplier",
        "Hokuriku Resin",
        "",
        "Ishikawa",
        "Japan",
        ["SUP102", "Hokuriku Resin", "Ishikawa"],
        ["Raw Resin"],
        ["Raw Resin"],
        [],
    ),
}

CONTEXT = SupplyChainContext(
    plants=PLANTS,
    warehouses=WAREHOUSES,
    t1_suppliers=T1_SUPPLIERS,
    t2_suppliers=T2_SUPPLIERS,
    plant_to_t1={"PLT008": ["SUP003"], "PLT009": ["SUP007"]},
    plant_to_t2={"PLT008": ["SUP102"], "PLT009": []},
    t1_to_plants={"SUP003": ["PLT008"], "SUP007": ["PLT009"]},
    t2_to_t1={"SUP102": ["SUP003"]},
    t2_to_plants={"SUP102": ["PLT008"]},
    warehouse_to_plants={"WHS005": ["PLT008"]},
    summary_text="mocked supply chain summary",
)


def test_stage2_builds_candidate_chain_from_t2_supplier():
    item = {
        "trend_name": "Resin supplier disruption",
        "source": "trends_route",
        "sample_tweets": [
            {"text": "Hokuriku Resin in Ishikawa stopped operations and may affect upstream supply."},
        ],
    }

    result = build_stage2_result(item, "Hokuriku Resin in Ishikawa stopped operations.", CONTEXT)

    assert result["status"] == "ready"
    assert result["ai_candidate"] is True
    assert "SUP102" in result["candidate_t2_suppliers"]
    assert "SUP003" in result["candidate_t1_suppliers"]
    assert "PLT008" in result["candidate_plants"]
    assert "SUP102 -> SUP003 -> PLT008" in result["candidate_paths"]


def test_stage2_matches_plant_by_product():
    item = {
        "trend_name": "Battery module shortage",
        "source": "trends_route",
        "sample_tweets": [
            {"text": "Battery Module shortage is affecting production in Sendai."},
        ],
    }

    result = build_stage2_result(item, "Battery Module shortage is affecting production in Sendai.", CONTEXT)

    assert "PLT008" in result["candidate_plants"]
    assert result["matched_plants"][0]["matched_products"] == ["Battery Module"]
    assert result["matched_plants"][0]["matched_produced_products"] == ["Battery Module"]
    assert result["matched_plants"][0]["matched_consumed_products"] == []


def test_produced_product_hit_scores_higher_than_consumed_product_hit():
    supplier_result = build_stage2_result(
        {
            "trend_name": "Battery module output stopped",
            "source": "trends_route",
            "sample_tweets": [{"text": "Iwate Metals battery module line stopped."}],
        },
        "Iwate Metals battery module line stopped.",
        CONTEXT,
    )
    plant_result = build_stage2_result(
        {
            "trend_name": "Sensor module shortage",
            "source": "trends_route",
            "sample_tweets": [{"text": "Sendai Plant is short on Sensor Module input."}],
        },
        "Sendai Plant is short on Sensor Module input.",
        CONTEXT,
    )

    supplier_match = next(match for match in supplier_result["matched_t1_suppliers"] if match["node_id"] == "SUP003")
    plant_match = next(match for match in plant_result["matched_plants"] if match["node_id"] == "PLT008")

    assert supplier_match["matched_produced_products"] == ["Battery Module"]
    assert supplier_match["matched_consumed_products"] == []
    assert plant_match["matched_produced_products"] == []
    assert plant_match["matched_consumed_products"] == ["Sensor Module"]
    assert supplier_match["score"] > plant_match["score"]


def test_high_authenticity_event_without_direct_node_match_still_goes_to_ai():
    item = {
        "trend_name": "Gasoline price surge",
        "tweet_count": 10,
        "source": "trends_route",
        "sample_tweets": [
            {"text": "Gasoline prices surged again and logistics costs are rising.", "author_id": "a1"},
            {"text": "Fuel costs are hitting transport and manufacturing nationwide.", "author_id": "a2"},
            {"text": "Gasoline and energy prices are rising sharply. https://example.com", "author_id": "a3"},
        ],
    }

    result = build_stage2_result(item, "Gasoline prices surged again and logistics costs are rising.", CONTEXT)

    assert result["candidate_plants"] == []
    assert result["candidate_t1_suppliers"] == []
    assert result["candidate_t2_suppliers"] == []
    assert result["ai_candidate"] is True


def test_low_evidence_ai_result_without_impacted_nodes_gets_capped():
    stage3 = normalize_score_for_evidence({
        "score": 58,
        "used_ai": True,
        "reason": "交通事故・道路障害を示す語句: 事故, 遅延, 交通障害, 道路",
        "impacted_plants": [],
        "impacted_suppliers": [],
    })
    assert stage3["score"] == 25


def test_indirect_ai_rationale_without_impacted_nodes_can_keep_moderate_score():
    stage3 = normalize_score_for_evidence({
        "score": 72,
        "used_ai": True,
        "reason": "燃料高騰により物流コストと石化系部材コストが上昇し、間接的にサプライチェーンへ影響する可能性がある",
        "impacted_plants": [],
        "impacted_suppliers": [],
    })
    assert stage3["score"] == 55


def test_determine_final_label():
    assert determine_final_label({"score": 85}) == "high"
    assert determine_final_label({"score": 55}) == "medium"
    assert determine_final_label({"score": 10}) == "low"


def test_build_processed_key():
    assert build_processed_key("raw/2026-03-10/01ABC.json") == "processed/2026-03-10/01ABC.json"
    assert build_processed_key("raw/2026-03-10/01ABC.json", "traffic") == "processed/traffic/2026-03-10/01ABC.json"


def test_lambda_handler_with_fixture_end_to_end(monkeypatch):
    fixture_path = Path(__file__).resolve().parents[1] / "docs" / "01KK926N6HN9BVXPJ8XVKYY5B7.json"
    raw_payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    saved: list[dict] = []

    def fake_get_master_data():
        return CONTEXT

    def fake_load_json_from_s3(bucket_name, key):
        assert bucket_name == "fixture-bucket"
        assert key == "raw/2026-03-09/01KK926N6HN9BVXPJ8XVKYY5B7.json"
        return raw_payload

    def fake_save_json_to_s3(data, key, bucket_name=None, client=None):
        saved.append({"data": data, "key": key, "bucket_name": bucket_name})
        return key

    def fake_invoke_ai_classifier(items, context):
        assert context.summary_text == "mocked supply chain summary"
        results = {}
        for item in items:
            if "gasoline" in item["trend_name"].lower() or "traffic" in item["trend_name"].lower():
                results[item["classification_id"]] = {
                    "id": item["classification_id"],
                    "time": "2026-03-09T10:27:23.000Z",
                    "score": 88,
                    "reason": "Traffic disruption may affect SUP102 -> SUP003 -> PLT008.",
                    "impacted_plants": ["PLT008"],
                    "impacted_suppliers": ["SUP003", "SUP102"],
                    "classification_code": "交通事故・道路障害",
                    "classification_slug": "traffic",
                }
        return results

    monkeypatch.setattr("classification_processor_function.get_master_data", fake_get_master_data)
    monkeypatch.setattr("classification_processor_function.load_json_from_s3", fake_load_json_from_s3)
    monkeypatch.setattr("classification_processor_function.save_json_to_s3", fake_save_json_to_s3)
    monkeypatch.setattr("classification_processor_function.invoke_ai_classifier", fake_invoke_ai_classifier)

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "fixture-bucket"},
                    "object": {"key": "raw/2026-03-09/01KK926N6HN9BVXPJ8XVKYY5B7.json"},
                }
            }
        ]
    }

    result = lambda_handler(event, None)

    assert result["processed_records"] == 1
    assert result["processed_files"] >= 1
    assert saved
    assert all(entry["bucket_name"] == "fixture-bucket" for entry in saved)
    assert any(entry["key"].startswith("processed/") for entry in saved)

    traffic_payloads = [entry for entry in saved if "/traffic/" in entry["key"]]
    assert traffic_payloads

    traffic_item = traffic_payloads[0]["data"]["items"][0]
    assert set(traffic_item.keys()) == {
        "time",
        "reason",
        "impacted_plants",
        "impacted_suppliers",
        "classification_code",
        "score",
    }

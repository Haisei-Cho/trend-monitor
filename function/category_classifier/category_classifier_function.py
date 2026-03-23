"""カテゴリ分類 Lambda関数。

S3イベントトリガーで起動し、raw JSONを読み込み、
S3ノードインデックスキャッシュを参照してサプライチェーン関連メッセージを分類・要約する。

処理フロー:
    1. S3からraw JSONを読み込み
    2. S3からノードインデックス（config/node_location_index.json）を読み込み
    3. itemsを10件ずつバッチ分割
    4. Bedrock AIでノードマッチング+分類を一括実行
    5. 分類結果をS3に保存（classified/{category}/{date}/{ulid}.json）
"""

import json
import os
from datetime import datetime, timezone
from typing import Any

import boto3
from aws_lambda_powertools.utilities.data_classes import (
    S3EventBridgeNotificationEvent,
    event_source,
)

from log_utils import setup_logger
from utils import generate_classified_s3_key, generate_ulid, serialize_json

logger = setup_logger("category_classifier")

BUCKET_NAME = os.environ["BUCKET_NAME"]
BEDROCK_MODEL_ID = "jp.anthropic.claude-sonnet-4-6"
NODE_INDEX_S3_KEY = "config/node_location_index.json"

s3_client = boto3.client("s3")
bedrock_runtime = boto3.client("bedrock-runtime")

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

BATCH_SIZE = 10

CLASSIFICATION_PROMPT_TEMPLATE = """あなたはサプライチェーンリスク分析の専門家です。
以下のサプライチェーンノード一覧とツイート群を分析し、
各itemがどのノードの生産活動に影響するかを判定し、関連するもののみ分類してください。

【サプライチェーンノード一覧】
各ノードの名称・所在地・生産品目を参考に、ツイートとの関連性を判定してください。
名称の完全一致だけでなく、略称・地域名・間接的な言及も考慮すること。
ただし、地名の一致だけでは不十分です。実際の生産・物流への影響があるか判断してください。

{node_list}

【リスクカテゴリ】
earthquake: 地震・津波, flood: 風水害, fire: 火災・爆発, traffic: 交通障害,
infra: 停電・インフラ障害, labor: 労務・操業リスク, geopolitics: 地政学・貿易, pandemic: 感染症

【出力形式】JSON配列のみ（マークダウンコードブロック不要）:
[
  {{
    "item_index": 0,
    "related_node_ids": ["PLT001", "SUP003"],
    "category_id": "8カテゴリのいずれか（入力にcategory_idがある場合はそのまま使用）",
    "summary": "何が発生したかの要約（100字以内。影響を受ける拠点の生産製品にも言及すること）",
    "ai_confidence": 0-100の整数（情報の確実性）,
    "reasoning": "信頼度スコアの根拠（50字以内）"
  }}
]

関連なしのitemは配列に含めないでください。
全itemが無関係な場合は空配列 [] を返してください。

【信頼度スコア基準】
- 90-100: 公式発表・複数の信頼性の高いソースで確認済み
- 70-89: 複数のツイートで一致、具体的な情報あり
- 50-69: 限られた情報ソース、詳細不明確
- 30-49: 噂レベル、未確認情報
- 0-29: 信頼性が極めて低い、誤情報の可能性

【重要】
- ツイート内容が拠点名を含んでいても、その拠点の生産活動に実際に影響しない場合は含めないこと
- 元社員の話題、比喩的な言及などは除外すること
"""


def _extract_json_array(text: str) -> list[dict] | None:
    """テキストからJSON配列を抽出する（ブラケットカウント方式）。"""
    start = text.find("[")
    if start == -1:
        return None

    depth = 0
    in_string = False
    escape_next = False
    for i in range(start, len(text)):
        c = text[i]
        if escape_next:
            escape_next = False
            continue
        if c == "\\":
            escape_next = True
            continue
        if c == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                try:
                    result = json.loads(text[start:i + 1])
                    return result if isinstance(result, list) else None
                except json.JSONDecodeError:
                    return None
    return None


def build_system_prompt(nodes: list[dict]) -> str:
    """ノード一覧を含むsystem promptを動的に構築する。"""
    node_lines = []
    for n in nodes:
        line = f"ID: {n['id']} | {n['name']} | {n['node_type']}"
        if n.get("tier"):
            line += f"({n['tier']})"
        loc = n.get("location_name", "")
        if loc:
            line += f" | 所在地: {loc}"
        products = n.get("products", [])
        if products:
            line += f" | 生産: {'、'.join(products)}"
        node_lines.append(line)

    return CLASSIFICATION_PROMPT_TEMPLATE.format(
        node_list="\n".join(node_lines)
    )


def load_s3_data(bucket: str, key: str) -> dict[str, Any]:
    """S3からJSONを読み込みパースする。"""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def load_node_index(bucket: str) -> list[dict[str, Any]]:
    """S3からノードインデックスを読み込む。

    NodeIndexGenerator Lambda が日次生成する config/node_location_index.json を読み込む。

    Returns:
        ノードリスト [{"id": "PLT001", "name": "...", "node_type": "plant", ...}, ...]
    """
    try:
        data = load_s3_data(bucket, NODE_INDEX_S3_KEY)
    except s3_client.exceptions.NoSuchKey:
        logger.error(f"ノードインデックス未生成: s3://{bucket}/{NODE_INDEX_S3_KEY}")
        return []

    nodes = data.get("nodes", [])
    logger.info(f"ノードインデックス読込完了: {len(nodes)}ノード（generated_at={data.get('generated_at', 'unknown')}）")
    return nodes



def classify_batch(
    items: list[dict[str, Any]],
    batch_start_index: int,
    system_prompt: str,
    node_map: dict[str, dict],
) -> list[dict[str, Any]]:
    """バッチ単位でBedrockを呼び出し、マッチング+分類を一体で行う。

    Returns:
        分類結果のリスト。各要素にrelated_nodes（完全なnode dict）を含む。
    """
    # user messageにitemsのバッチを送信
    items_for_prompt = []
    for i, item in enumerate(items):
        items_for_prompt.append({
            "item_index": batch_start_index + i,
            "trend_name": item.get("trend_name", ""),
            "source": item.get("source", ""),
            "category_id": item.get("category_id"),
            "sample_tweets": [t.get("text", "") for t in item.get("sample_tweets", [])],
        })

    user_content = json.dumps(items_for_prompt, ensure_ascii=False)

    response = bedrock_runtime.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4096,
            "system": [{
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }],
            "messages": [{
                "role": "user",
                "content": user_content,
            }],
        }, ensure_ascii=False),
    )

    body = json.loads(response["body"].read())
    text = body["content"][0]["text"].strip()

    # JSON配列を抽出（ブラケットカウント方式でネスト対応）
    results = _extract_json_array(text)
    if results is None:
        logger.warning(f"Bedrockレスポンス解析失敗: {text[:200]}")
        return []

    # related_node_idsを完全なnode dictに変換
    for result in results:
        node_ids = result.pop("related_node_ids", [])
        result["related_nodes"] = [
            node_map[nid] for nid in node_ids if nid in node_map
        ]

    return results


def save_classified_to_s3(classified_data: dict[str, Any]) -> str:
    """分類結果をS3に保存する。"""
    category_id = classified_data["category_id"]
    s3_key = generate_classified_s3_key(category_id)

    body = serialize_json(classified_data)
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"分類結果保存: s3://{BUCKET_NAME}/{s3_key}")
    return s3_key


@event_source(data_class=S3EventBridgeNotificationEvent)
def lambda_handler(event: S3EventBridgeNotificationEvent, context: Any) -> dict:
    """Lambda関数エントリーポイント。

    EventBridge S3通知 → raw JSON読み込み → バッチ分割 → AIマッチング+分類 → S3保存
    """
    bucket = event.detail.bucket.name
    raw_s3_key = event.detail.object.key

    logger.info(f"入力ファイル: s3://{bucket}/{raw_s3_key}")

    # 1. S3からraw JSONを読み込み
    raw_data = load_s3_data(bucket, raw_s3_key)
    items = raw_data.get("items", [])
    source = raw_data.get("source", "unknown")

    if not items:
        logger.info("itemsが空のため処理スキップ")
        return {"classified_count": 0}

    # 2. S3からノードインデックスを読み込み
    nodes = load_node_index(bucket)
    if not nodes:
        logger.warning("ノードインデックスが空のため分類スキップ")
        return {"classified_count": 0}
    node_map = {n["id"]: n for n in nodes}

    # 3. system prompt構築（ノード一覧を含む）
    system_prompt = build_system_prompt(nodes)

    # 4. itemsをバッチ分割してAIマッチング+分類
    classified_count = 0
    classified_keys: list[str] = []

    for i in range(0, len(items), BATCH_SIZE):
        batch = items[i:i + BATCH_SIZE]

        try:
            results = classify_batch(batch, i, system_prompt, node_map)
        except Exception as e:
            logger.warning(f"Bedrock呼び出しエラー（バッチ {i}-{i + len(batch)}）: {e}")
            continue

        for result in results:
            category_id = result.get("category_id", "unknown")
            if category_id not in RISK_CATEGORIES:
                logger.warning(f"不明なカテゴリ: {category_id}")
                continue

            # 元のitemからtrend_nameを取得
            item_index = result.get("item_index", -1)
            original_item = items[item_index] if 0 <= item_index < len(items) else {}

            now = datetime.now(timezone.utc)
            classified_data = {
                "event_id": generate_ulid(),
                "classified_at": now.isoformat(),
                "source": source,
                "category_id": category_id,
                "category_name": RISK_CATEGORIES[category_id],
                "raw_s3_key": raw_s3_key,
                "trend_name": original_item.get("trend_name", result.get("trend_name", "")),
                "summary": result.get("summary", ""),
                "ai_confidence": result.get("ai_confidence", 0),
                "reasoning": result.get("reasoning", ""),
                "related_nodes": result.get("related_nodes", []),
            }

            s3_key = save_classified_to_s3(classified_data)
            classified_keys.append(s3_key)
            classified_count += 1

    output = {
        "raw_s3_key": raw_s3_key,
        "source": source,
        "total_items": len(items),
        "classified_count": classified_count,
        "classified_keys": classified_keys,
    }

    logger.info(f"カテゴリ分類完了: {json.dumps(output, ensure_ascii=False)}")
    return output
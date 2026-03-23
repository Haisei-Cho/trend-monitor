"""2段AIパイプライン共通ロジック。

Stage 1 (Haiku): 重要度フィルタ。factデータのみを入力し、SCMリスクとして意味があるか判定。
Stage 2 (Sonnet): ノードマッチ。Stage 1をpassしたデータ+ノードインデックスを入力し、影響拠点を特定。
"""

import json
import re

import boto3

from log_utils import setup_logger

logger = setup_logger("fact_matcher")

bedrock_runtime = boto3.client("bedrock-runtime")

HAIKU_MODEL_ID = "anthropic.claude-haiku-4-5-20251001"
SONNET_MODEL_ID = "jp.anthropic.claude-sonnet-4-6"

# ─── Stage 1: 重要度フィルタ（Haiku） ───

STAGE1_SYSTEM_PROMPT = """あなたはサプライチェーンリスクのスクリーニング担当です。
以下の情報が、日本国内の製造業サプライチェーンに
影響を与える可能性があるか判定してください。

【判定基準】
・地震: 震度3以上、または津波警報・注意報 → pass
・台風: 上陸予想または暴風域接近 → pass
・道路: 通行止め、大規模車線規制 → pass
       速度規制、チェーン規制のみ → skip
・ニュース: 工場・港湾・物流・インフラへの実害を示唆する記事 → pass
          一般的な報道、被害報告なし → skip
・公式SNS: 災害・規制・障害に関する具体的な発表 → pass
           定期報告・広報・お知らせ → skip

迷ったら pass にしてください（見逃しより過検出のほうが安全）。

【出力形式】JSON配列のみ:
[
  {"fact_index": 0, "decision": "pass", "reason": "震度4、操業影響の可能性"},
  {"fact_index": 1, "decision": "skip", "reason": "震度1、影響なし"}
]"""


def invoke_stage1(facts: list[dict]) -> list[dict]:
    """Stage 1 Haiku: 重要度フィルタを実行する。

    Args:
        facts: ファクトデータの配列（各要素にfact_indexを含む）

    Returns:
        [{"fact_index": int, "decision": "pass"|"skip", "reason": str}, ...]
    """
    if not facts:
        return []

    user_content = json.dumps(facts, ensure_ascii=False)

    response = bedrock_runtime.invoke_model(
        modelId=HAIKU_MODEL_ID,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 2048,
            "system": STAGE1_SYSTEM_PROMPT,
            "messages": [{
                "role": "user",
                "content": user_content,
            }],
        }, ensure_ascii=False),
    )

    body = json.loads(response["body"].read())
    text = body["content"][0]["text"].strip()
    return _parse_json_array(text, "Stage 1")


# ─── Stage 2: ノードマッチ（Sonnet） ───

STAGE2_SYSTEM_PROMPT_TEMPLATE = """あなたはサプライチェーンリスク分析の専門家です。
以下のサプライチェーン拠点一覧と、新着のリスク情報を照合し、
影響を受ける拠点があるか判定してください。

【サプライチェーン拠点一覧】
{node_list}

【出力形式】JSON配列のみ:
[
  {{
    "fact_index": 0,
    "matched_node_ids": ["PLT001"],
    "impact_summary": "東名高速 豊田JCT付近の通行止めにより、豊田組立工場への部品搬入経路が遮断される可能性",
    "relevance_score": 85,
    "category_id": "リスクカテゴリ（earthquake/flood/fire/traffic/infra/labor/geopolitics/pandemic）",
    "reasoning": "通行止め区間が工場最寄りICを含む"
  }}
]
影響なしの場合は空配列 [] を返してください。

【重要】
・道路規制の区間名（IC名/JCT名）と拠点の最寄りICの地理的関係を考慮すること
・震源地名・震度観測地点と拠点所在地の地理的近接性を考慮すること
・間接影響（物流経路の遮断、港湾閉鎖による原材料入荷停止等）も考慮すること
・拠点名の完全一致だけでなく、略称・地域名・間接的な言及も考慮すること"""


def invoke_stage2(facts: list[dict], node_list_text: str) -> list[dict]:
    """Stage 2 Sonnet: ノードマッチを実行する。

    Args:
        facts: Stage 1をpassしたファクトデータの配列
        node_list_text: テキスト形式のノード一覧

    Returns:
        [{"fact_index": int, "matched_node_ids": [...], "impact_summary": str,
          "relevance_score": int, "category_id": str, "reasoning": str}, ...]
    """
    if not facts:
        return []

    system_prompt = STAGE2_SYSTEM_PROMPT_TEMPLATE.format(node_list=node_list_text)
    user_content = json.dumps(facts, ensure_ascii=False)

    response = bedrock_runtime.invoke_model(
        modelId=SONNET_MODEL_ID,
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
    return _parse_json_array(text, "Stage 2")


def invoke_stage2_classified(classified_event: dict, fact_sources: list[dict], node_list_text: str) -> list[dict]:
    """Stage 2 Sonnet: classified起点でのファクト照合を実行する。

    Args:
        classified_event: classifiedイベントデータ
        fact_sources: カテゴリ別に収集したファクトソースの配列
        node_list_text: テキスト形式のノード一覧

    Returns:
        [{"matched_node_ids": [...], "impact_summary": str, "relevance_score": int,
          "fact_match_details": [...], "reasoning": str}, ...]
    """
    system_prompt = STAGE2_SYSTEM_PROMPT_TEMPLATE.format(node_list=node_list_text)

    user_data = {
        "classified_event": classified_event,
        "fact_sources": fact_sources,
    }
    user_content = json.dumps(user_data, ensure_ascii=False)

    response = bedrock_runtime.invoke_model(
        modelId=SONNET_MODEL_ID,
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
    return _parse_json_array(text, "Stage 2 classified")


# ─── ノードインデックスヘルパー ───

def format_node_list(node_index: dict) -> str:
    """ノードインデックスJSONをテキスト形式のノード一覧に変換する。

    Args:
        node_index: {"nodes": [{"id": "PLT001", "name": "...", ...}, ...]}

    Returns:
        Stage 2 system promptに埋め込むテキスト
    """
    lines = []
    for n in node_index.get("nodes", []):
        line = f"ID: {n['id']} | {n['name']} | {n['node_type']}"
        if n.get("tier"):
            line += f"({n['tier']})"
        loc = n.get("location_name", "")
        if loc:
            line += f" | 所在地: {loc}"
        infra = n.get("related_infra", [])
        if infra:
            line += f"\n  関連インフラ: {', '.join(infra)}"
        products = n.get("products", [])
        if products:
            line += f"\n  生産: {'、'.join(products)}"
        lines.append(line)
    return "\n".join(lines)


# ─── 共通ユーティリティ ───

def _parse_json_array(text: str, stage_name: str) -> list[dict]:
    """BedrockレスポンスからJSON配列を抽出する。"""
    result = _extract_json(text, "[", "]")
    if result is None:
        logger.warning(f"{stage_name} レスポンス解析失敗: {text[:200]}")
        return []
    if not isinstance(result, list):
        logger.warning(f"{stage_name} JSON配列でない: {text[:200]}")
        return []
    return result


def _extract_json(text: str, open_char: str, close_char: str):
    """テキストからJSON構造を抽出する（ブラケットカウント方式）。

    最初の open_char を見つけ、対応する close_char まで取得してパースする。
    ネストした構造にも対応する。
    """
    start = text.find(open_char)
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
        if c == open_char:
            depth += 1
        elif c == close_char:
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start:i + 1])
                except json.JSONDecodeError:
                    return None
    return None

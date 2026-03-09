# -*- coding: utf-8 -*-
"""
SupplyChainMasterの拠点データを読み取り、Bedrock Claudeで周辺交通インフラを調査し、
地名KW + インフラKW を統合して TrendTable に SITE エンティティとして投入する。

keyword_search_function.py は TrendTable の TYPE#SITE → keywords[] を拠点KWとして使う。
本スクリプトで生成する keywords に地名もインフラも含めることで、
keyword_search_function.py 側の変更なしにインフラKWが監視対象に加わる。

使い方:
    # 確認のみ（dry-run）
    python scripts/generate_infra_keywords.py <TrendTableテーブル名> --profile profile_name --dry-run

    # TrendTableに投入
    python scripts/generate_infra_keywords.py <TrendTableテーブル名> --profile profile_name
"""

import argparse
import json
import re

import boto3
from boto3.dynamodb.conditions import Key

DYNAMODB_REGION = "ap-northeast-1"
SCM_TABLE_NAME = "SupplyChainMaster"
BEDROCK_MODEL_ID = "jp.anthropic.claude-sonnet-4-6"

MAX_INFRA_KEYWORDS = 8  # Python側で硬性上限を適用

PROMPT = """あなたは日本の交通インフラに詳しい物流コンサルタントです。
以下の拠点について、サプライチェーンリスク監視で重要な周辺交通インフラを回答してください。

【回答対象】
- 最寄りの高速道路・自動車道（通称。例: 東名高速）
- 最寄りのIC名
- 最寄りの主要鉄道路線名

【注意】
- X(Twitter)での災害監視に使うキーワードなので、SNSで実際に使われる通称を優先
- 半径10km以内のインフラのみ
- 重要度の高い順に並べる
- 駅名・国道番号・JCT名は不要

【出力形式】JSON配列のみ。説明文不要:
["キーワード1", "キーワード2", ...]
"""


def query_scm_by_type(scm_table, node_type: str) -> list[dict]:
    """GSI1で指定タイプのノードを全件取得する。"""
    items = []
    response = scm_table.query(
        IndexName="GSI1",
        KeyConditionExpression=Key("gsi1pk").eq(node_type),
    )
    items.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = scm_table.query(
            IndexName="GSI1",
            KeyConditionExpression=Key("gsi1pk").eq(node_type),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response["Items"])
    return items


def parse_location(location_name: str) -> list[str]:
    """所在地文字列から都道府県・市区町村を分割する。

    例: "東京都千代田区" → ["東京都", "千代田区"]
    """
    match = re.match(r"(..?[都道府県])(.+)", location_name)
    if match:
        return [match.group(1), match.group(2)]
    return [location_name]


def generate_infra_keywords(bedrock_client, node: dict) -> list[str]:
    """Bedrock Claudeで拠点周辺のインフラキーワードを生成する。"""
    name = node.get("name", node["pk"])
    location = node.get("location_name", node.get("region", ""))
    lat = node.get("lat", "")
    lon = node.get("lon", "")

    user_msg = f"拠点名: {name}\n所在地: {location}\n緯度経度: {lat}, {lon}"

    response = bedrock_client.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1024,
            "system": [{"type": "text", "text": PROMPT}],
            "messages": [{"role": "user", "content": user_msg}],
        }, ensure_ascii=False),
    )

    body = json.loads(response["body"].read())
    text = body["content"][0]["text"].strip()

    # 最初の JSON 配列を抽出（Bedrockが複数配列を返す場合に対応）
    start = text.find("[")
    if start == -1:
        print(f"    解析失敗: {text[:100]}")
        return []

    # "[" に対応する "]" を探す（ネスト対応）
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "[":
            depth += 1
        elif text[i] == "]":
            depth -= 1
            if depth == 0:
                return json.loads(text[start:i + 1])

    print(f"    解析失敗: 閉じ括弧なし: {text[:100]}")
    return []


def build_site_keywords(node: dict) -> list[str]:
    """SCMノードから地名キーワードを構築する。

    施設名・都道府県・市区町村・地域名は広すぎてノイズになるため除外。
    インフラKWのみを keywords として使用する。
    施設名等は siteName/location 属性に保持し、参照用に残す。
    """
    return []


def process_nodes(
    scm_table, trend_table, bedrock_client,
    node_type: str, label: str, dry_run: bool,
) -> int:
    """指定タイプのノードを処理する。"""
    print(f"\n{'='*60}")
    print(f"【{label}】 GSI1: gsi1pk={node_type}")
    print(f"{'='*60}")

    nodes = query_scm_by_type(scm_table, node_type)
    print(f"取得: {len(nodes)}件")
    count = 0

    for i, node in enumerate(nodes):
        pk = node["pk"]
        name = node.get("name", pk)
        location = node.get("location_name", node.get("region", ""))

        print(f"\n  [{i+1}/{len(nodes)}] {pk}: {name} ({location})")

        # 地名キーワード
        site_kws = build_site_keywords(node)

        # Bedrock でインフラキーワード生成（上限適用）
        try:
            infra_kws = generate_infra_keywords(bedrock_client, node)[:MAX_INFRA_KEYWORDS]
        except Exception as e:
            print(f"    Bedrockエラー: {e}")
            raise

        # 統合（地名KW + インフラKW → keywords）
        all_keywords = site_kws + infra_kws
        # 重複除去（順序保持）
        seen = set()
        unique_keywords = []
        for kw in all_keywords:
            if kw not in seen:
                seen.add(kw)
                unique_keywords.append(kw)

        print(f"    地名KW: {', '.join(site_kws)}")
        print(f"    インフラKW: {', '.join(infra_kws)}")
        print(f"    統合 keywords: {len(unique_keywords)}件")

        site_id = f"{node_type}_{pk.lower()}"
        item = {
            "PK": f"SITE#{site_id}",
            "SK": "META",
            "GSI1PK": "TYPE#SITE",
            "GSI1SK": f"SITE_TYPE#{node_type}#{site_id}",
            "siteName": name,
            "siteType": node_type,
            "location": location,
            "keywords": unique_keywords,
            "scmNodeId": pk,
        }

        if not dry_run:
            trend_table.put_item(Item=item)
            print(f"    ✓ TrendTable 投入済み")

        count += 1

    return count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SupplyChainMaster → Bedrock → TrendTable SITE投入")
    parser.add_argument("table_name", help="TrendTable DynamoDBテーブル名")
    parser.add_argument("--scm-table", default=SCM_TABLE_NAME, help="SupplyChainMasterテーブル名")
    parser.add_argument("--profile", default=None, help="AWS CLIプロファイル名")
    parser.add_argument("--dry-run", action="store_true", help="確認のみ（TrendTableに書き込まない）")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    dynamodb = session.resource("dynamodb", region_name=DYNAMODB_REGION)
    scm_table = dynamodb.Table(args.scm_table)
    trend_table = dynamodb.Table(args.table_name)
    bedrock_client = session.client("bedrock-runtime", region_name=DYNAMODB_REGION)

    print("拠点SITEキーワード生成（SupplyChainMaster → Bedrock → TrendTable）")
    print(f"  SCMテーブル: {args.scm_table}")
    print(f"  投入先:      {args.table_name}")
    print(f"  モデル:      {BEDROCK_MODEL_ID}")
    if args.dry_run:
        print("  *** DRY-RUN モード ***")

    total = 0
    for node_type, label in [("plant", "自社工場"), ("warehouse", "自社倉庫"), ("supplier", "サプライヤー")]:
        total += process_nodes(scm_table, trend_table, bedrock_client, node_type, label, args.dry_run)

    print(f"\n完了: {total}件のSITEキーワード生成")
    if args.dry_run:
        print("[DRY-RUN] TrendTableへの書き込みはスキップしました")


if __name__ == "__main__":
    main()
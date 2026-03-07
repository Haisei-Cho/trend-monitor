"""Trend Fetcher Lambda関数。

ルートA（トレンド線）:
    1. X API trends.get_by_woeid で日本トレンド一覧取得
    2. Bedrock でサプライチェーン関連トレンドをスクリーニング
    3. スクリーニング通過トレンドのみ search_recent で詳細ツイート取得
    4. S3保存（統一フォーマット）
"""

import json
import os
import re
import time
from typing import Any

import boto3
from xdk import Client

from aws_utils import get_bearer_token, get_today_start_time, save_to_s3
from log_utils import setup_logger

logger = setup_logger("trend_fetcher")

bedrock_runtime = boto3.client("bedrock-runtime")

SEARCH_MAX_RESULTS = int(os.environ.get("SEARCH_MAX_RESULTS", "10"))
WOEID = 23424856  # 日本
MAX_TRENDS = 50
BEDROCK_MODEL_ID = "jp.anthropic.claude-sonnet-4-6"

SCREENING_PROMPT = """あなたはサプライチェーンリスク分析の専門家です。
Xのトレンド一覧から、サプライチェーンに影響する可能性があるものを選別してください。

【対象リスクカテゴリ】
1. 地震・津波  2. 風水害  3. 火災・爆発  4. 交通障害
5. 停電・インフラ障害  6. 労務・操業リスク  7. 地政学・貿易  8. 感染症

【除外対象】
- エンタメ、スポーツ、芸能、ゲーム、アニメなど無関係なもの
- 比喩的・冗談的な使用（「地震級の衝撃」「爆発的人気」等）

【出力形式】JSON形式のみ。説明文不要:
{
  "screened": [
    {"trend_name": "トレンド名", "reason": "理由"}
  ]
}
"""


def fetch_trends(client: Client) -> list[dict[str, Any]]:
    """X APIでトレンド一覧を取得する（日本 WOEID=23424856）。"""
    response = client.trends.get_by_woeid(woeid=WOEID, max_trends=MAX_TRENDS)
    trends = [
        {
            "trend_name": t.get("trend_name"),
            "tweet_count": t.get("tweet_count"),
        }
        for t in (response.data or [])
    ]
    logger.info(f"トレンド取得完了: {len(trends)}件")
    return trends


def screen_trends_with_bedrock(trends: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Bedrockでサプライチェーン関連トレンドをスクリーニングする。"""
    if not trends:
        return []

    trend_list_text = "\n".join(
        f"{i}. {t['trend_name']}" for i, t in enumerate(trends, 1)
    )

    try:
        response = bedrock_runtime.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 4096,
                "system": [{
                    "type": "text",
                    "text": SCREENING_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }],
                "messages": [{
                    "role": "user",
                    "content": f"以下のトレンドをスクリーニングしてください:\n{trend_list_text}",
                }],
            }, ensure_ascii=False),
        )

        body = json.loads(response["body"].read())
        text = body["content"][0]["text"].strip()

        # マークダウンコードブロック除去
        match = re.search(r"\{.*}", text, re.DOTALL)
        if not match:
            logger.warning(f"Bedrockレスポンス解析失敗: {text[:200]}")
            return trends

        result = json.loads(match.group())
        screened_map = {s["trend_name"]: s for s in result.get("screened", [])}

        screened = [
            {
                **t,
                "screening_reason": screened_map[t["trend_name"]].get("reason", ""),
            }
            for t in trends if t["trend_name"] in screened_map
        ]

        logger.info(f"Bedrockスクリーニング完了: {len(trends)}件 → {len(screened)}件通過")
        return screened

    except Exception as e:
        logger.error(f"Bedrockスクリーニングエラー: {e}")
        return [
            {**t, "screening_reason": "Bedrockエラーのため未スクリーニング"}
            for t in trends
        ]


def fetch_trend_details(
    client: Client,
    screened_trends: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """スクリーニング通過トレンドごとにsearch_recentで詳細ツイートを取得する。"""
    if not screened_trends:
        return []

    start_time = get_today_start_time()
    enriched: list[dict[str, Any]] = []

    for trend in screened_trends:
        trend_name = trend["trend_name"]
        query = f"{trend_name} lang:ja -is:retweet"
        tweets = []

        try:
            first_page = next(client.posts.search_recent(
                query=query,
                start_time=start_time,
                max_results=min(SEARCH_MAX_RESULTS, 100),
                tweet_fields=["created_at", "author_id", "text", "public_metrics"],
            ))
            tweets = first_page.data or []
        except StopIteration:
            pass
        except Exception as e:
            logger.warning(f"search_recentエラー（{trend_name}）: {e}")

        sample_tweets = [
            {
                "text": tw.get("text", ""),
                "author_id": tw.get("author_id", ""),
                "created_at": tw.get("created_at", ""),
                "metrics": tw.get("public_metrics", {}),
            }
            for tw in tweets[:5]
        ]
        enriched.append({
            **trend,
            "source": "trends_route",
            "tweet_count": len(tweets),
            "sample_tweets": sample_tweets,
        })

        if tweets:
            logger.info(f"トレンド詳細取得: {trend_name} → {len(tweets)}件")

        # X API Rate Limit 対策（1リクエスト/秒）
        time.sleep(1)

    logger.info(f"トレンド詳細取得完了: {len(enriched)}件")
    return enriched


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda関数エントリーポイント。

    ルートA: get_by_woeid → Bedrockスクリーニング → search_recent → S3保存
    """
    logger.info(f"Trend Fetcher開始: event={json.dumps(event, ensure_ascii=False)}")

    bearer_token = get_bearer_token()
    client = Client(bearer_token=bearer_token)

    # 1. トレンド取得
    trends = fetch_trends(client)

    # 2. Bedrockスクリーニング（サプライチェーン関連のみ通過）
    screened = screen_trends_with_bedrock(trends)

    # 3. スクリーニング通過トレンドの詳細ツイート取得
    items = fetch_trend_details(client, screened)

    # 4. S3保存
    s3_key = save_to_s3(items, source="trends_route")

    output = {
        "s3_key": s3_key,
        "source": "trends_route",
        "trend_count": len(trends),
        "screened_count": len(screened),
        "item_count": len(items),
    }

    logger.info(f"Trend Fetcher完了: {json.dumps(output, ensure_ascii=False)}")
    return output

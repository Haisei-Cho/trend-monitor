from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError
from xdk import client

from common.utils import build_query, generate_s3_key, serialize_json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")
bedrock_runtime = boto3.client("bedrock-runtime")

TABLE_NAME = os.environ["TABLE_NAME"]
STAGE = os.environ.get("STAGE", "dev")
BUCKET_NAME = os.environ["BUCKET_NAME"]
SECRET_NAME = os.environ["SECRET_NAME"]
WOEID = int(os.environ.get("WOEID", "23424856"))
MAX_TRENDS = int(os.environ.get("MAX_TRENDS", "50"))
SEARCH_MAX_RESULTS = int(os.environ.get("SEARCH_MAX_RESULTS", "10"))

SCREENING_PROMPT = """
あなたはサプライチェーンリスク分析の専門家です。
次の日本トレンド一覧から、サプライチェーンに影響する可能性があるものを選別してください。

[対象とみなすカテゴリ]
1. 地震・津波 2. 風水害 3. 火災・爆発 4. 交通障害
5. 停電・インフラ障害 6. 労務・操業リスク 7. 地政学・貿易 8. 感染症

[除外例]
- エンタメ、スポーツ、芸能、ゲーム、アニメなど無関係なもの
- 比喩的・冗談的な使用

[出力]
JSONのみで返してください。説明文は不要です。
{
  "screened": [
    {"trend_name": "トレンド名", "reason": "理由", "category": "カテゴリ名"}
  ]
}
""".strip()

SUMMARY_PROMPT = """
あなたはサプライチェーンリスク分析の専門家です。
次の投稿群を読み、供給網・物流・調達・顧客影響の観点から投資家向けに要約してください。

[判定基準]
- 具体的な影響、原因、規模、状況、継続見込みを優先
- サプライチェーン（物流、製造、調達、販売）への影響を明確にできるもの
- 根拠が薄い推測、重複、ノイズは除外

[出力]
JSONのみで返してください。説明文は不要です。
{
  "items": [
    {
      "item_index": 1,
      "relevant": true,
      "summary": "リスク事象の要約"
    }
  ]
}
""".strip()


def get_bearer_token() -> str:
    """Secrets Manager から X API トークンを取得する。"""
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret_string = response["SecretString"]
        token = json.loads(secret_string).get("bearer_token", secret_string)
        if not token:
            raise ValueError("bearer_token が見つかりません")
        logger.info("X API トークン取得完了")
        return token
    except ClientError as exc:
        logger.error("Secrets Manager 取得エラー: %s", exc)
        raise


def get_master_data() -> tuple[dict[str, list[str]], list[str], list[str]]:
    """DynamoDB からマスターデータを取得する。"""
    table = dynamodb.Table(TABLE_NAME)

    try:
        kw_resp = table.query(
            IndexName="GSI1",
            KeyConditionExpression="GSI1PK = :t",
            ExpressionAttributeValues={":t": "TYPE#KEYWORD"},
        )

        risk_kw: dict[str, list[str]] = {}
        for item in kw_resp.get("Items", []):
            cat = item.get("category_id")
            if not cat:
                continue
            risk_kw.setdefault(cat, []).append(item.get("keyword", ""))

        site_resp = table.query(
            IndexName="GSI1",
            KeyConditionExpression="GSI1PK = :t",
            ExpressionAttributeValues={":t": "TYPE#SITE"},
        )
        site_kw: list[str] = []
        for item in site_resp.get("Items", []):
            site_kw.extend(item.get("keywords", []))
        site_kw = sorted(set(filter(None, site_kw)))

        exc_resp = table.query(
            IndexName="GSI1",
            KeyConditionExpression="GSI1PK = :t",
            ExpressionAttributeValues={":t": "TYPE#EXCLUSION"},
        )
        exc_kw: list[str] = []
        for item in exc_resp.get("Items", []):
            exc_kw.extend(item.get("keywords", []))

        logger.info(
            "マスターデータ取得: リスク=%sカテゴリ, 拠点=%s件, 除外=%s件",
            len(risk_kw),
            len(site_kw),
            len(exc_kw),
        )
        return risk_kw, site_kw, exc_kw
    except ClientError as exc:
        logger.error("DynamoDB クエリエラー: %s", exc)
        raise


def fetch_trends(bearer_token: str) -> list[dict[str, Any]]:
    """X のトレンド一覧を取得する。"""
    client_obj = client(bearer_token=bearer_token)
    response = client_obj.trends.get_by_woeid(
        woeid=WOEID,
        max_results=MAX_TRENDS,
        trend_fields=["trend_name"],
    )

    trends: list[dict[str, Any]] = []
    for trend in response.data:
        trends.append(
            {
                "trend_name": trend.get("trend_name", ""),
                "tweet_volume": trend.get("tweet_volume"),
            }
        )

    logger.info("トレンド取得完了: %s件", len(trends))
    return trends


def screen_trends_with_bedrock(trends: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Bedrock でサプライチェーン関連トレンドを選別する。"""
    if not trends:
        return []

    trend_list_text = "\n".join(
        f"{i}. {t['trend_name']}" for i, t in enumerate(trends, 1) if t.get("trend_name")
    )

    try:
        response = bedrock_runtime.invoke_model(
            modelId="jp.anthropic.claude-sonnet-4-6",
            body=json.dumps(
                {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 4096,
                    "system": [
                        {
                            "type": "text",
                            "text": SCREENING_PROMPT,
                            "cache_control": {"type": "ephemeral"},
                        }
                    ],
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": f"次の日本トレンドをスクリーニングしてください:\n{trend_list_text}",
                                }
                            ],
                        }
                    ],
                },
                ensure_ascii=False,
            ),
        )
        body = json.loads(response["body"].read())
        response_text = body["content"][0]["text"].strip()
        if response_text.startswith("```"):
            lines = response_text.split("\n")
            response_text = "\n".join(lines[1:-1]) if len(lines) > 2 else response_text
        response_text = (
            response_text.replace("```json", "").replace("```", "").strip()
        )

        result = json.loads(response_text)
        screened_map = {r["trend_name"]: r for r in result.get("screened", [])}

        screened: list[dict[str, Any]] = []
        for trend in trends:
            name = trend.get("trend_name", "")
            if name in screened_map:
                screened.append(
                    {
                        **trend,
                        "screening_reason": screened_map[name].get("reason", ""),
                        "screening_category": screened_map[name].get("category", ""),
                    }
                )

        logger.info("Bedrock スクリーニング完了: %s件中 %s件通過", len(trends), len(screened))
        return screened
    except Exception as exc:
        logger.error("Bedrock スクリーニングエラー: %s", exc)
        return [
            {
                **trend,
                "screening_reason": "Bedrock エラーのため未スクリーニング",
                "screening_category": "",
            }
            for trend in trends
        ]


def fetch_trend_details(
    bearer_token: str,
    screened_trends: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """各トレンドを search_recent で展開し、詳細サンプルを付与する。"""
    if not screened_trends:
        return []

    client_obj = client(bearer_token=bearer_token)
    enriched: list[dict[str, Any]] = []

    for trend in screened_trends:
        trend_name = trend.get("trend_name", "")
        query = f'"{trend_name}" lang:ja -is:retweet'

        try:
            first_page = next(
                client_obj.posts.search_recent(
                    query=query,
                    max_results=min(SEARCH_MAX_RESULTS, 100),
                    tweet_fields=["created_at", "author_id", "text", "public_metrics"],
                )
            )
            tweets = first_page.data or []
            sample_tweets = [
                {
                    "text": tw.get("text", ""),
                    "author_id": tw.get("author_id", ""),
                    "created_at": tw.get("created_at", ""),
                    "metrics": tw.get("public_metrics", {}),
                }
                for tw in tweets[:5]
            ]
            enriched.append(
                {
                    **trend,
                    "source": "trends_route",
                    "tweet_count": len(tweets),
                    "sample_tweets": sample_tweets,
                }
            )
            logger.info("トレンド詳細取得: %s %s件", trend_name, len(tweets))
        except StopIteration:
            logger.info("トレンド詳細結果なし: %s", trend_name)
            enriched.append(
                {
                    **trend,
                    "source": "trends_route",
                    "tweet_count": 0,
                    "sample_tweets": [],
                }
            )
        except Exception as exc:
            logger.warning("search_recent エラー (%s): %s", trend_name, exc)
            enriched.append(
                {
                    **trend,
                    "source": "trends_route",
                    "tweet_count": 0,
                    "sample_tweets": [],
                }
            )

    logger.info("トレンド詳細取得完了: %s件", len(enriched))
    return enriched


def fetch_keyword_hits(
    bearer_token: str,
    risk_kw: dict[str, list[str]],
    site_kw: list[str],
    exc_kw: list[str],
) -> list[dict[str, Any]]:
    """リスク x 拠点のキーワード検索を実行し、ヒット投稿を返す。"""
    if not risk_kw or not site_kw:
        logger.warning("リスクまたは拠点キーワード不足のためキーワード検索をスキップ")
        return []

    client_obj = client(bearer_token=bearer_token)
    hits: list[dict[str, Any]] = []

    for category_id, keywords in risk_kw.items():
        try:
            query = build_query(keywords, site_kw, exc_kw if exc_kw else None)
        except ValueError as exc:
            logger.warning("クエリ生成エラー (%s): %s", category_id, exc)
            continue

        try:
            first_page = next(
                client_obj.posts.search_recent(
                    query=query,
                    max_results=min(SEARCH_MAX_RESULTS, 100),
                    tweet_fields=["created_at", "author_id", "text", "public_metrics"],
                )
            )
            tweets = first_page.data or []
            if not tweets:
                continue

            sample_tweets = [
                {
                    "text": tw.get("text", ""),
                    "author_id": tw.get("author_id", ""),
                    "created_at": tw.get("created_at", ""),
                    "metrics": tw.get("public_metrics", {}),
                }
                for tw in tweets[:5]
            ]
            hits.append(
                {
                    "trend_name": f"[KW] {category_id}",
                    "category_id": category_id,
                    "source": "keyword_route",
                    "tweet_count": len(tweets),
                    "sample_tweets": sample_tweets,
                    "query_used": query,
                }
            )
            logger.info("キーワードヒット: %s %s件", category_id, len(tweets))
        except StopIteration:
            logger.info("キーワード検索結果なし: %s", category_id)
        except Exception as exc:
            logger.warning("search_recent エラー (%s): %s", category_id, exc)

    logger.info("キーワード検索完了: %s件", len(hits))
    return hits


def summarize_posts_with_bedrock(
    trend_tweets: list[dict[str, Any]],
    keyword_hits: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """トレンド由来とキーワード由来を統合し、Bedrock で要約する。"""
    all_items: list[dict[str, Any]] = []
    for item in trend_tweets:
        if item.get("tweet_count", 0) > 0:
            all_items.append(item)
    for item in keyword_hits:
        if item.get("tweet_count", 0) > 0:
            all_items.append(item)

    if not all_items:
        logger.info("ポスト精査: 対象データなし")
        return []

    input_lines: list[str] = []
    for i, item in enumerate(all_items, 1):
        trend_name = item.get("trend_name", "")
        tweets_text = " / ".join(
            tw.get("text", "")[:300] for tw in item.get("sample_tweets", [])
        )
        input_lines.append(f"[{i}] トレンド: {trend_name}\n投稿例: {tweets_text}")
    input_text = "\n".join(input_lines)

    try:
        response = bedrock_runtime.invoke_model(
            modelId="jp.anthropic.claude-sonnet-4-6",
            body=json.dumps(
                {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 4096,
                    "system": [
                        {
                            "type": "text",
                            "text": SUMMARY_PROMPT,
                            "cache_control": {"type": "ephemeral"},
                        }
                    ],
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": f"以下のアイテムを分類してください:\n\n{input_text}",
                                }
                            ],
                        }
                    ],
                },
                ensure_ascii=False,
            ),
        )

        body = json.loads(response["body"].read())
        response_text = body["content"][0]["text"].strip()
        if response_text.startswith("```"):
            lines = response_text.split("\n")
            response_text = "\n".join(lines[1:-1]) if len(lines) > 2 else response_text
        response_text = (
            response_text.replace("```json", "").replace("```", "").strip()
        )

        result = json.loads(response_text)
        relevant_indices: dict[int, dict[str, Any]] = {}
        for item in result.get("items", []):
            if item.get("relevant", False):
                relevant_indices[item["item_index"]] = item

        filtered: list[dict[str, Any]] = []
        for i, item in enumerate(all_items, 1):
            if i not in relevant_indices:
                continue
            bedrock_result = relevant_indices[i]
            filtered.append(
                {
                    "trend_name": item.get("trend_name", ""),
                    "source": item.get("source", ""),
                    "summary": bedrock_result.get("summary", ""),
                    "tweet_count": item.get("tweet_count", 0),
                    "sample_tweets": item.get("sample_tweets", []),
                    "screening_reason": item.get("screening_reason", ""),
                    "screening_category": item.get("screening_category", ""),
                    "category_id": item.get("category_id", ""),
                    "query_used": item.get("query_used", ""),
                }
            )

        logger.info("ポスト精査完了: %s件中 %s件採用", len(all_items), len(filtered))
        return filtered
    except Exception as exc:
        logger.error("ポスト精査エラー: %s", exc)
        return [
            {
                "trend_name": item.get("trend_name", ""),
                "source": item.get("source", ""),
                "summary": "ポスト精査エラーのため未精査",
                "tweet_count": item.get("tweet_count", 0),
                "sample_tweets": item.get("sample_tweets", []),
                "screening_reason": item.get("screening_reason", ""),
                "screening_category": item.get("screening_category", ""),
                "category_id": item.get("category_id", ""),
                "query_used": item.get("query_used", ""),
            }
            for item in all_items
        ]


def save_to_s3(items: list[dict[str, Any]]) -> str:
    """結果を統一フォーマットの JSON として S3 に保存する。"""
    now = datetime.now(timezone.utc)
    s3_key = generate_s3_key(now)
    data = {
        "fetched_at": now.isoformat(),
        "item_count": len(items),
        "items": items,
    }

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=serialize_json(data).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("S3 保存完了: s3://%s/%s", BUCKET_NAME, s3_key)
    return s3_key


def handler(event: dict, context: Any) -> dict:
    """Lambda エントリーポイント。"""
    logger.info("Trend Fetcher 開始: event=%s", json.dumps(event, ensure_ascii=False))

    try:
        bearer_token = get_bearer_token()
        risk_kw, site_kw, exc_kw = get_master_data()

        trends = fetch_trends(bearer_token)
        screened = screen_trends_with_bedrock(trends)
        trend_tweets = fetch_trend_details(bearer_token, screened)
        keyword_hits = fetch_keyword_hits(bearer_token, risk_kw, site_kw, exc_kw)
        items = summarize_posts_with_bedrock(trend_tweets, keyword_hits)
        s3_key = save_to_s3(items)

        output = {
            "s3_key": s3_key,
            "trend_count": len(trends),
            "screened_count": len(screened),
            "trend_tweet_count": sum(t.get("tweet_count", 0) for t in trend_tweets),
            "keyword_hit_count": sum(h.get("tweet_count", 0) for h in keyword_hits),
            "item_count": len(items),
        }
        logger.info("Trend Fetcher 完了: %s", json.dumps(output, ensure_ascii=False))
        return output
    except Exception as exc:
        logger.error("Trend Fetcher 失敗: %s", exc, exc_info=True)
        raise

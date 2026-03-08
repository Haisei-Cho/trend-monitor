"""Keyword Search Lambda関数。

ルートB（キーワード線）:
    1. DynamoDBからマスタデータ取得（リスクKW, 拠点KW, 除外KW）
    2. カテゴリ毎に build_query でクエリ構築
    3. search_recent でキーワードヒット取得
    4. S3保存（統一フォーマット）
"""

import json
import os
import time
from typing import Any

from xdk import Client

from aws_utils import get_bearer_token, get_today_start_time, save_to_s3, query_gsi1
from log_utils import setup_logger
from utils import build_query

logger = setup_logger("keyword_search")

TABLE_NAME = os.environ["TABLE_NAME"]
SEARCH_MAX_RESULTS = int(os.environ.get("SEARCH_MAX_RESULTS", "10"))


def get_master_data() -> tuple[dict[str, list[str]], list[str], list[str]]:
    """DynamoDBからマスタデータを取得する。

    Returns:
        tuple: (カテゴリID別リスクKW, 拠点KW, 除外KW)
    """
    # リスクキーワード
    risk_kw: dict[str, list[str]] = {}
    for item in query_gsi1(TABLE_NAME, "TYPE#KEYWORD"):
        cat, kw = item.get("category_id"), item.get("keyword")
        if cat and kw:
            risk_kw.setdefault(cat, []).append(kw)

    # 拠点キーワード
    site_kw: list[str] = []
    for item in query_gsi1(TABLE_NAME, "TYPE#SITE"):
        site_kw.extend(item.get("keywords", []))
    site_kw = sorted(set(site_kw))

    # 除外ルール
    exc_kw: list[str] = []
    for item in query_gsi1(TABLE_NAME, "TYPE#EXCLUSION"):
        exc_kw.extend(item.get("keywords", []))

    logger.info(
        f"マスタデータ取得: リスク={len(risk_kw)}カテゴリ, "
        f"拠点={len(site_kw)}件, 除外={len(exc_kw)}件"
    )
    return risk_kw, site_kw, exc_kw


def fetch_keyword_hits(
    client: Client,
    risk_kw: dict[str, list[str]],
    site_kw: list[str],
    exc_kw: list[str],
) -> list[dict[str, Any]]:
    """リスクKW×拠点KWでsearch_recentを実行し、キーワードヒットを取得する。"""
    if not risk_kw or not site_kw:
        logger.warning("リスクKWまたは拠点KWが空のためキーワード検索スキップ")
        return []

    start_time = get_today_start_time()
    logger.info(f"検索開始時刻: {start_time}（JST当日0時）")

    hits: list[dict[str, Any]] = []

    for category_id, keywords in risk_kw.items():
        try:
            queries = build_query(keywords, site_kw, exc_kw if exc_kw else None)
        except ValueError as e:
            logger.warning(f"クエリ構築エラー（{category_id}）: {e}")
            continue

        for qi, query in enumerate(queries):
            try:
                first_page = next(client.posts.search_recent(
                    query=query,
                    start_time=start_time,
                    max_results=min(SEARCH_MAX_RESULTS, 100),
                    tweet_fields=["created_at", "author_id", "text", "public_metrics"],
                ))
                tweets = first_page.data or []
                if not tweets:
                    continue

                sample_tweets = [
                    {
                        "id": tw.get("id", ""),
                        "text": tw.get("text", ""),
                        "author_id": tw.get("author_id", ""),
                        "created_at": tw.get("created_at", ""),
                        "metrics": tw.get("public_metrics", {}),
                    }
                    for tw in tweets[:5]
                ]
                label = f"[KW] {category_id}" if len(queries) == 1 else f"[KW] {category_id}#{qi + 1}"
                hits.append({
                    "trend_name": label,
                    "category_id": category_id,
                    "source": "keyword_route",
                    "tweet_count": len(tweets),
                    "sample_tweets": sample_tweets,
                    "query_used": query,
                })
                logger.info(f"キーワードヒット: {category_id}({qi + 1}/{len(queries)}) → {len(tweets)}件")
            except StopIteration:
                logger.info(f"キーワード検索結果なし: {category_id}({qi + 1}/{len(queries)})")
            except Exception as e:
                logger.warning(f"search_recentエラー（{category_id}）: {e}")

            # X API Rate Limit 対策（1リクエスト/秒）
            time.sleep(1)

    logger.info(f"キーワードヒット取得完了: {len(hits)}件")
    return hits


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda関数エントリーポイント。

    ルートB: DynamoDBマスタ取得 → build_query → search_recent → S3保存
    """
    logger.info(f"Keyword Search開始: event={json.dumps(event, ensure_ascii=False)}")

    bearer_token = get_bearer_token()
    client = Client(bearer_token=bearer_token)

    risk_kw, site_kw, exc_kw = get_master_data()

    items = fetch_keyword_hits(client, risk_kw, site_kw, exc_kw)

    s3_key = save_to_s3(items, source="keyword_route")

    output = {
        "s3_key": s3_key,
        "source": "keyword_route",
        "category_count": len(risk_kw),
        "item_count": len(items),
        "total_tweet_count": sum(h.get("tweet_count", 0) for h in items),
    }

    logger.info(f"Keyword Search完了: {json.dumps(output, ensure_ascii=False)}")
    return output

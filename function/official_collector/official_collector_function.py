"""OfficialCollector Lambda関数。

X API 公式アカウント監視ルート:
    1. DynamoDB から有効な公式アカウントリストを取得
    2. build_official_queries() でクエリ構築（512文字超で自動分割）
    3. search_recent で各クエリを実行（since_id による増分取得）
    4. S3 facts/official/{date}/{HHmm}.json に保存
    5. DynamoDB のカーソル（newest_id）を更新
"""

import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import boto3
from xdk import Client

from aws_utils import get_bearer_token, get_today_start_time, query_gsi1
from log_utils import setup_logger
from utils import build_official_queries

logger = setup_logger("official_collector")

TABLE_NAME = os.environ["TABLE_NAME"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
SEARCH_MAX_RESULTS = int(os.environ.get("SEARCH_MAX_RESULTS", "100"))

CURSOR_PK = "CURSOR#official_account_route"
CURSOR_SK = "META"

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


def get_official_accounts() -> list[str]:
    """DynamoDB から有効な公式アカウントの username リストを取得する。"""
    items = query_gsi1(TABLE_NAME, "TYPE#OFFICIAL_ACCT")
    usernames = [item["username"] for item in items if item.get("enabled") is True]
    if not usernames:
        raise ValueError("有効な公式アカウントが存在しません")
    logger.info(f"公式アカウント取得: {len(usernames)}件")
    return usernames


def get_cursor() -> str | None:
    """DynamoDB からカーソル（newest_id）を取得する。存在しない場合は None を返す。"""
    table = dynamodb.Table(TABLE_NAME)
    resp = table.get_item(Key={"PK": CURSOR_PK, "SK": CURSOR_SK})
    item = resp.get("Item")
    if item:
        return item.get("newest_id")
    return None


def save_cursor(newest_id: str) -> None:
    """カーソル（newest_id）を DynamoDB に保存する。"""
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(Item={
        "PK": CURSOR_PK,
        "SK": CURSOR_SK,
        "newest_id": newest_id,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    logger.info(f"カーソル更新: newest_id={newest_id}")


def save_to_s3(tweets: list[dict], since_id: str | None, newest_id: str | None) -> str:
    """収集ツイートを S3 facts/official/{date}/{HHmm}.json に保存する。"""
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H%M")
    s3_key = f"facts/official/{date_str}/{time_str}.json"

    data = {
        "fetched_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "since_id": since_id,
        "newest_id": newest_id,
        "result_count": len(tweets),
        "tweets": tweets,
    }

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"S3保存完了: s3://{BUCKET_NAME}/{s3_key} ({len(tweets)}件)")
    return s3_key


def collect_tweets(client: Client, usernames: list[str], since_id: str | None) -> tuple[list[dict], str | None]:
    """公式アカウントのツイートを収集する。

    Returns:
        (tweets, newest_id) - newest_id は全クエリの最大値
    """
    queries = build_official_queries(usernames)
    start_time = get_today_start_time() if since_id is None else None

    all_tweets: list[dict[str, Any]] = []
    newest_ids: list[str] = []

    # ユーザー情報マップ構築用
    user_map: dict[str, dict] = {}

    for qi, query in enumerate(queries):
        try:
            kwargs: dict[str, Any] = {
                "query": query,
                "max_results": SEARCH_MAX_RESULTS,
                "tweet_fields": ["created_at", "author_id", "text", "public_metrics"],
                "expansions": ["author_id"],
                "user_fields": ["username", "name"],
            }
            if since_id:
                kwargs["since_id"] = since_id
            else:
                kwargs["start_time"] = start_time

            first_page = next(client.posts.search_recent(**kwargs))

            # ユーザー情報を収集
            includes = first_page.includes or {}
            for user in includes.get("users") or []:
                user_map[user.get("id", "")] = user

            tweets = first_page.data or []
            meta = first_page.meta or {}

            if meta and meta.newest_id:
                newest_ids.append(meta.newest_id)

            for tw in tweets:
                author_id = tw.get("author_id", "")
                user = user_map.get(author_id, {})
                all_tweets.append({
                    "id": tw.get("id", ""),
                    "author_id": author_id,
                    "author_username": user.get("username", ""),
                    "author_name": user.get("name", ""),
                    "text": tw.get("text", ""),
                    "created_at": tw.get("created_at", ""),
                    "metrics": tw.get("public_metrics", {}),
                })

            logger.info(f"クエリ{qi + 1}/{len(queries)}: {len(tweets)}件取得")

        except StopIteration:
            logger.info(f"クエリ{qi + 1}/{len(queries)}: 新規投稿なし")
        except Exception as e:
            logger.warning(f"クエリ{qi + 1}/{len(queries)} エラー: {e}")

        if qi < len(queries) - 1:
            time.sleep(1)  # X API レート制限対策

    # 全クエリの newest_id の最大値を返す（ツイートIDは数値として比較）
    resolved_newest = max(newest_ids, key=lambda x: int(x)) if newest_ids else None
    logger.info(f"収集完了: 合計{len(all_tweets)}件, newest_id={resolved_newest}")
    return all_tweets, resolved_newest


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda関数エントリーポイント。"""
    logger.info(f"OfficialCollector開始: event={json.dumps(event, ensure_ascii=False)}")

    bearer_token = get_bearer_token()
    client = Client(bearer_token=bearer_token)

    usernames = get_official_accounts()
    since_id = get_cursor()

    if since_id:
        logger.info(f"増分取得: since_id={since_id}")
    else:
        logger.info("初回取得: start_time=当日0時UTC")

    tweets, newest_id = collect_tweets(client, usernames, since_id)

    s3_key = save_to_s3(tweets, since_id, newest_id)

    if newest_id:
        save_cursor(newest_id)

    output = {
        "s3_key": s3_key,
        "tweet_count": len(tweets),
        "newest_id": newest_id,
        "account_count": len(usernames),
    }

    logger.info(f"OfficialCollector完了: {json.dumps(output, ensure_ascii=False)}")
    return output

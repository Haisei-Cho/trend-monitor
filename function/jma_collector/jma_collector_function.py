"""JmaCollector Lambda関数。

気象庁 bosai JSON API から地震情報・津波情報・台風情報を取得し S3 に保存する。
"""

import json
import os
import urllib.request
from datetime import datetime, timezone
from typing import Any

import boto3

from aws_utils import save_if_changed
from log_utils import setup_logger

logger = setup_logger("jma_collector")

s3_client = boto3.client("s3")

BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
REQUEST_TIMEOUT = 10  # 秒

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

# 取得対象エンドポイント定義
ENDPOINTS = [
    {
        "data_type": "quake_list",
        "url": "https://www.jma.go.jp/bosai/quake/data/list.json",
        "s3_key": "facts/jma/latest/quake_list.json",
    },
    {
        "data_type": "tsunami",
        "url": "https://www.jma.go.jp/bosai/tsunami/data/list.json",
        "s3_key": "facts/jma/latest/tsunami.json",
    },
    {
        "data_type": "typhoon",
        "url": "https://www.jma.go.jp/bosai/typhoon/data/targetTc.json",
        "s3_key": "facts/jma/latest/typhoon.json",
    },
]


def fetch_jma_data(url: str) -> list | dict | None:
    """気象庁APIからJSONデータを取得する。

    Returns:
        パース済みJSONデータ。失敗時は None。
    """
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            raw = resp.read()
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        logger.error(f"HTTP エラー: {url} → {e.code} {e.reason}")
        return None
    except urllib.error.URLError as e:
        logger.error(f"接続エラー: {url} → {e.reason}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSONパースエラー: {url} → {e}")
        return None


def save_fact_to_s3(raw_data: list | dict, data_type: str, s3_key: str) -> bool:
    """ラッパーで包んでS3に保存する。内容変更時のみ書き込む。

    Returns:
        True=書き込み実行, False=変更なしスキップ
    """
    wrapped = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": "jma",
        "data_type": data_type,
        "raw_data": raw_data,
    }
    return save_if_changed(BUCKET_NAME, s3_key, wrapped)


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda関数エントリーポイント。

    気象庁APIから地震情報・津波情報・台風情報を取得し S3 に上書き保存する。
    """
    logger.info(f"JmaCollector開始: event={json.dumps(event, ensure_ascii=False)}")

    results = {}

    for ep in ENDPOINTS:
        data_type = ep["data_type"]
        url = ep["url"]
        s3_key = ep["s3_key"]

        logger.info(f"取得開始: {data_type} ({url})")
        raw_data = fetch_jma_data(url)

        if raw_data is None:
            logger.warning(f"取得失敗、スキップ: {data_type}")
            results[data_type] = "skipped"
            continue

        item_count = len(raw_data) if isinstance(raw_data, list) else 1
        changed = save_fact_to_s3(raw_data, data_type, s3_key)
        status = "saved" if changed else "unchanged"
        results[data_type] = f"{status} ({item_count} items)"
        logger.info(f"取得完了: {data_type} → {item_count}件 ({status})")

    output = {
        "source": "jma_collector",
        "results": results,
    }
    logger.info(f"JmaCollector完了: {json.dumps(output, ensure_ascii=False)}")
    return output
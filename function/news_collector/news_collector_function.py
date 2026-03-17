"""NewsCollector Lambda関数。

Google News RSS から8つのリスクカテゴリ別にニュース記事を収集し、
重複排除して S3 facts/news/latest/ に保存する。
"""

import json
import os
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from bs4 import BeautifulSoup
from typing import Any

import boto3

from aws_utils import query_gsi1
from log_utils import setup_logger

logger = setup_logger("news_collector")

s3_client = boto3.client("s3")

BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
TABLE_NAME = os.environ.get("TABLE_NAME", "")
REQUEST_INTERVAL = 2  # 秒
REQUEST_TIMEOUT = 10  # 秒
MAX_ARTICLES_PER_CATEGORY = 200

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

RSS_BASE_URL = "https://news.google.com/rss/search"


def get_master_data() -> tuple[dict[str, list[str]], list[str]]:
    """DynamoDBからリスクキーワードと除外ルールを取得する。

    Returns:
        (カテゴリID別リスクKW, 除外KWリスト)
    """
    # リスクキーワード
    risk_kw: dict[str, list[str]] = {}
    for item in query_gsi1(TABLE_NAME, "TYPE#KEYWORD"):
        cat = item.get("category_id")
        kw = item.get("keyword")
        if cat and kw:
            risk_kw.setdefault(cat, []).append(kw)

    # 除外ルール
    exc_kw: list[str] = []
    for item in query_gsi1(TABLE_NAME, "TYPE#EXCLUSION"):
        exc_kw.extend(item.get("keywords", []))

    logger.info(
        f"マスタデータ取得: リスク={len(risk_kw)}カテゴリ, 除外={len(exc_kw)}件"
    )
    return risk_kw, exc_kw


def build_news_query(keywords: list[str]) -> str:
    """リスクキーワードリストから Google News RSS 検索クエリを構築する。"""
    return " OR ".join(keywords) + " when:1d"


def filter_by_exclusion(articles: list[dict], exc_kw: list[str]) -> list[dict]:
    """除外キーワードに基づいて記事をフィルタリングする。"""
    if not exc_kw:
        return articles

    filtered = []
    for article in articles:
        text = article.get("title", "") + " " + article.get("description", "")
        if any(kw in text for kw in exc_kw):
            continue
        filtered.append(article)
    return filtered


def build_rss_url(query: str) -> str:
    """Google News RSS の検索URLを構築する。"""
    params = urllib.parse.urlencode({
        "q": query,
        "hl": "ja",
        "gl": "JP",
        "ceid": "JP:ja",
    })
    return f"{RSS_BASE_URL}?{params}"


def strip_html(text: str) -> str:
    """HTMLタグ除去・エンティティデコード・空白正規化を一括で行う。"""
    return BeautifulSoup(text, "html.parser").get_text(separator=" ", strip=True)


def parse_pub_date(date_str: str) -> str | None:
    """RFC 822形式の日付文字列をISO 8601に変換する。

    Returns:
        ISO 8601文字列。パース失敗時は None。
    """
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError):
        return None


def fetch_rss(url: str) -> str | None:
    """Google News RSS をHTTP GETで取得する。

    Returns:
        XMLテキスト。失敗時は None。
    """
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            return resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        logger.error(f"HTTPエラー: {url} → {e.code} {e.reason}")
        return None
    except urllib.error.URLError as e:
        logger.error(f"接続エラー: {url} → {e.reason}")
        return None


def parse_rss_xml(xml_text: str) -> list[dict]:
    """RSS XMLをパースし、記事リストを返す。

    Returns:
        記事辞書のリスト。パース失敗時は空リスト。
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.error(f"XMLパースエラー: {e}")
        return []

    articles = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for item in root.iter("item"):
        title = item.findtext("title", "")
        link = item.findtext("link", "")
        raw_pub_date = item.findtext("pubDate", "")
        raw_description = item.findtext("description", "")
        source_elem = item.find("source")
        source_name = source_elem.text if source_elem is not None and source_elem.text else ""

        # pubDate パース（欠損時は現在時刻をフォールバック）
        pub_date = parse_pub_date(raw_pub_date) if raw_pub_date else None
        if pub_date is None:
            pub_date = now_iso

        # description の HTML タグ除去
        description = strip_html(raw_description) if raw_description else ""

        articles.append({
            "title": title,
            "link": link,
            "pub_date": pub_date,
            "description": description,
            "source_name": source_name,
        })

    return articles


def load_existing_articles(s3_key: str) -> list[dict]:
    """S3から既存の記事データを読み込む。

    Returns:
        既存記事リスト。ファイルが存在しない場合は空リスト。
    """
    try:
        resp = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        data = json.loads(resp["Body"].read().decode("utf-8"))
        return data.get("articles", [])
    except s3_client.exceptions.NoSuchKey:
        return []
    except Exception as e:
        logger.warning(f"既存データ読み込みエラー: {s3_key} → {e}")
        return []


def merge_and_deduplicate(
    existing: list[dict], new_articles: list[dict]
) -> list[dict]:
    """既存記事と新規記事をマージし、linkベースで重複排除する。

    pubDate降順ソートし、最大MAX_ARTICLES_PER_CATEGORY件に切り詰める。
    """
    seen_links: set[str] = set()
    merged: list[dict] = []

    # 新規記事を優先（同一linkの場合は新しいデータを使う）
    for article in new_articles + existing:
        link = article.get("link", "")
        if link and link not in seen_links:
            seen_links.add(link)
            merged.append(article)

    # pubDate降順ソート
    merged.sort(key=lambda a: a.get("pub_date", ""), reverse=True)

    return merged[:MAX_ARTICLES_PER_CATEGORY]


def save_to_s3(articles: list[dict], category: str, s3_key: str) -> None:
    """記事データをS3に保存する。"""
    data = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": "google_news",
        "category": category,
        "article_count": len(articles),
        "articles": articles,
    }
    body = json.dumps(data, ensure_ascii=False)
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"S3保存完了: s3://{BUCKET_NAME}/{s3_key} ({len(articles)}件)")


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda関数エントリーポイント。

    DynamoDB マスタデータからクエリを動的構築し、Google News RSS を取得。
    除外フィルタ → 重複排除 → S3 facts/news/latest/ に保存する。
    """
    logger.info(f"NewsCollector開始: event={json.dumps(event, ensure_ascii=False)}")

    # マスタデータ取得
    risk_kw, exc_kw = get_master_data()
    if not risk_kw:
        raise RuntimeError("リスクキーワードが取得できませんでした。DynamoDBマスタデータを確認してください。")

    categories = list(risk_kw.items())
    results = {}

    for i, (category, keywords) in enumerate(categories):
        s3_key = f"facts/news/latest/{category}.json"

        # クエリ動的構築
        query = build_news_query(keywords)
        url = build_rss_url(query)
        logger.info(f"取得開始: {category} ({len(keywords)}KW)")

        # RSS取得
        xml_text = fetch_rss(url)
        if xml_text is None:
            logger.warning(f"取得失敗、スキップ: {category}")
            results[category] = "skipped"
            if i < len(categories) - 1:
                time.sleep(REQUEST_INTERVAL)
            continue

        # XMLパース
        new_articles = parse_rss_xml(xml_text)
        if not new_articles:
            logger.info(f"記事なし: {category}")
            results[category] = "no_articles"
            if i < len(categories) - 1:
                time.sleep(REQUEST_INTERVAL)
            continue

        # 除外フィルタリング
        before_count = len(new_articles)
        new_articles = filter_by_exclusion(new_articles, exc_kw)
        excluded = before_count - len(new_articles)
        if excluded:
            logger.info(f"除外フィルタ: {category} → {excluded}件除外")

        # S3既存データとマージ・重複排除
        existing = load_existing_articles(s3_key)
        merged = merge_and_deduplicate(existing, new_articles)

        # S3保存
        save_to_s3(merged, category, s3_key)
        results[category] = f"saved ({len(new_articles)} new, {excluded} excluded, {len(merged)} total)"
        logger.info(f"取得完了: {category} → 新規{len(new_articles)}件, 除外{excluded}件, 合計{len(merged)}件")

        if i < len(categories) - 1:
            time.sleep(REQUEST_INTERVAL)

    output = {
        "source": "news_collector",
        "category_count": len(categories),
        "results": results,
    }
    logger.info(f"NewsCollector完了: {json.dumps(output, ensure_ascii=False)}")
    return output

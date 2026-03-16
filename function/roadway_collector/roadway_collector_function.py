"""RoadwayCollector Lambda関数。

Yahoo!道路交通情報から高速道路の交通規制情報をクロールし、
DynamoDB RoadwayTraffic テーブルに差分記録する。
"""

import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from typing import Any

import boto3
from bs4 import BeautifulSoup

from log_utils import setup_logger

logger = setup_logger("roadway_collector")

dynamodb = boto3.resource("dynamodb")

ROADWAY_TABLE_NAME = os.environ.get("ROADWAY_TABLE_NAME", "")

BASE_URL = "https://roadway.yahoo.co.jp"
REQUEST_INTERVAL = 0.5  # 秒
REQUEST_TIMEOUT = 10  # 秒
TTL_DAYS = 30

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

JST = timezone(timedelta(hours=9))

# 規制テーブルヘッダー行の判定用
HEADER_KEYWORDS = {"規制区間", "規制内容"}

# 道路リンクの正規表現パターン
ROAD_LINK_PATTERN = re.compile(r"/traffic/pref/(\d+)/road/(\d+)/list")

# 都道府県名マッピング
PREF_NAMES = {
    "01": "北海道", "02": "青森県", "03": "岩手県", "04": "宮城県",
    "05": "秋田県", "06": "山形県", "07": "福島県", "08": "茨城県",
    "09": "栃木県", "10": "群馬県", "11": "埼玉県", "12": "千葉県",
    "13": "東京都", "14": "神奈川県", "15": "新潟県", "16": "富山県",
    "17": "石川県", "18": "福井県", "19": "山梨県", "20": "長野県",
    "21": "岐阜県", "22": "静岡県", "23": "愛知県", "24": "三重県",
    "25": "滋賀県", "26": "京都府", "27": "大阪府", "28": "兵庫県",
    "29": "奈良県", "30": "和歌山県", "31": "鳥取県", "32": "島根県",
    "33": "岡山県", "34": "広島県", "35": "山口県", "36": "徳島県",
    "37": "香川県", "38": "愛媛県", "39": "高知県", "40": "福岡県",
    "41": "佐賀県", "42": "長崎県", "43": "熊本県", "44": "大分県",
    "45": "宮崎県", "46": "鹿児島県", "47": "沖縄県",
}


def discover_roads() -> list[dict]:
    """47都道府県ページをクロールし、全道路リストを取得する。"""
    all_roads = []

    for pref_id in range(1, 48):
        pref_id_str = f"{pref_id:02d}"
        url = f"/traffic/pref/{pref_id}/list"

        html = fetch_page(url)
        if html is None:
            logger.warning(f"都道府県ページ取得失敗: {PREF_NAMES[pref_id_str]}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        seen_road_ids = set()

        for a_tag in soup.find_all("a", href=ROAD_LINK_PATTERN):
            match = ROAD_LINK_PATTERN.search(a_tag["href"])
            if not match:
                continue

            road_id = match.group(2)
            if road_id in seen_road_ids:
                continue

            road_name = a_tag.get_text(strip=True)
            if road_name in ("上り", "下り"):
                continue

            seen_road_ids.add(road_id)
            all_roads.append({
                "road_id": road_id,
                "road_name": road_name,
                "pref_id": pref_id_str,
                "pref_name": PREF_NAMES[pref_id_str],
                "source_url": f"/traffic/pref/{pref_id}/road/{road_id}/list",
            })

        time.sleep(REQUEST_INTERVAL)

    return all_roads


def get_active_regulations(table) -> dict:
    """DynamoDBから現在ACTIVEな規制一覧を取得する。

    Returns:
        規制キー（road_id#direction#section#regulation_type）→ アイテムの辞書
    """
    regs = {}
    query_kwargs = {
        "IndexName": "GSI2",
        "KeyConditionExpression": "gsi2pk = :active",
        "ExpressionAttributeValues": {":active": "ACTIVE"},
    }
    while True:
        response = table.query(**query_kwargs)
        for item in response.get("Items", []):
            key = _regulation_key(item)
            regs[key] = item
        if "LastEvaluatedKey" not in response:
            break
        query_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    return regs


def _regulation_key(reg: dict) -> str:
    """規制の同一性判定キーを生成する。"""
    road_id = reg.get("pk", "").replace("ROAD#", "") if "pk" in reg else reg.get("road_id", "")
    return f"{road_id}#{reg.get('direction', '')}#{reg.get('section', '')}#{reg.get('regulation_type', '')}"


def fetch_page(source_url: str) -> str | None:
    """指定パスのHTMLを取得する。"""
    url = f"{BASE_URL}{source_url}"
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


def parse_regulations(html: str) -> dict:
    """道路規制詳細ページのHTMLを解析し、規制情報を抽出する。

    Returns:
        {road_name, fetched_at, directions: [{direction, has_regulation, regulations: [...]}]}
    """
    soup = BeautifulSoup(html, "html.parser")

    # 道路名: <h1> or 見出しから抽出
    road_name = ""
    h1 = soup.find("h1") or soup.find("h2")
    if h1:
        text = h1.get_text(strip=True)
        road_name = text.replace("の事故・渋滞情報", "")

    # 取得時刻: 「X月X日 XX時XX分 現在」をパース
    fetched_at = datetime.now(JST).isoformat()
    time_pattern = re.compile(r"(\d+)月(\d+)日\s*(\d+)時(\d+)分\s*現在")
    for li in soup.find_all("li"):
        match = time_pattern.search(li.get_text())
        if match:
            now = datetime.now(JST)
            month, day, hour, minute = (int(x) for x in match.groups())
            fetched_at = now.replace(
                month=month, day=day, hour=hour, minute=minute, second=0, microsecond=0
            ).isoformat()
            break

    # 方向別の規制情報を抽出
    directions = []
    direction_pattern = re.compile(r"（(上り|下り)）")

    # h2 タグから方向ごとのセクションを特定
    h2_tags = soup.find_all("h2")
    for h2 in h2_tags:
        dir_match = direction_pattern.search(h2.get_text())
        if not dir_match:
            continue

        direction = dir_match.group(1)
        direction_data = {
            "direction": direction,
            "has_regulation": False,
            "regulations": [],
        }

        # h2 の次の兄弟要素から規制情報を探す
        sibling = h2.find_next_sibling()
        while sibling and sibling.name != "h2":
            text = sibling.get_text()

            # 規制なしパターン
            if "規制情報はありません" in text:
                break

            # テーブル（規制情報あり）
            if sibling.name == "table" or sibling.find("table"):
                table_elem = sibling if sibling.name == "table" else sibling.find("table")
                if table_elem:
                    rows = table_elem.find_all("tr")
                    for row in rows:
                        cells = row.find_all(["td", "th"])
                        cell_texts = [c.get_text(strip=True) for c in cells]
                        cell_classes = [" ".join(c.get("class", [])) for c in cells]

                        # ヘッダー行をスキップ
                        if not cell_texts or HEADER_KEYWORDS & set(cell_texts):
                            continue

                        # 区間型（4列）: start-point | end-point | 規制内容 | 原因
                        # 地点型（3列）: 地点 | 規制内容 | 原因
                        if len(cell_texts) >= 4 and "start-point" in cell_classes[0]:
                            section = f"{cell_texts[0]} {cell_texts[1]}"
                            regulation_type = cell_texts[2].replace("[!]", "").strip()
                            cause = cell_texts[3] if len(cell_texts) > 3 else ""
                        elif len(cell_texts) >= 3:
                            section = cell_texts[0]
                            regulation_type = cell_texts[1].replace("[!]", "").strip()
                            cause = cell_texts[2]
                        else:
                            continue

                        reg = {
                            "section": section,
                            "regulation_type": regulation_type,
                            "cause": cause,
                        }
                        direction_data["regulations"].append(reg)
                        direction_data["has_regulation"] = True

            sibling = sibling.find_next_sibling()

        directions.append(direction_data)

    return {
        "road_name": road_name,
        "fetched_at": fetched_at,
        "directions": directions,
    }


def detect_changes(
    active_regs: dict, current_regs: list[dict]
) -> tuple[list[dict], list[dict]]:
    """前回ACTIVEと今回の規制を比較し、新規と解除を検出する。

    Args:
        active_regs: 前回のACTIVE規制（キー→アイテム）
        current_regs: 今回検出した規制リスト

    Returns:
        (new_regulations, cleared_regulations)
    """
    current_keys = set()
    new_regs = []

    for reg in current_regs:
        key = _regulation_key(reg)
        current_keys.add(key)
        if key not in active_regs:
            new_regs.append(reg)

    cleared_regs = []
    for key, item in active_regs.items():
        if key not in current_keys:
            cleared_regs.append(item)

    return new_regs, cleared_regs


def save_new_regulation(table, reg: dict) -> None:
    """新規規制をDynamoDBに保存する。"""
    now = datetime.now(JST)
    ttl_epoch = int((now + timedelta(days=TTL_DAYS)).timestamp())
    road_id = reg["road_id"]
    pref_id = reg["pref_id"]
    direction = reg["direction"]
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%S")

    item = {
        "pk": f"ROAD#{road_id}",
        "sk": f"EVENT#{timestamp}#{direction}",
        "gsi1pk": f"PREF#{pref_id}",
        "gsi1sk": f"EVENT#{timestamp}",
        "gsi2pk": "ACTIVE",
        "gsi2sk": f"PREF#{pref_id}#ROAD#{road_id}",
        "road_name": reg.get("road_name", ""),
        "pref_id": pref_id,
        "pref_name": reg.get("pref_name", ""),
        "direction": direction,
        "section": reg.get("section", ""),
        "regulation_type": reg.get("regulation_type", ""),
        "cause": reg.get("cause", ""),
        "detected_at": now.isoformat(),
        "cleared_at": None,
        "source": "yahoo_roadway",
        "ttl": ttl_epoch,
    }
    table.put_item(Item=item)
    logger.info(f"新規規制: {reg['road_name']} {direction} {reg.get('section', '')}")


def clear_regulation(table, item: dict) -> None:
    """規制解除をDynamoDBに反映する。"""
    now = datetime.now(JST).isoformat()
    table.update_item(
        Key={"pk": item["pk"], "sk": item["sk"]},
        UpdateExpression="SET cleared_at = :cleared REMOVE gsi2pk, gsi2sk",
        ExpressionAttributeValues={":cleared": now},
    )
    logger.info(
        f"規制解除: {item.get('road_name', '')} {item.get('direction', '')} "
        f"{item.get('section', '')}"
    )


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda関数エントリーポイント。

    47都道府県の道路一覧を動的に取得し、各道路の規制情報をクロール。
    差分検知で新規規制・規制解除をDynamoDBに記録する。
    """
    logger.info(f"RoadwayCollector開始: event={json.dumps(event, ensure_ascii=False)}")

    table = dynamodb.Table(ROADWAY_TABLE_NAME)

    # 1. 都道府県ページから道路一覧を動的に取得
    roads = discover_roads()
    logger.info(f"道路発見: {len(roads)}路線")

    if not roads:
        logger.error("道路が1件も取得できませんでした。")
        return {"source": "roadway_collector", "status": "no_roads_found"}

    # 2. 現在ACTIVEな規制一覧を取得
    active_regs = get_active_regulations(table)
    logger.info(f"現在ACTIVE規制: {len(active_regs)}件")

    # 3. 各道路の規制情報をクロール
    current_regulations = []
    crawled_count = 0
    error_count = 0

    for i, road in enumerate(roads):
        html = fetch_page(road["source_url"])
        if html is None:
            error_count += 1
            continue

        parsed = parse_regulations(html)
        crawled_count += 1

        # 規制情報をフラット化
        for d in parsed.get("directions", []):
            if d["has_regulation"]:
                for reg in d["regulations"]:
                    current_regulations.append({
                        "road_id": road["road_id"],
                        "road_name": road["road_name"] or parsed.get("road_name", ""),
                        "pref_id": road["pref_id"],
                        "pref_name": road["pref_name"],
                        "direction": d["direction"],
                        "section": reg.get("section", ""),
                        "regulation_type": reg.get("regulation_type", ""),
                        "cause": reg.get("cause", ""),
                    })

        # 進捗ログ（50件ごと）
        if (i + 1) % 50 == 0:
            logger.info(f"クロール進捗: {i + 1}/{len(roads)}")

        time.sleep(REQUEST_INTERVAL)

    logger.info(
        f"クロール完了: {crawled_count}道路, "
        f"{len(current_regulations)}規制, {error_count}エラー"
    )

    # 4. 差分検知
    new_regs, cleared_regs = detect_changes(active_regs, current_regulations)
    logger.info(f"差分検知: 新規={len(new_regs)}件, 解除={len(cleared_regs)}件")

    # 5. DynamoDB更新
    for reg in new_regs:
        save_new_regulation(table, reg)

    for item in cleared_regs:
        clear_regulation(table, item)

    output = {
        "source": "roadway_collector",
        "road_count": crawled_count,
        "regulation_count": len(current_regulations),
        "new_regulations": len(new_regs),
        "cleared_regulations": len(cleared_regs),
        "errors": error_count,
    }
    logger.info(f"RoadwayCollector完了: {json.dumps(output, ensure_ascii=False)}")
    return output

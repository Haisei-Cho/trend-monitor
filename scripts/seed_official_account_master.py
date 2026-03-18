"""
公式アカウントマスタ投入スクリプト

OfficialCollector が監視する X 公式アカウント 23件を DynamoDB に投入する。

Usage:
    python scripts/seed_official_account_master.py <テーブル名> [--profile PROFILE]
"""

from datetime import datetime, timezone


ADDED_AT = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# (username, displayName, description, categories, priorityGroup)
OFFICIAL_ACCOUNTS = [
    # ══════════════════════════════════════════
    # 横断・防災・気象
    # ══════════════════════════════════════════
    (
        "UN_NERV",
        "特務機関NERV防災",
        "気象庁専用線接続・国内最速級の防災速報",
        ["earthquake", "flood"],
        "disaster",
    ),
    (
        "JMA_kishou",
        "気象庁",
        "地震・津波・気象警報の一次情報源",
        ["earthquake", "flood"],
        "disaster",
    ),
    (
        "JMA_bousai",
        "気象庁防災情報",
        "防災気象情報専用アカウント",
        ["earthquake", "flood"],
        "disaster",
    ),
    (
        "Kantei_Saigai",
        "首相官邸（災害・危機管理）",
        "政府の大規模災害対応・避難指示",
        ["earthquake", "flood", "fire", "traffic", "infra", "labor", "geopolitics", "pandemic"],
        "disaster",
    ),
    (
        "FDMA_JAPAN",
        "総務省消防庁",
        "大規模災害の被害状況・消防活動",
        ["earthquake", "flood", "fire"],
        "disaster",
    ),
    (
        "CAO_BOUSAI",
        "内閣府防災",
        "防災政策・避難情報・被害情報",
        ["earthquake", "flood", "fire", "traffic", "infra", "labor", "geopolitics", "pandemic"],
        "disaster",
    ),
    # ══════════════════════════════════════════
    # 火災・爆発
    # ══════════════════════════════════════════
    (
        "Tokyo_Fire_D",
        "東京消防庁",
        "関東エリアの火災・救急情報",
        ["fire"],
        "fire",
    ),
    # ══════════════════════════════════════════
    # 交通インフラ
    # ══════════════════════════════════════════
    (
        "JREast_official",
        "JR東日本（公式）",
        "東北・関東の鉄道運行情報",
        ["traffic"],
        "traffic",
    ),
    (
        "JRCentral_OFL",
        "JR東海News",
        "東海道新幹線・東海エリア運行情報",
        ["traffic"],
        "traffic",
    ),
    (
        "e_nexco_bousai",
        "NEXCO東日本（道路防災）",
        "災害時の高速道路通行止め情報",
        ["traffic"],
        "traffic",
    ),
    (
        "w_nexco_news",
        "NEXCO西日本",
        "西日本エリア高速道路情報",
        ["traffic"],
        "traffic",
    ),
    (
        "MLIT_JAPAN",
        "国土交通省",
        "道路・河川・港湾等の総合情報",
        ["traffic", "flood"],
        "traffic",
    ),
    (
        "MLIT_river",
        "国土交通省 水管理・国土保全",
        "河川水位・洪水予報の専門情報",
        ["flood"],
        "traffic",
    ),
    # ══════════════════════════════════════════
    # 電力・インフラ
    # ══════════════════════════════════════════
    (
        "TEPCOPG",
        "東京電力パワーグリッド",
        "関東エリア停電情報・復旧情報",
        ["infra"],
        "infra",
    ),
    (
        "KANDEN_souhai",
        "関西電力送配電",
        "関西エリア停電情報・復旧情報",
        ["infra"],
        "infra",
    ),
    (
        "Official_Chuden",
        "中部電力",
        "中部エリア停電・復旧情報",
        ["infra"],
        "infra",
    ),
    (
        "TH_nw_official",
        "東北電力ネットワーク",
        "東北エリア停電情報・復旧情報",
        ["infra"],
        "infra",
    ),
    # ══════════════════════════════════════════
    # 自治体防災
    # ══════════════════════════════════════════
    (
        "tokyo_bousai",
        "東京都防災",
        "東京エリアの防災・避難情報",
        ["earthquake", "flood"],
        "local",
    ),
    (
        "osaka_bousai",
        "おおさか防災ネット（大阪府）",
        "関西エリアの防災・避難情報",
        ["earthquake", "flood"],
        "local",
    ),
    # ══════════════════════════════════════════
    # 労務・操業リスク／地政学・貿易
    # ══════════════════════════════════════════
    (
        "meti_NIPPON",
        "経済産業省",
        "輸出規制・操業制限・産業政策",
        ["labor", "geopolitics"],
        "labor",
    ),
    (
        "MofaJapan_jp",
        "外務省",
        "制裁・渡航情報・貿易摩擦の公式発表",
        ["geopolitics"],
        "geopolitics",
    ),
    # ══════════════════════════════════════════
    # 感染症
    # ══════════════════════════════════════════
    (
        "MHLWitter",
        "厚生労働省",
        "感染症対策・労務安全衛生",
        ["pandemic", "labor"],
        "pandemic",
    ),
    (
        "JIHS_JP",
        "国立健康危機管理研究機構",
        "国内感染症サーベイランス（旧NIID、2025年再編）",
        ["pandemic"],
        "pandemic",
    ),
]


def get_all_items() -> list[dict]:
    items: list[dict] = []
    for username, display_name, description, categories, priority_group in OFFICIAL_ACCOUNTS:
        items.append({
            "PK": f"OFFICIAL_ACCT#{username}",
            "SK": "META",
            "GSI1PK": "TYPE#OFFICIAL_ACCT",
            "GSI1SK": f"#{username}",
            "username": username,
            "displayName": display_name,
            "description": description,
            "categories": categories,
            "priorityGroup": priority_group,
            "enabled": True,
            "addedAt": ADDED_AT,
        })
    return items


if __name__ == "__main__":
    import argparse

    import boto3

    parser = argparse.ArgumentParser(description="公式アカウントマスタ投入")
    parser.add_argument("table_name", help="DynamoDBテーブル名")
    parser.add_argument("--profile", default=None, help="AWS CLIプロファイル名")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    table = session.resource("dynamodb").Table(args.table_name)
    items = get_all_items()

    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

    print(f"完了: {len(items)} 件 → {args.table_name}")

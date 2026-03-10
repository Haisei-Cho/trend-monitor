"""
キーワード設計マスタデータ投入スクリプト

"""


# ======================================================================
# リスクカテゴリ
# ======================================================================

RISK_CATEGORIES = {
    "earthquake": "地震・津波",
    "flood": "風水害",
    "fire": "火災・爆発",
    "traffic": "交通障害",
    "infra": "停電・インフラ障害",
    "labor": "労務・操業リスク",
    "geopolitics": "地政学・貿易",
    "pandemic": "感染症",
}

# ======================================================================
# リスクキーワード（カテゴリID → キーワードリスト）
# ======================================================================

RISK_KEYWORDS = {
    "earthquake": ["地震", "震度", "揺れ", "津波", "津波警報", "震源"],
    "flood": ["台風", "豪雨", "洪水", "浸水", "冠水", "土砂崩れ", "氾濫", "暴風"],
    "fire": ["火災", "火事", "黒煙", "爆発", "炎上", "消防"],
    "traffic": ["通行止め", "道路封鎖", "渋滞", "事故", "不通", "運休", "遅延", "脱線", "立往生"],
    "infra": ["停電", "断水", "ガス漏れ", "通信障害", "回線障害"],
    "labor": ["ストライキ", "操業停止", "工場閉鎖", "倒産", "民事再生"],
    "geopolitics": ["輸出規制", "制裁", "関税引上げ", "禁輸", "港湾封鎖", "海峡封鎖"],
    "pandemic": ["感染拡大", "パンデミック", "工場ロックダウン", "港湾閉鎖"],
}

# ======================================================================
# 除外ルール (rule_id, ruleName, keywords[])
# ======================================================================

EXCLUSION_RULES = [
    ("retweet", "リツイート除外", ["is:retweet"]),
    ("bot", "Bot投稿除外", ["#相互フォロー", "自動投稿", "定期ツイート"]),
    ("entertainment", "エンタメ文脈除外", ["ゲーム", "アニメ", "ドラマ", "映画", "小説", "マンガ"]),
    ("promotion", "宣伝・広告除外", ["PR", "案件", "セール", "キャンペーン実施中", "お得情報"]),
    ("sns_flame", "SNS炎上除外", ["炎上商法", "炎上案件", "ネット炎上"]),
]

# ======================================================================
# 拠点マスタ（港湾・空港・幹線道路）
# (site_id, siteType, siteName, location, keywords[])
# ※ 自社工場・倉庫・サプライヤーは別スクリプトで生成
# ======================================================================

SITES = [
    # ══════════════════════════════════════════
    # 港湾：国際戦略港湾（5港）
    # ══════════════════════════════════════════
    ("port_tokyo", "port", "東京港", "東京都港区", ["東京港"]),
    ("port_yokohama", "port", "横浜港", "横浜市", ["横浜港"]),
    ("port_kawasaki", "port", "川崎港", "川崎市", ["川崎港"]),
    ("port_osaka", "port", "大阪港", "大阪市", ["大阪港"]),
    ("port_kobe", "port", "神戸港", "神戸市", ["神戸港"]),
    # ══════════════════════════════════════════
    # 港湾：国際拠点港湾（18港）
    # ══════════════════════════════════════════
    ("port_muroran", "port", "室蘭港", "室蘭市", ["室蘭港"]),
    ("port_tomakomai", "port", "苫小牧港", "苫小牧市", ["苫小牧港"]),
    ("port_sendai", "port", "仙台塩釜港", "仙台市/塩竈市", ["仙台塩釜港", "仙台港"]),
    ("port_chiba", "port", "千葉港", "千葉市", ["千葉港"]),
    ("port_niigata", "port", "新潟港", "新潟市", ["新潟港"]),
    ("port_fushiki", "port", "伏木富山港", "高岡市/射水市", ["伏木富山港"]),
    ("port_shimizu", "port", "清水港", "静岡市清水区", ["清水港"]),
    ("port_nagoya", "port", "名古屋港", "名古屋市港区", ["名古屋港"]),
    ("port_yokkaichi", "port", "四日市港", "四日市市", ["四日市港"]),
    ("port_sakai", "port", "堺泉北港", "堺市/高石市", ["堺泉北港"]),
    ("port_himeji", "port", "姫路港", "姫路市", ["姫路港"]),
    ("port_wakayama", "port", "和歌山下津港", "和歌山市/海南市", ["和歌山下津港"]),
    ("port_mizushima", "port", "水島港", "倉敷市", ["水島港"]),
    ("port_hiroshima", "port", "広島港", "広島市", ["広島港"]),
    ("port_tokuyama", "port", "徳山下松港", "周南市/下松市", ["徳山下松港"]),
    ("port_shimonoseki", "port", "下関港", "下関市", ["下関港"]),
    ("port_kitakyushu", "port", "北九州港", "北九州市", ["北九州港"]),
    ("port_hakata", "port", "博多港", "福岡市", ["博多港"]),
    # ══════════════════════════════════════════
    # 空港：会社管理空港（4空港）
    # ══════════════════════════════════════════
    ("airport_narita", "airport", "成田国際空港", "成田市", ["成田空港", "成田国際空港"]),
    ("airport_haneda", "airport", "東京国際空港", "大田区", ["羽田空港", "東京国際空港"]),
    ("airport_centrair", "airport", "中部国際空港", "常滑市", ["中部国際空港", "セントレア"]),
    ("airport_kansai", "airport", "関西国際空港", "泉佐野市", ["関西国際空港", "関空"]),
    # ══════════════════════════════════════════
    # 空港：国管理空港（19空港）
    # ══════════════════════════════════════════
    ("airport_itami", "airport", "大阪国際空港", "豊中市/池田市", ["伊丹空港", "大阪国際空港"]),
    ("airport_shinchitose", "airport", "新千歳空港", "千歳市", ["新千歳空港"]),
    ("airport_wakkanai", "airport", "稚内空港", "稚内市", ["稚内空港"]),
    ("airport_kushiro", "airport", "釧路空港", "釧路市", ["釧路空港"]),
    ("airport_hakodate", "airport", "函館空港", "函館市", ["函館空港"]),
    ("airport_sendai", "airport", "仙台空港", "名取市", ["仙台空港"]),
    ("airport_niigata", "airport", "新潟空港", "新潟市", ["新潟空港"]),
    ("airport_hiroshima", "airport", "広島空港", "三原市", ["広島空港"]),
    ("airport_takamatsu", "airport", "高松空港", "高松市", ["高松空港"]),
    ("airport_matsuyama", "airport", "松山空港", "松山市", ["松山空港"]),
    ("airport_kochi", "airport", "高知空港", "南国市", ["高知空港"]),
    ("airport_fukuoka", "airport", "福岡空港", "福岡市", ["福岡空港"]),
    ("airport_kitakyushu", "airport", "北九州空港", "北九州市", ["北九州空港"]),
    ("airport_nagasaki", "airport", "長崎空港", "大村市", ["長崎空港"]),
    ("airport_kumamoto", "airport", "熊本空港", "益城町", ["熊本空港"]),
    ("airport_oita", "airport", "大分空港", "国東市", ["大分空港"]),
    ("airport_miyazaki", "airport", "宮崎空港", "宮崎市", ["宮崎空港"]),
    ("airport_kagoshima", "airport", "鹿児島空港", "霧島市", ["鹿児島空港"]),
    ("airport_naha", "airport", "那覇空港", "那覇市", ["那覇空港"]),
    # ══════════════════════════════════════════
    # 幹線道路：NEXCO管理 主要高速道路
    # ══════════════════════════════════════════
    # ── 東日本 ──
    ("road_doo", "road", "道央自動車道", "大沼公園〜士別剣淵", ["道央道", "道央自動車道"]),
    ("road_doto", "road", "道東自動車道", "千歳恵庭〜阿寒", ["道東道", "道東自動車道"]),
    ("road_tohoku", "road", "東北自動車道", "川口〜青森", ["東北道", "東北自動車道"]),
    ("road_akita", "road", "秋田自動車道", "北上〜秋田", ["秋田道", "秋田自動車道"]),
    ("road_yamagata", "road", "山形自動車道", "村田〜酒田", ["山形道", "山形自動車道"]),
    ("road_ban_etsu", "road", "磐越自動車道", "いわき〜新潟", ["磐越道", "磐越自動車道"]),
    ("road_joban", "road", "常磐自動車道", "三郷〜亘理", ["常磐道", "常磐自動車道"]),
    ("road_kan_etsu", "road", "関越自動車道", "練馬〜長岡", ["関越道", "関越自動車道"]),
    ("road_joshinetsu", "road", "上信越自動車道", "藤岡〜上越", ["上信越道", "上信越自動車道"]),
    ("road_higashi_kanto", "road", "東関東自動車道", "湾岸市川〜潮来", ["東関東道", "東関東自動車道"]),
    ("road_kita_kanto", "road", "北関東自動車道", "高崎〜ひたちなか", ["北関東道", "北関東自動車道"]),
    ("road_kenodo", "road", "首都圏中央連絡自動車道", "茅ヶ崎〜大栄", ["圏央道"]),
    ("road_gaikan", "road", "東京外環自動車道", "大泉〜三郷", ["外環道", "東京外環"]),
    # ── 中日本 ──
    ("road_tomei", "road", "東名高速道路", "東京〜小牧", ["東名高速", "東名"]),
    ("road_shin_tomei", "road", "新東名高速道路", "海老名〜豊田東", ["新東名高速", "新東名"]),
    ("road_chuo", "road", "中央自動車道", "高井戸〜小牧", ["中央道", "中央自動車道"]),
    ("road_nagano", "road", "長野自動車道", "岡谷〜豊科", ["長野道", "長野自動車道"]),
    ("road_hokuriku", "road", "北陸自動車道", "米原〜新潟", ["北陸道", "北陸自動車道"]),
    ("road_tokai_hokuriku", "road", "東海北陸自動車道", "一宮〜小矢部砺波", ["東海北陸道", "東海北陸自動車道"]),
    ("road_ise", "road", "伊勢自動車道", "関〜伊勢", ["伊勢道", "伊勢自動車道"]),
    ("road_isewangan", "road", "伊勢湾岸自動車道", "豊田東〜四日市", ["伊勢湾岸道", "伊勢湾岸自動車道"]),
    ("road_tokai_kanjo", "road", "東海環状自動車道", "豊田東〜関広見", ["東海環状道", "東海環状自動車道"]),
    ("road_meishin", "road", "名神高速道路", "小牧〜西宮", ["名神高速", "名神"]),
    ("road_shin_meishin", "road", "新名神高速道路", "四日市〜神戸", ["新名神高速", "新名神"]),
    # ── 西日本 ──
    ("road_sanyo", "road", "山陽自動車道", "神戸〜下関", ["山陽道", "山陽自動車道"]),
    ("road_chugoku", "road", "中国自動車道", "吹田〜下関", ["中国道", "中国自動車道"]),
    ("road_hanwa", "road", "阪和自動車道", "松原〜南紀田辺", ["阪和道", "阪和自動車道"]),
    ("road_maizuru", "road", "舞鶴若狭自動車道", "吉川〜敦賀", ["舞鶴若狭道", "舞鶴若狭自動車道"]),
    ("road_kyushu", "road", "九州自動車道", "門司〜鹿児島", ["九州道", "九州自動車道"]),
    ("road_higashi_kyushu", "road", "東九州自動車道", "北九州〜清武", ["東九州道", "東九州自動車道"]),
    ("road_nagasaki", "road", "長崎自動車道", "鳥栖〜長崎", ["長崎道", "長崎自動車道"]),
    ("road_oita", "road", "大分自動車道", "日出〜速見", ["大分道", "大分自動車道"]),
    ("road_miyazaki", "road", "宮崎自動車道", "えびの〜宮崎", ["宮崎道", "宮崎自動車道"]),
]


# ======================================================================
# DynamoDB アイテム変換
# ======================================================================

def get_all_items() -> list[dict]:
    items: list[dict] = []

    for i, (cat_id, name) in enumerate(RISK_CATEGORIES.items(), 1):
        items.append({
            "PK": f"RISK_CAT#{cat_id}", "SK": "META",
            "GSI1PK": "TYPE#RISK_CAT", "GSI1SK": f"#{cat_id}",
            "categoryName": name, "sortOrder": i,
        })

    for cat_id, keywords in RISK_KEYWORDS.items():
        for kw in keywords:
            items.append({
                "PK": f"KW#{kw}", "SK": f"CAT#{cat_id}",
                "GSI1PK": "TYPE#KEYWORD", "GSI1SK": f"CAT#{cat_id}",
                "keyword": kw, "category_id": cat_id,
            })

    for rule_id, name, kws in EXCLUSION_RULES:
        items.append({
            "PK": f"EXCLUSION#{rule_id}", "SK": "META",
            "GSI1PK": "TYPE#EXCLUSION", "GSI1SK": f"#{rule_id}",
            "ruleName": name, "keywords": kws,
        })

    for site_id, site_type, name, location, kws in SITES:
        items.append({
            "PK": f"SITE#{site_id}", "SK": "META",
            "GSI1PK": "TYPE#SITE", "GSI1SK": f"SITE_TYPE#{site_type}#{site_id}",
            "siteName": name, "siteType": site_type, "location": location,
            "keywords": kws,
        })

    return items


if __name__ == "__main__":
    import argparse

    import boto3

    parser = argparse.ArgumentParser(description="キーワード設計マスタデータ投入")
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

# -*- coding: utf-8 -*-
"""
DynamoDB SupplyChainMaster テーブルに種子データを投入するスクリプト
マスタデータの原本。Neptuneへはここから同期する。

格納するもの:
  - 拠点（工場・倉庫・サプライヤー・カスタマ・ロケーション）
  - 製品
  - 供給関係（SUPPLIES_TO）

"""
import argparse
import boto3
from decimal import Decimal
from typing import Any

DYNAMODB_REGION = "ap-northeast-1"
TABLE_NAME = "SupplyChainMaster"


def get_table(profile: str | None = None):
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    dynamodb = session.resource("dynamodb", region_name=DYNAMODB_REGION)
    return dynamodb.Table(TABLE_NAME)


def to_decimal(val: Any) -> Any:
    """floatをDecimalに変換"""
    if isinstance(val, float):
        return Decimal(str(val))
    if isinstance(val, dict):
        return {k: to_decimal(v) for k, v in val.items()}
    if isinstance(val, list):
        return [to_decimal(v) for v in val]
    return val


def put(table, item: dict[str, Any]) -> None:
    """DynamoDBにアイテムを書き込み"""
    table.put_item(Item=to_decimal(item))


# ============================================================
# ノード
# ============================================================

def seed_locations(table) -> None:
    """ロケーションを投入"""
    print("ロケーションを投入中...")
    data = [
        ("LOC001", "東京都", "千代田区", 35.6762, 139.6503),
        ("LOC002", "大阪府", "大阪市", 34.6937, 135.5023),
        ("LOC003", "愛知県", "名古屋市", 35.1815, 136.9066),
        ("LOC004", "沖縄県", "宮古島市", 24.8055, 125.2811),
        ("LOC005", "沖縄県", "那覇市", 26.2124, 127.6809),
        ("LOC006", "福岡県", "福岡市", 33.5904, 130.4017),
        ("LOC007", "広島県", "広島市", 34.3853, 132.4553),
        ("LOC008", "宮城県", "仙台市", 38.2682, 140.8694),
        ("LOC009", "北海道", "札幌市", 43.0618, 141.3545),
        ("LOC010", "神奈川県", "横浜市", 35.4437, 139.6380),
        ("LOC011", "京都府", "京都市", 35.0116, 135.7681),
        ("LOC012", "兵庫県", "神戸市", 34.6901, 135.1956),
        ("LOC013", "新潟県", "新潟市", 37.9026, 139.0236),
        ("LOC014", "静岡県", "浜松市", 34.7108, 137.7261),
        ("LOC015", "長崎県", "長崎市", 32.7503, 129.8779),
    ]
    for loc_id, pref, city, lat, lon in data:
        put(table, {"pk": loc_id, "sk": "META", "node_type": "location",
             "pref": pref, "city": city, "lat": lat, "lon": lon,
             "gsi1pk": "location", "gsi1sk": loc_id})
    print(f"  {len(data)}件")


def seed_plants(table) -> None:
    """自社工場を投入"""
    print("自社工場を投入中...")
    data = [
        ("PLT001", "東京組立工場", "東京都千代田区", 35.6762, 139.6503, 5000, "LOC001"),
        ("PLT002", "大阪製造工場", "大阪府大阪市", 34.6937, 135.5023, 4000, "LOC002"),
        ("PLT003", "名古屋電子工場", "愛知県名古屋市", 35.1815, 136.9066, 3500, "LOC003"),
        ("PLT004", "宮古島半導体工場", "沖縄県宮古島市", 24.8055, 125.2811, 2000, "LOC004"),
        ("PLT005", "那覇物流センター", "沖縄県那覇市", 26.2124, 127.6809, 3000, "LOC005"),
        ("PLT006", "福岡組立工場", "福岡県福岡市", 33.5904, 130.4017, 2800, "LOC006"),
        ("PLT007", "広島部品工場", "広島県広島市", 34.3853, 132.4553, 2500, "LOC007"),
        ("PLT008", "仙台製造工場", "宮城県仙台市", 38.2682, 140.8694, 3200, "LOC008"),
        ("PLT009", "横浜精密工場", "神奈川県横浜市", 35.4437, 139.6380, 800, "LOC010"),
    ]
    for pid, name, loc_name, lat, lon, cap, loc_id in data:
        put(table, {"pk": pid, "sk": "META", "node_type": "plant",
             "name": name, "location_name": loc_name,
             "lat": lat, "lon": lon, "capacity": cap, "status": "active",
             "gsi1pk": "plant", "gsi1sk": pid})
        put(table, {"pk": pid, "sk": f"LOCATED_AT#{loc_id}",
             "edge_type": "located_at", "location_id": loc_id})
    print(f"  {len(data)}件")


def seed_warehouses(table) -> None:
    """自社倉庫を投入"""
    print("自社倉庫を投入中...")
    data = [
        ("WHS001", "東京中央倉庫", "東京都江東区", 35.6729, 139.8171, 10000, "LOC001"),
        ("WHS002", "大阪港倉庫", "大阪府大阪市", 34.6500, 135.4300, 8000, "LOC002"),
        ("WHS003", "名古屋物流倉庫", "愛知県名古屋市", 35.1500, 136.8800, 6000, "LOC003"),
        ("WHS004", "福岡配送センター", "福岡県福岡市", 33.6000, 130.4200, 5000, "LOC006"),
        ("WHS005", "仙台保管倉庫", "宮城県仙台市", 38.2500, 140.8500, 4000, "LOC008"),
    ]
    for wid, name, loc_name, lat, lon, cap, loc_id in data:
        put(table, {"pk": wid, "sk": "META", "node_type": "warehouse",
             "name": name, "location_name": loc_name,
             "lat": lat, "lon": lon, "capacity": cap, "status": "active",
             "gsi1pk": "warehouse", "gsi1sk": wid})
        put(table, {"pk": wid, "sk": f"LOCATED_AT#{loc_id}",
             "edge_type": "located_at", "location_id": loc_id})
    print(f"  {len(data)}件")


def seed_suppliers(table) -> None:
    """サプライヤーを投入"""
    print("サプライヤーを投入中...")
    data = [
        ("SUP001", "九州半導体", "日本", "九州", 33.2490, 131.6127),
        ("SUP002", "関西電子部品", "日本", "関西", 34.6851, 135.8050),
        ("SUP003", "東北素材", "日本", "東北", 39.7036, 141.1527),
        ("SUP004", "北海道金属", "日本", "北海道", 43.7687, 142.3650),
        ("SUP005", "沖縄部品供給", "日本", "沖縄", 26.5013, 127.9454),
        ("SUP006", "中部精密機器", "日本", "中部", 35.3912, 136.7223),
        ("SUP007", "関東化学工業", "日本", "関東", 36.0652, 140.1234),
        ("SUP101", "シリコンウェハー九州", "日本", "九州", 33.1500, 131.5000),
        ("SUP102", "レアメタル北陸", "日本", "北陸", 36.5613, 136.6562),
        ("SUP103", "化学原料四国", "日本", "四国", 33.8416, 132.7657),
    ]
    for sid, name, country, region, lat, lon in data:
        put(table, {"pk": sid, "sk": "META", "node_type": "supplier",
             "name": name, "country": country, "region": region,
             "lat": lat, "lon": lon, "status": "active",
             "gsi1pk": "supplier", "gsi1sk": sid})
    print(f"  {len(data)}件")


def seed_customers(table) -> None:
    """カスタマを投入"""
    print("カスタマを投入中...")
    data = [
        ("CUS001", "トヨタ自動車", "自動車", 35.0844, 137.1531),
        ("CUS002", "パナソニック", "電子機器", 34.7872, 135.4382),
        ("CUS003", "三菱重工業", "産業機械", 35.4544, 139.6319),
        ("CUS004", "ソニー", "家電", 35.6195, 139.7414),
        ("CUS005", "オリンパス", "医療機器", 35.6580, 139.7016),
        ("CUS006", "川崎重工業", "航空宇宙", 34.6784, 135.1956),
        ("CUS007", "日立製作所", "電機", 35.6812, 139.7671),
        ("CUS008", "デンソー", "自動車部品", 34.8833, 137.1167),
    ]
    for cid, name, industry, lat, lon in data:
        put(table, {"pk": cid, "sk": "META", "node_type": "customer",
             "name": name, "industry": industry,
             "lat": lat, "lon": lon,
             "gsi1pk": "customer", "gsi1sk": cid})
    print(f"  {len(data)}件")


def seed_products(table) -> None:
    """製品を投入"""
    print("製品を投入中...")
    data = [
        ("PRD001", "半導体チップA", "component", "pcs"),
        ("PRD002", "半導体チップB", "component", "pcs"),
        ("PRD003", "ディスプレイパネル", "component", "pcs"),
        ("PRD004", "バッテリーモジュール", "component", "pcs"),
        ("PRD005", "制御ユニット", "finished", "pcs"),
        ("PRD006", "センサーアセンブリ", "finished", "pcs"),
        ("PRD007", "電源ユニット", "finished", "pcs"),
        ("PRD008", "シリコン原料", "raw_material", "kg"),
        ("PRD009", "銅線", "raw_material", "kg"),
        ("PRD010", "プラスチック筐体", "component", "pcs"),
    ]
    for pid, name, ptype, unit in data:
        put(table, {"pk": pid, "sk": "META", "node_type": "product",
             "name": name, "product_type": ptype, "unit": unit,
             "gsi1pk": "product", "gsi1sk": pid})
    print(f"  {len(data)}件")


def seed_product_relations(table) -> None:
    """製品の生産・消費関係（PRODUCES / CONSUMES）を投入"""
    print("製品関係を投入中...")
    # (node_id, edge_type, product_id)
    data: list[tuple[str, str, str]] = [
        # ── T2 Supplier（原材料生産） ──
        ("SUP101", "produces", "PRD008"),  # シリコンウェハー九州 → シリコン原料
        ("SUP102", "produces", "PRD009"),  # レアメタル北陸 → 銅線
        ("SUP103", "produces", "PRD010"),  # 化学原料四国 → プラスチック筐体
        # ── T1 Supplier（部品生産） ──
        ("SUP001", "consumes", "PRD008"),  # 九州半導体 ← シリコン原料
        ("SUP001", "produces", "PRD001"),  # 九州半導体 → 半導体チップA
        ("SUP002", "produces", "PRD003"),  # 関西電子部品 → ディスプレイパネル
        ("SUP003", "consumes", "PRD009"),  # 東北素材 ← 銅線
        ("SUP003", "produces", "PRD002"),  # 東北素材 → 半導体チップB
        ("SUP004", "consumes", "PRD009"),  # 北海道金属 ← 銅線
        ("SUP004", "produces", "PRD009"),  # 北海道金属 → 銅線（精錬）
        ("SUP005", "produces", "PRD004"),  # 沖縄部品供給 → バッテリーモジュール
        ("SUP006", "consumes", "PRD010"),  # 中部精密機器 ← プラスチック筐体
        ("SUP006", "produces", "PRD010"),  # 中部精密機器 → プラスチック筐体（加工）
        ("SUP007", "produces", "PRD001"),  # 関東化学工業 → 半導体チップA
        ("SUP007", "produces", "PRD006"),  # 関東化学工業 → センサーアセンブリ
        # ── Plant（組立・加工） ──
        ("PLT001", "consumes", "PRD001"),  # 東京組立 ← 半導体チップA
        ("PLT001", "consumes", "PRD002"),  # 東京組立 ← 半導体チップB
        ("PLT001", "consumes", "PRD003"),  # 東京組立 ← ディスプレイパネル
        ("PLT001", "consumes", "PRD006"),  # 東京組立 ← センサーアセンブリ
        ("PLT001", "produces", "PRD005"),  # 東京組立 → 制御ユニット
        ("PLT002", "consumes", "PRD002"),  # 大阪製造 ← 半導体チップB
        ("PLT002", "consumes", "PRD004"),  # 大阪製造 ← バッテリーモジュール
        ("PLT002", "consumes", "PRD010"),  # 大阪製造 ← プラスチック筐体
        ("PLT002", "produces", "PRD007"),  # 大阪製造 → 電源ユニット
        ("PLT003", "consumes", "PRD003"),  # 名古屋電子 ← ディスプレイパネル
        ("PLT003", "produces", "PRD003"),  # 名古屋電子 → ディスプレイパネル（検査済）
        ("PLT004", "consumes", "PRD001"),  # 宮古島半導体 ← 半導体チップA
        ("PLT004", "produces", "PRD001"),  # 宮古島半導体 → 半導体チップA（加工済）
        ("PLT005", "consumes", "PRD004"),  # 那覇 ← バッテリーモジュール
        ("PLT005", "produces", "PRD004"),  # 那覇 → バッテリーモジュール（検査済）
        ("PLT006", "consumes", "PRD001"),  # 福岡組立 ← 半導体チップA
        ("PLT006", "consumes", "PRD004"),  # 福岡組立 ← バッテリーモジュール
        ("PLT006", "produces", "PRD005"),  # 福岡組立 → 制御ユニット
        ("PLT007", "produces", "PRD002"),  # 広島部品 → 半導体チップB
        ("PLT008", "consumes", "PRD002"),  # 仙台製造 ← 半導体チップB
        ("PLT008", "consumes", "PRD009"),  # 仙台製造 ← 銅線
        ("PLT008", "produces", "PRD002"),  # 仙台製造 → 半導体チップB（加工済）
        ("PLT008", "produces", "PRD006"),  # 仙台製造 → センサーアセンブリ
        ("PLT009", "consumes", "PRD006"),  # 横浜精密 ← センサーアセンブリ
        ("PLT009", "produces", "PRD006"),  # 横浜精密 → センサーアセンブリ（精密加工済）
        # ── Customer（消費のみ） ──
        ("CUS001", "consumes", "PRD005"),  # トヨタ ← 制御ユニット
        ("CUS002", "consumes", "PRD007"),  # パナソニック ← 電源ユニット
        ("CUS003", "consumes", "PRD005"),  # 三菱重工 ← 制御ユニット
        ("CUS004", "consumes", "PRD005"),  # ソニー ← 制御ユニット
        ("CUS005", "consumes", "PRD006"),  # オリンパス ← センサーアセンブリ
        ("CUS006", "consumes", "PRD007"),  # 川崎重工 ← 電源ユニット
        ("CUS007", "consumes", "PRD005"),  # 日立 ← 制御ユニット
        ("CUS008", "consumes", "PRD003"),  # デンソー ← ディスプレイパネル
    ]
    for node_id, edge_type, product_id in data:
        sk_prefix = "PRODUCES" if edge_type == "produces" else "CONSUMES"
        put(table, {
            "pk": node_id, "sk": f"{sk_prefix}#{product_id}",
            "edge_type": edge_type, "product_id": product_id,
        })
    print(f"  {len(data)}件")


def seed_relations(table) -> None:
    """供給関係を投入"""
    print("供給関係を投入中...")
    data: list[tuple[str, str, str, str]] = [
        ("SUP101", "SUP001", "supplier", "supplier"),
        ("SUP102", "SUP003", "supplier", "supplier"),
        ("SUP102", "SUP004", "supplier", "supplier"),
        ("SUP103", "SUP006", "supplier", "supplier"),
        ("SUP001", "PLT004", "supplier", "plant"),
        ("SUP002", "PLT003", "supplier", "plant"),
        ("SUP003", "PLT008", "supplier", "plant"),
        ("SUP004", "PLT008", "supplier", "plant"),
        ("SUP005", "PLT005", "supplier", "plant"),
        ("SUP006", "PLT002", "supplier", "plant"),
        ("SUP007", "PLT001", "supplier", "plant"),
        ("SUP007", "PLT009", "supplier", "plant"),
        ("PLT001", "WHS001", "plant", "warehouse"),
        ("PLT002", "WHS002", "plant", "warehouse"),
        ("PLT006", "WHS004", "plant", "warehouse"),
        ("PLT008", "WHS005", "plant", "warehouse"),
        ("PLT004", "PLT001", "plant", "plant"),
        ("PLT004", "PLT006", "plant", "plant"),
        ("PLT005", "PLT002", "plant", "plant"),
        ("PLT005", "PLT006", "plant", "plant"),
        ("PLT003", "PLT001", "plant", "plant"),
        ("PLT007", "PLT002", "plant", "plant"),
        ("PLT008", "PLT001", "plant", "plant"),
        ("PLT009", "PLT001", "plant", "plant"),
        ("WHS001", "CUS001", "warehouse", "customer"),
        ("WHS001", "CUS004", "warehouse", "customer"),
        ("WHS001", "CUS007", "warehouse", "customer"),
        ("WHS002", "CUS002", "warehouse", "customer"),
        ("WHS002", "CUS006", "warehouse", "customer"),
        ("WHS004", "CUS003", "warehouse", "customer"),
        ("PLT003", "CUS008", "plant", "customer"),
        ("PLT008", "CUS005", "plant", "customer"),
    ]
    for from_id, to_id, from_type, to_type in data:
        put(table, {
            "pk": from_id, "sk": f"SUPPLIES_TO#{to_id}",
            "edge_type": "supplies_to",
            "to_id": to_id, "from_type": from_type, "to_type": to_type,
            "gsi2pk": to_id, "gsi2sk": f"SUPPLIES_TO#{from_id}",
        })
    print(f"  {len(data)}件")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", help="AWS profile name")
    args = parser.parse_args()

    table = get_table(args.profile)

    print("=" * 60)
    print("SupplyChainMaster 種子データ投入")
    print("=" * 60)
    seed_locations(table)
    seed_plants(table)
    seed_warehouses(table)
    seed_suppliers(table)
    seed_customers(table)
    seed_products(table)
    seed_product_relations(table)
    seed_relations(table)
    print("\n投入完了!")


if __name__ == "__main__":
    main()

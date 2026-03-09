# サプライチェーンマスタデータ設計書

## 概要

```
DynamoDB (Master) ──同期──> Neptune Analytics (分析用)
```

## サプライチェーンフロー

```
T2 Supplier ──> T1 Supplier ──> Plant ──> Warehouse ──> Customer
                                  │                        ▲
                                  └── (直送) ──────────────┘
```

## テーブル: SupplyChainMaster

pk / sk の2パターンだけ:

| データ | pk | sk | 例 |
|--------|-----|-----|-----|
| ノード | ID | `META` | `PLT001 / META` |
| 関係 | 供給元 | `SUPPLIES_TO#供給先` | `SUP001 / SUPPLIES_TO#PLT004` |
| 所在地 | 拠点ID | `LOCATED_AT#ロケーションID` | `PLT001 / LOCATED_AT#LOC001` |

## ノード種別（node_type）

| node_type | ID接頭辞 | 例 |
|-----------|---------|-----|
| location | LOC | LOC001 東京都千代田区 |
| plant | PLT | PLT001 東京組立工場 |
| warehouse | WHS | WHS001 東京中央倉庫 |
| supplier | SUP | SUP001 九州半導体 |
| customer | CUS | CUS001 トヨタ自動車 |
| product | PRD | PRD001 半導体チップA |

## GSI

| GSI | PK | SK | 用途 |
|-----|-----|-----|------|
| GSI1 | gsi1pk (node_type) | gsi1sk (ID) | タイプ別一覧 |
| GSI2 | gsi2pk (供給先ID) | gsi2sk (`SUPPLIES_TO#供給元ID`) | 逆引き（供給先→供給元） |

## 関係（エッジ）

| 関係 | pk | sk | 説明 |
|------|-----|-----|------|
| SUPPLIES_TO | from_id | `SUPPLIES_TO#to_id` | 供給関係（全種別共通） |
| LOCATED_AT | 拠点ID | `LOCATED_AT#LOC_ID` | 工場・倉庫の所在地 |

SUPPLIES_TO の from_type / to_type の組み合わせ:

| from_type | to_type | 意味 |
|-----------|---------|------|
| supplier | supplier | T2→T1 サプライヤー間供給 |
| supplier | plant | T1 サプライヤー→工場 |
| plant | plant | 工場間の部品供給 |
| plant | warehouse | 工場→倉庫（出荷） |
| warehouse | customer | 倉庫→カスタマ（配送） |
| plant | customer | 工場→カスタマ（直送） |

## Tier の算出方法

Tier はテーブルに保存しない。SUPPLIES_TO 関係をたどり、アプリ側で算出する。

```python
from boto3.dynamodb.conditions import Key

# 1. 全サプライヤーを取得
resp = table.query(
    IndexName="GSI1",
    KeyConditionExpression=Key("gsi1pk").eq("supplier"))
all_suppliers = {item["pk"] for item in resp["Items"]}

# 2. 各サプライヤーの供給先を取得
edges = {}  # from_id → [to_id, ...]
for sid in all_suppliers:
    resp = table.query(
        KeyConditionExpression=(
            Key("pk").eq(sid) & Key("sk").begins_with("SUPPLIES_TO#")))
    edges[sid] = [item["to_id"] for item in resp["Items"]]

# 3. Tier算出
#   T1 = Plant に直接供給する Supplier
#   T2 = T1 Supplier に供給する Supplier
t1 = {sid for sid, targets in edges.items()
      if any(t.startswith("PLT") for t in targets)}
t2 = {sid for sid, targets in edges.items()
      if any(t in t1 for t in targets) and sid not in t1}
```

## アクセスパターン

| やりたいこと | クエリ |
|-------------|--------|
| 全工場一覧 | GSI1: `gsi1pk=plant` |
| 全倉庫一覧 | GSI1: `gsi1pk=warehouse` |
| 全サプライヤー一覧 | GSI1: `gsi1pk=supplier` |
| 全カスタマ一覧 | GSI1: `gsi1pk=customer` |
| 拠点の供給先 | `pk=PLT001, sk begins_with SUPPLIES_TO#` |
| 拠点の供給元（逆引き） | GSI2: `gsi2pk=PLT001` |
| 工場の所在地 | `pk=PLT001, sk begins_with LOCATED_AT#` |
| 下流チェーン | SUPPLIES_TO# を再帰的にたどる |
| 上流チェーン | GSI2 逆引きを再帰的にたどる |

## 別テーブル: SupplyChainOrders（未実装）

販売伝票・購買伝票はマスタテーブルに含めず、別テーブルで管理する予定。

## デプロイ

```bash
python scripts/seed_supply_chain_master_data.py
```

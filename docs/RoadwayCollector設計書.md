# RoadwayCollector 設計書

Yahoo!道路交通情報からの交通規制データ収集機能の詳細設計

---

## 1. 概要

### 目的

Yahoo!道路交通情報（roadway.yahoo.co.jp）をクロールし、日本全国47都道府県の高速道路・自動車専用道路の交通規制情報を収集する。
収集データは専用 DynamoDB テーブル（RoadwayTraffic）に保存し、SCMリスク監視の交通障害カテゴリ（traffic）のファクトデータとして活用する。

### データソース

| 項目 | 内容 |
|------|------|
| サイト | Yahoo!道路交通情報 (roadway.yahoo.co.jp) |
| 元データ | JARTIC（日本道路交通情報センター） |
| 対象 | 高速道路・自動車専用道路（全国） |
| 更新頻度 | 約5〜10分間隔（JARTIC更新に準拠） |

### 設計方針

- **2フェーズ分離**: 道路マスタ収集（初回のみ）と規制情報収集（定期実行）を分離
- **差分検知**: 規制の発生・解除を検知し、イベントとして記録
- **専用テーブル**: TrendTable とは分離した RoadwayTraffic テーブルを使用
- **既存パターン踏襲**: Lambda Layer、EventBridge Schedule、S3 生データ保存の既存パターンを流用

---

## 2. アーキテクチャ

### データフロー

```
                 初期構築（1回のみ）
  ┌──────────────────────────────────────────────────────────┐
  │                                                          │
  │  scripts/seed_road_master.py                             │
  │    GET /traffic/pref/{1~47}/list (×47)                   │
  │    → HTML解析 → 道路リンク抽出                            │
  │    → DynamoDB RoadwayTraffic に道路マスタ登録             │
  │                                                          │
  └──────────────────────────────────────────────────────────┘

                 定期収集（5分間隔）
  ┌──────────────────────────────────────────────────────────┐
  │                                                          │
  │  EventBridge(5分)                                        │
  │    │                                                     │
  │    ▼                                                     │
  │  RoadwayCollector Lambda                                 │
  │    │                                                     │
  │    ├─ 1. DynamoDB から道路マスタ一覧を取得               │
  │    │                                                     │
  │    ├─ 2. 各道路の規制情報ページをクロール                │
  │    │     GET /traffic/pref/{pref}/road/{road}/list       │
  │    │     → HTML解析 → 上り/下り規制テーブル抽出          │
  │    │                                                     │
  │    ├─ 3. 差分検知                                        │
  │    │     前回 ACTIVE 規制 vs 今回取得データ               │
  │    │     → 新規規制: EVENT作成 + ACTIVE登録              │
  │    │     → 規制解除: ACTIVE削除 + cleared_at更新         │
  │    │                                                     │
  │    └─ 4. S3 に生データ保存                               │
  │          raw/roadway/{date}/{timestamp}.json              │
  │                                                          │
  └──────────────────────────────────────────────────────────┘

                 既存パイプラインとの連携（将来）
  ┌──────────────────────────────────────────────────────────┐
  │                                                          │
  │  RoadwayTraffic.ACTIVE 規制検知                          │
  │    → SNS Topic 通知                                      │
  │    → FactChecker のファクトソースとして参照可能           │
  │                                                          │
  └──────────────────────────────────────────────────────────┘
```

---

## 3. クロール対象サイト構造

### URL体系

```
roadway.yahoo.co.jp
│
├── /pref                                      都道府県一覧（入口）
│
├── /traffic/pref/{pref_id}/list               県別の道路一覧
│   └── /traffic/pref/{pref_id}/road/{road_id}/list   道路の規制詳細
│
└── /traffic/pref/{pref_id}/road/{road_id}/list   道路の規制詳細
```

### 都道府県コード（標準 JIS X 0401）

```
 1=北海道  2=青森  3=岩手  4=宮城  5=秋田  6=山形  7=福島
 8=茨城   9=栃木  10=群馬 11=埼玉 12=千葉 13=東京 14=神奈川
15=新潟  16=富山  17=石川 18=福井 19=山梨 20=長野
21=岐阜  22=静岡  23=愛知 24=三重
25=滋賀  26=京都  27=大阪 28=兵庫 29=奈良 30=和歌山
31=鳥取  32=島根  33=岡山 34=広島 35=山口
36=徳島  37=香川  38=愛媛 39=高知
40=福岡  41=佐賀  42=長崎 43=熊本 44=大分 45=宮崎 46=鹿児島
47=沖縄
```

### 道路ID（road_id）の規則

road_id は連番ではなく、サイト独自の採番体系を持つ。規則性がないため、県別一覧ページからのリンク解析で収集する。

実測データ（北海道の例）:

| 道路名 | road_id |
|--------|---------|
| 道央道 | `1001001` |
| 札樽道 | `1003001` |
| 後志道 | `1003101` |
| 道東道 | `1005001` |
| 深川留萌道 | `3100070` |
| 日高道 | `3100090` |
| 旭川紋別道 | `3503040` |

---

## 4. ページ解析仕様

### 4.1 県別道路一覧ページ

**URL**: `/traffic/pref/{pref_id}/list`

**抽出対象**: 道路リンク

```html
<!-- 実際のHTML構造（推定） -->
<a href="/traffic/pref/1/road/1005001/list">道東道</a>
<a href="/traffic/pref/1/road/1005001/list#up-lane">上り</a>
<a href="/traffic/pref/1/road/1005001/list#down-lane">下り</a>
```

**解析ロジック**:

```
1. <a> タグの href が /traffic/pref/{pref_id}/road/{road_id}/list にマッチするものを抽出
2. 正規表現: /traffic/pref/\d+/road/(\d+)/list
3. road_id とリンクテキスト（道路名）を取得
4. 重複排除（同一ページ内に上り/下りで複数回出現するため）
```

### 4.2 道路規制詳細ページ

**URL**: `/traffic/pref/{pref_id}/road/{road_id}/list`

**ページ構成**:

```
┌───────────────────────────────────────────────────────────┐
│  {道路名}の事故・渋滞情報                                  │
│  情報：JARTIC  |  {月}月{日}日 {時}時{分}分 現在           │
├───────────────────────────────────────────────────────────┤
│                                                           │
│  <h2>{道路名}（上り）</h2>                                │
│                                                           │
│  パターンA: 規制なし                                       │
│  「現在、通行止め・規制情報はありません。」                  │
│                                                           │
│  パターンB: 規制あり                                       │
│  ┌──────────────┬──────────┬──────────┐                   │
│  │ 規制区間      │ 規制内容  │ （原因）  │                   │
│  ├──────────────┼──────────┼──────────┤                   │
│  │ A IC → B IC  │ 通行止め  │ 事故     │                   │
│  │ C付近        │ 速度規制  │ 雪       │                   │
│  └──────────────┴──────────┴──────────┘                   │
│                                                           │
│  <h2>{道路名}（下り）</h2>                                │
│  （同様の構造）                                            │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

**抽出フィールド**:

| フィールド | 抽出方法 | 例 |
|-----------|---------|-----|
| `road_name` | `<h1>` テキストから「の事故・渋滞情報」を除去 | `道東道` |
| `fetched_at` | `{月}月{日}日 {時}時{分}分 現在` をパース | `2026-03-16T18:35:00+09:00` |
| `direction` | `<h2>` テキストから `（上り）`/`（下り）` を抽出 | `上り` / `下り` |
| `has_regulation` | 「規制情報はありません」の有無で判定 | `true` / `false` |
| `section` | 規制テーブル 1列目 | `下トマムトンネル出口付近` |
| `regulation_type` | 規制テーブル 2列目 | `通行止め` / `速度規制` / `チェーン規制` |
| `cause` | 規制テーブル 3列目（存在しない場合あり） | `事故` / `故障車` / `雪` / `工事` |

### 規制内容の種別（実測 + 想定）

| regulation_type | 重要度 | 説明 |
|----------------|:---:|------|
| `通行止め` | 高 | 全面通行止め（最も深刻） |
| `速度規制` | 中 | 速度制限（50km/h等） |
| `チェーン規制` | 中 | 冬季チェーン装着義務 |
| `車線規制` | 低 | 一部車線の規制 |
| `規制なし` | - | 事象はあるが規制は解除済み |

---

## 5. DynamoDB 設計

### テーブル: RoadwayTraffic

既存 TrendTable とは独立した専用テーブル。キー名は SupplyChainMaster に合わせ小文字。

#### テーブル定義

| 設定 | 値 |
|------|-----|
| テーブル名 | `roadway-traffic-{stage}` |
| 課金モード | PAY_PER_REQUEST |
| PK | `pk` (String) |
| SK | `sk` (String) |
| GSI1 | `gsi1pk` (String) / `gsi1sk` (String) |
| GSI2 | `gsi2pk` (String) / `gsi2sk` (String) |
| TTL | `ttl` (Number) |

### Entity 1: 道路マスタ（ROAD_MASTER）

道路の基本情報。初回クロール時に登録し、以降は更新日時のみ更新。

| キー | 値 | 例 |
|------|-----|-----|
| pk | `ROAD#{road_id}` | `ROAD#1005001` |
| sk | `META` | `META` |
| gsi1pk | `PREF#{pref_id}` | `PREF#01` |
| gsi1sk | `ROAD#{road_id}` | `ROAD#1005001` |

| 属性 | 型 | 説明 | 例 |
|------|-----|------|-----|
| road_name | String | 道路名 | `道東道` |
| pref_id | String | 都道府県コード（ゼロ埋め2桁） | `01` |
| pref_name | String | 都道府県名 | `北海道` |
| road_type | String | 道路種別 | `expressway` |
| source_url | String | クロール元URL | `/traffic/pref/1/road/1005001/list` |
| updated_at | String | 最終更新日時（ISO 8601） | `2026-03-16T18:35:00+09:00` |

### Entity 2: 規制イベント（TRAFFIC_EVENT）

規制の発生を検知した時点で作成。解除時に `cleared_at` を更新し、`gsi2pk` を削除。

| キー | 値 | 例 |
|------|-----|-----|
| pk | `ROAD#{road_id}` | `ROAD#1005001` |
| sk | `EVENT#{iso_timestamp}#{direction}` | `EVENT#2026-03-16T18:35:00#下り` |
| gsi1pk | `PREF#{pref_id}` | `PREF#01` |
| gsi1sk | `EVENT#{iso_timestamp}` | `EVENT#2026-03-16T18:35:00` |
| gsi2pk | `ACTIVE` | `ACTIVE`（規制中のみ。解除時に削除） |
| gsi2sk | `PREF#{pref_id}#ROAD#{road_id}` | `PREF#01#ROAD#1005001` |

| 属性 | 型 | 説明 | 例 |
|------|-----|------|-----|
| road_name | String | 道路名 | `道東道` |
| pref_id | String | 都道府県コード | `01` |
| pref_name | String | 都道府県名 | `北海道` |
| direction | String | 方向 | `下り` |
| section | String | 規制区間 | `下トマムトンネル出口付近` |
| regulation_type | String | 規制内容 | `通行止め` |
| cause | String | 原因 | `故障車` |
| detected_at | String | 検知日時（ISO 8601） | `2026-03-16T18:35:00+09:00` |
| cleared_at | String / null | 解除日時 | `null` → `2026-03-16T20:00:00+09:00` |
| source | String | データソース | `yahoo_roadway` |
| ttl | Number | 自動削除（30日後） | `1742140800` |

### アクセスパターン

| # | ユースケース | インデックス | キー条件 |
|:---:|------------|:--------:|---------|
| 1 | 道路のマスタ情報取得 | Main | `pk = ROAD#{road_id}, sk = META` |
| 2 | 道路の規制イベント履歴 | Main | `pk = ROAD#{road_id}, sk begins_with EVENT#` |
| 3 | 県内の全道路一覧 | GSI1 | `gsi1pk = PREF#{pref_id}, gsi1sk begins_with ROAD#` |
| 4 | 県内の規制イベント一覧 | GSI1 | `gsi1pk = PREF#{pref_id}, gsi1sk begins_with EVENT#` |
| 5 | 全国の現在有効な規制一覧 | GSI2 | `gsi2pk = ACTIVE` |
| 6 | 県別の現在有効な規制 | GSI2 | `gsi2pk = ACTIVE, gsi2sk begins_with PREF#{pref_id}` |

### ACTIVE パターンによるリアルタイム規制管理

```
規制発生時:
  gsi2pk = "ACTIVE"               ← GSI2 に出現
  gsi2sk = "PREF#01#ROAD#1005001"

規制解除時:
  gsi2pk = null (属性削除)         ← GSI2 から消失
  cleared_at = "2026-03-16T20:00:00+09:00" を設定

GSI2 をクエリするだけで「現時点の全国規制一覧」が即座に取得可能。
規制解除後もイベントレコード自体は Main テーブルに残り、履歴として参照可能（TTL で 30 日後に自動削除）。
```

---

## 6. Lambda 関数設計

### RoadwayCollector

| 項目 | 内容 |
|------|------|
| Lambda関数名 | `roadway-collector-{stage}` |
| ハンドラ | `roadway_collector_function.lambda_handler` |
| CodeUri | `function/roadway_collector/` |
| 実行間隔 | 5分（EventBridge Schedule） |
| 外部通信 | Yahoo!道路交通情報（HTTP GET） |
| 入力 | DynamoDB RoadwayTraffic（道路マスタ） |
| 出力 | DynamoDB RoadwayTraffic（規制イベント）+ S3 生データ |
| ライブラリ | beautifulsoup4（HTML解析） |
| タイムアウト | 900秒 |
| メモリ | 512MB |

### 処理フロー

```
lambda_handler(event)
  │
  ├─ 1. DynamoDB から道路マスタ一覧を取得
  │     GSI1 Query: gsi1pk begins_with PREF#
  │     → 全道路の road_id, source_url を取得
  │     → 推定 約470件
  │
  ├─ 2. DynamoDB から現在 ACTIVE な規制一覧を取得
  │     GSI2 Query: gsi2pk = ACTIVE
  │     → 前回時点の規制状態（差分検知用）
  │
  ├─ 3. 各道路の規制情報ページをクロール
  │     GET https://roadway.yahoo.co.jp{source_url}
  │     → BeautifulSoup で HTML 解析
  │     → 上り/下り の規制テーブルを抽出
  │     ※ リクエスト間に 0.5〜1秒のスリープ（レート制限対策）
  │
  ├─ 4. 差分検知
  │     現在の ACTIVE 規制セット vs 今回クロール結果を比較
  │
  │     ┌─ 新規規制（今回あり & 前回なし）:
  │     │   → DynamoDB に EVENT アイテムを PutItem
  │     │   → gsi2pk = ACTIVE を設定
  │     │
  │     ├─ 継続規制（今回あり & 前回あり）:
  │     │   → 変更なし（スキップ）
  │     │
  │     └─ 規制解除（今回なし & 前回あり）:
  │         → gsi2pk を削除（REMOVE gsi2pk, gsi2sk）
  │         → cleared_at を設定
  │
  ├─ 5. S3 に生データ保存
  │     raw/roadway/{date}/{timestamp}.json
  │     → 全道路のクロール結果をまとめて保存
  │
  └─ 6. ログ出力
        道路数、新規規制数、解除数、エラー数を記録
```

### クロール最適化

```
全道路クロール時の見積:
  道路数: 約470件
  リクエスト間隔: 0.5秒
  合計所要時間: 470 × 0.5 = 約235秒（約4分）
  → Lambda タイムアウト 900秒 以内に完了

並列化（将来の最適化案）:
  asyncio + aiohttp で 5並列 → 約47秒に短縮
  ※ 初期実装はシンプルに逐次処理
```

### エラーハンドリング

| ケース | 対応 |
|--------|------|
| HTTP タイムアウト（10秒） | 該当道路をスキップ、他は継続 |
| HTTP 4xx/5xx | ログ出力、該当道路スキップ |
| HTML 解析失敗 | ログ出力、該当道路スキップ |
| DynamoDB 書込失敗 | リトライ（boto3 デフォルト） |
| 全道路のクロール失敗 | CloudWatch Alarm で通知 |

---

## 7. 道路マスタ投入スクリプト

### scripts/seed_road_master.py

道路マスタの初回投入用スクリプト。47都道府県の道路一覧ページをクロールし、全道路を DynamoDB に登録する。

| 項目 | 内容 |
|------|------|
| スクリプトパス | `scripts/seed_road_master.py` |
| 実行方法 | `python scripts/seed_road_master.py <テーブル名>` |
| 外部依存 | requests, beautifulsoup4 |
| 所要時間 | 約1分（47リクエスト） |

### 処理フロー

```
main()
  │
  ├─ 1. 47都道府県をループ
  │     GET https://roadway.yahoo.co.jp/traffic/pref/{1~47}/list
  │     ※ リクエスト間に 1秒のスリープ
  │
  ├─ 2. HTML 解析
  │     <a href="/traffic/pref/{pref_id}/road/{road_id}/list">道路名</a>
  │     → road_id, road_name を抽出
  │     → 重複排除（同一道路が上り/下りで2回出現）
  │
  ├─ 3. DynamoDB に BatchWriteItem
  │     pk = ROAD#{road_id}
  │     sk = META
  │     gsi1pk = PREF#{pref_id}
  │     gsi1sk = ROAD#{road_id}
  │     + 属性（road_name, pref_id, pref_name, road_type, source_url）
  │
  └─ 4. 結果サマリ出力
        都道府県別の道路数、総道路数を表示
```

### 都道府県名マッピング

```python
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
```

---

## 8. S3 保存設計

### 生データ保存パス

```
{BucketName}/
  └── raw/
      └── roadway/
          └── {date}/
              └── {timestamp}.json     # 全道路のスナップショット
```

例: `raw/roadway/2026-03-16/2026-03-16T18-35-00.json`

### 生データフォーマット

```json
{
  "fetched_at": "2026-03-16T18:35:00+09:00",
  "source": "yahoo_roadway",
  "road_count": 470,
  "regulation_count": 12,
  "roads": [
    {
      "road_id": "1005001",
      "road_name": "道東道",
      "pref_id": "01",
      "directions": [
        {
          "direction": "上り",
          "has_regulation": false,
          "regulations": []
        },
        {
          "direction": "下り",
          "has_regulation": true,
          "regulations": [
            {
              "section": "下トマムトンネル出口付近",
              "regulation_type": "規制なし",
              "cause": "故障車"
            }
          ]
        }
      ]
    }
  ]
}
```

### S3 ライフサイクルルール

```yaml
- Id: ExpireRoadwayRawData
  Prefix: raw/roadway/
  Status: Enabled
  ExpirationInDays: 30
```

---

## 9. template.yaml 追加リソース

### 新規 DynamoDB テーブル

```yaml
RoadwayTrafficTable:
  Type: AWS::DynamoDB::Table
  Properties:
    TableName: !Sub roadway-traffic-${StageName}
    BillingMode: PAY_PER_REQUEST
    AttributeDefinitions:
      - AttributeName: pk
        AttributeType: S
      - AttributeName: sk
        AttributeType: S
      - AttributeName: gsi1pk
        AttributeType: S
      - AttributeName: gsi1sk
        AttributeType: S
      - AttributeName: gsi2pk
        AttributeType: S
      - AttributeName: gsi2sk
        AttributeType: S
    KeySchema:
      - AttributeName: pk
        KeyType: HASH
      - AttributeName: sk
        KeyType: RANGE
    GlobalSecondaryIndexes:
      - IndexName: GSI1
        KeySchema:
          - AttributeName: gsi1pk
            KeyType: HASH
          - AttributeName: gsi1sk
            KeyType: RANGE
        Projection:
          ProjectionType: ALL
      - IndexName: GSI2
        KeySchema:
          - AttributeName: gsi2pk
            KeyType: HASH
          - AttributeName: gsi2sk
            KeyType: RANGE
        Projection:
          ProjectionType: ALL
    TimeToLiveSpecification:
      AttributeName: ttl
      Enabled: true
```

### 新規 Lambda 関数

```yaml
RoadwayCollectorFunction:
  Type: AWS::Serverless::Function
  Properties:
    FunctionName: !Sub roadway-collector-${StageName}
    Handler: roadway_collector_function.lambda_handler
    CodeUri: function/roadway_collector/
    Role: !GetAtt LambdaExecutionRole.Arn
    Layers:
      - !Ref CommonLayer
    Environment:
      Variables:
        ROADWAY_TABLE_NAME: !Ref RoadwayTrafficTable
        BUCKET_NAME: !Ref TrendOutputBucket
        STAGE: !Ref StageName
    Events:
      ScheduleEvent:
        Type: Schedule
        Properties:
          Schedule: rate(5 minutes)
          Enabled: false
```

### IAM ポリシー追加

既存 `LambdaExecutionRole` に RoadwayTraffic テーブルへのアクセス権を追加:

```yaml
- Effect: Allow
  Action:
    - dynamodb:Query
    - dynamodb:GetItem
    - dynamodb:PutItem
    - dynamodb:UpdateItem
    - dynamodb:DeleteItem
  Resource:
    - !GetAtt RoadwayTrafficTable.Arn
    - !Sub ${RoadwayTrafficTable.Arn}/index/*
```

### Lambda Layer 依存追加

`layers/common/requirements.txt` に追加:

```
beautifulsoup4
```

### Outputs 追加

```yaml
RoadwayTrafficTableName:
  Value: !Ref RoadwayTrafficTable
RoadwayCollectorArn:
  Value: !GetAtt RoadwayCollectorFunction.Arn
```

---

## 10. ディレクトリ構成（追加分）

```
trend-monitor/
├── function/
│   └── roadway_collector/              # 新規
│       └── roadway_collector_function.py
├── scripts/
│   └── seed_road_master.py             # 新規
├── tests/
│   └── test_roadway_collector.py       # 新規
└── docs/
    └── RoadwayCollector設計書.md        # 本ドキュメント
```

---

## 11. リスク・注意事項

### クロール制限

| リスク | 対策 |
|--------|------|
| Yahoo! による IP ブロック | リクエスト間隔 0.5〜1秒、User-Agent 設定 |
| robots.txt 制限 | 実装前に robots.txt を確認し遵守 |
| HTML 構造変更 | 解析失敗をログ監視、CloudWatch Alarm 設定 |
| サイト利用規約違反 | 利用規約を確認し、商用利用可否を判断 |

### パフォーマンス

| 項目 | 見積 |
|------|------|
| 道路数（全国） | 約470件 |
| 1回のクロール所要時間 | 約4分（逐次）/ 約50秒（5並列） |
| DynamoDB 書込（規制変更時のみ） | 平均 10〜50件/回 |
| S3 生データサイズ | 約 500KB/回 |
| Lambda コスト（512MB × 900秒 × 288回/日） | 約 $3.5/月 |

### 将来の拡張

| 項目 | 説明 |
|------|------|
| 一般道路対応 | Yahoo!道路交通情報の対象範囲次第 |
| JARTIC 直接連携 | 公式API公開時に切替検討 |
| FactChecker 連携 | `ACTIVE` 規制を FactChecker のファクトソースとして参照 |
| SNS 通知 | 通行止め検知時に SNS Topic へ即時通知 |

# FactChecker 詳細設計書

カテゴリ分類済みイベント＋ファクトデータを照合し、リスクイベントを判定・管理する。

---

## 1. 概要

### ■ 位置づけ

設計書.md Step 3（ファクトチェック）＋ Step 4（リスク評価）の実装設計。
CategoryClassifier の出力（classified/）と FactCollector の出力（facts/、RoadwayTraffic）を
突合し、信頼度付きのリスクイベントを EventTable に書き込む。

### ■ 設計方針

- **双方向トリガー**: ツイート起点（classified → facts 照合）とファクト起点（facts → ノード照合）の両方をカバー
- **2段AIパイプライン**: Haiku で重要度フィルタ → Sonnet でノードマッチ（コスト最適化）
- **収束型イベントモデル**: 同一事象の複数ソースを1つのイベントにマージし、信頼度を段階的に上げる
- **ノードインデックスキャッシュ**: SupplyChainMaster への高頻度アクセスを回避

### ■ スコープ

| 対象 | 対象外 |
|------|--------|
| FactChecker Lambda（3トリガー） | RiskNotifier（通知機能は別変更） |
| EventTable DynamoDB テーブル | ダッシュボード/管理画面 |
| ノードインデックス生成スクリプト | SupplyChainMaster の拡張 |
| 2段AIパイプライン | 人的確認ワークフロー |

---

## 2. 全体フロー

```
  データ収集層（実装済み）
  ════════════════════════════════════════════════════════

  TrendFetcher ──┐
  KeywordSearch ─┤──▶ S3: raw/ ──▶ CategoryClassifier ──▶ S3: classified/
                 │
  JmaCollector ────▶ S3: facts/jma/latest/
  NewsCollector ───▶ S3: facts/news/latest/
  OfficialCollector▶ S3: facts/official/
  RoadwayCollector ▶ DynamoDB: RoadwayTraffic

  ファクトチェック層（本設計）
  ════════════════════════════════════════════════════════

  S3: classified/ ─────────────┐
  S3: facts/ ──────────────────┤──▶ FactChecker Lambda
  DynamoDB: RoadwayTraffic ────┘         │
       (Streams)                         │
                                         ▼
                                    EventTable
                                    (DynamoDB)
                                         │
                                         ▼ (将来)
                                    RiskNotifier
```

---

## 3. トリガー定義

### ■ Trigger A: classified 起点（ツイート → ファクト照合）

| 項目 | 内容 |
|------|------|
| イベントソース | EventBridge S3 Object Created |
| フィルタ | `prefix: classified/` |
| 処理 | classified 読込 → 全 fact ソース照合 → EventTable 書込み |
| Stage 1 | **スキップ**（CategoryClassifier で関連性判定済み） |
| Stage 2 | Sonnet ノードマッチ + ファクト照合 |

```
S3: classified/{category}/{date}/{ulid}.json
  │
  ▼
FactChecker
  ├─ classified event 読込（category_id, related_nodes, ai_confidence）
  ├─ S3: node_location_index.json 読込
  ├─ カテゴリ別ファクトソース照合（Stage 2 Sonnet）
  │    earthquake → facts/jma/latest/quake_list.json, tsunami.json
  │    flood      → facts/jma/latest/typhoon.json + facts/news/latest/flood.json
  │    traffic    → RoadwayTraffic Query + facts/news/ + facts/official/
  │    fire       → facts/news/latest/fire.json + facts/official/
  │    infra      → facts/news/latest/infra.json + facts/official/
  │    labor      → facts/news/latest/labor.json
  │    geopolitics→ facts/news/latest/geopolitics.json
  │    pandemic   → facts/news/latest/pandemic.json
  ├─ 信頼度スコア計算
  ├─ Dedup 検索（GSI2 Query）
  └─ EventTable PUT or UPDATE
```

### ■ Trigger B: facts 起点（ファクト → ノード照合）

| 項目 | 内容 |
|------|------|
| イベントソース | EventBridge S3 Object Created |
| フィルタ | `prefix: facts/` |
| 処理 | fact 読込 → Stage 1 フィルタ → Stage 2 ノードマッチ → EventTable 書込み |
| Stage 1 | Haiku で重要度フィルタ |
| Stage 2 | Sonnet でノードマッチ（pass した場合のみ） |

```
S3: facts/{source}/...
  │
  ▼
FactChecker
  ├─ fact データ読込
  ├─ Stage 1: Haiku 重要度フィルタ（バッチ処理）
  │    → skip（大半）→ 終了
  │    → pass ↓
  ├─ S3: node_location_index.json 読込
  ├─ Stage 2: Sonnet ノードマッチ
  │    → no match → 終了
  │    → match ↓
  ├─ Dedup 検索（GSI2 Query）
  └─ EventTable PUT or UPDATE
```

### ■ Trigger C: Roadway 起点（道路規制 → ノード照合）

| 項目 | 内容 |
|------|------|
| イベントソース | DynamoDB Streams（RoadwayTraffic テーブル） |
| フィルタ | INSERT イベントのみ |
| 処理 | 新規規制 → Stage 1 フィルタ → Stage 2 ノードマッチ → EventTable 書込み |
| Stage 1 | Haiku で重要度フィルタ（通行止め判定等） |
| Stage 2 | Sonnet でノードマッチ |

```
DynamoDB Streams: RoadwayTraffic (INSERT)
  │
  ▼
FactChecker
  ├─ Stream レコードから新規規制情報を抽出
  ├─ Stage 1: Haiku 重要度フィルタ
  │    → 速度規制のみ → skip → 終了
  │    → 通行止め等 → pass ↓
  ├─ S3: node_location_index.json 読込
  ├─ Stage 2: Sonnet ノードマッチ
  │    → 区間名/IC名 と ノード関連インフラを照合
  │    → match → EventTable 書込み
  ├─ Dedup 検索
  └─ EventTable PUT or UPDATE
```

### ■ トリガー別処理まとめ

| | Trigger A (classified) | Trigger B (facts) | Trigger C (Roadway) |
|---|---|---|---|
| Stage 1 (Haiku) | スキップ | 実行 | 実行 |
| Stage 2 (Sonnet) | 実行 | pass時のみ | pass時のみ |
| ノードインデックス | 読込 | pass時のみ読込 | pass時のみ読込 |
| ファクトソース照合 | カテゴリ別に全照合 | 自身がファクト | Roadway自身 |
| Dedup | 実行 | 実行 | 実行 |

---

## 4. 2段AIパイプライン

### ■ Stage 1: 重要度フィルタ（Haiku）

**目的**: SCMリスクとして意味のある情報だけを Stage 2 に通す。

| 項目 | 内容 |
|------|------|
| モデル | Claude Haiku 4.5 (`anthropic.claude-haiku-4-5-20251001`) |
| 入力 | fact データのみ（ノード一覧は渡さない → input 小 → 低コスト） |
| 出力 | `pass` / `skip` + 理由 |
| バッチ処理 | 1トリガーの全 fact を1回の API 呼出しで処理 |

#### プロンプト

```
System:
あなたはサプライチェーンリスクのスクリーニング担当です。
以下の情報が、日本国内の製造業サプライチェーンに
影響を与える可能性があるか判定してください。

【判定基準】
・地震: 震度3以上、または津波警報・注意報 → pass
・台風: 上陸予想または暴風域接近 → pass
・道路: 通行止め、大規模車線規制 → pass
       速度規制、チェーン規制のみ → skip
・ニュース: 工場・港湾・物流・インフラへの実害を示唆する記事 → pass
          一般的な報道、被害報告なし → skip
・公式SNS: 災害・規制・障害に関する具体的な発表 → pass
           定期報告・広報・お知らせ → skip

【出力形式】JSON配列のみ:
[
  {"fact_index": 0, "decision": "pass", "reason": "震度4、操業影響の可能性"},
  {"fact_index": 1, "decision": "skip", "reason": "震度1、影響なし"}
]

User:
[fact データの配列]
```

#### コスト見積もり

```
平常時:
  JMA 288回/日 × ~500 input tokens = 144K tokens → ~$0.004/日
  Roadway ~10回/日 × ~300 input tokens = 3K tokens → ~$0.0001/日
  News 48回/日 × ~2000 input tokens = 96K tokens → ~$0.003/日
  Official ~50回/日 × ~500 input tokens = 25K tokens → ~$0.001/日
  ──────────────────────────────────────
  Stage 1 合計: ~$0.01/日（ほぼ無視可能）
```

### ■ Stage 2: ノードマッチ（Sonnet）

**目的**: Stage 1 を pass した fact が、具体的にどのサプライチェーンノードに影響するかを判定。

| 項目 | 内容 |
|------|------|
| モデル | Claude Sonnet 4.6 (`jp.anthropic.claude-sonnet-4-6`) |
| 入力 | fact データ + ノードインデックス（system prompt, cache_control: ephemeral） |
| 出力 | マッチしたノード、影響要約、関連度スコア |

#### プロンプト

```
System: (cache_control: ephemeral)
あなたはサプライチェーンリスク分析の専門家です。
以下のサプライチェーン拠点一覧と、新着のリスク情報を照合し、
影響を受ける拠点があるか判定してください。

【サプライチェーン拠点一覧】
ID: PLT001 | 豊田組立工場 | plant
  所在地: 愛知県豊田市
  関連インフラ: 豊田IC, 東名高速, 伊勢湾岸道, 名鉄三河線
  生産: エンジン部品, シャシー
ID: WH002 | 市川物流センター | warehouse
  所在地: 千葉県市川市
  関連インフラ: 首都高湾岸線, 京葉道路, 国道357号, JR京葉線
...

【出力形式】JSON配列のみ:
[
  {
    "fact_index": 0,
    "matched_node_ids": ["PLT001"],
    "impact_summary": "東名高速 豊田JCT付近の通行止めにより、豊田組立工場への部品搬入経路が遮断される可能性",
    "relevance_score": 85,
    "reasoning": "通行止め区間が工場最寄りICを含む"
  }
]
影響なしの場合は空配列 [] を返してください。

【重要】
・道路規制の区間名（IC名/JCT名）と拠点の最寄りICの地理的関係を考慮すること
・震源地名・震度観測地点と拠点所在地の地理的近接性を考慮すること
・間接影響（物流経路の遮断、港湾閉鎖による原材料入荷停止等）も考慮すること
・拠点名の完全一致だけでなく、略称・地域名・間接的な言及も考慮すること

User:
[Stage 1 を pass した fact データの配列]
```

#### Trigger A（classified 起点）での Stage 2 利用

classified イベントの場合、fact データの代わりに「classified event + 各ファクトソースのデータ」を投入:

```
User:
{
  "classified_event": {
    "category_id": "earthquake",
    "summary": "豊田市で強い揺れ、工場への影響懸念",
    "ai_confidence": 75,
    "related_nodes": [{"id": "PLT001", ...}]
  },
  "fact_sources": [
    {"source": "jma", "type": "quake_list", "data": [...]},
    {"source": "news", "type": "articles", "data": [...]},
    {"source": "official", "type": "tweets", "data": [...]}
  ]
}
```

出力に `fact_match_details`（どのファクトソースがマッチしたか）を含める。

---

## 5. ノードインデックスキャッシュ

### ■ 目的

FactChecker が SupplyChainMaster に毎回 Query するのを回避する。

### ■ 仕様

| 項目 | 内容 |
|------|------|
| S3 キー | `config/node_location_index.json` |
| 生成方法 | `scripts/generate_node_index.py` |
| 更新頻度 | 日次（EventBridge Schedule）or 手動 |
| データソース | SupplyChainMaster (plants, warehouses, T1/T2 suppliers) |
| S3 Lifecycle | なし（設定データのため永続） |

### ■ フォーマット

```json
{
  "generated_at": "2026-03-19T00:00:00Z",
  "node_count": 15,
  "nodes": [
    {
      "id": "PLT001",
      "name": "豊田組立工場",
      "node_type": "plant",
      "tier": null,
      "location_name": "愛知県豊田市",
      "related_infra": ["豊田IC", "東名高速", "伊勢湾岸道", "名鉄三河線"],
      "products": ["エンジン部品", "シャシー"]
    },
    {
      "id": "WH002",
      "name": "市川物流センター",
      "node_type": "warehouse",
      "tier": null,
      "location_name": "千葉県市川市",
      "related_infra": ["首都高湾岸線", "京葉道路", "国道357号", "JR京葉線"],
      "products": []
    }
  ]
}
```

### ■ Stage 2 プロンプトへの変換

`nodes` 配列を以下のテキスト形式に変換し、system prompt に埋め込む:

```
ID: PLT001 | 豊田組立工場 | plant | 所在地: 愛知県豊田市
  関連インフラ: 豊田IC, 東名高速, 伊勢湾岸道, 名鉄三河線
  生産: エンジン部品, シャシー
```

`cache_control: ephemeral` を使用するため、同一ノード一覧での2回目以降の呼出しはキャッシュヒットで安くなる。

---

## 6. EventTable DynamoDB 設計

### ■ テーブル定義

| 項目 | 内容 |
|------|------|
| テーブル名 | `event-table-${StageName}` |
| 課金モード | PAY_PER_REQUEST |
| TTL | `ttl`（30日） |

### ■ キー設計

| キー | 属性名 | 型 | 値 |
|------|--------|-----|-----|
| PK | `PK` | S | `EVT#{event_id}` |
| SK | `SK` | S | `META` |
| GSI1PK | `GSI1PK` | S | `STATUS#{status}` |
| GSI1SK | `GSI1SK` | S | `{created_at}` (ISO 8601) |
| GSI2PK | `GSI2PK` | S | `CAT#{category_id}` |
| GSI2SK | `GSI2SK` | S | `{created_at}` (ISO 8601) |

### ■ 属性一覧

| 属性名 | 型 | 説明 |
|--------|-----|------|
| `event_id` | S | ULID |
| `status` | S | `CONFIRMED` / `PENDING` / `WATCHING` / `DISMISSED` |
| `category_id` | S | リスクカテゴリ（earthquake, traffic, ...） |
| `category_name` | S | カテゴリ日本語名 |
| `summary` | S | 影響要約（100字以内） |
| `source_type` | S | イベント起点: `classified` / `fact` |
| `ai_confidence` | N | CategoryClassifier の信頼度（0-100、fact起点時は null） |
| `fact_score` | N | ファクト照合で加算されたスコア |
| `final_confidence` | N | min(ai_confidence + fact_score, 100) |
| `risk_level` | N | リスクレベル（1/2/3） |
| `related_nodes` | L | マッチしたノード詳細 |
| `fact_sources` | L | マッチしたファクトソース詳細 |
| `classified_s3_key` | S | 元の classified/ S3キー（nullable） |
| `raw_s3_key` | S | 元の raw/ S3キー（nullable） |
| `created_at` | S | イベント作成時刻（ISO 8601） |
| `updated_at` | S | 最終更新時刻（ISO 8601） |
| `reviewed_by` | S | 人的確認者（nullable、将来用） |
| `ttl` | N | TTL エポック秒（created_at + 30日） |

### ■ アクセスパターン

| ユースケース | 操作 | キー条件 |
|------------|------|---------|
| 要確認イベント一覧 | GSI1 Query | `GSI1PK = "STATUS#PENDING"` |
| 確定リスク一覧 | GSI1 Query | `GSI1PK = "STATUS#CONFIRMED"` |
| カテゴリ別イベント | GSI2 Query | `GSI2PK = "CAT#earthquake"` |
| 直近N時間のイベント | GSI2 Query | `GSI2PK = "CAT#...", GSI2SK > {N時間前}` |
| 特定イベント取得 | GetItem | `PK = "EVT#{id}", SK = "META"` |
| Dedup 検索 | GSI2 Query + Filter | `GSI2PK = "CAT#...", GSI2SK between {2h前} and {now}`, filter: node_id |

### ■ `fact_sources` 要素のフォーマット

```json
{
  "source": "jma",
  "data_type": "quake_list",
  "matched_text": "愛知県西部 震度5弱 豊田市:震度4",
  "matched_at": "2026-03-19T14:25:00Z",
  "score_added": 80
}
```

### ■ `related_nodes` 要素のフォーマット

```json
{
  "id": "PLT001",
  "name": "豊田組立工場",
  "node_type": "plant",
  "impact_summary": "震度4の揺れにより生産ライン停止の可能性",
  "relevance_score": 90
}
```

---

## 7. 収束型イベントモデル

### ■ コンセプト

同一事象について複数ソースが時間差で情報をもたらす場合、
新規イベントを作るのではなく既存イベントを「育てる」（マージ＆信頼度更新）。

### ■ 同一イベント判定

```
判定キー: category_id + matched_node_id + time_window(2時間)

EventTable GSI2 で検索:
  GSI2PK = "CAT#{category_id}"
  GSI2SK between {2時間前} and {現在}
  Filter: related_nodes[].id contains {matched_node_id}
```

### ■ 判定結果別処理

| 結果 | 処理 |
|------|------|
| ヒットなし | 新規イベント PUT |
| 1件ヒット | 既存イベント UPDATE（fact_sources 追加、スコア再計算） |
| 複数ヒット | 最新のイベントを UPDATE |

### ■ UPDATE 時の処理

```
1. fact_sources に新しいソースを追加
2. fact_score を再計算（全 fact_sources の score_added 合計）
3. final_confidence を再計算
4. status が上昇する場合のみ更新
   （WATCHING → PENDING → CONFIRMED は上昇、逆方向は不可）
5. updated_at を更新
6. GSI1PK (STATUS#) を必要に応じて更新
```

### ■ シナリオ例: 豊田市で震度5の地震

```
T+1分  JmaCollector → facts/jma/latest/quake_list.json 更新
       → Trigger B 起動
       → Stage 1 (Haiku): 震度5 → pass
       → Stage 2 (Sonnet): "豊田市 震度4" ∩ PLT001 → match
       → Dedup: CAT#earthquake + PLT001 + 2h → ヒットなし
       → 新規作成: EVT#001
         source_type=fact, ai_confidence=null, fact_score=80
         final_confidence=80, status=CONFIRMED
         fact_sources=[{source:"jma", data_type:"quake_list", score_added:80}]

T+5分  CategoryClassifier → classified/earthquake/... 出力
       → Trigger A 起動
       → classified: category=earthquake, nodes=[PLT001], ai_confidence=75
       → ファクト照合: JMA match あり
       → Dedup: CAT#earthquake + PLT001 + 2h → EVT#001 ヒット
       → UPDATE: EVT#001
         ai_confidence=75, classified_s3_key=... を追記
         final_confidence=min(75+80, 100)=100

T+10分 NewsCollector → facts/news/latest/earthquake.json 更新
       → Trigger B 起動
       → Stage 1 (Haiku): "豊田市で震度5弱" → pass
       → Stage 2 (Sonnet): PLT001 match
       → Dedup → EVT#001 ヒット
       → UPDATE: fact_sources に news を追加
         内容はさらに充実するが final_confidence は既に100

T+12分 OfficialCollector → facts/official/... 更新
       → Trigger B 起動
       → Stage 1 (Haiku): @UN_NERV 地震情報 → pass
       → Stage 2 (Sonnet): PLT001 match
       → Dedup → EVT#001 ヒット
       → UPDATE: fact_sources に official を追加
```

---

## 8. 信頼度スコア・ステータス判定

### ■ fact_score 加算ルール

| ファクトソース | マッチ条件 | score_added |
|--------------|----------|:-----------:|
| JMA（気象庁） | 拠点所在地の震度/津波/台風 | **80** |
| Roadway（道路交通情報） | 拠点関連道路の通行止め | **80** |
| News（3件以上） | 信頼メディア3件以上マッチ | **50** |
| News（1件以上） | 信頼メディア1件以上マッチ | **30** |
| Official（公式SNS） | 公式アカウントのツイートマッチ | **40** |

> JMA・Roadway は公式データのため score_added = 80。
> 設計書: 「公式発表があれば確定リスクに即昇格」に対応。

### ■ final_confidence 計算

```
classified 起点:
  final_confidence = min(ai_confidence + fact_score, 100)

fact 起点:
  final_confidence = fact_score（ai_confidence は null）

収束時（classified + fact が合流）:
  final_confidence = min(ai_confidence + Σ(fact_sources[].score_added), 100)
```

### ■ ステータス判定

| final_confidence | status | 意味 |
|:---:|---|---|
| 80〜100 | `CONFIRMED` | 確定リスク。公式データで裏付けあり。 |
| 50〜79 | `PENDING` | 要確認。情報はあるが公式裏付け不十分。 |
| 30〜49 | `WATCHING` | 監視継続。リスク兆候はあるが影響限定的。 |
| 0〜29 | `DISMISSED` | 除外。誤検知または無関係。 |

### ■ risk_level 判定

| level | 条件 | 通知レベル（将来） |
|:---:|------|---|
| 3 | CONFIRMED かつ重大事象（震度5以上、主要道路通行止め、工場火災等） | 即時電話＋メール＋Slack |
| 2 | CONFIRMED かつ中程度事象 | メール＋Slack |
| 1 | PENDING または WATCHING | Slackのみ / ダッシュボード |

> risk_level の判定は Stage 2 Sonnet の出力 `relevance_score` も参考にする。

---

## 9. 入出力仕様

### ■ 入力: classified イベント（Trigger A）

S3 キー: `classified/{category_id}/{date}/{ulid}.json`

```json
{
  "event_id": "01JXX...",
  "classified_at": "2026-03-19T14:30:00Z",
  "source": "keyword_route",
  "category_id": "earthquake",
  "category_name": "地震・津波",
  "raw_s3_key": "raw/2026-03-19/01JXX.json",
  "trend_name": "豊田市 地震",
  "summary": "豊田市で強い揺れ。工場への影響懸念。",
  "ai_confidence": 75,
  "reasoning": "複数の具体的なツイートで一致",
  "related_nodes": [
    {"id": "PLT001", "name": "豊田組立工場", "node_type": "plant"}
  ]
}
```

### ■ 入力: JMA ファクト（Trigger B）

S3 キー: `facts/jma/latest/quake_list.json`

```json
{
  "fetched_at": "2026-03-19T14:25:00Z",
  "source": "jma",
  "data_type": "quake_list",
  "raw_data": [
    {
      "eid": "20260319142300",
      "at": "2026-03-19T14:23:00+09:00",
      "anm": "愛知県西部",
      "mag": "5.2",
      "maxi": "5-",
      "int": [
        {
          "code": "23",
          "maxi": "5-",
          "city": [
            {"code": "2321100", "maxi": "4"}
          ]
        }
      ]
    }
  ]
}
```

### ■ 入力: News ファクト（Trigger B）

S3 キー: `facts/news/latest/{category}.json`

```json
{
  "fetched_at": "2026-03-19T14:30:00Z",
  "source": "google_news",
  "category": "earthquake",
  "article_count": 15,
  "articles": [
    {
      "title": "愛知県で震度5弱の地震 豊田市でも強い揺れ",
      "link": "https://...",
      "pub_date": "2026-03-19T14:25:00Z",
      "description": "19日午後2時23分ごろ...",
      "source_name": "NHKニュース"
    }
  ]
}
```

### ■ 入力: Official ファクト（Trigger B）

S3 キー: `facts/official/{date}/{HHmm}.json`

```json
{
  "fetched_at": "2026-03-19T14:25:00Z",
  "result_count": 3,
  "tweets": [
    {
      "id": "190000...",
      "author_username": "UN_NERV",
      "author_name": "特務機関NERV防災",
      "text": "【地震情報】19日14時23分頃、愛知県西部を震源とする地震...",
      "created_at": "2026-03-19T14:24:00Z",
      "metrics": {"retweet_count": 500, "like_count": 1200}
    }
  ]
}
```

### ■ 入力: Roadway ファクト（Trigger C）

DynamoDB Streams レコード:

```json
{
  "pk": "ROAD#123",
  "sk": "EVENT#2026-03-19T14:30:00#上り",
  "road_name": "東名高速",
  "direction": "上り",
  "section": "音羽蒲郡IC 豊田JCT",
  "regulation_type": "通行止め",
  "cause": "事故",
  "pref_name": "愛知県"
}
```

### ■ 出力: EventTable アイテム

```json
{
  "PK": "EVT#01JXXX",
  "SK": "META",
  "GSI1PK": "STATUS#CONFIRMED",
  "GSI1SK": "2026-03-19T14:25:00Z",
  "GSI2PK": "CAT#earthquake",
  "GSI2SK": "2026-03-19T14:25:00Z",

  "event_id": "01JXXX",
  "status": "CONFIRMED",
  "category_id": "earthquake",
  "category_name": "地震・津波",
  "summary": "愛知県西部で震度5弱。豊田組立工場（PLT001）所在地で震度4を観測。生産ライン停止の可能性。",
  "source_type": "fact",
  "ai_confidence": 75,
  "fact_score": 80,
  "final_confidence": 100,
  "risk_level": 3,

  "related_nodes": [
    {
      "id": "PLT001",
      "name": "豊田組立工場",
      "node_type": "plant",
      "impact_summary": "震度4の揺れにより生産ライン停止の可能性",
      "relevance_score": 95
    }
  ],

  "fact_sources": [
    {
      "source": "jma",
      "data_type": "quake_list",
      "matched_text": "愛知県西部 M5.2 最大震度5弱 豊田市:震度4",
      "matched_at": "2026-03-19T14:25:00Z",
      "score_added": 80
    },
    {
      "source": "news",
      "data_type": "article",
      "matched_text": "愛知県で震度5弱の地震 豊田市でも強い揺れ (NHKニュース)",
      "matched_at": "2026-03-19T14:32:00Z",
      "score_added": 30
    },
    {
      "source": "official",
      "data_type": "tweet",
      "matched_text": "@UN_NERV: 【地震情報】愛知県西部を震源とする地震...",
      "matched_at": "2026-03-19T14:26:00Z",
      "score_added": 40
    }
  ],

  "classified_s3_key": "classified/earthquake/2026-03-19/01JXX.json",
  "raw_s3_key": "raw/2026-03-19/01JXX.json",
  "created_at": "2026-03-19T14:25:00Z",
  "updated_at": "2026-03-19T14:32:00Z",
  "reviewed_by": null,
  "ttl": 1750521900
}
```

---

## 10. 新規ファイル一覧

| ファイル | 説明 |
|---------|------|
| `function/fact_checker/__init__.py` | パッケージ初期化 |
| `function/fact_checker/fact_checker_function.py` | FactChecker Lambda メイン |
| `layers/common/fact_matcher.py` | 2段AIパイプライン共通ロジック |
| `scripts/generate_node_index.py` | ノードインデックス生成スクリプト |

## 11. template.yaml 変更

| リソース | 種類 | 説明 |
|---------|------|------|
| `EventTable` | DynamoDB Table | PK/SK + GSI1 + GSI2 + TTL |
| `FactCheckerFunction` | Lambda | 3トリガー: classified/ + facts/ + Streams |
| `RoadwayTrafficTable` | 変更 | StreamSpecification 追加（NEW_IMAGE） |
| `LambdaExecutionRole` | 変更 | EventTable R/W + Bedrock InvokeModel 追加 |
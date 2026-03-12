# FactCollector / FactChecker 設計書

ファクト収集（3 Lambda）＋ファクト照合（1 Lambda）の詳細設計

---

## 1. アーキテクチャ全体像

### ■ 設計方針

- **収集と照合を分離する**: FactCollector がファクト情報を事前収集し、FactChecker は収集済みデータを参照するだけ
- **混合ストレージ**: スナップショット型データは S3、時系列ストリーム型データは DynamoDB
- **増分取得**: 全 Collector で差分取得を実装し、重複・API浪費を排除
- **既存パターン踏襲**: xdk ライブラリ、EventBridge Schedule、共通 Layer を流用

### ■ データフロー

```
                収集層
  ┌──────────────────────────────────────────────────────────┐
  │                                                          │
  │  EventBridge(1分) ──▶ JmaCollector ──▶ S3 facts/jma/     │
  │  EventBridge(10分) ──▶ NewsCollector ──▶ DynamoDB FACT#   │
  │  EventBridge(5分) ──▶ OfficialCollector ──▶ DynamoDB FACT#│
  │                                                          │
  └──────────────────────────────────────────────────────────┘

  EventBridge(5分) ──▶ TrendFetcher ──▶ S3 raw/
  EventBridge(5分) ──▶ KeywordSearch ──▶ S3 raw/
                                            │
                 S3 raw/ Created (EventBridge)
                                            │
                分類層                       ▼
                              CategoryClassifier ──▶ S3 classified/
                                                          │
                 S3 classified/ Created (EventBridge)      │
                                                          ▼
                検証層                  FactChecker ──▶ S3 verified/
                                            │
                                            ├── S3 facts/jma/ を読取
                                            ├── DynamoDB FACT#NEWS を Query
                                            └── DynamoDB FACT#OFFICIAL を Query
```

### ■ 混合ストレージの根拠

| データ種別 | 性質 | ストレージ | 理由 |
|-----------|------|-----------|------|
| 気象庁データ | スナップショット（APIが常に全量返す） | **S3** | 100件超のリストを毎分 DynamoDB に書く必要なし |
| ニュース記事 | 時系列ストリーム（増分蓄積） | **DynamoDB** | 時間範囲クエリ、TTL自動削除が必要 |
| 公式ツイート | 時系列ストリーム（since_id増分） | **DynamoDB** | 同上 |

---

## 2. JmaCollector（気象庁データ収集）

### ■ 基本仕様

| 項目 | 内容 |
|------|------|
| Lambda関数名 | `jma-collector-{stage}` |
| ハンドラ | `jma_collector_function.lambda_handler` |
| 実行間隔 | 1分 |
| 外部API | 気象庁 bosai JSON API（非公式・無認証・無料） |
| 出力先 | S3 `facts/jma/latest/` |
| 追加依存 | なし（urllib + json のみ） |
| タイムアウト | 30秒 |
| メモリ | 256MB |

### ■ 取得対象

| # | データ | エンドポイント | 対応カテゴリ |
|---|--------|--------------|------------|
| 1 | 地震情報一覧 | `https://www.jma.go.jp/bosai/quake/data/list.json` | earthquake |
| 2 | 津波警報 | `https://www.jma.go.jp/bosai/tsunami/data/list.json` | earthquake |
| 3 | 台風情報 | `https://www.jma.go.jp/bosai/typhoon/data/targetTc.json` | flood |
| 4 | 気象警報 | `https://www.jma.go.jp/bosai/warning/data/warning/{areaCode}.json` | flood |

### ■ S3 保存設計

```
facts/jma/latest/
  ├── quake_list.json      # 地震一覧（毎回全量上書き）
  ├── tsunami.json         # 津波警報（毎回上書き、空配列もあり得る）
  ├── typhoon.json         # 台風情報（毎回上書き）
  └── warning.json         # 気象警報（監視エリア分を統合）
```

各ファイルの統一ラッパーフォーマット:

```json
{
  "fetched_at": "2026-03-12T10:00:00Z",
  "source": "jma",
  "data_type": "quake_list",
  "raw_data": [ ... ]
}
```

- `raw_data` に気象庁APIのレスポンスをそのまま格納
- FactChecker は `raw_data` 内のフィールドを参照（詳細は下記）

### ■ 地震データの主要フィールド（実測確認済み、2026-03-12）

レスポンスは過去約15日分（約200件）の地震情報配列。新しい順にソート済み。

| フィールド | 型 | 説明 | FactChecker での活用 |
|-----------|-----|------|---------------------|
| `ctt` | string | 作成日時 (YYYYMMDDHHmmss) | 増分取得のカーソルとして使用 |
| `eid` | string | イベントID (YYYYMMDDHHmmss) | イベント一意識別 |
| `at` | string | 地震発生時刻 (ISO 8601 JST) | classified の時刻と照合 |
| `anm` | string | 震源地名（日本語） | 地名テキストマッチ |
| `en_anm` | string | 震源地名（英語） | - |
| `cod` | string | 震源座標+深さ (`+緯度+経度-深さ(m)/`) | 拠点との距離計算 |
| `mag` | string | マグニチュード | リスク重大度判定 |
| `maxi` | string | 最大震度 ("1"〜"7", "5-", "5+", "6-", "6+") | 影響度判定 |
| `ttl` | string | 情報種別（4種、下記参照） | フィルタリング |
| `ift` | string | 情報フラグ ("発表" 等) | - |
| `acd` | string | 震源地域コード（数値） | エリア照合 |
| `int` | array | 震度分布（都道府県→市区町村の階層） | 拠点所在地とのマッチ |
| `json` | string | 詳細JSONファイル名 | 必要に応じて詳細取得 |

#### `cod` フィールドの解析

```
cod = "+36.4+137.6-10000/"
       │     │      │
       緯度   経度   深さ(m) → 10km
```

拠点の緯度経度との距離を計算し、影響範囲内かを自動判定可能。

#### `int` の階層構造

```
int[] ← 都道府県レベル
  ├── code: "21"              ← 都道府県コード (JIS X 0401: 01=北海道〜47=沖縄)
  ├── maxi: "2"               ← その都道府県の最大震度
  └── city[] ← 市区町村レベル
        ├── code: "2121700"   ← 市区町村コード (7桁、先頭2桁=都道府県)
        └── maxi: "2"         ← その市区町村の震度
```

FactChecker は `int[].city[].code` と拠点の市区町村コードを照合し、拠点所在地の震度を特定する。

#### `ttl` の種類（4種）

| ttl | 内容 | FactChecker での扱い |
|-----|------|---------------------|
| `震源・震度情報` | 震源+震度の確定情報 | メインで使用（最も詳細） |
| `震源に関する情報` | 震源のみ（震度なし） | 補助 |
| `震度速報` | 速報値（震源未確定の場合あり） | 速報性重視 |
| `南海トラフ地震関連解説情報` | 南海トラフ特別情報 | 最重要アラート |

### ■ 処理フロー

```
lambda_handler
  │
  ├─ 1. 気象庁 quake/data/list.json を GET
  │     → facts/jma/latest/quake_list.json に上書き保存
  │
  ├─ 2. 気象庁 tsunami/data/list.json を GET
  │     → facts/jma/latest/tsunami.json に上書き保存
  │
  ├─ 3. 気象庁 typhoon/data/targetTc.json を GET
  │     → facts/jma/latest/typhoon.json に上書き保存
  │
  └─ 4. 気象庁 warning/data/warning/{areaCode}.json を GET
        → 監視対象エリア分を取得し統合
        → facts/jma/latest/warning.json に上書き保存
```

### ■ 監視対象エリアコード

SupplyChainMaster の拠点所在地から対応する気象庁エリアコードを導出する。
初期段階は主要エリアをハードコードし、将来的に DynamoDB マスタから動的取得に切替可能。

| areaCode | 地域 | 関連拠点（例） |
|----------|------|--------------|
| `130000` | 東京都 | 東京組立工場、東京中央倉庫 |
| `140000` | 神奈川県 | 横浜港関連 |
| `230000` | 愛知県 | 中部エリア拠点 |
| `270000` | 大阪府 | 大阪製造工場 |
| `400000` | 福岡県 | 九州エリア拠点 |

### ■ エラーハンドリング

- 気象庁APIタイムアウト（10秒）: 該当データ種別をスキップ、他は継続
- HTTPエラー（4xx/5xx）: ログ出力、前回の S3 ファイルはそのまま残る（上書きしない）
- レスポンスが空配列: 正常（津波なし等）。ラッパー付きで保存

---

## 3. NewsCollector（ニュース検索）

### ■ 基本仕様

| 項目 | 内容 |
|------|------|
| Lambda関数名 | `news-collector-{stage}` |
| ハンドラ | `news_collector_function.lambda_handler` |
| 実行間隔 | 10分 |
| 外部API | Google News RSS（Phase 1）/ Bing News API（Phase 2） |
| 出力先 | DynamoDB TrendTable `FACT#NEWS#{category_id}` |
| 追加依存 | なし（Phase 1: urllib + xml.etree のみ） |
| タイムアウト | 120秒 |
| メモリ | 256MB |

### ■ Phase 1: Google News RSS

#### 検索クエリ定義（全8カテゴリ）

| カテゴリ | 検索クエリ (q パラメータ) | 実測件数/日 |
|---------|------------------------|:---:|
| earthquake | `(地震 OR 津波 OR 震度) when:1d` | **100**（上限） |
| flood | `(台風 OR 大雨 OR 洪水 OR 浸水 OR 冠水) when:1d` | 30〜80 |
| fire | `(火災 OR 爆発 OR 工場火災) when:1d` | **100**（上限） |
| traffic | `(通行止め OR 運休 OR 遅延 OR 欠航) when:1d` | 50〜90 |
| infra | `(停電 OR 断水 OR 通信障害) when:1d` | 20〜50 |
| labor | `(ストライキ OR 操業停止 OR 労働争議 OR リコール) when:1d` | 10〜30 |
| geopolitics | `(関税 OR 制裁 OR 輸出規制 OR 貿易摩擦) when:1d` | **100**（上限） |
| pandemic | `(感染拡大 OR パンデミック OR 変異株) when:1d` | 10〜40 |

> **注意**: `when:1h` では結果が5件未満になるカテゴリがあるため `when:1d` を採用。
> earthquake / fire / geopolitics は100件上限に達するため、`pubDate` ベースの増分フィルタリングで重複排除する。

#### リクエストURL構築

```
https://news.google.com/rss/search?q={URLエンコード済クエリ}&hl=ja&gl=JP&ceid=JP:ja
```

#### 信頼メディアリスト（実測確認済み、2026-03-12）

RSS `<source>` タグの値でフィルタリングする。以下は実際のレスポンスから確認した値:

| メディア | `source` タグの実値 | 備考 |
|---------|-------------------|------|
| NHK | `NHKニュース` | 最も信頼性高。全カテゴリで出現 |
| 共同通信 | `47NEWS` | 共同通信の配信元は `47NEWS` として出現 |
| 時事通信 | `時事ドットコム` / `時事通信ニュース` | 2種の表記あり |
| 日本経済新聞 | `日本経済新聞` | |
| 読売新聞 | `読売新聞オンライン` | |
| 朝日新聞 | `朝日新聞デジタル` | |
| 毎日新聞 | `毎日新聞` | |
| 産経新聞 | `産経ニュース` | |
| Reuters | `Reuters` | 地政学カテゴリで出現 |
| Bloomberg | `Bloomberg` | 地政学カテゴリで出現 |

> **Yahoo!ニュースについて**: `Yahoo!ニュース` は最多出現メディアだが、アグリゲータ（転載元）のため
> 信頼メディアリストには**含めない**。元記事の `source` で判定する。

### ■ DynamoDB 保存設計

#### アイテム構造

```json
{
  "PK": "FACT#NEWS#earthquake",
  "SK": "2026-03-12T10:30:00Z",
  "GSI1PK": "TYPE#FACT_NEWS",
  "GSI1SK": "earthquake#2026-03-12T10:30:00Z",
  "fetched_at": "2026-03-12T10:30:00Z",
  "category_id": "earthquake",
  "query_used": "(地震 OR 津波 OR 震度) when:1d",
  "result_count": 5,
  "trusted_count": 3,
  "articles": [
    {
      "title": "東京で震度4の地震 津波の心配なし",
      "source": "NHKニュース",
      "pub_date": "2026-03-12T10:15:00Z",
      "url": "https://...",
      "is_trusted": true
    }
  ],
  "ttl": 1741910400
}
```

#### キー設計

| キー | 値 | 用途 |
|------|-----|------|
| PK | `FACT#NEWS#{category_id}` | カテゴリ別パーティション |
| SK | ISO 8601 タイムスタンプ | 時系列ソート |
| GSI1PK | `TYPE#FACT_NEWS` | 全ニュースファクト一覧 |
| GSI1SK | `{category_id}#{timestamp}` | カテゴリ＋時系列 |
| TTL | fetched_at + 24時間 | 自動削除 |

### ■ 処理フロー

```
lambda_handler
  │
  ├─ 1. 8カテゴリ分のクエリを順次実行
  │     GET https://news.google.com/rss/search?q=...&hl=ja&gl=JP&ceid=JP:ja
  │     ※ カテゴリ間に1秒のスリープ（レート制限対策）
  │
  ├─ 2. RSS XML をパース
  │     → item ごとに title, source, pubDate, link, description を抽出
  │     → source が信頼メディアリストに含まれるか判定 → is_trusted フラグ
  │
  ├─ 3. DynamoDB に PutItem
  │     PK = FACT#NEWS#{category_id}
  │     SK = 現在時刻 (ISO 8601)
  │     TTL = 24時間後のエポック秒
  │
  └─ 4. ログ出力
        各カテゴリの記事数、信頼メディア記事数を記録
```

### ■ 増分取得の方法

- `when:1d` で直近24時間のニュースを取得（`when:1h` では結果が少なすぎるため不採用）
- 10分間隔で実行するため、前回と記事が重複する
- **重複排除**: DynamoDB 保存前に記事の `title` + `source` でハッシュを生成し、同一ハッシュの記事は上書き（ConditionExpression で `attribute_not_exists`）
- 100件上限に達するカテゴリ（earthquake, fire, geopolitics）は最新ニュースが取得漏れする可能性があるが、10分間隔の定期取得で補完される

#### pubDate ベースフィルタリング

```
1. RSS レスポンスの全 item を取得
2. item.pubDate を ISO 8601 に変換
3. DynamoDB カーソルの last_pub_date 以降の記事のみ処理
4. 処理した記事の最新 pubDate をカーソルに保存
```

### ■ Phase 2 移行（Bing News API）

Phase 1 で Google News RSS の安定性に問題が出た場合、Bing News Search API に切替:

| 変更点 | Phase 1 (Google News RSS) | Phase 2 (Bing News API) |
|--------|--------------------------|------------------------|
| URL | `news.google.com/rss/search` | `api.bing.microsoft.com/v7.0/news/search` |
| 認証 | なし | Azure サブスクリプションキー |
| 形式 | RSS XML | JSON |
| パース | xml.etree | json.loads |
| 依存追加 | なし | Secrets Manager にキー追加 |
| コスト | 無料 | S0: 無料 (1,000 tx/月) |

切替は Lambda 関数内のデータ取得部分のみ。DynamoDB 保存フォーマットは同一。

---

## 4. OfficialCollector（公式アカウント監視）

### ■ 基本仕様

| 項目 | 内容 |
|------|------|
| Lambda関数名 | `official-collector-{stage}` |
| ハンドラ | `official_collector_function.lambda_handler` |
| 実行間隔 | 5分 |
| 外部API | X API v2 `search_recent` |
| 出力先 | DynamoDB TrendTable `FACT#OFFICIAL` |
| ライブラリ | xdk v0.9.0（既存Layer内） |
| タイムアウト | 120秒 |
| メモリ | 256MB |

### ■ 監視対象アカウント

#### DynamoDB マスタデータ

TrendTable に登録:

| PK | SK | GSI1PK | GSI1SK | screen_name | name | categories |
|----|-----|--------|--------|-------------|------|-----------|
| `OFFICIAL#UN_NERV` | `META` | `TYPE#OFFICIAL` | `PRI#1#UN_NERV` | `UN_NERV` | `特務機関NERV防災` | `["earthquake","flood"]` |
| `OFFICIAL#JMA_bousai` | `META` | `TYPE#OFFICIAL` | `PRI#1#JMA_bousai` | `JMA_bousai` | `気象庁防災情報` | `["earthquake","flood"]` |
| `OFFICIAL#c_nexco` | `META` | `TYPE#OFFICIAL` | `PRI#2#c_nexco` | `c_nexco` | `NEXCO中日本` | `["traffic"]` |
| ... | | | | | | |

- GSI1SK の `PRI#1` / `PRI#2` は優先度（1=最優先、2=高、3=中）
- `categories` はそのアカウントが対応するリスクカテゴリの配列

#### 初期登録アカウント（15件）

| 優先度 | 分類 | screen_name | categories |
|:---:|------|-------------|-----------|
| 1 | 防災 | `UN_NERV` | earthquake, flood |
| 1 | 防災 | `JMA_bousai` | earthquake, flood |
| 1 | 防災 | `Kantei_Saigai` | 全カテゴリ |
| 1 | 防災 | `FDMA_JAPAN` | earthquake, flood, fire |
| 1 | 防災 | `CAO_BOUSAI` | 全カテゴリ |
| 2 | 交通 | `c_nexco` | traffic |
| 2 | 交通 | `e_nexco` | traffic |
| 2 | 交通 | `w_nexco_info` | traffic |
| 2 | 交通 | `JRCentral_jp` | traffic |
| 2 | 交通 | `MLIT_JAPAN` | traffic, flood |
| 2 | インフラ | `OfficialTEPCO` | infra |
| 2 | インフラ | `KepcoOfficial` | infra |
| 2 | インフラ | `Official_CHUDEN` | infra |
| 3 | 自治体 | `tokyo_bousai` | 全カテゴリ |
| 3 | 自治体 | `osaka_bousai` | 全カテゴリ |

### ■ クエリ構築

```python
# from: オペレータでクエリ構築
# 15アカウント × 平均17文字 ≈ 255文字 + "lang:ja" = 約270文字 < 512文字制限
query = "(from:UN_NERV OR from:JMA_bousai OR from:Kantei_Saigai OR from:FDMA_JAPAN OR from:CAO_BOUSAI OR from:c_nexco OR from:e_nexco OR from:w_nexco_info OR from:JRCentral_jp OR from:MLIT_JAPAN OR from:OfficialTEPCO OR from:KepcoOfficial OR from:Official_CHUDEN OR from:tokyo_bousai OR from:osaka_bousai) lang:ja"
```

アカウント数が増えて 512 文字を超える場合は、既存 `build_query` パターンに倣いクエリ分割。

### ■ 増分取得 (since_id)

#### カーソル管理

DynamoDB TrendTable にカーソルを保存:

| PK | SK | newest_id | updated_at |
|----|-----|-----------|-----------|
| `CURSOR#official_collector` | `META` | `1900000000000000005` | `2026-03-12T10:05:00Z` |

#### フロー

```
1. DynamoDB から CURSOR#official_collector を取得
   → newest_id があれば since_id として使用
   → なければ start_time = 当日0時UTC にフォールバック（初回起動時）

2. search_recent(query=..., since_id=newest_id) を実行
   → 前回以降の新着ツイートのみ取得

3. レスポンスの meta.newest_id を DynamoDB に保存
   → 次回の since_id として使用
```

### ■ DynamoDB 保存設計

#### 個別ツイートをアイテムとして保存

```json
{
  "PK": "FACT#OFFICIAL",
  "SK": "2026-03-12T10:25:30Z#1900000000000000005",
  "GSI1PK": "TYPE#FACT_OFFICIAL",
  "GSI1SK": "earthquake#2026-03-12T10:25:30Z",
  "tweet_id": "1900000000000000005",
  "author_username": "UN_NERV",
  "author_name": "特務機関NERV",
  "text": "【地震情報】12日10時25分頃、千葉県北西部を震源とする地震。東京都大田区で震度4。",
  "created_at": "2026-03-12T10:25:30Z",
  "categories": ["earthquake"],
  "metrics": {
    "retweet_count": 1500,
    "like_count": 3000
  },
  "ttl": 1741910400
}
```

#### キー設計

| キー | 値 | 用途 |
|------|-----|------|
| PK | `FACT#OFFICIAL` | 全公式ツイートパーティション |
| SK | `{created_at}#{tweet_id}` | 時系列ソート + 一意性保証 |
| GSI1PK | `TYPE#FACT_OFFICIAL` | 全ファクト一覧 |
| GSI1SK | `{category_id}#{created_at}` | カテゴリ別時系列検索 |
| TTL | created_at + 24時間 | 自動削除 |

#### GSI1SK の category_id 決定ロジック

- DynamoDB マスタの `categories` 配列を参照
- 複数カテゴリに対応するアカウント（例: `@UN_NERV` → earthquake, flood）の場合:
  - ツイート本文のキーワードマッチで最適カテゴリを選定
  - 判定不能な場合は先頭カテゴリを使用

### ■ レート制限管理

```
Pro tier 消費見積:
  OfficialCollector: 1 req × 3回/15分 = 3 req/15分
  既存 TrendFetcher:   ≈ 20 req/15分
  既存 KeywordSearch:  ≈ 40 req/15分
  ────────────────────────────────────
  合計:                ≈ 63 req/15分 << 上限 300 req/15分

月間ツイート読取:
  15アカウント × 平均5件/回 × 288回/日 × 30日 ≈ 648,000件 << 上限 1,000,000件
```

---

## 5. FactChecker（ファクト照合）

### ■ 基本仕様

| 項目 | 内容 |
|------|------|
| Lambda関数名 | `fact-checker-{stage}` |
| ハンドラ | `fact_checker_function.lambda_handler` |
| トリガー | EventBridge Rule（S3 `classified/*` Object Created） |
| 入力 | S3 `classified/{category}/{date}/{ulid}.json` |
| 出力 | S3 `verified/{category}/{date}/{ulid}.json` |
| 外部API呼出 | **なし**（S3 読取 + DynamoDB Query のみ） |
| タイムアウト | 120秒 |
| メモリ | 512MB |

### ■ 処理フロー

```
EventBridge: classified/ Object Created
  │
  ▼
lambda_handler(event)
  │
  ├─ 1. S3 から classified データを読込
  │     → category_id, summary, related_nodes, ai_confidence を取得
  │
  ├─ 2. Step 1: 気象庁データ照合（S3読取）
  │     → S3 facts/jma/latest/{data_type}.json を読込
  │     → category_id に応じて適切なファイルを選択:
  │        earthquake → quake_list.json, tsunami.json
  │        flood      → typhoon.json, warning.json
  │        その他     → スキップ（気象庁データなし）
  │     → classified の summary / related_nodes と照合
  │     → 一致あり: jma_matched = true
  │
  ├─ 3. Step 2: ニュース照合（DynamoDB Query）
  │     → Query: PK = FACT#NEWS#{category_id}, SK > 1時間前
  │     → articles の title / description と summary をテキストマッチ
  │     → ヒット数をカウント: news_hit_count
  │
  ├─ 4. Step 3: 公式SNS照合（DynamoDB Query）
  │     → Query: PK = FACT#OFFICIAL, SK > 30分前
  │     → text と summary をテキストマッチ
  │     → ヒットしたアカウント名を記録: official_hits
  │
  ├─ 5. 信頼度スコア計算
  │     → 下記ルールで加算
  │
  └─ 6. S3 verified/ に保存
```

### ■ 信頼度スコア計算ルール

```
初期値: ai_confidence（CategoryClassifier が付与した値）

Step 1 加算:
  気象庁データに一致あり → confidence = 100（確定リスク、即時昇格）

Step 2 加算:
  信頼メディア 1件以上ヒット  → confidence += 30
  信頼メディア 3件以上ヒット  → confidence += 50（30の代わりに50）
  非信頼メディアのみヒット    → confidence += 15

Step 3 加算:
  優先度1 アカウントがヒット  → confidence += 40
  優先度2 アカウントがヒット  → confidence += 25
  優先度3 アカウントがヒット  → confidence += 15

最終値: min(confidence, 100)
```

### ■ 最終分類ラベル判定

| 最終 confidence | ラベル | 後続アクション |
|:---:|--------|---------------|
| 80〜100 | 確定リスク | リスク評価→即時通知 |
| 50〜79 | 要確認 | 担当者へ通知→人的判断 |
| 30〜49 | 監視継続 | ダッシュボード表示 |
| 0〜29 | 除外 | 記録のみ |

### ■ verified/ 出力フォーマット

```json
{
  "verified_at": "2026-03-12T10:32:00Z",
  "category_id": "earthquake",
  "original_s3_key": "classified/earthquake/2026-03-12/01JXXX.json",
  "summary": "東京都大田区で震度4の地震",
  "related_nodes": [...],

  "ai_confidence": 78,
  "final_confidence": 100,
  "label": "確定リスク",

  "fact_check_results": {
    "jma": {
      "matched": true,
      "data_type": "quake_list",
      "match_method": "city_code",
      "matched_event": {
        "eid": "20260312102500",
        "at": "2026-03-12T10:25:00+09:00",
        "anm": "千葉県北西部",
        "en_anm": "Northwestern Chiba Prefecture",
        "cod": "+35.6+139.9-60000/",
        "mag": "4.8",
        "maxi": "4",
        "ttl": "震源・震度情報",
        "site_intensity": "4",
        "site_distance_km": 18.5
      }
    },
    "news": {
      "hit_count": 3,
      "trusted_hit_count": 2,
      "confidence_added": 50,
      "top_articles": [
        {
          "title": "東京で震度4の地震 津波の心配なし",
          "source": "NHKニュース",
          "pub_date": "2026-03-12T10:27:00Z"
        }
      ]
    },
    "official": {
      "hit_count": 2,
      "confidence_added": 40,
      "hits": [
        {
          "author": "@UN_NERV",
          "priority": 1,
          "text": "【地震情報】12日10時25分頃...",
          "created_at": "2026-03-12T10:25:30Z"
        }
      ]
    }
  }
}
```

### ■ マッチングロジック（実データ構造に基づく）

FactChecker は classified の情報と facts を照合する際、以下の3層マッチングを使用:

#### 1. 気象庁マッチ（Step 1）

```
方式A: 市区町村コード照合（最も正確）
  classified.related_nodes[].location_code  → "1311100"  (大田区)
  jma.raw_data[].int[].city[].code          → "1311100"
  → 完全一致で拠点の震度を特定

方式B: 座標距離照合
  classified.related_nodes[].lat/lon  → 35.56, 139.72 (東京組立工場)
  jma.raw_data[].cod                  → "+35.6+139.9-60000/"
  → cod を解析し、拠点との距離を計算
  → 100km以内: 近距離、50km以内: 直接影響圏

方式C: 地名テキスト照合（フォールバック）
  classified.related_nodes[].location_name → "東京都大田区"
  jma.raw_data[].anm                       → "千葉県北西部"
  jma.raw_data[].int[].code                → "13" (東京都)
  → 都道府県コード先頭2桁で照合
```

#### 2. ニュースマッチ（Step 2）

```
classified.summary → "東京都大田区で震度4の地震"
  ↓ キーワード抽出: ["東京", "震度4", "地震"]

news.articles[].title → "東京で震度4の地震 津波の心配なし"
  → キーワード2個以上含む → ヒット
  → source が信頼メディアリストに含まれる → is_trusted = true
```

#### 3. 公式SNSマッチ（Step 3）

```
classified.summary → "東京都大田区で震度4の地震"
  ↓ キーワード抽出: ["東京", "震度4", "地震"]

official.text → "【地震情報】12日10時25分頃、千葉県北西部を震源とする地震。東京都大田区で震度4。"
  → キーワード2個以上含む → ヒット
  → author_username のマスタから priority を取得
```

#### 4. 時間窓フィルタ（全ステップ共通）

```
classified.sample_tweets[].created_at と facts の時刻を比較:
  JMA:      ±60分以内（地震情報は発生直後に更新されるため）
  ニュース: ±120分以内（報道には時間差がある）
  公式SNS:  ±60分以内
```

---

## 6. DynamoDB キー設計まとめ

### ■ 新規追加アイテム一覧

既存 TrendTable に追加:

| エンティティ | PK | SK | GSI1PK | GSI1SK | TTL |
|---|---|---|---|---|---|
| ニュースファクト | `FACT#NEWS#{category}` | `{timestamp}` | `TYPE#FACT_NEWS` | `{category}#{timestamp}` | 24h |
| 公式ツイートファクト | `FACT#OFFICIAL` | `{timestamp}#{tweet_id}` | `TYPE#FACT_OFFICIAL` | `{category}#{timestamp}` | 24h |
| 公式アカウントマスタ | `OFFICIAL#{screen_name}` | `META` | `TYPE#OFFICIAL` | `PRI#{priority}#{screen_name}` | - |
| カーソル（JMA） | `CURSOR#jma_collector` | `META` | - | - | - |
| カーソル（公式） | `CURSOR#official_collector` | `META` | - | - | - |
| カーソル（KW検索） | `CURSOR#keyword_route#{category}` | `META` | - | - | - |
| カーソル（トレンド） | `CURSOR#trends_route` | `META` | - | - | - |

### ■ FactChecker のクエリパターン

| 用途 | 操作 | キー条件 |
|------|------|---------|
| ニュースファクト取得 | Query | `PK = FACT#NEWS#{category}, SK > {1時間前}` |
| 公式ツイート取得 | Query | `PK = FACT#OFFICIAL, SK > {30分前}` |
| カーソル取得 | GetItem | `PK = CURSOR#..., SK = META` |
| 公式アカウント一覧 | Query (GSI1) | `GSI1PK = TYPE#OFFICIAL` |

---

## 7. S3 キー設計まとめ

### ■ 新規追加パス

```
{BucketName}/
  ├── raw/          # (既存) TrendFetcher / KeywordSearch 出力
  ├── classified/   # (既存) CategoryClassifier 出力
  ├── facts/        # (新規) FactCollector 出力
  │   └── jma/
  │       └── latest/
  │           ├── quake_list.json
  │           ├── tsunami.json
  │           ├── typhoon.json
  │           └── warning.json
  └── verified/     # (新規) FactChecker 出力
      └── {category}/
          └── {date}/
              └── {ulid}.json
```

### ■ S3 ライフサイクルルール追加

```yaml
- Id: ExpireFactsData
  Prefix: facts/
  Status: Enabled
  ExpirationInDays: 7    # ファクトデータは7日で十分

- Id: ExpireVerifiedData
  Prefix: verified/
  Status: Enabled
  ExpirationInDays: 90   # 検証済みデータは classified と同じ
```

---

## 8. template.yaml 追加リソース

### ■ 新規 Lambda 関数（4つ）

| 関数名 | CodeUri | Events | 環境変数 |
|--------|---------|--------|---------|
| JmaCollectorFunction | `function/jma_collector/` | Schedule: rate(1 minute) | BUCKET_NAME |
| NewsCollectorFunction | `function/news_collector/` | Schedule: rate(10 minutes) | TABLE_NAME |
| OfficialCollectorFunction | `function/official_collector/` | Schedule: rate(5 minutes) | TABLE_NAME, SECRET_NAME |
| FactCheckerFunction | `function/fact_checker/` | EventBridgeRule: classified/* | TABLE_NAME, BUCKET_NAME |

### ■ IAM ポリシー追加

既存 `LambdaExecutionRole` に以下を追加:

```yaml
# DynamoDB: FACT# / CURSOR# / OFFICIAL# アイテムの読み書き
- Effect: Allow
  Action:
    - dynamodb:PutItem
    - dynamodb:GetItem
    - dynamodb:Query
    - dynamodb:UpdateItem
  Resource:
    - !GetAtt TrendTable.Arn
    - !Sub ${TrendTable.Arn}/index/*
```

※ 既存ポリシーは `Query` + `GetItem` のみ。`PutItem` + `UpdateItem` を追加。

---

## 9. マスタデータ投入

### ■ 公式アカウントマスタ投入スクリプト

`scripts/seed_official_accounts.py` を新規作成:

```
python scripts/seed_official_accounts.py <テーブル名>
```

投入データ: 15件の公式アカウント情報（本設計書 4章の初期登録アカウント一覧）

---

## 10. 既存 Lambda への since_id 導入（オプション）

現在の `keyword_search` と `trend_fetcher` は `start_time = 当日0時` で毎回全量検索している。
FactCollector と同様に since_id カーソルを導入することで、既存 Lambda の API 効率も改善可能。

| 対象 | 変更内容 | カーソルキー |
|------|---------|-----------|
| keyword_search | `since_id` パラメータ追加 | `CURSOR#keyword_route#{category}` |
| trend_fetcher | `since_id` パラメータ追加 | `CURSOR#trends_route` |

※ FactCollector とは独立して段階的に導入可能。
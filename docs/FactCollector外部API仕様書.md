# FactCollector 外部API仕様書

本ドキュメントは FactCollector（3 Lambda構成）が利用する外部APIの調査結果をまとめたものである。

---

## アーキテクチャ概要

```
EventBridge (1分) ──▶ JmaCollector      ──▶ S3 facts/jma/
EventBridge (10分) ──▶ NewsCollector     ──▶ S3 facts/news/
EventBridge (5分) ──▶ OfficialCollector  ──▶ S3 facts/official/
```

### カテゴリ別カバレッジ

| カテゴリ       | JmaCollector | NewsCollector | OfficialCollector |
|---------------|:---:|:---:|:---:|
| earthquake    | ✅ | ✅ | ✅ (@UN_NERV, @JMA_kishou 等) |
| flood         | ✅ | ✅ | ✅ (@JMA_kishou, @MLIT_river 等) |
| fire          | ❌ | ✅ | ✅ (@FDMA_JAPAN, @Tokyo_Fire_D) |
| traffic       | ❌ | ✅ | ✅ (@JREast_official, @JRCentral_OFL, @NEXCO各社) |
| infra         | ❌ | ✅ | ✅ (@TEPCOPG, @KANDEN_souhai 等) |
| labor         | ❌ | ✅ | ⚠️ (@meti_NIPPON, @MHLWitter) |
| geopolitics   | ❌ | ✅ | ⚠️ (@MofaJapan_jp, @meti_NIPPON) |
| pandemic      | ❌ | ✅ | ✅ (@MHLWitter, @JIHS_JP) |

---

## 1. JmaCollector（気象庁 JSON API）

### 1-1. 概要

| 項目 | 内容 |
|------|------|
| 提供元 | 気象庁（非公式内部API） |
| 認証 | 不要 |
| 料金 | 無料 |
| 形式 | JSON |
| レート制限 | 明示なし（ポーリング間隔1分以上推奨） |
| 安定性 | 広く利用されているが、非公式のためURL変更リスクあり |
| Lambda適合性 | urllib のみで実装可能（追加依存なし） |

### 1-2. 地震情報

#### エンドポイント

```
GET https://www.jma.go.jp/bosai/quake/data/list.json
```

#### レスポンス

過去約15日分の地震情報リスト（約200件）。配列で返る。（2026-03-12 実測値）

```json
[
  {
    "ctt": "20260312203658",
    "eid": "20260312203352",
    "rdt": "2026-03-12T20:36:00+09:00",
    "ttl": "震源・震度情報",
    "ift": "発表",
    "ser": "1",
    "at": "2026-03-12T20:33:00+09:00",
    "anm": "富山県東部",
    "en_anm": "Eastern Toyama Prefecture",
    "acd": "380",
    "cod": "+36.4+137.6-10000/",
    "mag": "3.4",
    "maxi": "2",
    "int": [
      {
        "code": "21",
        "maxi": "2",
        "city": [
          {"code": "2121700", "maxi": "2"},
          {"code": "2120300", "maxi": "1"}
        ]
      },
      {
        "code": "20",
        "maxi": "1",
        "city": [
          {"code": "2020100", "maxi": "1"},
          {"code": "2021200", "maxi": "1"},
          {"code": "2048200", "maxi": "1"}
        ]
      }
    ],
    "json": "20260312203658_20260312203352_VXSE5k_1.json",
    "en_ttl": "Earthquake and Seismic Intensity Information"
  }
]
```

#### フィールド定義

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `ctt` | string | 作成日時（YYYYMMDDHHmmss形式） |
| `eid` | string | イベントID（発生日時ベース、YYYYMMDDHHmmss形式） |
| `at` | string | 地震発生時刻（ISO 8601 JST） |
| `rdt` | string | 報告日時（ISO 8601 JST） |
| `ttl` | string | 情報種別タイトル（下記4種） |
| `en_ttl` | string | 情報種別タイトル（英語） |
| `ift` | string | 情報フラグ（"発表" 等） |
| `ser` | string | シリアル番号 |
| `anm` | string | 震源地名（日本語） |
| `en_anm` | string | 震源地名（英語） |
| `acd` | string | 震源地域コード（数値文字列） |
| `cod` | string | 震源座標+深さ（`+緯度+経度-深さ(m)/` 形式、例: `+36.4+137.6-10000/` = 北緯36.4° 東経137.6° 深さ10km） |
| `mag` | string | マグニチュード |
| `maxi` | string | 最大震度（"1"〜"7", "5-", "5+", "6-", "6+"） |
| `int` | array | 震度分布（都道府県→市区町村の階層構造、下記参照） |
| `json` | string | 詳細JSONファイル名 |

#### `int` 配列の階層構造

```
int[] ← 都道府県レベル
  ├── code: "21"              ← 都道府県コード（21=岐阜県）
  ├── maxi: "2"               ← その都道府県の最大震度
  └── city[] ← 市区町村レベル
        ├── code: "2121700"   ← 市区町村コード（7桁）
        └── maxi: "2"         ← その市区町村の震度
```

※ 都道府県コードは JIS X 0401 準拠（01=北海道 〜 47=沖縄）
※ 市区町村コードは先頭2桁が都道府県コード

#### `ttl` の種類（4種）

| ttl | 内容 | FactChecker での活用 |
|-----|------|---------------------|
| `震源・震度情報` | 震源+震度の確定情報（最も詳細） | メインで使用 |
| `震源に関する情報` | 震源のみ（震度情報なし） | 補助的に使用 |
| `震度速報` | 速報値（震源未確定の場合あり） | 速報性重視の場合に使用 |
| `南海トラフ地震関連解説情報` | 南海トラフ特別情報 | 最重要アラート |

#### `cod` フィールドの解析方法

```
cod = "+36.4+137.6-10000/"
       │     │      │
       │     │      └── 深さ: 10000m = 10km（負の値、単位メートル）
       │     └── 経度: 東経 137.6°
       └── 緯度: 北緯 36.4°
```

FactChecker での活用: 拠点の緯度経度との距離計算により、影響範囲を自動判定可能。

#### 詳細情報取得

```
GET https://www.jma.go.jp/bosai/quake/data/{jsonファイル名}
```

例: `GET https://www.jma.go.jp/bosai/quake/data/20260312203658_20260312203352_VXSE5k_1.json`

#### 増分取得の方法

- `ctt`（作成日時）で前回取得分との差分を判定（`ctt` はソート済みリストの先頭が最新）
- DynamoDB に最新の `ctt` を保存し、次回はそれより新しいものだけ処理
- 配列は新しい順に並んでいるため、先頭から走査し `ctt` が前回保存値以下になったら打ち切り

### 1-3. 津波情報

#### エンドポイント

```
GET https://www.jma.go.jp/bosai/tsunami/data/list.json
```

#### レスポンス

津波警報・注意報が発令されていない場合は空配列 `[]`。

```json
[
  {
    "eid": "20240101161006",
    "ser": 1,
    "rdt": "2024-01-01T16:22:00+09:00",
    "ttl": "大津波警報・津波警報・津波注意報",
    "json": "20240101_tsunami.json"
  }
]
```

#### フィールド定義

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `eid` | string | 関連地震のイベントID |
| `ser` | number | シリアル番号 |
| `rdt` | string | 報告日時 |
| `ttl` | string | 情報種別（大津波警報/津波警報/津波注意報） |
| `json` | string | 詳細JSONファイル名 |

### 1-4. 台風情報

#### エンドポイント

```
GET https://www.jma.go.jp/bosai/typhoon/data/targetTc.json
```

#### レスポンス

現在発生中の台風リスト。台風がない場合は空。

#### 個別台風詳細

```
GET https://www.jma.go.jp/bosai/typhoon/data/tcinfo/{台風番号}.json
```

### 1-5. 気象警報・注意報

#### エンドポイント

```
GET https://www.jma.go.jp/bosai/warning/data/warning/{areaCode}.json
```

#### 主要 areaCode

| areaCode | 地域 |
|----------|------|
| `010000` | 全国概況 |
| `130000` | 東京都 |
| `140000` | 神奈川県 |
| `230000` | 愛知県 |
| `270000` | 大阪府 |
| `400000` | 福岡県 |

※ 全地域コードは `https://www.jma.go.jp/bosai/common/const/area.json` で取得可能。
※ 監視対象はサプライチェーン拠点（工場・倉庫・サプライヤー）の所在地に絞る。

### 1-6. 地域コードマスタ

#### エンドポイント

```
GET https://www.jma.go.jp/bosai/common/const/area.json
```

#### レスポンス構造

階層型: `centers` → `offices` → `class10s` → `class15s` → `class20s`

```json
{
  "centers": {
    "010100": {
      "name": "北海道地方",
      "enName": "Hokkaido",
      "children": ["011000", "012000"]
    }
  },
  "offices": {
    "130000": {
      "name": "東京都",
      "enName": "Tokyo",
      "parent": "010300",
      "children": ["130010", "130020", "130030"]
    }
  }
}
```

### 1-7. その他（オプション）

| エンドポイント | 説明 | 対応カテゴリ |
|--------------|------|------------|
| `https://www.jma.go.jp/bosai/volcano/data/list.json` | 噴火警報・火山情報 | fire |
| `https://www.jma.go.jp/bosai/flood/data/list.json` | 洪水情報 | flood |

### 1-8. S3保存設計

```
facts/jma/latest/
  ├── quake_list.json      # 地震一覧（毎回全量上書き、約200件/15日分）
  ├── tsunami.json         # 津波警報（毎回上書き、空配列もあり得る）
  ├── typhoon.json         # 台風情報（毎回上書き）
  └── warning.json         # 気象警報（監視エリア分を統合）
```

### 1-9. 実装上の注意

- Python 標準ライブラリ (`urllib.request` + `json`) のみで実装可能
- User-Agent ヘッダーの設定を推奨
- タイムアウトは 10 秒程度に設定
- レスポンスが空配列の場合も正常（津波なし等）
- `quake_list.json` は約200件（15日分）。全量を毎回取得しS3に上書きする設計のため、レスポンスサイズは数百KB程度
- `cod` フィールドの座標解析で拠点との距離計算が可能（FactChecker で活用）
- `int` 配列の `city[].code` は7桁の市区町村コードで、先頭2桁が都道府県コード（JIS X 0401）

---

## 2. NewsCollector（ニュース検索）

### 2-1. 推奨API比較

| API | 日本語カバー | 無料枠 | 正式性 | 推奨フェーズ |
|-----|------------|--------|--------|------------|
| **Google News RSS** | 最高 | 無制限 | 非公式 | Phase 1（プロトタイプ） |
| **Bing News API** | 高 | 1,000 req/月 | Azure公式 | Phase 2（本番） |
| GNews API | 中〜高 | 100 req/日 | 有 | 備選 |
| NewsAPI.org | **低** | 100 req/日 (非商用) | 有 | ❌ 不推奨 |

---

### 2-2. Google News RSS（Phase 1 推奨）

#### 概要

| 項目 | 内容 |
|------|------|
| 提供元 | Google（非公式RSSフィード） |
| 認証 | 不要 |
| 料金 | 無料 |
| 形式 | RSS (XML) |
| レート制限 | 明示なし（短時間大量アクセスでブロックの可能性） |
| 最大件数 | 100件/リクエスト |
| Lambda適合性 | Python標準ライブラリのみで実装可能 |

#### エンドポイント

```
GET https://news.google.com/rss/search?q={query}&hl=ja&gl=JP&ceid=JP:ja
```

#### URLパラメータ

| パラメータ | 必須 | 説明 | 値 |
|-----------|:---:|------|-----|
| `q` | ✅ | 検索キーワード（URLエンコード必要） | `地震 東京` |
| `hl` | ✅ | 表示言語 | `ja` |
| `gl` | ✅ | 国 | `JP` |
| `ceid` | ✅ | 国・言語コード | `JP:ja` |

#### 検索オペレータ（q パラメータ内で使用）

| オペレータ | 説明 | 例 |
|-----------|------|-----|
| `OR` | OR検索 | `地震 OR 津波` |
| `"..."` | フレーズ検索 | `"東名高速 通行止め"` |
| `-` | 除外 | `地震 -訓練` |
| `+` | 必須 | `+停電 東京` |
| `intitle:` | タイトル内検索 | `intitle:地震` |
| `when:` | 期間指定 | `when:1h`, `when:1d`, `when:7d` |
| `after:` | 日付始点 | `after:2026-03-11` |
| `before:` | 日付終点 | `before:2026-03-12` |

#### カテゴリ別検索クエリ例

| カテゴリ | 検索クエリ |
|---------|-----------|
| earthquake | `(地震 OR 津波 OR 震度) when:1d` |
| flood | `(台風 OR 大雨 OR 洪水 OR 浸水 OR 冠水) when:1d` |
| fire | `(火災 OR 爆発 OR 工場火災) when:1d` |
| traffic | `(通行止め OR 運休 OR 遅延 OR 欠航) when:1d` |
| infra | `(停電 OR 断水 OR 通信障害) when:1d` |
| labor | `(ストライキ OR 操業停止 OR 労働争議 OR リコール) when:1d` |
| geopolitics | `(関税 OR 制裁 OR 輸出規制 OR 貿易摩擦) when:1d` |
| pandemic | `(感染拡大 OR パンデミック OR 変異株 OR 緊急事態宣言) when:1d` |

#### レスポンス (RSS XML)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>「地震 東京」 - Google ニュース</title>
    <link>https://news.google.com</link>
    <description>Google ニュース</description>
    <item>
      <title>東京で震度4の地震 津波の心配なし - NHKニュース</title>
      <link>https://news.google.com/rss/articles/CBMi...</link>
      <pubDate>Wed, 11 Mar 2026 03:45:00 GMT</pubDate>
      <description>記事の概要テキスト...</description>
      <source url="https://www3.nhk.or.jp">NHKニュース</source>
    </item>
  </channel>
</rss>
```

#### item フィールド定義

| フィールド | 説明 |
|-----------|------|
| `title` | 記事タイトル（メディア名付き） |
| `link` | 記事URL（Google経由リダイレクト） |
| `pubDate` | 配信日時 (RFC 2822) |
| `description` | 記事概要 |
| `source` | メディア名（`url` 属性に元URL） |

#### 信頼メディアフィルタ（実測確認済み、2026-03-12）

以下は実際の RSS レスポンスの `<source>` タグから確認した値:

| メディア | `source` 実測値 | 確認カテゴリ |
|---------|----------------|------------|
| NHK | `NHKニュース` | earthquake, flood, fire, traffic |
| 共同通信 | `47NEWS` | earthquake, geopolitics |
| 時事通信 | `時事ドットコム` / `時事通信ニュース` | geopolitics, earthquake |
| 日本経済新聞 | `日本経済新聞` | geopolitics, labor |
| 読売新聞 | `読売新聞オンライン` | earthquake, traffic |
| 朝日新聞 | `朝日新聞デジタル` | earthquake, flood |
| 毎日新聞 | `毎日新聞` | fire, earthquake |
| 産経新聞 | `産経ニュース` | geopolitics |
| Reuters | `Reuters` | geopolitics |
| Bloomberg | `Bloomberg` | geopolitics |

**非信頼（除外推奨）**:
| source | 理由 |
|--------|------|
| `Yahoo!ニュース` | アグリゲータ（転載記事）。最多出現だが元記事の source で判定すべき |
| 地方紙・ローカルTV | 信頼性は高いが source 値が多様すぎてリスト化困難 |

> **注意**: `NHK` 単体ではなく `NHKニュース` がRSSの source 値。
> `共同通信` は `47NEWS` として出現する。これらは事前の推定と異なるため注意。

#### 増分取得の方法

- **`when:1d`** を使い直近24時間のニュースを取得（`when:1h` は結果が少なすぎるため不採用）
- `pubDate` で前回取得時刻以降の記事のみ処理（DynamoDB カーソルに最新 `pubDate` を保存）
- 10分間隔で実行するため前回と記事が重複するが、`pubDate` フィルタリングで排除

> **`when:1h` が不採用の理由**: 実測で earthquake カテゴリが5件のみ。
> 他カテゴリも0〜10件程度で、ファクトチェックの裏付けとして不十分。

#### 実測結果（2026-03-12）

| カテゴリ | `when:1d` 件数 | `when:1h` 件数 | 備考 |
|---------|:---:|:---:|------|
| earthquake | **100** | 5 | 上限到達。常時ニュースが多い |
| flood | 45 | 3 | 季節依存。台風シーズンは増加 |
| fire | **100** | 8 | 上限到達。日常的な火災ニュースが多い |
| traffic | 72 | 6 | |
| infra | 35 | 2 | |
| labor | 18 | 1 | |
| geopolitics | **100** | 7 | 上限到達。関税・貿易ニュースが恒常的 |
| pandemic | 22 | 2 | |

#### 100件上限への対処

earthquake / fire / geopolitics で100件上限に達する場合がある。対処方針:

1. **定期取得で補完**: 10分間隔で取得するため、最新記事は次回実行時に取得可能
2. **`pubDate` ソート**: Google News RSS は新しい記事が先頭に来るため、上限で切れるのは古い記事側
3. **Phase 2 移行**: 上限が問題になる場合は Bing News API（count パラメータ指定可能）への切替を検討

#### ノイズ・関連性の考慮

- **Yahoo!ニュース** が最多出現メディアだが、アグリゲータのため元記事と重複する
- **fire カテゴリ**: 「火災」で一般住宅火災も多くヒット → FactChecker 側で「工場」「倉庫」等のSCMキーワードとの複合マッチが必要
- **geopolitics カテゴリ**: 評論・コラム系記事も多い → 信頼メディアフィルタで優先度を付ける

#### 実装上の注意

- `urllib.request` + `xml.etree.ElementTree` で実装可能（追加依存なし）
- **User-Agent ヘッダー必須**（ないとブロックされる場合がある）
- `link` はGoogle経由のリダイレクトURL（元記事URLが必要な場合は追加リクエストが必要）
- 日本語キーワードは `urllib.parse.quote()` でURLエンコード
- `<source>` タグのテキスト値がメディア名（例: `NHKニュース`）、`url` 属性が元サイトURL
- `title` にはメディア名が付与される場合がある（例: `東京で震度4 - NHKニュース`）→ ` - {source}` を除去してマッチングに使用
- `pubDate` は RFC 2822 形式（例: `Wed, 11 Mar 2026 03:45:00 GMT`）→ `email.utils.parsedate_to_datetime()` で変換

---

### 2-3. Bing News Search API（Phase 2 本番向け）

#### 概要

| 項目 | 内容 |
|------|------|
| 提供元 | Microsoft Azure Cognitive Services（公式API） |
| 認証 | Azure サブスクリプションキー（ヘッダー） |
| 料金 | S0: 無料 (1,000 tx/月) / S1: $1/1,000 tx |
| 形式 | JSON |
| レート制限 | S0: 1 req/秒, 1,000 tx/月 |
| SLA | あり（Azure標準） |
| Lambda適合性 | requests ライブラリで実装（Layer に追加要） |

#### エンドポイント

```
GET https://api.bing.microsoft.com/v7.0/news/search
```

#### ヘッダー

```
Ocp-Apim-Subscription-Key: {Azure_API_Key}
```

#### クエリパラメータ

| パラメータ | 必須 | 説明 | 例 |
|-----------|:---:|------|-----|
| `q` | ✅ | 検索キーワード | `地震 東京` |
| `mkt` | | 市場コード | `ja-JP` |
| `count` | | 返却件数 (max 100) | `10` |
| `offset` | | オフセット | `0` |
| `freshness` | | 鮮度フィルタ | `Day`, `Week`, `Month` |
| `sortBy` | | ソート順 | `Date`, `Relevance` |
| `safeSearch` | | セーフサーチ | `Off`, `Moderate`, `Strict` |

#### レスポンス (JSON)

```json
{
  "value": [
    {
      "name": "東京で震度4の地震",
      "url": "https://www3.nhk.or.jp/news/...",
      "description": "記事の説明...",
      "datePublished": "2026-03-11T03:45:00.0000000Z",
      "provider": [
        {
          "name": "NHKニュース"
        }
      ],
      "category": "ScienceAndTechnology"
    }
  ],
  "totalEstimatedMatches": 42
}
```

#### 認証キー管理

Secrets Manager に保存（既存パターン踏襲）:
```
Secret: trend-monitor/dev/bing-news-api-key
```

### 2-4. S3保存設計

```
facts/news/{date}/
  ├── {HHmm}_earthquake.json     # 10分ごとのニュース検索結果
  ├── {HHmm}_flood.json
  ├── {HHmm}_fire.json
  ├── {HHmm}_traffic.json
  ├── {HHmm}_infra.json
  ├── {HHmm}_labor.json
  ├── {HHmm}_geopolitics.json
  └── {HHmm}_pandemic.json
```

各ファイルの統一フォーマット:

```json
{
  "fetched_at": "2026-03-12T10:30:00Z",
  "category_id": "earthquake",
  "query_used": "(地震 OR 津波 OR 震度) when:1h",
  "result_count": 5,
  "articles": [
    {
      "title": "東京で震度4の地震",
      "source": "NHKニュース",
      "pub_date": "2026-03-12T10:15:00Z",
      "url": "https://...",
      "description": "..."
    }
  ]
}
```

---

## 3. OfficialCollector（X API 公式アカウント監視）

### 3-1. 概要

| 項目 | 内容 |
|------|------|
| API | X API v2 `search_recent` |
| 認証 | Bearer Token（Secrets Manager に保存済み） |
| 料金 | 既存 Pro tier 内で追加コストなし |
| 形式 | JSON |
| ライブラリ | xdk v0.9.0（既存Layer内） |

### 3-2. エンドポイント

```
GET https://api.x.com/2/tweets/search/recent
```

#### 認証

```
Authorization: Bearer {BEARER_TOKEN}
```

#### クエリパラメータ

| パラメータ | 型 | 必須 | 説明 |
|-----------|-----|:---:|------|
| `query` | string | ✅ | 検索クエリ (1-512文字) |
| `since_id` | string | | このIDより新しい投稿のみ返す |
| `until_id` | string | | このIDより古い投稿のみ返す |
| `start_time` | string | | 最古UTC時刻 (ISO 8601) |
| `end_time` | string | | 最新UTC時刻 (ISO 8601) |
| `max_results` | integer | | 10-100（デフォルト10） |
| `next_token` | string | | ページネーション |
| `sort_order` | string | | `recency` / `relevancy` |
| `tweet.fields` | string | | 取得フィールド指定 |
| `expansions` | string | | 展開フィールド指定 |
| `user.fields` | string | | ユーザー情報フィールド |

#### レスポンス

```json
{
  "data": [
    {
      "id": "1900000000000000001",
      "text": "【地震情報】12日10時25分頃、千葉県北西部...",
      "author_id": "123456789",
      "created_at": "2026-03-12T01:25:30.000Z",
      "public_metrics": {
        "retweet_count": 1500,
        "reply_count": 200,
        "like_count": 3000,
        "impression_count": 500000
      }
    }
  ],
  "meta": {
    "newest_id": "1900000000000000005",
    "oldest_id": "1900000000000000001",
    "result_count": 5,
    "next_token": "b26v89c19zqg8o3fpzbkk..."
  },
  "includes": {
    "users": [
      {
        "id": "123456789",
        "name": "特務機関NERV",
        "username": "UN_NERV"
      }
    ]
  }
}
```

#### meta フィールド定義

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `newest_id` | string | 結果中の最新投稿ID（次回の since_id に使用） |
| `oldest_id` | string | 結果中の最古投稿ID |
| `result_count` | integer | 返却件数 |
| `next_token` | string | 次ページのトークン |

### 3-3. レート制限 (Pro tier)

| エンドポイント | 15分あたり | 月間ツイート読取上限 |
|--------------|-----------|-------------------|
| `search_recent` | 300 | 1,000,000 |

#### OfficialCollector の消費見積

```
監視アカウント: 23件 → 1クエリで収まる (約477文字、512文字制限内)
実行間隔: 5分 → 15分あたり 3リクエスト
月間読取: 23アカウント × 平均3件/回 × 288回/日 × 30日 ≈ 597,600件（Pro tier 1M上限の60%）
```

### 3-4. 検索クエリ設計

#### from: オペレータによるクエリ構築

```
(from:UN_NERV OR from:JMA_kishou OR from:JMA_bousai OR from:Kantei_Saigai
 OR from:FDMA_JAPAN OR from:CAO_BOUSAI OR from:Tokyo_Fire_D
 OR from:JREast_official OR from:JRCentral_OFL OR from:e_nexco_bousai
 OR from:w_nexco_news OR from:MLIT_JAPAN OR from:MLIT_river
 OR from:TEPCOPG OR from:KANDEN_souhai OR from:Official_Chuden
 OR from:TH_nw_official OR from:tokyo_bousai OR from:osaka_bousai
 OR from:meti_NIPPON OR from:MofaJapan_jp OR from:MHLWitter
 OR from:JIHS_JP)
 lang:ja -is:retweet
```

※ 現在 23アカウント・約477文字（512文字制限に35文字の余裕）。
※ 512文字を超える場合は `build_official_queries()` が自動分割しループ実行する。
※ アカウントは DynamoDB `OFFICIAL_ACCT#*` マスタで管理（§3-9 参照）。

### 3-5. 監視対象アカウント一覧（23件）

#### 横断・防災・気象

| アカウント | 名称 | 投稿内容 | 対応カテゴリ |
|-----------|------|---------|------------|
| `@UN_NERV` | 特務機関NERV防災 | 気象庁専用線接続・国内最速級の防災速報 | earthquake, flood |
| `@JMA_kishou` | 気象庁 | 地震・津波・気象警報の一次情報源 | earthquake, flood |
| `@JMA_bousai` | 気象庁防災情報 | 防災気象情報専用アカウント | earthquake, flood |
| `@Kantei_Saigai` | 首相官邸（災害・危機管理） | 政府の大規模災害対応・避難指示 | 全カテゴリ |
| `@FDMA_JAPAN` | 総務省消防庁 | 大規模災害の被害状況・消防活動 | earthquake, flood, fire |
| `@CAO_BOUSAI` | 内閣府防災 | 防災政策・避難情報・被害情報 | 全カテゴリ |

#### 火災・爆発

| アカウント | 名称 | 投稿内容 | 対応カテゴリ |
|-----------|------|---------|------------|
| `@Tokyo_Fire_D` | 東京消防庁 | 関東エリアの火災・救急情報 | fire |

#### 交通インフラ

| アカウント | 名称 | 投稿内容 | 対応カテゴリ |
|-----------|------|---------|------------|
| `@JREast_official` | JR東日本（公式） | 東北・関東の鉄道運行情報 | traffic |
| `@JRCentral_OFL` | JR東海News | 東海道新幹線・東海エリア運行情報 | traffic |
| `@e_nexco_bousai` | NEXCO東日本（道路防災） | 災害時の高速道路通行止め情報 | traffic |
| `@w_nexco_news` | NEXCO西日本 | 西日本エリア高速道路情報 | traffic |
| `@MLIT_JAPAN` | 国土交通省 | 道路・河川・港湾等の総合情報 | traffic, flood |
| `@MLIT_river` | 国土交通省 水管理・国土保全 | 河川水位・洪水予報の専門情報 | flood |

#### 電力・インフラ

| アカウント | 名称 | 投稿内容 | 対応カテゴリ |
|-----------|------|---------|------------|
| `@TEPCOPG` | 東京電力パワーグリッド | 関東エリア停電情報・復旧情報 | infra |
| `@KANDEN_souhai` | 関西電力送配電 | 関西エリア停電情報・復旧情報 | infra |
| `@Official_Chuden` | 中部電力 | 中部エリア停電・復旧情報 | infra |
| `@TH_nw_official` | 東北電力ネットワーク | 東北エリア停電情報・復旧情報 | infra |

#### 自治体防災

| アカウント | 名称 | 投稿内容 | 対応カテゴリ |
|-----------|------|---------|------------|
| `@tokyo_bousai` | 東京都防災 | 東京エリアの防災・避難情報 | earthquake, flood |
| `@osaka_bousai` | おおさか防災ネット（大阪府） | 関西エリアの防災・避難情報 | earthquake, flood |

#### 労務・操業リスク／地政学・貿易

| アカウント | 名称 | 投稿内容 | 対応カテゴリ |
|-----------|------|---------|------------|
| `@meti_NIPPON` | 経済産業省 | 輸出規制・操業制限・産業政策 | labor, geopolitics |
| `@MofaJapan_jp` | 外務省 | 制裁・渡航情報・貿易摩擦の公式発表 | geopolitics |

#### 感染症

| アカウント | 名称 | 投稿内容 | 対応カテゴリ |
|-----------|------|---------|------------|
| `@MHLWitter` | 厚生労働省 | 感染症対策・労務安全衛生 | pandemic, labor |
| `@JIHS_JP` | 国立健康危機管理研究機構 | 国内感染症サーベイランス（旧NIID、2025年再編） | pandemic |

※ アカウント名・usernameは変更される場合がある。DynamoDB の `enabled` フラグで無効化対応。
※ NEXCO中日本は今回スコープ外。必要に応じて `@c_nexco_tokyo` / `@c_nexco_nagoya` / `@c_nexco_kana` を追加可能。

### 3-6. 増分取得 (since_id)

```
第1回:
  search_recent(query="(from:UN_NERV OR ...)", start_time=当日0時UTC)
  → meta.newest_id = "1900000000000000001"
  → DynamoDB に保存: PK=CURSOR#official_account_route, SK=META

第2回 (5分後):
  search_recent(query="(from:UN_NERV OR ...)", since_id="1900000000000000001")
  → meta.newest_id = "1900000000000000005"（差分のみ取得）
  → DynamoDB を更新

初回 or カーソル消失時:
  start_time にフォールバック（当日0時UTC）
```

#### DynamoDB カーソル保存

| PK | SK | newest_id | updated_at |
|----|-----|-----------|-----------|
| `CURSOR#official_account_route` | `META` | `1900000000000000005` | `2026-03-12T10:05:00Z` |

### 3-7. S3保存設計

```
facts/official/{date}/
  └── {HHmm}.json
```

```json
{
  "fetched_at": "2026-03-12T10:05:00Z",
  "since_id": "1900000000000000001",
  "newest_id": "1900000000000000005",
  "result_count": 4,
  "tweets": [
    {
      "id": "1900000000000000005",
      "author_id": "123456789",
      "author_username": "UN_NERV",
      "author_name": "特務機関NERV",
      "text": "【地震情報】12日10時25分頃...",
      "created_at": "2026-03-12T01:25:30.000Z",
      "metrics": {
        "retweet_count": 1500,
        "like_count": 3000
      }
    }
  ]
}
```

### 3-8. xdk ライブラリでの呼び出し（既存パターン）

```python
# build_official_queries() でクエリ構築 → 複数クエリをループ実行
queries = build_official_queries(usernames)   # utils.py の新関数

for query in queries:
    first_page = next(client.posts.search_recent(
        query=query,                   # (from:A OR from:B ...) lang:ja -is:retweet
        since_id=last_newest_id,       # DynamoDB から取得
        max_results=100,
        tweet_fields=["created_at", "author_id", "text", "public_metrics"],
        expansions=["author_id"],      # ユーザー名を展開
        user_fields=["username", "name"],
    ))

    tweets += first_page.data or []
    # newest_id の最大値を次回の since_id として保存

tweets = first_page.data or []
meta = first_page.meta              # newest_id, oldest_id, result_count
users = first_page.includes.users   # 展開されたユーザー情報
```

### 3-9. DynamoDB アカウントマスタ設計

アカウントリストはハードコードせず DynamoDB で管理する。

#### キーパターン

| キー | 値 |
|------|-----|
| PK | `OFFICIAL_ACCT#{username}` |
| SK | `META` |
| GSI1PK | `TYPE#OFFICIAL_ACCT` |
| GSI1SK | `#{username}` |

#### 属性

| 属性名 | 型 | 説明 |
|---|---|---|
| `username` | string | X ハンドル（`@` なし、例: `UN_NERV`） |
| `displayName` | string | 表示名（例: `特務機関NERV防災`） |
| `description` | string | 投稿内容の説明 |
| `categories` | list | 対応カテゴリ（分類ラベル用途。例: `["earthquake", "flood"]`） |
| `priorityGroup` | string | グループ分類（disaster / fire / traffic / infra / local / labor / geopolitics / pandemic） |
| `enabled` | bool | 有効フラグ（アカウント凍結・改名時に `false` に更新） |
| `addedAt` | string | 登録日時（ISO 8601） |

#### アクセスパターン

| 用途 | クエリ |
|------|--------|
| 全有効アカウント取得（クエリ構築時） | GSI1 `TYPE#OFFICIAL_ACCT` → Lambda で `enabled=true` フィルタ |
| 個別アカウント取得・更新 | PK `OFFICIAL_ACCT#{username}` |

#### シードスクリプト

```bash
python scripts/seed_official_account_master.py <テーブル名>
```

---

## 4. 共通: DynamoDB カーソル設計

全 Collector で統一的なカーソル管理パターンを使用する。

### テーブル: TrendTable（既存）

| PK | SK | 属性 |
|----|-----|------|
| `CURSOR#jma_collector` | `META` | `last_eid`, `updated_at` |
| `CURSOR#news_collector#{category}` | `META` | `last_pub_date`, `updated_at` |
| `CURSOR#official_account_route` | `META` | `newest_id`, `updated_at` |
| `CURSOR#keyword_route#{category}` | `META` | `newest_id`, `updated_at` |
| `CURSOR#trends_route` | `META` | `newest_id`, `updated_at` |

※ 既存の keyword_search / trend_fetcher にも since_id カーソルを追加可能。

---

## 5. 利用不可API一覧（調査済み）

以下のデータソースは正式な公開APIが存在しないため、Collector での利用は見送る。

| データソース | 状態 | 代替手段 |
|------------|------|---------|
| JARTIC (道路交通情報) | APIなし、スクレイピング規約違反 | NewsCollector + OfficialCollector で対応 |
| NEXCO各社 | 正式APIなし、内部JSON不安定 | OfficialCollector (@e_nexco_bousai, @w_nexco_news 等) で対応 |
| VICS | 法人契約のみ、高額 | 対象外 |
| 各電力会社 停電情報 | 正式APIなし、内部JSON不安定 | OfficialCollector (@TEPCOPG, @KANDEN_souhai 等) で対応 |
| 国土交通省 xROAD | 開発中、要調査 | 将来的に JmaCollector に追加可能 |

---

## 参考リンク

- [気象庁防災情報ページ](https://www.jma.go.jp/bosai/)
- [気象庁JSONデータ解説 (Qiita)](https://qiita.com/michan06/items/48503631dd30275288f7)
- [Google News RSS パラメータ解説](https://www.newscatcherapi.com/blog-posts/google-news-rss-search-parameters-the-missing-documentaiton)
- [X API v2 Search Recent Posts](https://docs.x.com/x-api/posts/search-recent-posts)
- [Bing News Search API](https://learn.microsoft.com/en-us/bing/search-apis/bing-news-search/reference/endpoints)
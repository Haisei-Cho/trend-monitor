# SCMリスク監視システム (trend-monitor)

## プロジェクト概要

X(Twitter) APIを用いたSCM(サプライチェーン)リスク監視システム。
トレンド取得 → カテゴリ分類 → ファクトチェック → リスク評価・通知 の5ステップで構成。

## アーキテクチャ

- **IaC**: AWS SAM (`template.yaml`)
- **ランタイム**: Python 3.13
- **DB**: DynamoDB Single Table Design (PK/SK + GSI1 + GSI2 + TTL)
- **ストレージ**: S3 (生データ保存、90日ライフサイクル)
- **AI**: Amazon Bedrock (Claude Sonnet)
- **通知**: SNS Topic

## ディレクトリ構成

```
trend-monitor/
├── template.yaml              # SAMテンプレート
├── function/
│   ├── trend_fetcher/         # ルートA: トレンド線 (X API → Bedrock → S3)
│   │   └── trend_fetcher_function.py
│   └── keyword_search/        # ルートB: キーワード線 (DynamoDB → build_query → X API → S3)
│       └── keyword_search_function.py
├── layers/
│   └── common/                # Lambda Layer (共通ユーティリティ)
│       ├── utils.py           # build_query() 等
│       └── requirements.txt
├── scripts/
│   └── seed_keyword_master.py # キーワードマスタデータ投入スクリプト
├── tests/
│   └── test_build_query.py    # build_query テスト (pytest)
└── docs/
    ├── 設計書.md               # 業務設計書
    └── DynamoDB設計書.xlsx     # DynamoDBテーブル設計
```

## コーディング規約

- **コメント・docstring・変数名**: 日本語で書く
- **Lambda handler ファイル名**: `{機能名}_function.py` (例: `trend_fetcher_function.py`)
- **template.yaml の Handler**: `{機能名}_function.lambda_handler`
- **`from __future__ import annotations`**: 使用しない (Python 3.13)
- **シンプルさ優先**: 不要な抽象化・過剰なエラーハンドリングは追加しない
- **argparse不使用**: CLIスクリプトは `sys.argv` で十分

## DynamoDB設計 (Single Table)

### テーブル: TrendTable

| キー | 用途 |
|------|------|
| PK/SK | メインアクセス |
| GSI1 (GSI1PK/GSI1SK) | TYPE別一覧取得 |
| GSI2 (GSI2PK/GSI2SK) | 時系列・カテゴリ別検索 |
| TTL (`ttl`) | イベントデータの自動削除 |

### マスタデータ キーパターン

| エンティティ | PK | SK | GSI1PK | GSI1SK |
|---|---|---|---|---|
| リスクカテゴリ | `RISK_CAT#{id}` | `META` | `TYPE#RISK_CAT` | `#{id}` |
| キーワード | `KW#{keyword}` | `CAT#{category_id}` | `TYPE#KEYWORD` | `CAT#{category_id}` |
| 除外ルール | `EXCLUSION#{rule_id}` | `META` | `TYPE#EXCLUSION` | `#{rule_id}` |
| 拠点 | `SITE#{site_id}` | `META` | `TYPE#SITE` | `SITE_TYPE#{type}#{site_id}` |

## キーワード設計

### 監視ロジック

`監視クエリ = (リスクKW) AND (拠点KW) lang:ja -is:retweet -(除外KW)`

### リスクカテゴリ (8種)

earthquake(地震・津波), flood(風水害), fire(火災・爆発), traffic(交通障害),
infra(停電・インフラ障害), labor(労務・操業リスク), geopolitics(地政学・貿易), pandemic(感染症)

### 拠点種別

- 港湾: 国際戦略港湾(5) + 国際拠点港湾(18)
- 空港: 会社管理空港(4) + 国管理空港(19)
- 幹線道路: NEXCO主要高速道路(33)
- 自社工場・倉庫・サプライヤー: 別スクリプトで生成 (未実装)

### X API制約

- クエリ最大長: 1024文字 (拠点数が多い場合はクエリ分割が必要)

## テスト

```bash
# pytest 実行
pytest tests/ -v -s

# 単体実行
python tests/test_build_query.py
```

## マスタデータ投入

```bash
python scripts/seed_keyword_master.py <テーブル名>
```

## SAMデプロイ

```bash
sam build
sam deploy --guided
```

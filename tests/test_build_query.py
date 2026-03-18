"""
build_query テスト

シードデータから DynamoDB 取得を模倣し、
全カテゴリのクエリ生成結果を検証する。
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "layers"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from common.utils import build_query, build_official_queries
from scripts.seed_keyword_master import get_all_items

# X API クエリ上限
X_API_QUERY_MAX_LENGTH = 1024


# ──────────────────────────────────────────────
# DynamoDB 取得を模倣するフィクスチャ
# ──────────────────────────────────────────────

@pytest.fixture()
def master_data():
    """
    get_all_items() の結果を DynamoDB の GSI1 Query と同様に
    TYPE別に振り分けて返す。
    """
    all_items = get_all_items()

    risk_kw: dict[str, list[str]] = {}
    site_kw: list[str] = []
    exc_kw: list[str] = []

    for item in all_items:
        gsi1pk = item.get("GSI1PK", "")

        if gsi1pk == "TYPE#KEYWORD":
            cat = item["category_id"]
            risk_kw.setdefault(cat, []).append(item["keyword"])

        elif gsi1pk == "TYPE#SITE":
            site_kw.extend(item.get("keywords", []))

        elif gsi1pk == "TYPE#EXCLUSION":
            exc_kw.extend(item.get("keywords", []))

    return risk_kw, sorted(set(site_kw)), exc_kw


@pytest.fixture()
def sample_sites():
    """拠点キーワードのサンプル（拠点マスタ未投入のためテスト用）"""
    return ["豊田市", "名古屋港", "東名高速", "堺市", "成田空港"]


# ──────────────────────────────────────────────
# テスト
# ──────────────────────────────────────────────

class TestMasterDataStructure:
    """マスタデータ構造の検証"""

    def test_risk_categories_count(self, master_data):
        risk_kw, _, _ = master_data
        assert len(risk_kw) == 8

    def test_all_categories_have_keywords(self, master_data):
        risk_kw, _, _ = master_data
        expected = [
            "earthquake", "flood", "fire", "traffic",
            "infra", "labor", "geopolitics", "pandemic",
        ]
        for cat in expected:
            assert cat in risk_kw, f"カテゴリ {cat} のキーワードがない"
            assert len(risk_kw[cat]) > 0

    def test_exclusion_keywords_exist(self, master_data):
        _, _, exc_kw = master_data
        assert "ゲーム" in exc_kw
        assert "炎上商法" in exc_kw


class TestBuildQueryBasic:
    """build_query の基本動作"""

    def test_simple_query(self):
        queries = build_query(["地震"], ["豊田市"])
        assert queries == ["(地震) (豊田市) lang:ja"]

    def test_with_exclusion(self):
        queries = build_query(["地震"], ["豊田市"], ["ゲーム", "アニメ"])
        assert queries == ["(地震) (豊田市) lang:ja -(ゲーム OR アニメ)"]

    def test_empty_risk_raises(self):
        with pytest.raises(ValueError):
            build_query([], ["豊田市"])

    def test_empty_site_raises(self):
        with pytest.raises(ValueError):
            build_query(["地震"], [])


class TestBuildQueryAllCategories:
    """全カテゴリでクエリ生成し、内容と長さを検証"""

    def test_all_categories_generate_valid_query(self, master_data, sample_sites):
        risk_kw, _, exc_kw = master_data

        for cat_id, keywords in risk_kw.items():
            queries = build_query(keywords, sample_sites, exc_kw)

            for query in queries:
                # 基本構造の検証
                assert query.startswith("(")
                assert "lang:ja" in query
                assert "is:retweet" in query  # 除外ルール経由で含まれる

                # リスクキーワードが含まれている
                for kw in keywords:
                    assert kw in query, f"{cat_id}: {kw} がクエリに含まれていない"

    def test_all_queries_within_length_limit(self, master_data, sample_sites):
        risk_kw, _, exc_kw = master_data

        for cat_id, keywords in risk_kw.items():
            queries = build_query(keywords, sample_sites, exc_kw)
            for i, query in enumerate(queries):
                assert len(query) <= X_API_QUERY_MAX_LENGTH, (
                    f"{cat_id}[{i}]: クエリ長 {len(query)} > {X_API_QUERY_MAX_LENGTH}"
                )

    def test_all_queries_within_length_limit_full_sites(self, master_data):
        """全拠点KWを使用した場合も1024文字以内に収まること"""
        risk_kw, site_kw, exc_kw = master_data

        for cat_id, keywords in risk_kw.items():
            queries = build_query(keywords, site_kw, exc_kw)
            for i, query in enumerate(queries):
                assert len(query) <= X_API_QUERY_MAX_LENGTH, (
                    f"{cat_id}[{i}]: クエリ長 {len(query)} > {X_API_QUERY_MAX_LENGTH}"
                )

    def test_full_sites_split_into_multiple_queries(self, master_data):
        """全拠点KWの場合、複数クエリに分割されること"""
        risk_kw, site_kw, exc_kw = master_data
        queries = build_query(risk_kw["earthquake"], site_kw, exc_kw)
        assert len(queries) > 1

    def test_exclusion_applied(self, master_data, sample_sites):
        risk_kw, _, exc_kw = master_data
        queries = build_query(risk_kw["fire"], sample_sites, exc_kw)
        assert "炎上商法" in queries[0]
        assert "ゲーム" in queries[0]

    def test_no_exclusion(self, master_data, sample_sites):
        risk_kw, _, _ = master_data
        queries = build_query(risk_kw["earthquake"], sample_sites)
        assert "-(" not in queries[0]


class TestBuildQueryOutput:
    """全パターンの出力を表示（pytest -s で確認）"""

    def test_print_all_queries(self, master_data, capsys):
        risk_kw, site_kw, exc_kw = master_data

        category_names = {
            "earthquake": "地震・津波", "flood": "風水害",
            "fire": "火災・爆発", "traffic": "交通障害",
            "infra": "停電・インフラ障害", "labor": "労務・操業リスク",
            "geopolitics": "地政学・貿易", "pandemic": "感染症",
        }

        print("\n" + "=" * 70)
        print(f"全カテゴリ クエリ生成結果（拠点KW: {len(site_kw)}件）")
        print(f"除外KW: {exc_kw}")
        print("=" * 70)

        for cat_id, keywords in risk_kw.items():
            queries = build_query(keywords, site_kw, exc_kw)
            name = category_names.get(cat_id, cat_id)
            print(f"\n■ {name}（{cat_id}）— {len(queries)}クエリ")
            for i, q in enumerate(queries):
                print(f"  [{i + 1}] {len(q)}文字: {q}")


class TestBuildOfficialQueries:
    """build_official_queries の動作検証"""

    SAMPLE_USERNAMES = [
        "UN_NERV", "JMA_kishou", "JMA_bousai", "Kantei_Saigai",
        "FDMA_JAPAN", "CAO_BOUSAI",
    ]

    def test_single_query_within_limit(self):
        queries = build_official_queries(self.SAMPLE_USERNAMES)
        assert len(queries) == 1
        assert len(queries[0]) <= 512

    def test_query_contains_from_operators(self):
        queries = build_official_queries(self.SAMPLE_USERNAMES)
        for username in self.SAMPLE_USERNAMES:
            assert f"from:{username}" in queries[0]

    def test_query_contains_suffix(self):
        queries = build_official_queries(self.SAMPLE_USERNAMES)
        assert "lang:ja" in queries[0]
        assert "-is:retweet" in queries[0]

    def test_empty_list_raises(self):
        with pytest.raises(ValueError):
            build_official_queries([])

    def test_split_when_exceeds_512(self):
        # 512文字を超えるよう長いusernameを大量に作る
        long_usernames = [f"VeryLongAccountUsername{i:03d}" for i in range(30)]
        queries = build_official_queries(long_usernames)
        assert len(queries) > 1
        for q in queries:
            assert len(q) <= 512
            assert "lang:ja" in q
            assert "-is:retweet" in q

    def test_all_usernames_covered_after_split(self):
        long_usernames = [f"VeryLongAccountUsername{i:03d}" for i in range(30)]
        queries = build_official_queries(long_usernames)
        combined = " ".join(queries)
        for u in long_usernames:
            assert f"from:{u}" in combined

    def test_full_seed_accounts_within_limit(self):
        """実際の23アカウントが512文字以内に収まること"""
        seed_usernames = [
            "UN_NERV", "JMA_kishou", "JMA_bousai", "Kantei_Saigai",
            "FDMA_JAPAN", "CAO_BOUSAI", "Tokyo_Fire_D",
            "JREast_official", "JRCentral_OFL", "e_nexco_bousai",
            "w_nexco_news", "MLIT_JAPAN", "MLIT_river",
            "TEPCOPG", "KANDEN_souhai", "Official_Chuden", "TH_nw_official",
            "tokyo_bousai", "osaka_bousai",
            "meti_NIPPON", "MofaJapan_jp", "MHLWitter", "JIHS_JP",
        ]
        queries = build_official_queries(seed_usernames)
        assert len(queries) == 1
        assert len(queries[0]) <= 512


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

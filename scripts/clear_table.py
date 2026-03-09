# -*- coding: utf-8 -*-
"""
DynamoDBテーブルの全データを削除するスクリプト。

使い方:
    python scripts/clear_table.py <テーブル名> --profile <プロファイル名>

    # 確認プロンプトをスキップ
    python scripts/clear_table.py <テーブル名> --profile <プロファイル名> --yes
"""

import argparse

import boto3

DYNAMODB_REGION = "ap-northeast-1"


def clear_table(table_name: str, profile: str | None = None, skip_confirm: bool = False) -> None:
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    dynamodb = session.resource("dynamodb", region_name=DYNAMODB_REGION)
    table = dynamodb.Table(table_name)

    # キースキーマ取得
    key_names = [k["AttributeName"] for k in table.key_schema]

    # 件数確認
    table.reload()
    count = table.item_count
    print(f"テーブル: {table_name}")
    print(f"件数（概算）: {count}")

    if not skip_confirm:
        ans = input(f"\n全データを削除しますか？ (yes/no): ")
        if ans != "yes":
            print("中止しました")
            return

    # スキャン → 削除
    deleted = 0
    scan_kwargs: dict = {}

    while True:
        response = table.scan(**scan_kwargs)
        items = response.get("Items", [])

        with table.batch_writer() as batch:
            for item in items:
                key = {k: item[k] for k in key_names}
                batch.delete_item(Key=key)
                deleted += 1

        if deleted % 100 == 0 and deleted > 0:
            print(f"  削除中... {deleted}件")

        if "LastEvaluatedKey" not in response:
            break
        scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    print(f"\n完了: {deleted}件削除 → {table_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DynamoDBテーブルの全データを削除")
    parser.add_argument("table_name", help="テーブル名")
    parser.add_argument("--profile", default=None, help="AWS CLIプロファイル名")
    parser.add_argument("--yes", action="store_true", help="確認プロンプトをスキップ")
    args = parser.parse_args()

    clear_table(args.table_name, args.profile, args.yes)
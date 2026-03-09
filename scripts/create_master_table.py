# -*- coding: utf-8 -*-
"""
DynamoDB SupplyChainMaster テーブルを作成するスクリプト（boto3版）
"""
import argparse
import boto3

DYNAMODB_REGION = "ap-northeast-1"
TABLE_NAME = "SupplyChainMaster"


def get_dynamodb_client(profile: str | None = None):
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    return session.client("dynamodb", region_name=DYNAMODB_REGION)


def create_table(dynamodb) -> None:
    """SupplyChainMaster テーブルを作成"""
    print(f"テーブル '{TABLE_NAME}' を作成中...")

    try:
        dynamodb.create_table(
            TableName=TABLE_NAME,
            # キー定義
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            # 属性定義（キーとGSIで使う属性のみ）
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gsi1pk", "AttributeType": "S"},
                {"AttributeName": "gsi1sk", "AttributeType": "S"},
                {"AttributeName": "gsi2pk", "AttributeType": "S"},
                {"AttributeName": "gsi2sk", "AttributeType": "S"},
            ],
            # GSI定義
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "GSI1",
                    "KeySchema": [
                        {"AttributeName": "gsi1pk", "KeyType": "HASH"},
                        {"AttributeName": "gsi1sk", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "GSI2",
                    "KeySchema": [
                        {"AttributeName": "gsi2pk", "KeyType": "HASH"},
                        {"AttributeName": "gsi2sk", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            # オンデマンド課金
            BillingMode="PAY_PER_REQUEST",
        )
    except dynamodb.exceptions.ResourceInUseException:
        print(f"  テーブル '{TABLE_NAME}' は既に存在します。")
        return

    # テーブルがACTIVEになるまで待機
    print("  テーブル作成待機中...")
    waiter = dynamodb.get_waiter("table_exists")
    waiter.wait(TableName=TABLE_NAME)
    print(f"  テーブル '{TABLE_NAME}' 作成完了!")

    # ポイントインタイムリカバリを有効化
    dynamodb.update_continuous_backups(
        TableName=TABLE_NAME,
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
    )
    print("  ポイントインタイムリカバリを有効化")


def delete_table(dynamodb) -> None:
    """テーブルを削除"""
    print(f"テーブル '{TABLE_NAME}' を削除中...")
    try:
        dynamodb.delete_table(TableName=TABLE_NAME)
        waiter = dynamodb.get_waiter("table_not_exists")
        waiter.wait(TableName=TABLE_NAME)
        print("  削除完了!")
    except dynamodb.exceptions.ResourceNotFoundException:
        print("  テーブルが存在しません。")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", help="AWS profile name")
    parser.add_argument("--delete", action="store_true", help="Delete the table")
    args = parser.parse_args()

    dynamodb = get_dynamodb_client(args.profile)

    if args.delete:
        delete_table(dynamodb)
    else:
        create_table(dynamodb)


if __name__ == "__main__":
    main()

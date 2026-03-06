from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any

import boto3

from seed_data import EXCLUSION_KEYWORDS, RISK_KEYWORDS, SITE_KEYWORDS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ["TABLE_NAME"]


def send_cfn_response(
    event: dict[str, Any],
    context: Any,
    status: str,
    data: dict[str, Any],
    physical_resource_id: str,
    reason: str | None = None,
) -> None:
    body = json.dumps(
        {
            "Status": status,
            "Reason": reason or f"See CloudWatch Log Stream: {context.log_stream_name}",
            "PhysicalResourceId": physical_resource_id,
            "StackId": event["StackId"],
            "RequestId": event["RequestId"],
            "LogicalResourceId": event["LogicalResourceId"],
            "Data": data,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        event["ResponseURL"],
        data=body,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(body))},
    )
    with urllib.request.urlopen(req) as response:
        logger.info("CloudFormation response status=%s", response.status)


def build_items() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []

    for category_id, keywords in RISK_KEYWORDS.items():
        for keyword in keywords:
            items.append(
                {
                    "PK": f"KEYWORD#{category_id}",
                    "SK": f"KEYWORD#{keyword}",
                    "GSI1PK": "TYPE#KEYWORD",
                    "GSI1SK": f"{category_id}#{keyword}",
                    "type": "keyword",
                    "category_id": category_id,
                    "keyword": keyword,
                }
            )

    items.append(
        {
            "PK": "SITE#DEFAULT",
            "SK": "SITE#DEFAULT",
            "GSI1PK": "TYPE#SITE",
            "GSI1SK": "DEFAULT",
            "type": "site",
            "keywords": SITE_KEYWORDS,
        }
    )
    items.append(
        {
            "PK": "EXCLUSION#DEFAULT",
            "SK": "EXCLUSION#DEFAULT",
            "GSI1PK": "TYPE#EXCLUSION",
            "GSI1SK": "DEFAULT",
            "type": "exclusion",
            "keywords": EXCLUSION_KEYWORDS,
        }
    )
    return items


def seed_table(table_name: str) -> int:
    table = dynamodb.Table(table_name)
    items = build_items()
    with table.batch_writer(overwrite_by_pkeys=["PK", "SK"]) as batch:
        for item in items:
            batch.put_item(Item=item)
    return len(items)


def delete_seeded_items(table_name: str) -> int:
    table = dynamodb.Table(table_name)
    items = build_items()
    with table.batch_writer() as batch:
        for item in items:
            batch.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})
    return len(items)


def handler(event: dict[str, Any], context: Any) -> None:
    physical_resource_id = f"{TABLE_NAME}-seed"
    request_type = event["RequestType"]
    logger.info("Seed request: %s", json.dumps(event))

    try:
        if request_type in {"Create", "Update"}:
            count = seed_table(TABLE_NAME)
            send_cfn_response(
                event,
                context,
                "SUCCESS",
                {"SeededItemCount": count, "TableName": TABLE_NAME},
                physical_resource_id,
            )
            return

        if request_type == "Delete":
            count = delete_seeded_items(TABLE_NAME)
            send_cfn_response(
                event,
                context,
                "SUCCESS",
                {"DeletedItemCount": count, "TableName": TABLE_NAME},
                physical_resource_id,
            )
            return

        raise ValueError(f"Unsupported request type: {request_type}")
    except Exception as exc:
        logger.exception("Seed failed")
        try:
            send_cfn_response(
                event,
                context,
                "FAILED",
                {"Error": str(exc), "TableName": TABLE_NAME},
                physical_resource_id,
                reason=str(exc),
            )
        except urllib.error.URLError:
            logger.exception("Failed to send CloudFormation failure response")
        raise

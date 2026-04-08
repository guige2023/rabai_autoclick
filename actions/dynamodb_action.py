"""DynamoDB action module for RabAI AutoClick.

Provides NoSQL document and key-value operations via AWS DynamoDB API
for scalable, serverless data storage.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import hashlib
import hmac
import base64
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DynamoDBAction(BaseAction):
    """AWS DynamoDB integration for NoSQL operations.

    Supports CRUD operations on tables, queries with partition/sort keys,
    scans, and batch operations.

    Args:
        config: DynamoDB configuration containing region, table_name,
                access_key, and secret_key
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.region = self.config.get("region", "us-east-1")
        self.table_name = self.config.get("table_name", "")
        self.access_key = self.config.get("access_key", "")
        self.secret_key = self.config.get("secret_key", "")
        self.endpoint = self.config.get(
            "endpoint",
            f"https://dynamodb.{self.region}.amazonaws.com"
        )

    def _sign(self, key: bytes, msg: str) -> bytes:
        """Create HMAC signature."""
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    def _get_signature_key(self, key: str, date_stamp: str, region: str) -> bytes:
        """Get signing key."""
        k_date = self._sign(("AWS4" + key).encode("utf-8"), date_stamp)
        k_region = self._sign(k_date, region)
        k_service = self._sign(k_region, "dynamodb")
        k_signing = self._sign(k_service, "aws4_request")
        return k_signing

    def _make_request(self, action: str, payload: Dict) -> Dict[str, Any]:
        """Make signed HTTP request to DynamoDB."""
        host = f"dynamodb.{self.region}.amazonaws.com"
        service = "dynamodb"
        algorithm = "AWS4-HMAC-SHA256"

        now = datetime.utcnow()
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")

        canonical_uri = "/"
        canonical_querystr = ""
        payload_json = json.dumps(payload, separators=(",", ":"))
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

        headers = {
            "Content-Type": "application/x-amz-json-1.0",
            "X-Amz-Target": action,
            "X-Amz-Date": amz_date,
            "Host": host,
        }

        sorted_headers = sorted(headers.items())
        canonical_headers = "\n".join(f"{k}:{v}" for k, v in sorted_headers) + "\n"
        signed_headers = ";".join(k.lower() for k, v in sorted_headers)

        canonical_request = "\n".join([
            "POST", canonical_uri, canonical_querystr,
            canonical_headers, signed_headers, payload_hash
        ])

        credential_scope = f"{date_stamp}/{self.region}/{service}/aws4_request"
        string_to_sign = "\n".join([
            algorithm, amz_date, credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
        ])

        signing_key = self._get_signature_key(self.secret_key, date_stamp, self.region)
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        authorization = (
            f"{algorithm} "
            f"Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )
        headers["Authorization"] = authorization

        body = payload_json.encode("utf-8")
        req = Request(
            self.endpoint,
            data=body,
            headers=headers,
            method="POST",
        )

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def put_item(
        self,
        item: Dict[str, Any],
        table: Optional[str] = None,
        condition: Optional[str] = None,
    ) -> ActionResult:
        """Put an item into a table.

        Args:
            item: Item attributes
            table: Table name (uses config default if not provided)
            condition: Optional condition expression

        Returns:
            ActionResult with put status
        """
        table_name = table or self.table_name
        if not table_name:
            return ActionResult(success=False, error="Missing table_name")

        payload = {"TableName": table_name, "Item": item}
        if condition:
            payload["ConditionExpression"] = condition

        result = self._make_request("DynamoDB_20120810.PutItem", payload)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"put": True})

    def get_item(
        self,
        key: Dict[str, Any],
        table: Optional[str] = None,
        projection: Optional[str] = None,
    ) -> ActionResult:
        """Get an item by key.

        Args:
            key: Primary key attributes
            table: Table name
            projection: Optional projection expression

        Returns:
            ActionResult with item data
        """
        table_name = table or self.table_name
        if not table_name:
            return ActionResult(success=False, error="Missing table_name")

        payload = {"TableName": table_name, "Key": key}
        if projection:
            payload["ProjectionExpression"] = projection

        result = self._make_request("DynamoDB_20120810.GetItem", payload)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        item = result.get("Item", {})
        if not item:
            return ActionResult(success=True, data={"item": None})
        return ActionResult(success=True, data={"item": item})

    def delete_item(
        self,
        key: Dict[str, Any],
        table: Optional[str] = None,
        condition: Optional[str] = None,
    ) -> ActionResult:
        """Delete an item by key.

        Args:
            key: Primary key attributes
            table: Table name
            condition: Optional condition expression

        Returns:
            ActionResult with delete status
        """
        table_name = table or self.table_name
        if not table_name:
            return ActionResult(success=False, error="Missing table_name")

        payload = {"TableName": table_name, "Key": key}
        if condition:
            payload["ConditionExpression"] = condition

        result = self._make_request("DynamoDB_20120810.DeleteItem", payload)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"deleted": True})

    def update_item(
        self,
        key: Dict[str, Any],
        updates: Dict[str, Any],
        table: Optional[str] = None,
    ) -> ActionResult:
        """Update an item.

        Args:
            key: Primary key attributes
            updates: Dict of attribute -> value updates
            table: Table name

        Returns:
            ActionResult with updated item
        """
        table_name = table or self.table_name
        if not table_name:
            return ActionResult(success=False, error="Missing table_name")

        update_expr = "SET " + ", ".join(
            f"#{k} = :{k}" for k in updates.keys()
        )
        expr_names = {f"#{k}": k for k in updates.keys()}
        expr_values = {f":{k}": v for k, v in updates.items()}

        payload = {
            "TableName": table_name,
            "Key": key,
            "UpdateExpression": update_expr,
            "ExpressionAttributeNames": expr_names,
            "ExpressionAttributeValues": expr_values,
        }

        result = self._make_request("DynamoDB_20120810.UpdateItem", payload)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"updated": True})

    def query(
        self,
        key_condition: str,
        index_name: Optional[str] = None,
        filter_expr: Optional[str] = None,
        limit: int = 100,
    ) -> ActionResult:
        """Query items by key condition.

        Args:
            key_condition: Key condition expression
            index_name: Optional secondary index name
            filter_expr: Optional filter expression
            limit: Maximum items to return

        Returns:
            ActionResult with items list
        """
        if not self.table_name:
            return ActionResult(success=False, error="Missing table_name")

        payload = {
            "TableName": self.table_name,
            "KeyConditionExpression": key_condition,
            "Limit": limit,
        }
        if index_name:
            payload["IndexName"] = index_name
        if filter_expr:
            payload["FilterExpression"] = filter_expr

        result = self._make_request("DynamoDB_20120810.Query", payload)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"items": result.get("Items", [])},
        )

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute DynamoDB operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "put_item": self.put_item,
            "get_item": self.get_item,
            "delete_item": self.delete_item,
            "update_item": self.update_item,
            "query": self.query,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)

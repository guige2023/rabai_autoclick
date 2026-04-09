"""AWS Lambda action module for RabAI AutoClick.

Provides AWS Lambda operations:
- LambdaInvoker: Invoke Lambda functions
- LambdaAsyncInvoker: Invoke Lambda asynchronously
- LambdaFunctionManager: List, create, update functions
- LambdaEventProcessor: Process Lambda event sources
- LambdaLayerManager: Manage Lambda layers
"""

from __future__ import annotations

import json
import sys
import os
import base64
import hashlib
import time
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


@dataclass
class LambdaConfig:
    """Lambda configuration container."""
    function_name: str
    runtime: str = "python3.11"
    handler: str = "index.handler"
    memory_size: int = 128
    timeout: int = 300
    role_arn: str = ""
    environment: Dict[str, str] = field(default_factory=dict)


class LambdaInvokerAction(BaseAction):
    """Invoke AWS Lambda functions synchronously."""
    action_type = "lambda_invoker"
    display_name = "Lambda函数调用"
    description = "同步调用AWS Lambda函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not BOTO3_AVAILABLE:
            return ActionResult(success=False, message="boto3 not installed: pip install boto3")

        try:
            function_name = params.get("function_name", "")
            payload = params.get("payload", {})
            invocation_type = params.get("invocation_type", "RequestResponse")
            region = params.get("region", "us-east-1")
            qualifier = params.get("qualifier", None)

            if not function_name:
                return ActionResult(success=False, message="function_name is required")

            client = boto3.client("lambda", region_name=region)

            if isinstance(payload, dict):
                payload_bytes = json.dumps(payload).encode("utf-8")
            elif isinstance(payload, str):
                payload_bytes = payload.encode("utf-8")
            else:
                payload_bytes = payload

            invoke_params: Dict[str, Any] = {
                "FunctionName": function_name,
                "InvocationType": invocation_type,
                "Payload": payload_bytes,
            }
            if qualifier:
                invoke_params["Qualifier"] = qualifier

            start_time = time.time()
            response = client.invoke(**invoke_params)
            duration_ms = (time.time() - start_time) * 1000

            status_code = response.get("StatusCode", 0)
            function_error = response.get("FunctionError", None)

            response_payload = response.get("Payload")
            if response_payload:
                response_data = json.loads(response_payload.read().decode("utf-8"))
            else:
                response_data = None

            if function_error:
                return ActionResult(
                    success=False,
                    message=f"Lambda error: {function_error}",
                    data={"status_code": status_code, "function_error": function_error, "duration_ms": duration_ms}
                )

            return ActionResult(
                success=True,
                message=f"Invoked {function_name}: {status_code}",
                data={"status_code": status_code, "response": response_data, "duration_ms": duration_ms}
            )

        except ClientError as e:
            return ActionResult(success=False, message=f"AWS error: {e.response['Error']['Message']}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class LambdaAsyncInvokerAction(BaseAction):
    """Invoke Lambda functions asynchronously."""
    action_type = "lambda_async_invoker"
    display_name = "Lambda异步调用"
    description = "异步调用AWS Lambda函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not BOTO3_AVAILABLE:
            return ActionResult(success=False, message="boto3 not installed: pip install boto3")

        try:
            function_name = params.get("function_name", "")
            payload = params.get("payload", {})
            region = params.get("region", "us-east-1")
            qualifier = params.get("qualifier", None)
            source_arn = params.get("source_arn", None)

            if not function_name:
                return ActionResult(success=False, message="function_name is required")

            client = boto3.client("lambda", region_name=region)

            if isinstance(payload, dict):
                payload_bytes = json.dumps(payload).encode("utf-8")
            elif isinstance(payload, str):
                payload_bytes = payload.encode("utf-8")
            else:
                payload_bytes = payload

            invoke_params: Dict[str, Any] = {
                "FunctionName": function_name,
                "InvocationType": "Event",
                "Payload": payload_bytes,
            }
            if qualifier:
                invoke_params["Qualifier"] = qualifier
            if source_arn:
                invoke_params["SourceArn"] = source_arn

            response = client.invoke(**invoke_params)
            status_code = response.get("StatusCode", 0)

            return ActionResult(
                success=True,
                message=f"Async invoke queued: {status_code}",
                data={"status_code": status_code, "function_name": function_name}
            )

        except ClientError as e:
            return ActionResult(success=False, message=f"AWS error: {e.response['Error']['Message']}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class LambdaFunctionManagerAction(BaseAction):
    """Manage Lambda function lifecycle."""
    action_type = "lambda_function_manager"
    display_name = "Lambda函数管理"
    description = "管理Lambda函数生命周期"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not BOTO3_AVAILABLE:
            return ActionResult(success=False, message="boto3 not installed: pip install boto3")

        try:
            operation = params.get("operation", "list")
            function_name = params.get("function_name", "")
            region = params.get("region", "us-east-1")
            runtime = params.get("runtime", "python3.11")
            handler = params.get("handler", "index.handler")
            memory_size = params.get("memory_size", 128)
            timeout = params.get("timeout", 300)
            role_arn = params.get("role_arn", "")
            environment = params.get("environment", {})
            publish = params.get("publish", False)

            client = boto3.client("lambda", region_name=region)

            if operation == "list":
                functions = []
                paginator = client.get_paginator("list_functions")
                for page in paginator.paginate():
                    for fn in page.get("Functions", []):
                        functions.append({
                            "name": fn["FunctionName"],
                            "runtime": fn["Runtime"],
                            "memory": fn["MemorySize"],
                            "timeout": fn["Timeout"],
                            "state": fn.get("State", "Unknown"),
                        })
                return ActionResult(success=True, message=f"Listed {len(functions)} functions", data={"functions": functions})

            elif operation == "get":
                if not function_name:
                    return ActionResult(success=False, message="function_name required for get")
                response = client.get_function(FunctionName=function_name)
                config = response.get("Configuration", {})
                return ActionResult(
                    success=True,
                    message=f"Function: {function_name}",
                    data={
                        "name": config.get("FunctionName"),
                        "runtime": config.get("Runtime"),
                        "memory": config.get("MemorySize"),
                        "timeout": config.get("Timeout"),
                        "handler": config.get("Handler"),
                        "arn": config.get("FunctionArn"),
                        "state": config.get("State", "Unknown"),
                    }
                )

            elif operation == "delete":
                if not function_name:
                    return ActionResult(success=False, message="function_name required for delete")
                client.delete_function(FunctionName=function_name)
                return ActionResult(success=True, message=f"Deleted: {function_name}")

            elif operation == "update_config":
                if not function_name:
                    return ActionResult(success=False, message="function_name required for update_config")
                update_params: Dict[str, Any] = {}
                if memory_size:
                    update_params["MemorySize"] = memory_size
                if timeout:
                    update_params["Timeout"] = timeout
                if environment:
                    update_params["Environment"] = {"Variables": environment}

                if update_params:
                    client.update_function_configuration(FunctionName=function_name, **update_params)

                return ActionResult(success=True, message=f"Updated config: {function_name}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except ClientError as e:
            return ActionResult(success=False, message=f"AWS error: {e.response['Error']['Message']}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class LambdaEventProcessorAction(BaseAction):
    """Process Lambda event sources (S3, SQS, DynamoDB, etc)."""
    action_type = "lambda_event_processor"
    display_name = "Lambda事件处理"
    description = "处理Lambda事件源"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not BOTO3_AVAILABLE:
            return ActionResult(success=False, message="boto3 not installed: pip install boto3")

        try:
            event = params.get("event", {})
            event_source = params.get("event_source", "")

            if not event:
                return ActionResult(success=False, message="event is required")

            records = []
            s3_records = event.get("Records", [])
            sqs_records = event.get("messages", []) or event.get("Records", [])
            dynamodb_records = event.get("Records", [])

            if s3_records and any("s3" in str(r) for r in s3_records):
                event_source = "s3"
                for record in s3_records:
                    if "s3" in record:
                        s3_info = record["s3"]
                        records.append({
                            "bucket": s3_info.get("bucket", {}).get("name", ""),
                            "key": s3_info.get("object", {}).get("key", ""),
                            "size": s3_info.get("object", {}).get("size", 0),
                            "event_type": record.get("eventName", ""),
                        })

            elif sqs_records and any("messageId" in str(r) for r in sqs_records):
                event_source = "sqs"
                for record in sqs_records:
                    if "messageId" in record:
                        records.append({
                            "message_id": record.get("messageId", ""),
                            "body": record.get("body", ""),
                            "attributes": record.get("messageAttributes", {}),
                            "receipt_handle": record.get("receiptHandle", ""),
                        })

            elif dynamodb_records and any("dynamodb" in str(r) for r in dynamodb_records):
                event_source = "dynamodb"
                for record in dynamodb_records:
                    if "dynamodb" in record:
                        records.append({
                            "event_id": record.get("eventID", ""),
                            "event_name": record.get("eventName", ""),
                            "keys": record.get("dynamodb", {}).get("Keys", {}),
                        })

            else:
                return ActionResult(
                    success=True,
                    message=f"Processed {event_source or 'unknown'} event",
                    data={"event": event, "raw": True}
                )

            return ActionResult(
                success=True,
                message=f"Processed {len(records)} {event_source} records",
                data={"event_source": event_source, "records": records, "count": len(records)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class LambdaLayerManagerAction(BaseAction):
    """Manage Lambda layers."""
    action_type = "lambda_layer_manager"
    display_name = "Lambda层管理"
    description = "管理Lambda层版本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not BOTO3_AVAILABLE:
            return ActionResult(success=False, message="boto3 not installed: pip install boto3")

        try:
            operation = params.get("operation", "list")
            layer_name = params.get("layer_name", "")
            layer_version = params.get("layer_version", None)
            compatible_runtimes = params.get("compatible_runtimes", ["python3.11"])
            region = params.get("region", "us-east-1")
            zip_path = params.get("zip_path", None)

            client = boto3.client("lambda", region_name=region)

            if operation == "list":
                layers = client.list_layers()
                return ActionResult(
                    success=True,
                    message=f"Listed layers",
                    data={"layers": layers.get("Layers", [])}
                )

            elif operation == "publish":
                if not layer_name or not zip_path:
                    return ActionResult(success=False, message="layer_name and zip_path required for publish")

                if not os.path.exists(zip_path):
                    return ActionResult(success=False, message=f"Zip file not found: {zip_path}")

                with open(zip_path, "rb") as f:
                    zip_data = f.read()

                response = client.publish_layer_version(
                    LayerName=layer_name,
                    Description=params.get("description", ""),
                    Content={"ZipFile": zip_data, "Bucket": "", "Key": ""},
                    CompatibleRuntimes=compatible_runtimes,
                )

                return ActionResult(
                    success=True,
                    message=f"Published layer: {layer_name}",
                    data={
                        "layer_arn": response.get("LayerArn"),
                        "version": response.get("Version"),
                    }
                )

            elif operation == "delete":
                if not layer_name or layer_version is None:
                    return ActionResult(success=False, message="layer_name and layer_version required for delete")

                client.delete_layer_version(LayerName=layer_name, VersionNumber=layer_version)
                return ActionResult(success=True, message=f"Deleted layer version {layer_version}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except ClientError as e:
            return ActionResult(success=False, message=f"AWS error: {e.response['Error']['Message']}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")

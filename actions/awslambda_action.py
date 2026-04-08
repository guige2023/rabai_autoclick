"""AWS Lambda invoke action module for RabAI AutoClick.

Provides AWS Lambda function invocation via boto3.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AWSLambdaInvokeAction(BaseAction):
    """Invoke AWS Lambda functions."""
    action_type = "aws_lambda_invoke"
    display_name = "AWS Lambda调用"
    description = "AWS Lambda函数调用"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Invoke Lambda function.

        Args:
            context: Execution context.
            params: Dict with keys:
                - function_name: Lambda function name
                - payload: Event payload (dict)
                - invocation_type: 'RequestResponse' (default), 'Event', 'DryRun'
                - region: AWS region

        Returns:
            ActionResult with Lambda response.
        """
        function_name = params.get('function_name', '')
        payload = params.get('payload', {})
        invocation_type = params.get('invocation_type', 'RequestResponse')
        region = params.get('region', 'us-east-1')

        if not function_name:
            return ActionResult(success=False, message="function_name is required")

        try:
            import boto3
        except ImportError:
            return ActionResult(success=False, message="boto3 not installed. Run: pip install boto3")

        start = time.time()
        try:
            lambda_client = boto3.client('lambda', region_name=region)
            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType=invocation_type,
                Payload=json.dumps(payload)
            )
            payload_response = response['Payload'].read().decode('utf-8')
            if payload_response:
                result_data = json.loads(payload_response)
            else:
                result_data = {}
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Lambda invoked ({invocation_type})",
                data={
                    'status_code': response['StatusCode'],
                    'response': result_data,
                    'function': function_name,
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Lambda error: {str(e)}")

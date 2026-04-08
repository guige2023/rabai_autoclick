"""Batch process action module for RabAI AutoClick.

Provides parallel batch processing for file operations, API calls, and data transformations.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class BatchProcessAction(BaseAction):
    """Execute batch operations in parallel using thread pool.

    Supports batch file processing, API calls, and data transformations.
    """
    action_type = "batch_process"
    display_name = "批量处理"
    description = "并行批量数据处理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch processing.

        Args:
            context: Execution context.
            params: Dict with keys:
                - items: List of items to process
                - operation: 'read_file', 'write_file', 'api_call', 'transform'
                - max_workers: Number of parallel workers (default: 4)
                - operation_params: Additional params for the operation
                - timeout: Timeout per item in seconds

        Returns:
            ActionResult with batch processing results.
        """
        items = params.get('items', [])
        operation = params.get('operation', '')
        max_workers = params.get('max_workers', 4)
        operation_params = params.get('operation_params', {})
        timeout = params.get('timeout', 60)

        if not items:
            return ActionResult(success=False, message="items list is required")
        if not operation:
            return ActionResult(success=False, message="operation is required")

        start = time.time()
        results = []
        errors = []

        def process_item(item: Any) -> Dict[str, Any]:
            """Process a single item based on operation type."""
            try:
                if operation == 'read_file':
                    path = item if isinstance(item, str) else item.get('path', '')
                    if not path or not os.path.exists(path):
                        return {'success': False, 'item': item, 'error': f'File not found: {path}'}
                    with open(path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    return {'success': True, 'item': item, 'data': content}
                elif operation == 'write_file':
                    path = item.get('path', '')
                    content = item.get('content', '')
                    if not path:
                        return {'success': False, 'item': item, 'error': 'path required'}
                    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
                    with open(path, 'w', encoding='utf-8') as f:
                        f.write(content)
                    return {'success': True, 'item': item, 'path': path}
                elif operation == 'transform':
                    value = item if isinstance(item, str) else item.get('value', '')
                    transformer = operation_params.get('transformer', 'upper')
                    if transformer == 'upper':
                        result = value.upper()
                    elif transformer == 'lower':
                        result = value.lower()
                    elif transformer == 'strip':
                        result = value.strip()
                    elif transformer == 'len':
                        result = len(value)
                    else:
                        result = value
                    return {'success': True, 'item': item, 'data': result}
                elif operation == 'api_call':
                    # Generic API call - requires url and method
                    url = item.get('url', '')
                    method = item.get('method', 'GET')
                    headers = item.get('headers', {})
                    data = item.get('data', {})
                    if not url:
                        return {'success': False, 'item': item, 'error': 'url required'}
                    import urllib.request
                    if method == 'GET':
                        req = urllib.request.Request(url, headers=headers)
                    else:
                        body = json.dumps(data).encode('utf-8')
                        headers['Content-Type'] = 'application/json'
                        req = urllib.request.Request(url, data=body, headers=headers, method=method)
                    with urllib.request.urlopen(req, timeout=timeout) as resp:
                        body = resp.read().decode('utf-8')
                        try:
                            result = json.loads(body)
                        except json.JSONDecodeError:
                            result = body
                        return {'success': True, 'item': item, 'data': result, 'status': resp.status}
                else:
                    return {'success': False, 'item': item, 'error': f'Unknown operation: {operation}'}
            except Exception as e:
                return {'success': False, 'item': item, 'error': str(e)}

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_item = {executor.submit(process_item, item): item for item in items}
                for future in as_completed(future_to_item, timeout=timeout * len(items)):
                    result = future.result()
                    if result['success']:
                        results.append(result)
                    else:
                        errors.append(result)

            success_count = len(results)
            error_count = len(errors)
            duration = time.time() - start
            return ActionResult(
                success=True,
                message=f"Batch completed: {success_count} succeeded, {error_count} failed",
                data={
                    'total': len(items),
                    'succeeded': success_count,
                    'failed': error_count,
                    'errors': errors[:10] if errors else [],
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch process error: {str(e)}")


class BatchRenameAction(BaseAction):
    """Batch rename files using patterns."""
    action_type = "batch_rename"
    display_name = "批量重命名"
    description = "批量文件重命名"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Batch rename files.

        Args:
            context: Execution context.
            params: Dict with keys:
                - files: List of file paths
                - pattern: New filename pattern (e.g. 'file_{index:03d}')
                - base_dir: Base directory for renamed files
                - dry_run: If True, only show what would happen

        Returns:
            ActionResult with rename results.
        """
        files = params.get('files', [])
        pattern = params.get('pattern', 'file_{index}')
        base_dir = params.get('base_dir', '')
        dry_run = params.get('dry_run', False)

        if not files:
            return ActionResult(success=False, message="files list is required")

        start = time.time()
        results = []
        for i, old_path in enumerate(files):
            if not os.path.exists(old_path):
                results.append({'success': False, 'old': old_path, 'error': 'File not found'})
                continue
            ext = os.path.splitext(old_path)[1]
            new_name = pattern.format(index=i+1, filename=os.path.basename(old_path), ext=ext)
            new_path = os.path.join(base_dir, new_name) if base_dir else os.path.join(os.path.dirname(old_path), new_name)
            try:
                if not dry_run:
                    os.makedirs(os.path.dirname(new_path) or '.', exist_ok=True)
                    os.rename(old_path, new_path)
                results.append({'success': True, 'old': old_path, 'new': new_path})
            except Exception as e:
                results.append({'success': False, 'old': old_path, 'error': str(e)})

        duration = time.time() - start
        succeeded = sum(1 for r in results if r['success'])
        return ActionResult(
            success=True,
            message=f"Batch rename: {succeeded}/{len(files)} succeeded",
            data={'results': results, 'succeeded': succeeded, 'failed': len(files) - succeeded},
            duration=duration
        )

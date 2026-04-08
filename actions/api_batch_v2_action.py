"""API Batch V2 action module for RabAI AutoClick.

Advanced batch API operations with chunking, parallel
execution, and result aggregation.
"""

import time
import json
import sys
import os
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiBatchV2Action(BaseAction):
    """Advanced batch API with chunking and parallelization.

    Splits large batches into chunks, executes in parallel,
    aggregates results, and handles partial failures.
    """
    action_type = "api_batch_v2"
    display_name = "API批量V2"
    description = "带分块和并行的高级批量API"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch API call.

        Args:
            context: Execution context.
            params: Dict with keys: url, items, chunk_size,
                   max_workers, aggregate, continue_on_partial_failure.

        Returns:
            ActionResult with batch results.
        """
        start_time = time.time()
        try:
            url = params.get('url', '')
            items = params.get('items', [])
            chunk_size = params.get('chunk_size', 10)
            max_workers = params.get('max_workers', 4)
            aggregate = params.get('aggregate', True)
            continue_on_partial = params.get('continue_on_partial_failure', True)

            if not items:
                return ActionResult(success=False, message="items is required", duration=time.time() - start_time)

            chunks = [items[i:i+chunk_size] for i in range(0, len(items), chunk_size)]

            def process_chunk(chunk: List, chunk_idx: int) -> Dict:
                results = []
                from urllib.request import Request, urlopen
                for item in chunk:
                    try:
                        body = json.dumps(item).encode('utf-8')
                        req = Request(url, data=body, headers={'Content-Type': 'application/json'}, method='POST')
                        with urlopen(req, timeout=30) as resp:
                            results.append({'success': True, 'status': resp.status, 'data': json.loads(resp.read())})
                    except Exception as e:
                        results.append({'success': False, 'error': str(e)})
                return {'chunk_idx': chunk_idx, 'results': results, 'success_count': sum(1 for r in results if r.get('success'))}

            all_results = []
            with ThreadPoolExecutor(max_workers=min(len(chunks), max_workers)) as executor:
                futures = {executor.submit(process_chunk, chunk, i): i for i, chunk in enumerate(chunks)}
                for future in as_completed(futures):
                    all_results.append(future.result())

            all_results.sort(key=lambda x: x['chunk_idx'])
            flat_results = []
            for chunk_result in all_results:
                flat_results.extend(chunk_result['results'])

            total_success = sum(r.get('success_count', 0) for r in all_results)
            all_success = total_success == len(items)

            duration = time.time() - start_time
            return ActionResult(
                success=all_success if not continue_on_partial else True,
                message=f"Batch: {total_success}/{len(items)} succeeded",
                data={
                    'total': len(items),
                    'successful': total_success,
                    'failed': len(items) - total_success,
                    'chunks': len(chunks),
                    'results': flat_results,
                },
                duration=duration,
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Batch v2 error: {str(e)}", duration=time.time() - start_time)

"""Data Ledger action module for RabAI AutoClick.

Maintains audit trails and immutable ledgers for
data changes and automation events.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from hashlib import sha256
import hashlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataLedgerAction(BaseAction):
    """Immutable ledger for tracking data changes.

    Records entries with hashes for tamper detection
    and provides query capabilities.
    """
    action_type = "data_ledger"
    display_name = "数据账本"
    description = "维护数据变更的不可变账本"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage ledger entries.

        Args:
            context: Execution context.
            params: Dict with keys: action (append/query/verify),
                   ledger_name, entry, query_filter.

        Returns:
            ActionResult with ledger data.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'append')
            ledger_name = params.get('ledger_name', 'default')
            entry = params.get('entry')
            query_filter = params.get('query_filter', {})
            ledger_dir = params.get('ledger_dir', '/tmp/ledgers')

            os.makedirs(ledger_dir, exist_ok=True)
            ledger_path = os.path.join(ledger_dir, f"ledger_{ledger_name}.jsonl")
            meta_path = os.path.join(ledger_dir, f"ledger_{ledger_name}_meta.json")

            if action == 'append':
                if entry is None:
                    return ActionResult(
                        success=False,
                        message="Entry is required for append",
                        duration=time.time() - start_time,
                    )

                # Load previous hash
                prev_hash = ''
                if os.path.exists(meta_path):
                    with open(meta_path, 'r') as f:
                        meta = json.load(f)
                        prev_hash = meta.get('last_hash', '')

                # Create entry
                entry_data = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'data': entry,
                }

                # Compute hash chain
                entry_str = json.dumps(entry_data, sort_keys=True, default=str)
                entry_hash = sha256((prev_hash + entry_str).encode()).hexdigest()
                entry_data['hash'] = entry_hash
                entry_data['prev_hash'] = prev_hash
                entry_data['sequence'] = 0

                if os.path.exists(meta_path):
                    with open(meta_path, 'r') as f:
                        meta = json.load(f)
                    entry_data['sequence'] = meta.get('last_sequence', -1) + 1

                # Append to ledger
                with open(ledger_path, 'a') as f:
                    f.write(json.dumps(entry_data, default=str) + '\n')

                # Update meta
                meta = {
                    'ledger_name': ledger_name,
                    'last_hash': entry_hash,
                    'last_sequence': entry_data['sequence'],
                    'last_updated': datetime.now(timezone.utc).isoformat(),
                    'total_entries': entry_data['sequence'] + 1,
                }
                with open(meta_path, 'w') as f:
                    json.dump(meta, f)

                return ActionResult(
                    success=True,
                    message=f"Appended entry {entry_data['sequence']} to ledger",
                    data={
                        'sequence': entry_data['sequence'],
                        'hash': entry_hash,
                        'timestamp': entry_data['timestamp'],
                    },
                    duration=time.time() - start_time,
                )

            elif action == 'query':
                entries = []
                if os.path.exists(ledger_path):
                    with open(ledger_path, 'r') as f:
                        for line in f:
                            try:
                                e = json.loads(line)
                                if self._matches_filter(e, query_filter):
                                    entries.append(e)
                            except Exception:
                                pass

                return ActionResult(
                    success=True,
                    message=f"Found {len(entries)} entries",
                    data={'entries': entries, 'count': len(entries)},
                    duration=time.time() - start_time,
                )

            elif action == 'verify':
                if not os.path.exists(ledger_path):
                    return ActionResult(
                        success=False,
                        message="Ledger does not exist",
                        duration=time.time() - start_time,
                    )

                prev_hash = ''
                valid = True
                errors = []
                sequence = 0

                with open(ledger_path, 'r') as f:
                    for i, line in enumerate(f):
                        try:
                            e = json.loads(line)
                            if e.get('prev_hash') != prev_hash:
                                errors.append(f"Hash chain broken at line {i}")
                                valid = False
                            entry_str = json.dumps({'timestamp': e['timestamp'], 'data': e['data']}, sort_keys=True, default=str)
                            computed_hash = sha256((prev_hash + entry_str).encode()).hexdigest()
                            if e.get('hash') != computed_hash:
                                errors.append(f"Invalid hash at sequence {e.get('sequence')}")
                                valid = False
                            prev_hash = e.get('hash', '')
                            sequence = e.get('sequence', i)
                        except Exception as ex:
                            errors.append(f"Parse error at line {i}: {str(ex)}")
                            valid = False

                return ActionResult(
                    success=valid,
                    message=f"Verification: {'PASS' if valid else 'FAIL'}",
                    data={
                        'valid': valid,
                        'total_entries': sequence + 1,
                        'errors': errors,
                    },
                    duration=time.time() - start_time,
                )

            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown action: {action}",
                    duration=time.time() - start_time,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Ledger error: {str(e)}",
                duration=duration,
            )

    def _matches_filter(self, entry: Dict, filter: Dict) -> bool:
        """Check if entry matches filter."""
        if not filter:
            return True
        for key, value in filter.items():
            if entry.get(key) != value:
                return False
        return True


class AuditLogAction(BaseAction):
    """Structured audit logging for automation actions.

    Records who, what, when, and optionally why for
    compliance and debugging.
    """
    action_type = "audit_log"
    display_name = "审计日志"
    description = "结构化审计日志记录"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Record audit log.

        Args:
            context: Execution context.
            params: Dict with keys: action (log/query), event_type,
                   actor, resource, operation, result, metadata.

        Returns:
            ActionResult with audit result.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'log')
            event_type = params.get('event_type', 'automation')
            actor = params.get('actor', 'system')
            resource = params.get('resource', '')
            operation = params.get('operation', '')
            result = params.get('result', 'success')
            metadata = params.get('metadata', {})
            audit_dir = params.get('audit_dir', '/tmp/audit_logs')
            query_filter = params.get('query_filter', {})

            os.makedirs(audit_dir, exist_ok=True)
            audit_path = os.path.join(audit_dir, 'audit.jsonl')

            if action == 'log':
                log_entry = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'event_type': event_type,
                    'actor': actor,
                    'resource': resource,
                    'operation': operation,
                    'result': result,
                    'metadata': metadata,
                }

                with open(audit_path, 'a') as f:
                    f.write(json.dumps(log_entry, default=str) + '\n')

                return ActionResult(
                    success=True,
                    message=f"Audit logged: {actor} {operation} {resource}",
                    data={'entry': log_entry},
                    duration=time.time() - start_time,
                )

            elif action == 'query':
                entries = []
                if os.path.exists(audit_path):
                    with open(audit_path, 'r') as f:
                        for line in f:
                            try:
                                e = json.loads(line)
                                if self._matches_audit_filter(e, query_filter):
                                    entries.append(e)
                            except Exception:
                                pass

                return ActionResult(
                    success=True,
                    message=f"Found {len(entries)} audit entries",
                    data={'entries': entries, 'count': len(entries)},
                    duration=time.time() - start_time,
                )

            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown action: {action}",
                    duration=time.time() - start_time,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Audit error: {str(e)}",
                duration=duration,
            )

    def _matches_audit_filter(self, entry: Dict, filter: Dict) -> bool:
        """Check if audit entry matches filter."""
        if not filter:
            return True
        for key, value in filter.items():
            if key == 'from' or key == 'to':
                ts_key = 'timestamp'
                continue
            if entry.get(key) != value:
                return False
        return True

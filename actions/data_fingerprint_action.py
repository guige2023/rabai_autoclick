"""Data fingerprint action module for RabAI AutoClick.

Provides data fingerprinting operations:
- FingerprintGenerateAction: Generate data fingerprint
- FingerprintVerifyAction: Verify data against fingerprint
- FingerprintCompareAction: Compare two fingerprints
- FingerprintUpdateAction: Update fingerprint on data change
"""

import hashlib
import time
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FingerprintGenerateAction(BaseAction):
    """Generate fingerprint for data."""
    action_type = "fingerprint_generate"
    display_name = "生成指纹"
    description = "生成数据指纹"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            algorithm = params.get("algorithm", "md5")
            if not data:
                return ActionResult(success=False, message="data is required")

            data_str = data if isinstance(data, str) else str(data)
            if algorithm == "md5":
                fp = hashlib.md5(data_str.encode()).hexdigest()
            elif algorithm == "sha1":
                fp = hashlib.sha1(data_str.encode()).hexdigest()
            elif algorithm == "sha256":
                fp = hashlib.sha256(data_str.encode()).hexdigest()
            else:
                fp = hashlib.md5(data_str.encode()).hexdigest()

            return ActionResult(
                success=True,
                data={"fingerprint": fp, "algorithm": algorithm, "length": len(data_str)},
                message=f"Fingerprint generated: {fp[:16]}...",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Fingerprint generate failed: {e}")


class FingerprintVerifyAction(BaseAction):
    """Verify data against fingerprint."""
    action_type = "fingerprint_verify"
    display_name = "验证指纹"
    description = "验证数据指纹"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            expected_fp = params.get("fingerprint", "")

            if not data or not expected_fp:
                return ActionResult(success=False, message="data and fingerprint are required")

            data_str = data if isinstance(data, str) else str(data)
            actual_fp = hashlib.md5(data_str.encode()).hexdigest()

            match = actual_fp == expected_fp

            return ActionResult(
                success=True,
                data={"match": match, "expected": expected_fp, "actual": actual_fp},
                message=f"Fingerprint match: {match}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Fingerprint verify failed: {e}")


class FingerprintCompareAction(BaseAction):
    """Compare two fingerprints."""
    action_type = "fingerprint_compare"
    display_name = "对比指纹"
    description = "对比两个数据指纹"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data_a = params.get("data_a", "")
            data_b = params.get("data_b", "")

            if not data_a or not data_b:
                return ActionResult(success=False, message="data_a and data_b are required")

            fp_a = hashlib.md5(str(data_a).encode()).hexdigest()
            fp_b = hashlib.md5(str(data_b).encode()).hexdigest()

            identical = fp_a == fp_b

            return ActionResult(
                success=True,
                data={"fingerprint_a": fp_a, "fingerprint_b": fp_b, "identical": identical},
                message=f"Fingerprints identical: {identical}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Fingerprint compare failed: {e}")


class FingerprintUpdateAction(BaseAction):
    """Update fingerprint when data changes."""
    action_type = "fingerprint_update"
    display_name = "更新指纹"
    description = "数据变更时更新指纹"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            old_data = params.get("old_data", "")
            new_data = params.get("new_data", "")

            if not old_data or not new_data:
                return ActionResult(success=False, message="old_data and new_data are required")

            fp_old = hashlib.md5(str(old_data).encode()).hexdigest()
            fp_new = hashlib.md5(str(new_data).encode()).hexdigest()

            changed = fp_old != fp_new

            return ActionResult(
                success=True,
                data={"old_fingerprint": fp_old, "new_fingerprint": fp_new, "changed": changed},
                message=f"Data changed: {changed}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Fingerprint update failed: {e}")

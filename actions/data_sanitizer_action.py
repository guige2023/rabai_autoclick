"""Data sanitization and security action module for RabAI AutoClick.

Provides:
- DataSanitizerAction: Sanitize sensitive data
- DataEncryptionAction: Encrypt/decrypt data
- DataSecurityAction: Data security utilities
- DataPrivacyAction: Privacy-preserving operations
"""

import time
import hashlib
import json
import base64
import re
from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataSanitizerAction(BaseAction):
    """Sanitize sensitive data."""
    action_type = "data_sanitizer"
    display_name = "数据脱敏"
    description = "敏感数据脱敏"

    def __init__(self):
        super().__init__()
        self._patterns: Dict[str, str] = {
            "email": r"[\w.-]+@[\w.-]+\.\w+",
            "phone": r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b",
            "ssn": r"\b\d{3}-\d{2}-\d{4}\b",
            "credit_card": r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",
            "ip_address": r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"
        }

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "sanitize")
            data = params.get("data", "")

            if operation == "sanitize":
                if not data:
                    return ActionResult(success=False, message="data required")

                replacement = params.get("replacement", "***")
                pattern_type = params.get("pattern_type", "")

                result = data
                if pattern_type and pattern_type in self._patterns:
                    result = re.sub(self._patterns[pattern_type], replacement, str(data))
                elif pattern_type == "all":
                    for ptype, pattern in self._patterns.items():
                        result = re.sub(pattern, replacement, str(result))
                else:
                    for ptype, pattern in self._patterns.items():
                        result = re.sub(pattern, replacement, str(result))

                return ActionResult(
                    success=True,
                    data={
                        "sanitized": result,
                        "original_size": len(str(data)),
                        "sanitized_size": len(result),
                        "patterns_applied": list(self._patterns.keys())
                    },
                    message=f"Sanitized: replaced sensitive data"
                )

            elif operation == "sanitize_fields":
                data_obj = params.get("data_obj", {})
                fields = params.get("fields", [])
                replacement = params.get("replacement", "***")

                if not isinstance(data_obj, dict):
                    return ActionResult(success=False, message="data_obj must be a dict")

                result = dict(data_obj)
                for field in fields:
                    if field in result:
                        result[field] = replacement

                return ActionResult(
                    success=True,
                    data={
                        "sanitized_fields": fields,
                        "result": result
                    }
                )

            elif operation == "add_pattern":
                name = params.get("name", "")
                pattern = params.get("pattern", "")
                if name and pattern:
                    self._patterns[name] = pattern
                return ActionResult(success=True, data={"patterns": list(self._patterns.keys())})

            elif operation == "list_patterns":
                return ActionResult(success=True, data={"patterns": list(self._patterns.keys())})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Sanitizer error: {str(e)}")


class DataEncryptionAction(BaseAction):
    """Encrypt and decrypt data."""
    action_type = "data_encryption"
    display_name: "数据加密"
    description = "数据加密解密"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "encrypt")
            data = params.get("data", "")
            key = params.get("key", "")

            if operation == "encrypt":
                if not data:
                    return ActionResult(success=False, message="data required")

                algorithm = params.get("algorithm", "aes")
                if not key:
                    key = hashlib.sha256(str(time.time()).encode()).hexdigest()[:32]

                if algorithm == "aes":
                    encrypted = self._simple_encrypt(str(data), key)
                elif algorithm == "base64":
                    encrypted = base64.b64encode(str(data).encode()).decode()
                elif algorithm == "reverse":
                    encrypted = str(data)[::-1]
                else:
                    encrypted = hashlib.sha256(str(data).encode()).hexdigest()

                return ActionResult(
                    success=True,
                    data={
                        "encrypted": encrypted,
                        "algorithm": algorithm,
                        "key": key if not params.get("key") else "provided"
                    }
                )

            elif operation == "decrypt":
                if not data:
                    return ActionResult(success=False, message="data required")

                algorithm = params.get("algorithm", "aes")
                if not key:
                    return ActionResult(success=False, message="key required for decryption")

                if algorithm == "aes":
                    decrypted = self._simple_decrypt(data, key)
                elif algorithm == "base64":
                    decrypted = base64.b64decode(data).decode()
                elif algorithm == "reverse":
                    decrypted = data[::-1]
                else:
                    decrypted = data

                return ActionResult(
                    success=True,
                    data={"decrypted": decrypted, "algorithm": algorithm}
                )

            elif operation == "hash":
                if not data:
                    return ActionResult(success=False, message="data required")

                algo = params.get("algorithm", "sha256")
                if algo == "sha256":
                    hashed = hashlib.sha256(str(data).encode()).hexdigest()
                elif algo == "sha512":
                    hashed = hashlib.sha512(str(data).encode()).hexdigest()
                elif algo == "md5":
                    hashed = hashlib.md5(str(data).encode()).hexdigest()
                elif algo == "blake2b":
                    hashed = hashlib.blake2b(str(data).encode()).hexdigest()
                else:
                    hashed = hashlib.sha256(str(data).encode()).hexdigest()

                return ActionResult(success=True, data={"hash": hashed, "algorithm": algo})

            elif operation == "generate_key":
                length = params.get("length", 32)
                generated = hashlib.sha256(str(time.time()).encode()).hexdigest()[:length]
                return ActionResult(success=True, data={"key": generated, "length": length})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Encryption error: {str(e)}")

    def _simple_encrypt(self, data: str, key: str) -> str:
        encoded = base64.b64encode(data.encode()).decode()
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        return f"{key_hash[:16]}:{encoded}"

    def _simple_decrypt(self, data: str, key: str) -> str:
        try:
            parts = data.split(":")
            if len(parts) == 2:
                return base64.b64decode(parts[1]).decode()
            return data
        except:
            return data


class DataSecurityAction(BaseAction):
    """Data security utilities."""
    action_type = "data_security"
    display_name = "数据安全"
    description = "数据安全工具"

    def __init__(self):
        super().__init__()
        self._audit_log: List[Dict] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "secure")

            if operation == "secure":
                data = params.get("data", {})
                security_level = params.get("level", "standard")

                secured = {
                    "_security": {
                        "level": security_level,
                        "secured_at": time.time(),
                        "version": "1.0"
                    }
                }

                if isinstance(data, dict):
                    secured.update(data)

                self._audit_log.append({
                    "operation": "secure",
                    "level": security_level,
                    "timestamp": time.time()
                })

                return ActionResult(
                    success=True,
                    data={"secured": True, "level": security_level}
                )

            elif operation == "verify":
                data = params.get("data", {})
                if "_security" in data:
                    return ActionResult(
                        success=True,
                        data={
                            "verified": True,
                            "level": data["_security"].get("level"),
                            "secured_at": data["_security"].get("secured_at")
                        }
                    )
                return ActionResult(success=True, data={"verified": False})

            elif operation == "audit":
                return ActionResult(
                    success=True,
                    data={
                        "audit_log": self._audit_log[-50:],
                        "total_entries": len(self._audit_log)
                    }
                )

            elif operation == "check":
                checks = params.get("checks", ["pii", "sensitive"])
                data = params.get("data", "")

                findings = []
                for check in checks:
                    if check == "pii":
                        patterns = [r"[\w.-]+@[\w.-]+\.\w+", r"\b\d{3}-\d{2}-\d{4}\b"]
                        for p in patterns:
                            if re.search(p, str(data)):
                                findings.append({"type": "pii", "pattern": p})
                    elif check == "sensitive":
                        sensitive_keywords = ["password", "secret", "token", "api_key"]
                        for kw in sensitive_keywords:
                            if kw.lower() in str(data).lower():
                                findings.append({"type": "sensitive", "keyword": kw})

                return ActionResult(
                    success=True,
                    data={
                        "findings": findings,
                        "has_issues": len(findings) > 0
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Security error: {str(e)}")


class DataPrivacyAction(BaseAction):
    """Privacy-preserving data operations."""
    action_type = "data_privacy"
    display_name = "数据隐私"
    description = "隐私保护操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "anonymize")
            data = params.get("data", {})

            if operation == "anonymize":
                if not isinstance(data, dict):
                    return ActionResult(success=False, message="data must be a dict")

                sensitive_fields = params.get("sensitive_fields", ["name", "email", "phone", "address"])
                anonymized = dict(data)

                for field in sensitive_fields:
                    if field in anonymized:
                        original = str(anonymized[field])
                        anonymized[field] = hashlib.sha256(original.encode()).hexdigest()[:16]

                return ActionResult(
                    success=True,
                    data={
                        "anonymized": anonymized,
                        "fields_anonymized": len(sensitive_fields)
                    }
                )

            elif operation == "pseudonymize":
                if not isinstance(data, dict):
                    return ActionResult(success=False, message="data must be a dict")

                fields = params.get("fields", ["id", "name"])
                pseudonym_map = {}

                pseudonymized = dict(data)
                for field in fields:
                    if field in pseudonymized:
                        original = str(pseudonymized[field])
                        pseudonym = hashlib.sha256((original + str(time.time())).encode()).hexdigest()[:12]
                        pseudonym_map[original] = pseudonym
                        pseudonymized[field] = pseudonym

                return ActionResult(
                    success=True,
                    data={
                        "pseudonymized": pseudonymized,
                        "pseudonym_count": len(pseudonym_map)
                    }
                )

            elif operation == "k_anonymity":
                quasi_identifiers = params.get("quasi_identifiers", [])
                k = params.get("k", 5)
                dataset = params.get("dataset", [])

                if not quasi_identifiers or not dataset:
                    return ActionResult(success=False, message="quasi_identifiers and dataset required")

                groups = {}
                for row in dataset:
                    key = tuple(row.get(qi) for qi in quasi_identifiers)
                    if key not in groups:
                        groups[key] = []
                    groups[key].append(row)

                anonymized = []
                for group_key, group_rows in groups.items():
                    if len(group_rows) >= k:
                        anonymized.extend(group_rows)

                return ActionResult(
                    success=True,
                    data={
                        "original_count": len(dataset),
                        "anonymized_count": len(anonymized),
                        "k": k,
                        "groups": len(groups)
                    }
                )

            elif operation == "differential_privacy":
                data_list = params.get("data", [])
                epsilon = params.get("epsilon", 1.0)

                noisy_data = []
                for val in data_list:
                    noise = (hashlib.sha256(str(time.time() + val).encode()).hexdigest())
                    noise_val = int(noise[:8], 16) / (2**32 - 1)
                    noise_val = (noise_val - 0.5) * 2 * epsilon
                    noisy_data.append(val + noise_val)

                return ActionResult(
                    success=True,
                    data={
                        "noisy_data": noisy_data,
                        "epsilon": epsilon,
                        "count": len(noisy_data)
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Privacy error: {str(e)}")

"""Data anonymization and masking action module for RabAI AutoClick.

Provides data privacy and anonymization capabilities:
- DataAnonymizerAction: Anonymize sensitive data fields
- PIIDetectorAction: Detect personally identifiable information
- DataMaskerAction: Mask data while preserving format
- FieldAnonymizerAction: Anonymize structured data fields
- DataSwapAction: Swap sensitive data with synthetic data
"""

from typing import Any, Dict, List, Optional, Tuple, Callable
import hashlib
import re
import random
import string
import logging

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


# Predefined PII patterns
PII_PATTERNS = {
    "email": [
        r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    ],
    "phone": [
        r"\+?1?\d{9,15}",
        r"\+?86?\d{10,11}",
        r"\d{3}-\d{3,4}-\d{4}",
    ],
    "ssn": [
        r"\d{3}-\d{2}-\d{4}",
        r"\d{9}",
    ],
    "credit_card": [
        r"\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}",
    ],
    "ip_address": [
        r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}",
    ],
    "date_of_birth": [
        r"\d{4}-\d{2}-\d{2}",
        r"\d{2}/\d{2}/\d{4}",
    ],
    "passport": [
        r"[A-Z]{1,2}\d{6,9}",
    ],
    "driver_license": [
        r"[A-Z]\d{7,8}",
    ],
}


class PIIDetectorAction(BaseAction):
    """Detect personally identifiable information in text."""
    
    action_type = "pii_detector"
    display_name = "PII检测器"
    description = "检测文本中的个人身份信息"
    
    def __init__(self) -> None:
        super().__init__()
        self._compiled_patterns: Dict[str, List[re.Pattern]] = {}
        self._initialize_patterns()
    
    def _initialize_patterns(self) -> None:
        """Compile all PII patterns."""
        for pii_type, patterns in PII_PATTERNS.items():
            self._compiled_patterns[pii_type] = [
                re.compile(p) for p in patterns
            ]
    
    def register_pattern(self, pii_type: str, patterns: List[str]) -> None:
        """Register custom PII patterns."""
        self._compiled_patterns[pii_type] = [
            re.compile(p) for p in patterns
        ]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Detect PII in text or data.
        
        Args:
            params: {
                "text": Text to scan (str),
                "data": Structured data to scan (dict),
                "pii_types": Types to detect (list, default all),
                "include_matches": Include actual matches (bool, default True),
                "redact": Replace matches with placeholder (bool, default False)
            }
        """
        try:
            text = params.get("text", "")
            data = params.get("data", {})
            pii_types = params.get("pii_types", list(self._compiled_patterns.keys()))
            include_matches = params.get("include_matches", True)
            redact = params.get("redact", False)
            
            detections: Dict[str, List[Dict[str, Any]]] = {}
            
            # Scan text
            if text:
                for pii_type in pii_types:
                    if pii_type not in self._compiled_patterns:
                        continue
                    
                    type_detections = []
                    for pattern in self._compiled_patterns[pii_type]:
                        for match in pattern.finditer(text):
                            detection = {
                                "type": pii_type,
                                "value": match.group() if include_matches else None,
                                "start": match.start(),
                                "end": match.end(),
                                "context": text[max(0, match.start() - 10):min(len(text), match.end() + 10)]
                            }
                            type_detections.append(detection)
                    
                    if type_detections:
                        detections[pii_type] = type_detections
            
            # Scan data recursively
            if data:
                flat_text = self._flatten_dict(data)
                for pii_type in pii_types:
                    if pii_type not in self._compiled_patterns:
                        continue
                    
                    if pii_type not in detections:
                        detections[pii_type] = []
                    
                    for pattern in self._compiled_patterns[pii_type]:
                        for key, value in flat_text.items():
                            if isinstance(value, str):
                                for match in pattern.finditer(value):
                                    detections[pii_type].append({
                                        "type": pii_type,
                                        "field": key,
                                        "value": match.group() if include_matches else None,
                                        "start": match.start(),
                                        "end": match.end()
                                    })
            
            total_detections = sum(len(v) for v in detections.values())
            
            result_data: Dict[str, Any] = {
                "total_detections": total_detections,
                "detections_by_type": {k: len(v) for k, v in detections.items()}
            }
            
            if redact:
                # Redact all detections
                redacted_text = text
                if text:
                    for pii_type, matches in detections.items():
                        for match in matches:
                            if match.get("value"):
                                redacted_text = redacted_text.replace(
                                    match["value"],
                                    f"[{pii_type.upper()}_REDACTED]"
                                )
                result_data["redacted_text"] = redacted_text
            
            return ActionResult(
                success=True,
                message=f"Detected {total_detections} PII instances across {len(detections)} types",
                data=result_data
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"PII detection error: {str(e)}")
    
    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = "") -> Dict[str, Any]:
        """Flatten nested dict to dotted keys."""
        items: Dict[str, Any] = {}
        for k, v in d.items():
            new_key = f"{parent_key}.{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(self._flatten_dict(v, new_key))
            else:
                items[new_key] = v
        return items


class DataMaskerAction(BaseAction):
    """Mask sensitive data while preserving format."""
    
    action_type = "data_masker"
    display_name = "数据掩码器"
    description = "掩码敏感数据同时保留格式"
    
    MASK_STRATEGIES = {
        "full": lambda v, c="*": c * len(str(v)),
        "partial": lambda v, c="*", keep=4: str(v)[:-keep] if len(str(v)) > keep else c * len(str(v)),
        "email": lambda v: str(v).split("@")[0][:2] + "***@" + str(v).split("@")[1] if "@" in str(v) else "***",
        "phone": lambda v: "***" + str(v)[-4:] if len(str(v)) >= 4 else "****",
        "card": lambda v: "****-****-****-" + str(v)[-4:] if len(str(v)) >= 4 else "************",
        "name": lambda v: str(v)[0] + "*" * (len(str(v)) - 1) if str(v) else "",
    }
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Mask sensitive data.
        
        Args:
            params: {
                "data": Data to mask (dict or str),
                "mask_rules": Field -> mask strategy mapping (dict),
                "default_strategy": Default mask strategy (str, default "partial"),
                "preserve_keys": Keys that should not be masked (list)
            }
        """
        try:
            data = params.get("data", {})
            mask_rules = params.get("mask_rules", {})
            default_strategy = params.get("default_strategy", "partial")
            preserve_keys = params.get("preserve_keys", [])
            
            if not data:
                return ActionResult(success=False, message="data parameter required")
            
            masked_data = self._mask_data(
                data,
                mask_rules,
                default_strategy,
                preserve_keys
            )
            
            return ActionResult(
                success=True,
                message="Data masked successfully",
                data={"masked_data": masked_data}
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Masking error: {str(e)}")
    
    def _mask_data(
        self,
        data: Any,
        mask_rules: Dict[str, str],
        default_strategy: str,
        preserve_keys: List[str]
    ) -> Any:
        """Recursively mask data."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if key in preserve_keys:
                    result[key] = value
                elif key in mask_rules:
                    result[key] = self._apply_mask(value, mask_rules[key])
                else:
                    result[key] = self._mask_data(value, mask_rules, default_strategy, preserve_keys)
            return result
        
        elif isinstance(data, list):
            return [self._mask_data(item, mask_rules, default_strategy, preserve_keys) for item in data]
        
        else:
            return self._apply_mask(data, default_strategy)
    
    def _apply_mask(self, value: Any, strategy: str) -> Any:
        """Apply mask strategy to a value."""
        if strategy in self.MASK_STRATEGIES:
            return self.MASK_STRATEGIES[strategy](value)
        return str(value)


class DataAnonymizerAction(BaseAction):
    """Anonymize data using various techniques."""
    
    action_type = "data_anonymizer"
    display_name = "数据匿名化"
    description = "使用多种技术匿名化数据"
    
    def __init__(self) -> None:
        super().__init__()
        self._hash_salts: Dict[str, str] = {}
    
    def set_salt(self, purpose: str, salt: str) -> None:
        """Set salt for hashing."""
        self._hash_salts[purpose] = salt
    
    def generate_salt(self, length: int = 32) -> str:
        """Generate a random salt."""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Anonymize data using specified techniques.
        
        Args:
            params: {
                "data": Data to anonymize (dict or str),
                "technique": "hash", "redact", "generalize", "pseudonymize" (str),
                "fields": Fields to anonymize (list),
                "salt": Salt for hashing (str),
                "preserve_fields": Fields to keep unchanged (list)
            }
        """
        try:
            data = params.get("data", {})
            technique = params.get("technique", "hash")
            fields = params.get("fields", [])
            salt = params.get("salt", "")
            preserve_fields = params.get("preserve_fields", [])
            
            if not data:
                return ActionResult(success=False, message="data parameter required")
            
            if technique == "hash":
                result = self._hash_anonymize(data, fields, salt, preserve_fields)
            elif technique == "redact":
                result = self._redact_anonymize(data, fields, preserve_fields)
            elif technique == "generalize":
                result = self._generalize_anonymize(data, fields, preserve_fields)
            elif technique == "pseudonymize":
                result = self._pseudonymize(data, fields, salt, preserve_fields)
            else:
                return ActionResult(success=False, message=f"Unknown technique: {technique}")
            
            return ActionResult(
                success=True,
                message=f"Anonymized using {technique}",
                data={"anonymized_data": result}
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Anonymization error: {str(e)}")
    
    def _hash_anonymize(
        self,
        data: Any,
        fields: List[str],
        salt: str,
        preserve_fields: List[str]
    ) -> Any:
        """Hash specified fields."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if key in preserve_fields:
                    result[key] = value
                elif key in fields:
                    result[key] = self._hash_value(value, salt)
                else:
                    result[key] = self._hash_anonymize(value, fields, salt, preserve_fields)
            return result
        elif isinstance(data, list):
            return [self._hash_anonymize(item, fields, salt, preserve_fields) for item in data]
        elif isinstance(data, str) and fields == ["*"]:
            return self._hash_value(data, salt)
        return data
    
    def _hash_value(self, value: Any, salt: str) -> str:
        """Hash a value with salt."""
        data = f"{salt}:{value}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]
    
    def _redact_anonymize(
        self,
        data: Any,
        fields: List[str],
        preserve_fields: List[str]
    ) -> Any:
        """Redact specified fields."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if key in preserve_fields:
                    result[key] = value
                elif key in fields:
                    result[key] = "[REDACTED]"
                else:
                    result[key] = self._redact_anonymize(value, fields, preserve_fields)
            return result
        elif isinstance(data, list):
            return [self._redact_anonymize(item, fields, preserve_fields) for item in data]
        elif isinstance(data, str) and fields == ["*"]:
            return "[REDACTED]"
        return data
    
    def _generalize_anonymize(
        self,
        data: Any,
        fields: List[str],
        preserve_fields: List[str]
    ) -> Any:
        """Generalize data (e.g., age -> age range)."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if key in preserve_fields:
                    result[key] = value
                elif key in fields:
                    result[key] = self._generalize_value(value)
                else:
                    result[key] = self._generalize_anonymize(value, fields, preserve_fields)
            return result
        elif isinstance(data, list):
            return [self._generalize_anonymize(item, fields, preserve_fields) for item in data]
        elif isinstance(data, str) and fields == ["*"]:
            return self._generalize_value(data)
        return data
    
    def _generalize_value(self, value: Any) -> Any:
        """Generalize a value."""
        if isinstance(value, (int, float)):
            # Age ranges
            if 0 <= value <= 120:
                age = int(value)
                if age < 18:
                    return "minor"
                elif age < 30:
                    return "20s"
                elif age < 40:
                    return "30s"
                elif age < 50:
                    return "40s"
                elif age < 60:
                    return "50s"
                else:
                    return "60+"
            return value
        elif isinstance(value, str):
            # Truncate to first 3 chars + ***
            if len(value) > 3:
                return value[:3] + "***"
            return "***"
        return value
    
    def _pseudonymize(
        self,
        data: Any,
        fields: List[str],
        salt: str,
        preserve_fields: List[str]
    ) -> Any:
        """Replace with consistent pseudonyms."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if key in preserve_fields:
                    result[key] = value
                elif key in fields:
                    result[key] = self._generate_pseudonym(value, salt)
                else:
                    result[key] = self._pseudonymize(value, fields, salt, preserve_fields)
            return result
        elif isinstance(data, list):
            return [self._pseudonymize(item, fields, salt, preserve_fields) for item in data]
        elif isinstance(data, str) and fields == ["*"]:
            return self._generate_pseudonym(data, salt)
        return data
    
    def _generate_pseudonym(self, value: Any, salt: str) -> str:
        """Generate a consistent pseudonym for a value."""
        hash_input = f"{salt}:{value}"
        hash_bytes = hashlib.sha256(hash_input.encode()).digest()
        return f"PSEUDO_{hash_bytes[:8].hex()}"


class FieldAnonymizerAction(BaseAction):
    """Anonymize structured data with field-level control."""
    
    action_type = "field_anonymizer"
    display_name = "字段级匿名化"
    description = "精细化控制字段级别的匿名化"
    
    def __init__(self) -> None:
        super().__init__()
        self._field_configs: Dict[str, Dict[str, Any]] = {}
    
    def configure_field(
        self,
        field_name: str,
        anonymizer: str,
        params: Optional[Dict[str, Any]] = None
    ) -> None:
        """Configure anonymization for a specific field."""
        self._field_configs[field_name] = {
            "anonymizer": anonymizer,
            "params": params or {}
        }
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Anonymize data with field-level configuration.
        
        Args:
            params: {
                "data": Structured data to anonymize (dict),
                "field_configs": Override field configs (dict),
                "default_anonymizer": Default anonymizer to use (str, default "hash")
            }
        """
        try:
            data = params.get("data", {})
            field_configs = params.get("field_configs", {})
            default_anonymizer = params.get("default_anonymizer", "hash")
            
            if not data:
                return ActionResult(success=False, message="data parameter required")
            
            # Merge configs
            configs = {**self._field_configs, **field_configs}
            
            anonymizer = DataAnonymizerAction()
            
            result = self._anonymize_recursive(
                data,
                configs,
                default_anonymizer
            )
            
            return ActionResult(
                success=True,
                message=f"Anonymized {len(configs)} configured fields",
                data={"anonymized_data": result, "fields_configured": list(configs.keys())}
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Field anonymization error: {str(e)}")
    
    def _anonymize_recursive(
        self,
        data: Any,
        configs: Dict[str, Dict[str, Any]],
        default: str
    ) -> Any:
        """Recursively anonymize data based on configs."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if key in configs:
                    cfg = configs[key]
                    result[key] = self._apply_field_anonymizer(
                        value,
                        cfg["anonymizer"],
                        cfg.get("params", {})
                    )
                else:
                    result[key] = self._anonymize_recursive(value, configs, default)
            return result
        elif isinstance(data, list):
            return [self._anonymize_recursive(item, configs, default) for item in data]
        return data
    
    def _apply_field_anonymizer(
        self,
        value: Any,
        anonymizer: str,
        params: Dict[str, Any]
    ) -> Any:
        """Apply anonymizer to a field value."""
        if anonymizer == "hash":
            salt = params.get("salt", "default_salt")
            return hashlib.sha256(f"{salt}:{value}".encode()).hexdigest()[:16]
        elif anonymizer == "redact":
            return "[REDACTED]"
        elif anonymizer == "zero":
            return 0
        elif anonymizer == "empty":
            return ""
        elif anonymizer == "null":
            return None
        return str(value)


class DataSwapAction(BaseAction):
    """Swap sensitive data with synthetic alternatives."""
    
    action_type = "data_swap"
    display_name = "数据替换"
    description = "用合成数据替换敏感数据"
    
    SYNTHETIC_GENERATORS = {
        "name": lambda: random.choice(["张三", "李四", "王五", "赵六", "孙七"]),
        "email": lambda: f"user{random.randint(1000, 9999)}@example.com",
        "phone": lambda: f"138{random.randint(10000000, 99999999)}",
        "address": lambda: random.choice([
            "北京市朝阳区建国路1号",
            "上海市浦东新区世纪大道100号",
            "广州市天河区天河路100号"
        ]),
        "company": lambda: random.choice([
            "示例科技有限公司",
            "测试实业集团",
            "模拟贸易有限公司"
        ]),
        "id": lambda: str(random.randint(100000, 999999)),
    }
    
    def __init__(self) -> None:
        super().__init__()
        self._swap_map: Dict[str, Any] = {}  # original -> synthetic
        self._reverse_map: Dict[str, Any] = {}  # synthetic -> original
    
    def register_synthetic_generator(
        self,
        field_type: str,
        generator: Callable[[], Any]
    ) -> None:
        """Register a custom synthetic data generator."""
        self.SYNTHETIC_GENERATORS[field_type] = generator
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Swap sensitive data with synthetic data.
        
        Args:
            params: {
                "data": Data to process (dict or list),
                "swap_rules": Field -> synthetic type mapping (dict),
                "consistent": Use consistent mapping (bool, default True),
                "reverse_map": Include reverse mapping in result (bool, default False)
            }
        """
        try:
            data = params.get("data", {})
            swap_rules = params.get("swap_rules", {})
            consistent = params.get("consistent", True)
            include_reverse = params.get("reverse_map", False)
            
            if not data:
                return ActionResult(success=False, message="data parameter required")
            
            self._current_swap_map = self._swap_map if consistent else {}
            swapped_data = self._swap_recursive(data, swap_rules)
            
            return ActionResult(
                success=True,
                message="Data swapped with synthetic values",
                data={
                    "swapped_data": swapped_data,
                    "swap_count": len(self._current_swap_map),
                    "reverse_map": self._current_swap_map if include_reverse else None
                }
            )
        
        except Exception as e:
            return ActionResult(success=False, message=f"Data swap error: {str(e)}")
    
    def _swap_recursive(self, data: Any, swap_rules: Dict[str, str]) -> Any:
        """Recursively swap data values."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if key in swap_rules:
                    result[key] = self._get_synthetic(value, swap_rules[key])
                else:
                    result[key] = self._swap_recursive(value, swap_rules)
            return result
        elif isinstance(data, list):
            return [self._swap_recursive(item, swap_rules) for item in data]
        return data
    
    def _get_synthetic(self, original: Any, synthetic_type: str) -> Any:
        """Get or generate synthetic data for original value."""
        if original in self._current_swap_map:
            return self._current_swap_map[original]
        
        if synthetic_type in self.SYNTHETIC_GENERATORS:
            synthetic = self.SYNTHETIC_GENERATORS[synthetic_type]()
        else:
            synthetic = f"SYNTHETIC_{original}"
        
        self._current_swap_map[original] = synthetic
        return synthetic

"""YAML action module for RabAI AutoClick.

Provides YAML parsing, serialization, validation, and transformation
operations with support for custom types and anchors.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class YAMLConfig:
    """YAML processing configuration."""
    indent: int = 2
    width: int = 80
    allow_unicode: bool = True
    default_flow_style: bool = False
    sort_keys: bool = False
    safe_load: bool = True


class YAMLAction(BaseAction):
    """Action for YAML operations.
    
    Features:
        - Parse YAML from string or file
        - Serialize Python objects to YAML
        - Validate YAML syntax
        - Merge multiple YAML documents
        - Transform YAML structures
        - Handle custom Python types
        - Support for YAML anchors and aliases
    """
    
    def __init__(self, config: Optional[YAMLConfig] = None):
        """Initialize YAML action.
        
        Args:
            config: YAML configuration for serialization.
        """
        super().__init__()
        self.config = config or YAMLConfig()
        self._yaml_available = self._check_yaml_library()
    
    def _check_yaml_library(self) -> bool:
        """Check if PyYAML is available."""
        try:
            import yaml
            return True
        except ImportError:
            return False
    
    def _get_yaml_lib(self):
        """Get YAML library (PyYAML or built-in json fallback)."""
        if self._yaml_available:
            import yaml
            return yaml
        else:
            import json
            return None
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute YAML operation.
        
        Args:
            params: Dictionary containing:
                - operation: Operation to perform (parse, serialize, validate, 
                           merge, transform, diff)
                - content: YAML content string (for parse/validate)
                - file_path: Path to YAML file
                - data: Python object (for serialize)
                - output_path: Path to save result
        
        Returns:
            ActionResult with operation result
        """
        try:
            operation = params.get("operation", "")
            
            if operation == "parse":
                return self._parse(params)
            elif operation == "serialize":
                return self._serialize(params)
            elif operation == "validate":
                return self._validate(params)
            elif operation == "merge":
                return self._merge(params)
            elif operation == "transform":
                return self._transform(params)
            elif operation == "diff":
                return self._diff(params)
            elif operation == "extract":
                return self._extract(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
                
        except Exception as e:
            return ActionResult(success=False, message=f"YAML operation failed: {str(e)}")
    
    def _parse(self, params: Dict[str, Any]) -> ActionResult:
        """Parse YAML content to Python object."""
        yaml = self._get_yaml_lib()
        if yaml is None:
            return ActionResult(
                success=False, 
                message="PyYAML not available. Install with: pip install pyyaml"
            )
        
        content = params.get("content", "")
        file_path = params.get("file_path", "")
        
        if file_path:
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
            except yaml.YAMLError as e:
                return ActionResult(success=False, message=f"YAML parse error: {str(e)}")
        elif content:
            try:
                data = yaml.safe_load(content) or {}
            except yaml.YAMLError as e:
                return ActionResult(success=False, message=f"YAML parse error: {str(e)}")
        else:
            return ActionResult(success=False, message="content or file_path required")
        
        data_type = type(data).__name__
        return ActionResult(
            success=True,
            message=f"Parsed YAML to {data_type}",
            data={"data": data, "type": data_type}
        )
    
    def _serialize(self, params: Dict[str, Any]) -> ActionResult:
        """Serialize Python object to YAML string."""
        yaml = self._get_yaml_lib()
        if yaml is None:
            return ActionResult(
                success=False, 
                message="PyYAML not available. Install with: pip install pyyaml"
            )
        
        data = params.get("data", {})
        output_path = params.get("output_path", "")
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        try:
            yaml_content = yaml.dump(
                data,
                indent=self.config.indent,
                width=self.config.width,
                allow_unicode=self.config.allow_unicode,
                default_flow_style=self.config.default_flow_style,
                sort_keys=self.config.sort_keys
            )
            
            if output_path:
                os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(yaml_content)
                return ActionResult(
                    success=True,
                    message=f"Serialized to {output_path}",
                    data={"path": output_path, "size": len(yaml_content)}
                )
            
            return ActionResult(
                success=True,
                message="Serialized to YAML string",
                data={"yaml": yaml_content}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Serialization error: {str(e)}")
    
    def _validate(self, params: Dict[str, Any]) -> ActionResult:
        """Validate YAML syntax."""
        yaml = self._get_yaml_lib()
        if yaml is None:
            return ActionResult(
                success=False, 
                message="PyYAML not available. Install with: pip install pyyaml"
            )
        
        content = params.get("content", "")
        file_path = params.get("file_path", "")
        
        if file_path:
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    yaml.safe_load(f)
                return ActionResult(
                    success=True,
                    message="YAML is valid",
                    data={"path": file_path, "valid": True}
                )
            except yaml.YAMLError as e:
                return ActionResult(
                    success=False,
                    message=f"YAML is invalid: {str(e)}",
                    data={"path": file_path, "valid": False, "error": str(e)}
                )
        elif content:
            try:
                yaml.safe_load(content)
                return ActionResult(
                    success=True,
                    message="YAML is valid",
                    data={"valid": True}
                )
            except yaml.YAMLError as e:
                return ActionResult(
                    success=False,
                    message=f"YAML is invalid: {str(e)}",
                    data={"valid": False, "error": str(e)}
                )
        else:
            return ActionResult(success=False, message="content or file_path required")
    
    def _merge(self, params: Dict[str, Any]) -> ActionResult:
        """Merge multiple YAML documents."""
        yaml = self._get_yaml_lib()
        if yaml is None:
            return ActionResult(
                success=False, 
                message="PyYAML not available. Install with: pip install pyyaml"
            )
        
        documents = params.get("documents", [])
        base = params.get("base", {})
        strategy = params.get("strategy", "deep")  # deep or shallow
        
        if not documents and not base:
            return ActionResult(success=False, message="documents or base required")
        
        try:
            if strategy == "deep":
                merged = self._deep_merge(base.copy() if base else {}, documents)
            else:
                merged = {**base, **base}
                for doc in documents:
                    merged.update(doc)
            
            return ActionResult(
                success=True,
                message=f"Merged {len(documents) + (1 if base else 0)} documents",
                data={"merged": merged}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Merge error: {str(e)}")
    
    def _deep_merge(self, base: Dict, documents: List) -> Dict:
        """Deep merge multiple dictionaries."""
        result = base.copy()
        
        for doc in documents:
            if isinstance(doc, dict):
                for key, value in doc.items():
                    if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                        result[key] = self._deep_merge(result[key], [value])
                    else:
                        result[key] = value
            else:
                result = doc if not result else result
        
        return result
    
    def _transform(self, params: Dict[str, Any]) -> ActionResult:
        """Transform YAML structure using mapping rules."""
        yaml = self._get_yaml_lib()
        if yaml is None:
            return ActionResult(
                success=False, 
                message="PyYAML not available. Install with: pip install pyyaml"
            )
        
        content = params.get("content", "")
        file_path = params.get("file_path", "")
        mappings = params.get("mappings", {})  # {"old_key": "new_key"}
        operations = params.get("operations", [])  # list of transform ops
        
        if file_path:
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
        
        if not content:
            return ActionResult(success=False, message="content or file_path required")
        
        try:
            data = yaml.safe_load(content)
            
            if mappings:
                data = self._apply_mappings(data, mappings)
            
            if operations:
                data = self._apply_operations(data, operations)
            
            yaml_output = yaml.dump(
                data,
                indent=self.config.indent,
                allow_unicode=self.config.allow_unicode,
                default_flow_style=self.config.default_flow_style
            )
            
            return ActionResult(
                success=True,
                message="Transformation complete",
                data={"transformed": data, "yaml": yaml_output}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform error: {str(e)}")
    
    def _apply_mappings(self, data: Any, mappings: Dict[str, str]) -> Any:
        """Apply key renaming mappings."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                new_key = mappings.get(key, key)
                result[new_key] = self._apply_mappings(value, mappings)
            return result
        elif isinstance(data, list):
            return [self._apply_mappings(item, mappings) for item in data]
        else:
            return data
    
    def _apply_operations(self, data: Any, operations: List[Dict]) -> Any:
        """Apply transformation operations."""
        result = data
        
        for op in operations:
            op_type = op.get("type", "")
            
            if op_type == "filter_keys":
                keys = op.get("keys", [])
                keep = op.get("keep", True)
                if isinstance(result, dict):
                    if keep:
                        result = {k: v for k, v in result.items() if k in keys}
                    else:
                        result = {k: v for k, v in result.items() if k not in keys}
            
            elif op_type == "map_values":
                key = op.get("key", "")
                func = op.get("func", "upper")
                if isinstance(result, dict) and key in result:
                    if func == "upper":
                        result[key] = str(result[key]).upper()
                    elif func == "lower":
                        result[key] = str(result[key]).lower()
                    elif func == "int":
                        result[key] = int(result[key])
                    elif func == "float":
                        result[key] = float(result[key])
            
            elif op_type == "flatten":
                if isinstance(result, dict):
                    result = {str(k): v for k, v in result.items()}
            
            elif op_type == "unflatten":
                pass  # Implementation depends on structure
        
        return result
    
    def _diff(self, params: Dict[str, Any]) -> ActionResult:
        """Compare two YAML structures and report differences."""
        yaml = self._get_yaml_lib()
        if yaml is None:
            return ActionResult(
                success=False, 
                message="PyYAML not available. Install with: pip install pyyaml"
            )
        
        content1 = params.get("content1", "")
        content2 = params.get("content2", "")
        file1 = params.get("file1", "")
        file2 = params.get("file2", "")
        
        if file1:
            with open(file1, "r", encoding="utf-8") as f:
                data1 = yaml.safe_load(f) or {}
        elif content1:
            data1 = yaml.safe_load(content1) or {}
        else:
            return ActionResult(success=False, message="content1 or file1 required")
        
        if file2:
            with open(file2, "r", encoding="utf-8") as f:
                data2 = yaml.safe_load(f) or {}
        elif content2:
            data2 = yaml.safe_load(content2) or {}
        else:
            return ActionResult(success=False, message="content2 or file2 required")
        
        diff = self._compute_diff(data1, data2)
        
        return ActionResult(
            success=True,
            message=f"Found {len(diff)} differences",
            data={"diff": diff, "count": len(diff)}
        )
    
    def _compute_diff(self, obj1: Any, obj2: Any, path: str = "") -> List[Dict]:
        """Recursively compute differences between two objects."""
        differences = []
        
        if type(obj1) != type(obj2):
            differences.append({
                "path": path or "root",
                "type": "type_mismatch",
                "from": f"{type(obj1).__name__}",
                "to": f"{type(obj2).__name__}"
            })
            return differences
        
        if isinstance(obj1, dict):
            all_keys = set(obj1.keys()) | set(obj2.keys())
            for key in all_keys:
                new_path = f"{path}.{key}" if path else key
                if key not in obj1:
                    differences.append({
                        "path": new_path,
                        "type": "added",
                        "value": obj2[key]
                    })
                elif key not in obj2:
                    differences.append({
                        "path": new_path,
                        "type": "removed",
                        "value": obj1[key]
                    })
                else:
                    differences.extend(self._compute_diff(obj1[key], obj2[key], new_path))
        
        elif isinstance(obj1, list):
            if len(obj1) != len(obj2):
                differences.append({
                    "path": path,
                    "type": "length_mismatch",
                    "from": len(obj1),
                    "to": len(obj2)
                })
            else:
                for i, (v1, v2) in enumerate(zip(obj1, obj2)):
                    differences.extend(self._compute_diff(v1, v2, f"{path}[{i}]"))
        
        else:
            if obj1 != obj2:
                differences.append({
                    "path": path,
                    "type": "value_changed",
                    "from": obj1,
                    "to": obj2
                })
        
        return differences
    
    def _extract(self, params: Dict[str, Any]) -> ActionResult:
        """Extract specific values from YAML using paths."""
        yaml = self._get_yaml_lib()
        if yaml is None:
            return ActionResult(
                success=False, 
                message="PyYAML not available. Install with: pip install pyyaml"
            )
        
        content = params.get("content", "")
        file_path = params.get("file_path", "")
        path = params.get("path", "")  # e.g., "database.host"
        
        if file_path:
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
        
        if not content:
            return ActionResult(success=False, message="content or file_path required")
        
        if not path:
            return ActionResult(success=False, message="path is required for extraction")
        
        try:
            data = yaml.safe_load(content)
            value = self._navigate_path(data, path)
            
            return ActionResult(
                success=True,
                message=f"Extracted value at {path}",
                data={"path": path, "value": value, "found": value is not None}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Extract error: {str(e)}")
    
    def _navigate_path(self, data: Any, path: str) -> Any:
        """Navigate into nested structure using dot notation."""
        parts = path.split(".")
        current = data
        
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    idx = int(part)
                    current = current[idx] if 0 <= idx < len(current) else None
                except ValueError:
                    return None
            else:
                return None
            
            if current is None:
                return None
        
        return current

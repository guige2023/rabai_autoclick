"""Extended YAML processing action module for RabAI AutoClick.

Provides YAML operations:
- YamlReadAction: Read YAML file
- YamlWriteAction: Write YAML file
- YamlMergeAction: Merge YAML documents
- YamlValidateAction: Validate YAML syntax
- YamlDiffAction: Compare two YAML documents
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class YamlReadAction(BaseAction):
    """Read YAML file."""
    action_type = "yaml2_read"
    display_name = "YAML读取"
    description = "读取YAML文件"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute YAML read."""
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'yaml_data')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import yaml

            resolved_path = context.resolve_value(file_path) if context else file_path

            with open(resolved_path, 'r') as f:
                data = yaml.safe_load(f)

            if data is None:
                data = {}

            result = {'data': data, 'keys': list(data.keys()) if isinstance(data, dict) else []}
            if context:
                context.set(output_var, data)
            return ActionResult(success=True, message=f"Read YAML: {len(result['keys'])} keys", data=result)
        except ImportError:
            return ActionResult(success=False, message="PyYAML not installed. Run: pip install PyYAML")
        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_path}")
        except yaml.YAMLError as e:
            return ActionResult(success=False, message=f"YAML parse error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"YAML read error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'yaml_data'}


class YamlWriteAction(BaseAction):
    """Write YAML file."""
    action_type = "yaml2_write"
    display_name = "YAML写入"
    description = "写入YAML文件"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute YAML write."""
        file_path = params.get('file_path', '')
        data = params.get('data', {})
        default_flow_style = params.get('default_flow_style', False)
        output_var = params.get('output_var', 'yaml_write_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import yaml

            resolved_path = context.resolve_value(file_path) if context else file_path
            resolved_data = context.resolve_value(data) if context else data
            resolved_style = context.resolve_value(default_flow_style) if context else default_flow_style

            _os.makedirs(_os.path.dirname(resolved_path) or '.', exist_ok=True)

            with open(resolved_path, 'w') as f:
                yaml.dump(resolved_data, f, default_flow_style=resolved_style, allow_unicode=True, sort_keys=False)

            result = {'written': True, 'path': resolved_path}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Wrote YAML to {resolved_path}", data=result)
        except ImportError:
            return ActionResult(success=False, message="PyYAML not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"YAML write error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default_flow_style': False, 'output_var': 'yaml_write_result'}


class YamlMergeAction(BaseAction):
    """Merge YAML documents."""
    action_type = "yaml2_merge"
    display_name = "YAML合并"
    description = "合并YAML文档"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute YAML merge."""
        yaml_docs = params.get('yaml_docs', [])  # list of dicts or YAML strings
        output_var = params.get('output_var', 'yaml_merged')

        if not yaml_docs:
            return ActionResult(success=False, message="yaml_docs is required")

        try:
            import yaml
            import copy

            resolved_docs = context.resolve_value(yaml_docs) if context else yaml_docs

            merged = {}
            for doc in resolved_docs:
                if isinstance(doc, str):
                    doc = yaml.safe_load(doc)
                if isinstance(doc, dict):
                    merged.update(copy.deepcopy(doc))

            result = {'merged': merged, 'keys': list(merged.keys())}
            if context:
                context.set(output_var, merged)
            return ActionResult(success=True, message=f"Merged {len(resolved_docs)} YAML docs", data=result)
        except ImportError:
            return ActionResult(success=False, message="PyYAML not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"YAML merge error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['yaml_docs']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'yaml_merged'}


class YamlValidateAction(BaseAction):
    """Validate YAML syntax."""
    action_type = "yaml2_validate"
    display_name = "YAML验证"
    description = "验证YAML语法"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute YAML validate."""
        yaml_str = params.get('yaml', '')
        yaml_file = params.get('yaml_file', None)
        output_var = params.get('output_var', 'yaml_valid')

        if not yaml_str and not yaml_file:
            return ActionResult(success=False, message="yaml or yaml_file is required")

        try:
            import yaml

            resolved_str = context.resolve_value(yaml_str) if context else yaml_str
            resolved_file = context.resolve_value(yaml_file) if context else yaml_file

            if resolved_file:
                with open(resolved_file, 'r') as f:
                    yaml.safe_load(f)
            else:
                yaml.safe_load(resolved_str)

            result = {'valid': True}
            if context:
                context.set(output_var, True)
            return ActionResult(success=True, message="YAML is valid")
        except ImportError:
            return ActionResult(success=False, message="PyYAML not installed")
        except yaml.YAMLError as e:
            result = {'valid': False, 'error': str(e)}
            if context:
                context.set(output_var, False)
            return ActionResult(success=False, message=f"YAML invalid: {str(e)}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"YAML validate error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'yaml': '', 'yaml_file': None, 'output_var': 'yaml_valid'}


class YamlDiffAction(BaseAction):
    """Compare two YAML documents."""
    action_type = "yaml2_diff"
    display_name = "YAML对比"
    description = "对比YAML文档"
    version = "2.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute YAML diff."""
        yaml1 = params.get('yaml1', None)  # dict or string
        yaml2 = params.get('yaml2', None)  # dict or string
        output_var = params.get('output_var', 'yaml_diff')

        if yaml1 is None or yaml2 is None:
            return ActionResult(success=False, message="yaml1 and yaml2 are required")

        try:
            import yaml
            import copy

            resolved1 = context.resolve_value(yaml1) if context else yaml1
            resolved2 = context.resolve_value(yaml2) if context else yaml2

            if isinstance(resolved1, str):
                resolved1 = yaml.safe_load(resolved1) or {}
            if isinstance(resolved2, str):
                resolved2 = yaml.safe_load(resolved2) or {}

            def get_all_keys(d, prefix=''):
                keys = set()
                if isinstance(d, dict):
                    for k, v in d.items():
                        full_key = f"{prefix}.{k}" if prefix else k
                        keys.add(full_key)
                        keys.update(get_all_keys(v, full_key))
                elif isinstance(d, list):
                    for i, item in enumerate(d):
                        full_key = f"{prefix}[{i}]"
                        keys.add(full_key)
                        keys.update(get_all_keys(item, full_key))
                return keys

            keys1 = get_all_keys(resolved1)
            keys2 = get_all_keys(resolved2)

            added = sorted(keys2 - keys1)
            removed = sorted(keys1 - keys2)
            common = sorted(keys1 & keys2)

            diff_items = []
            for key in common:
                parts1 = resolved1
                parts2 = resolved2
                for p in key.replace(']', '').split('.'):
                    if '[' in p:
                        name, idx = p.split('[')
                        parts1 = parts1.get(name, [{}])[int(idx)]
                        parts2 = parts2.get(name, [{}])[int(idx)]
                    else:
                        parts1 = parts1.get(p, {})
                        parts2 = parts2.get(p, {})
                if parts1 != parts2:
                    diff_items.append({'key': key, 'old': parts1, 'new': parts2})

            result = {'added': added, 'removed': removed, 'changed': diff_items, 'unchanged': len(common) - len(diff_items)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"YAML diff: +{len(added)} -{len(removed)} ~{len(diff_items)}", data=result)
        except ImportError:
            return ActionResult(success=False, message="PyYAML not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"YAML diff error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['yaml1', 'yaml2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'yaml_diff'}

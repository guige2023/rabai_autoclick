"""Comprehensive workflow validation library for RabAI AutoClick.

Validates workflow JSON/YAML files for schema compliance, semantic correctness,
action references, variable definitions, type checking, circular dependencies,
and security issues.
"""

import json
import re
import yaml
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

try:
    import jsonschema
    JSONSCHEMA_AVAILABLE = True
except ImportError:
    JSONSCHEMA_AVAILABLE = False


class ValidationSeverity(Enum):
    """Severity levels for validation issues."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationIssue:
    """Represents a single validation issue."""
    severity: ValidationSeverity
    message: str
    location: Optional[str] = None
    code: Optional[str] = None
    context: Optional[Dict[str, Any]] = None


@dataclass
class ValidationResult:
    """Result of workflow validation."""
    is_valid: bool
    errors: List[ValidationIssue] = field(default_factory=list)
    warnings: List[ValidationIssue] = field(default_factory=list)
    info: List[ValidationIssue] = field(default_factory=list)

    def add_error(self, message: str, location: Optional[str] = None,
                  code: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        self.errors.append(ValidationIssue(
            ValidationSeverity.ERROR, message, location, code, context
        ))
        self.is_valid = False

    def add_warning(self, message: str, location: Optional[str] = None,
                    code: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        self.warnings.append(ValidationIssue(
            ValidationSeverity.WARNING, message, location, code, context
        ))

    def add_info(self, message: str, location: Optional[str] = None,
                 code: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        self.info.append(ValidationIssue(
            ValidationSeverity.INFO, message, location, code, context
        ))

    def get_all_issues(self) -> List[ValidationIssue]:
        """Get all issues sorted by severity."""
        return self.errors + self.warnings + self.info


# JSON Schema for workflow validation
WORKFLOW_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["name", "steps"],
    "properties": {
        "workflow_id": {"type": "string"},
        "name": {"type": "string", "minLength": 1},
        "description": {"type": "string"},
        "version": {"type": "string"},
        "steps": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "action"],
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "action": {"type": "string"},
                    "params": {"type": "object"},
                    "conditions": {
                        "type": "array",
                        "items": {"type": "object"}
                    },
                    "on_success": {"type": "string"},
                    "on_failure": {"type": "string"},
                    "retry": {
                        "type": "object",
                        "properties": {
                            "max_attempts": {"type": "integer", "minimum": 1},
                            "delay": {"type": "number", "minimum": 0}
                        }
                    }
                }
            }
        },
        "triggers": {
            "type": "array",
            "items": {"type": "object"}
        },
        "variables": {
            "type": "object"
        },
        "settings": {
            "type": "object"
        },
        "metadata": {
            "type": "object"
        }
    }
}


class WorkflowValidator:
    """Comprehensive workflow validator.

    Validates workflows against JSON schema, checks semantic correctness,
    verifies action references, validates variables, type checks parameters,
    detects circular dependencies, and scans for security issues.
    """

    def __init__(
        self,
        action_loader=None,
        schema: Optional[Dict[str, Any]] = None
    ):
        """Initialize the workflow validator.

        Args:
            action_loader: Optional ActionLoader instance for action validation.
            schema: Optional custom JSON schema for workflow validation.
        """
        self.action_loader = action_loader
        self.schema = schema or WORKFLOW_SCHEMA

    def validate(
        self,
        workflow: Union[Dict[str, Any], str],
        skip_security: bool = False
    ) -> ValidationResult:
        """Validate a workflow comprehensively.

        Args:
            workflow: Workflow dict or YAML/JSON string.
            skip_security: If True, skip security scanning.

        Returns:
            ValidationResult with all issues found.
        """
        result = ValidationResult(is_valid=True)

        # Parse workflow if string
        if isinstance(workflow, str):
            workflow = self._parse_workflow_string(workflow, result)
            if workflow is None:
                return result

        # Ensure workflow is a dict before proceeding
        if not isinstance(workflow, dict):
            result.add_error("Workflow must be a dict or valid YAML/JSON string", code="INVALID_WORKFLOW_TYPE")
            return result

        # 1. Schema validation
        self._validate_schema(workflow, result)

        # 2. Semantic validation
        self._validate_semantics(workflow, result)

        # 3. Action validation
        self._validate_actions(workflow, result)

        # 4. Variable validation
        self._validate_variables(workflow, result)

        # 5. Type checking
        self._validate_types(workflow, result)

        # 6. Circular reference detection
        self._detect_circular_references(workflow, result)

        # 7. Missing required params
        self._validate_required_params(workflow, result)

        # 8. Security scan
        if not skip_security:
            self._scan_security(workflow, result)

        return result

    def _parse_workflow_string(
        self,
        workflow_str: str,
        result: ValidationResult
    ) -> Optional[Dict[str, Any]]:
        """Parse workflow from YAML or JSON string."""
        # Try YAML first
        try:
            return yaml.safe_load(workflow_str)
        except yaml.YAMLError:
            pass

        # Try JSON
        try:
            return json.loads(workflow_str)
        except json.JSONDecodeError as e:
            result.add_error(
                f"Invalid workflow format: {str(e)}",
                code="PARSE_ERROR"
            )
            return None

    def _validate_schema(
        self,
        workflow: Dict[str, Any],
        result: ValidationResult
    ) -> None:
        """Validate workflow against JSON schema."""
        if not JSONSCHEMA_AVAILABLE:
            result.add_warning(
                "jsonschema package not available, skipping schema validation",
                code="SCHEMA_SKIP"
            )
            return

        try:
            jsonschema.validate(workflow, self.schema)
        except jsonschema.ValidationError as e:
            result.add_error(
                f"Schema validation failed: {e.message}",
                location=self._format_json_path(e.absolute_path),
                code="SCHEMA_ERROR",
                context={"validator": e.validator, "value": e.instance}
            )
        except jsonschema.SchemaError as e:
            result.add_error(
                f"Invalid schema: {str(e)}",
                code="SCHEMA_ERROR"
            )

    def _format_json_path(self, path: Tuple) -> str:
        """Format JSON path for display."""
        if not path:
            return "root"
        return ".".join(str(p) for p in path)

    def _validate_semantics(
        self,
        workflow: Dict[str, Any],
        result: ValidationResult
    ) -> None:
        """Check step IDs are unique and references exist."""
        steps = workflow.get("steps", [])
        if not isinstance(steps, list):
            return

        # Collect step IDs
        step_ids: Set[str] = set()
        id_to_index: Dict[str, int] = {}

        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                continue

            step_id = step.get("id")
            if step_id:
                if step_id in step_ids:
                    result.add_error(
                        f"Duplicate step ID: '{step_id}'",
                        location=f"steps[{i}]",
                        code="DUPLICATE_STEP_ID"
                    )
                else:
                    step_ids.add(step_id)
                    id_to_index[step_id] = i

        # Check on_success/on_failure references
        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                continue

            on_success = step.get("on_success")
            if on_success and on_success not in step_ids:
                result.add_error(
                    f"on_success references non-existent step: '{on_success}'",
                    location=f"steps[{i}].on_success",
                    code="INVALID_REFERENCE"
                )

            on_failure = step.get("on_failure")
            if on_failure and on_failure not in step_ids:
                result.add_error(
                    f"on_failure references non-existent step: '{on_failure}'",
                    location=f"steps[{i}].on_failure",
                    code="INVALID_REFERENCE"
                )

    def _validate_actions(
        self,
        workflow: Dict[str, Any],
        result: ValidationResult
    ) -> None:
        """Verify all referenced actions exist in ActionLoader."""
        if self.action_loader is None:
            result.add_warning(
                "No ActionLoader provided, skipping action validation",
                code="ACTION_LOADER_MISSING"
            )
            return

        steps = workflow.get("steps", [])
        available_actions = self.action_loader.get_all_actions()

        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                continue

            action_type = step.get("action")
            if not action_type:
                result.add_error(
                    "Step missing 'action' field",
                    location=f"steps[{i}]",
                    code="MISSING_ACTION"
                )
                continue

            if action_type not in available_actions:
                result.add_error(
                    f"Unknown action type: '{action_type}'",
                    location=f"steps[{i}].action",
                    code="UNKNOWN_ACTION",
                    context={"available_actions": list(available_actions.keys())}
                )

    def _validate_variables(
        self,
        workflow: Dict[str, Any],
        result: ValidationResult
    ) -> None:
        """Check all {{variable}} references are defined."""
        # Collect defined variables
        defined_vars: Set[str] = set()

        # From variables section
        variables = workflow.get("variables", {})
        if isinstance(variables, dict):
            defined_vars.update(variables.keys())

        # From params section
        params = workflow.get("params", {}) or workflow.get("parameters", {})
        if isinstance(params, dict):
            defined_vars.update(params.keys())

        # From step outputs (steps can produce variables)
        steps = workflow.get("steps", [])
        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                continue
            # Steps can produce output variables via 'output_var' or similar
            output_var = step.get("output_var")
            if output_var and isinstance(output_var, str):
                defined_vars.add(output_var)

        # Check variable references in all string values
        variable_pattern = re.compile(r'\{\{([^}]+)\}\}')

        def check_value(value: Any, path: str):
            if isinstance(value, str):
                for match in variable_pattern.finditer(value):
                    var_name = match.group(1).strip()
                    # Skip built-in functions
                    if var_name.startswith('$') or var_name.startswith('env:'):
                        continue
                    if var_name not in defined_vars:
                        result.add_warning(
                            f"Undefined variable reference: '{{{{{var_name}}}}}'",
                            location=path,
                            code="UNDEFINED_VARIABLE",
                            context={"defined_vars": list(defined_vars)}
                        )
            elif isinstance(value, dict):
                for k, v in value.items():
                    check_value(v, f"{path}.{k}" if path else k)
            elif isinstance(value, list):
                for idx, item in enumerate(value):
                    check_value(item, f"{path}[{idx}]")

        # Check all string fields
        check_value(workflow, "workflow")

    def _validate_types(
        self,
        workflow: Dict[str, Any],
        result: ValidationResult
    ) -> None:
        """Verify parameter types match action signatures."""
        if self.action_loader is None:
            return

        steps = workflow.get("steps", [])

        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                continue

            action_type = step.get("action")
            if not action_type:
                continue

            action_class = self.action_loader.get_action(action_type)
            if action_class is None:
                continue

            try:
                instance = action_class()
                params = step.get("params", {})

                # Check type mismatches
                required = instance.get_required_params()
                optional = instance.get_optional_params()

                for param_name, param_value in (params or {}).items():
                    expected_type = None

                    if param_name in required:
                        # Required param - infer type from class annotation or default
                        expected_type = self._infer_param_type(instance, param_name)
                    elif param_name in optional:
                        default = optional[param_name]
                        if default is not None:
                            expected_type = type(default)

                    if expected_type and param_value is not None:
                        if not isinstance(param_value, expected_type):
                            result.add_warning(
                                f"Parameter '{param_name}' type mismatch: "
                                f"expected {expected_type.__name__}, "
                                f"got {type(param_value).__name__}",
                                location=f"steps[{i}].params.{param_name}",
                                code="TYPE_MISMATCH"
                            )
            except Exception as e:
                result.add_warning(
                    f"Type checking failed for action '{action_type}': {str(e)}",
                    location=f"steps[{i}]",
                    code="TYPE_CHECK_ERROR"
                )

    def _infer_param_type(self, action_instance: Any, param_name: str) -> Optional[type]:
        """Infer parameter type from action's type annotations."""
        if hasattr(action_instance, '__annotations__'):
            annotations = action_instance.__annotations__
            if param_name in annotations:
                return annotations[param_name]
        return None

    def _detect_circular_references(
        self,
        workflow: Dict[str, Any],
        result: ValidationResult
    ) -> None:
        """Detect circular step dependencies."""
        steps = workflow.get("steps", [])
        if not isinstance(steps, list):
            return

        # Build adjacency list
        graph: Dict[str, Set[str]] = defaultdict(set)

        step_ids: Set[str] = set()
        for step in steps:
            if not isinstance(step, dict):
                continue
            step_id = step.get("id")
            if step_id:
                step_ids.add(step_id)

        for step in steps:
            if not isinstance(step, dict):
                continue
            step_id = step.get("id", "")
            if not step_id:
                continue

            # Direct next step
            next_step = step.get("next")
            if next_step and next_step in step_ids:
                graph[step_id].add(next_step)

            # on_success reference
            on_success = step.get("on_success")
            if on_success and on_success in step_ids:
                graph[step_id].add(on_success)

            # on_failure reference
            on_failure = step.get("on_failure")
            if on_failure and on_failure in step_ids:
                graph[step_id].add(on_failure)

        # DFS to detect cycles
        def find_cycle(node: str, path: List[str], visited: Set[str]) -> Optional[List[str]]:
            if node in path:
                cycle_start = path.index(node)
                return path[cycle_start:] + [node]

            if node in visited:
                return None

            visited.add(node)
            path.append(node)

            for neighbor in graph.get(node, set()):
                cycle = find_cycle(neighbor, path.copy(), visited)
                if cycle:
                    return cycle

            return None

        visited: Set[str] = set()
        for start in step_ids:
            if start not in visited:
                cycle = find_cycle(start, [], visited)
                if cycle:
                    result.add_error(
                        f"Circular dependency detected: {' -> '.join(cycle)}",
                        code="CIRCULAR_DEPENDENCY",
                        context={"cycle": cycle}
                    )
                    break  # Report one cycle at a time

    def _validate_required_params(
        self,
        workflow: Dict[str, Any],
        result: ValidationResult
    ) -> None:
        """Check all required params are present."""
        if self.action_loader is None:
            return

        steps = workflow.get("steps", [])

        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                continue

            action_type = step.get("action")
            if not action_type:
                continue

            action_class = self.action_loader.get_action(action_type)
            if action_class is None:
                continue

            try:
                instance = action_class()
                required_params = instance.get_required_params()
                params = step.get("params", {}) or {}

                for param in required_params:
                    if param not in params or params[param] is None:
                        result.add_error(
                            f"Missing required parameter '{param}' for action '{action_type}'",
                            location=f"steps[{i}].params",
                            code="MISSING_REQUIRED_PARAM"
                        )
            except Exception:
                pass  # Skip if we can't instantiate the action

    def _scan_security(
        self,
        workflow: Dict[str, Any],
        result: ValidationResult
    ) -> None:
        """Run security scan on workflow."""
        try:
            from .security_scan import SecurityScanner
            scanner = SecurityScanner()
            is_safe, issues = scanner.scan(workflow)

            if not is_safe:
                for issue in issues:
                    if issue.get("severity") in ("high", "critical"):
                        result.add_error(
                            issue.get("message", "Security issue detected"),
                            location=issue.get("location"),
                            code="SECURITY_ISSUE",
                            context=issue
                        )
                    else:
                        result.add_warning(
                            issue.get("message", "Security concern"),
                            location=issue.get("location"),
                            code="SECURITY_CONCERN",
                            context=issue
                        )
        except ImportError:
            result.add_warning(
                "Security scanner not available",
                code="SECURITY_SCAN_UNAVAILABLE"
            )

    # ==================== Roundtrip Validation ====================

    def validate_roundtrip(
        self,
        workflow: Union[Dict[str, Any], str],
        format: str = "yaml"
    ) -> Tuple[bool, Optional[str]]:
        """Validate YAML/JSON -> dict -> YAML/JSON preserves content.

        Args:
            workflow: Original workflow dict or string.
            format: 'yaml' or 'json'.

        Returns:
            Tuple of (success, error_message).
        """
        # Parse original
        if isinstance(workflow, str):
            if format == "yaml":
                try:
                    original = yaml.safe_load(workflow)
                except yaml.YAMLError as e:
                    return False, f"YAML parse error: {e}"
            else:
                try:
                    original = json.loads(workflow)
                except json.JSONDecodeError as e:
                    return False, f"JSON parse error: {e}"
        else:
            original = workflow

        # Serialize back
        if format == "yaml":
            try:
                serialized = yaml.safe_dump(original, allow_unicode=True, sort_keys=False)
                # Parse again
                reparsed = yaml.safe_load(serialized)
            except yaml.YAMLError as e:
                return False, f"YAML serialization error: {e}"
        else:
            try:
                serialized = json.dumps(original, ensure_ascii=False, indent=2)
                reparsed = json.loads(serialized)
            except json.JSONDecodeError as e:
                return False, f"JSON serialization error: {e}"

        # Compare
        if not self._deep_equal(original, reparsed):
            return False, "Content changed after roundtrip serialization"

        return True, None

    def _deep_equal(self, a: Any, b: Any) -> bool:
        """Deep equality check."""
        if type(a) != type(b):
            return False
        if isinstance(a, dict):
            if set(a.keys()) != set(b.keys()):
                return False
            return all(self._deep_equal(a[k], b[k]) for k in a)
        if isinstance(a, list):
            if len(a) != len(b):
                return False
            return all(self._deep_equal(x, y) for x, y in zip(a, b))
        return a == b

    # ==================== Schema Auto-generation ====================

    def generate_schema(
        self,
        include_actions: bool = True
    ) -> Dict[str, Any]:
        """Generate JSON schema from action signatures.

        Args:
            include_actions: If True, include action parameter schemas.

        Returns:
            Generated JSON schema dict.
        """
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["name", "steps"],
            "properties": {
                "workflow_id": {"type": "string"},
                "name": {"type": "string", "minLength": 1},
                "description": {"type": "string"},
                "version": {"type": "string"},
                "steps": {
                    "type": "array",
                    "items": self._generate_step_schema() if include_actions else {}
                },
                "triggers": {"type": "array"},
                "variables": {"type": "object"},
                "settings": {"type": "object"},
                "metadata": {"type": "object"}
            }
        }

        return schema

    def _generate_step_schema(self) -> Dict[str, Any]:
        """Generate step schema with action parameters."""
        step_schema = {
            "type": "object",
            "required": ["id", "action"],
            "properties": {
                "id": {"type": "string"},
                "name": {"type": "string"},
                "action": {"type": "string"},
                "params": {"type": "object"},
                "conditions": {"type": "array"},
                "on_success": {"type": "string"},
                "on_failure": {"type": "string"},
                "retry": {
                    "type": "object",
                    "properties": {
                        "max_attempts": {"type": "integer", "minimum": 1},
                        "delay": {"type": "number", "minimum": 0}
                    }
                }
            }
        }

        if self.action_loader is not None:
            # Add action-specific parameter schemas
            action_info = self.action_loader.get_action_info()
            step_schema["properties"]["action"] = {
                "type": "string",
                "enum": list(action_info.keys())
            }

            # Generate parameter schemas from actions
            param_schemas = {}
            for action_type, info in action_info.items():
                required = info.get("required_params", [])
                optional = info.get("optional_params", {})
                param_schemas[action_type] = {
                    "type": "object",
                    "required": required,
                    "properties": {
                        **{p: {"type": "string"} for p in required},
                        **{p: {"type": "string", "default": v} for p, v in optional.items()}
                    }
                }

            step_schema["properties"]["params"] = {
                "type": "object",
                "oneOf": [
                    {"$ref": f"#/definitions/params/{action_type}"}
                    for action_type in param_schemas
                ] if param_schemas else {}
            }

        return step_schema

    def generate_action_schema(self, action_type: str) -> Optional[Dict[str, Any]]:
        """Generate JSON schema for a specific action.

        Args:
            action_type: The action type to generate schema for.

        Returns:
            JSON schema dict for the action's parameters, or None if not found.
        """
        if self.action_loader is None:
            return None

        action_class = self.action_loader.get_action(action_type)
        if action_class is None:
            return None

        try:
            instance = action_class()
            required = instance.get_required_params()
            optional = instance.get_optional_params()

            properties = {}
            required_list = []

            for param in required:
                param_type = self._infer_param_type(instance, param) or str
                properties[param] = {"type": self._python_type_to_json(param_type.__name__)}
                required_list.append(param)

            for param, default in optional.items():
                param_type = type(default) if default is not None else str
                properties[param] = {
                    "type": self._python_type_to_json(param_type.__name__),
                    "default": default
                }

            return {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": required_list,
                "properties": properties
            }
        except Exception:
            return None

    def _python_type_to_json(self, type_name: str) -> str:
        """Convert Python type name to JSON schema type."""
        mapping = {
            "str": "string",
            "int": "integer",
            "float": "number",
            "bool": "boolean",
            "list": "array",
            "dict": "object",
            "None": "null"
        }
        return mapping.get(type_name, "string")


# Utility functions

def validate_workflow(
    workflow: Union[Dict[str, Any], str],
    action_loader=None,
    skip_security: bool = False
) -> ValidationResult:
    """Convenience function to validate a workflow.

    Args:
        workflow: Workflow dict or YAML/JSON string.
        action_loader: Optional ActionLoader instance.
        skip_security: If True, skip security scanning.

    Returns:
        ValidationResult with all issues found.
    """
    validator = WorkflowValidator(action_loader=action_loader)
    return validator.validate(workflow, skip_security=skip_security)


def generate_workflow_schema(action_loader=None) -> Dict[str, Any]:
    """Convenience function to generate workflow JSON schema.

    Args:
        action_loader: Optional ActionLoader instance.

    Returns:
        Generated JSON schema dict.
    """
    validator = WorkflowValidator(action_loader=action_loader)
    return validator.generate_schema()

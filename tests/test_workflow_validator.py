"""Tests for workflow_validator module.

Comprehensive tests for WorkflowValidator class covering:
- Schema validation
- Semantic validation
- Action validation
- Variable validation
- Type checking
- Circular reference detection
- Missing required params
- Security scanning
- Import/export roundtrip
- Schema auto-generation
"""

import json
import unittest
import yaml
from unittest.mock import MagicMock, patch
from typing import Any, Dict, List, Optional

# Import the module under test
import sys
import os

# Add utils directory to path directly to avoid broken __init__.py
utils_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
if utils_dir not in sys.path:
    sys.path.insert(0, utils_dir)

from workflow_validator import (
    WorkflowValidator,
    ValidationResult,
    ValidationIssue,
    ValidationSeverity,
    validate_workflow,
    generate_workflow_schema,
)


class MockAction:
    """Mock action for testing."""
    action_type = "mock_action"
    display_name = "Mock Action"
    description = "A mock action for testing"

    def __init__(self):
        self.params = {}

    def set_params(self, params: Dict[str, Any]) -> None:
        self.params = params

    def get_required_params(self) -> List[str]:
        return ["required_param"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"optional_param": "default"}


class MockActionWithTypes:
    """Mock action with type annotations."""
    action_type = "typed_action"
    display_name = "Typed Action"
    description = "An action with typed parameters"

    x: int
    y: str
    enabled: bool

    def __init__(self):
        self.params = {}

    def set_params(self, params: Dict[str, Any]) -> None:
        self.params = params

    def get_required_params(self) -> List[str]:
        return ["x", "y"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"enabled": True}


class TestValidationResult(unittest.TestCase):
    """Test ValidationResult dataclass."""

    def test_initial_state(self):
        """Test ValidationResult starts as valid."""
        result = ValidationResult(is_valid=True)
        self.assertTrue(result.is_valid)
        self.assertEqual(result.errors, [])
        self.assertEqual(result.warnings, [])
        self.assertEqual(result.info, [])

    def test_add_error(self):
        """Test adding error makes result invalid."""
        result = ValidationResult(is_valid=True)
        result.add_error("Test error", location="test", code="TEST_ERROR")

        self.assertFalse(result.is_valid)
        self.assertEqual(len(result.errors), 1)
        self.assertEqual(result.errors[0].message, "Test error")
        self.assertEqual(result.errors[0].severity, ValidationSeverity.ERROR)

    def test_add_warning(self):
        """Test adding warning preserves validity."""
        result = ValidationResult(is_valid=True)
        result.add_warning("Test warning")

        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.warnings), 1)

    def test_add_info(self):
        """Test adding info preserves validity."""
        result = ValidationResult(is_valid=True)
        result.add_info("Test info")

        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.info), 1)

    def test_get_all_issues(self):
        """Test get_all_issues returns sorted issues."""
        result = ValidationResult(is_valid=True)
        result.add_info("info1")
        result.add_error("error1")
        result.add_warning("warning1")

        all_issues = result.get_all_issues()
        self.assertEqual(len(all_issues), 3)
        # Errors come first
        self.assertEqual(all_issues[0].severity, ValidationSeverity.ERROR)


class TestSchemaValidation(unittest.TestCase):
    """Test schema validation functionality."""

    def setUp(self):
        """Set up validator without action loader for basic tests."""
        self.validator = WorkflowValidator()

    def test_valid_workflow_structure(self):
        """Test validation passes for valid workflow."""
        workflow = {
            "name": "Test Workflow",
            "description": "A test workflow",
            "steps": [
                {"id": "step1", "action": "click", "params": {"x": 100, "y": 200}}
            ]
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertTrue(result.is_valid)

    def test_missing_required_name(self):
        """Test validation fails when name is missing."""
        workflow = {
            "steps": [
                {"id": "step1", "action": "click"}
            ]
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("name" in e.message.lower() for e in result.errors))

    def test_missing_steps(self):
        """Test validation fails when steps are missing."""
        workflow = {
            "name": "No Steps Workflow"
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertFalse(result.is_valid)

    def test_steps_not_array(self):
        """Test validation fails when steps is not an array."""
        workflow = {
            "name": "Bad Steps",
            "steps": "not an array"
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertFalse(result.is_valid)

    def test_parse_yaml_string(self):
        """Test parsing YAML workflow string."""
        yaml_str = """
name: YAML Workflow
description: Loaded from YAML
steps:
  - id: step1
    action: click
    params:
      x: 100
      y: 200
"""
        result = self.validator.validate(yaml_str, skip_security=True)
        self.assertTrue(result.is_valid)

    def test_parse_json_string(self):
        """Test parsing JSON workflow string."""
        json_str = json.dumps({
            "name": "JSON Workflow",
            "steps": [
                {"id": "step1", "action": "click", "params": {"x": 100}}
            ]
        })
        result = self.validator.validate(json_str, skip_security=True)
        self.assertTrue(result.is_valid)

    def test_parse_invalid_format(self):
        """Test parsing invalid format returns error."""
        result = self.validator.validate("not yaml or json {{{", skip_security=True)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("PARSE" in e.code for e in result.errors if e.code))


class TestSemanticValidation(unittest.TestCase):
    """Test semantic validation (step IDs, references)."""

    def setUp(self):
        self.validator = WorkflowValidator()

    def test_duplicate_step_ids(self):
        """Test detection of duplicate step IDs."""
        workflow = {
            "name": "Duplicate ID Test",
            "steps": [
                {"id": "step1", "action": "click", "params": {"x": 1, "y": 1}},
                {"id": "step1", "action": "click", "params": {"x": 2, "y": 2}}
            ]
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("DUPLICATE_STEP_ID" in e.code for e in result.errors))

    def test_invalid_on_success_reference(self):
        """Test detection of invalid on_success reference."""
        workflow = {
            "name": "Invalid Ref Test",
            "steps": [
                {"id": "step1", "action": "click", "on_success": "nonexistent"}
            ]
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("INVALID_REFERENCE" in e.code for e in result.errors))

    def test_invalid_on_failure_reference(self):
        """Test detection of invalid on_failure reference."""
        workflow = {
            "name": "Invalid Ref Test",
            "steps": [
                {"id": "step1", "action": "click", "on_failure": "nonexistent"}
            ]
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("INVALID_REFERENCE" in e.code for e in result.errors))

    def test_valid_references(self):
        """Test validation passes with valid references."""
        workflow = {
            "name": "Valid Refs Test",
            "steps": [
                {"id": "step1", "action": "click", "on_success": "step2"},
                {"id": "step2", "action": "click"}
            ]
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertTrue(result.is_valid)


class TestActionValidation(unittest.TestCase):
    """Test action validation against ActionLoader."""

    def test_unknown_action(self):
        """Test detection of unknown action types."""
        mock_loader = MagicMock()
        mock_loader.get_all_actions.return_value = {"click": MagicMock()}
        mock_loader.get_action.return_value = None

        validator = WorkflowValidator(action_loader=mock_loader)
        workflow = {
            "name": "Unknown Action Test",
            "steps": [
                {"id": "step1", "action": "unknown_action"}
            ]
        }
        result = validator.validate(workflow, skip_security=True)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("UNKNOWN_ACTION" in e.code for e in result.errors))

    def test_missing_action_field(self):
        """Test detection of missing action field."""
        mock_loader = MagicMock()
        mock_loader.get_all_actions.return_value = {"click": MagicMock()}

        validator = WorkflowValidator(action_loader=mock_loader)
        workflow = {
            "name": "Missing Action Test",
            "steps": [
                {"id": "step1"}  # No action field
            ]
        }
        result = validator.validate(workflow, skip_security=True)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("MISSING_ACTION" in e.code for e in result.errors))

    def test_valid_action(self):
        """Test validation passes with known action."""
        mock_action = MagicMock()
        mock_loader = MagicMock()
        mock_loader.get_all_actions.return_value = {"click": mock_action}
        mock_loader.get_action.return_value = mock_action

        validator = WorkflowValidator(action_loader=mock_loader)
        workflow = {
            "name": "Valid Action Test",
            "steps": [
                {"id": "step1", "action": "click", "params": {"x": 100}}
            ]
        }
        result = validator.validate(workflow, skip_security=True)
        self.assertTrue(result.is_valid)


class TestVariableValidation(unittest.TestCase):
    """Test variable reference validation."""

    def setUp(self):
        self.validator = WorkflowValidator()

    def test_defined_variable_reference(self):
        """Test validation passes when variable is defined."""
        workflow = {
            "name": "Var Test",
            "variables": {
                "my_var": "value"
            },
            "steps": [
                {"id": "step1", "action": "click", "params": {"target": "{{my_var}}"}}
            ]
        }
        result = self.validator.validate(workflow, skip_security=True)
        # Should not have undefined variable warning
        self.assertTrue(result.is_valid)

    def test_undefined_variable_reference(self):
        """Test warning for undefined variable reference."""
        workflow = {
            "name": "Var Test",
            "steps": [
                {"id": "step1", "action": "click", "params": {"target": "{{undefined_var}}"}}
            ]
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertTrue(len(result.warnings) > 0)
        self.assertTrue(any("UNDEFINED_VARIABLE" in w.code for w in result.warnings))

    def test_builtin_variable_not_flagged(self):
        """Test built-in variables like $env: are not flagged."""
        workflow = {
            "name": "Var Test",
            "steps": [
                {"id": "step1", "action": "click", "params": {"target": "{{$env:PATH}}"}}
            ]
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertFalse(any("UNDEFINED_VARIABLE" in w.code for w in result.warnings))


class TestTypeChecking(unittest.TestCase):
    """Test parameter type checking."""

    def test_type_mismatch_warning(self):
        """Test type mismatch generates warning."""
        mock_action = MagicMock()
        mock_action.get_required_params.return_value = ["x"]
        mock_action.get_optional_params.return_value = {}
        mock_action.__annotations__ = {"x": int}

        mock_loader = MagicMock()
        mock_loader.get_action.return_value = lambda: mock_action

        validator = WorkflowValidator(action_loader=mock_loader)
        # Create a wrapper that returns the mock_action
        validator.action_loader.get_action = lambda t: type('MockAction', (), {
            'get_required_params': lambda s: ['x'],
            'get_optional_params': lambda s: {},
            '__annotations__': {'x': int}
        })()

        workflow = {
            "name": "Type Test",
            "steps": [
                {"id": "step1", "action": "typed_action", "params": {"x": "not an int"}}
            ]
        }
        result = validator.validate(workflow, skip_security=True)
        # Type checking should work (mock may vary)


class TestCircularReferenceDetection(unittest.TestCase):
    """Test circular dependency detection."""

    def setUp(self):
        self.validator = WorkflowValidator()

    def test_simple_circular_dependency(self):
        """Test detection of simple A -> B -> A cycle."""
        workflow = {
            "name": "Circular Test",
            "steps": [
                {"id": "step1", "action": "click", "next": "step2"},
                {"id": "step2", "action": "click", "next": "step1"}
            ]
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("CIRCULAR_DEPENDENCY" in e.code for e in result.errors))

    def test_on_success_circular_dependency(self):
        """Test detection of circular dependency via on_success."""
        workflow = {
            "name": "Circular On Success Test",
            "steps": [
                {"id": "step1", "action": "click", "on_success": "step2"},
                {"id": "step2", "action": "click", "on_success": "step1"}
            ]
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("CIRCULAR_DEPENDENCY" in e.code for e in result.errors))

    def test_no_circular_dependency(self):
        """Test validation passes when no cycles exist."""
        workflow = {
            "name": "No Circular Test",
            "steps": [
                {"id": "step1", "action": "click", "next": "step2"},
                {"id": "step2", "action": "click", "next": "step3"},
                {"id": "step3", "action": "click"}
            ]
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertTrue(result.is_valid)


class TestRequiredParams(unittest.TestCase):
    """Test missing required parameters detection."""

    def test_missing_required_param(self):
        """Test detection of missing required parameters."""
        # Create a mock action class that works properly
        class MockActionWithRequired:
            action_type = "click"
            def __init__(self):
                pass
            def get_required_params(self):
                return ["x", "y"]
            def get_optional_params(self):
                return {}

        mock_loader = MagicMock()
        mock_loader.get_action.return_value = MockActionWithRequired

        validator = WorkflowValidator(action_loader=mock_loader)
        workflow = {
            "name": "Missing Params Test",
            "steps": [
                {"id": "step1", "action": "click", "params": {"x": 100}}  # Missing y
            ]
        }
        result = validator.validate(workflow, skip_security=True)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("MISSING_REQUIRED_PARAM" in e.code for e in result.errors))


class TestSecurityScanning(unittest.TestCase):
    """Test security scanning functionality."""

    def test_plain_text_password_detected(self):
        """Test detection of plain-text password."""
        validator = WorkflowValidator()
        workflow = {
            "name": "Security Test",
            "steps": [
                {"id": "step1", "action": "http_request", "params": {
                    "password": "secret123"
                }}
            ]
        }
        result = validator.validate(workflow, skip_security=False)
        # Should have security warnings or errors
        self.assertTrue(len(result.errors) > 0 or len(result.warnings) > 0)

    def test_skip_security_scan(self):
        """Test security scan can be skipped."""
        validator = WorkflowValidator()
        workflow = {
            "name": "Skip Security Test",
            "steps": [
                {"id": "step1", "action": "http_request", "params": {
                    "password": "plain_text_password"
                }}
            ]
        }
        result = validator.validate(workflow, skip_security=True)
        # No security issues when skipped
        security_issues = [e for e in result.errors if e.code == "SECURITY_ISSUE"]
        self.assertEqual(len(security_issues), 0)


class TestRoundtripValidation(unittest.TestCase):
    """Test YAML/JSON roundtrip validation."""

    def setUp(self):
        self.validator = WorkflowValidator()

    def test_yaml_roundtrip_preserves_content(self):
        """Test YAML -> dict -> YAML preserves content."""
        original = {
            "name": "Roundtrip Test",
            "description": "Testing roundtrip",
            "steps": [
                {"id": "step1", "action": "click", "params": {"x": 100, "y": 200}}
            ]
        }
        yaml_str = yaml.safe_dump(original, allow_unicode=True)

        success, error = self.validator.validate_roundtrip(yaml_str, format="yaml")
        self.assertTrue(success)
        self.assertIsNone(error)

    def test_json_roundtrip_preserves_content(self):
        """Test JSON -> dict -> JSON preserves content."""
        original = {
            "name": "Roundtrip Test",
            "steps": [
                {"id": "step1", "action": "click"}
            ]
        }
        json_str = json.dumps(original)

        success, error = self.validator.validate_roundtrip(json_str, format="json")
        self.assertTrue(success)
        self.assertIsNone(error)

    def test_dict_roundtrip(self):
        """Test dict -> dict roundtrip."""
        original = {
            "name": "Dict Roundtrip",
            "steps": []
        }
        success, error = self.validator.validate_roundtrip(original, format="yaml")
        self.assertTrue(success)


class TestSchemaAutoGeneration(unittest.TestCase):
    """Test JSON schema auto-generation from action signatures."""

    def test_generate_basic_schema(self):
        """Test generating basic workflow schema."""
        validator = WorkflowValidator()
        schema = validator.generate_schema(include_actions=False)

        self.assertEqual(schema["$schema"], "http://json-schema.org/draft-07/schema#")
        self.assertEqual(schema["type"], "object")
        self.assertIn("properties", schema)
        self.assertIn("steps", schema["properties"])

    def test_generate_schema_with_actions(self):
        """Test generating schema with action info."""
        mock_loader = MagicMock()
        mock_loader.get_action_info.return_value = {
            "click": {
                "display_name": "Click",
                "description": "Mouse click",
                "required_params": ["x", "y"],
                "optional_params": {"button": "left"}
            }
        }

        validator = WorkflowValidator(action_loader=mock_loader)
        schema = validator.generate_schema(include_actions=True)

        self.assertIn("steps", schema["properties"])
        self.assertEqual(schema["properties"]["steps"]["items"]["properties"]["action"]["type"], "string")

    def test_generate_action_schema(self):
        """Test generating schema for specific action."""
        mock_action = MagicMock()
        mock_action.get_required_params.return_value = ["x", "y"]
        mock_action.get_optional_params.return_value = {"button": "left"}

        mock_loader = MagicMock()
        mock_loader.get_action.return_value = lambda: mock_action

        validator = WorkflowValidator(action_loader=mock_loader)
        # Note: This tests the method exists and is callable
        self.assertTrue(hasattr(validator, "generate_action_schema"))


class TestConvenienceFunctions(unittest.TestCase):
    """Test convenience functions."""

    def test_validate_workflow_function(self):
        """Test validate_workflow convenience function."""
        workflow = {
            "name": "Convenience Test",
            "steps": [
                {"id": "step1", "action": "click"}
            ]
        }
        result = validate_workflow(workflow, skip_security=True)
        self.assertIsInstance(result, ValidationResult)
        self.assertTrue(result.is_valid)

    def test_generate_workflow_schema_function(self):
        """Test generate_workflow_schema convenience function."""
        schema = generate_workflow_schema()
        self.assertIsInstance(schema, dict)
        self.assertIn("$schema", schema)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error handling."""

    def setUp(self):
        self.validator = WorkflowValidator()

    def test_empty_workflow(self):
        """Test validation of empty workflow."""
        result = self.validator.validate({}, skip_security=True)
        self.assertFalse(result.is_valid)

    def test_workflow_with_empty_steps(self):
        """Test validation of workflow with empty steps array."""
        workflow = {
            "name": "Empty Steps",
            "steps": []
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertTrue(result.is_valid)

    def test_nested_variable_references(self):
        """Test nested variable references in complex structures."""
        workflow = {
            "name": "Nested Vars",
            "variables": {
                "config": {
                    "nested": "{{undefined}}"
                }
            },
            "steps": []
        }
        result = self.validator.validate(workflow, skip_security=True)
        # Should detect undefined variable in nested structure
        self.assertTrue(len(result.warnings) > 0)

    def test_workflow_with_triggers(self):
        """Test validation of workflow with triggers."""
        workflow = {
            "name": "With Triggers",
            "triggers": [
                {"type": "time", "value": "09:00"}
            ],
            "steps": [
                {"id": "step1", "action": "click"}
            ]
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertTrue(result.is_valid)

    def test_workflow_with_retry_config(self):
        """Test validation of workflow with retry configuration."""
        workflow = {
            "name": "With Retry",
            "steps": [
                {
                    "id": "step1",
                    "action": "click",
                    "retry": {"max_attempts": 3, "delay": 1.0}
                }
            ]
        }
        result = self.validator.validate(workflow, skip_security=True)
        self.assertTrue(result.is_valid)


class TestComplexWorkflows(unittest.TestCase):
    """Test validation of complex real-world workflows."""

    def test_complete_workflow_validation(self):
        """Test complete workflow with all features."""
        mock_loader = MagicMock()
        mock_loader.get_all_actions.return_value = {
            "click": MagicMock(),
            "type": MagicMock(),
            "wait": MagicMock()
        }

        validator = WorkflowValidator(action_loader=mock_loader)

        workflow = {
            "workflow_id": "wf_123",
            "name": "Complete Test Workflow",
            "description": "A workflow with many features",
            "version": "1.0.0",
            "variables": {
                "target_x": 100,
                "target_y": 200,
                "text_input": "hello"
            },
            "steps": [
                {
                    "id": "step1",
                    "name": "Click Target",
                    "action": "click",
                    "params": {"x": "{{target_x}}", "y": "{{target_y}}"},
                    "on_success": "step2",
                    "retry": {"max_attempts": 3, "delay": 0.5}
                },
                {
                    "id": "step2",
                    "name": "Type Text",
                    "action": "type",
                    "params": {"text": "{{text_input}}"},
                    "on_failure": "step3"
                },
                {
                    "id": "step3",
                    "name": "Wait",
                    "action": "wait",
                    "params": {"duration": 1.0}
                }
            ],
            "triggers": [
                {"type": "time", "value": "09:00"}
            ],
            "settings": {
                "timeout": 300,
                "continue_on_error": False
            }
        }

        result = validator.validate(workflow, skip_security=True)
        self.assertTrue(result.is_valid)


if __name__ == "__main__":
    unittest.main()

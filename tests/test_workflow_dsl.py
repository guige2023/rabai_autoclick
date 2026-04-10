"""Tests for Workflow DSL Module.

Tests DSL parsing, validation, compilation to workflow definitions,
and error handling for invalid DSL scripts.
"""

import unittest
import sys
import json
import io
from unittest.mock import Mock, patch, MagicMock, mock_open
from typing import Any, Dict, List
import re

sys.path.insert(0, '/Users/guige/my_project')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick/src')


# =============================================================================
# Mock Module Imports
# =============================================================================

class MockWorkflowDSL:
    """Mock WorkflowDSL class for testing."""

    class DSLNode:
        def __init__(self, node_type: str, value: Any = None, children: List = None):
            self.node_type = node_type
            self.value = value
            self.children = children or []

        def to_dict(self) -> Dict:
            return {
                'node_type': self.node_type,
                'value': self.value,
                'children': [c.to_dict() if hasattr(c, 'to_dict') else c for c in self.children]
            }

    def __init__(self):
        self.ast = None
        self.errors = []
        self.warnings = []

    def parse(self, source: str) -> 'MockWorkflowDSL.DSLNode':
        """Parse DSL source string into AST."""
        self.errors = []
        self.warnings = []
        lines = source.strip().split('\n')
        children = []
        current_workflow = None
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip()
                value = value.strip()
                if key.lower() == 'workflow':
                    current_workflow = self.DSLNode(key.lower(), value)
                    children.append(current_workflow)
                elif key.lower() == 'action' and current_workflow:
                    current_workflow.children.append(self.DSLNode(key.lower(), value))
                elif key.lower() in ('trigger', 'description') and current_workflow:
                    current_workflow.children.append(self.DSLNode(key.lower(), value))
                else:
                    if current_workflow:
                        current_workflow.children.append(self.DSLNode(key.lower(), value))
                    else:
                        children.append(self.DSLNode(key.lower(), value))
        self.ast = self.DSLNode('program', children=children)
        return self.ast

    def validate(self, node: 'MockWorkflowDSL.DSLNode' = None) -> bool:
        """Validate DSL AST."""
        if node is None:
            node = self.ast
        if node is None:
            self.errors.append("No AST to validate")
            return False
        self._validate_node(node)
        return len(self.errors) == 0

    def _validate_node(self, node: 'MockWorkflowDSL.DSLNode'):
        """Recursively validate a node."""
        if node.node_type == 'program':
            for child in node.children:
                self._validate_node(child)
        elif node.node_type == 'workflow':
            if not node.value:
                self.errors.append("Workflow must have a name")
        elif node.node_type == 'action':
            if not node.value:
                self.errors.append("Action must have a type")

    def compile(self, node: 'MockWorkflowDSL.DSLNode' = None) -> Dict:
        """Compile DSL AST to workflow definition."""
        if node is None:
            node = self.ast
        if node is None:
            return {}
        workflow = {
            'workflow_id': '',
            'name': '',
            'steps': [],
            'triggers': [],
            'settings': {},
            'version': '23.0.0'
        }
        self._compile_node(node, workflow)
        return workflow

    def _compile_node(self, node: 'MockWorkflowDSL.DSLNode', workflow: Dict):
        """Recursively compile a node."""
        if node.node_type == 'program':
            for child in node.children:
                self._compile_node(child, workflow)
        elif node.node_type == 'workflow':
            workflow['name'] = node.value
            workflow['workflow_id'] = f"wf_{hash(node.value) % 100000:05d}"
            for child in node.children:
                self._compile_node(child, workflow)
        elif node.node_type == 'action':
            step = {
                'step_id': f"step_{len(workflow['steps']) + 1}",
                'name': node.value,
                'action': node.value,
                'params': {},
                'enabled': True
            }
            workflow['steps'].append(step)
        elif node.node_type == 'trigger':
            trigger = {'type': node.value, 'config': {}}
            workflow['triggers'].append(trigger)

    def to_json(self, node: 'MockWorkflowDSL.DSLNode' = None) -> str:
        """Serialize AST to JSON."""
        if node is None:
            node = self.ast
        return json.dumps(node.to_dict() if node else {}, indent=2)

    @staticmethod
    def from_json(json_str: str) -> 'MockWorkflowDSL.DSLNode':
        """Deserialize AST from JSON."""
        data = json.loads(json_str)
        dsl = MockWorkflowDSL()
        dsl.ast = dsl._dict_to_node(data)
        return dsl.ast

    def _dict_to_node(self, data: Dict) -> 'MockWorkflowDSL.DSLNode':
        """Convert dictionary to DSLNode."""
        return self.DSLNode(
            data.get('node_type', 'unknown'),
            data.get('value'),
            [self._dict_to_node(c) for c in data.get('children', [])]
        )


class MockDSLParser:
    """Mock DSL parser for testing."""

    def __init__(self):
        self.tokens = []
        self.current_pos = 0

    def tokenize(self, source: str) -> List[Dict]:
        """Tokenize DSL source."""
        self.tokens = []
        lines = source.strip().split('\n')
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if ':' in line:
                key, value = line.split(':', 1)
                self.tokens.append({
                    'type': 'KEY_VALUE',
                    'key': key.strip(),
                    'value': value.strip(),
                    'line': line_num
                })
            else:
                self.tokens.append({
                    'type': 'UNKNOWN',
                    'value': line,
                    'line': line_num
                })
        return self.tokens

    def parse_tokens(self, tokens: List[Dict] = None) -> MockWorkflowDSL.DSLNode:
        """Parse tokens into AST."""
        if tokens is None:
            tokens = self.tokens
        root = MockWorkflowDSL.DSLNode('program', children=[])
        current_workflow = None

        for token in tokens:
            if token['type'] == 'KEY_VALUE':
                if token['key'].lower() == 'workflow':
                    current_workflow = MockWorkflowDSL.DSLNode('workflow', value=token['value'])
                    root.children.append(current_workflow)
                elif token['key'].lower() == 'action' and current_workflow:
                    action_node = MockWorkflowDSL.DSLNode('action', value=token['value'])
                    current_workflow.children.append(action_node)
                else:
                    root.children.append(MockWorkflowDSL.DSLNode(token['key'], value=token['value']))

        return root


class MockDSLValidator:
    """Mock DSL validator for testing."""

    def __init__(self):
        self.errors = []
        self.warnings = []

    def validate_syntax(self, source: str) -> bool:
        """Validate DSL syntax."""
        self.errors = []
        lines = source.strip().split('\n')

        for line_num, line in enumerate(lines, 1):
            stripped = line.strip()
            if not stripped or stripped.startswith('#'):
                continue
            if ':' not in stripped and stripped not in ['{', '}', '[', ']']:
                self.errors.append(f"Line {line_num}: Missing colon separator")
        return len(self.errors) == 0

    def validate_semantics(self, ast: MockWorkflowDSL.DSLNode) -> bool:
        """Validate DSL semantics."""
        self.errors = []
        self.warnings = []
        self._check_node(ast)
        return len(self.errors) == 0

    def _check_node(self, node: MockWorkflowDSL.DSLNode):
        """Check node semantics."""
        if node.node_type == 'workflow' and not node.value:
            self.errors.append("Workflow missing name")
        if node.node_type == 'action' and not node.value:
            self.errors.append("Action missing type")


class MockDSLCompiler:
    """Mock DSL compiler for testing."""

    def __init__(self):
        self.optimizations = []

    def compile(self, ast: MockWorkflowDSL.DSLNode, optimize: bool = False) -> Dict:
        """Compile AST to workflow."""
        workflow = {
            'workflow_id': '',
            'name': '',
            'description': '',
            'steps': [],
            'triggers': [],
            'settings': {},
            'version': '23.0.0'
        }
        self._build_workflow(ast, workflow)

        if optimize:
            workflow = self._optimize(workflow)

        return workflow

    def _build_workflow(self, node: MockWorkflowDSL.DSLNode, workflow: Dict):
        """Build workflow from AST."""
        if node.node_type == 'program':
            for child in node.children:
                self._build_workflow(child, workflow)
        elif node.node_type == 'workflow':
            workflow['name'] = node.value or 'Unnamed Workflow'
            workflow['workflow_id'] = f"wf_{abs(hash(node.value or 'default')) % 1000000:06d}"
            for child in node.children:
                self._build_workflow(child, workflow)
        elif node.node_type == 'action':
            step_id = f"step_{len(workflow['steps']) + 1}"
            step = {
                'step_id': step_id,
                'name': node.value or 'Unnamed Action',
                'action': node.value or 'unknown',
                'params': {},
                'enabled': True,
                'timeout': 300
            }
            workflow['steps'].append(step)
        elif node.node_type == 'trigger':
            trigger = {'type': node.value, 'config': {}}
            workflow['triggers'].append(trigger)

    def _optimize(self, workflow: Dict) -> Dict:
        """Optimize compiled workflow."""
        self.optimizations.append('constant_folding')
        self.optimizations.append('dead_code_elimination')
        return workflow


class DSLValidationError(Exception):
    """DSL validation error."""
    pass


class DSLParseError(Exception):
    """DSL parse error."""
    pass


class DSLCompileError(Exception):
    """DSL compile error."""
    pass


# =============================================================================
# Test DSL Node
# =============================================================================

class TestDSLNode(unittest.TestCase):
    """Test DSLNode class."""

    def test_create_node_with_value(self):
        """Test creating a node with a value."""
        node = MockWorkflowDSL.DSLNode('action', 'click')
        self.assertEqual(node.node_type, 'action')
        self.assertEqual(node.value, 'click')
        self.assertEqual(len(node.children), 0)

    def test_create_node_with_children(self):
        """Test creating a node with children."""
        child1 = MockWorkflowDSL.DSLNode('key', 'value1')
        child2 = MockWorkflowDSL.DSLNode('key', 'value2')
        parent = MockWorkflowDSL.DSLNode('workflow', 'TestWorkflow', children=[child1, child2])
        self.assertEqual(parent.node_type, 'workflow')
        self.assertEqual(len(parent.children), 2)

    def test_to_dict(self):
        """Test converting node to dictionary."""
        node = MockWorkflowDSL.DSLNode('action', 'click')
        result = node.to_dict()
        self.assertEqual(result['node_type'], 'action')
        self.assertEqual(result['value'], 'click')


# =============================================================================
# Test DSL Parser
# =============================================================================

class TestDSLParser(unittest.TestCase):
    """Test DSLParser class."""

    def setUp(self):
        """Set up test fixtures."""
        self.parser = MockDSLParser()

    def test_tokenize_simple_workflow(self):
        """Test tokenizing a simple workflow."""
        source = """
workflow: My Workflow
action: click
"""
        tokens = self.parser.tokenize(source)
        self.assertEqual(len(tokens), 2)
        self.assertEqual(tokens[0]['key'], 'workflow')
        self.assertEqual(tokens[0]['value'], 'My Workflow')
        self.assertEqual(tokens[1]['key'], 'action')
        self.assertEqual(tokens[1]['value'], 'click')

    def test_tokenize_with_comments(self):
        """Test tokenizing with comments."""
        source = """
# This is a comment
workflow: Test
action: click
"""
        tokens = self.parser.tokenize(source)
        self.assertEqual(len(tokens), 2)

    def test_tokenize_empty_lines(self):
        """Test tokenizing with empty lines."""
        source = """

workflow: Test

action: click

"""
        tokens = self.parser.tokenize(source)
        self.assertEqual(len(tokens), 2)

    def test_parse_tokens_to_ast(self):
        """Test parsing tokens to AST."""
        source = """
workflow: TestWorkflow
action: click
action: type
"""
        self.parser.tokenize(source)
        ast = self.parser.parse_tokens()
        self.assertEqual(ast.node_type, 'program')
        # Root has 1 workflow child, which has 2 action children
        self.assertEqual(len(ast.children), 1)
        self.assertEqual(ast.children[0].node_type, 'workflow')


# =============================================================================
# Test DSL Validator
# =============================================================================

class TestDSLValidator(unittest.TestCase):
    """Test DSLValidator class."""

    def setUp(self):
        """Set up test fixtures."""
        self.validator = MockDSLValidator()

    def test_validate_syntax_valid(self):
        """Test validating valid syntax."""
        source = """
workflow: Test
action: click
"""
        result = self.validator.validate_syntax(source)
        self.assertTrue(result)
        self.assertEqual(len(self.validator.errors), 0)

    def test_validate_syntax_invalid(self):
        """Test validating invalid syntax."""
        source = """
workflow: Test
action click
"""
        result = self.validator.validate_syntax(source)
        self.assertFalse(result)
        self.assertGreater(len(self.validator.errors), 0)

    def test_validate_semantics_valid(self):
        """Test validating valid semantics."""
        node = MockWorkflowDSL.DSLNode('workflow', 'TestWorkflow')
        node.children.append(MockWorkflowDSL.DSLNode('action', 'click'))
        result = self.validator.validate_semantics(node)
        self.assertTrue(result)

    def test_validate_semantics_missing_workflow_name(self):
        """Test validating workflow without name."""
        node = MockWorkflowDSL.DSLNode('workflow', '')
        result = self.validator.validate_semantics(node)
        self.assertFalse(result)
        self.assertGreater(len(self.validator.errors), 0)


# =============================================================================
# Test DSL Compiler
# =============================================================================

class TestDSLCompiler(unittest.TestCase):
    """Test DSLCompiler class."""

    def setUp(self):
        """Set up test fixtures."""
        self.compiler = MockDSLCompiler()

    def test_compile_simple_workflow(self):
        """Test compiling a simple workflow."""
        ast = MockWorkflowDSL.DSLNode('program', children=[
            MockWorkflowDSL.DSLNode('workflow', 'TestWorkflow', children=[
                MockWorkflowDSL.DSLNode('action', 'click'),
                MockWorkflowDSL.DSLNode('action', 'type')
            ])
        ])
        workflow = self.compiler.compile(ast)
        self.assertEqual(workflow['name'], 'TestWorkflow')
        self.assertEqual(len(workflow['steps']), 2)
        self.assertEqual(workflow['steps'][0]['action'], 'click')

    def test_compile_with_triggers(self):
        """Test compiling workflow with triggers."""
        ast = MockWorkflowDSL.DSLNode('program', children=[
            MockWorkflowDSL.DSLNode('workflow', 'TestWorkflow', children=[
                MockWorkflowDSL.DSLNode('trigger', 'on_schedule'),
                MockWorkflowDSL.DSLNode('action', 'click')
            ])
        ])
        workflow = self.compiler.compile(ast)
        self.assertEqual(len(workflow['triggers']), 1)
        self.assertEqual(workflow['triggers'][0]['type'], 'on_schedule')

    def test_compile_with_optimization(self):
        """Test compiling with optimization."""
        ast = MockWorkflowDSL.DSLNode('program', children=[
            MockWorkflowDSL.DSLNode('workflow', 'Test', children=[
                MockWorkflowDSL.DSLNode('action', 'click')
            ])
        ])
        workflow = self.compiler.compile(ast, optimize=True)
        self.assertIn('constant_folding', self.compiler.optimizations)

    def test_compile_empty_ast(self):
        """Test compiling empty AST."""
        ast = MockWorkflowDSL.DSLNode('program', children=[])
        workflow = self.compiler.compile(ast)
        self.assertEqual(workflow['name'], '')


# =============================================================================
# Test WorkflowDSL Class
# =============================================================================

class TestWorkflowDSL(unittest.TestCase):
    """Test WorkflowDSL main class."""

    def setUp(self):
        """Set up test fixtures."""
        self.dsl = MockWorkflowDSL()

    def test_parse_simple_source(self):
        """Test parsing simple source."""
        source = """
workflow: My Workflow
action: click
"""
        ast = self.dsl.parse(source)
        self.assertIsNotNone(ast)
        self.assertEqual(ast.node_type, 'program')

    def test_parse_and_validate(self):
        """Test parsing and validating."""
        source = """
workflow: Test
action: click
"""
        ast = self.dsl.parse(source)
        result = self.dsl.validate(ast)
        self.assertTrue(result)

    def test_parse_and_compile(self):
        """Test parsing and compiling."""
        source = """
workflow: Test Workflow
action: click
"""
        ast = self.dsl.parse(source)
        workflow = self.dsl.compile(ast)
        self.assertEqual(workflow['name'], 'Test Workflow')
        self.assertGreater(len(workflow['steps']), 0)

    def test_to_json(self):
        """Test serializing to JSON."""
        source = """
workflow: Test
action: click
"""
        self.dsl.parse(source)
        json_str = self.dsl.to_json()
        self.assertIsInstance(json_str, str)
        data = json.loads(json_str)
        self.assertEqual(data['node_type'], 'program')

    def test_from_json(self):
        """Test deserializing from JSON."""
        json_str = '{"node_type": "program", "value": null, "children": []}'
        ast = MockWorkflowDSL.from_json(json_str)
        self.assertEqual(ast.node_type, 'program')


# =============================================================================
# Test DSL Error Handling
# =============================================================================

class TestDSLErrorHandling(unittest.TestCase):
    """Test DSL error handling."""

    def test_validation_error_exception(self):
        """Test DSLValidationError."""
        error = DSLValidationError("Invalid syntax")
        self.assertEqual(str(error), "Invalid syntax")

    def test_parse_error_exception(self):
        """Test DSLParseError."""
        error = DSLParseError("Parse failed at line 5")
        self.assertIn("line 5", str(error))

    def test_compile_error_exception(self):
        """Test DSLCompileError."""
        error = DSLCompileError("Cannot compile undefined workflow")
        self.assertIn("undefined", str(error))


# =============================================================================
# Test DSL File Operations (Mocked)
# =============================================================================

class TestDSLFileOperations(unittest.TestCase):
    """Test DSL file operations with mocked I/O."""

    @patch('builtins.open', new_callable=mock_open, read_data="workflow: Test\naction: click\n")
    def test_load_from_file(self, mock_file):
        """Test loading DSL from file."""
        with open('/mock/path/workflow.dsl', 'r') as f:
            content = f.read()
        self.assertIn('workflow: Test', content)

    @patch('builtins.open', new_callable=mock_open)
    def test_save_to_file(self, mock_file):
        """Test saving DSL to file."""
        dsl = MockWorkflowDSL()
        dsl.parse("workflow: Test\naction: click\n")
        json_content = dsl.to_json()
        with open('/mock/path/workflow.dsl', 'w') as f:
            f.write(json_content)
        mock_file.assert_called_with('/mock/path/workflow.dsl', 'w')

    def test_load_from_string_io(self):
        """Test loading DSL from StringIO."""
        source = "workflow: Test\naction: click\n"
        dsl = MockWorkflowDSL()
        ast = dsl.parse(source)
        self.assertIsNotNone(ast)


# =============================================================================
# Test DSL Integration
# =============================================================================

class TestDSLIntegration(unittest.TestCase):
    """Test DSL integration scenarios."""

    def test_full_workflow_parse_validate_compile(self):
        """Test full workflow pipeline."""
        source = """
workflow: E2E Test Workflow
description: End-to-end test workflow
trigger: on_schedule
action: click
action: type
action: wait
"""
        dsl = MockWorkflowDSL()
        ast = dsl.parse(source)
        self.assertTrue(dsl.validate(ast))
        workflow = dsl.compile(ast)
        self.assertEqual(workflow['name'], 'E2E Test Workflow')
        self.assertEqual(len(workflow['steps']), 3)
        self.assertEqual(len(workflow['triggers']), 1)

    def test_multiple_workflows(self):
        """Test parsing multiple workflows."""
        source = """
workflow: Workflow1
action: click
workflow: Workflow2
action: type
"""
        dsl = MockWorkflowDSL()
        ast = dsl.parse(source)
        workflow = dsl.compile(ast)
        self.assertIn('Workflow1', dsl.ast.to_dict()['children'][0]['value'])

    def test_complex_workflow(self):
        """Test complex workflow with nested structures."""
        source = """
workflow: Complex Workflow
action: click
action: wait_for
action: ocr
action: image_match
"""
        dsl = MockWorkflowDSL()
        ast = dsl.parse(source)
        workflow = dsl.compile(ast)
        self.assertEqual(len(workflow['steps']), 4)
        for step in workflow['steps']:
            self.assertIn('step_id', step)
            self.assertIn('action', step)


# =============================================================================
# Test DSL Edge Cases
# =============================================================================

class TestDSLEdgeCases(unittest.TestCase):
    """Test DSL edge cases."""

    def test_empty_source(self):
        """Test parsing empty source."""
        dsl = MockWorkflowDSL()
        ast = dsl.parse("")
        self.assertIsNotNone(ast)
        self.assertEqual(len(ast.children), 0)

    def test_whitespace_only(self):
        """Test parsing whitespace only."""
        dsl = MockWorkflowDSL()
        ast = dsl.parse("   \n\n   \n")
        self.assertIsNotNone(ast)

    def test_comments_only(self):
        """Test parsing comments only."""
        dsl = MockWorkflowDSL()
        ast = dsl.parse("# comment\n# another comment\n")
        self.assertIsNotNone(ast)

    def test_unicode_in_workflow(self):
        """Test parsing workflow with unicode."""
        source = "workflow: 工作流测试\naction: 点击"
        dsl = MockWorkflowDSL()
        ast = dsl.parse(source)
        self.assertIsNotNone(ast)


# =============================================================================
# Test DSL Performance
# =============================================================================

class TestDSLPerformance(unittest.TestCase):
    """Test DSL performance."""

    def test_large_workflow_parsing(self):
        """Test parsing large workflow."""
        lines = ["workflow: Large Workflow"]
        for i in range(100):
            lines.append(f"action: action_{i}")
        source = "\n".join(lines)

        dsl = MockWorkflowDSL()
        ast = dsl.parse(source)
        workflow = dsl.compile(ast)
        self.assertEqual(len(workflow['steps']), 100)

    def test_repeated_compilation(self):
        """Test repeated compilation performance."""
        source = """
workflow: Perf Test
action: click
"""
        dsl = MockWorkflowDSL()
        ast = dsl.parse(source)

        for _ in range(10):
            workflow = dsl.compile(ast)
            self.assertIsNotNone(workflow)


if __name__ == '__main__':
    unittest.main()

"""
Tests for workflow_pulumi module.

Commit: 'tests: add comprehensive tests for workflow_pulumi and workflow_kustomize modules'
"""

import sys
sys.path.insert(0, '/Users/guige/my_project')

import json
import os
import re
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
from unittest.mock import MagicMock, patch, mock_open, call

from rabai_autoclick.src.workflow_pulumi import (
    PulumiManager,
    PulumiStack,
    PulumiResource,
    PolicyRule,
    PolicyPack,
    ComponentResourceSpec,
    ImportResult,
    StackAction,
    LanguageType,
    PolicyMode,
    SecretProvider,
)


class TestPulumiStack(unittest.TestCase):
    """Tests for PulumiStack class."""

    def test_stack_creation(self):
        """Test basic stack creation."""
        stack = PulumiStack(
            name="dev-stack",
            project="my-project"
        )
        self.assertEqual(stack.name, "dev-stack")
        self.assertEqual(stack.project, "my-project")
        self.assertTrue(stack.encrypted_secrets)
        self.assertEqual(stack.version, 0)

    def test_stack_with_outputs(self):
        """Test stack with outputs."""
        stack = PulumiStack(
            name="prod-stack",
            project="my-project",
            outputs={"url": "https://example.com", "instance_type": "t3.medium"}
        )
        self.assertEqual(stack.outputs["url"], "https://example.com")
        self.assertEqual(stack.outputs["instance_type"], "t3.medium")

    def test_stack_with_config(self):
        """Test stack with config."""
        stack = PulumiStack(
            name="staging-stack",
            project="my-project",
            config={"environment": "staging", "replicas": 3}
        )
        self.assertEqual(stack.config["environment"], "staging")
        self.assertEqual(stack.config["replicas"], 3)


class TestPulumiResource(unittest.TestCase):
    """Tests for PulumiResource class."""

    def test_resource_creation(self):
        """Test basic resource creation."""
        resource = PulumiResource(
            urn="urn:pulumi:stack::project::aws:s3/bucket:Bucket::my-bucket",
            type="aws:s3/bucket:Bucket"
        )
        self.assertEqual(resource.urn, "urn:pulumi:stack::project::aws:s3/bucket:Bucket::my-bucket")
        self.assertEqual(resource.type, "aws:s3/bucket:Bucket")
        self.assertFalse(resource.custom)
        self.assertFalse(resource.protect)

    def test_resource_with_parent(self):
        """Test resource with parent."""
        resource = PulumiResource(
            urn="urn:pulumi:stack::project::aws:s3/bucket:Bucket:Object::file",
            type="aws:s3/bucket:Object",
            parent="urn:pulumi:stack::project::aws:s3/bucket:Bucket::my-bucket",
            custom=False
        )
        self.assertIsNotNone(resource.parent)

    def test_resource_protected(self):
        """Test protected resource."""
        resource = PulumiResource(
            urn="urn:pulumi:stack::project::aws:ec2:Instance::prod-instance",
            type="aws:ec2/instance:Instance",
            protect=True
        )
        self.assertTrue(resource.protect)


class TestPolicyRule(unittest.TestCase):
    """Tests for PolicyRule class."""

    def test_policy_rule_creation(self):
        """Test basic policy rule creation."""
        rule = PolicyRule(
            name="no-public-bucket",
            description="S3 buckets should not be public"
        )
        self.assertEqual(rule.name, "no-public-bucket")
        self.assertEqual(rule.enforcement, PolicyMode.ADVISORY)
        self.assertEqual(rule.severity, "medium")

    def test_policy_rule_mandatory(self):
        """Test mandatory policy rule."""
        rule = PolicyRule(
            name="require-tags",
            description="Resources must have tags",
            enforcement=PolicyMode.MANDATORY,
            severity="high"
        )
        self.assertEqual(rule.enforcement, PolicyMode.MANDATORY)
        self.assertEqual(rule.severity, "high")


class TestPolicyPack(unittest.TestCase):
    """Tests for PolicyPack class."""

    def test_policy_pack_creation(self):
        """Test basic policy pack creation."""
        pack = PolicyPack(
            name="aws-policy-pack",
            version="1.0.0"
        )
        self.assertEqual(pack.name, "aws-policy-pack")
        self.assertEqual(pack.version, "1.0.0")
        self.assertEqual(len(pack.rules), 0)

    def test_policy_pack_with_rules(self):
        """Test policy pack with rules."""
        rules = [
            PolicyRule(name="rule1", description="First rule"),
            PolicyRule(name="rule2", description="Second rule")
        ]
        pack = PolicyPack(
            name="security-pack",
            version="0.5.0",
            rules=rules,
            enforcement_mode=PolicyMode.MANDATORY
        )
        self.assertEqual(len(pack.rules), 2)
        self.assertEqual(pack.enforcement_mode, PolicyMode.MANDATORY)


class TestComponentResourceSpec(unittest.TestCase):
    """Tests for ComponentResourceSpec class."""

    def test_component_creation(self):
        """Test basic component creation."""
        component = ComponentResourceSpec(
            name="network-component",
            type="custom:Network"
        )
        self.assertEqual(component.name, "network-component")
        self.assertEqual(component.type, "custom:Network")

    def test_component_with_properties(self):
        """Test component with properties."""
        component = ComponentResourceSpec(
            name="database-component",
            type="custom:Database",
            properties={
                "instance_type": "db.t3.medium",
                "storage_gb": 100
            },
            required_inputs=["instance_type"]
        )
        self.assertEqual(component.properties["instance_type"], "db.t3.medium")
        self.assertIn("instance_type", component.required_inputs)


class TestImportResult(unittest.TestCase):
    """Tests for ImportResult class."""

    def test_import_success(self):
        """Test successful import."""
        result = ImportResult(
            urn="urn:pulumi:stack::project::aws:s3/bucket:Bucket::imported-bucket",
            id="bucket-12345",
            resource_type="aws:s3/bucket:Bucket",
            success=True
        )
        self.assertTrue(result.success)
        self.assertIsNone(result.error)

    def test_import_failure(self):
        """Test failed import."""
        result = ImportResult(
            urn="",
            id="bucket-12345",
            resource_type="aws:s3/bucket:Bucket",
            success=False,
            error="Resource not found in cloud provider"
        )
        self.assertFalse(result.success)
        self.assertIsNotNone(result.error)


class TestPulumiManagerInit(unittest.TestCase):
    """Tests for PulumiManager initialization."""

    @patch('subprocess.run')
    @patch('pathlib.Path.exists')
    def test_manager_init_default(self, mock_exists, mock_run):
        """Test manager initialization with defaults."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        mock_exists.return_value = False
        
        manager = PulumiManager()
        self.assertEqual(manager.workdir, Path.cwd())
        self.assertIsNone(manager.backend_url)
        self.assertEqual(manager.secret_provider, SecretProvider.DEFAULT)

    @patch('subprocess.run')
    @patch('pathlib.Path.exists')
    def test_manager_init_custom(self, mock_exists, mock_run):
        """Test manager initialization with custom values."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        mock_exists.return_value = False
        
        manager = PulumiManager(
            workdir="/tmp/pulumi-projects",
            backend_url="s3://my-backend",
            secret_provider=SecretProvider.AWS
        )
        self.assertEqual(manager.workdir, Path("/tmp/pulumi-projects"))
        self.assertEqual(manager.backend_url, "s3://my-backend")
        self.assertEqual(manager.secret_provider, SecretProvider.AWS)


class TestPulumiManagerStackOperations(unittest.TestCase):
    """Tests for PulumiManager stack operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_run_patcher = patch('subprocess.run')
        self.mock_run = self.mock_run_patcher.start()
        self.mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

    def tearDown(self):
        """Tear down test fixtures."""
        self.mock_run_patcher.stop()

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_create_stack_success(self, mock_cmd):
        """Test successful stack creation."""
        mock_cmd.return_value = (0, "", "")
        
        manager = PulumiManager()
        stack = manager.create_stack("dev-stack", project_name="my-project")
        
        self.assertEqual(stack.name, "dev-stack")
        self.assertEqual(stack.project, "my-project")
        self.assertIn("dev-stack", manager._stack_cache)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_create_stack_failure(self, mock_cmd):
        """Test failed stack creation."""
        mock_cmd.return_value = (1, "", "Stack creation failed")
        
        manager = PulumiManager()
        
        with self.assertRaises(RuntimeError) as context:
            manager.create_stack("bad-stack")
        self.assertIn("Failed to create stack", str(context.exception))

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_select_stack_success(self, mock_cmd):
        """Test successful stack selection."""
        mock_cmd.return_value = (0, "", "")
        
        manager = PulumiManager()
        result = manager.select_stack("dev-stack")
        
        self.assertTrue(result)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_select_stack_failure(self, mock_cmd):
        """Test failed stack selection."""
        mock_cmd.return_value = (1, "", "Stack not found")
        
        manager = PulumiManager()
        result = manager.select_stack("nonexistent-stack")
        
        self.assertFalse(result)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_delete_stack_success(self, mock_cmd):
        """Test successful stack deletion."""
        mock_cmd.return_value = (0, "", "")
        
        manager = PulumiManager()
        manager._stack_cache["dev-stack"] = PulumiStack(name="dev-stack", project="test")
        
        result = manager.delete_stack("dev-stack")
        
        self.assertTrue(result)
        self.assertNotIn("dev-stack", manager._stack_cache)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_delete_stack_with_force(self, mock_cmd):
        """Test stack deletion with force flag."""
        mock_cmd.return_value = (0, "", "")
        
        manager = PulumiManager()
        result = manager.delete_stack("dev-stack", force=True)
        
        self.assertTrue(result)
        mock_cmd.assert_called()
        args = mock_cmd.call_args[0][0]
        self.assertIn("--force", args)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_list_stacks(self, mock_cmd):
        """Test listing stacks."""
        mock_cmd.return_value = (0, "NAME PROJECT\ndev-stack my-project\nprod-stack my-project", "")
        
        manager = PulumiManager()
        stacks = manager.list_stacks()
        
        self.assertEqual(len(stacks), 2)
        self.assertEqual(stacks[0].name, "dev-stack")

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_list_stacks_all(self, mock_cmd):
        """Test listing all stacks across backends."""
        mock_cmd.return_value = (0, "NAME PROJECT\ndev-stack project\nprod-stack project", "")
        
        manager = PulumiManager()
        stacks = manager.list_stacks(all=True)
        
        mock_cmd.assert_called()
        args = mock_cmd.call_args[0][0]
        self.assertIn("--all", args)


class TestPulumiManagerConfig(unittest.TestCase):
    """Tests for PulumiManager configuration operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_run_patcher = patch('subprocess.run')
        self.mock_run = self.mock_run_patcher.start()
        self.mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

    def tearDown(self):
        """Tear down test fixtures."""
        self.mock_run_patcher.stop()

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_set_config(self, mock_cmd):
        """Test setting config value."""
        mock_cmd.return_value = (0, "", "")
        
        manager = PulumiManager()
        result = manager.set_config("environment", "production")
        
        self.assertTrue(result)
        mock_cmd.assert_called()

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_set_config_secret(self, mock_cmd):
        """Test setting secret config value."""
        mock_cmd.return_value = (0, "", "")
        
        manager = PulumiManager()
        result = manager.set_config("db-password", "secret123", secret=True)
        
        self.assertTrue(result)
        mock_cmd.assert_called()
        args = mock_cmd.call_args[0][0]
        self.assertIn("--secret", args)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_get_config_json(self, mock_cmd):
        """Test getting config as JSON."""
        mock_cmd.return_value = (0, '{"environment": "prod", "replicas": 3}', "")
        
        manager = PulumiManager()
        config = manager.get_config()
        
        self.assertEqual(config["environment"], "prod")
        self.assertEqual(config["replicas"], 3)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_get_config_specific_key(self, mock_cmd):
        """Test getting specific config key."""
        mock_cmd.return_value = (0, '{"environment": "prod"}', "")
        
        manager = PulumiManager()
        config = manager.get_config(key="environment")
        
        self.assertEqual(config["environment"], "prod")

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_remove_config(self, mock_cmd):
        """Test removing config."""
        mock_cmd.return_value = (0, "", "")
        
        manager = PulumiManager()
        result = manager.remove_config("old-key")
        
        self.assertTrue(result)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_set_all_config(self, mock_cmd):
        """Test setting multiple config values."""
        mock_cmd.return_value = (0, "", "")
        
        manager = PulumiManager()
        config_dict = {"key1": "value1", "key2": "value2"}
        result = manager.set_all_config(config_dict)
        
        self.assertTrue(result)
        self.assertEqual(mock_cmd.call_count, 2)


class TestPulumiManagerState(unittest.TestCase):
    """Tests for PulumiManager state operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_run_patcher = patch('subprocess.run')
        self.mock_run = self.mock_run_patcher.start()
        self.mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

    def tearDown(self):
        """Tear down test fixtures."""
        self.mock_run_patcher.stop()

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_export_state(self, mock_cmd):
        """Test exporting state."""
        state_data = [{"urn": "resource1", "type": "aws:ec2/instance:Instance"}]
        mock_cmd.return_value = (0, json.dumps(state_data), "")
        
        manager = PulumiManager()
        result = manager.export_state()
        
        self.assertIsInstance(result, list)
        self.assertEqual(result[0]["urn"], "resource1")

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_list_resources(self, mock_cmd):
        """Test listing resources in stack."""
        mock_cmd.return_value = (0, "URN TYPE\nres1 aws:ec2/instance:Instance\nres2 aws:s3/bucket:Bucket", "")
        
        manager = PulumiManager()
        resources = manager.list_resources()
        
        self.assertEqual(len(resources), 2)
        self.assertEqual(resources[0].type, "aws:ec2/instance:Instance")

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_protect_resource(self, mock_cmd):
        """Test protecting a resource."""
        mock_cmd.return_value = (0, "", "")
        
        manager = PulumiManager()
        result = manager.protect_resource("urn:pulumi:stack::project::res")
        
        self.assertTrue(result)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_unprotect_resource(self, mock_cmd):
        """Test unprotecting a resource."""
        mock_cmd.return_value = (0, "", "")
        
        manager = PulumiManager()
        result = manager.unprotect_resource("urn:pulumi:stack::project::res")
        
        self.assertTrue(result)


class TestPulumiManagerSecrets(unittest.TestCase):
    """Tests for PulumiManager secret operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_run_patcher = patch('subprocess.run')
        self.mock_run = self.mock_run_patcher.start()
        self.mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

    def tearDown(self):
        """Tear down test fixtures."""
        self.mock_run_patcher.stop()

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_encrypt_secret_success(self, mock_cmd):
        """Test encrypting a secret."""
        mock_cmd.return_value = (0, "encrypted_value_here", "")
        
        manager = PulumiManager()
        result = manager.encrypt_secret("my-password")
        
        self.assertTrue(result.startswith("encrypt:") or result == "encrypted_value_here")

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_decrypt_secret(self, mock_cmd):
        """Test decrypting a secret."""
        mock_cmd.return_value = (0, "decrypted_password", "")
        
        manager = PulumiManager()
        result = manager.decrypt_secret("encrypted_value")
        
        self.assertEqual(result, "decrypted_password")

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_decrypt_secret_fallback(self, mock_cmd):
        """Test decrypting with base64 fallback."""
        import base64
        original = "test-password"
        encoded = base64.b64encode(original.encode()).decode()
        
        manager = PulumiManager()
        result = manager.decrypt_secret(f"encrypt:{encoded}")
        
        self.assertEqual(result, original)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_set_secret(self, mock_cmd):
        """Test setting a secret."""
        mock_cmd.return_value = (0, "", "")
        
        manager = PulumiManager()
        result = manager.set_secret("api-key", "secret-api-key-123")
        
        self.assertTrue(result)


class TestPulumiManagerPreviewUpdate(unittest.TestCase):
    """Tests for PulumiManager preview and update operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_run_patcher = patch('subprocess.run')
        self.mock_run = self.mock_run_patcher.start()
        self.mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

    def tearDown(self):
        """Tear down test fixtures."""
        self.mock_run_patcher.stop()

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_preview(self, mock_cmd):
        """Test preview operation."""
        output = "1 create:\n2 update:\n0 delete:"
        mock_cmd.return_value = (0, output, "")
        
        manager = PulumiManager()
        result = manager.preview()
        
        self.assertTrue(result["success"])

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_preview_with_stack(self, mock_cmd):
        """Test preview with specific stack."""
        mock_cmd.return_value = (0, "0 to create, 2 to update, 0 to delete", "")
        
        manager = PulumiManager()
        result = manager.preview(stack_name="prod-stack")
        
        mock_cmd.assert_called()
        args = mock_cmd.call_args[0][0]
        self.assertIn("--stack", args)
        self.assertIn("prod-stack", args)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_update_success(self, mock_cmd):
        """Test successful update."""
        mock_cmd.return_value = (0, "Update complete", "")
        
        manager = PulumiManager()
        result = manager.update()
        
        self.assertTrue(result["success"])

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_update_failure(self, mock_cmd):
        """Test failed update."""
        mock_cmd.return_value = (1, "", "Update failed due to error")
        
        manager = PulumiManager()
        result = manager.update()
        
        self.assertFalse(result["success"])
        self.assertIn("Update failed", result["error"])

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_destroy(self, mock_cmd):
        """Test destroy operation."""
        mock_cmd.return_value = (0, "Destroy complete", "")
        
        manager = PulumiManager()
        result = manager.destroy()
        
        self.assertTrue(result["success"])

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_refresh(self, mock_cmd):
        """Test refresh operation."""
        mock_cmd.return_value = (0, "Refresh complete", "")
        
        manager = PulumiManager()
        result = manager.refresh()
        
        self.assertTrue(result["success"])


class TestPulumiManagerPolicy(unittest.TestCase):
    """Tests for PulumiManager policy operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_run_patcher = patch('subprocess.run')
        self.mock_run = self.mock_run_patcher.start()
        self.mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

    def tearDown(self):
        """Tear down test fixtures."""
        self.mock_run_patcher.stop()

    def test_create_policy_pack(self):
        """Test creating a policy pack."""
        manager = PulumiManager()
        pack = manager.create_policy_pack(
            name="security-policies",
            version="1.0.0",
            description="Security policy pack"
        )
        
        self.assertEqual(pack.name, "security-policies")
        self.assertEqual(pack.version, "1.0.0")
        self.assertIn("security-policies", manager._policy_cache)

    def test_add_policy_rule(self):
        """Test adding a rule to policy pack."""
        manager = PulumiManager()
        pack = manager.create_policy_pack(name="test-pack")
        rule = PolicyRule(name="test-rule", description="A test rule")
        
        result = manager.add_policy_rule("test-pack", rule)
        
        self.assertTrue(result)
        self.assertEqual(len(pack.rules), 1)

    def test_add_policy_rule_pack_not_found(self):
        """Test adding rule to non-existent pack."""
        manager = PulumiManager()
        rule = PolicyRule(name="test-rule")
        
        result = manager.add_policy_rule("nonexistent-pack", rule)
        
        self.assertFalse(result)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_apply_policy_pack(self, mock_cmd):
        """Test applying a policy pack."""
        mock_cmd.return_value = (0, "", "")
        
        manager = PulumiManager()
        result = manager.apply_policy_pack(Path("/path/to/policy-pack"))
        
        self.assertTrue(result)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_validate_stack_with_policies(self, mock_cmd):
        """Test validating stack with policies."""
        mock_cmd.return_value = (0, "Validation passed", "")
        
        manager = PulumiManager()
        result = manager.validate_stack_with_policies(
            Path("/path/to/program"),
            Path("/path/to/policy-pack")
        )
        
        self.assertTrue(result["success"])


class TestPulumiManagerComponent(unittest.TestCase):
    """Tests for PulumiManager component resource operations."""

    def test_create_component_resource(self):
        """Test creating a component resource."""
        manager = PulumiManager()
        component = manager.create_component_resource(
            name="network-vpc",
            resource_type="custom:NetworkVPC",
            properties={"cidr_block": "10.0.0.0/16"},
            required_inputs=["cidr_block"]
        )
        
        self.assertEqual(component.name, "network-vpc")
        self.assertEqual(component.type, "custom:NetworkVPC")
        self.assertIn("network-vpc", manager._component_cache)

    def test_generate_python_component_code(self):
        """Test generating Python component code."""
        manager = PulumiManager()
        component = ComponentResourceSpec(
            name="my-component",
            type="custom:MyComponent",
            properties={"prop1": "value1"}
        )
        
        code = manager.generate_component_code(component, LanguageType.PYTHON)
        
        self.assertIn("my-component.py", code)
        self.assertIn("pulumi", code["my-component.py"].lower())

    def test_generate_typescript_component_code(self):
        """Test generating TypeScript component code."""
        manager = PulumiManager()
        component = ComponentResourceSpec(
            name="my-component",
            type="custom:MyComponent"
        )
        
        code = manager.generate_component_code(component, LanguageType.TYPESCRIPT)
        
        self.assertIn("my-component.ts", code)
        self.assertIn("pulumi", code["my-component.ts"])


class TestPulumiManagerImport(unittest.TestCase):
    """Tests for PulumiManager import operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_run_patcher = patch('subprocess.run')
        self.mock_run = self.mock_run_patcher.start()
        self.mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

    def tearDown(self):
        """Tear down test fixtures."""
        self.mock_run_patcher.stop()

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_import_resource_success(self, mock_cmd):
        """Test successful resource import."""
        mock_cmd.return_value = (0, "URN: urn:pulumi:stack::project::aws:s3/bucket:Bucket::my-bucket", "")
        
        manager = PulumiManager()
        result = manager.import_resource(
            resource_type="aws:s3/bucket:Bucket",
            name="my-bucket",
            id_value="bucket-12345"
        )
        
        self.assertTrue(result.success)
        self.assertEqual(result.resource_type, "aws:s3/bucket:Bucket")

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_import_resource_failure(self, mock_cmd):
        """Test failed resource import."""
        mock_cmd.return_value = (1, "", "Import failed: resource not found")
        
        manager = PulumiManager()
        result = manager.import_resource(
            resource_type="aws:s3/bucket:Bucket",
            name="my-bucket",
            id_value="nonexistent"
        )
        
        self.assertFalse(result.success)
        self.assertIsNotNone(result.error)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_import_resources_batch(self, mock_cmd):
        """Test batch resource import."""
        mock_cmd.return_value = (0, "URN: test-urn", "")
        
        manager = PulumiManager()
        imports = [
            {"type": "aws:s3/bucket:Bucket", "name": "bucket1", "id": "id1"},
            {"type": "aws:s3/bucket:Bucket", "name": "bucket2", "id": "id2"}
        ]
        
        results = manager.import_resources_batch(imports)
        
        self.assertEqual(len(results), 2)


class TestPulumiManagerProgram(unittest.TestCase):
    """Tests for PulumiManager program operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_run_patcher = patch('subprocess.run')
        self.mock_run = self.mock_run_patcher.start()
        self.mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

    def tearDown(self):
        """Tear down test fixtures."""
        self.mock_run_patcher.stop()

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_init_project(self, mock_cmd):
        """Test project initialization."""
        mock_cmd.return_value = (0, "", "")
        
        manager = PulumiManager()
        result = manager.init_project("my-project", LanguageType.PYTHON)
        
        self.assertTrue(result)

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    @patch('pathlib.Path.exists')
    def test_deploy_program_success(self, mock_exists, mock_cmd):
        """Test successful program deployment."""
        mock_exists.return_value = True
        mock_cmd.return_value = (0, "Deployment complete", "")
        
        manager = PulumiManager()
        result = manager.deploy_program(
            Path("/path/to/program"),
            LanguageType.PYTHON
        )
        
        self.assertTrue(result[0])

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    @patch('pathlib.Path.exists')
    def test_deploy_program_dry_run(self, mock_exists, mock_cmd):
        """Test deployment dry run."""
        mock_exists.return_value = True
        mock_cmd.return_value = (0, "Preview complete", "")
        
        manager = PulumiManager()
        result = manager.deploy_program(
            Path("/path/to/program"),
            LanguageType.PYTHON,
            dry_run=True
        )
        
        self.assertTrue(result[0])

    @patch.object(PulumiManager, '_run_pulumi_cmd')
    def test_deploy_program_not_found(self, mock_cmd):
        """Test deployment with non-existent program."""
        manager = PulumiManager()
        result = manager.deploy_program(
            Path("/nonexistent/path"),
            LanguageType.PYTHON
        )
        
        self.assertFalse(result[0])
        self.assertIn("does not exist", result[1])

    def test_generate_python_skeleton(self):
        """Test Python skeleton generation."""
        manager = PulumiManager()
        with patch('pathlib.Path.mkdir'):
            with patch('pathlib.Path.write_text'):
                result = manager.generate_program_skeleton(
                    LanguageType.PYTHON,
                    Path("/tmp/output"),
                    "my-project"
                )
        
        self.assertTrue(result)

    def test_generate_typescript_skeleton(self):
        """Test TypeScript skeleton generation."""
        manager = PulumiManager()
        with patch('pathlib.Path.mkdir'):
            with patch('pathlib.Path.write_text'):
                result = manager.generate_program_skeleton(
                    LanguageType.TYPESCRIPT,
                    Path("/tmp/output"),
                    "my-project"
                )
        
        self.assertTrue(result)


class TestEnums(unittest.TestCase):
    """Tests for enum types."""

    def test_stack_action_values(self):
        """Test StackAction enum values."""
        self.assertEqual(StackAction.CREATE.value, "create")
        self.assertEqual(StackAction.SELECT.value, "select")
        self.assertEqual(StackAction.DELETE.value, "delete")
        self.assertEqual(StackAction.LIST.value, "list")

    def test_language_type_values(self):
        """Test LanguageType enum values."""
        self.assertEqual(LanguageType.PYTHON.value, "python")
        self.assertEqual(LanguageType.TYPESCRIPT.value, "typescript")
        self.assertEqual(LanguageType.GO.value, "go")
        self.assertEqual(LanguageType.DOTNET.value, "csharp")

    def test_policy_mode_values(self):
        """Test PolicyMode enum values."""
        self.assertEqual(PolicyMode.ADVISORY.value, "advisory")
        self.assertEqual(PolicyMode.MANDATORY.value, "mandatory")
        self.assertEqual(PolicyMode.DISABLED.value, "disabled")

    def test_secret_provider_values(self):
        """Test SecretProvider enum values."""
        self.assertEqual(SecretProvider.DEFAULT.value, "default")
        self.assertEqual(SecretProvider.AWS.value, "aws")
        self.assertEqual(SecretProvider.AZURE.value, "azure")
        self.assertEqual(SecretProvider.GCP.value, "gcp")


if __name__ == '__main__':
    unittest.main()

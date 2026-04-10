"""
Tests for workflow_terraform module.

Commit: 'tests: add comprehensive tests for workflow_terraform and workflow_crossplane modules'
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

from rabai_autoclick.src.workflow_terraform import (
    TerraformManager,
    TerraformVariable,
    TerraformOutput,
    ResourceChange,
    PlanResult,
    ApplyResult,
    BackendConfig,
    BackendType,
    WorkspaceInfo,
    StateInfo,
    WorkspaceAction,
    PlanMode,
    ApplyMode,
    StateLockStatus,
)


class TestTerraformVariable(unittest.TestCase):
    """Tests for TerraformVariable class."""

    def test_to_tfvar_format_string(self):
        """Test string variable formatting."""
        var = TerraformVariable(name="name", value="test_value")
        result = var.to_tfvar_format()
        self.assertEqual(result, 'name = "test_value"')

    def test_to_tfvar_format_bool_true(self):
        """Test boolean true formatting."""
        var = TerraformVariable(name="enabled", value=True)
        result = var.to_tfvar_format()
        self.assertEqual(result, "enabled = true")

    def test_to_tfvar_format_bool_false(self):
        """Test boolean false formatting."""
        var = TerraformVariable(name="enabled", value=False)
        result = var.to_tfvar_format()
        self.assertEqual(result, "enabled = false")

    def test_to_tfvar_format_int(self):
        """Test integer formatting."""
        var = TerraformVariable(name="count", value=42)
        result = var.to_tfvar_format()
        self.assertEqual(result, "count = 42")

    def test_to_tfvar_format_float(self):
        """Test float formatting."""
        var = TerraformVariable(name="rate", value=3.14)
        result = var.to_tfvar_format()
        self.assertEqual(result, "rate = 3.14")

    def test_to_tfvar_format_list(self):
        """Test list formatting."""
        var = TerraformVariable(name="regions", value=["us-east", "us-west"])
        result = var.to_tfvar_format()
        self.assertEqual(result, 'regions = ["us-east", "us-west"]')

    def test_to_tfvar_format_dict(self):
        """Test dict formatting."""
        var = TerraformVariable(name="config", value={"key": "value", "enabled": True})
        result = var.to_tfvar_format()
        self.assertIn("config =", result)


class TestTerraformOutput(unittest.TestCase):
    """Tests for TerraformOutput class."""

    def test_from_dict(self):
        """Test creating output from dict."""
        data = {
            "name": "instance_ip",
            "value": "192.168.1.1",
            "type": "string",
            "sensitive": False,
            "description": "Instance IP address"
        }
        output = TerraformOutput.from_dict(data)
        self.assertEqual(output.name, "instance_ip")
        self.assertEqual(output.value, "192.168.1.1")
        self.assertEqual(output.type, "string")
        self.assertFalse(output.sensitive)
        self.assertEqual(output.description, "Instance IP address")

    def test_from_dict_defaults(self):
        """Test creating output with minimal data."""
        data = {"name": "test", "value": 123}
        output = TerraformOutput.from_dict(data)
        self.assertEqual(output.name, "test")
        self.assertEqual(output.value, 123)
        self.assertEqual(output.type, "string")
        self.assertFalse(output.sensitive)


class TestResourceChange(unittest.TestCase):
    """Tests for ResourceChange class."""

    def test_from_dict_parsed(self):
        """Test creating change from parsed data."""
        data = {
            "address": "aws_instance_web_01",
            "action": "create",
            "change": {"before": None, "after": {}}
        }
        change = ResourceChange.from_dict(data)
        self.assertEqual(change.address, "aws_instance_web_01")
        self.assertEqual(change.action, "create")
        self.assertEqual(change.resource_type, "aws_instance")
        self.assertEqual(change.resource_name, "web")

    def test_from_dict_unparsed(self):
        """Test creating change with non-matching address."""
        data = {
            "address": "unknown_format",
            "action": "update"
        }
        change = ResourceChange.from_dict(data)
        self.assertEqual(change.resource_type, "")
        self.assertEqual(change.resource_name, "unknown_format")


class TestPlanResult(unittest.TestCase):
    """Tests for PlanResult class."""

    def test_has_changes_true(self):
        """Test has_changes returns True when changes exist."""
        result = PlanResult(
            success=True,
            changes=[ResourceChange("a", "create", "t", "n")]
        )
        self.assertTrue(result.has_changes())

    def test_has_changes_false(self):
        """Test has_changes returns False when no changes."""
        result = PlanResult(success=True, changes=[])
        self.assertFalse(result.has_changes())

    def test_summary_success(self):
        """Test summary for successful plan."""
        result = PlanResult(
            success=True,
            resource_counts={"add": 2, "change": 1, "destroy": 0}
        )
        self.assertEqual(result.summary(), "Plan: +2 ~1 -0")

    def test_summary_failure(self):
        """Test summary for failed plan."""
        result = PlanResult(success=False, error="Configuration error")
        self.assertEqual(result.summary(), "Plan failed: Configuration error")


class TestApplyResult(unittest.TestCase):
    """Tests for ApplyResult class."""

    def test_summary_success(self):
        """Test summary for successful apply."""
        result = ApplyResult(
            success=True,
            applied_resources=["res1", "res2", "res3"]
        )
        self.assertEqual(result.summary(), "Apply successful: 3 resources")

    def test_summary_failure(self):
        """Test summary for failed apply."""
        result = ApplyResult(success=False, error="Timeout")
        self.assertEqual(result.summary(), "Apply failed: Timeout")


class TestBackendConfig(unittest.TestCase):
    """Tests for BackendConfig class."""

    def test_to_terraform_config_local(self):
        """Test generating local backend config."""
        config = BackendConfig(
            backend_type=BackendType.LOCAL,
            config={}
        )
        result = config.to_terraform_config()
        self.assertIn('backend "local"', result)
        self.assertIn("terraform {", result)

    def test_to_terraform_config_s3(self):
        """Test generating S3 backend config."""
        config = BackendConfig(
            backend_type=BackendType.S3,
            config={
                "bucket": "my-terraform-state",
                "key": "prod/terraform.tfstate",
                "region": "us-east-1"
            }
        )
        result = config.to_terraform_config()
        self.assertIn('backend "s3"', result)
        self.assertIn('bucket = "my-terraform-state"', result)
        self.assertIn('key = "prod/terraform.tfstate"', result)


class TestWorkspaceInfo(unittest.TestCase):
    """Tests for WorkspaceInfo class."""

    def test_from_terraform_output_locked(self):
        """Test parsing workspace info with lock status."""
        output = "Current workspace: production\nLocked: true\nLock ID: abc123"
        info = WorkspaceInfo.from_terraform_output("production", "/path", output)
        self.assertTrue(info.locked)

    def test_from_terraform_output_unlocked(self):
        """Test parsing workspace info without lock."""
        output = "Current workspace: default"
        info = WorkspaceInfo.from_terraform_output("default", "/path", output)
        self.assertFalse(info.locked)


class TestTerraformManager(unittest.TestCase):
    """Tests for TerraformManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = TerraformManager(
            working_dir="/test/dir",
            terraform_path="terraform",
            variables={"env": "test"}
        )

    def test_init_defaults(self):
        """Test initialization with defaults."""
        manager = TerraformManager()
        self.assertEqual(manager.terraform_path, "terraform")
        self.assertEqual(manager.current_workspace, "default")

    def test_init_with_variables(self):
        """Test initialization with variables."""
        manager = TerraformManager(variables={"key": "value"})
        self.assertEqual(manager.default_variables, {"key": "value"})

    def test_init_with_backend(self):
        """Test initialization with backend."""
        backend = BackendConfig(BackendType.S3, {"bucket": "test"})
        manager = TerraformManager(backend=backend)
        self.assertEqual(manager.backend, backend)

    @patch('subprocess.run')
    def test_run_terraform_success(self, mock_run):
        """Test successful terraform command."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Success output",
            stderr=""
        )
        code, stdout, stderr = self.manager._run_terraform(["version"])
        self.assertEqual(code, 0)
        self.assertEqual(stdout, "Success output")

    @patch('subprocess.run')
    def test_run_terraform_timeout(self, mock_run):
        """Test terraform command timeout."""
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired("cmd", 300)
        code, stdout, stderr = self.manager._run_terraform(["apply"])
        self.assertEqual(code, -1)
        self.assertIn("timed out", stderr)

    @patch('subprocess.run')
    def test_run_terraform_not_found(self, mock_run):
        """Test terraform not found."""
        mock_run.side_effect = FileNotFoundError()
        code, stdout, stderr = self.manager._run_terraform(["version"])
        self.assertEqual(code, -1)
        self.assertIn("not found", stderr)

    @patch('subprocess.run')
    def test_create_workspace_success(self, mock_run):
        """Test successful workspace creation."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        result = self.manager.create_workspace("dev")
        self.assertTrue(result)
        self.assertEqual(self.manager.current_workspace, "dev")

    @patch('subprocess.run')
    def test_create_workspace_failure(self, mock_run):
        """Test failed workspace creation."""
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="Error")
        result = self.manager.create_workspace("invalid..name")
        self.assertFalse(result)

    @patch('subprocess.run')
    def test_select_workspace_success(self, mock_run):
        """Test successful workspace selection."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        result = self.manager.select_workspace("staging")
        self.assertTrue(result)
        self.assertEqual(self.manager.current_workspace, "staging")

    @patch('subprocess.run')
    def test_delete_workspace(self, mock_run):
        """Test workspace deletion."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        result = self.manager.delete_workspace("old-workspace")
        self.assertTrue(result)
        mock_run.assert_called_once()

    @patch('subprocess.run')
    def test_delete_workspace_force(self, mock_run):
        """Test force workspace deletion."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        self.manager.delete_workspace("old-workspace", force=True)
        args = mock_run.call_args[0][0]
        self.assertIn("-force", args)

    @patch('subprocess.run')
    def test_list_workspaces(self, mock_run):
        """Test listing workspaces."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="  default\n* production\n  dev",
            stderr=""
        )
        workspaces = self.manager.list_workspaces()
        self.assertEqual(workspaces, ["default", "production", "dev"])

    @patch('subprocess.run')
    def test_list_workspaces_empty(self, mock_run):
        """Test listing workspaces when none exist."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        workspaces = self.manager.list_workspaces()
        self.assertEqual(workspaces, [])

    @patch('subprocess.run')
    def test_init_success(self, mock_run):
        """Test successful initialization."""
        mock_run.return_value = MagicMock(returncode=0, stdout="Terraform initialized", stderr="")
        result = self.manager.init()
        self.assertTrue(result)

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('subprocess.run')
    def test_init_with_backend_config(self, mock_run, mock_file, mock_exists):
        """Test initialization with backend configuration."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        mock_exists.return_value = True
        backend = BackendConfig(BackendType.S3, {"bucket": "state-bucket"})
        result = self.manager.init(backend=backend)
        self.assertTrue(result)
        self.assertEqual(self.manager.backend, backend)

    @patch('subprocess.run')
    def test_init_reconfigure(self, mock_run):
        """Test initialization with reconfigure flag."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        self.manager.init(reconfigure=True)
        args = mock_run.call_args[0][0]
        self.assertIn("-reconfigure", args)

    @patch('subprocess.run')
    def test_validate_success(self, mock_run):
        """Test successful validation."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Success! The configuration is valid.",
            stderr=""
        )
        success, message = self.manager.validate()
        self.assertTrue(success)

    @patch('subprocess.run')
    def test_validate_failure(self, mock_run):
        """Test failed validation."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Error: Invalid configuration"
        )
        success, message = self.manager.validate()
        self.assertFalse(success)

    @patch('subprocess.run')
    def test_fmt_success(self, mock_run):
        """Test successful formatting."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        success, message = self.manager.fmt()
        self.assertTrue(success)

    @patch('subprocess.run')
    def test_fmt_check(self, mock_run):
        """Test format check mode."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        self.manager.fmt(check=True)
        args = mock_run.call_args[0][0]
        self.assertIn("-check", args)

    @patch('subprocess.run')
    def test_plan_success(self, mock_run):
        """Test successful plan generation."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Plan: +1 ~0 -0",
            stderr=""
        )
        result = self.manager.plan()
        self.assertTrue(result.success)
        self.assertIsNotNone(result.resource_counts)

    @patch('subprocess.run')
    def test_plan_with_out_file(self, mock_run):
        """Test plan with output file."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        self.manager.plan(out_file="/path/to/plan.tfplan")
        args = mock_run.call_args[0][0]
        self.assertIn("-out", args)
        self.assertIn("/path/to/plan.tfplan", args)

    @patch('subprocess.run')
    def test_plan_destroy(self, mock_run):
        """Test destroy plan generation."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        self.manager.plan(destroy=True)
        args = mock_run.call_args[0][0]
        self.assertIn("-destroy", args)

    @patch('subprocess.run')
    def test_plan_with_variables(self, mock_run):
        """Test plan with override variables."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        self.manager.plan(variables={"count": 5})
        args = mock_run.call_args[0][0]
        self.assertIn("-var", args)
        self.assertIn("count=5", args)

    def test_parse_plan_changes(self):
        """Test parsing plan change output."""
        output = """
  # aws_instance.web_01 will be created
  # aws_security_group.sg_01 will be updated
  # aws_instance.web_02 will be destroyed
        """
        changes = self.manager._parse_plan_changes(output)
        self.assertEqual(len(changes), 3)
        self.assertEqual(changes[0].action, "created")
        self.assertEqual(changes[1].action, "updated")
        self.assertEqual(changes[2].action, "destroyed")

    def test_count_resources(self):
        """Test counting resources by action type."""
        changes = [
            ResourceChange("a", "created", "t", "n"),
            ResourceChange("b", "created", "t", "n"),
            ResourceChange("c", "updated", "t", "n"),
            ResourceChange("d", "destroyed", "t", "n"),
        ]
        counts = self.manager._count_resources(changes)
        self.assertEqual(counts["add"], 2)
        self.assertEqual(counts["change"], 1)
        self.assertEqual(counts["destroy"], 1)

    @patch('subprocess.run')
    def test_apply_success(self, mock_run):
        """Test successful apply."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Apply complete! Resources: 3 resources",
            stderr=""
        )
        result = self.manager.apply()
        self.assertTrue(result.success)

    @patch('subprocess.run')
    def test_apply_with_plan_file(self, mock_run):
        """Test apply with plan file."""
        mock_run.return_value = MagicMock(returncode=0, stdout="Apply complete!", stderr="")
        self.manager.apply(plan_file="/path/to/plan.tfplan")
        args = mock_run.call_args[0][0]
        self.assertIn("/path/to/plan.tfplan", args)

    @patch('subprocess.run')
    def test_apply_auto_approve(self, mock_run):
        """Test apply with auto-approve."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        self.manager.apply(auto_approve=True)
        args = mock_run.call_args[0][0]
        self.assertIn("-auto-approve", args)

    @patch('subprocess.run')
    def test_apply_no_refresh(self, mock_run):
        """Test apply without refresh."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        self.manager.apply(refresh=False)
        args = mock_run.call_args[0][0]
        self.assertIn("-refresh=false", args)

    @patch('subprocess.run')
    def test_destroy_success(self, mock_run):
        """Test successful destroy."""
        mock_run.return_value = MagicMock(returncode=0, stdout="Destroy complete!", stderr="")
        result = self.manager.destroy()
        self.assertTrue(result.success)

    @patch('subprocess.run')
    def test_destroy_with_variables(self, mock_run):
        """Test destroy with variables."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        self.manager.destroy(variables={"force": True})
        args = mock_run.call_args[0][0]
        self.assertTrue(any("-var" in arg for arg in args))

    @patch('subprocess.run')
    def test_state_list(self, mock_run):
        """Test listing state resources."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="aws_instance.web_01\naws_security_group.sg_01",
            stderr=""
        )
        resources = self.manager.state_list()
        self.assertEqual(len(resources), 2)
        self.assertEqual(resources[0], "aws_instance.web_01")

    @patch('subprocess.run')
    def test_state_list_filtered(self, mock_run):
        """Test listing state with filter."""
        mock_run.return_value = MagicMock(returncode=0, stdout="aws_instance.web_01", stderr="")
        self.manager.state_list(resource_address="aws_instance.web_01")
        args = mock_run.call_args[0][0]
        self.assertIn("aws_instance.web_01", args)

    @patch('subprocess.run')
    def test_state_pull(self, mock_run):
        """Test pulling state."""
        mock_run.return_value = MagicMock(returncode=0, stdout='{"resources": []}', stderr="")
        state = self.manager.state_pull()
        self.assertEqual(state, '{"resources": []}')

    @patch('subprocess.Popen')
    def test_state_push(self, mock_popen):
        """Test pushing state."""
        mock_popen.return_value = MagicMock(
            returncode=0,
            communicate=MagicMock(return_value=("", "")),
            poll=MagicMock(return_value=0)
        )
        result = self.manager.state_push('{"resources": []}')
        self.assertTrue(result)

    @patch('subprocess.run')
    def test_state_mv(self, mock_run):
        """Test moving state resource."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        result = self.manager.state_mv("aws_instance.old", "aws_instance.new")
        self.assertTrue(result)

    @patch('subprocess.run')
    def test_state_mv_dry_run(self, mock_run):
        """Test dry run state move."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        self.manager.state_mv("aws_instance.old", "aws_instance.new", dry_run=True)
        args = mock_run.call_args[0][0]
        self.assertIn("-dry-run", args)

    @patch('subprocess.run')
    def test_state_rm(self, mock_run):
        """Test removing resource from state."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        result = self.manager.state_rm("aws_instance.deleted")
        self.assertTrue(result)

    @patch('subprocess.run')
    def test_state_show(self, mock_run):
        """Test showing resource state."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="id: i-12345\ninstance_type: t2.micro",
            stderr=""
        )
        details = self.manager.state_show("aws_instance.web_01")
        self.assertIn("id:", details)

    @patch('builtins.open', new_callable=mock_open)
    @patch('subprocess.run')
    def test_state_backup(self, mock_run, mock_file):
        """Test state backup."""
        mock_run.return_value = MagicMock(returncode=0, stdout="state content", stderr="")
        result = self.manager.state_backup("/backup/terraform.tfstate.backup")
        self.assertTrue(result)
        mock_file.assert_called_once()

    def test_set_variable(self):
        """Test setting a variable."""
        var = self.manager.set_variable("count", 5)
        self.assertEqual(var.name, "count")
        self.assertEqual(self.manager.default_variables["count"], 5)

    def test_set_variable_sensitive(self):
        """Test setting a sensitive variable."""
        var = self.manager.set_variable("password", "secret", sensitive=True)
        self.assertTrue(var.sensitive)

    def test_set_variables(self):
        """Test setting multiple variables."""
        variables = {"a": 1, "b": 2}
        result = self.manager.set_variables(variables)
        self.assertEqual(len(result), 2)

    @patch('builtins.open', new_callable=mock_open, read_data='key = "value"\ncount = 42\n')
    def test_write_variables_file(self, mock_file):
        """Test writing variables file."""
        result = self.manager.write_variables_file({"key": "value", "count": 42})
        self.assertEqual(result, "/test/dir/terraform.tfvars")
        mock_file.assert_called()

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data='key = "value"\ncount = 42\n')
    def test_read_variables_file(self, mock_file, mock_exists):
        """Test reading variables file."""
        mock_exists.return_value = True
        variables = self.manager.read_variables_file()
        self.assertEqual(variables.get("key"), "value")
        self.assertEqual(variables.get("count"), 42)

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_read_variables_file_not_found(self, mock_file, mock_exists):
        """Test reading non-existent variables file."""
        mock_exists.return_value = False
        variables = self.manager.read_variables_file("missing.tfvars")
        self.assertEqual(variables, {})

    def test_parse_tfvar_value_string(self):
        """Test parsing string values."""
        self.assertEqual(self.manager._parse_tfvar_value('"string"'), "string")
        self.assertEqual(self.manager._parse_tfvar_value("'string'"), "string")

    def test_parse_tfvar_value_bool(self):
        """Test parsing boolean values."""
        self.assertTrue(self.manager._parse_tfvar_value("true"))
        self.assertTrue(self.manager._parse_tfvar_value("True"))
        self.assertFalse(self.manager._parse_tfvar_value("false"))

    def test_parse_tfvar_value_null(self):
        """Test parsing null values."""
        self.assertIsNone(self.manager._parse_tfvar_value("null"))

    def test_parse_tfvar_value_int(self):
        """Test parsing integer values."""
        self.assertEqual(self.manager._parse_tfvar_value("42"), 42)

    def test_parse_tfvar_value_float(self):
        """Test parsing float values."""
        self.assertEqual(self.manager._parse_tfvar_value("3.14"), 3.14)

    def test_parse_tfvar_value_list(self):
        """Test parsing list values."""
        result = self.manager._parse_tfvar_value('["a", "b", "c"]')
        self.assertEqual(result, ["a", "b", "c"])

    @patch('subprocess.run')
    def test_output_json(self, mock_run):
        """Test getting JSON output."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "instance_ip": {"value": "192.168.1.1", "type": "string"}
            }),
            stderr=""
        )
        outputs = self.manager.output()
        self.assertIn("instance_ip", outputs)

    @patch('subprocess.run')
    def test_output_specific(self, mock_run):
        """Test getting specific output."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"value": "192.168.1.1", "type": "string"}),
            stderr=""
        )
        outputs = self.manager.output(name="instance_ip")
        self.assertIn("instance_ip", outputs)

    @patch('subprocess.run')
    def test_output_failure(self, mock_run):
        """Test output command failure."""
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="Error")
        outputs = self.manager.output()
        self.assertEqual(outputs, {})

    @patch('subprocess.run')
    def test_output_raw(self, mock_run):
        """Test getting raw output."""
        mock_run.return_value = MagicMock(returncode=0, stdout="192.168.1.1", stderr="")
        output = self.manager.output_raw("instance_ip")
        self.assertEqual(output, "192.168.1.1")


class TestTerraformManagerFileOperations(unittest.TestCase):
    """Tests for TerraformManager file operations with mocks."""

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_create_backend_config_file(self, mock_file, mock_exists):
        """Test creating backend config file."""
        mock_exists.return_value = False
        manager = TerraformManager(working_dir="/test")
        backend = BackendConfig(BackendType.S3, {"bucket": "my-bucket"})
        config_file = manager._create_backend_config_file(backend)
        self.assertEqual(config_file, "/test/.terraform_backend_config")
        mock_file.assert_called_once()

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.exists')
    def test_state_backup_creates_file(self, mock_exists, mock_file):
        """Test state backup creates file."""
        mock_exists.return_value = True
        mock_file.return_value = MagicMock()
        manager = TerraformManager(working_dir="/test")

        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="state content", stderr="")
            result = manager.state_backup("backup.tfstate")

        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()

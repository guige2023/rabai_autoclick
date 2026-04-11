"""
Tests for the workflow import/export system.
"""

import sys
import unittest
import json
import tempfile
import os
import tarfile
from unittest.mock import patch, MagicMock, Mock
from datetime import datetime

sys.path.insert(0, '/Users/guige/my_project')

from src.workflow_import_export import (
    WorkflowImportExport,
    Workflow,
    WorkflowStep,
    ExportFormat,
    ImportSource,
    ValidationLevel,
    BundleManifest,
    ExportMetadata,
    ImportValidationResult
)


def create_sample_workflow(version="24.0.0"):
    """Helper to create a sample workflow for testing."""
    step1 = WorkflowStep(
        step_id="step_1",
        action="click",
        params={"x": 100, "y": 200, "button": "left"},
        conditions=[],
        timeout=30.0,
        retry=0
    )
    step2 = WorkflowStep(
        step_id="step_2",
        action="type",
        params={"text": "hello world"},
        conditions=[{"field": "x", "operator": ">", "value": 50}],
        timeout=10.0,
        retry=2
    )
    return Workflow(
        workflow_id="wf_test_123",
        name="Test Workflow",
        description="A test workflow for unit testing",
        version=version,
        steps=[step1, step2],
        triggers=[{"type": "schedule", "cron": "0 * * * *"}],
        settings={"timeout": 300, "retry_on_failure": True},
        created_at=1700000000.0,
        updated_at=1700003600.0,
        metadata={"tags": ["test", "automation"], "author": "tester"}
    )


class TestWorkflowDataClasses(unittest.TestCase):
    """Tests for Workflow and WorkflowStep dataclasses."""

    def test_workflow_to_dict(self):
        """Test Workflow.to_dict()."""
        workflow = create_sample_workflow()
        data = workflow.to_dict()

        self.assertEqual(data["workflow_id"], "wf_test_123")
        self.assertEqual(data["name"], "Test Workflow")
        self.assertEqual(data["version"], "24.0.0")
        self.assertEqual(len(data["steps"]), 2)
        self.assertIsInstance(data["steps"][0], dict)

    def test_workflow_from_dict(self):
        """Test Workflow.from_dict()."""
        data = {
            "workflow_id": "wf_456",
            "name": "From Dict Workflow",
            "description": "Created from dict",
            "version": "24.0.0",
            "steps": [
                {"step_id": "s1", "action": "click", "params": {}, "conditions": [], "timeout": 30.0, "retry": 0}
            ],
            "triggers": [],
            "settings": {},
            "created_at": 1700000000.0,
            "updated_at": 1700000000.0,
            "metadata": {}
        }
        workflow = Workflow.from_dict(data)

        self.assertEqual(workflow.workflow_id, "wf_456")
        self.assertEqual(workflow.name, "From Dict Workflow")
        self.assertEqual(len(workflow.steps), 1)
        self.assertIsInstance(workflow.steps[0], WorkflowStep)

    def test_workflow_roundtrip(self):
        """Test Workflow to_dict -> from_dict roundtrip."""
        original = create_sample_workflow()
        data = original.to_dict()
        restored = Workflow.from_dict(data)

        self.assertEqual(original.workflow_id, restored.workflow_id)
        self.assertEqual(original.name, restored.name)
        self.assertEqual(original.version, restored.version)
        self.assertEqual(len(original.steps), len(restored.steps))


class TestJSONImportExport(unittest.TestCase):
    """Tests for JSON import/export functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = WorkflowImportExport(data_dir=tempfile.mkdtemp())
        self.workflow = create_sample_workflow()

    def test_export_to_json_basic(self):
        """Test basic JSON export."""
        json_str = self.exporter.export_to_json(self.workflow, pretty=False)

        self.assertIsInstance(json_str, str)
        data = json.loads(json_str)
        self.assertEqual(data["workflow_id"], "wf_test_123")
        self.assertIn("_metadata", data)

    def test_export_to_json_pretty(self):
        """Test pretty-printed JSON export."""
        json_str = self.exporter.export_to_json(self.workflow, pretty=True)

        self.assertIn("\n", json_str)
        self.assertIn("  ", json_str)  # Indentation spaces

    def test_export_to_json_contains_metadata(self):
        """Test that exported JSON contains proper metadata."""
        json_str = self.exporter.export_to_json(self.workflow)
        data = json.loads(json_str)

        self.assertIn("_metadata", data)
        self.assertEqual(data["_metadata"]["format"], "json")
        self.assertEqual(data["_metadata"]["version"], "24.0.0")
        self.assertIn("checksum", data["_metadata"])

    def test_import_from_json_valid(self):
        """Test importing valid JSON."""
        json_str = self.exporter.export_to_json(self.workflow)
        imported = self.exporter.import_from_json(json_str)

        self.assertEqual(imported.workflow_id, self.workflow.workflow_id)
        self.assertEqual(imported.name, self.workflow.name)
        self.assertEqual(len(imported.steps), len(self.workflow.steps))

    def test_import_from_json_roundtrip(self):
        """Test JSON roundtrip: export -> import."""
        original = create_sample_workflow()
        json_str = self.exporter.export_to_json(original)
        imported = self.exporter.import_from_json(json_str)

        self.assertEqual(original.workflow_id, imported.workflow_id)
        self.assertEqual(original.name, imported.name)
        for i, step in enumerate(original.steps):
            self.assertEqual(step.step_id, imported.steps[i].step_id)
            self.assertEqual(step.action, imported.steps[i].action)

    def test_import_from_json_invalid_raises(self):
        """Test that invalid JSON raises ValueError."""
        with self.assertRaises(ValueError):
            self.exporter.import_from_json("not valid json {")

    def test_import_from_json_invalid_structure_raises(self):
        """Test that invalid structure raises ValueError when validate=True."""
        invalid_json = json.dumps({"name": "Missing workflow_id"})
        with self.assertRaises(ValueError):
            self.exporter.import_from_json(invalid_json, validate=True)

    def test_import_from_json_skips_metadata(self):
        """Test that _metadata is stripped on import."""
        json_str = self.exporter.export_to_json(self.workflow)
        imported = self.exporter.import_from_json(json_str)

        data = json.loads(json_str)
        self.assertIn("_metadata", data)
        self.assertNotIn("_metadata", imported.to_dict())


class TestYAMLImportExport(unittest.TestCase):
    """Tests for YAML import/export functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = WorkflowImportExport(data_dir=tempfile.mkdtemp())
        self.workflow = create_sample_workflow()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temp files."""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @patch('src.workflow_import_export.yaml')
    def test_export_to_yaml(self, mock_yaml):
        """Test YAML export."""
        yaml_path = os.path.join(self.temp_dir, "test.yaml")

        self.exporter.export_to_yaml(self.workflow, yaml_path)

        mock_yaml.dump.assert_called_once()
        self.assertTrue(os.path.exists(yaml_path))

    @patch('src.workflow_import_export.yaml')
    def test_import_from_yaml(self, mock_yaml):
        """Test YAML import."""
        yaml_path = os.path.join(self.temp_dir, "test.yaml")

        # Mock yaml.safe_load to return valid workflow data
        mock_yaml.safe_load.return_value = {
            "workflow_id": "wf_yaml_test",
            "name": "YAML Workflow",
            "description": "",
            "version": "24.0.0",
            "steps": [{"step_id": "s1", "action": "click", "params": {}, "conditions": [], "timeout": 30.0, "retry": 0}],
            "triggers": [],
            "settings": {},
            "metadata": {}
        }

        imported = self.exporter.import_from_yaml(yaml_path)

        self.assertEqual(imported.workflow_id, "wf_yaml_test")
        mock_yaml.safe_load.assert_called_once()

    @patch('src.workflow_import_export.yaml')
    def test_import_from_yaml_validates(self, mock_yaml):
        """Test YAML import validates by default."""
        yaml_path = os.path.join(self.temp_dir, "test.yaml")

        mock_yaml.safe_load.return_value = {"name": "Missing required fields"}

        with self.assertRaises(ValueError):
            self.exporter.import_from_yaml(yaml_path, validate=True)


class TestBinaryExportImport(unittest.TestCase):
    """Tests for binary (.rabai) export/import functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = WorkflowImportExport(data_dir=tempfile.mkdtemp())
        self.workflow = create_sample_workflow()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temp files."""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_export_to_binary_creates_file(self):
        """Test binary export creates a file."""
        output_path = os.path.join(self.temp_dir, "test.rabai")

        self.exporter.export_to_binary(self.workflow, output_path)

        self.assertTrue(os.path.exists(output_path))
        with open(output_path, 'rb') as f:
            magic = f.read(4)
            self.assertEqual(magic, b'RBAI')

    def test_export_to_binary_includes_signature(self):
        """Test binary export includes signature."""
        output_path = os.path.join(self.temp_dir, "test.rabai")

        self.exporter.export_to_binary(self.workflow, output_path)

        with open(output_path, 'rb') as f:
            f.read(4)  # magic
            f.read(2)  # version
            f.read(1)  # flags
            checksum = f.read(32)  # checksum
            signature = f.read(44)  # signature

        self.assertTrue(len(checksum) > 0)
        self.assertTrue(len(signature.strip(b'\0')) > 0)

    def test_import_from_binary_valid_roundtrip(self):
        """Test binary import/export roundtrip."""
        output_path = os.path.join(self.temp_dir, "test.rabai")

        self.exporter.export_to_binary(self.workflow, output_path)
        imported = self.exporter.import_from_binary(output_path)

        self.assertEqual(imported.workflow_id, self.workflow.workflow_id)
        self.assertEqual(imported.name, self.workflow.name)
        self.assertEqual(len(imported.steps), len(self.workflow.steps))

    def test_import_from_binary_invalid_magic_raises(self):
        """Test that invalid magic number raises ValueError."""
        output_path = os.path.join(self.temp_dir, "invalid.rabai")

        with open(output_path, 'wb') as f:
            f.write(b'XXXX')  # Invalid magic

        with self.assertRaises(ValueError) as ctx:
            self.exporter.import_from_binary(output_path)
        self.assertIn("bad magic", str(ctx.exception))

    def test_import_from_binary_checksum_mismatch_raises(self):
        """Test that checksum mismatch raises ValueError."""
        output_path = os.path.join(self.temp_dir, "corrupted.rabai")

        # Create a valid file then corrupt it
        self.exporter.export_to_binary(self.workflow, output_path)
        
        # Corrupt the data after checksum
        with open(output_path, 'r+b') as f:
            f.seek(0)
            f.write(b'RBAI')  # Keep magic
            f.seek(0, 2)
            f.write(b'CORRUPTED')  # Corrupt data at end

        with self.assertRaises(ValueError) as ctx:
            self.exporter.import_from_binary(output_path)
        self.assertIn("Checksum mismatch", str(ctx.exception))

    def test_import_from_binary_signature_verification(self):
        """Test signature verification on import."""
        output_path = os.path.join(self.temp_dir, "test.rabai")

        self.exporter.export_to_binary(self.workflow, output_path)
        
        # Modify signature to cause verification failure
        with open(output_path, 'r+b') as f:
            f.seek(39)  # Position after magic(4) + version(2) + flags(1) + checksum(32)
            f.write(b'XXXX' + b'\x00' * 40)  # Corrupt signature

        with self.assertRaises(ValueError) as ctx:
            self.exporter.import_from_binary(output_path)
        self.assertIn("Signature verification failed", str(ctx.exception))

    def test_export_to_binary_with_encryption(self):
        """Test binary export with encryption."""
        output_path = os.path.join(self.temp_dir, "encrypted.rabai")
        password = "test_password_123"

        self.exporter.export_to_binary(self.workflow, output_path, encrypt_password=password)

        self.assertTrue(os.path.exists(output_path))
        
        # Check flags byte indicates encryption
        with open(output_path, 'rb') as f:
            f.read(4)  # magic
            f.read(2)  # version
            flags = f.read(1)
            self.assertEqual(flags[0] & 1, 1)  # Encryption flag set

    def test_import_binary_encrypted_requires_password(self):
        """Test that importing encrypted binary requires password."""
        output_path = os.path.join(self.temp_dir, "encrypted.rabai")
        password = "test_password_123"

        self.exporter.export_to_binary(self.workflow, output_path, encrypt_password=password)

        # Without password should raise
        with self.assertRaises(ValueError) as ctx:
            self.exporter.import_from_binary(output_path)
        self.assertIn("encrypted", str(ctx.exception).lower())

    def test_binary_roundtrip_with_encryption(self):
        """Test binary export/import roundtrip with encryption."""
        output_path = os.path.join(self.temp_dir, "encrypted.rabai")
        password = "secure_password"

        self.exporter.export_to_binary(self.workflow, output_path, encrypt_password=password)
        imported = self.exporter.import_from_binary(output_path, decrypt_password=password)

        self.assertEqual(imported.workflow_id, self.workflow.workflow_id)
        self.assertEqual(imported.name, self.workflow.name)


class TestEncryptionDecryption(unittest.TestCase):
    """Tests for encryption/decryption functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = WorkflowImportExport(data_dir=tempfile.mkdtemp())
        self.workflow = create_sample_workflow()

    def test_encrypt_workflow_returns_bytes(self):
        """Test encrypt_workflow returns bytes."""
        password = "test_password"
        encrypted = self.exporter.encrypt_workflow(self.workflow, password)

        self.assertIsInstance(encrypted, bytes)
        self.assertNotEqual(encrypted, b'')

    def test_decrypt_workflow_restores_original(self):
        """Test decrypt_workflow restores original workflow."""
        password = "test_password"
        encrypted = self.exporter.encrypt_workflow(self.workflow, password)
        decrypted = self.exporter.decrypt_workflow(encrypted, password)

        self.assertEqual(decrypted.workflow_id, self.workflow.workflow_id)
        self.assertEqual(decrypted.name, self.workflow.name)

    def test_decrypt_with_wrong_password_raises(self):
        """Test that decryption with wrong password fails."""
        password = "test_password"
        encrypted = self.exporter.encrypt_workflow(self.workflow, password)

        with self.assertRaises(Exception):  # Fernet decrypt will raise
            self.exporter.decrypt_workflow(encrypted, "wrong_password")

    def test_encrypted_data_is_different_from_plaintext(self):
        """Test that encrypted data differs from plaintext."""
        password = "test_password"
        json_bytes = json.dumps(self.workflow.to_dict(), ensure_ascii=False).encode()
        encrypted = self.exporter.encrypt_workflow(self.workflow, password)

        self.assertNotEqual(encrypted, json_bytes)


class TestDigitalSignature(unittest.TestCase):
    """Tests for digital signature sign/verify functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = WorkflowImportExport(data_dir=tempfile.mkdtemp())
        self.workflow = create_sample_workflow()

    def test_sign_workflow_returns_dict(self):
        """Test sign_workflow returns a dictionary."""
        signed = self.exporter.sign_workflow(self.workflow)

        self.assertIsInstance(signed, dict)
        self.assertIn("workflow", signed)
        self.assertIn("signature", signed)
        self.assertIn("checksum", signed)
        self.assertIn("signed_at", signed)
        self.assertIn("version", signed)

    def test_verify_signed_workflow_valid(self):
        """Test verify_signed_workflow with valid signature."""
        signed = self.exporter.sign_workflow(self.workflow)
        result = self.exporter.verify_signed_workflow(signed)

        self.assertTrue(result)

    def test_verify_signed_workflow_tampered_raises(self):
        """Test that verification fails when data is tampered."""
        signed = self.exporter.sign_workflow(self.workflow)
        signed["workflow"]["name"] = "Tampered Name"
        result = self.exporter.verify_signed_workflow(signed)

        self.assertFalse(result)

    def test_verify_signed_workflow_wrong_signature_raises(self):
        """Test that verification fails with wrong signature."""
        signed = self.exporter.sign_workflow(self.workflow)
        signed["signature"] = "invalid_signature"
        result = self.exporter.verify_signed_workflow(signed)

        self.assertFalse(result)

    def test_signature_changes_with_workflow_content(self):
        """Test that signature changes when workflow content changes."""
        signed1 = self.exporter.sign_workflow(self.workflow)
        
        self.workflow.name = "Modified Name"
        signed2 = self.exporter.sign_workflow(self.workflow)

        self.assertNotEqual(signed1["signature"], signed2["signature"])


class TestVersionMigration(unittest.TestCase):
    """Tests for version migration functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = WorkflowImportExport(data_dir=tempfile.mkdtemp())

    def test_migrate_v2_to_v3_adds_settings(self):
        """Test v2 to v3 migration adds settings."""
        data = {
            "workflow_id": "wf_old",
            "name": "Old Workflow",
            "description": "",
            "version": "2.0.0",
            "steps": [],
            "triggers": [],
            "settings": {}
        }
        
        migrated = self.exporter._migrate_v2_to_v3(data)
        
        self.assertIn("settings", migrated)
        self.assertIn("timeout", migrated["settings"])

    def test_migrate_v20_to_v21_adds_metadata(self):
        """Test v20 to v21 migration adds metadata."""
        data = {
            "workflow_id": "wf_old",
            "name": "Old Workflow",
            "description": "",
            "version": "20.0.0",
            "steps": [],
            "triggers": [],
            "settings": {},
            "metadata": {}
        }
        
        migrated = self.exporter._migrate_v20_to_v21(data)
        
        self.assertTrue(migrated["metadata"].get("migrated_from_v20"))

    def test_migrate_v21_to_v22_adds_triggers(self):
        """Test v21 to v22 migration adds triggers."""
        data = {
            "workflow_id": "wf_old",
            "name": "Old Workflow",
            "description": "",
            "version": "21.0.0",
            "steps": [],
            "triggers": None,
            "settings": {},
            "metadata": {}
        }
        
        migrated = self.exporter._migrate_v21_to_v22(data)
        
        self.assertIsInstance(migrated["triggers"], list)

    def test_migrate_v23_to_v24_adds_step_defaults(self):
        """Test v23 to v24 migration adds step defaults."""
        data = {
            "workflow_id": "wf_old",
            "name": "Old Workflow",
            "description": "",
            "version": "23.0.0",
            "steps": [
                {"step_id": "s1", "action": "click", "params": {}},
                {"step_id": "s2", "action": "type", "params": {}, "timeout": 60.0}
            ],
            "triggers": [],
            "settings": {},
            "metadata": {}
        }
        
        migrated = self.exporter._migrate_v23_to_v24(data)
        
        # First step should get defaults
        self.assertEqual(migrated["steps"][0]["timeout"], 30.0)
        self.assertEqual(migrated["steps"][0]["retry"], 0)
        # Second step should keep its timeout
        self.assertEqual(migrated["steps"][1]["timeout"], 60.0)

    def test_migrate_workflow_data_no_migration_needed(self):
        """Test migration when already at current version."""
        data = {
            "workflow_id": "wf_current",
            "name": "Current Workflow",
            "description": "",
            "version": "24.0.0",
            "steps": [],
            "triggers": [],
            "settings": {},
            "metadata": {}
        }
        
        migrated = self.exporter._migrate_workflow_data(data.copy())
        
        self.assertEqual(migrated["version"], "24.0.0")
        self.assertNotIn("_migration_steps", migrated)

    def test_migrate_workflow_data_needs_migration(self):
        """Test migration from older version."""
        data = {
            "workflow_id": "wf_old",
            "name": "Old Workflow",
            "description": "",
            "version": "2.0.0",
            "steps": [],
            "triggers": [],
            "settings": {},
            "metadata": {}
        }
        
        migrated = self.exporter._migrate_workflow_data(data.copy())
        
        self.assertEqual(migrated["version"], "24.0.0")
        self.assertIn("_migration_steps", migrated)
        self.assertTrue(len(migrated["_migration_steps"]) > 0)

    def test_auto_migrate_returns_steps(self):
        """Test auto_migrate returns migration steps."""
        data = {
            "workflow_id": "wf_old",
            "name": "Old Workflow",
            "description": "",
            "version": "20.0.0",
            "steps": [],
            "triggers": [],
            "settings": {},
            "metadata": {}
        }
        
        migrated, steps = self.exporter.auto_migrate(data)
        
        self.assertIsInstance(steps, list)
        self.assertTrue(len(steps) > 0)


class TestPartialExport(unittest.TestCase):
    """Tests for partial export functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = WorkflowImportExport(data_dir=tempfile.mkdtemp())
        self.workflow = create_sample_workflow()

    def test_export_partial_with_valid_steps(self):
        """Test partial export with valid step IDs."""
        partial = self.exporter.export_partial(self.workflow, ["step_1"])

        self.assertEqual(len(partial.steps), 1)
        self.assertEqual(partial.steps[0].step_id, "step_1")
        self.assertIn("partial_export", partial.metadata)
        self.assertIn("(Partial)", partial.name)

    def test_export_partial_with_multiple_steps(self):
        """Test partial export with multiple step IDs."""
        partial = self.exporter.export_partial(self.workflow, ["step_1", "step_2"])

        self.assertEqual(len(partial.steps), 2)

    def test_export_partial_empty_step_ids_raises(self):
        """Test that empty step_ids raises ValueError."""
        with self.assertRaises(ValueError):
            self.exporter.export_partial(self.workflow, [])

    def test_export_partial_nonexistent_step(self):
        """Test partial export with non-existent step ID."""
        partial = self.exporter.export_partial(self.workflow, ["step_999"])

        self.assertEqual(len(partial.steps), 0)

    def test_export_partial_preserves_workflow_id_suffix(self):
        """Test that partial export adds suffix to workflow ID."""
        partial = self.exporter.export_partial(self.workflow, ["step_1"])

        self.assertEqual(partial.workflow_id, self.workflow.workflow_id + "_partial")

    def test_export_partial_with_conditions(self):
        """Test partial export with conditions."""
        partial = self.exporter.export_partial(self.workflow, ["step_2"], include_conditions=True)

        self.assertEqual(len(partial.triggers), len(self.workflow.triggers))

    def test_export_partial_without_conditions(self):
        """Test partial export without conditions."""
        partial = self.exporter.export_partial(self.workflow, ["step_2"], include_conditions=False)

        self.assertEqual(len(partial.triggers), 0)

    def test_export_steps_range_valid(self):
        """Test export_steps_range with valid indices."""
        partial = self.exporter.export_steps_range(self.workflow, 0, 1)

        self.assertEqual(len(partial.steps), 2)

    def test_export_steps_range_invalid_index_raises(self):
        """Test export_steps_range with invalid index raises."""
        with self.assertRaises(ValueError):
            self.exporter.export_steps_range(self.workflow, -1, 1)

    def test_export_steps_range_out_of_bounds_raises(self):
        """Test export_steps_range with out of bounds index raises."""
        with self.assertRaises(ValueError):
            self.exporter.export_steps_range(self.workflow, 0, 999)


class TestBatchExport(unittest.TestCase):
    """Tests for batch export functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = WorkflowImportExport(data_dir=tempfile.mkdtemp())
        self.temp_dir = tempfile.mkdtemp()
        
        # Add multiple workflows
        wf1 = create_sample_workflow()
        wf1.workflow_id = "wf_1"
        wf1.name = "Workflow One"
        wf2 = create_sample_workflow()
        wf2.workflow_id = "wf_2"
        wf2.name = "Workflow Two"
        
        self.exporter.save_workflow(wf1)
        self.exporter.save_workflow(wf2)

    def tearDown(self):
        """Clean up temp files."""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_batch_export_json_creates_files(self):
        """Test batch export to JSON creates files."""
        results = self.exporter.batch_export(self.temp_dir, ExportFormat.JSON)

        self.assertEqual(len(results), 2)
        for filepath in results.values():
            self.assertTrue(filepath.endswith('.json'))
            self.assertTrue(os.path.exists(filepath))

    @patch('src.workflow_import_export.yaml')
    def test_batch_export_yaml_creates_files(self, mock_yaml):
        """Test batch export to YAML creates files."""
        results = self.exporter.batch_export(self.temp_dir, ExportFormat.YAML)

        self.assertEqual(len(results), 2)
        for filepath in results.values():
            self.assertTrue(filepath.endswith('.yaml'))

    def test_batch_export_binary_creates_files(self):
        """Test batch export to BINARY creates files."""
        results = self.exporter.batch_export(self.temp_dir, ExportFormat.BINARY)

        self.assertEqual(len(results), 2)
        for filepath in results.values():
            self.assertTrue(filepath.endswith('.rabai'))
            self.assertTrue(os.path.exists(filepath))

    def test_batch_export_with_encryption(self):
        """Test batch export with encryption."""
        results = self.exporter.batch_export(
            self.temp_dir, 
            ExportFormat.BINARY,
            encrypt_password="password"
        )

        self.assertEqual(len(results), 2)

    def test_batch_export_by_tags(self):
        """Test batch export filtered by tags."""
        # Add a workflow without the tag
        wf3 = create_sample_workflow()
        wf3.workflow_id = "wf_3"
        wf3.name = "No Tag Workflow"
        wf3.metadata = {"tags": ["other"]}
        self.exporter.save_workflow(wf3)

        results = self.exporter.batch_export_by_tags(["test"], self.temp_dir)

        self.assertEqual(len(results), 2)


class TestBundleCreationExtraction(unittest.TestCase):
    """Tests for workflow bundle creation and extraction."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = WorkflowImportExport(data_dir=tempfile.mkdtemp())
        self.temp_dir = tempfile.mkdtemp()
        
        # Add workflows
        wf1 = create_sample_workflow()
        wf1.workflow_id = "wf_bundle_1"
        wf1.name = "Bundle Workflow 1"
        wf2 = create_sample_workflow()
        wf2.workflow_id = "wf_bundle_2"
        wf2.name = "Bundle Workflow 2"
        
        self.exporter.save_workflow(wf1)
        self.exporter.save_workflow(wf2)

    def tearDown(self):
        """Clean up temp files."""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_create_bundle_manifest(self):
        """Test creating a bundle manifest."""
        manifest = self.exporter.create_bundle(
            ["wf_bundle_1", "wf_bundle_2"],
            "Test Bundle"
        )

        self.assertEqual(manifest.name, "Test Bundle")
        self.assertEqual(len(manifest.workflows), 2)
        self.assertIsNotNone(manifest.bundle_id)

    def test_create_bundle_with_assets(self):
        """Test creating a bundle with assets."""
        # Create a temp asset file
        asset_path = os.path.join(self.temp_dir, "test_asset.txt")
        with open(asset_path, 'w') as f:
            f.write("test content")
        
        manifest = self.exporter.create_bundle(
            ["wf_bundle_1"],
            "Bundle With Assets",
            assets={"asset1": asset_path}
        )

        self.assertEqual(len(manifest.assets), 1)
        self.assertIn("asset1", manifest.assets)

    def test_export_bundle_creates_tar(self):
        """Test exporting a bundle to tar.gz."""
        manifest = self.exporter.create_bundle(
            ["wf_bundle_1", "wf_bundle_2"],
            "Test Bundle"
        )
        output_path = os.path.join(self.temp_dir, "bundle.tar.gz")

        self.exporter.export_bundle(manifest, output_path)

        self.assertTrue(os.path.exists(output_path))
        self.assertTrue(tarfile.is_tarfile(output_path))

    def test_import_bundle_restores_workflows(self):
        """Test importing a bundle restores workflows."""
        manifest = self.exporter.create_bundle(
            ["wf_bundle_1", "wf_bundle_2"],
            "Test Bundle"
        )
        output_path = os.path.join(self.temp_dir, "bundle.tar.gz")
        self.exporter.export_bundle(manifest, output_path)

        # Clear existing workflows
        self.exporter.workflows = {}
        
        # Import
        imported = self.exporter.import_bundle(output_path)

        self.assertEqual(imported.name, "Test Bundle")
        self.assertEqual(len(self.exporter.workflows), 2)
        self.assertIn("wf_bundle_1", self.exporter.workflows)


class TestImportValidation(unittest.TestCase):
    """Tests for import validation functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = WorkflowImportExport(data_dir=tempfile.mkdtemp())

    def test_validate_import_valid_data(self):
        """Test validation with valid data."""
        data = {
            "workflow_id": "wf_valid",
            "name": "Valid Workflow",
            "description": "",
            "version": "24.0.0",
            "steps": [
                {"step_id": "s1", "action": "click", "params": {}, "conditions": [], "timeout": 30.0, "retry": 0}
            ],
            "triggers": [],
            "settings": {},
            "metadata": {}
        }

        result = self.exporter.validate_import(data, ExportFormat.JSON)

        self.assertTrue(result.valid)
        self.assertEqual(len(result.errors), 0)

    def test_validate_import_missing_required_field(self):
        """Test validation fails with missing required field."""
        data = {
            "name": "Missing workflow_id",
            "steps": []
        }

        result = self.exporter.validate_import(data, ExportFormat.JSON)

        self.assertFalse(result.valid)
        self.assertTrue(any("workflow_id" in e for e in result.errors))

    def test_validate_import_missing_steps(self):
        """Test validation fails with missing steps."""
        data = {
            "workflow_id": "wf_no_steps",
            "name": "No Steps"
        }

        result = self.exporter.validate_import(data, ExportFormat.JSON)

        self.assertFalse(result.valid)

    def test_validate_import_string_data(self):
        """Test validation with JSON string data."""
        json_str = json.dumps({
            "workflow_id": "wf_str",
            "name": "String Data",
            "steps": [{"step_id": "s1", "action": "click", "params": {}, "conditions": [], "timeout": 30.0, "retry": 0}]
        })

        result = self.exporter.validate_import(json_str, ExportFormat.JSON)

        self.assertTrue(result.valid)

    def test_validate_import_invalid_json_string(self):
        """Test validation fails with invalid JSON string."""
        result = self.exporter.validate_import("not json {", ExportFormat.JSON)

        self.assertFalse(result.valid)
        self.assertTrue(any("JSON" in e for e in result.errors))

    def test_validate_import_non_dict_list_raises(self):
        """Test validation fails with non-dict, non-string data."""
        result = self.exporter.validate_import([1, 2, 3], ExportFormat.JSON)

        self.assertFalse(result.valid)

    def test_validate_import_warns_on_old_version(self):
        """Test validation warns when migrating from old version."""
        data = {
            "workflow_id": "wf_old",
            "name": "Old Version",
            "version": "2.0.0",
            "steps": [
                {"step_id": "s1", "action": "click", "params": {}, "conditions": []}
            ],
            "triggers": [],
            "settings": {},
            "metadata": {}
        }

        result = self.exporter.validate_import(data, ExportFormat.JSON)

        self.assertTrue(result.migration_performed)
        self.assertTrue(any("Migrated" in w for w in result.warnings))

    def test_validate_import_step_missing_action(self):
        """Test validation warns on step missing action."""
        data = {
            "workflow_id": "wf_bad_step",
            "name": "Bad Step",
            "version": "24.0.0",
            "steps": [
                {"step_id": "s1", "params": {}}
            ],
            "triggers": [],
            "settings": {},
            "metadata": {}
        }

        result = self.exporter.validate_import(data, ExportFormat.JSON)

        self.assertFalse(result.valid)
        self.assertTrue(any("action" in e for e in result.errors))

    def test_validate_workflow_strict_level(self):
        """Test validate_workflow with STRICT level."""
        workflow = create_sample_workflow()
        
        result = self.exporter.validate_workflow(workflow, ValidationLevel.STRICT)

        self.assertTrue(result.valid)

    def test_validate_workflow_lenient_level(self):
        """Test validate_workflow with LENIENT level downgrades some errors."""
        data = {
            "workflow_id": "wf_test",
            "name": "Test",
            "version": "24.0.0",
            "steps": [
                {"step_id": "s1", "action": "click", "params": {}, "conditions": [], "timeout": 30.0, "retry": 0}
            ],
            "triggers": [],
            "settings": {},
            "metadata": {}
        }
        
        result = self.exporter.validate_workflow(Workflow.from_dict(data), ValidationLevel.LENIENT)

        self.assertTrue(result.valid or len(result.warnings) > 0)

    def test_validate_batch_import(self):
        """Test batch import validation."""
        items = [
            {
                "workflow_id": "wf_1",
                "name": "One",
                "steps": [{"step_id": "s1", "action": "click", "params": {}, "conditions": [], "timeout": 30.0, "retry": 0}]
            },
            {
                "workflow_id": "wf_2",
                "name": "Two",
                "steps": [{"step_id": "s2", "action": "type", "params": {}, "conditions": [], "timeout": 30.0, "retry": 0}]
            }
        ]

        results = self.exporter.validate_batch_import(items)

        self.assertEqual(len(results), 2)
        self.assertTrue(all(r.valid for r in results))


class TestCloudImport(unittest.TestCase):
    """Tests for cloud import (S3, HTTP) functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = WorkflowImportExport(data_dir=tempfile.mkdtemp())

    @patch('src.workflow_import_export.urlopen')
    def test_import_from_http_json(self, mock_urlopen):
        """Test importing from HTTP URL with JSON content."""
        workflow = create_sample_workflow()
        json_bytes = json.dumps(workflow.to_dict(), ensure_ascii=False).encode()
        
        mock_response = MagicMock()
        mock_response.__enter__.return_value.read.return_value = json_bytes
        mock_urlopen.return_value = mock_response

        imported = self.exporter.import_from_url("https://example.com/workflow.json")

        self.assertEqual(imported.workflow_id, workflow.workflow_id)
        mock_urlopen.assert_called_once()

    @patch('src.workflow_import_export.urlopen')
    def test_import_from_http_binary(self, mock_urlopen):
        """Test importing from HTTP URL with binary content."""
        workflow = create_sample_workflow()
        
        # Create minimal binary content
        import io
        buf = io.BytesIO()
        buf.write(b'RBAI')  # magic
        buf.write(struct.pack('>H', 1))  # version
        buf.write(struct.pack('B', 0))  # flags
        buf.write(b'0' * 32)  # checksum
        buf.write(b'0' * 44)  # signature
        compressed = zlib.compress(json.dumps(workflow.to_dict()).encode())
        buf.write(struct.pack('>I', len(compressed)))
        buf.write(compressed)
        binary_content = buf.getvalue()
        
        mock_response = MagicMock()
        mock_response.__enter__.return_value.read.return_value = binary_content
        mock_urlopen.return_value = mock_response

        imported = self.exporter.import_from_url("https://example.com/workflow.rabai")

        self.assertEqual(imported.workflow_id, workflow.workflow_id)

    def test_import_from_unsupported_scheme_raises(self):
        """Test importing from unsupported URL scheme raises."""
        with self.assertRaises(ValueError) as ctx:
            self.exporter.import_from_url("ftp://example.com/workflow.json")
        
        self.assertIn("Unsupported", str(ctx.exception))


class TestTemplateExtraction(unittest.TestCase):
    """Tests for template extraction functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = WorkflowImportExport(data_dir=tempfile.mkdtemp())
        self.workflow = create_sample_workflow()
        self.workflow.steps[0].params["password"] = "secret123"
        self.workflow.steps[0].params["api_key"] = "key_abc"

    def test_extract_template_creates_new_id(self):
        """Test extract_template creates new workflow ID."""
        template = self.exporter.extract_template(self.workflow, "My Template")

        self.assertNotEqual(template.workflow_id, self.workflow.workflow_id)
        self.assertTrue(template.workflow_id.startswith("template_"))

    def test_extract_template_clears_sensitive_data(self):
        """Test extract_template clears sensitive parameters."""
        template = self.exporter.extract_template(self.workflow, "My Template", clear_sensitive=True)

        for step in template.steps:
            if isinstance(step, WorkflowStep):
                self.assertEqual(step.params.get("password"), "***REDACTED***")
                self.assertEqual(step.params.get("api_key"), "***REDACTED***")

    def test_extract_template_preserves_action(self):
        """Test extract_template preserves action types."""
        template = self.exporter.extract_template(self.workflow, "My Template", clear_sensitive=False)

        for i, step in enumerate(template.steps):
            self.assertEqual(step.action, self.workflow.steps[i].action)

    def test_extract_template_sets_metadata(self):
        """Test extract_template sets template metadata."""
        template = self.exporter.extract_template(self.workflow, "My Template")

        self.assertTrue(template.metadata.get("is_template"))
        self.assertEqual(template.metadata.get("template_of"), self.workflow.workflow_id)

    def test_extract_template_clears_triggers(self):
        """Test extract_template clears triggers."""
        template = self.exporter.extract_template(self.workflow, "My Template")

        self.assertEqual(len(template.triggers), 0)

    def test_create_template_from_steps(self):
        """Test create_template_from_steps creates workflow."""
        step_defs = [
            {"action": "click", "params": {"x": 100, "y": 200}},
            {"action": "type", "params": {"text": "hello"}}
        ]

        template = self.exporter.create_template_from_steps(step_defs, "Simple Template")

        self.assertEqual(len(template.steps), 2)
        self.assertTrue(template.metadata.get("is_template"))


class TestChecksumSignature(unittest.TestCase):
    """Tests for checksum and signature utilities."""

    def setUp(self):
        """Set up test fixtures."""
        self.exporter = WorkflowImportExport(data_dir=tempfile.mkdtemp())

    def test_compute_checksum_returns_hex(self):
        """Test _compute_checksum returns hex string."""
        checksum = self.exporter._compute_checksum(b"test data")

        self.assertIsInstance(checksum, str)
        self.assertEqual(len(checksum), 64)  # SHA256 hex length

    def test_compute_checksum_deterministic(self):
        """Test _compute_checksum is deterministic."""
        checksum1 = self.exporter._compute_checksum(b"test data")
        checksum2 = self.exporter._compute_checksum(b"test data")

        self.assertEqual(checksum1, checksum2)

    def test_compute_checksum_differs_for_different_data(self):
        """Test _compute_checksum differs for different data."""
        checksum1 = self.exporter._compute_checksum(b"data1")
        checksum2 = self.exporter._compute_checksum(b"data2")

        self.assertNotEqual(checksum1, checksum2)

    def test_sign_data_returns_base64(self):
        """Test _sign_data returns base64 string."""
        signature = self.exporter._sign_data(b"test data")

        self.assertIsInstance(signature, str)
        # base64 should be valid base64 chars
        import base64
        try:
            base64.b64decode(signature)
        except Exception:
            self.fail("Signature is not valid base64")

    def test_verify_signature_valid(self):
        """Test _verify_signature with valid signature."""
        data = b"test data"
        signature = self.exporter._sign_data(data)

        result = self.exporter._verify_signature(data, signature)

        self.assertTrue(result)

    def test_verify_signature_invalid(self):
        """Test _verify_signature with invalid signature."""
        data = b"test data"

        result = self.exporter._verify_signature(data, "invalid_signature")

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()

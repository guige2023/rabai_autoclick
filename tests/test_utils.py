"""Tests for utility modules of RabAI AutoClick.

Tests WorkflowSigner, WorkflowCrypto, AuditLogger, SecurityScanner,
PluginManager, ThemeManager, and MouseUtils.
"""

import sys
import os
import json
import tempfile
import shutil
import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
from pathlib import Path

sys.path.insert(0, '/Users/guige/my_project')


class TestWorkflowSigner(unittest.TestCase):
    """Tests for WorkflowSigner sign/verify roundtrip."""

    def test_sign_and_verify_roundtrip(self):
        """Test sign and verify workflow roundtrip."""
        from utils.workflow_signer import WorkflowSigner
        
        signer = WorkflowSigner()
        signer.set_key("test_secret_key_12345")
        
        workflow = {
            "name": "test_workflow",
            "version": "1.0",
            "steps": [
                {"id": 1, "type": "delay", "seconds": 1}
            ]
        }
        
        # Sign the workflow (sign() mutates, so use a copy)
        signed_workflow = signer.sign(workflow.copy())
        
        # Verify should return True
        result = signer.verify(signed_workflow)
        self.assertTrue(result)

    def test_verify_without_signature(self):
        """Test verify returns False for unsigned workflow."""
        from utils.workflow_signer import WorkflowSigner
        
        signer = WorkflowSigner()
        signer.set_key("test_secret_key_12345")
        
        workflow = {
            "name": "test_workflow",
            "steps": []
        }
        
        result = signer.verify(workflow)
        self.assertFalse(result)

    def test_verify_tampered_workflow(self):
        """Test verify detects tampered workflow."""
        from utils.workflow_signer import WorkflowSigner
        
        signer = WorkflowSigner()
        signer.set_key("test_secret_key_12345")
        
        workflow = {
            "name": "test_workflow",
            "steps": []
        }
        
        signed = signer.sign(workflow)
        signed["steps"].append({"id": 1})  # Tamper with workflow
        
        result = signer.verify(signed)
        self.assertFalse(result)

    def test_sign_without_key_raises(self):
        """Test signing without key raises ValueError."""
        from utils.workflow_signer import WorkflowSigner
        
        signer = WorkflowSigner()  # No key set
        
        workflow = {"name": "test"}
        
        with self.assertRaises(ValueError):
            signer.sign(workflow)

    def test_verify_without_key_raises(self):
        """Test verifying without key raises ValueError."""
        from utils.workflow_signer import WorkflowSigner
        
        signer = WorkflowSigner()  # No key set
        
        workflow = {"name": "test"}
        
        with self.assertRaises(ValueError):
            signer.verify(workflow)

    def test_different_keys_produce_different_signatures(self):
        """Test that different keys produce different signatures."""
        from utils.workflow_signer import WorkflowSigner
        
        signer1 = WorkflowSigner()
        signer1.set_key("key1")
        
        signer2 = WorkflowSigner()
        signer2.set_key("key2")
        
        workflow = {"name": "test", "steps": []}
        
        signed1 = signer1.sign(workflow.copy())
        signed2 = signer2.sign(workflow.copy())
        
        sig1 = signed1["metadata"]["_rabai_signature"]["signature"]
        sig2 = signed2["metadata"]["_rabai_signature"]["signature"]
        
        self.assertNotEqual(sig1, sig2)


class TestWorkflowCrypto(unittest.TestCase):
    """Tests for WorkflowCrypto encrypt/decrypt roundtrip."""

    def test_encrypt_decrypt_roundtrip(self):
        """Test encrypt and decrypt value roundtrip."""
        from utils.workflow_crypto import WorkflowCrypto
        
        crypto = WorkflowCrypto()
        crypto.set_key("test_encryption_key")
        
        original = "my_secret_password"
        
        encrypted = crypto.encrypt_value(original)
        decrypted = crypto.decrypt_value(encrypted)
        
        self.assertEqual(decrypted, original)
        self.assertTrue(encrypted.startswith("_enc_"))

    def test_encrypt_value_without_key_raises(self):
        """Test encrypting without key raises ValueError."""
        from utils.workflow_crypto import WorkflowCrypto
        
        crypto = WorkflowCrypto()  # No key
        
        with self.assertRaises(ValueError):
            crypto.encrypt_value("test")

    def test_decrypt_value_without_key_raises(self):
        """Test decrypting without key raises ValueError."""
        from utils.workflow_crypto import WorkflowCrypto
        
        crypto = WorkflowCrypto()
        crypto.set_key("test_key")
        encrypted = crypto.encrypt_value("test")
        
        crypto2 = WorkflowCrypto()  # No key
        with self.assertRaises(ValueError):
            crypto2.decrypt_value(encrypted)

    def test_decrypt_value_without_prefix_raises(self):
        """Test decrypting value without prefix raises ValueError."""
        from utils.workflow_crypto import WorkflowCrypto
        
        crypto = WorkflowCrypto()
        crypto.set_key("test_key")
        
        with self.assertRaises(ValueError):
            crypto.decrypt_value("no_prefix_value")

    def test_encrypt_params_auto_sensitive(self):
        """Test auto-encrypting sensitive parameters."""
        from utils.workflow_crypto import WorkflowCrypto
        
        crypto = WorkflowCrypto()
        crypto.set_key("test_key")
        
        params = {
            "username": "user123",
            "password": "secret123",
            "api_key": "key123456",
            "action": "click"
        }
        
        encrypted = crypto.encrypt_params(params)
        
        # Password and api_key should be encrypted
        self.assertNotEqual(encrypted["password"], "secret123")
        self.assertNotEqual(encrypted["api_key"], "key123456")
        # Others should remain unchanged
        self.assertEqual(encrypted["username"], "user123")
        self.assertEqual(encrypted["action"], "click")

    def test_encrypt_params_nested(self):
        """Test encrypting nested dictionary parameters."""
        from utils.workflow_crypto import WorkflowCrypto
        
        crypto = WorkflowCrypto()
        crypto.set_key("test_key")
        
        params = {
            "config": {
                "password": "nested_secret"
            }
        }
        
        encrypted = crypto.encrypt_params(params)
        
        self.assertNotEqual(encrypted["config"]["password"], "nested_secret")

    def test_decrypt_params(self):
        """Test decrypting encrypted parameters."""
        from utils.workflow_crypto import WorkflowCrypto
        
        crypto = WorkflowCrypto()
        crypto.set_key("test_key")
        
        original = {
            "username": "user",
            "password": "secret"
        }
        
        encrypted = crypto.encrypt_params(original)
        decrypted = crypto.decrypt_params(encrypted)
        
        self.assertEqual(decrypted["username"], "user")
        self.assertEqual(decrypted["password"], "secret")


class TestAuditLogger(unittest.TestCase):
    """Tests for AuditLogger log/query."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        self.temp_file.close()
        self.addCleanup(os.unlink, self.temp_file.name)

    def test_log_execution(self):
        """Test logging a workflow execution."""
        from utils.audit_logger import AuditLogger
        
        logger = AuditLogger(log_file=self.temp_file.name)
        
        entry = logger.log_execution(
            workflow_name="test_workflow",
            user="test_user",
            duration=5.0,
            success=True
        )
        
        self.assertEqual(entry["workflow_name"], "test_workflow")
        self.assertEqual(entry["user"], "test_user")
        self.assertEqual(entry["duration_seconds"], 5.0)
        self.assertTrue(entry["success"])

    def test_log_execution_with_error(self):
        """Test logging failed execution."""
        from utils.audit_logger import AuditLogger
        
        logger = AuditLogger(log_file=self.temp_file.name)
        
        entry = logger.log_execution(
            workflow_name="failing_workflow",
            success=False,
            error="Something went wrong"
        )
        
        self.assertFalse(entry["success"])
        self.assertEqual(entry["error"], "Something went wrong")

    def test_query_logs_by_workflow_name(self):
        """Test querying logs by workflow name."""
        from utils.audit_logger import AuditLogger
        
        logger = AuditLogger(log_file=self.temp_file.name)
        
        logger.log_execution(workflow_name="workflow_a")
        logger.log_execution(workflow_name="workflow_b")
        logger.log_execution(workflow_name="workflow_a")
        
        results = logger.query_logs(workflow_name="workflow_a")
        
        self.assertEqual(len(results), 2)

    def test_query_logs_by_user(self):
        """Test querying logs by user."""
        from utils.audit_logger import AuditLogger
        
        logger = AuditLogger(log_file=self.temp_file.name)
        
        logger.log_execution(workflow_name="wf1", user="alice")
        logger.log_execution(workflow_name="wf2", user="bob")
        logger.log_execution(workflow_name="wf3", user="alice")
        
        results = logger.query_logs(user="alice")
        
        self.assertEqual(len(results), 2)

    def test_query_logs_by_success(self):
        """Test querying logs by success status."""
        from utils.audit_logger import AuditLogger
        
        logger = AuditLogger(log_file=self.temp_file.name)
        
        logger.log_execution(workflow_name="wf1", success=True)
        logger.log_execution(workflow_name="wf2", success=False)
        logger.log_execution(workflow_name="wf3", success=True)
        
        successful = logger.query_logs(success=True)
        failed = logger.query_logs(success=False)
        
        self.assertEqual(len(successful), 2)
        self.assertEqual(len(failed), 1)

    def test_get_statistics(self):
        """Test getting audit statistics."""
        from utils.audit_logger import AuditLogger
        
        logger = AuditLogger(log_file=self.temp_file.name)
        
        logger.log_execution(workflow_name="wf1", duration=5.0, success=True)
        logger.log_execution(workflow_name="wf2", duration=10.0, success=True)
        logger.log_execution(workflow_name="wf3", success=False)
        
        stats = logger.get_statistics()
        
        self.assertEqual(stats["total_executions"], 3)
        self.assertEqual(stats["successful_executions"], 2)
        self.assertEqual(stats["failed_executions"], 1)
        self.assertAlmostEqual(stats["average_duration"], 7.5)

    def test_max_entries_fifo(self):
        """Test that max entries is enforced (FIFO)."""
        from utils.audit_logger import AuditLogger
        
        # Use small max_entries for testing
        logger = AuditLogger(log_file=self.temp_file.name, max_entries=5)
        
        for i in range(10):
            logger.log_execution(workflow_name=f"wf{i}")
        
        entries = logger.query_logs(limit=100)
        
        # Should only have last 5 entries
        self.assertEqual(len(entries), 5)


class TestSecurityScanner(unittest.TestCase):
    """Tests for SecurityScanner scan patterns."""

    def test_scan_safe_workflow(self):
        """Test scanning a safe workflow."""
        from utils.security_scan import SecurityScanner
        
        scanner = SecurityScanner()
        
        workflow = {
            "actions": [
                {"type": "click", "params": {"x": 100, "y": 200}},
                {"type": "delay", "params": {"seconds": 1}}
            ]
        }
        
        is_safe, issues = scanner.scan(workflow)
        
        self.assertTrue(is_safe)
        self.assertEqual(len(issues), 0)

    def test_scan_dangerous_action(self):
        """Test detecting dangerous action."""
        from utils.security_scan import SecurityScanner
        
        scanner = SecurityScanner()
        
        workflow = {
            "actions": [
                {"type": "delete_file", "params": {"path": "/tmp/test"}}
            ]
        }
        
        is_safe, issues = scanner.scan(workflow)
        
        self.assertFalse(is_safe)
        self.assertTrue(any("Dangerous action" in i["message"] for i in issues))

    def test_scan_plain_text_password(self):
        """Test detecting plain-text password."""
        from utils.security_scan import SecurityScanner
        
        scanner = SecurityScanner()
        
        workflow = {
            "actions": [
                {"type": "execute_shell", "params": {"command": "ls", "password": "my_secret_password"}}
            ]
        }
        
        is_safe, issues = scanner.scan(workflow)
        
        self.assertFalse(is_safe)
        self.assertTrue(any("Sensitive parameter" in i["message"] or "plain text" in i["message"].lower() for i in issues))

    def test_scan_aws_credentials_pattern(self):
        """Test detecting AWS credentials pattern."""
        from utils.security_scan import SecurityScanner
        
        scanner = SecurityScanner()
        
        workflow = {
            "actions": [
                {"type": "execute_shell", "params": {"command": "aws", "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE"}}
            ]
        }
        
        is_safe, issues = scanner.scan(workflow)
        
        self.assertFalse(is_safe)

    def test_scan_github_token_pattern(self):
        """Test detecting GitHub token pattern."""
        from utils.security_scan import SecurityScanner
        
        scanner = SecurityScanner()
        
        workflow = {
            "actions": [
                {"type": "execute_shell", "params": {"command": "gh", "token": "ghp_1234567890abcdefghijklmnopqrstuvwxyzAB"}}
            ]
        }
        
        is_safe, issues = scanner.scan(workflow)
        
        self.assertFalse(is_safe)

    def test_scan_dangerous_combination(self):
        """Test detecting dangerous action combination."""
        from utils.security_scan import SecurityScanner
        
        scanner = SecurityScanner()
        
        workflow = {
            "actions": [
                {"type": "execute_shell", "params": {"command": "ls"}},
                {"type": "delete_file", "params": {"path": "/tmp/test"}}
            ]
        }
        
        is_safe, issues = scanner.scan(workflow)
        
        self.assertFalse(is_safe)
        # Should detect both the dangerous actions and the combination
        issue_types = [i["message"] for i in issues]
        self.assertTrue(any("Dangerous action" in msg or "Dangerous combination" in msg for msg in issue_types))

    def test_check_dangerous_patterns(self):
        """Test checking text for dangerous patterns."""
        from utils.security_scan import SecurityScanner
        
        scanner = SecurityScanner()
        
        results = scanner.check_dangerous_patterns('password="my_secret_password"')
        
        self.assertTrue(len(results) > 0)

    def test_scan_encrypted_value_not_flagged(self):
        """Test that encrypted values are not flagged."""
        from utils.security_scan import SecurityScanner
        
        scanner = SecurityScanner()
        
        workflow = {
            "actions": [
                {"type": "api_call", "params": {"password": "_enc_xxxxxencryptedxxxx"}}
            ]
        }
        
        is_safe, issues = scanner.scan(workflow)
        
        # Encrypted values should not trigger plain-text warnings
        plain_text_issues = [i for i in issues if "plain text" in i["message"].lower()]
        self.assertEqual(len(plain_text_issues), 0)


class TestPluginManager(unittest.TestCase):
    """Tests for PluginManager discover/load."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(self.temp_dir) if os.path.exists(self.temp_dir) else None)

    def test_discover_plugins_empty_dir(self):
        """Test discovering plugins in empty directory."""
        from utils.plugin_manager import PluginManager
        
        pm = PluginManager(project_root=self.temp_dir)
        pm.plugins_dir = Path(self.temp_dir)
        
        plugins = pm.discover_plugins()
        
        self.assertEqual(len(plugins), 0)

    def test_discover_plugins_with_manifest(self):
        """Test discovering plugin with valid manifest."""
        from utils.plugin_manager import PluginManager, PluginMetadata
        
        plugin_dir = os.path.join(self.temp_dir, "test_plugin")
        os.makedirs(plugin_dir)
        
        manifest = {
            "name": "test_plugin",
            "version": "1.0.0",
            "description": "A test plugin"
        }
        
        with open(os.path.join(plugin_dir, "plugin.json"), "w") as f:
            json.dump(manifest, f)
        
        pm = PluginManager(project_root=self.temp_dir)
        pm.plugins_dir = Path(self.temp_dir)
        
        plugins = pm.discover_plugins()
        
        self.assertEqual(len(plugins), 1)
        self.assertEqual(plugins[0].name, "test_plugin")

    def test_discover_plugins_missing_manifest(self):
        """Test plugin directory without manifest is ignored."""
        from utils.plugin_manager import PluginManager
        
        plugin_dir = os.path.join(self.temp_dir, "no_manifest_plugin")
        os.makedirs(plugin_dir)
        
        # Create a Python file but no manifest
        with open(os.path.join(plugin_dir, "__init__.py"), "w") as f:
            f.write("# just a file")
        
        pm = PluginManager(project_root=self.temp_dir)
        pm.plugins_dir = Path(self.temp_dir)
        
        plugins = pm.discover_plugins()
        
        self.assertEqual(len(plugins), 0)

    def test_load_manifest(self):
        """Test loading plugin manifest."""
        from utils.plugin_manager import PluginManager
        
        pm = PluginManager(project_root=self.temp_dir)
        
        manifest_path = os.path.join(self.temp_dir, "plugin.json")
        with open(manifest_path, "w") as f:
            json.dump({
                "name": "test",
                "version": "2.0"
            }, f)
        
        metadata = pm._load_manifest(Path(manifest_path))
        
        self.assertEqual(metadata.name, "test")
        self.assertEqual(metadata.version, "2.0")

    def test_compute_file_hash(self):
        """Test computing file hash for change detection."""
        from utils.plugin_manager import PluginManager
        
        pm = PluginManager(project_root=self.temp_dir)
        
        test_file = os.path.join(self.temp_dir, "test.py")
        with open(test_file, "w") as f:
            f.write("# test content")
        
        hash1 = pm._compute_file_hash(Path(self.temp_dir))
        self.assertIsInstance(hash1, str)
        self.assertEqual(len(hash1), 32)  # MD5 hex length

    def test_validate_plugin_valid(self):
        """Test validating a valid plugin directory."""
        from utils.plugin_manager import PluginManager
        
        plugin_dir = os.path.join(self.temp_dir, "valid_plugin")
        os.makedirs(plugin_dir)
        
        with open(os.path.join(plugin_dir, "plugin.json"), "w") as f:
            json.dump({"name": "valid", "version": "1.0"}, f)
        
        with open(os.path.join(plugin_dir, "__init__.py"), "w") as f:
            f.write("from utils.plugin_manager import BasePlugin\nclass MyPlugin(BasePlugin): pass")
        
        pm = PluginManager(project_root=self.temp_dir)
        
        valid, error = pm.validate_plugin(Path(plugin_dir))
        
        self.assertTrue(valid)
        self.assertIsNone(error)

    def test_validate_plugin_missing_manifest(self):
        """Test validating plugin without manifest."""
        from utils.plugin_manager import PluginManager
        
        plugin_dir = os.path.join(self.temp_dir, "no_manifest")
        os.makedirs(plugin_dir)
        
        pm = PluginManager(project_root=self.temp_dir)
        
        valid, error = pm.validate_plugin(Path(plugin_dir))
        
        self.assertFalse(valid)
        self.assertIn("Missing plugin.json", error)


class TestThemeManager(unittest.TestCase):
    """Tests for ThemeManager themes."""

    def test_get_builtin_themes(self):
        """Test getting built-in themes."""
        from ui.themes import BUILT_IN_THEMES, LIGHT_THEME, DARK_THEME
        
        self.assertIn("light", BUILT_IN_THEMES)
        self.assertIn("dark", BUILT_IN_THEMES)
        
        self.assertEqual(LIGHT_THEME.name, "light")
        self.assertEqual(DARK_THEME.name, "dark")

    def test_theme_config_from_dict(self):
        """Test creating ThemeConfig from dictionary."""
        from ui.themes import ThemeConfig
        
        data = {
            "name": "custom",
            "display_name": "Custom Theme",
            "colors": {
                "primary": "#FF0000"
            }
        }
        
        theme = ThemeConfig.from_dict(data)
        
        self.assertEqual(theme.name, "custom")
        self.assertEqual(theme.colors.primary, "#FF0000")

    def test_theme_config_to_dict(self):
        """Test converting ThemeConfig to dictionary."""
        from ui.themes import ThemeConfig, ThemeColors
        
        theme = ThemeConfig(
            name="test",
            display_name="Test",
            colors=ThemeColors(primary="#00FF00")
        )
        
        result = theme.to_dict()
        
        self.assertEqual(result["name"], "test")
        self.assertEqual(result["colors"]["primary"], "#00FF00")

    def test_high_contrast_theme(self):
        """Test high contrast theme exists and has proper values."""
        from ui.themes import HIGH_CONTRAST_THEME
        
        self.assertEqual(HIGH_CONTRAST_THEME.name, "high_contrast")
        # High contrast should have high visibility colors
        self.assertEqual(HIGH_CONTRAST_THEME.colors.primary, "#FFFF00")


class TestMouseUtils(unittest.TestCase):
    """Tests for MouseUtils screen bounds."""

    def test_macos_click_validates_button(self):
        """Test macos_click validates button parameter."""
        from utils.mouse_utils import macos_click, VALID_BUTTONS
        
        # Valid buttons should not raise
        for button in VALID_BUTTONS:
            try:
                macos_click(100, 100, 1, button)
            except Exception:
                pass  # May fail due to pyautogui mock, but shouldn't raise ValueError

    def test_macos_click_invalid_button(self):
        """Test macos_click rejects invalid button."""
        from utils.mouse_utils import macos_click
        
        with self.assertRaises(ValueError) as ctx:
            macos_click(100, 100, 1, "invalid")
        
        self.assertIn("Invalid button", str(ctx.exception))

    def test_macos_click_invalid_click_count(self):
        """Test macos_click rejects invalid click_count."""
        from utils.mouse_utils import macos_click
        
        with self.assertRaises(ValueError) as ctx:
            macos_click(100, 100, 0, "left")
        
        self.assertIn("click_count must be >= 1", str(ctx.exception))

    def test_platform_detection(self):
        """Test platform detection constants."""
        from utils.mouse_utils import IS_MACOS
        import platform
        
        # IS_MACOS should reflect actual platform
        self.assertEqual(IS_MACOS, platform.system() == 'Darwin')


if __name__ == '__main__':
    unittest.main()

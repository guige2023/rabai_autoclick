"""
Tests for workflow_helm module.

Commit: 'tests: add comprehensive tests for workflow_helm and workflow_knative modules'
"""

import sys
sys.path.insert(0, '/Users/guige/my_project')

import json
import os
import subprocess
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
from unittest.mock import MagicMock, patch, mock_open, call

from rabai_autoclick.src.workflow_helm import (
    HelmManager,
    HookType,
    HookDeletionPolicy,
    ChartInfo,
    ReleaseInfo,
    RepositoryInfo,
    HistoryEntry,
    HookConfig,
)


class TestHelmManagerInit(unittest.TestCase):
    """Tests for HelmManager initialization."""

    def test_default_init(self):
        """Test default initialization."""
        manager = HelmManager()
        self.assertEqual(manager.helm_path, "helm")
        self.assertIsNone(manager.kube_context)
        self.assertEqual(manager._cached_repos, {})

    def test_custom_init(self):
        """Test custom initialization."""
        manager = HelmManager(kube_context="minikube", helm_path="/usr/local/bin/helm")
        self.assertEqual(manager.helm_path, "/usr/local/bin/helm")
        self.assertEqual(manager.kube_context, "minikube")

    def test_cached_repos_initialized(self):
        """Test that cached repos is initialized as empty dict."""
        manager = HelmManager()
        self.assertIsInstance(manager._cached_repos, dict)
        self.assertEqual(len(manager._cached_repos), 0)


class TestHelmManagerRunHelm(unittest.TestCase):
    """Tests for _run_helm method."""

    def setUp(self):
        self.manager = HelmManager()

    @patch('subprocess.run')
    def test_run_helm_basic(self, mock_run):
        """Test basic helm command execution."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "SUCCESS"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        result = self.manager._run_helm(["list"])

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "SUCCESS")
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        self.assertEqual(args[0], "helm")
        self.assertEqual(args[1], "list")

    @patch('subprocess.run')
    def test_run_helm_with_kube_context(self, mock_run):
        """Test helm command with kube context."""
        manager = HelmManager(kube_context="test-cluster")
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        manager._run_helm(["list"])

        args = mock_run.call_args[0][0]
        self.assertIn("--kube-context", args)
        self.assertIn("test-cluster", args)

    @patch('subprocess.run')
    def test_run_helm_with_input_data(self, mock_run):
        """Test helm command with input data."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        self.manager._run_helm(["install", "my-release", "my-chart"], input_data="replicas: 2")

        kwargs = mock_run.call_args[1]
        self.assertEqual(kwargs["input"], "replicas: 2")


class TestChartInfo(unittest.TestCase):
    """Tests for ChartInfo dataclass."""

    def test_chart_info_creation(self):
        """Test ChartInfo creation."""
        chart = ChartInfo(
            name="my-chart",
            version="1.0.0",
            api_version="v2"
        )
        self.assertEqual(chart.name, "my-chart")
        self.assertEqual(chart.version, "1.0.0")
        self.assertIsNone(chart.app_version)
        self.assertIsNone(chart.description)

    def test_chart_info_full(self):
        """Test ChartInfo with all fields."""
        chart = ChartInfo(
            name="my-chart",
            version="1.0.0",
            api_version="v2",
            app_version="1.0.0",
            description="My chart description"
        )
        self.assertEqual(chart.app_version, "1.0.0")
        self.assertEqual(chart.description, "My chart description")


class TestReleaseInfo(unittest.TestCase):
    """Tests for ReleaseInfo dataclass."""

    def test_release_info_creation(self):
        """Test ReleaseInfo creation."""
        release = ReleaseInfo(
            name="my-release",
            namespace="default",
            revision=1,
            status="deployed",
            chart="my-chart-1.0.0",
            chart_version="1.0.0"
        )
        self.assertEqual(release.name, "my-release")
        self.assertEqual(release.namespace, "default")
        self.assertEqual(release.revision, 1)
        self.assertIsNone(release.last_deployed)


class TestRepositoryInfo(unittest.TestCase):
    """Tests for RepositoryInfo dataclass."""

    def test_repository_info_creation(self):
        """Test RepositoryInfo creation."""
        repo = RepositoryInfo(name="stable", url="https://charts.helm.sh/stable")
        self.assertEqual(repo.name, "stable")
        self.assertEqual(repo.url, "https://charts.helm.sh/stable")
        self.assertFalse(repo.cached)

    def test_repository_info_cached(self):
        """Test RepositoryInfo with cached flag."""
        repo = RepositoryInfo(name="stable", url="https://charts.helm.sh/stable", cached=True)
        self.assertTrue(repo.cached)


class TestHistoryEntry(unittest.TestCase):
    """Tests for HistoryEntry dataclass."""

    def test_history_entry_creation(self):
        """Test HistoryEntry creation."""
        entry = HistoryEntry(
            revision=1,
            app_version="1.0.0",
            chart_version="1.0.0",
            status="deployed",
            description="Initial release"
        )
        self.assertEqual(entry.revision, 1)
        self.assertIsNone(entry.deployed_at)


class TestHookConfig(unittest.TestCase):
    """Tests for HookConfig dataclass."""

    def test_hook_config_creation(self):
        """Test HookConfig creation."""
        config = HookConfig(
            name="my-hook",
            hook_type=HookType.PRE_INSTALL,
            path="./hooks/pre-install.yaml"
        )
        self.assertEqual(config.name, "my-hook")
        self.assertEqual(config.hook_type, HookType.PRE_INSTALL)
        self.assertEqual(config.weight, 0)
        self.assertEqual(config.deletion_policy, HookDeletionPolicy.DELETE)

    def test_hook_config_custom_weight(self):
        """Test HookConfig with custom weight."""
        config = HookConfig(
            name="my-hook",
            hook_type=HookType.POST_UPGRADE,
            path="./hooks/post-upgrade.yaml",
            weight=5,
            deletion_policy=HookDeletionPolicy.DELETE_WAIT
        )
        self.assertEqual(config.weight, 5)
        self.assertEqual(config.deletion_policy, HookDeletionPolicy.DELETE_WAIT)


class TestHookType(unittest.TestCase):
    """Tests for HookType enum."""

    def test_hook_types_exist(self):
        """Test all hook types exist."""
        self.assertEqual(HookType.PRE_INSTALL.value, "pre-install")
        self.assertEqual(HookType.POST_INSTALL.value, "post-install")
        self.assertEqual(HookType.PRE_UPGRADE.value, "pre-upgrade")
        self.assertEqual(HookType.POST_UPGRADE.value, "post-upgrade")
        self.assertEqual(HookType.PRE_DELETE.value, "pre-delete")
        self.assertEqual(HookType.POST_DELETE.value, "post-delete")
        self.assertEqual(HookType.TEST.value, "test")


class TestHookDeletionPolicy(unittest.TestCase):
    """Tests for HookDeletionPolicy enum."""

    def test_deletion_policies_exist(self):
        """Test all deletion policies exist."""
        self.assertEqual(HookDeletionPolicy.DELETE.value, "delete")
        self.assertEqual(HookDeletionPolicy.RETAIN.value, "retain")
        self.assertEqual(HookDeletionPolicy.DELETE_WAIT.value, "hook-delete-policy")


class TestHelmManagerChartManagement(unittest.TestCase):
    """Tests for chart management methods."""

    def setUp(self):
        self.manager = HelmManager()

    @patch('subprocess.run')
    def test_create_chart_success(self, mock_run):
        """Test successful chart creation."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        result = self.manager.create_chart("my-chart", "/tmp/charts")

        self.assertTrue(result)
        args = mock_run.call_args[0][0]
        self.assertIn("create", args)
        self.assertIn("my-chart", args)

    @patch('subprocess.run')
    def test_create_chart_failure(self, mock_run):
        """Test chart creation failure."""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_run.return_value = mock_result

        result = self.manager.create_chart("invalid-chart")

        self.assertFalse(result)

    @patch('subprocess.run')
    def test_package_chart(self, mock_run):
        """Test chart packaging."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        result = self.manager.package_chart("/tmp/my-chart", destination="/tmp/packages")

        self.assertTrue(result)
        args = mock_run.call_args[0][0]
        self.assertIn("package", args)
        self.assertIn("/tmp/my-chart", args)
        self.assertIn("--destination", args)
        self.assertIn("/tmp/packages", args)

    @patch('subprocess.run')
    def test_package_chart_with_signing(self, mock_run):
        """Test chart packaging with signing."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        result = self.manager.package_chart(
            "/tmp/my-chart",
            destination="/tmp/packages",
            sign=True,
            key="my-key",
            keyring="/tmp/keyring"
        )

        self.assertTrue(result)
        args = mock_run.call_args[0][0]
        self.assertIn("--sign", args)
        self.assertIn("--key", args)
        self.assertIn("my-key", args)

    @patch('subprocess.run')
    def test_lint_chart_pass(self, mock_run):
        """Test chart linting - passing."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "2 chart(s) linted, 0 chart(s) failed"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        result = self.manager.lint_chart("/tmp/my-chart")

        self.assertTrue(result["passed"])
        self.assertIn("linted", result["output"])

    @patch('subprocess.run')
    def test_lint_chart_fail(self, mock_run):
        """Test chart linting - failing."""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "ERROR: validation failed"
        mock_run.return_value = mock_result

        result = self.manager.lint_chart("/tmp/my-chart", strict=True)

        self.assertFalse(result["passed"])
        self.assertIn("validation failed", result["errors"])

    @patch('subprocess.run')
    def test_show_chart_info(self, mock_run):
        """Test getting chart info."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = """
name: my-chart
version: 1.0.0
apiVersion: v2
appVersion: 1.0.0
description: My test chart
"""
        mock_run.return_value = mock_result

        chart_info = self.manager.show_chart_info("my-chart")

        self.assertIsNotNone(chart_info)
        self.assertEqual(chart_info.name, "my-chart")
        self.assertEqual(chart_info.version, "1.0.0")

    @patch('subprocess.run')
    def test_show_chart_info_failure(self, mock_run):
        """Test getting chart info - failure."""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "chart not found"
        mock_run.return_value = mock_result

        chart_info = self.manager.show_chart_info("nonexistent-chart")

        self.assertIsNone(chart_info)

    @patch('subprocess.run')
    def test_pull_chart(self, mock_run):
        """Test pulling a chart."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        result = self.manager.pull_chart("stable/nginx", destination="/tmp/charts")

        self.assertTrue(result)
        args = mock_run.call_args[0][0]
        self.assertIn("pull", args)
        self.assertIn("--destination", args)


class TestHelmManagerRepositoryManagement(unittest.TestCase):
    """Tests for repository management methods."""

    def setUp(self):
        self.manager = HelmManager()

    @patch('subprocess.run')
    def test_add_repository_success(self, mock_run):
        """Test adding a repository."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        result = self.manager.add_repository("stable", "https://charts.helm.sh/stable")

        self.assertTrue(result)
        self.assertIn("stable", self.manager._cached_repos)
        args = mock_run.call_args[0][0]
        self.assertIn("repo", args)
        self.assertIn("add", args)

    @patch('subprocess.run')
    def test_add_repository_with_auth(self, mock_run):
        """Test adding a repository with authentication."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        result = self.manager.add_repository(
            "private",
            "https://private.charts.com",
            username="user",
            password="pass"
        )

        self.assertTrue(result)
        args = mock_run.call_args[0][0]
        self.assertIn("--username", args)
        self.assertIn("user", args)
        self.assertIn("--password", args)

    @patch('subprocess.run')
    def test_update_repositories(self, mock_run):
        """Test updating repositories."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        result = self.manager.update_repositories()

        self.assertTrue(result)
        args = mock_run.call_args[0][0]
        self.assertIn("repo", args)
        self.assertIn("update", args)

    @patch('subprocess.run')
    def test_remove_repository(self, mock_run):
        """Test removing a repository."""
        self.manager._cached_repos["stable"] = RepositoryInfo(name="stable", url="https://stable")
        
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        result = self.manager.remove_repository("stable")

        self.assertTrue(result)
        self.assertNotIn("stable", self.manager._cached_repos)

    @patch('subprocess.run')
    def test_list_repositories(self, mock_run):
        """Test listing repositories."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps([
            {"name": "stable", "url": "https://charts.helm.sh/stable"},
            {"name": "ingress-nginx", "url": "https://kubernetes.github.io/ingress-nginx"}
        ])
        mock_run.return_value = mock_result

        repos = self.manager.list_repositories()

        self.assertEqual(len(repos), 2)
        self.assertEqual(repos[0].name, "stable")
        self.assertEqual(repos[1].name, "ingress-nginx")

    @patch('subprocess.run')
    def test_list_repositories_failure(self, mock_run):
        """Test listing repositories - failure."""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_run.return_value = mock_result

        repos = self.manager.list_repositories()

        self.assertEqual(len(repos), 0)

    @patch('subprocess.run')
    def test_search_repositories(self, mock_run):
        """Test searching repositories."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = """NAME                	VERSION  	APP VERSION   DESCRIPTION
stable/nginx        	1.0.0    	1.21.0       NGINX Ingress Controller
stable/nginx-ingress	2.0.0    	1.21.0       DEPRECATED! Use stable/nginx
"""
        mock_run.return_value = mock_result

        results = self.manager.search_repositories("nginx")

        self.assertGreater(len(results), 0)
        self.assertEqual(results[0]["name"], "stable/nginx")


class TestHelmManagerReleaseManagement(unittest.TestCase):
    """Tests for release management methods."""

    def setUp(self):
        self.manager = HelmManager()

    @patch('subprocess.run')
    def test_install_release_success(self, mock_run):
        """Test successful release installation."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "NAME: my-release\nNAMESPACE: default\nSTATUS: deployed"
        mock_run.return_value = mock_result

        result = self.manager.install_release("my-release", "my-chart")

        self.assertTrue(result["success"])
        self.assertEqual(result["action"], "installed")

    @patch('subprocess.run')
    def test_install_release_with_namespace(self, mock_run):
        """Test release installation with namespace."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_run.return_value = mock_result

        self.manager.install_release("my-release", "my-chart", namespace="my-namespace")

        args = mock_run.call_args[0][0]
        self.assertIn("--namespace", args)
        self.assertIn("my-namespace", args)
        self.assertIn("--create-namespace", args)

    @patch('subprocess.run')
    def test_install_release_with_values(self, mock_run):
        """Test release installation with values."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_run.return_value = mock_result

        values = {"replicas": 3, "image": {"tag": "1.21"}}
        self.manager.install_release("my-release", "my-chart", values=values)

        kwargs = mock_run.call_args[1]
        self.assertIn("input", kwargs)

    @patch('subprocess.run')
    def test_install_release_failure(self, mock_run):
        """Test release installation failure."""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "release not found"
        mock_run.return_value = mock_result

        result = self.manager.install_release("my-release", "nonexistent-chart")

        self.assertFalse(result["success"])

    @patch('subprocess.run')
    def test_upgrade_release(self, mock_run):
        """Test release upgrade."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_run.return_value = mock_result

        result = self.manager.upgrade_release("my-release", "my-chart")

        self.assertTrue(result["success"])
        args = mock_run.call_args[0][0]
        self.assertIn("upgrade", args)

    @patch('subprocess.run')
    def test_upgrade_release_with_options(self, mock_run):
        """Test release upgrade with options."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_run.return_value = mock_result

        self.manager.upgrade_release(
            "my-release", "my-chart",
            wait=True, atomic=True, dry_run=True
        )

        args = mock_run.call_args[0][0]
        self.assertIn("--wait", args)
        self.assertIn("--atomic", args)
        self.assertIn("--dry-run", args)

    @patch('subprocess.run')
    def test_rollback_release(self, mock_run):
        """Test release rollback."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_run.return_value = mock_result

        result = self.manager.rollback_release("my-release", revision=1)

        self.assertTrue(result["success"])
        args = mock_run.call_args[0][0]
        self.assertIn("rollback", args)
        self.assertIn("1", args)

    @patch('subprocess.run')
    def test_uninstall_release(self, mock_run):
        """Test release uninstallation."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "release uninstalled"
        mock_run.return_value = mock_result

        result = self.manager.uninstall_release("my-release")

        self.assertTrue(result["success"])
        args = mock_run.call_args[0][0]
        self.assertIn("uninstall", args)

    @patch('subprocess.run')
    def test_uninstall_release_keep_history(self, mock_run):
        """Test release uninstallation with keep history."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        self.manager.uninstall_release("my-release", keep_history=True)

        args = mock_run.call_args[0][0]
        self.assertIn("--keep-history", args)

    @patch('subprocess.run')
    def test_list_releases(self, mock_run):
        """Test listing releases."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps([
            {
                "name": "my-release",
                "namespace": "default",
                "revision": "1",
                "status": "deployed",
                "chart": "my-chart-1.0.0",
                "chart_version": "1.0.0"
            }
        ])
        mock_run.return_value = mock_result

        releases = self.manager.list_releases()

        self.assertEqual(len(releases), 1)
        self.assertEqual(releases[0].name, "my-release")
        self.assertEqual(releases[0].revision, 1)

    @patch('subprocess.run')
    def test_get_release_status(self, mock_run):
        """Test getting release status."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"name": "my-release", "status": "deployed"})
        mock_run.return_value = mock_result

        status = self.manager.get_release_status("my-release")

        self.assertIsNotNone(status)
        self.assertEqual(status["name"], "my-release")

    @patch('subprocess.run')
    def test_get_release_values(self, mock_run):
        """Test getting release values."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "replicas: 3\nimage:\n  tag: 1.21"
        mock_run.return_value = mock_result

        values = self.manager.get_release_values("my-release")

        self.assertIsNotNone(values)
        self.assertEqual(values["replicas"], 3)

    @patch('subprocess.run')
    def test_get_release_manifest(self, mock_run):
        """Test getting release manifest."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "apiVersion: v1\nkind: Deployment"
        mock_run.return_value = mock_result

        manifest = self.manager.get_release_manifest("my-release")

        self.assertIsNotNone(manifest)
        self.assertIn("Deployment", manifest)


class TestHelmManagerValueManagement(unittest.TestCase):
    """Tests for value management methods."""

    def test_merge_values(self):
        """Test merging values."""
        manager = HelmManager()
        
        values1 = {"replicas": 1, "image": {"repository": "nginx"}}
        values2 = {"replicas": 3, "image": {"tag": "1.21"}}
        
        result = manager.merge_values(values1, values2)
        
        self.assertEqual(result["replicas"], 3)
        self.assertEqual(result["image"]["repository"], "nginx")
        self.assertEqual(result["image"]["tag"], "1.21")

    def test_merge_values_nested(self):
        """Test merging nested values."""
        manager = HelmManager()
        
        values1 = {"config": {"key1": "value1"}}
        values2 = {"config": {"key2": "value2"}}
        
        result = manager.merge_values(values1, values2)
        
        self.assertEqual(result["config"]["key1"], "value1")
        self.assertEqual(result["config"]["key2"], "value2")

    @patch('builtins.open', new_callable=mock_open, read_data="replicas: 3\nimage:\n  tag: 1.21")
    def test_load_values_file(self, mock_file):
        """Test loading values from file."""
        manager = HelmManager()
        
        result = manager.load_values_file("/tmp/values.yaml")
        
        self.assertIsNotNone(result)
        self.assertEqual(result["replicas"], 3)

    @patch('builtins.open', new_callable=mock_open)
    def test_save_values_file(self, mock_file):
        """Test saving values to file."""
        manager = HelmManager()
        
        values = {"replicas": 3, "image": {"tag": "1.21"}}
        result = manager.save_values_file(values, "/tmp/values.yaml")
        
        self.assertTrue(result)
        mock_file.assert_called_once_with("/tmp/values.yaml", 'w')


class TestHelmManagerTemplateRendering(unittest.TestCase):
    """Tests for template rendering methods."""

    def setUp(self):
        self.manager = HelmManager()

    @patch('subprocess.run')
    def test_render_templates(self, mock_run):
        """Test template rendering."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "apiVersion: v1\nkind: Deployment"
        mock_run.return_value = mock_result

        result = self.manager.render_templates("my-chart", values={"replicas": 3})

        self.assertTrue(result["success"])
        self.assertIn("Deployment", result["manifest"])

    @patch('subprocess.run')
    def test_render_templates_with_name(self, mock_run):
        """Test template rendering with release name."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "apiVersion: v1\nkind: Deployment"
        mock_run.return_value = mock_result

        result = self.manager.render_templates(
            "my-chart",
            name="my-release",
            namespace="default"
        )

        self.assertTrue(result["success"])
        args = mock_run.call_args[0][0]
        self.assertIn("--namespace", args)


class TestHelmManagerDependencyManagement(unittest.TestCase):
    """Tests for dependency management methods."""

    def setUp(self):
        self.manager = HelmManager()

    @patch('subprocess.run')
    def test_update_dependencies(self, mock_run):
        """Test updating dependencies."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_mock = MagicMock()
        mock_mock.returncode = 0

        result = self.manager.update_dependencies("/tmp/my-chart")

        self.assertTrue(result)
        args = mock_run.call_args[0][0]
        self.assertIn("dependency", args)
        self.assertIn("update", args)

    @patch('subprocess.run')
    def test_list_dependencies(self, mock_run):
        """Test listing dependencies."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = """NAME        	VERSION  	REPOSITORY                          STATUS  
postgresql  	11.6.3   	https://charts.bitnami.com/bitnami   ok      
redis       	15.7.6   	https://charts.bitnami.com/bitnami   ok      
"""
        mock_run.return_value = mock_result

        deps = self.manager.list_dependencies("/tmp/my-chart")

        self.assertGreater(len(deps), 0)
        self.assertEqual(deps[0]["name"], "postgresql")


class TestHelmManagerHookManagement(unittest.TestCase):
    """Tests for hook management methods."""

    def setUp(self):
        self.manager = HelmManager()

    def test_create_hook(self):
        """Test hook creation."""
        manager = HelmManager()
        
        config = HookConfig(
            name="my-hook",
            hook_type=HookType.PRE_INSTALL,
            path="./hooks/pre-install.yaml",
            weight=5
        )
        
        hook_yaml = manager.create_hook(config)
        
        self.assertIn("pre-install", hook_yaml)
        self.assertIn("my-hook", hook_yaml)
        self.assertIn("helm.sh/hook-weight", hook_yaml)

    def test_add_hook_annotation(self):
        """Test adding hook annotations to resource."""
        manager = HelmManager()
        
        resource = {"kind": "Deployment", "metadata": {}}
        result = manager.add_hook_annotation(
            resource,
            HookType.POST_UPGRADE,
            weight=3,
            deletion_policy=HookDeletionPolicy.HOOK_SUCCEEDED
        )
        
        annotations = result["metadata"]["annotations"]
        self.assertEqual(annotations["helm.sh/hook"], "post-upgrade")
        self.assertEqual(annotations["helm.sh/hook-weight"], "3")
        self.assertEqual(annotations["helm.sh/hook-delete-policy"], "hook-succeeded")

    def test_remove_hook_annotation(self):
        """Test removing hook annotations from resource."""
        manager = HelmManager()
        
        resource = {
            "kind": "Deployment",
            "metadata": {
                "annotations": {
                    "helm.sh/hook": "pre-install",
                    "helm.sh/hook-weight": "1",
                    "helm.sh/hook-delete-policy": "delete"
                }
            }
        }
        result = manager.remove_hook_annotation(resource)
        
        self.assertNotIn("helm.sh/hook", result["metadata"]["annotations"])

    def test_get_hooks_from_manifest(self):
        """Test extracting hooks from manifest."""
        manager = HelmManager()
        
        manifest = """
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: pre-install-hook
  annotations:
    helm.sh/hook: pre-install
    helm.sh/hook-weight: "1"
---
apiVersion: v1
kind: Service
metadata:
  name: my-service
"""
        
        hooks = manager.get_hooks_from_manifest(manifest)
        
        self.assertEqual(len(hooks), 1)
        self.assertEqual(hooks[0]["name"], "pre-install-hook")

    @patch('subprocess.run')
    def test_test_hooks(self, mock_run):
        """Test running hooks/tests."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "Hook 1: Passed"
        mock_run.return_value = mock_result

        result = self.manager.test_hooks("my-release", cleanup=True, parallel=True)

        self.assertTrue(result["success"])
        args = mock_run.call_args[0][0]
        self.assertIn("test", args)
        self.assertIn("--cleanup", args)
        self.assertIn("--parallel", args)


class TestHelmManagerPluginManagement(unittest.TestCase):
    """Tests for plugin management methods."""

    def setUp(self):
        self.manager = HelmManager()

    @patch('subprocess.run')
    def test_list_plugins(self, mock_run):
        """Test listing plugins."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps([
            {"name": "hub", "version": "0.9.0"},
            {"name": "diff", "version": "3.3.0"}
        ])
        mock_run.return_value = mock_result

        plugins = self.manager.list_plugins()

        self.assertEqual(len(plugins), 2)
        self.assertEqual(plugins[0]["name"], "hub")

    @patch('subprocess.run')
    def test_install_plugin(self, mock_run):
        """Test installing plugin."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        result = self.manager.install_plugin("https://example.com/plugin.tar.gz")

        self.assertTrue(result)
        args = mock_run.call_args[0][0]
        self.assertIn("plugin", args)
        self.assertIn("install", args)


class TestHelmManagerHistory(unittest.TestCase):
    """Tests for history analysis methods."""

    def setUp(self):
        self.manager = HelmManager()

    @patch('subprocess.run')
    def test_get_release_history(self, mock_run):
        """Test getting release history."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps([
            {
                "revision": "1",
                "app_version": "1.0.0",
                "chart_version": "1.0.0",
                "status": "deployed",
                "description": "Install complete"
            },
            {
                "revision": "2",
                "app_version": "1.1.0",
                "chart_version": "1.1.0",
                "status": "deployed",
                "description": "Upgrade complete"
            }
        ])
        mock_run.return_value = mock_result

        history = self.manager.get_release_history("my-release")

        self.assertEqual(len(history), 2)
        self.assertEqual(history[0].revision, 1)
        self.assertEqual(history[1].revision, 2)


class TestHelmManagerOCIRegistry(unittest.TestCase):
    """Tests for OCI registry methods."""

    def setUp(self):
        self.manager = HelmManager()

    @patch('subprocess.run')
    def test_login_to_oci(self, mock_run):
        """Test logging into OCI registry."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        result = self.manager.login_to_oci("registry.example.com", "user", "pass")

        self.assertTrue(result)
        args = mock_run.call_args[0][0]
        self.assertIn("registry", args)
        self.assertIn("login", args)
        self.assertIn("registry.example.com", args)

    @patch('subprocess.run')
    def test_pull_from_oci(self, mock_run):
        """Test pulling from OCI registry."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        result = self.manager.pull_from_oci(
            "oci://registry.example.com/charts/my-chart",
            destination="/tmp/charts"
        )

        self.assertTrue(result)
        args = mock_run.call_args[0][0]
        self.assertIn("oci://", args[2])


class TestHelmManagerImportExport(unittest.TestCase):
    """Tests for import/export methods."""

    def setUp(self):
        self.manager = HelmManager()

    @patch('subprocess.run')
    def test_extract_values(self, mock_run):
        """Test extracting values from chart."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "replicas: 3\nimage:\n  tag: 1.21"
        mock_run.return_value = mock_result

        result = self.manager.extract_values("my-chart")

        self.assertIsNotNone(result)
        self.assertEqual(result["replicas"], 3)
        mock_run.assert_called()

    @patch('subprocess.run')
    def test_extract_values_with_version(self, mock_run):
        """Test extracting values with specific version."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "replicas: 5"
        mock_run.return_value = mock_result

        result = self.manager.extract_values("my-chart", values_file="2.0.0")

        self.assertIsNotNone(result)
        args = mock_run.call_args[0][0]
        self.assertIn("--version", args)
        self.assertIn("2.0.0", args)


if __name__ == '__main__':
    unittest.main()

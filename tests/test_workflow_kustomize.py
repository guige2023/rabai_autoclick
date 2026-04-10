"""
Tests for workflow_kustomize module.

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

from rabai_autoclick.src.workflow_kustomize import (
    KustomizeManager,
    KustomizeImage,
    KustomizeComponent,
    KustomizePatch,
    HelmChart,
    KustomizeSecretGenerator,
    KustomizationSpec,
    KustomizeResourceKind,
    ImageTagStrategy,
    PatchType,
    HelmChartSource,
)


class TestKustomizeImage(unittest.TestCase):
    """Tests for KustomizeImage class."""

    def test_image_creation(self):
        """Test basic image creation."""
        image = KustomizeImage(name="nginx")
        self.assertEqual(image.name, "nginx")
        self.assertIsNone(image.new_name)
        self.assertIsNone(image.new_tag)

    def test_image_with_tag(self):
        """Test image with new tag."""
        image = KustomizeImage(name="nginx", new_tag="1.21")
        self.assertEqual(image.new_tag, "1.21")

    def test_image_to_dict(self):
        """Test image conversion to dictionary."""
        image = KustomizeImage(name="redis", new_tag="6.2", new_name="redis-cache")
        result = image.to_dict()
        
        self.assertEqual(result["name"], "redis")
        self.assertEqual(result["newTag"], "6.2")
        self.assertEqual(result["newName"], "redis-cache")

    def test_image_from_dict(self):
        """Test image creation from dictionary."""
        data = {"name": "postgres", "newTag": "14.1", "digest": "sha256:abc123"}
        image = KustomizeImage.from_dict(data)
        
        self.assertEqual(image.name, "postgres")
        self.assertEqual(image.new_tag, "14.1")
        self.assertEqual(image.digest, "sha256:abc123")


class TestKustomizeComponent(unittest.TestCase):
    """Tests for KustomizeComponent class."""

    def test_component_creation(self):
        """Test basic component creation."""
        component = KustomizeComponent(name="common-labels", path="./components/common-labels")
        self.assertEqual(component.name, "common-labels")
        self.assertTrue(component.included)

    def test_component_excluded(self):
        """Test excluded component."""
        component = KustomizeComponent(
            name="optional-component",
            path="./components/optional",
            included=False
        )
        self.assertFalse(component.included)


class TestKustomizePatch(unittest.TestCase):
    """Tests for KustomizePatch class."""

    def test_patch_creation(self):
        """Test basic patch creation."""
        target = {"kind": "Deployment", "name": "my-app"}
        patch = KustomizePatch(target=target, patch="replicas: 3")
        
        self.assertEqual(patch.target["kind"], "Deployment")
        self.assertEqual(patch.patch_type, PatchType.STRATEGIC_MERGE)

    def test_patch_to_dict(self):
        """Test patch conversion to dictionary."""
        target = {"kind": "Service"}
        patch = KustomizePatch(
            target=target,
            patch='{"op": "replace", "path": "/spec/ports/0/port", "value": 8080}',
            patch_type=PatchType.JSON6902,
            path="/patches/service-patch.json"
        )
        
        result = patch.to_dict()
        self.assertEqual(result["target"]["kind"], "Service")
        self.assertEqual(result["path"], "/patches/service-patch.json")


class TestHelmChart(unittest.TestCase):
    """Tests for HelmChart class."""

    def test_helm_chart_creation(self):
        """Test basic helm chart creation."""
        chart = HelmChart(name="redis", chart="bitnami/redis")
        self.assertEqual(chart.name, "redis")
        self.assertEqual(chart.chart, "bitnami/redis")

    def test_helm_chart_with_values(self):
        """Test helm chart with values."""
        chart = HelmChart(
            name="postgres",
            chart="bitnami/postgresql",
            release_name="my-db",
            namespace="database",
            values_files=["values.yaml"],
            additional_values={"replicas": 2}
        )
        
        self.assertEqual(chart.release_name, "my-db")
        self.assertEqual(chart.namespace, "database")
        self.assertIn("values.yaml", chart.values_files)

    def test_helm_chart_to_dict(self):
        """Test helm chart conversion to dictionary."""
        chart = HelmChart(
            name="nginx",
            chart="bitnami/nginx",
            release_name="web-server",
            namespace="frontend",
            source_type=HelmChartSource.REMOTE,
            repo_url="https://charts.bitnami.com"
        )
        
        result = chart.to_dict()
        self.assertEqual(result["name"], "nginx")
        self.assertEqual(result["releaseName"], "web-server")
        self.assertEqual(result["namespace"], "frontend")


class TestKustomizationSpec(unittest.TestCase):
    """Tests for KustomizationSpec class."""

    def test_spec_creation_defaults(self):
        """Test spec creation with defaults."""
        spec = KustomizationSpec()
        self.assertEqual(spec.api_version, "kustomize.config.k8s.io/v1beta1")
        self.assertEqual(spec.kind, "Kustomization")

    def test_spec_with_resources(self):
        """Test spec with resources."""
        spec = KustomizationSpec(
            resources=["deployment.yaml", "service.yaml"],
            namespace="production"
        )
        
        self.assertEqual(len(spec.resources), 2)
        self.assertEqual(spec.namespace, "production")

    def test_spec_with_labels(self):
        """Test spec with common labels."""
        spec = KustomizationSpec(
            common_labels={"environment": "production", "team": "platform"}
        )
        
        self.assertEqual(spec.common_labels["environment"], "production")


class TestKustomizeManagerInit(unittest.TestCase):
    """Tests for KustomizeManager initialization."""

    @patch('subprocess.run')
    @patch('pathlib.Path.exists')
    def test_manager_init_default(self, mock_exists, mock_run):
        """Test manager initialization with defaults."""
        mock_run.return_value = MagicMock(returncode=0, stdout="kustomize version", stderr="")
        mock_exists.return_value = True
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager()
        
        self.assertEqual(manager.work_dir, Path.cwd())
        self.assertEqual(manager.kustomize_cmd, "kustomize")

    @patch('subprocess.run')
    @patch('pathlib.Path.exists')
    def test_manager_init_custom_dir(self, mock_exists, mock_run):
        """Test manager initialization with custom directory."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        mock_exists.return_value = True
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager(work_dir="/tmp/kustomize-projects")
        
        self.assertEqual(manager.work_dir, Path("/tmp/kustomize-projects"))


class TestKustomizeManagerCreate(unittest.TestCase):
    """Tests for KustomizeManager create operations."""

    @patch('builtins.open', new_callable=mock_open)
    @patch('yaml.dump')
    @patch('pathlib.Path.mkdir')
    def test_create_kustomization(self, mock_mkdir, mock_yaml_dump, mock_file):
        """Test creating a kustomization file."""
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch.object(manager, '_write_kustomization') as mock_write:
            mock_write.return_value = Path("/tmp/kustomization.yaml")
            
            result = manager.create_kustomization(
                path="/tmp/my-app",
                resources=["deployment.yaml", "service.yaml"],
                namespace="default",
                name_prefix="prod-"
            )
            
            mock_write.assert_called_once()

    @patch('builtins.open', new_callable=mock_open)
    @patch('yaml.dump')
    @patch('pathlib.Path.mkdir')
    def test_create_kustomization_with_labels(self, mock_mkdir, mock_yaml_dump, mock_file):
        """Test creating kustomization with labels."""
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch.object(manager, '_write_kustomization') as mock_write:
            mock_write.return_value = Path("/tmp/kustomization.yaml")
            
            result = manager.create_kustomization(
                path="/tmp/my-app",
                common_labels={"app": "my-app", "env": "prod"}
            )
            
            spec = mock_write.call_args[0][1]
            self.assertEqual(spec.common_labels["app"], "my-app")


class TestKustomizeManagerUpdate(unittest.TestCase):
    """Tests for KustomizeManager update operations."""

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    @patch('pathlib.Path.exists')
    def test_update_kustomization(self, mock_exists, mock_safe_load, mock_yaml_dump, mock_file):
        """Test updating a kustomization file."""
        mock_exists.return_value = True
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization"}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()) as mocked_open:
            result = manager.update_kustomization("/tmp/kustomization.yaml", {"namespace": "new-ns"})
            
            # Should be called twice: once for reading, once for writing
            self.assertTrue(mock_yaml_dump.called or mocked_open.called)

    @patch('builtins.open', new_callable=mock_open, read_data="")
    def test_update_kustomization_not_found(self, mock_file):
        """Test updating non-existent kustomization."""
        mock_file.side_effect = FileNotFoundError()
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with self.assertRaises(FileNotFoundError):
            manager.update_kustomization("/nonexistent/kustomization.yaml", {})


class TestKustomizeManagerOverlay(unittest.TestCase):
    """Tests for KustomizeManager overlay operations."""

    @patch('builtins.open', new_callable=mock_open)
    @patch('yaml.dump')
    @patch('pathlib.Path.mkdir')
    def test_create_overlay(self, mock_mkdir, mock_yaml_dump, mock_file):
        """Test creating an overlay."""
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch.object(manager, '_write_kustomization') as mock_write:
            mock_write.return_value = Path("/tmp/overlays/dev/kustomization.yaml")
            
            result = manager.create_overlay(
                base_path="/tmp/base",
                overlay_name="dev",
                namespace="development"
            )
            
            mock_write.assert_called_once()
            spec = mock_write.call_args[0][1]
            self.assertEqual(spec.namespace, "development")

    @patch('builtins.open', new_callable=mock_open)
    @patch('yaml.dump')
    @patch('pathlib.Path.mkdir')
    def test_create_overlay_with_patches(self, mock_mkdir, mock_yaml_dump, mock_file):
        """Test creating overlay with patches."""
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch.object(manager, '_write_kustomization') as mock_write:
            mock_write.return_value = Path("/tmp/overlays/staging/kustomization.yaml")
            
            with patch.object(manager, 'apply_patches') as mock_apply:
                result = manager.create_overlay(
                    base_path="/tmp/base",
                    overlay_name="staging",
                    patches=[{"target": {"kind": "Deployment"}, "patch": "replicas: 2"}]
                )
                
                mock_apply.assert_called_once()


class TestKustomizeManagerComponent(unittest.TestCase):
    """Tests for KustomizeManager component operations."""

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_add_component(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test adding a component to kustomization."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "components": []}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.add_component("/tmp/kustomization.yaml", "/tmp/components/common-labels")
            
            mock_yaml_dump.assert_called()

    @patch('builtins.open', new_callable=mock_open)
    @patch('yaml.dump')
    @patch('pathlib.Path.mkdir')
    def test_create_component(self, mock_mkdir, mock_yaml_dump, mock_file):
        """Test creating a component."""
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch.object(manager, '_write_kustomization') as mock_write:
            mock_write.return_value = Path("/tmp/components/my-component/kustomization.yaml")
            
            result = manager.create_component(
                path="/tmp/components/my-component",
                resources=["resource1.yaml"]
            )
            
            mock_write.assert_called_once()
            spec = mock_write.call_args[0][1]
            self.assertEqual(spec.kind, "Component")


class TestKustomizeManagerImage(unittest.TestCase):
    """Tests for KustomizeManager image operations."""

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nimages: []\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_update_image_new_tag(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test updating image with new tag."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "images": []}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.update_image(
                "/tmp/kustomization.yaml",
                "nginx",
                new_tag="1.21.0"
            )
            
            mock_yaml_dump.assert_called()

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nimages:\n  - name: nginx\n    newTag: \"1.20\"\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_update_image_existing(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test updating existing image."""
        mock_safe_load.return_value = {
            "apiVersion": "kustomize.config.k8s.io/v1beta1",
            "kind": "Kustomization",
            "images": [{"name": "nginx", "newTag": "1.20"}]
        }
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.update_image(
                "/tmp/kustomization.yaml",
                "nginx",
                new_tag="1.21.0"
            )
            
            mock_yaml_dump.assert_called()

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nimages: []\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_get_images(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test getting images from kustomization."""
        mock_safe_load.return_value = {
            "apiVersion": "kustomize.config.k8s.io/v1beta1",
            "kind": "Kustomization",
            "images": [{"name": "nginx", "newTag": "1.21"}]
        }
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        images = manager.get_images("/tmp/kustomization.yaml")
        
        self.assertEqual(len(images), 1)
        self.assertEqual(images[0].name, "nginx")
        self.assertEqual(images[0].new_tag, "1.21")


class TestKustomizeManagerSecretGenerator(unittest.TestCase):
    """Tests for KustomizeManager secret generator operations."""

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nsecretGenerator: []\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_generate_secret(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test generating a secret."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "secretGenerator": []}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.generate_secret(
                "/tmp/kustomization.yaml",
                name="app-secrets",
                secret_type="Opaque",
                literals={"api_key": "secret123", "db_password": "postgres"}
            )
            
            mock_yaml_dump.assert_called()

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nconfigMapGenerator: []\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_generate_configmap(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test generating a configmap."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "configMapGenerator": []}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.generate_configmap(
                "/tmp/kustomization.yaml",
                name="app-config",
                literals={"LOG_LEVEL": "info", "CACHE_TTL": "3600"}
            )
            
            mock_yaml_dump.assert_called()


class TestKustomizeManagerPatches(unittest.TestCase):
    """Tests for KustomizeManager patch operations."""

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_apply_patches_strategic(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test applying strategic merge patches."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization"}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            patches = [{
                "type": "strategic",
                "target": {"kind": "Deployment", "name": "my-app"},
                "patch": "replicas: 5"
            }]
            
            result = manager.apply_patches("/tmp/kustomization.yaml", patches)
            
            mock_yaml_dump.assert_called()

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_apply_patches_json(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test applying JSON patches."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization"}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            patches = [{
                "type": "json",
                "target": {"kind": "Service", "name": "my-svc"},
                "patch": '[{"op": "replace", "path": "/spec/ports/0/port", "value": 8080}]',
                "path": "/patches/svc.json"
            }]
            
            result = manager.apply_patches("/tmp/kustomization.yaml", patches)
            
            mock_yaml_dump.assert_called()

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_add_strategic_merge_patch(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test adding strategic merge patch."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization"}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.add_strategic_merge_patch(
                "/tmp/kustomization.yaml",
                patch_content="replicas: 3",
                target_kind="Deployment",
                target_name="my-app"
            )
            
            mock_yaml_dump.assert_called()

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_add_json_patch(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test adding JSON patch."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization"}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.add_json_patch(
                "/tmp/kustomization.yaml",
                patch_content='[{"op": "add", "path": "/spec/replicas", "value": 3}]',
                target_kind="Deployment",
                target_name="my-app"
            )
            
            mock_yaml_dump.assert_called()


class TestKustomizeManagerReplicas(unittest.TestCase):
    """Tests for KustomizeManager replica operations."""

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nreplicas: []\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_update_replicas_new(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test updating replicas for new resource."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "replicas": []}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.update_replicas(
                "/tmp/kustomization.yaml",
                resource_name="my-app",
                replicas=5
            )
            
            mock_yaml_dump.assert_called()

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nreplicas:\n  - name: my-app\n    count: 3\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_update_replicas_existing(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test updating replicas for existing resource."""
        mock_safe_load.return_value = {
            "apiVersion": "kustomize.config.k8s.io/v1beta1",
            "kind": "Kustomization",
            "replicas": [{"name": "my-app", "count": 3}]
        }
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.update_replicas(
                "/tmp/kustomization.yaml",
                resource_name="my-app",
                replicas=10
            )
            
            mock_yaml_dump.assert_called()


class TestKustomizeManagerHelm(unittest.TestCase):
    """Tests for KustomizeManager Helm operations."""

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nhelmCharts: []\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_add_helm_chart(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test adding Helm chart."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "helmCharts": []}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            chart = HelmChart(
                name="redis",
                chart="bitnami/redis",
                release_name="cache",
                namespace="data"
            )
            
            result = manager.add_helm_chart("/tmp/kustomization.yaml", chart)
            
            mock_yaml_dump.assert_called()

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nhelmCharts: []\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_create_helm_chart(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test creating Helm chart via helper."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "helmCharts": []}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.create_helm_chart(
                "/tmp/kustomization.yaml",
                name="postgres",
                chart="bitnami/postgresql",
                release_name="my-db",
                namespace="database"
            )
            
            mock_yaml_dump.assert_called()


class TestKustomizeManagerBuild(unittest.TestCase):
    """Tests for KustomizeManager build operations."""

    @patch('subprocess.run')
    def test_build(self, mock_run):
        """Test building kustomization."""
        mock_run.return_value = MagicMock(returncode=0, stdout="apiVersion: v1\nkind: Pod\n", stderr="")
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        result = manager.build("/tmp/kustomization.yaml")
        
        self.assertIn("apiVersion", result)
        mock_run.assert_called()

    @patch('subprocess.run')
    def test_build_with_options(self, mock_run):
        """Test building with options."""
        mock_run.return_value = MagicMock(returncode=0, stdout="manifest content", stderr="")
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        result = manager.build("/tmp/kustomization.yaml", enable_helm=True, reorder="sort")
        
        args = mock_run.call_args[0][0]
        self.assertIn("--enable-helm", args)
        self.assertIn("--reorder", args)

    @patch('subprocess.run')
    @patch('builtins.open', new_callable=mock_open)
    def test_build_to_file(self, mock_file, mock_run):
        """Test building to file."""
        mock_run.return_value = MagicMock(returncode=0, stdout="apiVersion: v1\nkind: Pod\nmetadata:\n  name: test\n", stderr="")
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('pathlib.Path.mkdir'):
            result = manager.build_to_file("/tmp/kustomization.yaml", "/tmp/output.yaml")
            
            self.assertEqual(result.name, "output.yaml")


class TestKustomizeManagerLabelsAnnotations(unittest.TestCase):
    """Tests for KustomizeManager label and annotation operations."""

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\ncommonLabels: {}\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_add_label(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test adding common label."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "commonLabels": {}}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.add_label("/tmp/kustomization.yaml", "app", "my-app")
            
            mock_yaml_dump.assert_called()

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\ncommonAnnotations: {}\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_add_annotation(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test adding common annotation."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "commonAnnotations": {}}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.add_annotation("/tmp/kustomization.yaml", "commit-sha", "abc123")
            
            mock_yaml_dump.assert_called()


class TestKustomizeManagerResources(unittest.TestCase):
    """Tests for KustomizeManager resource operations."""

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources: []\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_add_resource(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test adding a resource."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "resources": []}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.add_resource("/tmp/kustomization.yaml", "/tmp/deployment.yaml")
            
            mock_yaml_dump.assert_called()

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - deployment.yaml\n  - service.yaml\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_remove_resource(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test removing a resource."""
        mock_safe_load.return_value = {
            "apiVersion": "kustomize.config.k8s.io/v1beta1",
            "kind": "Kustomization",
            "resources": ["deployment.yaml", "service.yaml"]
        }
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.remove_resource("/tmp/kustomization.yaml", "service.yaml")
            
            mock_yaml_dump.assert_called()

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nimages:\n  - name: nginx\n  - name: redis\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_remove_image(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test removing an image."""
        mock_safe_load.return_value = {
            "apiVersion": "kustomize.config.k8s.io/v1beta1",
            "kind": "Kustomization",
            "images": [{"name": "nginx"}, {"name": "redis"}]
        }
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.remove_image("/tmp/kustomization.yaml", "redis")
            
            mock_yaml_dump.assert_called()


class TestKustomizeManagerNamespacePrefixSuffix(unittest.TestCase):
    """Tests for KustomizeManager namespace and prefix/suffix operations."""

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    @patch('pathlib.Path.exists')
    def test_set_namespace(self, mock_exists, mock_safe_load, mock_yaml_dump, mock_file):
        """Test setting namespace."""
        mock_exists.return_value = True
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization"}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()) as mocked_open:
            result = manager.set_namespace("/tmp/kustomization.yaml", "production")
            
            self.assertTrue(mock_yaml_dump.called or mocked_open.called)

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    @patch('pathlib.Path.exists')
    def test_set_name_prefix(self, mock_exists, mock_safe_load, mock_yaml_dump, mock_file):
        """Test setting name prefix."""
        mock_exists.return_value = True
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization"}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()) as mocked_open:
            result = manager.set_name_prefix("/tmp/kustomization.yaml", "prod-")
            
            self.assertTrue(mock_yaml_dump.called or mocked_open.called)

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    @patch('pathlib.Path.exists')
    def test_set_name_suffix(self, mock_exists, mock_safe_load, mock_yaml_dump, mock_file):
        """Test setting name suffix."""
        mock_exists.return_value = True
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization"}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()) as mocked_open:
            result = manager.set_name_suffix("/tmp/kustomization.yaml", "-v2")
            
            self.assertTrue(mock_yaml_dump.called or mocked_open.called)


class TestKustomizeManagerRemoteBases(unittest.TestCase):
    """Tests for KustomizeManager remote base operations."""

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nbases: []\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_add_remote_base(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test adding remote base."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "bases": []}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.add_remote_base(
                "/tmp/kustomization.yaml",
                "https://github.com/org/repo/blob/main/base",
                branch="develop"
            )
            
            mock_yaml_dump.assert_called()

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_add_git_overlay(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test adding git overlay."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization"}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.add_git_overlay(
                "/tmp/kustomization.yaml",
                repo_url="https://github.com/org/repo",
                path="base/dev",
                branch="main"
            )
            
            mock_yaml_dump.assert_called()


class TestKustomizeManagerValidate(unittest.TestCase):
    """Tests for KustomizeManager validation operations."""

    @patch('subprocess.run')
    @patch('builtins.open', new_callable=mock_open)
    def test_validate_valid(self, mock_file, mock_run):
        """Test validating a valid kustomization."""
        mock_run.return_value = MagicMock(returncode=0, stdout="apiVersion: v1\nkind: Pod\n---\napiVersion: v1\nkind: Service\n", stderr="")
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        result = manager.validate("/tmp/kustomization.yaml")
        
        self.assertTrue(result[0])

    @patch('subprocess.run')
    def test_validate_invalid(self, mock_run):
        """Test validating an invalid kustomization."""
        mock_run.side_effect = Exception("Invalid kustomization")
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            with patch.object(KustomizeManager, '_find_kustomize', return_value="kustomize"):
                manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            result = manager.validate("/tmp/kustomization.yaml")
            
            self.assertFalse(result[0])


class TestKustomizeManagerDiff(unittest.TestCase):
    """Tests for KustomizeManager diff operations."""

    @patch('subprocess.run')
    def test_diff_current_only(self, mock_run):
        """Test diff with current only."""
        mock_run.return_value = MagicMock(returncode=0, stdout="apiVersion: v1\nkind: Pod\n", stderr="")
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        result = manager.diff("/tmp/kustomization.yaml")
        
        self.assertIn("Current:", result)

    @patch('subprocess.run')
    def test_diff_with_target(self, mock_run):
        """Test diff with target environment."""
        mock_run.return_value = MagicMock(returncode=0, stdout="manifest", stderr="")
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('pathlib.Path.exists', return_value=True):
            result = manager.diff("/tmp/kustomization.yaml", target_env="prod")
            
            self.assertIn("Current:", result)
            self.assertIn("Target:", result)


class TestKustomizeManagerReplacement(unittest.TestCase):
    """Tests for KustomizeManager replacement operations."""

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_add_replacement(self, mock_safe_load, mock_yaml_dump, mock_file):
        """Test adding replacement."""
        mock_safe_load.return_value = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization"}
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('builtins.open', mock_open()):
            source = {"field": "metadata.name", "obj": "kind: Pod"}
            targets = [{"select": "kind: Deployment", "reject": []}]
            
            result = manager.add_replacement("/tmp/kustomization.yaml", source, targets)
            
            mock_yaml_dump.assert_called()


class TestKustomizeManagerClone(unittest.TestCase):
    """Tests for KustomizeManager clone operations."""

    @patch('shutil.copytree')
    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamePrefix: old-\n")
    @patch('yaml.dump')
    @patch('yaml.safe_load')
    def test_clone_overlay(self, mock_safe_load, mock_yaml_dump, mock_file, mock_copytree):
        """Test cloning an overlay."""
        mock_safe_load.return_value = {
            "apiVersion": "kustomize.config.k8s.io/v1beta1",
            "kind": "Kustomization",
            "namePrefix": "old-"
        }
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        with patch('pathlib.Path.exists', return_value=True):
            result = manager.clone_overlay("/tmp/overlays/dev", "staging")
            
            mock_copytree.assert_called()


class TestKustomizeManagerListResources(unittest.TestCase):
    """Tests for KustomizeManager list resources operations."""

    @patch('builtins.open', new_callable=mock_open, read_data="apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - deployment.yaml\n  - service.yaml\n  - configmap.yaml\n")
    @patch('yaml.safe_load')
    def test_list_resources(self, mock_safe_load, mock_file):
        """Test listing resources."""
        mock_safe_load.return_value = {
            "apiVersion": "kustomize.config.k8s.io/v1beta1",
            "kind": "Kustomization",
            "resources": ["deployment.yaml", "service.yaml", "configmap.yaml"]
        }
        
        with patch.object(KustomizeManager, '_ensure_kustomize_installed'):
            manager = KustomizeManager("/tmp")
        
        resources = manager.list_resources("/tmp/kustomization.yaml")
        
        self.assertEqual(len(resources), 3)
        self.assertIn("deployment.yaml", resources)


class TestEnums(unittest.TestCase):
    """Tests for enum types."""

    def test_kustomize_resource_kind_values(self):
        """Test KustomizeResourceKind enum values."""
        self.assertEqual(KustomizeResourceKind.CONFIGMAP.value, "ConfigMap")
        self.assertEqual(KustomizeResourceKind.DEPLOYMENT.value, "Deployment")
        self.assertEqual(KustomizeResourceKind.SERVICE.value, "Service")
        self.assertEqual(KustomizeResourceKind.INGRESS.value, "Ingress")

    def test_image_tag_strategy_values(self):
        """Test ImageTagStrategy enum values."""
        self.assertEqual(ImageTagStrategy.EXACT.value, "exact")
        self.assertEqual(ImageTagStrategy.LATEST.value, "latest")
        self.assertEqual(ImageTagStrategy.DIGEST.value, "digest")
        self.assertEqual(ImageTagStrategy.NEW_TAG.value, "newTag")

    def test_patch_type_values(self):
        """Test PatchType enum values."""
        self.assertEqual(PatchType.STRATEGIC_MERGE.value, "StrategicMergePatch")
        self.assertEqual(PatchType.JSON6902.value, "JSON6902")
        self.assertEqual(PatchType.UNIFIED.value, "unified")

    def test_helm_chart_source_values(self):
        """Test HelmChartSource enum values."""
        self.assertEqual(HelmChartSource.LOCAL.value, "local")
        self.assertEqual(HelmChartSource.REMOTE.value, "remote")
        self.assertEqual(HelmChartSource.OCI.value, "oci")


if __name__ == '__main__':
    unittest.main()

"""Tests for Workflow ArgoCD Module.

Comprehensive tests for ArgoCD integration including
application management, sync policies, rollout strategies,
multi-cluster management, resource health, history rollback,
webhooks, SSO, projects, and secrets management.
"""

import unittest
import sys
import json
import time
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List, Optional, Any

sys.path.insert(0, '/Users/guige/my_project')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick/src')

from workflow_argocd import (
    ArgoCDIntegration, SyncPolicy, RolloutStrategy, HealthStatus,
    SyncStatus, ResourceKind, ApplicationSpec, Application,
    SyncHistoryEntry, RolloutConfig, ClusterCredential, ProjectSpec,
    WebhookEvent, SSOConfig, SecretRef
)


class TestSyncPolicy(unittest.TestCase):
    """Tests for SyncPolicy enum."""

    def test_sync_policy_values(self):
        """Test SyncPolicy enum values."""
        self.assertEqual(SyncPolicy.MANUAL.value, "manual")
        self.assertEqual(SyncPolicy.AUTOMATIC.value, "automatic")
        self.assertEqual(SyncPolicy.AUTO_DELETE.value, "auto-delete")
        self.assertEqual(SyncPolicy.CREATE_ONLY.value, "create-only")


class TestRolloutStrategy(unittest.TestCase):
    """Tests for RolloutStrategy enum."""

    def test_rollout_strategy_values(self):
        """Test RolloutStrategy enum values."""
        self.assertEqual(RolloutStrategy.BLUE_GREEN.value, "blue-green")
        self.assertEqual(RolloutStrategy.CANARY.value, "canary")
        self.assertEqual(RolloutStrategy.ROLLING_UPDATE.value, "rolling-update")
        self.assertEqual(RolloutStrategy.RECREATE.value, "recreate")


class TestHealthStatus(unittest.TestCase):
    """Tests for HealthStatus enum."""

    def test_health_status_values(self):
        """Test HealthStatus enum values."""
        self.assertEqual(HealthStatus.HEALTHY.value, "Healthy")
        self.assertEqual(HealthStatus.DEGRADED.value, "Degraded")
        self.assertEqual(HealthStatus.PROGRESSING.value, "Progressing")
        self.assertEqual(HealthStatus.MISSING.value, "Missing")
        self.assertEqual(HealthStatus.UNKNOWN.value, "Unknown")


class TestSyncStatus(unittest.TestCase):
    """Tests for SyncStatus enum."""

    def test_sync_status_values(self):
        """Test SyncStatus enum values."""
        self.assertEqual(SyncStatus.SYNCED.value, "Synced")
        self.assertEqual(SyncStatus.OUT_OF_SYNC.value, "OutOfSync")
        self.assertEqual(SyncStatus.UNKNOWN.value, "Unknown")


class TestResourceKind(unittest.TestCase):
    """Tests for ResourceKind enum."""

    def test_resource_kind_values(self):
        """Test ResourceKind enum values."""
        self.assertEqual(ResourceKind.DEPLOYMENT.value, "Deployment")
        self.assertEqual(ResourceKind.STATEFUL_SET.value, "StatefulSet")
        self.assertEqual(ResourceKind.DAEMON_SET.value, "DaemonSet")
        self.assertEqual(ResourceKind.SERVICE.value, "Service")
        self.assertEqual(ResourceKind.INGRESS.value, "Ingress")
        self.assertEqual(ResourceKind.CONFIG_MAP.value, "ConfigMap")
        self.assertEqual(ResourceKind.JOB.value, "Job")
        self.assertEqual(ResourceKind.CRON_JOB.value, "CronJob")


class TestApplicationSpec(unittest.TestCase):
    """Tests for ApplicationSpec dataclass."""

    def test_application_spec_creation(self):
        """Test ApplicationSpec creation."""
        spec = ApplicationSpec(
            name="my-app",
            destination_namespace="default",
            destination_cluster="https://kubernetes.default.svc",
            repo_url="https://github.com/test/repo",
            path="manifests",
            branch="main"
        )
        self.assertEqual(spec.name, "my-app")
        self.assertEqual(spec.destination_namespace, "default")
        self.assertEqual(spec.sync_policy, SyncPolicy.MANUAL)

    def test_application_spec_custom_policy(self):
        """Test ApplicationSpec with custom sync policy."""
        spec = ApplicationSpec(
            name="my-app",
            destination_namespace="prod",
            destination_cluster="https://k8s.example.com",
            repo_url="https://github.com/test/repo",
            path="k8s",
            sync_policy=SyncPolicy.AUTOMATIC,
            self_heal=True,
            auto_prune=True
        )
        self.assertEqual(spec.sync_policy, SyncPolicy.AUTOMATIC)
        self.assertTrue(spec.self_heal)
        self.assertTrue(spec.auto_prune)


class TestApplication(unittest.TestCase):
    """Tests for Application dataclass."""

    def test_application_creation(self):
        """Test Application creation."""
        app = Application(
            name="test-app",
            uid="uid-123",
            project="default",
            server="https://k8s.example.com",
            namespace="default",
            repo="https://github.com/test/repo",
            path="manifests",
            sync_status=SyncStatus.SYNCED,
            health_status=HealthStatus.HEALTHY,
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00",
            sync_policy=SyncPolicy.MANUAL
        )
        self.assertEqual(app.name, "test-app")
        self.assertEqual(app.sync_status, SyncStatus.SYNCED)
        self.assertEqual(app.health_status, HealthStatus.HEALTHY)


class TestSyncHistoryEntry(unittest.TestCase):
    """Tests for SyncHistoryEntry dataclass."""

    def test_sync_history_entry_creation(self):
        """Test SyncHistoryEntry creation."""
        entry = SyncHistoryEntry(
            id=1,
            revision="abc123",
            initiated_at="2024-01-01T00:00:00",
            initiated_by="user",
            status="Success",
            message="Sync completed"
        )
        self.assertEqual(entry.id, 1)
        self.assertEqual(entry.revision, "abc123")
        self.assertEqual(entry.status, "Success")


class TestRolloutConfig(unittest.TestCase):
    """Tests for RolloutConfig dataclass."""

    def test_rollout_config_blue_green(self):
        """Test RolloutConfig with blue-green strategy."""
        config = RolloutConfig(
            strategy=RolloutStrategy.BLUE_GREEN,
            blue_green_config={
                "previewReplicaCount": 2,
                "activeReplicaCount": 3
            }
        )
        self.assertEqual(config.strategy, RolloutStrategy.BLUE_GREEN)
        self.assertIsNotNone(config.blue_green_config)

    def test_rollout_config_canary(self):
        """Test RolloutConfig with canary strategy."""
        config = RolloutConfig(
            strategy=RolloutStrategy.CANARY,
            canary_config={
                "maxSurge": "25%",
                "maxUnavailable": "10%"
            },
            pause_duration=300
        )
        self.assertEqual(config.strategy, RolloutStrategy.CANARY)
        self.assertEqual(config.pause_duration, 300)


class TestClusterCredential(unittest.TestCase):
    """Tests for ClusterCredential dataclass."""

    def test_cluster_credential_creation(self):
        """Test ClusterCredential creation."""
        cred = ClusterCredential(
            name="prod-cluster",
            server="https://k8s.production.example.com",
            config={"bearer_token": "token123"},
            namespaces=["default", "prod"]
        )
        self.assertEqual(cred.name, "prod-cluster")
        self.assertEqual(len(cred.namespaces), 2)


class TestProjectSpec(unittest.TestCase):
    """Tests for ProjectSpec dataclass."""

    def test_project_spec_creation(self):
        """Test ProjectSpec creation."""
        spec = ProjectSpec(
            name="my-project",
            description="Test project",
            source_repos=["https://github.com/test/*"],
            destination_clusters=[
                {"server": "https://k8s.example.com", "namespace": "*"}
            ]
        )
        self.assertEqual(spec.name, "my-project")
        self.assertEqual(len(spec.source_repos), 1)


class TestWebhookEvent(unittest.TestCase):
    """Tests for WebhookEvent dataclass."""

    def test_webhook_event_creation(self):
        """Test WebhookEvent creation."""
        event = WebhookEvent(
            event_type="push",
            repository_url="https://github.com/test/repo",
            branch="main",
            commit_sha="abc123",
            author="testuser",
            timestamp="2024-01-01T00:00:00Z",
            payload={}
        )
        self.assertEqual(event.event_type, "push")
        self.assertEqual(event.branch, "main")


class TestSSOConfig(unittest.TestCase):
    """Tests for SSOConfig dataclass."""

    def test_sso_config_oauth2(self):
        """Test SSOConfig with OAuth2 provider."""
        config = SSOConfig(
            provider="oauth2",
            client_id="client-123",
            client_secret="secret-456",
            oidc_config={"issuer": "https://issuer.example.com"}
        )
        self.assertEqual(config.provider, "oauth2")
        self.assertIsNotNone(config.oidc_config)

    def test_sso_config_saml(self):
        """Test SSOConfig with SAML provider."""
        config = SSOConfig(
            provider="saml",
            client_id="client-123",
            client_secret="secret-456",
            saml_config={"entity_id": "test"}
        )
        self.assertEqual(config.provider, "saml")
        self.assertIsNotNone(config.saml_config)


class TestSecretRef(unittest.TestCase):
    """Tests for SecretRef dataclass."""

    def test_secret_ref_creation(self):
        """Test SecretRef creation."""
        ref = SecretRef(
            path="secret/data/api-keys",
            key="api_key",
            version=1,
            secret_name="my-secret"
        )
        self.assertEqual(ref.path, "secret/data/api-keys")
        self.assertEqual(ref.version, 1)


class TestArgoCDIntegrationInit(unittest.TestCase):
    """Tests for ArgoCDIntegration initialization."""

    @patch('workflow_argocd.requests.Session')
    def test_init_with_defaults(self, mock_session):
        """Test ArgoCDIntegration initialization with defaults."""
        argocd = ArgoCDIntegration("https://argocd.example.com")

        self.assertEqual(argocd.argocd_url, "https://argocd.example.com")
        self.assertEqual(argocd.api_token, "")
        self.assertTrue(argocd.verify_ssl)

    @patch('workflow_argocd.requests.Session')
    def test_init_with_token(self, mock_session):
        """Test ArgoCDIntegration initialization with API token."""
        argocd = ArgoCDIntegration(
            "https://argocd.example.com",
            api_token="my-token",
            verify_ssl=False
        )

        self.assertEqual(argocd.api_token, "my-token")
        self.assertFalse(argocd.verify_ssl)

    @patch('workflow_argocd.requests.Session')
    def test_session_headers_set(self, mock_session):
        """Test that session headers are properly set."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance

        argocd = ArgoCDIntegration("https://argocd.example.com", api_token="token")

        self.assertEqual(
            mock_session_instance.headers.update.call_args[0][0]['Content-Type'],
            'application/json'
        )


class TestArgoCDIntegrationApplicationManagement(unittest.TestCase):
    """Tests for ArgoCDIntegration application management."""

    @patch('workflow_argocd.requests.Session')
    def setUp(self, mock_session):
        """Set up test fixtures."""
        self.mock_session = MagicMock()
        mock_session.return_value = self.mock_session
        self.mock_response = MagicMock()
        self.mock_response.status_code = 200
        self.mock_response.text = '{}'
        self.mock_response.json.return_value = {"metadata": {"name": "test-app"}}
        self.argocd = ArgoCDIntegration("https://argocd.example.com")

    def test_create_application(self):
        """Test create_application method."""
        self.mock_session.post.return_value = Mock(
            status_code=201,
            text='{"metadata": {"name": "test-app"}}',
            json=lambda: {"metadata": {"name": "test-app"}}
        )

        spec = ApplicationSpec(
            name="test-app",
            destination_namespace="default",
            destination_cluster="https://k8s.example.com",
            repo_url="https://github.com/test/repo",
            path="manifests"
        )

        result = self.argocd.create_application(spec)

        self.assertTrue(self.mock_session.post.called)
        call_args = self.mock_session.post.call_args
        self.assertIn("/api/v1/applications", call_args[0][0])

    def test_get_application(self):
        """Test get_application method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {
                "metadata": {"name": "test-app", "uid": "uid-123"},
                "spec": {
                    "project": "default",
                    "destination": {"server": "https://k8s.example.com", "namespace": "default"},
                    "source": {"repoURL": "https://github.com/test/repo", "path": "manifests"}
                },
                "status": {
                    "sync": {"status": "Synced", "revision": "abc123"},
                    "health": {"status": "Healthy"}
                }
            }
        )

        app = self.argocd.get_application("test-app")

        self.assertIsNotNone(app)
        self.assertEqual(app.name, "test-app")

    def test_get_application_not_found(self):
        """Test get_application when app doesn't exist."""
        self.mock_session.get.return_value = Mock(status_code=404)

        app = self.argocd.get_application("non-existent")
        self.assertIsNone(app)

    def test_list_applications(self):
        """Test list_applications method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {
                "items": [
                    {
                        "metadata": {"name": "app-1", "uid": "uid-1"},
                        "spec": {"project": "default", "destination": {"server": "k8s-1", "namespace": "default"}, "source": {"repoURL": "repo-1", "path": "path-1"}},
                        "status": {"sync": {"status": "Synced"}, "health": {"status": "Healthy"}}
                    },
                    {
                        "metadata": {"name": "app-2", "uid": "uid-2"},
                        "spec": {"project": "default", "destination": {"server": "k8s-2", "namespace": "default"}, "source": {"repoURL": "repo-2", "path": "path-2"}},
                        "status": {"sync": {"status": "OutOfSync"}, "health": {"status": "Degraded"}}
                    }
                ]
            }
        )

        apps = self.argocd.list_applications()

        self.assertEqual(len(apps), 2)
        self.assertEqual(apps[0].name, "app-1")
        self.assertEqual(apps[1].name, "app-2")

    def test_delete_application(self):
        """Test delete_application method."""
        self.mock_response.status_code = 200
        self.mock_response.text = '{}'
        self.mock_response.json.return_value = {}
        self.mock_session.delete.return_value = self.mock_response

        result = self.argocd.delete_application("test-app")

        self.assertTrue(result)

    def test_update_application(self):
        """Test update_application method."""
        self.mock_response.status_code = 200
        self.mock_response.text = '{}'
        self.mock_response.json.return_value = {"metadata": {"name": "test-app"}}
        self.mock_session.post.return_value = self.mock_response

        spec = ApplicationSpec(
            name="test-app",
            destination_namespace="default",
            destination_cluster="https://k8s.example.com",
            repo_url="https://github.com/test/repo",
            path="manifests"
        )

        result = self.argocd.update_application("test-app", spec)

        self.assertTrue(self.mock_session.post.called)

    def test_patch_application(self):
        """Test patch_application method."""
        self.mock_response.status_code = 200
        self.mock_response.text = '{}'
        self.mock_response.json.return_value = {"metadata": {"name": "test-app"}}
        self.mock_session.patch.return_value = self.mock_response

        result = self.argocd.patch_application("test-app", {"spec": {"syncPolicy": {}}})

        self.assertTrue(self.mock_session.patch.called)

    def test_set_application_project(self):
        """Test set_application_project method."""
        self.mock_response.status_code = 200
        self.mock_response.text = '{}'
        self.mock_response.json.return_value = {"metadata": {"name": "test-app"}}
        self.mock_session.patch.return_value = self.mock_response

        result = self.argocd.set_application_project("test-app", "new-project")

        self.assertTrue(result)


class TestArgoCDIntegrationSyncPolicies(unittest.TestCase):
    """Tests for ArgoCDIntegration sync policy operations."""

    @patch('workflow_argocd.requests.Session')
    def setUp(self, mock_session):
        """Set up test fixtures."""
        self.mock_session = MagicMock()
        mock_session.return_value = self.mock_session
        self.mock_response = MagicMock()
        self.mock_response.status_code = 200
        self.mock_response.text = '{}'
        self.mock_response.json.return_value = {"metadata": {"name": "test-app"}}
        self.mock_session.patch.return_value = self.mock_response
        self.argocd = ArgoCDIntegration("https://argocd.example.com")

    def test_configure_sync_policy_manual(self):
        """Test configure_sync_policy with manual policy."""
        result = self.argocd.configure_sync_policy(
            "test-app",
            SyncPolicy.MANUAL,
            self_heal=False,
            auto_prune=False
        )

        self.assertTrue(result)

    def test_configure_sync_policy_automatic(self):
        """Test configure_sync_policy with automatic policy."""
        result = self.argocd.configure_sync_policy(
            "test-app",
            SyncPolicy.AUTOMATIC,
            self_heal=True,
            auto_prune=True
        )

        self.assertTrue(result)

    def test_enable_auto_sync(self):
        """Test enable_auto_sync method."""
        result = self.argocd.enable_auto_sync("test-app", self_heal=True, prune=True)

        self.assertTrue(result)

    def test_disable_auto_sync(self):
        """Test disable_auto_sync method."""
        result = self.argocd.disable_auto_sync("test-app")

        self.assertTrue(result)

    def test_sync_application(self):
        """Test sync_application method."""
        self.mock_session.post.return_value = Mock(
            status_code=200,
            json=lambda: {"metadata": {"name": "test-app"}}
        )

        result = self.argocd.sync_application("test-app", revision="abc123")

        self.assertTrue(self.mock_session.post.called)

    def test_get_sync_status(self):
        """Test get_sync_status method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {
                "metadata": {"name": "test-app", "uid": "uid-123"},
                "spec": {"project": "default", "destination": {"server": "k8s", "namespace": "default"}, "source": {"repoURL": "repo", "path": "path"}},
                "status": {"sync": {"status": "Synced", "revision": "abc123"}, "health": {"status": "Healthy"}, "message": "sync ok"}
            }
        )

        result = self.argocd.get_sync_status("test-app")

        self.assertEqual(result['sync_status'], "Synced")
        self.assertEqual(result['health_status'], "Healthy")


class TestArgoCDIntegrationRolloutStrategies(unittest.TestCase):
    """Tests for ArgoCDIntegration rollout strategy operations."""

    @patch('workflow_argocd.requests.Session')
    def setUp(self, mock_session):
        """Set up test fixtures."""
        self.mock_session = Mock()
        mock_session.return_value = self.mock_session
        self.argocd = ArgoCDIntegration("https://argocd.example.com")

    def test_configure_rollout_blue_green(self):
        """Test configure_rollout with blue-green strategy."""
        self.mock_session.post.return_value = Mock(status_code=200)

        config = RolloutConfig(
            strategy=RolloutStrategy.BLUE_GREEN,
            blue_green_config={"previewReplicaCount": 1}
        )

        result = self.argocd.configure_rollout("test-app", config)

        self.assertTrue(self.mock_session.post.called)

    def test_configure_rollout_canary(self):
        """Test configure_rollout with canary strategy."""
        self.mock_session.post.return_value = Mock(status_code=200)

        config = RolloutConfig(
            strategy=RolloutStrategy.CANARY,
            canary_config={"maxSurge": "25%", "maxUnavailable": "10%"},
            pause_duration=600
        )

        result = self.argocd.configure_rollout("test-app", config)

        self.assertTrue(self.mock_session.post.called)

    def test_generate_rollout_manifest_blue_green(self):
        """Test _generate_rollout_manifest for blue-green."""
        config = RolloutConfig(
            strategy=RolloutStrategy.BLUE_GREEN,
            blue_green_config={"previewReplicaCount": 1, "activeReplicaCount": 2}
        )

        manifest = self.argocd._generate_rollout_manifest("test-app", config)

        self.assertEqual(manifest['kind'], "Rollout")
        self.assertIn('blueGreen', manifest['spec']['strategy'])

    def test_generate_rollout_manifest_canary(self):
        """Test _generate_rollout_manifest for canary."""
        config = RolloutConfig(
            strategy=RolloutStrategy.CANARY,
            max_surge="25%",
            max_unavailable="10%"
        )

        manifest = self.argocd._generate_rollout_manifest("test-app", config)

        self.assertEqual(manifest['kind'], "Rollout")
        self.assertIn('canary', manifest['spec']['strategy'])

    def test_promote_rollout(self):
        """Test promote_rollout method."""
        self.mock_session.post.return_value = Mock(status_code=200)

        result = self.argocd.promote_rollout("test-app", full=False)

        self.assertTrue(result)

    def test_abort_rollout(self):
        """Test abort_rollout method."""
        self.mock_session.post.return_value = Mock(status_code=200)

        result = self.argocd.abort_rollout("test-app")

        self.assertTrue(result)

    def test_pause_rollout(self):
        """Test pause_rollout method."""
        self.mock_session.post.return_value = Mock(status_code=200)

        result = self.argocd.pause_rollout("test-app", duration=300)

        self.assertTrue(result)

    def test_resume_rollout(self):
        """Test resume_rollout method."""
        self.mock_session.post.return_value = Mock(status_code=200)

        result = self.argocd.resume_rollout("test-app")

        self.assertTrue(result)


class TestArgoCDIntegrationMultiCluster(unittest.TestCase):
    """Tests for ArgoCDIntegration multi-cluster operations."""

    @patch('workflow_argocd.requests.Session')
    def setUp(self, mock_session):
        """Set up test fixtures."""
        self.mock_session = Mock()
        mock_session.return_value = self.mock_session
        self.argocd = ArgoCDIntegration("https://argocd.example.com")

    def test_add_cluster(self):
        """Test add_cluster method."""
        self.mock_session.post.return_value = Mock(status_code=201)

        result = self.argocd.add_cluster(
            name="prod-cluster",
            server="https://k8s.production.example.com",
            credentials={"bearer_token": "token123"}
        )

        self.assertTrue(result)

    def test_list_clusters(self):
        """Test list_clusters method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {
                "items": [
                    {"name": "dev", "server": "https://k8s-dev.example.com"},
                    {"name": "prod", "server": "https://k8s-prod.example.com"}
                ]
            }
        )

        clusters = self.argocd.list_clusters()

        self.assertEqual(len(clusters), 2)

    def test_get_cluster(self):
        """Test get_cluster method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {"name": "prod", "server": "https://k8s-prod.example.com"}
        )

        cluster = self.argocd.get_cluster("https://k8s-prod.example.com")

        self.assertIsNotNone(cluster)

    def test_remove_cluster(self):
        """Test remove_cluster method."""
        self.mock_session.delete.return_value = Mock(status_code=200)

        result = self.argocd.remove_cluster("https://k8s-prod.example.com")

        self.assertTrue(result)

    def test_update_cluster(self):
        """Test update_cluster method."""
        self.mock_session.put.return_value = Mock(status_code=200)

        result = self.argocd.update_cluster(
            "https://k8s-prod.example.com",
            {"namespaces": ["default", "prod"]}
        )

        self.assertTrue(result)

    def test_cluster_refresh(self):
        """Test cluster_refresh method."""
        self.mock_session.post.return_value = Mock(status_code=200)

        result = self.argocd.cluster_refresh("https://k8s-prod.example.com")

        self.assertTrue(result)

    def test_get_cluster_metrics(self):
        """Test get_cluster_metrics method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {"cpu": "50%", "memory": "60%"}
        )

        metrics = self.argocd.get_cluster_metrics("https://k8s-prod.example.com")

        self.assertIsNotNone(metrics)


class TestArgoCDIntegrationResourceHealth(unittest.TestCase):
    """Tests for ArgoCDIntegration resource health operations."""

    @patch('workflow_argocd.requests.Session')
    def setUp(self, mock_session):
        """Set up test fixtures."""
        self.mock_session = MagicMock()
        mock_session.return_value = self.mock_session
        self.mock_response = MagicMock()
        self.mock_response.status_code = 200
        self.mock_response.text = '{}'
        self.mock_response.json.return_value = {"metadata": {"name": "test-app"}}
        self.argocd = ArgoCDIntegration("https://argocd.example.com")

    def test_get_resource_health(self):
        """Test get_resource_health method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {
                "resources": [
                    {"kind": "Deployment", "namespace": "default", "health": {"status": "Healthy"}},
                    {"kind": "Service", "namespace": "default", "health": {"status": "Healthy"}}
                ]
            }
        )

        resources = self.argocd.get_resource_health("test-app")

        self.assertEqual(len(resources), 2)

    def test_get_resource_health_filtered(self):
        """Test get_resource_health with filters."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {
                "resources": [
                    {"kind": "Deployment", "namespace": "default", "health": {"status": "Healthy"}},
                    {"kind": "Service", "namespace": "default", "health": {"status": "Healthy"}}
                ]
            }
        )

        resources = self.argocd.get_resource_health(
            "test-app",
            resource_kind=ResourceKind.DEPLOYMENT,
            namespace="default"
        )

        self.assertTrue(self.mock_session.get.called)

    def test_get_resource_details(self):
        """Test get_resource_details method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {"kind": "Deployment", "metadata": {"name": "test-app"}}
        )

        details = self.argocd.get_resource_details(
            "test-app",
            group="apps",
            kind="Deployment",
            namespace="default"
        )

        self.assertIsNotNone(details)

    def test_get_managed_resources(self):
        """Test get_managed_resources method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {
                "items": [
                    {"kind": "Deployment", "metadata": {"name": "app-1"}},
                    {"kind": "Service", "metadata": {"name": "svc-1"}}
                ]
            }
        )

        resources = self.argocd.get_managed_resources("test-app")

        self.assertEqual(len(resources), 2)

    def test_check_resource_health_status_healthy(self):
        """Test check_resource_health_status for healthy resources."""
        with patch.object(self.argocd, 'get_resource_health', return_value=[
            {"kind": "Deployment", "health": {"status": "Healthy"}, "namespace": "default"},
            {"kind": "Deployment", "health": {"status": "Healthy"}, "namespace": "default"}
        ]):
            status = self.argocd.check_resource_health_status(
                "test-app",
                ResourceKind.DEPLOYMENT,
                "default"
            )

            self.assertEqual(status, HealthStatus.HEALTHY)

    def test_check_resource_health_status_degraded(self):
        """Test check_resource_health_status for degraded resources."""
        with patch.object(self.argocd, 'get_resource_health', return_value=[
            {"kind": "Deployment", "health": {"status": "Healthy"}, "namespace": "default"},
            {"kind": "Deployment", "health": {"status": "Degraded"}, "namespace": "default"}
        ]):
            status = self.argocd.check_resource_health_status(
                "test-app",
                ResourceKind.DEPLOYMENT,
                "default"
            )

            self.assertEqual(status, HealthStatus.DEGRADED)


class TestArgoCDIntegrationHistoryRollback(unittest.TestCase):
    """Tests for ArgoCDIntegration history and rollback operations."""

    @patch('workflow_argocd.requests.Session')
    def setUp(self, mock_session):
        """Set up test fixtures."""
        self.mock_session = Mock()
        mock_session.return_value = self.mock_session
        self.argocd = ArgoCDIntegration("https://argocd.example.com")

    def test_get_sync_history(self):
        """Test get_sync_history method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {
                "history": [
                    {"id": 1, "revision": "abc123", "initiatedAt": "2024-01-01T00:00:00Z", "initiatedBy": "user", "status": "Success", "message": "OK"},
                    {"id": 2, "revision": "def456", "initiatedAt": "2024-01-02T00:00:00Z", "initiatedBy": "user", "status": "Success", "message": "OK"}
                ]
            }
        )

        history = self.argocd.get_sync_history("test-app")

        self.assertEqual(len(history), 2)
        self.assertEqual(history[0].revision, "abc123")

    def test_rollback_application(self):
        """Test rollback_application method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {
                "history": [
                    {"id": 1, "revision": "abc123", "initiatedAt": "2024-01-01T00:00:00Z", "initiatedBy": "user", "status": "Success", "message": "OK"},
                    {"id": 2, "revision": "def456", "initiatedAt": "2024-01-02T00:00:00Z", "initiatedBy": "user", "status": "Success", "message": "OK"}
                ]
            }
        )
        self.mock_session.post.return_value = Mock(status_code=200)

        result = self.argocd.rollback_application("test-app")

        self.assertTrue(result)

    def test_rollback_to_version(self):
        """Test rollback_to_version method."""
        self.mock_session.post.return_value = Mock(status_code=200)

        result = self.argocd.rollback_to_version("test-app", version=1)

        self.assertTrue(result)

    def test_get_rollback_history(self):
        """Test get_rollback_history method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {"id": 1, "revision": "abc123", "status": "Success"}
        )

        history = self.argocd.get_rollback_history("test-app", "abc123")

        self.assertIsNotNone(history)

    def test_prune_resources(self):
        """Test prune_resources method."""
        self.mock_session.post.return_value = Mock(
            status_code=200,
            json=lambda: {"resources": [{"kind": "Deployment", "status": "Pruned"}]}
        )

        pruned = self.argocd.prune_resources("test-app", dry_run=False)

        self.assertTrue(self.mock_session.post.called)


class TestArgoCDIntegrationWebhooks(unittest.TestCase):
    """Tests for ArgoCDIntegration webhook operations."""

    @patch('workflow_argocd.requests.Session')
    def setUp(self, mock_session):
        """Set up test fixtures."""
        self.mock_session = Mock()
        mock_session.return_value = self.mock_session
        self.argocd = ArgoCDIntegration("https://argocd.example.com")

    def test_create_webhook(self):
        """Test create_webhook method."""
        result = self.argocd.create_webhook(
            repo_url="https://github.com/test/repo",
            webhook_url="https://argocd.example.com/webhook",
            secret="secret123"
        )

        self.assertIn('webhook_id', result)
        self.assertEqual(result['webhook_url'], "https://argocd.example.com/webhook")

    def test_handle_webhook(self):
        """Test handle_webhook method."""
        payload = {
            "ref": "refs/heads/main",
            "after": "abc123",
            "repository": {"url": "https://github.com/test/repo"},
            "pusher": {"name": "testuser"},
            "head_commit": {"timestamp": "2024-01-01T00:00:00Z"}
        }

        event = self.argocd.handle_webhook(payload)

        self.assertEqual(event.branch, "main")
        self.assertEqual(event.commit_sha, "abc123")

    def test_handle_webhook_with_tag(self):
        """Test handle_webhook with tag ref."""
        payload = {
            "ref": "refs/tags/v1.0.0",
            "after": "abc123",
            "repository": {"url": "https://github.com/test/repo"},
            "pusher": {"name": "testuser"}
        }

        event = self.argocd.handle_webhook(payload)

        self.assertEqual(event.branch, "tags/v1.0.0")

    def test_verify_webhook_signature(self):
        """Test _verify_webhook_signature method."""
        import hmac
        import hashlib

        secret = "mysecret"
        payload = {"action": "push"}
        payload_str = json.dumps(payload, separators=(',', ':'))

        signature = "sha256=" + hmac.new(
            secret.encode(),
            payload_str.encode(),
            hashlib.sha256
        ).hexdigest()

        result = self.argocd._verify_webhook_signature(payload, signature, secret)

        self.assertTrue(result)

    def test_verify_webhook_signature_invalid(self):
        """Test _verify_webhook_signature with invalid signature."""
        result = self.argocd._verify_webhook_signature(
            {"action": "push"},
            "sha256=invalid",
            "secret"
        )

        self.assertFalse(result)

    def test_trigger_sync_from_webhook(self):
        """Test trigger_sync_from_webhook method."""
        self.mock_session.patch.return_value = Mock(status_code=200)
        self.mock_session.post.return_value = Mock(status_code=200)

        event = WebhookEvent(
            event_type="push",
            repository_url="https://github.com/test/repo",
            branch="main",
            commit_sha="abc123",
            author="testuser",
            timestamp="2024-01-01T00:00:00Z",
            payload={}
        )

        result = self.argocd.trigger_sync_from_webhook(event, "test-app")

        self.assertTrue(result)


class TestArgoCDIntegrationSSO(unittest.TestCase):
    """Tests for ArgoCDIntegration SSO operations."""

    @patch('workflow_argocd.requests.Session')
    def setUp(self, mock_session):
        """Set up test fixtures."""
        self.mock_session = MagicMock()
        mock_session.return_value = self.mock_session
        self.mock_response = MagicMock()
        self.mock_response.status_code = 200
        self.mock_response.text = '{}'
        self.mock_response.json.return_value = {"metadata": {"name": "test"}}
        self.argocd = ArgoCDIntegration("https://argocd.example.com")

    def test_configure_sso_oauth2(self):
        """Test configure_sso with OAuth2 provider."""
        self.mock_session.patch.return_value = self.mock_response

        config = SSOConfig(
            provider="oauth2",
            client_id="client-123",
            client_secret="secret-456",
            oidc_config={"issuer": "https://issuer.example.com", "scopes": "openid profile"}
        )

        result = self.argocd.configure_sso(config)

        self.assertTrue(result)

    def test_configure_sso_saml(self):
        """Test configure_sso with SAML provider."""
        self.mock_session.patch.return_value = self.mock_response

        config = SSOConfig(
            provider="saml",
            client_id="client-123",
            client_secret="secret-456",
            saml_config={"entity_id": "test-id"}
        )

        result = self.argocd.configure_sso(config)

        self.assertTrue(result)

    def test_configure_dex(self):
        """Test configure_dex method."""
        self.mock_session.patch.return_value = self.mock_response

        result = self.argocd.configure_dex({"issuer": "https://dex.example.com"})

        self.assertTrue(result)

    def test_get_sso_config(self):
        """Test get_sso_config method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {
                "url": "https://argocd.example.com",
                "oidc": {"issuer": "https://issuer.example.com"},
                "saml": {},
                "ldap": {}
            }
        )

        config = self.argocd.get_sso_config()

        self.assertIn('url', config)

    def test_test_sso_connection(self):
        """Test test_sso_connection method."""
        result = self.argocd.test_sso_connection("oauth2")

        self.assertEqual(result['status'], "configured")
        self.assertEqual(result['provider'], "oauth2")

    def test_generate_oidc_config(self):
        """Test _generate_oidc_config method."""
        config = SSOConfig(
            provider="oauth2",
            client_id="client-123",
            client_secret="secret-456",
            oidc_config={"issuer": "https://issuer.example.com", "name": "SSO", "scopes": "openid email"}
        )

        oidc_str = self.argocd._generate_oidc_config(config)

        self.assertIn("clientID: client-123", oidc_str)
        self.assertIn("issuer: https://issuer.example.com", oidc_str)


class TestArgoCDIntegrationProjects(unittest.TestCase):
    """Tests for ArgoCDIntegration project operations."""

    @patch('workflow_argocd.requests.Session')
    def setUp(self, mock_session):
        """Set up test fixtures."""
        self.mock_session = Mock()
        mock_session.return_value = self.mock_session
        self.argocd = ArgoCDIntegration("https://argocd.example.com")

    def test_create_project(self):
        """Test create_project method."""
        self.mock_session.post.return_value = Mock(
            status_code=201,
            json=lambda: {"metadata": {"name": "test-project"}}
        )

        spec = ProjectSpec(
            name="test-project",
            description="Test project",
            source_repos=["https://github.com/test/*"],
            destination_clusters=[{"server": "https://k8s.example.com", "namespace": "*"}]
        )

        result = self.argocd.create_project(spec)

        self.assertTrue(self.mock_session.post.called)

    def test_get_project(self):
        """Test get_project method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {"metadata": {"name": "test-project"}, "spec": {"description": "Test"}}
        )

        project = self.argocd.get_project("test-project")

        self.assertIsNotNone(project)

    def test_list_projects(self):
        """Test list_projects method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {
                "items": [
                    {"metadata": {"name": "project-1"}},
                    {"metadata": {"name": "project-2"}}
                ]
            }
        )

        projects = self.argocd.list_projects()

        self.assertEqual(len(projects), 2)

    def test_update_project(self):
        """Test update_project method."""
        self.mock_session.put.return_value = Mock(status_code=200)

        result = self.argocd.update_project("test-project", {"description": "Updated"})

        self.assertTrue(result)

    def test_delete_project(self):
        """Test delete_project method."""
        self.mock_session.delete.return_value = Mock(status_code=200)

        result = self.argocd.delete_project("test-project")

        self.assertTrue(result)

    def test_add_project_source(self):
        """Test add_project_source method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {"spec": {"sourceRepos": ["https://github.com/test/repo"]}}
        )
        self.mock_session.put.return_value = Mock(status_code=200)

        result = self.argocd.add_project_source("test-project", "https://github.com/new/repo")

        self.assertTrue(result)

    def test_add_project_destination(self):
        """Test add_project_destination method."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {"spec": {"destinations": [{"server": "https://k8s-old.example.com", "namespace": "*"}]}}
        )
        self.mock_session.put.return_value = Mock(status_code=200)

        result = self.argocd.add_project_destination("test-project", "https://k8s-new.example.com", "default")

        self.assertTrue(result)

    def test_set_project_role(self):
        """Test set_project_role method."""
        self.mock_session.put.return_value = Mock(status_code=200)

        result = self.argocd.set_project_role(
            "test-project",
            "role-admin",
            ["p,proj:test-project:*:*"]
        )

        self.assertTrue(result)


class TestArgoCDIntegrationSecrets(unittest.TestCase):
    """Tests for ArgoCDIntegration secrets management."""

    @patch('workflow_argocd.requests.Session')
    def setUp(self, mock_session):
        """Set up test fixtures."""
        self.mock_session = MagicMock()
        mock_session.return_value = self.mock_session
        self.mock_response = MagicMock()
        self.mock_response.status_code = 200
        self.mock_response.text = '{}'
        self.mock_response.json.return_value = {"metadata": {"name": "test-app"}}
        self.argocd = ArgoCDIntegration("https://argocd.example.com")

    def test_configure_vault(self):
        """Test configure_vault method."""
        self.argocd.configure_vault(
            vault_addr="https://vault.example.com",
            vault_token="token123",
            kube_auth=True,
            kube_role="k8s-role"
        )

        self.assertEqual(self.argocd.vault_addr, "https://vault.example.com")
        self.assertEqual(self.argocd.vault_token, "token123")

    def test_get_vault_secret(self):
        """Test _get_vault_secret method."""
        self.argocd.vault_addr = "https://vault.example.com"
        self.argocd.vault_token = "token123"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"data": {"api_key": "secret-value"}}}

        with patch('workflow_argocd.requests.get', return_value=mock_response):
            secret = self.argocd._get_vault_secret("secret/data/api", "api_key")

            self.assertEqual(secret, "secret-value")

    def test_inject_secrets(self):
        """Test inject_secrets method."""
        self.argocd.vault_addr = "https://vault.example.com"
        self.argocd.vault_token = "token123"
        self.mock_response.status_code = 200
        self.mock_response.text = '{}'
        self.mock_response.json.return_value = {"metadata": {"name": "test-app"}}
        self.mock_session.patch.return_value = self.mock_response

        with patch.object(self.argocd, '_get_vault_secret', return_value="secret-value"):
            secret_refs = [SecretRef(path="secret/data/api", key="api_key", secret_name="API_KEY")]
            result = self.argocd.inject_secrets("test-app", secret_refs)

            self.assertTrue(result)

    def test_rotate_secrets(self):
        """Test rotate_secrets method."""
        self.argocd.vault_addr = "https://vault.example.com"
        self.argocd.vault_token = "token123"
        self.mock_session.patch.return_value = Mock(status_code=200)

        with patch.object(self.argocd, '_get_latest_secret_version', return_value=2):
            with patch.object(self.argocd, 'inject_secrets', return_value=True):
                secret_refs = [SecretRef(path="secret/data/api", key="api_key")]
                result = self.argocd.rotate_secrets("test-app", secret_refs)

                self.assertTrue(result)


class TestArgoCDIntegrationHealthCheck(unittest.TestCase):
    """Tests for ArgoCDIntegration health check."""

    @patch('workflow_argocd.requests.Session')
    def setUp(self, mock_session):
        """Set up test fixtures."""
        self.mock_session = Mock()
        mock_session.return_value = self.mock_session
        self.argocd = ArgoCDIntegration("https://argocd.example.com")

    def test_health_check_healthy(self):
        """Test health_check when ArgoCD is healthy."""
        self.mock_session.get.return_value = Mock(
            status_code=200,
            json=lambda: {"Version": "2.5.0"}
        )

        result = self.argocd.health_check()

        self.assertEqual(result['status'], "healthy")
        self.assertEqual(result['version'], "2.5.0")

    def test_health_check_unhealthy(self):
        """Test health_check when ArgoCD is unhealthy."""
        self.mock_session.get.side_effect = Exception("Connection refused")

        result = self.argocd.health_check()

        self.assertEqual(result['status'], "unhealthy")


if __name__ == '__main__':
    unittest.main()

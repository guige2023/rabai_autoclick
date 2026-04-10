"""
Tests for workflow_crossplane module.

Commit: 'tests: add comprehensive tests for workflow_terraform and workflow_crossplane modules'
"""

import sys
sys.path.insert(0, '/Users/guige/my_project')

import time
import unittest
from typing import Dict, List, Any, Optional
from unittest.mock import MagicMock, patch

from rabai_autoclick.src.workflow_crossplane import (
    CrossplaneManager,
    Provider,
    ProviderType,
    ProviderStatus,
    XRD,
    XRDStatus,
    Composition,
    CompositionStatus,
    Claim,
    ManagedResource,
    UsageStats,
)


class TestProviderType(unittest.TestCase):
    """Tests for ProviderType enum."""

    def test_provider_types(self):
        """Test all provider types exist."""
        self.assertEqual(ProviderType.AWS.value, "aws")
        self.assertEqual(ProviderType.GCP.value, "gcp")
        self.assertEqual(ProviderType.AZURE.value, "azure")
        self.assertEqual(ProviderType.UNIVERSAL.value, "universal")


class TestProviderStatus(unittest.TestCase):
    """Tests for ProviderStatus enum."""

    def test_statuses(self):
        """Test all provider statuses exist."""
        self.assertEqual(ProviderStatus.PENDING.value, "pending")
        self.assertEqual(ProviderStatus.INSTALLING.value, "installing")
        self.assertEqual(ProviderStatus.INSTALLED.value, "installed")
        self.assertEqual(ProviderStatus.HEALTHY.value, "healthy")
        self.assertEqual(ProviderStatus.UNHEALTHY.value, "unhealthy")
        self.assertEqual(ProviderStatus.DELETING.value, "deleting")


class TestXRDStatus(unittest.TestCase):
    """Tests for XRDStatus enum."""

    def test_statuses(self):
        """Test all XRD statuses exist."""
        self.assertEqual(XRDStatus.PENDING.value, "pending")
        self.assertEqual(XRDStatus.ESTABLISHED.value, "established")
        self.assertEqual(XRDStatus.OFFERED.value, "offered")


class TestCompositionStatus(unittest.TestCase):
    """Tests for CompositionStatus enum."""

    def test_statuses(self):
        """Test all composition statuses exist."""
        self.assertEqual(CompositionStatus.PENDING.value, "pending")
        self.assertEqual(CompositionStatus.ACTIVE.value, "active")


class TestProvider(unittest.TestCase):
    """Tests for Provider dataclass."""

    def test_create_provider(self):
        """Test creating a provider."""
        provider = Provider(
            name="test-provider",
            provider_type=ProviderType.AWS,
            version="v1.0.0"
        )
        self.assertEqual(provider.name, "test-provider")
        self.assertEqual(provider.provider_type, ProviderType.AWS)
        self.assertEqual(provider.version, "v1.0.0")
        self.assertEqual(provider.namespace, "crossplane-system")

    def test_provider_defaults(self):
        """Test provider default values."""
        provider = Provider(name="test", provider_type=ProviderType.GCP)
        self.assertEqual(provider.version, "latest")
        self.assertEqual(provider.status, ProviderStatus.PENDING)
        self.assertIsNone(provider.created_at)


class TestXRD(unittest.TestCase):
    """Tests for XRD dataclass."""

    def test_create_xrd(self):
        """Test creating an XRD."""
        xrd = XRD(
            name="test-xrd",
            group="example.com",
            version="v1alpha1",
            kind="TestResource",
            plural="testresources"
        )
        self.assertEqual(xrd.name, "test-xrd")
        self.assertEqual(xrd.group, "example.com")
        self.assertEqual(xrd.status, XRDStatus.PENDING)

    def test_xrd_with_claims(self):
        """Test XRD with claim kinds."""
        xrd = XRD(
            name="xrd-with-claims",
            group="example.com",
            version="v1alpha1",
            kind="Composite",
            plural="composites",
            claims=["ClaimA", "ClaimB"]
        )
        self.assertEqual(len(xrd.claims), 2)


class TestComposition(unittest.TestCase):
    """Tests for Composition dataclass."""

    def test_create_composition(self):
        """Test creating a composition."""
        comp = Composition(
            name="test-composition",
            composite_type="CompositeType"
        )
        self.assertEqual(comp.name, "test-composition")
        self.assertEqual(comp.status, CompositionStatus.PENDING)

    def test_composition_with_resources(self):
        """Test composition with resources."""
        resources = [{"name": "res1"}, {"name": "res2"}]
        comp = Composition(
            name="test",
            composite_type="Type",
            resources=resources
        )
        self.assertEqual(len(comp.resources), 2)


class TestClaim(unittest.TestCase):
    """Tests for Claim dataclass."""

    def test_create_claim(self):
        """Test creating a claim."""
        claim = Claim(
            name="test-claim",
            composite_kind="TestComposite"
        )
        self.assertEqual(claim.name, "test-claim")
        self.assertEqual(claim.namespace, "default")
        self.assertEqual(claim.status, "Pending")

    def test_claim_with_connection_details(self):
        """Test claim with connection details."""
        details = {"username": "admin", "password": "secret"}
        claim = Claim(
            name="test",
            composite_kind="Test",
            connection_details=details
        )
        self.assertEqual(claim.connection_details["username"], "admin")


class TestManagedResource(unittest.TestCase):
    """Tests for ManagedResource dataclass."""

    def test_create_managed_resource(self):
        """Test creating a managed resource."""
        resource = ManagedResource(
            name="test-resource",
            kind="EC2Instance",
            api_version="ec2.aws.crossplane.io/v1alpha1",
            namespace="default",
            provider="provider-aws",
            status="Pending"
        )
        self.assertEqual(resource.name, "test-resource")
        self.assertFalse(resource.ready)
        self.assertFalse(resource.synced)
        self.assertEqual(resource.age, 0.0)


class TestUsageStats(unittest.TestCase):
    """Tests for UsageStats dataclass."""

    def test_create_usage_stats(self):
        """Test creating usage stats."""
        stats = UsageStats()
        self.assertEqual(stats.total_providers, 0)
        self.assertEqual(stats.healthy_providers, 0)
        self.assertEqual(stats.provider_families, {})

    def test_usage_stats_with_data(self):
        """Test usage stats with populated data."""
        stats = UsageStats(
            total_providers=5,
            healthy_providers=3,
            total_xrds=10,
            active_compositions=7,
            total_claims=15,
            active_claims=12,
            total_managed_resources=100,
            ready_resources=80,
            provider_families={"aws": 3, "gcp": 2}
        )
        self.assertEqual(stats.total_providers, 5)
        self.assertEqual(stats.provider_families["aws"], 3)


class TestCrossplaneManager(unittest.TestCase):
    """Tests for CrossplaneManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = CrossplaneManager()

    def test_init_defaults(self):
        """Test initialization with defaults."""
        manager = CrossplaneManager()
        self.assertIsNone(manager.kubeconfig)
        self.assertIsNone(manager.context)
        self.assertFalse(manager._initialized)

    def test_init_with_kubeconfig(self):
        """Test initialization with kubeconfig."""
        manager = CrossplaneManager(kubeconfig="/path/to/kubeconfig")
        self.assertEqual(manager.kubeconfig, "/path/to/kubeconfig")

    def test_init_with_context(self):
        """Test initialization with context."""
        manager = CrossplaneManager(context="my-cluster")
        self.assertEqual(manager.context, "my-cluster")

    # =========================================================================
    # Provider Management Tests
    # =========================================================================

    def test_install_provider_aws(self):
        """Test installing AWS provider."""
        provider = self.manager.install_provider(
            name="provider-aws",
            provider_type=ProviderType.AWS,
            version="v0.40.0"
        )
        self.assertEqual(provider.name, "provider-aws")
        self.assertEqual(provider.provider_type, ProviderType.AWS)
        self.assertEqual(provider.status, ProviderStatus.INSTALLED)

    def test_install_provider_gcp(self):
        """Test installing GCP provider."""
        provider = self.manager.install_provider(
            name="provider-gcp",
            provider_type=ProviderType.GCP,
            version="v0.40.0"
        )
        self.assertEqual(provider.provider_type, ProviderType.GCP)

    def test_install_provider_azure(self):
        """Test installing Azure provider."""
        provider = self.manager.install_provider(
            name="provider-azure",
            provider_type=ProviderType.AZURE
        )
        self.assertEqual(provider.provider_type, ProviderType.AZURE)

    def test_install_provider_with_labels(self):
        """Test installing provider with labels."""
        labels = {"env": "production", "team": "platform"}
        provider = self.manager.install_provider(
            name="provider-aws",
            provider_type=ProviderType.AWS,
            labels=labels
        )
        self.assertEqual(provider.labels, labels)

    def test_install_provider_stores_provider(self):
        """Test that installed provider is stored."""
        self.manager.install_provider("test-provider", ProviderType.AWS)
        self.assertIn("test-provider", self.manager._providers)

    def test_get_provider(self):
        """Test getting a provider."""
        self.manager.install_provider("test-provider", ProviderType.AWS)
        provider = self.manager.get_provider("test-provider")
        self.assertIsNotNone(provider)
        self.assertEqual(provider.name, "test-provider")

    def test_get_provider_not_found(self):
        """Test getting non-existent provider."""
        provider = self.manager.get_provider("nonexistent")
        self.assertIsNone(provider)

    def test_list_providers(self):
        """Test listing providers."""
        self.manager.install_provider("provider-1", ProviderType.AWS)
        self.manager.install_provider("provider-2", ProviderType.GCP)
        providers = self.manager.list_providers()
        self.assertEqual(len(providers), 2)

    def test_delete_provider(self):
        """Test deleting a provider."""
        self.manager.install_provider("test-provider", ProviderType.AWS)
        result = self.manager.delete_provider("test-provider")
        self.assertTrue(result)
        self.assertNotIn("test-provider", self.manager._providers)

    def test_delete_provider_not_found(self):
        """Test deleting non-existent provider."""
        result = self.manager.delete_provider("nonexistent")
        self.assertFalse(result)

    def test_check_provider_health_installed(self):
        """Test health check for installed provider."""
        self.manager.install_provider("test-provider", ProviderType.AWS)
        status = self.manager.check_provider_health("test-provider")
        self.assertEqual(status, ProviderStatus.HEALTHY)

    def test_check_provider_health_not_found(self):
        """Test health check for non-existent provider."""
        status = self.manager.check_provider_health("nonexistent")
        self.assertEqual(status, ProviderStatus.UNHEALTHY)

    # =========================================================================
    # XRD Management Tests
    # =========================================================================

    def test_create_xrd(self):
        """Test creating an XRD."""
        xrd = self.manager.create_xrd(
            name="test-xrd",
            group="example.com",
            version="v1alpha1",
            kind="TestResource",
            plural="testresources"
        )
        self.assertEqual(xrd.name, "test-xrd")
        self.assertEqual(xrd.status, XRDStatus.ESTABLISHED)

    def test_create_xrd_stores_xrd(self):
        """Test that created XRD is stored."""
        self.manager.create_xrd(
            name="test-xrd",
            group="example.com",
            version="v1alpha1",
            kind="Test",
            plural="tests"
        )
        self.assertIn("test-xrd", self.manager._xrds)

    def test_create_xrd_with_short_names(self):
        """Test creating XRD with short names."""
        xrd = self.manager.create_xrd(
            name="test-xrd",
            group="example.com",
            version="v1alpha1",
            kind="Test",
            plural="tests",
            short_names=["tst", "tr"]
        )
        self.assertEqual(len(xrd.short_names), 2)

    def test_create_xrd_with_claims(self):
        """Test creating XRD with claim kinds."""
        xrd = self.manager.create_xrd(
            name="test-xrd",
            group="example.com",
            version="v1alpha1",
            kind="Test",
            plural="tests",
            claims=["ClaimA"]
        )
        self.assertIn("ClaimA", xrd.claims)

    def test_get_xrd(self):
        """Test getting an XRD."""
        self.manager.create_xrd(
            name="test-xrd",
            group="example.com",
            version="v1alpha1",
            kind="Test",
            plural="tests"
        )
        xrd = self.manager.get_xrd("test-xrd")
        self.assertIsNotNone(xrd)
        self.assertEqual(xrd.name, "test-xrd")

    def test_get_xrd_not_found(self):
        """Test getting non-existent XRD."""
        xrd = self.manager.get_xrd("nonexistent")
        self.assertIsNone(xrd)

    def test_list_xrds(self):
        """Test listing XRDs."""
        self.manager.create_xrd("xrd-1", "g1.com", "v1", "K1", "p1")
        self.manager.create_xrd("xrd-2", "g2.com", "v1", "K2", "p2")
        xrds = self.manager.list_xrds()
        self.assertEqual(len(xrds), 2)

    def test_delete_xrd(self):
        """Test deleting an XRD."""
        self.manager.create_xrd(
            name="test-xrd",
            group="example.com",
            version="v1alpha1",
            kind="Test",
            plural="tests"
        )
        result = self.manager.delete_xrd("test-xrd")
        self.assertTrue(result)
        self.assertNotIn("test-xrd", self.manager._xrds)

    def test_delete_xrd_not_found(self):
        """Test deleting non-existent XRD."""
        result = self.manager.delete_xrd("nonexistent")
        self.assertFalse(result)

    def test_offer_claim(self):
        """Test offering a claim from XRD."""
        self.manager.create_xrd(
            name="test-xrd",
            group="example.com",
            version="v1alpha1",
            kind="Test",
            plural="tests"
        )
        result = self.manager.offer_claim("test-xrd", "ClaimA")
        self.assertTrue(result)
        xrd = self.manager.get_xrd("test-xrd")
        self.assertIn("ClaimA", xrd.claims)
        self.assertEqual(xrd.status, XRDStatus.OFFERED)

    def test_offer_claim_xrd_not_found(self):
        """Test offering claim from non-existent XRD."""
        result = self.manager.offer_claim("nonexistent", "ClaimA")
        self.assertFalse(result)

    # =========================================================================
    # Composition Management Tests
    # =========================================================================

    def test_create_composition(self):
        """Test creating a composition."""
        comp = self.manager.create_composition(
            name="test-composition",
            composite_type="TestComposite"
        )
        self.assertEqual(comp.name, "test-composition")
        self.assertEqual(comp.status, CompositionStatus.ACTIVE)

    def test_create_composition_stores_composition(self):
        """Test that created composition is stored."""
        self.manager.create_composition("test-comp", "TestType")
        self.assertIn("test-comp", self.manager._compositions)

    def test_create_composition_with_resources(self):
        """Test creating composition with resources."""
        resources = [{"name": "res1", "type": "aws-ec2"}]
        comp = self.manager.create_composition(
            name="test-comp",
            composite_type="TestType",
            resources=resources
        )
        self.assertEqual(len(comp.resources), 1)

    def test_get_composition(self):
        """Test getting a composition."""
        self.manager.create_composition("test-comp", "TestType")
        comp = self.manager.get_composition("test-comp")
        self.assertIsNotNone(comp)
        self.assertEqual(comp.name, "test-comp")

    def test_get_composition_not_found(self):
        """Test getting non-existent composition."""
        comp = self.manager.get_composition("nonexistent")
        self.assertIsNone(comp)

    def test_list_compositions(self):
        """Test listing compositions."""
        self.manager.create_composition("comp-1", "Type1")
        self.manager.create_composition("comp-2", "Type2")
        compositions = self.manager.list_compositions()
        self.assertEqual(len(compositions), 2)

    def test_delete_composition(self):
        """Test deleting a composition."""
        self.manager.create_composition("test-comp", "TestType")
        result = self.manager.delete_composition("test-comp")
        self.assertTrue(result)
        self.assertNotIn("test-comp", self.manager._compositions)

    def test_delete_composition_not_found(self):
        """Test deleting non-existent composition."""
        result = self.manager.delete_composition("nonexistent")
        self.assertFalse(result)

    def test_add_composition_resource(self):
        """Test adding resource to composition."""
        self.manager.create_composition("test-comp", "TestType")
        resource = {"name": "new-resource", "type": "aws-s3"}
        result = self.manager.add_composition_resource("test-comp", resource)
        self.assertTrue(result)
        comp = self.manager.get_composition("test-comp")
        self.assertEqual(len(comp.resources), 1)

    def test_add_composition_resource_not_found(self):
        """Test adding resource to non-existent composition."""
        result = self.manager.add_composition_resource("nonexistent", {})
        self.assertFalse(result)

    # =========================================================================
    # Claim Management Tests
    # =========================================================================

    def test_create_claim(self):
        """Test creating a claim."""
        claim = self.manager.create_claim(
            name="test-claim",
            composite_kind="TestComposite"
        )
        self.assertEqual(claim.name, "test-claim")
        self.assertEqual(claim.status, "Ready")

    def test_create_claim_stores_claim(self):
        """Test that created claim is stored."""
        self.manager.create_claim("test-claim", "TestComposite")
        self.assertIn("test-claim", self.manager._claims)

    def test_create_claim_with_namespace(self):
        """Test creating claim with custom namespace."""
        claim = self.manager.create_claim(
            name="test-claim",
            composite_kind="TestComposite",
            namespace="production"
        )
        self.assertEqual(claim.namespace, "production")

    def test_create_claim_with_connection_details(self):
        """Test creating claim with connection details."""
        details = {"endpoint": "http://localhost:8080"}
        claim = self.manager.create_claim(
            name="test-claim",
            composite_kind="TestComposite",
            connection_details=details
        )
        self.assertEqual(claim.connection_details["endpoint"], "http://localhost:8080")

    def test_get_claim(self):
        """Test getting a claim."""
        self.manager.create_claim("test-claim", "TestComposite")
        claim = self.manager.get_claim("test-claim")
        self.assertIsNotNone(claim)
        self.assertEqual(claim.name, "test-claim")

    def test_get_claim_not_found(self):
        """Test getting non-existent claim."""
        claim = self.manager.get_claim("nonexistent")
        self.assertIsNone(claim)

    def test_list_claims(self):
        """Test listing claims."""
        self.manager.create_claim("claim-1", "Type1")
        self.manager.create_claim("claim-2", "Type2")
        claims = self.manager.list_claims()
        self.assertEqual(len(claims), 2)

    def test_delete_claim(self):
        """Test deleting a claim."""
        self.manager.create_claim("test-claim", "TestComposite")
        result = self.manager.delete_claim("test-claim")
        self.assertTrue(result)
        self.assertNotIn("test-claim", self.manager._claims)

    def test_delete_claim_not_found(self):
        """Test deleting non-existent claim."""
        result = self.manager.delete_claim("nonexistent")
        self.assertFalse(result)

    def test_update_claim_connection_details(self):
        """Test updating claim connection details."""
        self.manager.create_claim("test-claim", "TestComposite")
        result = self.manager.update_claim_connection_details(
            "test-claim",
            {"username": "admin", "password": "secret"}
        )
        self.assertTrue(result)
        claim = self.manager.get_claim("test-claim")
        self.assertEqual(claim.connection_details["username"], "admin")

    def test_update_claim_connection_details_not_found(self):
        """Test updating non-existent claim."""
        result = self.manager.update_claim_connection_details("nonexistent", {})
        self.assertFalse(result)

    # =========================================================================
    # Configuration Management Tests
    # =========================================================================

    def test_package_configuration(self):
        """Test packaging a configuration."""
        pkg = self.manager.package_configuration(
            name="test-config",
            xrds=["xrd-1", "xrd-2"],
            compositions=["comp-1"],
            providers=["provider-aws"]
        )
        self.assertEqual(pkg["metadata"]["name"], "test-config")
        self.assertEqual(len(pkg["spec"]["xrds"]), 2)

    def test_package_configuration_with_meta(self):
        """Test packaging with metadata."""
        meta = {"version": "1.0.0", "team": "platform"}
        pkg = self.manager.package_configuration(
            name="test-config",
            xrds=[],
            compositions=[],
            providers=[],
            meta=meta
        )
        self.assertEqual(pkg["metadata"]["annotations"]["version"], "1.0.0")

    def test_distribute_configuration(self):
        """Test distributing a configuration."""
        result = self.manager.distribute_configuration(
            name="test-config",
            package_image="crossplane/example-config:v1.0"
        )
        self.assertTrue(result)

    def test_list_configurations(self):
        """Test listing configurations."""
        configs = self.manager.list_configurations()
        self.assertIsInstance(configs, list)

    # =========================================================================
    # Secret Integration Tests
    # =========================================================================

    def test_create_secret_sync(self):
        """Test creating secret sync configuration."""
        sync = self.manager.create_secret_sync(
            name="secret-sync-1",
            secret_name="my-secret",
            secret_namespace="default",
            provider="provider-aws"
        )
        self.assertEqual(sync["name"], "secret-sync-1")
        self.assertEqual(sync["status"], "pending")

    def test_create_secret_sync_with_keys(self):
        """Test creating secret sync with specific keys."""
        sync = self.manager.create_secret_sync(
            name="secret-sync-1",
            secret_name="my-secret",
            secret_namespace="default",
            provider="provider-aws",
            keys=["username", "password"]
        )
        self.assertEqual(len(sync["keys"]), 2)

    def test_sync_secret(self):
        """Test syncing a secret."""
        self.manager.create_secret_sync(
            name="sync-1",
            secret_name="my-secret",
            secret_namespace="default",
            provider="provider-aws"
        )
        result = self.manager.sync_secret("sync-1")
        self.assertTrue(result)
        sync = self.manager.get_secret_sync("sync-1")
        self.assertEqual(sync["status"], "synced")

    def test_sync_secret_not_found(self):
        """Test syncing non-existent secret."""
        result = self.manager.sync_secret("nonexistent")
        self.assertFalse(result)

    def test_get_secret_sync(self):
        """Test getting secret sync configuration."""
        self.manager.create_secret_sync(
            name="sync-1",
            secret_name="my-secret",
            secret_namespace="default",
            provider="provider-aws"
        )
        sync = self.manager.get_secret_sync("sync-1")
        self.assertIsNotNone(sync)
        self.assertEqual(sync["name"], "sync-1")

    # =========================================================================
    # Observation Tests
    # =========================================================================

    def test_observe_managed_resource(self):
        """Test observing a managed resource."""
        resource = self.manager.observe_managed_resource(
            name="test-resource",
            kind="EC2Instance",
            api_version="ec2.aws.crossplane.io/v1alpha1",
            namespace="default",
            provider="provider-aws"
        )
        self.assertEqual(resource.name, "test-resource")
        self.assertFalse(resource.ready)
        self.assertFalse(resource.synced)

    def test_observe_managed_resource_stores_resource(self):
        """Test that observed resource is stored."""
        self.manager.observe_managed_resource(
            name="test-resource",
            kind="S3Bucket",
            api_version="s3.aws.crossplane.io/v1alpha1",
            namespace="default",
            provider="provider-aws"
        )
        self.assertIn("default/test-resource", self.manager._managed_resources)

    def test_update_resource_status(self):
        """Test updating managed resource status."""
        self.manager.observe_managed_resource(
            name="test-resource",
            kind="EC2Instance",
            api_version="ec2.aws.crossplane.io/v1alpha1",
            namespace="default",
            provider="provider-aws"
        )
        result = self.manager.update_resource_status(
            name="default/test-resource",
            status="Ready",
            ready=True,
            synced=True,
            conditions=[{"type": "Ready", "status": "True"}]
        )
        self.assertTrue(result)
        resource = self.manager.get_managed_resource("default/test-resource")
        self.assertEqual(resource.status, "Ready")
        self.assertTrue(resource.ready)
        self.assertTrue(resource.synced)

    def test_update_resource_status_not_found(self):
        """Test updating non-existent resource."""
        result = self.manager.update_resource_status(
            name="nonexistent",
            status="Ready",
            ready=True,
            synced=True
        )
        self.assertFalse(result)

    def test_list_managed_resources(self):
        """Test listing managed resources."""
        self.manager.observe_managed_resource(
            name="res-1",
            kind="EC2Instance",
            api_version="v1",
            namespace="default",
            provider="aws"
        )
        self.manager.observe_managed_resource(
            name="res-2",
            kind="S3Bucket",
            api_version="v1",
            namespace="default",
            provider="aws"
        )
        resources = self.manager.list_managed_resources()
        self.assertEqual(len(resources), 2)

    def test_get_managed_resource(self):
        """Test getting a managed resource."""
        self.manager.observe_managed_resource(
            name="test-resource",
            kind="EC2Instance",
            api_version="v1",
            namespace="default",
            provider="aws"
        )
        resource = self.manager.get_managed_resource("default/test-resource")
        self.assertIsNotNone(resource)
        self.assertEqual(resource.name, "test-resource")

    # =========================================================================
    # Connection Sharing Tests
    # =========================================================================

    def test_publish_connection_details(self):
        """Test publishing connection details."""
        result = self.manager.publish_connection_details(
            claim_name="test-claim",
            details={"endpoint": "http://localhost:8080", "password": "secret"},
            target_namespaces=["namespace-1", "namespace-2"]
        )
        self.assertTrue(result)
        details = self.manager.get_connection_details("test-claim", "namespace-1")
        self.assertIsNotNone(details)
        self.assertEqual(details["details"]["endpoint"], "http://localhost:8080")

    def test_get_connection_details(self):
        """Test getting connection details."""
        self.manager.publish_connection_details(
            claim_name="test-claim",
            details={"key": "value"},
            target_namespaces=["default"]
        )
        details = self.manager.get_connection_details("test-claim", "default")
        self.assertIsNotNone(details)
        self.assertEqual(details["claim_name"], "test-claim")

    def test_list_shared_connections(self):
        """Test listing shared connections."""
        self.manager.publish_connection_details(
            claim_name="claim-1",
            details={},
            target_namespaces=["ns-1"]
        )
        self.manager.publish_connection_details(
            claim_name="claim-2",
            details={},
            target_namespaces=["ns-2"]
        )
        connections = self.manager.list_shared_connections()
        self.assertEqual(len(connections), 2)

    # =========================================================================
    # Usage Statistics Tests
    # =========================================================================

    def test_get_usage_stats_empty(self):
        """Test getting usage stats with no data."""
        stats = self.manager.get_usage_stats()
        self.assertEqual(stats.total_providers, 0)
        self.assertEqual(stats.total_xrds, 0)
        self.assertEqual(stats.total_managed_resources, 0)

    def test_get_usage_stats_with_providers(self):
        """Test getting usage stats with providers."""
        self.manager.install_provider("p1", ProviderType.AWS)
        self.manager.install_provider("p2", ProviderType.AWS)
        self.manager.install_provider("p3", ProviderType.GCP)
        stats = self.manager.get_usage_stats()
        self.assertEqual(stats.total_providers, 3)
        self.assertEqual(stats.provider_families["aws"], 2)
        self.assertEqual(stats.provider_families["gcp"], 1)

    def test_get_usage_stats_with_claims(self):
        """Test getting usage stats with claims."""
        self.manager.create_claim("claim-1", "Type1")
        self.manager.create_claim("claim-2", "Type2")
        stats = self.manager.get_usage_stats()
        self.assertEqual(stats.total_claims, 2)
        self.assertEqual(stats.active_claims, 2)

    def test_report_usage_stats(self):
        """Test generating usage report."""
        self.manager.install_provider("p1", ProviderType.AWS)
        self.manager.create_xrd("xrd-1", "g.com", "v1", "K", "p")
        report = self.manager.report_usage_stats()
        self.assertIn("Crossplane Usage Report", report)
        self.assertIn("Providers:", report)

    # =========================================================================
    # Provider Family Management Tests
    # =========================================================================

    def test_install_aws_provider_family(self):
        """Test installing AWS provider family."""
        provider = self.manager.install_aws_provider_family("aws-provider")
        self.assertEqual(provider.provider_type, ProviderType.AWS)
        self.assertEqual(provider.config.get("region"), "us-east-1")

    def test_install_aws_provider_family_custom_config(self):
        """Test AWS provider with custom config."""
        provider = self.manager.install_aws_provider_family(
            "aws-provider",
            config={"region": "us-west-2"}
        )
        self.assertEqual(provider.config["region"], "us-west-2")

    def test_install_gcp_provider_family(self):
        """Test installing GCP provider family."""
        provider = self.manager.install_gcp_provider_family("gcp-provider")
        self.assertEqual(provider.provider_type, ProviderType.GCP)
        self.assertEqual(provider.config.get("projectID"), "my-project")

    def test_install_azure_provider_family(self):
        """Test installing Azure provider family."""
        provider = self.manager.install_azure_provider_family("azure-provider")
        self.assertEqual(provider.provider_type, ProviderType.AZURE)
        self.assertEqual(provider.config.get("tenantID"), "my-tenant")

    def test_list_provider_families(self):
        """Test listing provider families."""
        self.manager.install_provider("aws-1", ProviderType.AWS)
        self.manager.install_provider("aws-2", ProviderType.AWS)
        self.manager.install_provider("gcp-1", ProviderType.GCP)
        families = self.manager.list_provider_families()
        self.assertEqual(len(families["aws"]), 2)
        self.assertEqual(len(families["gcp"]), 1)

    def test_configure_provider_family(self):
        """Test configuring a provider family."""
        self.manager.install_provider("test-provider", ProviderType.AWS)
        result = self.manager.configure_provider_family(
            "test-provider",
            {"region": "eu-west-1", "custom_setting": "value"}
        )
        self.assertTrue(result)
        provider = self.manager.get_provider("test-provider")
        self.assertEqual(provider.config["region"], "eu-west-1")
        self.assertEqual(provider.config["custom_setting"], "value")

    def test_configure_provider_family_not_found(self):
        """Test configuring non-existent provider."""
        result = self.manager.configure_provider_family("nonexistent", {})
        self.assertFalse(result)


class TestCrossplaneManagerEdgeCases(unittest.TestCase):
    """Edge case tests for CrossplaneManager."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = CrossplaneManager()

    def test_multiple_provider_installations(self):
        """Test installing multiple providers."""
        for i in range(10):
            self.manager.install_provider(f"provider-{i}", ProviderType.AWS)
        self.assertEqual(len(self.manager.list_providers()), 10)

    def test_multiple_xrd_creations(self):
        """Test creating multiple XRDs."""
        for i in range(5):
            self.manager.create_xrd(
                f"xrd-{i}",
                group="example.com",
                version="v1alpha1",
                kind=f"Kind{i}",
                plural=f"kinds{i}"
            )
        self.assertEqual(len(self.manager.list_xrds()), 5)

    def test_resource_status_update_sequence(self):
        """Test updating resource status multiple times."""
        self.manager.observe_managed_resource(
            name="test",
            kind="Kind",
            api_version="v1",
            namespace="default",
            provider="aws"
        )
        
        self.manager.update_resource_status("default/test", "Pending", False, False)
        resource = self.manager.get_managed_resource("default/test")
        self.assertEqual(resource.status, "Pending")
        
        self.manager.update_resource_status("default/test", "Creating", False, False)
        resource = self.manager.get_managed_resource("default/test")
        self.assertEqual(resource.status, "Creating")
        
        self.manager.update_resource_status("default/test", "Ready", True, True)
        resource = self.manager.get_managed_resource("default/test")
        self.assertEqual(resource.status, "Ready")
        self.assertTrue(resource.ready)

    def test_claim_connection_details_update_merge(self):
        """Test that connection details updates are merged."""
        self.manager.create_claim("test-claim", "TestComposite")
        
        self.manager.update_claim_connection_details("test-claim", {"key1": "value1"})
        self.manager.update_claim_connection_details("test-claim", {"key2": "value2"})
        
        claim = self.manager.get_claim("test-claim")
        self.assertEqual(claim.connection_details["key1"], "value1")
        self.assertEqual(claim.connection_details["key2"], "value2")


if __name__ == '__main__':
    unittest.main()

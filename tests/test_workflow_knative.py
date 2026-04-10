"""
Tests for workflow_knative module.

Commit: 'tests: add comprehensive tests for workflow_helm and workflow_knative modules'
"""

import sys
sys.path.insert(0, '/Users/guige/my_project')

import json
import time
import unittest
from datetime import datetime
from typing import Dict, List, Any
from unittest.mock import MagicMock, patch, call

import requests

from rabai_autoclick.src.workflow_knative import (
    KnativeManager,
    ServiceState,
    RevisionState,
    TrafficTargetType,
    EventSourceType,
    BrokerDeliveryMode,
    AutoScalingMetric,
    ServiceMeshType,
    ServiceSpec,
    KnativeService,
    RevisionSpec,
    KnativeRevision,
    TrafficTarget,
    TrafficSplit,
    EventSourceSpec,
    EventSource,
    BrokerSpec,
    Broker,
    TriggerSpec,
    Trigger,
    DomainMappingSpec,
    DomainMapping,
    AutoScalingConfig,
    ConfigMapSpec,
    ConfigMap,
    ServiceMeshConfig,
    ObservabilityConfig,
)


class TestKnativeManagerInit(unittest.TestCase):
    """Tests for KnativeManager initialization."""

    def test_default_init(self):
        """Test default initialization."""
        manager = KnativeManager()
        self.assertEqual(manager.namespace, "default")
        self.assertEqual(manager.knative_serving_url, "")
        self.assertEqual(manager.knative_eventing_url, "")
        self.assertEqual(manager.timeout, 30)

    def test_custom_init(self):
        """Test custom initialization."""
        manager = KnativeManager(
            namespace="test-ns",
            kubeconfig_path="/tmp/kubeconfig",
            knative_serving_url="http://serving:80",
            knative_eventing_url="http://eventing:80",
            timeout=60
        )
        self.assertEqual(manager.namespace, "test-ns")
        self.assertEqual(manager.knative_serving_url, "http://serving:80")
        self.assertEqual(manager.knative_eventing_url, "http://eventing:80")
        self.assertEqual(manager.timeout, 60)

    def test_url_stripping(self):
        """Test that URLs are stripped of trailing slashes."""
        manager = KnativeManager(
            knative_serving_url="http://serving:80/",
            knative_eventing_url="http://eventing:80///"
        )
        self.assertEqual(manager.knative_serving_url, "http://serving:80")
        self.assertEqual(manager.knative_eventing_url, "http://eventing:80")


class TestKnativeManagerHelpers(unittest.TestCase):
    """Tests for helper methods."""

    def test_make_url(self):
        """Test URL construction."""
        manager = KnativeManager()
        
        url = manager._make_url("http://base:80", "/api/v1/services")
        self.assertEqual(url, "http://base:80/api/v1/services")

    def test_make_url_no_base(self):
        """Test URL construction without base."""
        manager = KnativeManager()
        
        url = manager._make_url("", "/api/v1/services")
        self.assertEqual(url, "/api/v1/services")

    def test_handle_response_success(self):
        """Test handling successful response."""
        manager = KnativeManager()
        response = MagicMock()
        response.status_code = 200
        response.content = b'{"name": "test"}'
        response.json.return_value = {"name": "test"}
        
        result = manager._handle_response(response)
        self.assertEqual(result["name"], "test")

    def test_handle_response_created(self):
        """Test handling 201 response."""
        manager = KnativeManager()
        response = MagicMock()
        response.status_code = 201
        response.content = b'{"name": "test"}'
        response.json.return_value = {"name": "test"}
        
        result = manager._handle_response(response)
        self.assertEqual(result["name"], "test")

    def test_handle_response_not_found(self):
        """Test handling 404 response."""
        manager = KnativeManager()
        response = MagicMock()
        response.status_code = 404
        response.text = "Resource not found"
        
        with self.assertRaises(ValueError) as context:
            manager._handle_response(response)
        self.assertIn("not found", str(context.exception))

    def test_handle_response_conflict(self):
        """Test handling 409 response."""
        manager = KnativeManager()
        response = MagicMock()
        response.status_code = 409
        response.text = "Resource already exists"
        
        with self.assertRaises(ValueError) as context:
            manager._handle_response(response)
        self.assertIn("conflict", str(context.exception))

    def test_handle_response_error(self):
        """Test handling error response."""
        manager = KnativeManager()
        response = MagicMock()
        response.status_code = 500
        response.text = "Internal server error"
        
        with self.assertRaises(Exception) as context:
            manager._handle_response(response)
        self.assertIn("500", str(context.exception))


class TestServiceState(unittest.TestCase):
    """Tests for ServiceState enum."""

    def test_service_states(self):
        """Test all service states exist."""
        self.assertEqual(ServiceState.ACTIVE.value, "Active")
        self.assertEqual(ServiceState.INACTIVE.value, "Inactive")
        self.assertEqual(ServiceState.DEPLOYING.value, "Deploying")
        self.assertEqual(ServiceState.FAILED.value, "Failed")
        self.assertEqual(ServiceState.UNKNOWN.value, "Unknown")


class TestRevisionState(unittest.TestCase):
    """Tests for RevisionState enum."""

    def test_revision_states(self):
        """Test all revision states exist."""
        self.assertEqual(RevisionState.ACTIVE.value, "Active")
        self.assertEqual(RevisionState.RESERVE.value, "Reserve")
        self.assertEqual(RevisionState.DELETED.value, "Deleted")


class TestTrafficTargetType(unittest.TestCase):
    """Tests for TrafficTargetType enum."""

    def test_traffic_target_types(self):
        """Test all traffic target types exist."""
        self.assertEqual(TrafficTargetType.LATEST.value, "latest")
        self.assertEqual(TrafficTargetType.PINNED.value, "pinned")
        self.assertEqual(TrafficTargetType.PERCENTAGE.value, "percentage")


class TestEventSourceType(unittest.TestCase):
    """Tests for EventSourceType enum."""

    def test_event_source_types(self):
        """Test all event source types exist."""
        self.assertEqual(EventSourceType.APISERVER.value, "ApiServerSource")
        self.assertEqual(EventSourceType.PING.value, "PingSource")
        self.assertEqual(EventSourceType.KAFKA.value, "KafkaSource")
        self.assertEqual(EventSourceType.NATS.value, "NatsSource")


class TestAutoScalingMetric(unittest.TestCase):
    """Tests for AutoScalingMetric enum."""

    def test_autoscaling_metrics(self):
        """Test all autoscaling metrics exist."""
        self.assertEqual(AutoScalingMetric.CONCURRENCY.value, "concurrency")
        self.assertEqual(AutoScalingMetric.RPS.value, "rps")
        self.assertEqual(AutoScalingMetric.CPU.value, "cpu")


class TestServiceMeshType(unittest.TestCase):
    """Tests for ServiceMeshType enum."""

    def test_service_mesh_types(self):
        """Test all service mesh types exist."""
        self.assertEqual(ServiceMeshType.ISTIO.value, "istio")
        self.assertEqual(ServiceMeshType.LINKERD.value, "linkerd")
        self.assertEqual(ServiceMeshType.SMI.value, "smi")


class TestServiceSpec(unittest.TestCase):
    """Tests for ServiceSpec dataclass."""

    def test_service_spec_creation(self):
        """Test ServiceSpec creation."""
        spec = ServiceSpec(name="my-service", image="nginx:latest")
        self.assertEqual(spec.name, "my-service")
        self.assertEqual(spec.namespace, "default")
        self.assertEqual(spec.image, "nginx:latest")

    def test_service_spec_full(self):
        """Test ServiceSpec with all fields."""
        spec = ServiceSpec(
            name="my-service",
            namespace="test-ns",
            image="nginx:latest",
            env_vars={"ENV1": "value1"},
            env_secrets=["my-secret"],
            ports=[{"containerPort": 8080}],
            resources={"limits": {"cpu": "100m"}},
            annotations={"key": "value"},
            labels={"app": "my-service"},
            service_account="my-sa"
        )
        self.assertEqual(spec.namespace, "test-ns")
        self.assertEqual(spec.env_vars["ENV1"], "value1")
        self.assertEqual(spec.env_secrets[0], "my-secret")


class TestKnativeService(unittest.TestCase):
    """Tests for KnativeService dataclass."""

    def test_knative_service_creation(self):
        """Test KnativeService creation."""
        service = KnativeService(
            name="my-service",
            namespace="default",
            uid="123-456",
            state=ServiceState.ACTIVE,
            latest_created="my-service-00001",
            latest_ready="my-service-00001",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z"
        )
        self.assertEqual(service.name, "my-service")
        self.assertEqual(service.state, ServiceState.ACTIVE)


class TestRevisionSpec(unittest.TestCase):
    """Tests for RevisionSpec dataclass."""

    def test_revision_spec_creation(self):
        """Test RevisionSpec creation."""
        spec = RevisionSpec(service_name="my-service", image="nginx:latest")
        self.assertEqual(spec.service_name, "my-service")
        self.assertEqual(spec.image, "nginx:latest")
        self.assertEqual(spec.container_concurrency, 0)
        self.assertEqual(spec.timeout_seconds, 300)


class TestKnativeRevision(unittest.TestCase):
    """Tests for KnativeRevision dataclass."""

    def test_knative_revision_creation(self):
        """Test KnativeRevision creation."""
        revision = KnativeRevision(
            name="my-service-00001",
            service_name="my-service",
            namespace="default",
            uid="123-456",
            state=RevisionState.ACTIVE,
            image="nginx:latest",
            created_at="2024-01-01T00:00:00Z"
        )
        self.assertEqual(revision.name, "my-service-00001")
        self.assertEqual(revision.state, RevisionState.ACTIVE)


class TestTrafficTarget(unittest.TestCase):
    """Tests for TrafficTarget dataclass."""

    def test_traffic_target_creation(self):
        """Test TrafficTarget creation."""
        target = TrafficTarget(
            revision_name="my-service-00001",
            percentage=100.0
        )
        self.assertEqual(target.revision_name, "my-service-00001")
        self.assertEqual(target.percentage, 100.0)
        self.assertEqual(target.target_type, TrafficTargetType.PERCENTAGE)

    def test_traffic_target_latest(self):
        """Test TrafficTarget with latest revision."""
        target = TrafficTarget(
            revision_name="my-service-00002",
            percentage=0.0,
            latest_revision=True
        )
        self.assertTrue(target.latest_revision)


class TestTrafficSplit(unittest.TestCase):
    """Tests for TrafficSplit dataclass."""

    def test_traffic_split_creation(self):
        """Test TrafficSplit creation."""
        targets = [
            TrafficTarget(revision_name="v1", percentage=90.0),
            TrafficTarget(revision_name="v2", percentage=10.0)
        ]
        split = TrafficSplit(service_name="my-service", targets=targets)
        self.assertEqual(split.service_name, "my-service")
        self.assertEqual(len(split.targets), 2)


class TestEventSourceSpec(unittest.TestCase):
    """Tests for EventSourceSpec dataclass."""

    def test_event_source_spec_creation(self):
        """Test EventSourceSpec creation."""
        spec = EventSourceSpec(
            name="my-source",
            source_type=EventSourceType.PING,
            namespace="default"
        )
        self.assertEqual(spec.name, "my-source")
        self.assertEqual(spec.source_type, EventSourceType.PING)


class TestEventSource(unittest.TestCase):
    """Tests for EventSource dataclass."""

    def test_event_source_creation(self):
        """Test EventSource creation."""
        source = EventSource(
            name="my-source",
            namespace="default",
            source_type=EventSourceType.KAFKA,
            uid="123-456",
            sink="http://my-service",
            created_at="2024-01-01T00:00:00Z"
        )
        self.assertEqual(source.name, "my-source")
        self.assertEqual(source.source_type, EventSourceType.KAFKA)


class TestBrokerSpec(unittest.TestCase):
    """Tests for BrokerSpec dataclass."""

    def test_broker_spec_creation(self):
        """Test BrokerSpec creation."""
        spec = BrokerSpec(
            name="my-broker",
            namespace="default"
        )
        self.assertEqual(spec.name, "my-broker")
        self.assertEqual(spec.namespace, "default")


class TestBroker(unittest.TestCase):
    """Tests for Broker dataclass."""

    def test_broker_creation(self):
        """Test Broker creation."""
        broker = Broker(
            name="my-broker",
            namespace="default",
            uid="123-456",
            url="http://my-broker",
            created_at="2024-01-01T00:00:00Z"
        )
        self.assertEqual(broker.name, "my-broker")
        self.assertEqual(broker.url, "http://my-broker")


class TestTriggerSpec(unittest.TestCase):
    """Tests for TriggerSpec dataclass."""

    def test_trigger_spec_creation(self):
        """Test TriggerSpec creation."""
        spec = TriggerSpec(
            name="my-trigger",
            broker_name="my-broker",
            filter_attributes={"type": "dev.knative.samples.hello"}
        )
        self.assertEqual(spec.name, "my-trigger")
        self.assertEqual(spec.filter_attributes["type"], "dev.knative.samples.hello")


class TestTrigger(unittest.TestCase):
    """Tests for Trigger dataclass."""

    def test_trigger_creation(self):
        """Test Trigger creation."""
        trigger = Trigger(
            name="my-trigger",
            namespace="default",
            broker="my-broker",
            uid="123-456",
            created_at="2024-01-01T00:00:00Z"
        )
        self.assertEqual(trigger.name, "my-trigger")
        self.assertEqual(trigger.broker, "my-broker")


class TestDomainMappingSpec(unittest.TestCase):
    """Tests for DomainMappingSpec dataclass."""

    def test_domain_mapping_spec_creation(self):
        """Test DomainMappingSpec creation."""
        spec = DomainMappingSpec(
            name="my-domain.example.com",
            ref_service="my-service"
        )
        self.assertEqual(spec.name, "my-domain.example.com")
        self.assertEqual(spec.ref_service, "my-service")
        self.assertEqual(spec.ref_service_port, 80)
        self.assertTrue(spec.tls_enabled)

    def test_domain_mapping_spec_full(self):
        """Test DomainMappingSpec with all fields."""
        spec = DomainMappingSpec(
            name="my-domain.example.com",
            namespace="custom-ns",
            ref_service="my-service",
            ref_service_port=8080,
            tls_enabled=True,
            tls_secret="my-secret"
        )
        self.assertEqual(spec.namespace, "custom-ns")
        self.assertEqual(spec.ref_service_port, 8080)
        self.assertEqual(spec.tls_secret, "my-secret")


class TestDomainMapping(unittest.TestCase):
    """Tests for DomainMapping dataclass."""

    def test_domain_mapping_creation(self):
        """Test DomainMapping creation."""
        mapping = DomainMapping(
            name="my-domain.example.com",
            namespace="default",
            url="https://my-domain.example.com",
            service_name="my-service",
            service_port=80,
            uid="123-456",
            created_at="2024-01-01T00:00:00Z"
        )
        self.assertEqual(mapping.url, "https://my-domain.example.com")


class TestAutoScalingConfig(unittest.TestCase):
    """Tests for AutoScalingConfig dataclass."""

    def test_autoscaling_config_defaults(self):
        """Test AutoScalingConfig defaults."""
        config = AutoScalingConfig()
        self.assertEqual(config.min_scale, 0)
        self.assertEqual(config.max_scale, 10)
        self.assertEqual(config.metric_type, AutoScalingMetric.CONCURRENCY)
        self.assertEqual(config.target_value, 100.0)

    def test_autoscaling_config_custom(self):
        """Test AutoScalingConfig with custom values."""
        config = AutoScalingConfig(
            min_scale=2,
            max_scale=20,
            metric_type=AutoScalingMetric.RPS,
            target_value=50.0,
            scale_down_delay=60,
            scale_up_delay=10
        )
        self.assertEqual(config.min_scale, 2)
        self.assertEqual(config.max_scale, 20)
        self.assertEqual(config.metric_type, AutoScalingMetric.RPS)


class TestConfigMapSpec(unittest.TestCase):
    """Tests for ConfigMapSpec dataclass."""

    def test_config_map_spec_creation(self):
        """Test ConfigMapSpec creation."""
        spec = ConfigMapSpec(
            name="my-config",
            data={"key": "value"}
        )
        self.assertEqual(spec.name, "my-config")
        self.assertEqual(spec.data["key"], "value")


class TestConfigMap(unittest.TestCase):
    """Tests for ConfigMap dataclass."""

    def test_config_map_creation(self):
        """Test ConfigMap creation."""
        cm = ConfigMap(
            name="my-config",
            namespace="default",
            uid="123-456",
            data={"key": "value"},
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z"
        )
        self.assertEqual(cm.name, "my-config")
        self.assertEqual(cm.data["key"], "value")


class TestServiceMeshConfig(unittest.TestCase):
    """Tests for ServiceMeshConfig dataclass."""

    def test_service_mesh_config_creation(self):
        """Test ServiceMeshConfig creation."""
        config = ServiceMeshConfig(
            mesh_type=ServiceMeshType.ISTIO,
            mtls_enabled=True
        )
        self.assertEqual(config.mesh_type, ServiceMeshType.ISTIO)
        self.assertTrue(config.mtls_enabled)


class TestObservabilityConfig(unittest.TestCase):
    """Tests for ObservabilityConfig dataclass."""

    def test_observability_config_defaults(self):
        """Test ObservabilityConfig defaults."""
        config = ObservabilityConfig()
        self.assertTrue(config.logging_enabled)
        self.assertEqual(config.log_level, "INFO")
        self.assertTrue(config.metrics_enabled)
        self.assertTrue(config.tracing_enabled)
        self.assertEqual(config.tracing_sample_rate, 0.1)


class TestKnativeManagerServiceManagement(unittest.TestCase):
    """Tests for service management methods."""

    def setUp(self):
        self.manager = KnativeManager()

    @patch('requests.Session.post')
    def test_create_service_success(self, mock_post):
        """Test successful service creation."""
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "metadata": {
                "name": "my-service",
                "namespace": "default",
                "uid": "123-456",
                "creationTimestamp": "2024-01-01T00:00:00Z"
            },
            "status": {
                "latestCreatedRevisionName": "my-service-00001",
                "latestReadyRevisionName": "my-service-00001"
            }
        }
        mock_post.return_value = mock_response

        spec = ServiceSpec(name="my-service", image="nginx:latest")
        service = self.manager.create_service(spec)

        self.assertEqual(service.name, "my-service")
        self.assertEqual(service.state, ServiceState.ACTIVE)

    def test_create_service_without_url(self):
        """Test service creation without URL (mock mode)."""
        spec = ServiceSpec(name="my-service", image="nginx:latest")
        service = self.manager.create_service(spec)

        self.assertEqual(service.name, "my-service")
        self.assertEqual(service.namespace, "default")

    @patch('requests.Session.get')
    def test_get_service_success(self, mock_get):
        """Test getting service successfully."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "metadata": {
                "name": "my-service",
                "namespace": "default",
                "uid": "123-456",
                "creationTimestamp": "2024-01-01T00:00:00Z"
            },
            "status": {
                "latestCreatedRevisionName": "my-service-00001",
                "latestReadyRevisionName": "my-service-00001"
            }
        }
        mock_get.return_value = mock_response

        service = self.manager.get_service("my-service")

        self.assertIsNotNone(service)
        self.assertEqual(service.name, "my-service")

    @patch('requests.Session.get')
    def test_get_service_not_found(self, mock_get):
        """Test getting non-existent service when URL is configured."""
        self.manager.knative_serving_url = "http://serving:80"
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        service = self.manager.get_service("nonexistent")

        self.assertIsNone(service)

    def test_get_service_without_url(self):
        """Test getting service without URL (mock mode)."""
        service = self.manager.get_service("my-service")

        self.assertIsNotNone(service)
        self.assertEqual(service.name, "my-service")

    @patch('requests.Session.get')
    def test_list_services(self, mock_get):
        """Test listing services when URL is configured."""
        self.manager.knative_serving_url = "http://serving:80"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "items": [
                {
                    "metadata": {
                        "name": "service1",
                        "namespace": "default",
                        "uid": "123",
                        "creationTimestamp": "2024-01-01T00:00:00Z"
                    },
                    "status": {}
                },
                {
                    "metadata": {
                        "name": "service2",
                        "namespace": "default",
                        "uid": "456",
                        "creationTimestamp": "2024-01-01T00:00:00Z"
                    },
                    "status": {}
                }
            ]
        }
        mock_get.return_value = mock_response

        services = self.manager.list_services()

        self.assertEqual(len(services), 2)

    def test_list_services_without_url(self):
        """Test listing services without URL (mock mode)."""
        services = self.manager.list_services()

        self.assertEqual(len(services), 0)

    @patch('requests.Session.delete')
    def test_delete_service_success(self, mock_delete):
        """Test deleting service."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_delete.return_value = mock_response

        result = self.manager.delete_service("my-service")

        self.assertTrue(result)

    def test_delete_service_without_url(self):
        """Test deleting service without URL (mock mode)."""
        result = self.manager.delete_service("my-service")

        self.assertTrue(result)


class TestKnativeManagerRevisionManagement(unittest.TestCase):
    """Tests for revision management methods."""

    def setUp(self):
        self.manager = KnativeManager()

    @patch('requests.Session.get')
    def test_get_revision_success(self, mock_get):
        """Test getting revision."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "metadata": {
                "name": "my-service-00001",
                "namespace": "default",
                "uid": "123-456",
                "creationTimestamp": "2024-01-01T00:00:00Z",
                "labels": {"serving.knative.dev/service": "my-service"}
            },
            "spec": {
                "containers": [{"image": "nginx:latest"}]
            }
        }
        mock_get.return_value = mock_response

        revision = self.manager.get_revision("my-service-00001")

        self.assertIsNotNone(revision)
        self.assertEqual(revision.name, "my-service-00001")

    def test_get_revision_without_url(self):
        """Test getting revision without URL (mock mode)."""
        revision = self.manager.get_revision("my-service-00001")

        self.assertIsNotNone(revision)
        self.assertEqual(revision.name, "my-service-00001")

    @patch('requests.Session.get')
    def test_list_revisions(self, mock_get):
        """Test listing revisions."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"items": []}
        mock_get.return_value = mock_response

        revisions = self.manager.list_revisions()

        self.assertEqual(len(revisions), 0)

    def test_list_revisions_without_url(self):
        """Test listing revisions without URL (mock mode)."""
        revisions = self.manager.list_revisions()

        self.assertEqual(len(revisions), 0)

    @patch('requests.Session.delete')
    def test_delete_revision_success(self, mock_delete):
        """Test deleting revision."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_delete.return_value = mock_response

        result = self.manager.delete_revision("my-service-00001")

        self.assertTrue(result)


class TestKnativeManagerTrafficManagement(unittest.TestCase):
    """Tests for traffic management methods."""

    def setUp(self):
        self.manager = KnativeManager()

    @patch('requests.Session.patch')
    @patch('requests.Session.get')
    def test_update_traffic(self, mock_get, mock_patch):
        """Test updating traffic."""
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {
            "metadata": {
                "name": "my-service",
                "namespace": "default",
                "uid": "123-456",
                "creationTimestamp": "2024-01-01T00:00:00Z"
            },
            "status": {}
        }
        mock_get.return_value = mock_get_response

        mock_patch_response = MagicMock()
        mock_patch_response.status_code = 200
        mock_patch.return_value = mock_patch_response

        targets = [
            TrafficTarget(revision_name="v1", percentage=90.0),
            TrafficTarget(revision_name="v2", percentage=10.0)
        ]
        
        split = self.manager.update_traffic("my-service", targets)

        self.assertEqual(split.service_name, "my-service")
        self.assertEqual(len(split.targets), 2)

    def test_update_traffic_without_url(self):
        """Test updating traffic without URL (mock mode)."""
        targets = [
            TrafficTarget(revision_name="v1", percentage=100.0)
        ]
        
        split = self.manager.update_traffic("my-service", targets)

        self.assertEqual(split.service_name, "my-service")
        self.assertEqual(len(split.targets), 1)


class TestKnativeManagerEventSources(unittest.TestCase):
    """Tests for event source management methods."""

    def setUp(self):
        self.manager = KnativeManager()

    @patch('requests.Session.post')
    def test_create_ping_source(self, mock_post):
        """Test creating PingSource."""
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "metadata": {
                "name": "my-ping-source",
                "namespace": "default",
                "uid": "123-456",
                "creationTimestamp": "2024-01-01T00:00:00Z"
            }
        }
        mock_post.return_value = mock_response

        spec = EventSourceSpec(
            name="my-ping-source",
            source_type=EventSourceType.PING,
            params={"schedule": "*/2 * * * *", "data": "hello"}
        )
        
        source = self.manager.create_event_source(spec)

        self.assertEqual(source.name, "my-ping-source")

    def test_create_event_source_without_url(self):
        """Test creating event source without URL (mock mode)."""
        spec = EventSourceSpec(
            name="my-source",
            source_type=EventSourceType.KAFKA,
            sink_uri="http://my-service"
        )
        
        source = self.manager.create_event_source(spec)

        self.assertEqual(source.name, "my-source")

    @patch('requests.Session.get')
    def test_list_event_sources(self, mock_get):
        """Test listing event sources."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"items": []}
        mock_get.return_value = mock_response

        sources = self.manager.list_event_sources()

        self.assertEqual(len(sources), 0)


class TestKnativeManagerBrokers(unittest.TestCase):
    """Tests for broker management methods."""

    def setUp(self):
        self.manager = KnativeManager()

    @patch('requests.Session.post')
    def test_create_broker(self, mock_post):
        """Test creating broker."""
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "metadata": {
                "name": "my-broker",
                "namespace": "default",
                "uid": "123-456",
                "creationTimestamp": "2024-01-01T00:00:00Z"
            },
            "status": {"url": "http://my-broker"}
        }
        mock_post.return_value = mock_response

        spec = BrokerSpec(name="my-broker")
        
        broker = self.manager.create_broker(spec)

        self.assertEqual(broker.name, "my-broker")

    def test_create_broker_without_url(self):
        """Test creating broker without URL (mock mode)."""
        spec = BrokerSpec(name="my-broker")
        
        broker = self.manager.create_broker(spec)

        self.assertEqual(broker.name, "my-broker")


class TestKnativeManagerTriggers(unittest.TestCase):
    """Tests for trigger management methods."""

    def setUp(self):
        self.manager = KnativeManager()

    @patch('requests.Session.post')
    def test_create_trigger(self, mock_post):
        """Test creating trigger."""
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "metadata": {
                "name": "my-trigger",
                "namespace": "default",
                "uid": "123-456",
                "creationTimestamp": "2024-01-01T00:00:00Z"
            }
        }
        mock_post.return_value = mock_response

        spec = TriggerSpec(
            name="my-trigger",
            broker_name="my-broker",
            filter_attributes={"type": "dev.knative.samples.hello"}
        )
        
        trigger = self.manager.create_trigger(spec)

        self.assertEqual(trigger.name, "my-trigger")

    def test_create_trigger_without_url(self):
        """Test creating trigger without URL (mock mode)."""
        spec = TriggerSpec(
            name="my-trigger",
            broker_name="my-broker"
        )
        
        trigger = self.manager.create_trigger(spec)

        self.assertEqual(trigger.name, "my-trigger")


class TestKnativeManagerDomainMappings(unittest.TestCase):
    """Tests for domain mapping methods."""

    def setUp(self):
        self.manager = KnativeManager()

    @patch('requests.Session.post')
    def test_create_domain_mapping(self, mock_post):
        """Test creating domain mapping."""
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "metadata": {
                "name": "my-domain.example.com",
                "namespace": "default",
                "uid": "123-456",
                "creationTimestamp": "2024-01-01T00:00:00Z"
            },
            "status": {"url": "https://my-domain.example.com"}
        }
        mock_post.return_value = mock_response

        spec = DomainMappingSpec(
            name="my-domain.example.com",
            ref_service="my-service"
        )
        
        mapping = self.manager.create_domain_mapping(spec)

        self.assertEqual(mapping.name, "my-domain.example.com")

    def test_create_domain_mapping_without_url(self):
        """Test creating domain mapping without URL (mock mode)."""
        spec = DomainMappingSpec(
            name="my-domain.example.com",
            ref_service="my-service"
        )
        
        mapping = self.manager.create_domain_mapping(spec)

        self.assertEqual(mapping.name, "my-domain.example.com")

    @patch('requests.Session.get')
    def test_list_domain_mappings(self, mock_get):
        """Test listing domain mappings."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"items": []}
        mock_get.return_value = mock_response

        mappings = self.manager.list_domain_mappings()

        self.assertEqual(len(mappings), 0)


class TestKnativeManagerAutoScaling(unittest.TestCase):
    """Tests for auto-scaling methods."""

    def setUp(self):
        self.manager = KnativeManager()

    @patch('requests.Session.patch')
    @patch('requests.Session.get')
    def test_configure_autoscaling(self, mock_get, mock_patch):
        """Test configuring autoscaling."""
        mock_get_response = MagicMock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {
            "metadata": {
                "name": "my-service",
                "namespace": "default",
                "uid": "123-456",
                "creationTimestamp": "2024-01-01T00:00:00Z"
            },
            "status": {}
        }
        mock_get.return_value = mock_get_response

        mock_patch_response = MagicMock()
        mock_patch_response.status_code = 200
        mock_patch.return_value = mock_patch_response

        config = AutoScalingConfig(
            min_scale=2,
            max_scale=10,
            metric_type=AutoScalingMetric.CONCURRENCY,
            target_value=50.0
        )
        
        result = self.manager.configure_autoscaling("my-service", config)

        self.assertEqual(result.min_scale, 2)
        self.assertEqual(result.max_scale, 10)


class TestKnativeManagerConfigMaps(unittest.TestCase):
    """Tests for config map methods."""

    def setUp(self):
        self.manager = KnativeManager()

    @patch('requests.Session.post')
    def test_create_config_map(self, mock_post):
        """Test creating config map."""
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "metadata": {
                "name": "my-config",
                "namespace": "default",
                "uid": "123-456",
                "creationTimestamp": "2024-01-01T00:00:00Z"
            },
            "data": {"key": "value"}
        }
        mock_post.return_value = mock_response

        spec = ConfigMapSpec(
            name="my-config",
            data={"key": "value"}
        )
        
        cm = self.manager.create_config_map(spec)

        self.assertEqual(cm.name, "my-config")

    def test_create_config_map_without_url(self):
        """Test creating config map without URL (mock mode)."""
        spec = ConfigMapSpec(
            name="my-config",
            data={"key": "value"}
        )
        
        cm = self.manager.create_config_map(spec)

        self.assertEqual(cm.name, "my-config")

    @patch('requests.Session.get')
    def test_get_config_map(self, mock_get):
        """Test getting config map."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "metadata": {
                "name": "my-config",
                "namespace": "default",
                "uid": "123-456",
                "creationTimestamp": "2024-01-01T00:00:00Z"
            },
            "data": {"key": "value"}
        }
        mock_get.return_value = mock_response

        cm = self.manager.get_config_map("my-config")

        self.assertIsNotNone(cm)
        self.assertEqual(cm.name, "my-config")


class TestKnativeManagerServiceMesh(unittest.TestCase):
    """Tests for service mesh methods."""

    def setUp(self):
        self.manager = KnativeManager()

    @patch('requests.Session.patch')
    def test_configure_service_mesh_istio(self, mock_patch):
        """Test configuring Istio service mesh."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_patch.return_value = mock_response

        config = ServiceMeshConfig(
            mesh_type=ServiceMeshType.ISTIO,
            mtls_enabled=True
        )
        
        result = self.manager.configure_service_mesh("my-service", config)

        self.assertEqual(result.mesh_type, ServiceMeshType.ISTIO)
        self.assertTrue(result.mtls_enabled)


class TestKnativeManagerObservability(unittest.TestCase):
    """Tests for observability methods."""

    def setUp(self):
        self.manager = KnativeManager()

    @patch('requests.Session.patch')
    def test_configure_observability_logging(self, mock_patch):
        """Test configuring observability with logging settings."""
        self.manager.knative_serving_url = "http://serving:80"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_patch.return_value = mock_response

        config = ObservabilityConfig(
            logging_enabled=True,
            log_level="DEBUG"
        )
        
        result = self.manager.configure_observability("my-service", config)

        self.assertTrue(result.logging_enabled)
        self.assertEqual(result.log_level, "DEBUG")

    @patch('requests.Session.patch')
    def test_configure_observability_tracing(self, mock_patch):
        """Test configuring observability with tracing settings."""
        self.manager.knative_serving_url = "http://serving:80"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_patch.return_value = mock_response

        config = ObservabilityConfig(
            tracing_enabled=True,
            tracing_sample_rate=0.5,
            tracing_backend="jaeger"
        )
        
        result = self.manager.configure_observability("my-service", config)

        self.assertTrue(result.tracing_enabled)
        self.assertEqual(result.tracing_sample_rate, 0.5)


if __name__ == '__main__':
    unittest.main()

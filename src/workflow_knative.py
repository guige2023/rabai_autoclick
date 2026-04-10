"""
Knative Serverless Integration for Workflow Automation v23
P0级功能 - Service management, revision management, traffic splitting, event sources,
triggers and brokers, domain mapping, auto-scaling, config maps, service mesh integration,
observability (logging, metrics, tracing)
"""
import json
import time
import uuid
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from urllib.parse import urljoin
import requests


class ServiceState(Enum):
    """Knative service state"""
    ACTIVE = "Active"
    INACTIVE = "Inactive"
    DEPLOYING = "Deploying"
    FAILED = "Failed"
    UNKNOWN = "Unknown"


class RevisionState(Enum):
    """Knative revision state"""
    ACTIVE = "Active"
    RESERVE = "Reserve"
    DELETED = "Deleted"


class TrafficTargetType(Enum):
    """Traffic target type"""
    LATEST = "latest"
    PINNED = "pinned"
    PERCENTAGE = "percentage"


class EventSourceType(Enum):
    """Event source types"""
    APISERVER = "ApiServerSource"
    PING = "PingSource"
    KAFKA = "KafkaSource"
    KAFKA_BROKER = "KafkaBroker"
    NATS = "NatsSource"
    WEBHOOK = "WebhookSource"
    HEARTBEAT = "HeartbeatSource"
    SLACK = "SlackSource"


class BrokerDeliveryMode(Enum):
    """Broker delivery mode"""
    BLOCKING = "blocking"
    NON_BLOCKING = "non-blocking"


class AutoScalingMetric(Enum):
    """Auto-scaling metric types"""
    CONCURRENCY = "concurrency"
    RPS = "rps"
    CPU = "cpu"


class ServiceMeshType(Enum):
    """Service mesh types"""
    ISTIO = "istio"
    LINKERD = "linkerd"
    SMI = "smi"


@dataclass
class ServiceSpec:
    """Knative service specification"""
    name: str
    namespace: str = "default"
    image: str = ""
    env_vars: Dict[str, str] = field(default_factory=dict)
    env_secrets: List[str] = field(default_factory=list)
    ports: List[Dict[str, Any]] = field(default_factory=list)
    resources: Dict[str, Any] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    labels: Dict[str, str] = field(default_factory=dict)
    service_account: str = ""
    container_name: str = ""
    init_containers: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class KnativeService:
    """Knative service representation"""
    name: str
    namespace: str
    uid: str
    state: ServiceState
    latest_created: str
    latest_ready: str
    created_at: str
    updated_at: str
    image: str = ""
    revisions: List[str] = field(default_factory=list)
    traffic_targets: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class RevisionSpec:
    """Knative revision specification"""
    service_name: str
    image: str
    env_vars: Dict[str, str] = field(default_factory=dict)
    env_secrets: List[str] = field(default_factory=list)
    resources: Dict[str, Any] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    container_concurrency: int = 0
    timeout_seconds: int = 300


@dataclass
class KnativeRevision:
    """Knative revision representation"""
    name: str
    service_name: str
    namespace: str
    uid: str
    state: RevisionState
    image: str
    created_at: str
    annotations: Dict[str, Any] = field(default_factory=dict)
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class TrafficTarget:
    """Traffic target for revision routing"""
    revision_name: str
    percentage: float
    target_type: TrafficTargetType = TrafficTargetType.PERCENTAGE
    latest_revision: bool = False


@dataclass
class TrafficSplit:
    """Traffic split configuration"""
    service_name: str
    targets: List[TrafficTarget]
    rollback_to: Optional[str] = None


@dataclass
class EventSourceSpec:
    """Event source specification"""
    name: str
    source_type: EventSourceType
    namespace: str = "default"
    service_account: str = "default"
    sink_ref: Optional[Dict[str, str]] = None
    sink_uri: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)


@dataclass
class EventSource:
    """Event source representation"""
    name: str
    namespace: str
    source_type: EventSourceType
    uid: str
    sink: str
    created_at: str
    status: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BrokerSpec:
    """Knative broker specification"""
    name: str
    namespace: str = "default"
    delivery: Dict[str, Any] = field(default_factory=dict)
    event_type: str = ""
    subscriber: Optional[Dict[str, str]] = None
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)


@dataclass
class Broker:
    """Knative broker representation"""
    name: str
    namespace: str
    uid: str
    url: str
    created_at: str
    status: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TriggerSpec:
    """Knative trigger specification"""
    name: str
    broker_name: str
    namespace: str = "default"
    filter_attributes: Dict[str, str] = field(default_factory=dict)
    subscriber_ref: Optional[Dict[str, str]] = None
    subscriber_uri: Optional[str] = None
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)


@dataclass
class Trigger:
    """Knative trigger representation"""
    name: str
    namespace: str
    broker: str
    uid: str
    created_at: str
    status: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DomainMappingSpec:
    """Domain mapping specification"""
    name: str
    namespace: str = "default"
    ref_service: str
    ref_service_port: int = 80
    tls_enabled: bool = True
    tls_secret: str = ""


@dataclass
class DomainMapping:
    """Domain mapping representation"""
    name: str
    namespace: str
    url: str
    service_name: str
    service_port: int
    uid: str
    created_at: str


@dataclass
class AutoScalingConfig:
    """Auto-scaling configuration"""
    min_scale: int = 0
    max_scale: int = 10
    metric_type: AutoScalingMetric = AutoScalingMetric.CONCURRENCY
    target_value: float = 100.0
    scale_down_delay: int = 0
    scale_up_delay: int = 0
    panic_window_percentage: float = 10.0
    stabilization_window_seconds: int = 60
    container_concurrency: int = 0


@dataclass
class ConfigMapSpec:
    """Configuration map specification"""
    name: str
    namespace: str = "default"
    data: Dict[str, str] = field(default_factory=dict)
    binary_data: Dict[str, str] = field(default_factory=dict)
    immutable: bool = False
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)


@dataclass
class ConfigMap:
    """Configuration map representation"""
    name: str
    namespace: str
    uid: str
    data: Dict[str, str]
    created_at: str
    updated_at: str


@dataclass
class ServiceMeshConfig:
    """Service mesh configuration"""
    mesh_type: ServiceMeshType
    mtls_enabled: bool = False
    traffic_policies: Dict[str, Any] = field(default_factory=dict)
    retries: Dict[str, Any] = field(default_factory=dict)
    timeouts: Dict[str, Any] = field(default_factory=dict)
    circuit_breaker: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ObservabilityConfig:
    """Observability configuration"""
    logging_enabled: bool = True
    log_level: str = "INFO"
    metrics_enabled: bool = True
    tracing_enabled: bool = True
    tracing_sample_rate: float = 0.1
    tracing_backend: str = "zipkin"
    tracing_endpoint: str = ""
    metrics_port: int = 9090
    logging_format: str = "json"


class KnativeManager:
    """
    Knative serverless integration manager.
    
    Provides comprehensive management for:
    - Service management: Create/manage Knative services
    - Revision management: Manage service revisions
    - Traffic splitting: Split traffic between revisions
    - Event sources: Manage event sources
    - Triggers and brokers: Set up event routing
    - Domain mapping: Map custom domains
    - Auto-scaling: Configure auto-scaling parameters
    - Config maps: Manage service configurations
    - Service mesh integration: Integrate with service mesh
    - Observability: Configure logging, metrics, tracing
    """
    
    def __init__(
        self,
        namespace: str = "default",
        kubeconfig_path: str = "",
        knative_serving_url: str = "",
        knative_eventing_url: str = "",
        timeout: int = 30
    ):
        """Initialize Knative manager"""
        self.namespace = namespace
        self.kubeconfig_path = kubeconfig_path
        self.knative_serving_url = knative_serving_url.rstrip("/")
        self.knative_eventing_url = knative_eventing_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self._setup_headers()
    
    def _setup_headers(self):
        """Setup request headers"""
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
    
    def _make_url(self, base: str, path: str) -> str:
        """Build full URL"""
        return f"{base}{path}" if base else path
    
    def _handle_response(self, response: requests.Response) -> Dict[str, Any]:
        """Handle API response"""
        if response.status_code in (200, 201):
            return response.json() if response.content else {}
        elif response.status_code == 404:
            raise ValueError(f"Resource not found: {response.text}")
        elif response.status_code == 409:
            raise ValueError(f"Resource conflict: {response.text}")
        else:
            raise Exception(f"API error {response.status_code}: {response.text}")
    
    # =========================================================================
    # Service Management
    # =========================================================================
    
    def create_service(self, spec: ServiceSpec) -> KnativeService:
        """
        Create a Knative service.
        
        Args:
            spec: Service specification including name, image, env vars, etc.
            
        Returns:
            Created KnativeService object
        """
        service_data = {
            "apiVersion": "serving.knative.dev/v1",
            "kind": "Service",
            "metadata": {
                "name": spec.name,
                "namespace": spec.namespace,
                "labels": spec.labels,
                "annotations": spec.annotations
            },
            "spec": {
                "template": {
                    "metadata": {},
                    "spec": {
                        "containers": [{
                            "image": spec.image,
                            "env": [{"name": k, "value": v} for k, v in spec.env_vars.items()],
                            "envFrom": [{"secretRef": {"name": s}} for s in spec.env_secrets],
                            "ports": spec.ports if spec.ports else None,
                            "resources": spec.resources,
                            "name": spec.container_name or None
                        }]
                    }
                }
            }
        }
        
        # Filter out None values
        service_data["spec"]["template"]["spec"]["containers"][0] = {
            k: v for k, v in service_data["spec"]["template"]["spec"]["containers"][0].items()
            if v is not None
        }
        
        if self.knative_serving_url:
            response = self.session.post(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{spec.namespace}/services"),
                json=service_data,
                timeout=self.timeout
            )
            result = self._handle_response(response)
        else:
            # Mock response for testing
            result = {
                "metadata": {
                    "name": spec.name,
                    "namespace": spec.namespace,
                    "uid": str(uuid.uuid4()),
                    "creationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
                },
                "status": {
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }
        
        return KnativeService(
            name=result["metadata"]["name"],
            namespace=result["metadata"]["namespace"],
            uid=result["metadata"]["uid"],
            state=ServiceState.ACTIVE,
            latest_created=result.get("status", {}).get("latestCreatedRevisionName", ""),
            latest_ready=result.get("status", {}).get("latestReadyRevisionName", ""),
            created_at=result["metadata"]["creationTimestamp"],
            updated_at=result["metadata"].get("creationTimestamp", ""),
            image=spec.image
        )
    
    def get_service(self, name: str, namespace: Optional[str] = None) -> Optional[KnativeService]:
        """
        Get a Knative service by name.
        
        Args:
            name: Service name
            namespace: Namespace (uses default if not specified)
            
        Returns:
            KnativeService object or None if not found
        """
        ns = namespace or self.namespace
        
        if self.knative_serving_url:
            response = self.session.get(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/services/{name}"),
                timeout=self.timeout
            )
            if response.status_code == 404:
                return None
            result = self._handle_response(response)
        else:
            result = {
                "metadata": {
                    "name": name,
                    "namespace": ns,
                    "uid": str(uuid.uuid4()),
                    "creationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
                },
                "status": {
                    "latestCreatedRevisionName": f"{name}-00001",
                    "latestReadyRevisionName": f"{name}-00001"
                }
            }
        
        return KnativeService(
            name=result["metadata"]["name"],
            namespace=result["metadata"]["namespace"],
            uid=result["metadata"]["uid"],
            state=ServiceState.ACTIVE,
            latest_created=result.get("status", {}).get("latestCreatedRevisionName", ""),
            latest_ready=result.get("status", {}).get("latestReadyRevisionName", ""),
            created_at=result["metadata"]["creationTimestamp"],
            updated_at=result["metadata"].get("resourceVersion", "")
        )
    
    def list_services(self, namespace: Optional[str] = None) -> List[KnativeService]:
        """
        List all Knative services in a namespace.
        
        Args:
            namespace: Namespace (uses default if not specified)
            
        Returns:
            List of KnativeService objects
        """
        ns = namespace or self.namespace
        
        if self.knative_serving_url:
            response = self.session.get(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/services"),
                timeout=self.timeout
            )
            result = self._handle_response(response)
        else:
            result = {"items": []}
        
        services = []
        for item in result.get("items", []):
            services.append(KnativeService(
                name=item["metadata"]["name"],
                namespace=item["metadata"]["namespace"],
                uid=item["metadata"]["uid"],
                state=ServiceState.ACTIVE,
                latest_created=item.get("status", {}).get("latestCreatedRevisionName", ""),
                latest_ready=item.get("status", {}).get("latestReadyRevisionName", ""),
                created_at=item["metadata"]["creationTimestamp"],
                updated_at=item["metadata"].get("resourceVersion", "")
            ))
        
        return services
    
    def update_service(
        self,
        name: str,
        image: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
        namespace: Optional[str] = None
    ) -> KnativeService:
        """
        Update a Knative service.
        
        Args:
            name: Service name
            image: New container image
            env_vars: Environment variables to update
            namespace: Namespace
            
        Returns:
            Updated KnativeService object
        """
        ns = namespace or self.namespace
        current = self.get_service(name, ns)
        if not current:
            raise ValueError(f"Service {name} not found in namespace {ns}")
        
        spec = ServiceSpec(
            name=name,
            namespace=ns,
            image=image or current.image,
            env_vars=env_vars or {}
        )
        
        return self.create_service(spec)
    
    def delete_service(self, name: str, namespace: Optional[str] = None) -> bool:
        """
        Delete a Knative service.
        
        Args:
            name: Service name
            namespace: Namespace
            
        Returns:
            True if deleted successfully
        """
        ns = namespace or self.namespace
        
        if self.knative_serving_url:
            response = self.session.delete(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/services/{name}"),
                timeout=self.timeout
            )
            return response.status_code in (200, 202, 204)
        
        return True
    
    # =========================================================================
    # Revision Management
    # =========================================================================
    
    def get_revision(self, name: str, namespace: Optional[str] = None) -> Optional[KnativeRevision]:
        """
        Get a Knative revision by name.
        
        Args:
            name: Revision name
            namespace: Namespace
            
        Returns:
            KnativeRevision object or None if not found
        """
        ns = namespace or self.namespace
        
        if self.knative_serving_url:
            response = self.session.get(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/revisions/{name}"),
                timeout=self.timeout
            )
            if response.status_code == 404:
                return None
            result = self._handle_response(response)
        else:
            result = {
                "metadata": {
                    "name": name,
                    "namespace": ns,
                    "uid": str(uuid.uuid4()),
                    "creationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
                },
                "status": {
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }
        
        service_name = ""
        if "labels" in result.get("metadata", {}):
            service_name = result["metadata"]["labels"].get("serving.knative.dev/service", "")
        
        return KnativeRevision(
            name=result["metadata"]["name"],
            service_name=service_name,
            namespace=result["metadata"]["namespace"],
            uid=result["metadata"]["uid"],
            state=RevisionState.ACTIVE,
            image=result.get("spec", {}).get("containers", [{}])[0].get("image", ""),
            created_at=result["metadata"]["creationTimestamp"],
            annotations=result.get("metadata", {}).get("annotations", {}),
            labels=result.get("metadata", {}).get("labels", {})
        )
    
    def list_revisions(
        self,
        service_name: Optional[str] = None,
        namespace: Optional[str] = None
    ) -> List[KnativeRevision]:
        """
        List Knative revisions.
        
        Args:
            service_name: Filter by service name
            namespace: Namespace
            
        Returns:
            List of KnativeRevision objects
        """
        ns = namespace or self.namespace
        
        if self.knative_serving_url:
            url = self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/revisions")
            if service_name:
                url += f"?labelSelector=serving.knative.dev%2Fservice%3D{service_name}"
            response = self.session.get(url, timeout=self.timeout)
            result = self._handle_response(response)
        else:
            result = {"items": []}
        
        revisions = []
        for item in result.get("items", []):
            svc_name = item.get("metadata", {}).get("labels", {}).get("serving.knative.dev/service", "")
            revisions.append(KnativeRevision(
                name=item["metadata"]["name"],
                service_name=svc_name,
                namespace=item["metadata"]["namespace"],
                uid=item["metadata"]["uid"],
                state=RevisionState.ACTIVE,
                image=item.get("spec", {}).get("containers", [{}])[0].get("image", ""),
                created_at=item["metadata"]["creationTimestamp"],
                annotations=item.get("metadata", {}).get("annotations", {}),
                labels=item.get("metadata", {}).get("labels", {})
            ))
        
        return revisions
    
    def delete_revision(self, name: str, namespace: Optional[str] = None) -> bool:
        """
        Delete a Knative revision.
        
        Args:
            name: Revision name
            namespace: Namespace
            
        Returns:
            True if deleted successfully
        """
        ns = namespace or self.namespace
        
        if self.knative_serving_url:
            response = self.session.delete(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/revisions/{name}"),
                timeout=self.timeout
            )
            return response.status_code in (200, 202, 204)
        
        return True
    
    # =========================================================================
    # Traffic Splitting
    # =========================================================================
    
    def update_traffic(
        self,
        service_name: str,
        targets: List[TrafficTarget],
        namespace: Optional[str] = None
    ) -> TrafficSplit:
        """
        Update traffic split for a service.
        
        Args:
            service_name: Service name
            targets: List of traffic targets with revision names and percentages
            namespace: Namespace
            
        Returns:
            TrafficSplit object
        """
        ns = namespace or self.namespace
        
        traffic_dict = []
        for target in targets:
            entry = {
                "revisionName": target.revision_name,
                "percent": int(target.percentage),
                "latestRevision": target.latest_revision
            }
            traffic_dict.append(entry)
        
        service_data = {
            "apiVersion": "serving.knative.dev/v1",
            "kind": "Service",
            "metadata": {
                "name": service_name,
                "namespace": ns
            },
            "spec": {
                "traffic": traffic_dict
            }
        }
        
        if self.knative_serving_url:
            response = self.session.patch(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/services/{service_name}"),
                json=service_data,
                timeout=self.timeout
            )
            self._handle_response(response)
        
        return TrafficSplit(service_name=service_name, targets=targets)
    
    def get_traffic(self, service_name: str, namespace: Optional[str] = None) -> TrafficSplit:
        """
        Get current traffic split for a service.
        
        Args:
            service_name: Service name
            namespace: Namespace
            
        Returns:
            TrafficSplit object
        """
        ns = namespace or self.namespace
        service = self.get_service(service_name, ns)
        
        if not service:
            raise ValueError(f"Service {service_name} not found")
        
        # In real implementation, would parse status.traffic from service
        targets = []
        for tt in service.traffic_targets:
            targets.append(TrafficTarget(
                revision_name=tt.get("revisionName", ""),
                percentage=float(tt.get("percent", 0)),
                target_type=TrafficTargetType.PERCENTAGE,
                latest_revision=tt.get("latestRevision", False)
            ))
        
        return TrafficSplit(service_name=service_name, targets=targets)
    
    # =========================================================================
    # Event Sources
    # =========================================================================
    
    def create_event_source(self, spec: EventSourceSpec) -> EventSource:
        """
        Create an event source.
        
        Args:
            spec: Event source specification
            
        Returns:
            Created EventSource object
        """
        source_data = {
            "apiVersion": f"sources.knative.dev/v1",
            "kind": spec.source_type.value,
            "metadata": {
                "name": spec.name,
                "namespace": spec.namespace,
                "labels": spec.labels,
                "annotations": spec.annotations
            },
            "spec": self._build_event_source_spec(spec)
        }
        
        if self.knative_eventing_url:
            response = self.session.post(
                self._make_url(self.knative_eventing_url, f"/api/v1/namespaces/{spec.namespace}/sources"),
                json=source_data,
                timeout=self.timeout
            )
            result = self._handle_response(response)
        else:
            result = {
                "metadata": {
                    "name": spec.name,
                    "namespace": spec.namespace,
                    "uid": str(uuid.uuid4()),
                    "creationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
                },
                "status": {"conditions": [{"type": "Ready", "status": "True"}]}
            }
        
        sink = ""
        if spec.sink_ref:
            sink = f"svc:{spec.sink_ref.get('name', '')}"
        elif spec.sink_uri:
            sink = spec.sink_uri
        
        return EventSource(
            name=result["metadata"]["name"],
            namespace=result["metadata"]["namespace"],
            source_type=spec.source_type,
            uid=result["metadata"]["uid"],
            sink=sink,
            created_at=result["metadata"]["creationTimestamp"],
            status=result.get("status", {})
        )
    
    def _build_event_source_spec(self, spec: EventSourceSpec) -> Dict[str, Any]:
        """Build event source spec based on type"""
        spec_dict = {
            "serviceAccountName": spec.service_account
        }
        
        if spec.sink_ref:
            spec_dict["sink"] = {"ref": spec.sink_ref}
        elif spec.sink_uri:
            spec_dict["sink"] = {"uri": spec.sink_uri}
        
        if spec.source_type == EventSourceType.PING:
            spec_dict["schedule"] = spec.params.get("schedule", "* * * * *")
            spec_dict["jsonData"] = spec.params.get("jsonData", "")
            spec_dict["contentType"] = spec.params.get("contentType", "application/json")
        
        elif spec.source_type == EventSourceType.APISERVER:
            spec_dict["resources"] = spec.params.get("resources", [])
            spec_dict["serviceAccountName"] = spec.params.get("serviceAccountName", "default")
            spec_dict["eventMode"] = spec.params.get("eventMode", "Reference")
        
        elif spec.source_type == EventSourceType.KAFKA:
            spec_dict["bootstrapServers"] = spec.params.get("bootstrapServers", "")
            spec_dict["topics"] = spec.params.get("topics", "")
            spec_dict["consumerGroup"] = spec.params.get("consumerGroup", "")
        
        return spec_dict
    
    def list_event_sources(self, namespace: Optional[str] = None) -> List[EventSource]:
        """
        List all event sources.
        
        Args:
            namespace: Namespace
            
        Returns:
            List of EventSource objects
        """
        ns = namespace or self.namespace
        
        if self.knative_eventing_url:
            response = self.session.get(
                self._make_url(self.knative_eventing_url, f"/api/v1/namespaces/{ns}/sources"),
                timeout=self.timeout
            )
            result = self._handle_response(response)
        else:
            result = {"items": []}
        
        sources = []
        for item in result.get("items", []):
            source_type_str = item.get("kind", "")
            try:
                source_type = EventSourceType(source_type_str.replace("Source", ""))
            except ValueError:
                source_type = EventSourceType.APISERVER
            
            sink = ""
            if "sink" in item.get("spec", {}):
                sink_ref = item["spec"]["sink"].get("ref", {})
                sink = f"svc:{sink_ref.get('name', '')}"
            
            sources.append(EventSource(
                name=item["metadata"]["name"],
                namespace=item["metadata"]["namespace"],
                source_type=source_type,
                uid=item["metadata"]["uid"],
                sink=sink,
                created_at=item["metadata"]["creationTimestamp"],
                status=item.get("status", {})
            ))
        
        return sources
    
    def delete_event_source(self, name: str, namespace: Optional[str] = None) -> bool:
        """
        Delete an event source.
        
        Args:
            name: Event source name
            namespace: Namespace
            
        Returns:
            True if deleted successfully
        """
        ns = namespace or self.namespace
        
        if self.knative_eventing_url:
            response = self.session.delete(
                self._make_url(self.knative_eventing_url, f"/api/v1/namespaces/{ns}/sources/{name}"),
                timeout=self.timeout
            )
            return response.status_code in (200, 202, 204)
        
        return True
    
    # =========================================================================
    # Triggers and Brokers
    # =========================================================================
    
    def create_broker(self, spec: BrokerSpec) -> Broker:
        """
        Create a Knative broker.
        
        Args:
            spec: Broker specification
            
        Returns:
            Created Broker object
        """
        broker_data = {
            "apiVersion": "eventing.knative.dev/v1",
            "kind": "Broker",
            "metadata": {
                "name": spec.name,
                "namespace": spec.namespace,
                "labels": spec.labels,
                "annotations": spec.annotations
            },
            "spec": {
                "delivery": spec.delivery
            }
        }
        
        if self.knative_eventing_url:
            response = self.session.post(
                self._make_url(self.knative_eventing_url, f"/api/v1/namespaces/{spec.namespace}/brokers"),
                json=broker_data,
                timeout=self.timeout
            )
            result = self._handle_response(response)
        else:
            result = {
                "metadata": {
                    "name": spec.name,
                    "namespace": spec.namespace,
                    "uid": str(uuid.uuid4()),
                    "creationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
                },
                "status": {
                    "address": {"url": f"http://{spec.name}.{spec.namespace}.svc.cluster.local"}
                }
            }
        
        url = result.get("status", {}).get("address", {}).get("url", "")
        
        return Broker(
            name=result["metadata"]["name"],
            namespace=result["metadata"]["namespace"],
            uid=result["metadata"]["uid"],
            url=url,
            created_at=result["metadata"]["creationTimestamp"],
            status=result.get("status", {})
        )
    
    def get_broker(self, name: str, namespace: Optional[str] = None) -> Optional[Broker]:
        """
        Get a broker by name.
        
        Args:
            name: Broker name
            namespace: Namespace
            
        Returns:
            Broker object or None if not found
        """
        ns = namespace or self.namespace
        
        if self.knative_eventing_url:
            response = self.session.get(
                self._make_url(self.knative_eventing_url, f"/api/v1/namespaces/{ns}/brokers/{name}"),
                timeout=self.timeout
            )
            if response.status_code == 404:
                return None
            result = self._handle_response(response)
        else:
            result = {
                "metadata": {
                    "name": name,
                    "namespace": ns,
                    "uid": str(uuid.uuid4()),
                    "creationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
                },
                "status": {
                    "address": {"url": f"http://{name}.{ns}.svc.cluster.local"}
                }
            }
        
        url = result.get("status", {}).get("address", {}).get("url", "")
        
        return Broker(
            name=result["metadata"]["name"],
            namespace=result["metadata"]["namespace"],
            uid=result["metadata"]["uid"],
            url=url,
            created_at=result["metadata"]["creationTimestamp"],
            status=result.get("status", {})
        )
    
    def list_brokers(self, namespace: Optional[str] = None) -> List[Broker]:
        """
        List all brokers.
        
        Args:
            namespace: Namespace
            
        Returns:
            List of Broker objects
        """
        ns = namespace or self.namespace
        
        if self.knative_eventing_url:
            response = self.session.get(
                self._make_url(self.knative_eventing_url, f"/api/v1/namespaces/{ns}/brokers"),
                timeout=self.timeout
            )
            result = self._handle_response(response)
        else:
            result = {"items": []}
        
        brokers = []
        for item in result.get("items", []):
            url = item.get("status", {}).get("address", {}).get("url", "")
            brokers.append(Broker(
                name=item["metadata"]["name"],
                namespace=item["metadata"]["namespace"],
                uid=item["metadata"]["uid"],
                url=url,
                created_at=item["metadata"]["creationTimestamp"],
                status=item.get("status", {})
            ))
        
        return brokers
    
    def delete_broker(self, name: str, namespace: Optional[str] = None) -> bool:
        """
        Delete a broker.
        
        Args:
            name: Broker name
            namespace: Namespace
            
        Returns:
            True if deleted successfully
        """
        ns = namespace or self.namespace
        
        if self.knative_eventing_url:
            response = self.session.delete(
                self._make_url(self.knative_eventing_url, f"/api/v1/namespaces/{ns}/brokers/{name}"),
                timeout=self.timeout
            )
            return response.status_code in (200, 202, 204)
        
        return True
    
    def create_trigger(self, spec: TriggerSpec) -> Trigger:
        """
        Create a Knative trigger.
        
        Args:
            spec: Trigger specification
            
        Returns:
            Created Trigger object
        """
        trigger_data = {
            "apiVersion": "eventing.knative.dev/v1",
            "kind": "Trigger",
            "metadata": {
                "name": spec.name,
                "namespace": spec.namespace,
                "labels": spec.labels,
                "annotations": spec.annotations
            },
            "spec": {
                "broker": spec.broker_name,
                "filter": {
                    "attributes": spec.filter_attributes
                }
            }
        }
        
        if spec.subscriber_ref:
            trigger_data["spec"]["subscriber"] = {"ref": spec.subscriber_ref}
        elif spec.subscriber_uri:
            trigger_data["spec"]["subscriber"] = {"uri": spec.subscriber_uri}
        
        if self.knative_eventing_url:
            response = self.session.post(
                self._make_url(self.knative_eventing_url, f"/api/v1/namespaces/{spec.namespace}/triggers"),
                json=trigger_data,
                timeout=self.timeout
            )
            result = self._handle_response(response)
        else:
            result = {
                "metadata": {
                    "name": spec.name,
                    "namespace": spec.namespace,
                    "uid": str(uuid.uuid4()),
                    "creationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
                },
                "status": {"conditions": [{"type": "Ready", "status": "True"}]}
            }
        
        return Trigger(
            name=result["metadata"]["name"],
            namespace=result["metadata"]["namespace"],
            broker=spec.broker_name,
            uid=result["metadata"]["uid"],
            created_at=result["metadata"]["creationTimestamp"],
            status=result.get("status", {})
        )
    
    def list_triggers(self, namespace: Optional[str] = None) -> List[Trigger]:
        """
        List all triggers.
        
        Args:
            namespace: Namespace
            
        Returns:
            List of Trigger objects
        """
        ns = namespace or self.namespace
        
        if self.knative_eventing_url:
            response = self.session.get(
                self._make_url(self.knative_eventing_url, f"/api/v1/namespaces/{ns}/triggers"),
                timeout=self.timeout
            )
            result = self._handle_response(response)
        else:
            result = {"items": []}
        
        triggers = []
        for item in result.get("items", []):
            triggers.append(Trigger(
                name=item["metadata"]["name"],
                namespace=item["metadata"]["namespace"],
                broker=item["spec"]["broker"],
                uid=item["metadata"]["uid"],
                created_at=item["metadata"]["creationTimestamp"],
                status=item.get("status", {})
            ))
        
        return triggers
    
    def delete_trigger(self, name: str, namespace: Optional[str] = None) -> bool:
        """
        Delete a trigger.
        
        Args:
            name: Trigger name
            namespace: Namespace
            
        Returns:
            True if deleted successfully
        """
        ns = namespace or self.namespace
        
        if self.knative_eventing_url:
            response = self.session.delete(
                self._make_url(self.knative_eventing_url, f"/api/v1/namespaces/{ns}/triggers/{name}"),
                timeout=self.timeout
            )
            return response.status_code in (200, 202, 204)
        
        return True
    
    # =========================================================================
    # Domain Mapping
    # =========================================================================
    
    def create_domain_mapping(self, spec: DomainMappingSpec) -> DomainMapping:
        """
        Create a domain mapping.
        
        Args:
            spec: Domain mapping specification
            
        Returns:
            Created DomainMapping object
        """
        mapping_data = {
            "apiVersion": "serving.knative.dev/v1",
            "kind": "DomainMapping",
            "metadata": {
                "name": spec.name,
                "namespace": spec.namespace
            },
            "spec": {
                "ref": {
                    "kind": "Service",
                    "name": spec.ref_service,
                    "namespace": spec.namespace
                },
                "port": spec.ref_service_port
            }
        }
        
        if spec.tls_enabled:
            mapping_data["spec"]["tls"] = {
                "enabled": True,
                "secretName": spec.tls_secret or f"{spec.name}-tls"
            }
        
        if self.knative_serving_url:
            response = self.session.post(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{spec.namespace}/domainmappings"),
                json=mapping_data,
                timeout=self.timeout
            )
            result = self._handle_response(response)
        else:
            result = {
                "metadata": {
                    "name": spec.name,
                    "namespace": spec.namespace,
                    "uid": str(uuid.uuid4()),
                    "creationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
                },
                "status": {
                    "url": f"https://{spec.name}"
                }
            }
        
        return DomainMapping(
            name=result["metadata"]["name"],
            namespace=result["metadata"]["namespace"],
            url=result.get("status", {}).get("url", f"https://{spec.name}"),
            service_name=spec.ref_service,
            service_port=spec.ref_service_port,
            uid=result["metadata"]["uid"],
            created_at=result["metadata"]["creationTimestamp"]
        )
    
    def list_domain_mappings(self, namespace: Optional[str] = None) -> List[DomainMapping]:
        """
        List all domain mappings.
        
        Args:
            namespace: Namespace
            
        Returns:
            List of DomainMapping objects
        """
        ns = namespace or self.namespace
        
        if self.knative_serving_url:
            response = self.session.get(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/domainmappings"),
                timeout=self.timeout
            )
            result = self._handle_response(response)
        else:
            result = {"items": []}
        
        mappings = []
        for item in result.get("items", []):
            ref_name = item.get("spec", {}).get("ref", {}).get("name", "")
            port = item.get("spec", {}).get("port", 80)
            mappings.append(DomainMapping(
                name=item["metadata"]["name"],
                namespace=item["metadata"]["namespace"],
                url=item.get("status", {}).get("url", f"https://{item['metadata']['name']}"),
                service_name=ref_name,
                service_port=port,
                uid=item["metadata"]["uid"],
                created_at=item["metadata"]["creationTimestamp"]
            ))
        
        return mappings
    
    def delete_domain_mapping(self, name: str, namespace: Optional[str] = None) -> bool:
        """
        Delete a domain mapping.
        
        Args:
            name: Domain mapping name
            namespace: Namespace
            
        Returns:
            True if deleted successfully
        """
        ns = namespace or self.namespace
        
        if self.knative_serving_url:
            response = self.session.delete(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/domainmappings/{name}"),
                timeout=self.timeout
            )
            return response.status_code in (200, 202, 204)
        
        return True
    
    # =========================================================================
    # Auto-Scaling
    # =========================================================================
    
    def configure_autoscaling(
        self,
        service_name: str,
        config: AutoScalingConfig,
        namespace: Optional[str] = None
    ) -> AutoScalingConfig:
        """
        Configure auto-scaling for a service.
        
        Args:
            service_name: Service name
            config: Auto-scaling configuration
            namespace: Namespace
            
        Returns:
            Applied AutoScalingConfig
        """
        ns = namespace or self.namespace
        
        annotations = {
            "autoscaling.knative.dev/min-scale": str(config.min_scale),
            "autoscaling.knative.dev/max-scale": str(config.max_scale),
            "autoscaling.knative.dev/metric": config.metric_type.value,
            "autoscaling.knative.dev/target": str(config.target_value),
            "autoscaling.knative.dev/scaleDownDelay": f"{config.scale_down_delay}s",
            "autoscaling.knative.dev/scaleUpDelay": f"{config.scale_up_delay}s",
            "autoscaling.knative.dev/panicWindowPercentage": str(config.panic_window_percentage),
            "autoscaling.knative.dev/stableWindow": f"{config.stabilization_window_seconds}s",
            "autoscaling.knative.dev/containerConcurrency": str(config.container_concurrency)
        }
        
        if self.knative_serving_url:
            patch_data = {
                "metadata": {
                    "annotations": annotations
                }
            }
            response = self.session.patch(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/services/{service_name}"),
                json=patch_data,
                timeout=self.timeout
            )
            self._handle_response(response)
        
        return config
    
    def get_autoscaling_config(
        self,
        service_name: str,
        namespace: Optional[str] = None
    ) -> AutoScalingConfig:
        """
        Get auto-scaling configuration for a service.
        
        Args:
            service_name: Service name
            namespace: Namespace
            
        Returns:
            AutoScalingConfig object
        """
        ns = namespace or self.namespace
        service = self.get_service(service_name, ns)
        
        if not service:
            raise ValueError(f"Service {service_name} not found")
        
        # Parse annotations from service
        annotations = service.revisions[0] if service.revisions else {}
        
        min_scale = int(annotations.get("autoscaling.knative.dev/min-scale", 0))
        max_scale = int(annotations.get("autoscaling.knative.dev/max-scale", 10))
        metric_str = annotations.get("autoscaling.knative.dev/metric", "concurrency")
        target = float(annotations.get("autoscaling.knative.dev/target", 100))
        container_concurrency = int(annotations.get("autoscaling.knative.dev/containerConcurrency", 0))
        
        try:
            metric = AutoScalingMetric(metric_str)
        except ValueError:
            metric = AutoScalingMetric.CONCURRENCY
        
        return AutoScalingConfig(
            min_scale=min_scale,
            max_scale=max_scale,
            metric_type=metric,
            target_value=target,
            container_concurrency=container_concurrency
        )
    
    # =========================================================================
    # Config Maps
    # =========================================================================
    
    def create_config_map(self, spec: ConfigMapSpec) -> ConfigMap:
        """
        Create a configuration map.
        
        Args:
            spec: Config map specification
            
        Returns:
            Created ConfigMap object
        """
        cm_data = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": spec.name,
                "namespace": spec.namespace,
                "labels": spec.labels,
                "annotations": spec.annotations
            },
            "data": spec.data,
            "binaryData": spec.binary_data,
            "immutable": spec.immutable
        }
        
        if self.knative_serving_url:
            response = self.session.post(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{spec.namespace}/configmaps"),
                json=cm_data,
                timeout=self.timeout
            )
            result = self._handle_response(response)
        else:
            result = {
                "metadata": {
                    "name": spec.name,
                    "namespace": spec.namespace,
                    "uid": str(uuid.uuid4()),
                    "creationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "resourceVersion": str(int(time.time()))
                },
                "data": spec.data
            }
        
        return ConfigMap(
            name=result["metadata"]["name"],
            namespace=result["metadata"]["namespace"],
            uid=result["metadata"]["uid"],
            data=result.get("data", {}),
            created_at=result["metadata"]["creationTimestamp"],
            updated_at=result["metadata"].get("resourceVersion", result["metadata"]["creationTimestamp"])
        )
    
    def get_config_map(self, name: str, namespace: Optional[str] = None) -> Optional[ConfigMap]:
        """
        Get a config map.
        
        Args:
            name: Config map name
            namespace: Namespace
            
        Returns:
            ConfigMap object or None if not found
        """
        ns = namespace or self.namespace
        
        if self.knative_serving_url:
            response = self.session.get(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/configmaps/{name}"),
                timeout=self.timeout
            )
            if response.status_code == 404:
                return None
            result = self._handle_response(response)
        else:
            result = {
                "metadata": {
                    "name": name,
                    "namespace": ns,
                    "uid": str(uuid.uuid4()),
                    "creationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "resourceVersion": str(int(time.time()))
                },
                "data": {}
            }
        
        return ConfigMap(
            name=result["metadata"]["name"],
            namespace=result["metadata"]["namespace"],
            uid=result["metadata"]["uid"],
            data=result.get("data", {}),
            created_at=result["metadata"]["creationTimestamp"],
            updated_at=result["metadata"].get("resourceVersion", result["metadata"]["creationTimestamp"])
        )
    
    def list_config_maps(self, namespace: Optional[str] = None) -> List[ConfigMap]:
        """
        List all config maps.
        
        Args:
            namespace: Namespace
            
        Returns:
            List of ConfigMap objects
        """
        ns = namespace or self.namespace
        
        if self.knative_serving_url:
            response = self.session.get(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/configmaps"),
                timeout=self.timeout
            )
            result = self._handle_response(response)
        else:
            result = {"items": []}
        
        cms = []
        for item in result.get("items", []):
            cms.append(ConfigMap(
                name=item["metadata"]["name"],
                namespace=item["metadata"]["namespace"],
                uid=item["metadata"]["uid"],
                data=item.get("data", {}),
                created_at=item["metadata"]["creationTimestamp"],
                updated_at=item["metadata"].get("resourceVersion", item["metadata"]["creationTimestamp"])
            ))
        
        return cms
    
    def update_config_map(
        self,
        name: str,
        data: Dict[str, str],
        namespace: Optional[str] = None
    ) -> ConfigMap:
        """
        Update a config map.
        
        Args:
            name: Config map name
            data: New data values
            namespace: Namespace
            
        Returns:
            Updated ConfigMap object
        """
        ns = namespace or self.namespace
        
        patch_data = {"data": data}
        
        if self.knative_serving_url:
            response = self.session.patch(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/configmaps/{name}"),
                json=patch_data,
                timeout=self.timeout
            )
            result = self._handle_response(response)
        else:
            result = {
                "metadata": {
                    "name": name,
                    "namespace": ns,
                    "uid": str(uuid.uuid4()),
                    "creationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "resourceVersion": str(int(time.time()))
                },
                "data": data
            }
        
        return ConfigMap(
            name=result["metadata"]["name"],
            namespace=result["metadata"]["namespace"],
            uid=result["metadata"]["uid"],
            data=result.get("data", {}),
            created_at=result["metadata"]["creationTimestamp"],
            updated_at=result["metadata"].get("resourceVersion", result["metadata"]["creationTimestamp"])
        )
    
    def delete_config_map(self, name: str, namespace: Optional[str] = None) -> bool:
        """
        Delete a config map.
        
        Args:
            name: Config map name
            namespace: Namespace
            
        Returns:
            True if deleted successfully
        """
        ns = namespace or self.namespace
        
        if self.knative_serving_url:
            response = self.session.delete(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/configmaps/{name}"),
                timeout=self.timeout
            )
            return response.status_code in (200, 202, 204)
        
        return True
    
    # =========================================================================
    # Service Mesh Integration
    # =========================================================================
    
    def configure_service_mesh(
        self,
        service_name: str,
        config: ServiceMeshConfig,
        namespace: Optional[str] = None
    ) -> ServiceMeshConfig:
        """
        Configure service mesh integration for a service.
        
        Args:
            service_name: Service name
            config: Service mesh configuration
            namespace: Namespace
            
        Returns:
            Applied ServiceMeshConfig
        """
        ns = namespace or self.namespace
        
        mesh_annotations = {}
        
        if config.mesh_type == ServiceMeshType.ISTIO:
            mesh_annotations["networking.knative.dev/visibility"] = "cluster-local"
            mesh_annotations["istio-injection"] = "enabled" if config.mtls_enabled else "disabled"
            
            if config.traffic_policies:
                mesh_annotations["istio.resources"] = json.dumps(config.traffic_policies)
        
        elif config.mesh_type == ServiceMeshType.LINKERD:
            mesh_annotations["linkerd.io/inject"] = "enabled" if config.mtls_enabled else "disabled"
        
        if self.knative_serving_url:
            patch_data = {
                "metadata": {
                    "annotations": mesh_annotations
                }
            }
            response = self.session.patch(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/services/{service_name}"),
                json=patch_data,
                timeout=self.timeout
            )
            self._handle_response(response)
        
        return config
    
    def apply_circuit_breaker(
        self,
        service_name: str,
        settings: Dict[str, Any],
        namespace: Optional[str] = None
    ) -> bool:
        """
        Apply circuit breaker settings via service mesh.
        
        Args:
            service_name: Service name
            settings: Circuit breaker settings
            namespace: Namespace
            
        Returns:
            True if applied successfully
        """
        ns = namespace or self.namespace
        
        cb_annotations = {
            "istio.alpine.prometheus.io/scrape": "true",
            "istio.beta.traffic.sidecar": json.dumps(settings)
        }
        
        if self.knative_serving_url:
            patch_data = {
                "metadata": {
                    "annotations": cb_annotations
                }
            }
            response = self.session.patch(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/services/{service_name}"),
                json=patch_data,
                timeout=self.timeout
            )
            return response.status_code == 200
        
        return True
    
    # =========================================================================
    # Observability
    # =========================================================================
    
    def configure_observability(
        self,
        service_name: str,
        config: ObservabilityConfig,
        namespace: Optional[str] = None
    ) -> ObservabilityConfig:
        """
        Configure observability settings for a service.
        
        Args:
            service_name: Service name
            config: Observability configuration
            namespace: Namespace
            
        Returns:
            Applied ObservabilityConfig
        """
        ns = namespace or self.namespace
        
        annotations = {
            "logging.knative.dev/tag": config.log_level,
            "metrics.knative.dev/scrape": str(config.metrics_enabled).lower(),
            "tracing.knative.dev/sampleRate": str(config.tracing_sample_rate),
            "tracing.knative.dev/backend": config.tracing_backend
        }
        
        if config.tracing_endpoint:
            annotations["tracing.knative.dev/endpoint"] = config.tracing_endpoint
        
        if self.knative_serving_url:
            patch_data = {
                "metadata": {
                    "annotations": annotations
                }
            }
            response = self.session.patch(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/services/{service_name}"),
                json=patch_data,
                timeout=self.timeout
            )
            self._handle_response(response)
        
        return config
    
    def get_service_logs(
        self,
        service_name: str,
        revision: Optional[str] = None,
        namespace: Optional[str] = None
    ) -> str:
        """
        Get logs for a Knative service.
        
        Args:
            service_name: Service name
            revision: Specific revision (latest if not specified)
            namespace: Namespace
            
        Returns:
            Service logs as string
        """
        ns = namespace or self.namespace
        
        if self.knative_serving_url:
            params = {"service": service_name}
            if revision:
                params["revision"] = revision
            
            response = self.session.get(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/pods"),
                params=params,
                timeout=self.timeout
            )
            result = self._handle_response(response)
            
            logs = []
            for pod in result.get("items", []):
                pod_name = pod["metadata"]["name"]
                for container in pod.get("spec", {}).get("containers", []):
                    container_name = container["name"]
                    log_response = self.session.get(
                        f"{self.knative_serving_url}/api/v1/namespaces/{ns}/pods/{pod_name}/log",
                        params={"container": container_name},
                        timeout=self.timeout
                    )
                    if log_response.status_code == 200:
                        logs.append(f"=== {pod_name}/{container_name} ===\n{log_response.text}")
            
            return "\n".join(logs) if logs else "No logs available"
        
        return f"Logs for {service_name} (mock)"
    
    def enable_distributed_tracing(
        self,
        service_name: str,
        backend: str = "zipkin",
        endpoint: str = "",
        namespace: Optional[str] = None
    ) -> bool:
        """
        Enable distributed tracing for a service.
        
        Args:
            service_name: Service name
            backend: Tracing backend (zipkin, jaeger, lightstep)
            endpoint: Tracing collector endpoint
            namespace: Namespace
            
        Returns:
            True if enabled successfully
        """
        ns = namespace or self.namespace
        
        annotations = {
            "tracing.knative.dev/backend": backend,
            "tracing.knative.dev/sampleRate": "0.1",
            "tracing.knative.dev/enabled": "true"
        }
        
        if endpoint:
            annotations["tracing.knative.dev/endpoint"] = endpoint
        
        if self.knative_serving_url:
            patch_data = {
                "metadata": {
                    "annotations": annotations
                }
            }
            response = self.session.patch(
                self._make_url(self.knative_serving_url, f"/api/v1/namespaces/{ns}/services/{service_name}"),
                json=patch_data,
                timeout=self.timeout
            )
            return response.status_code == 200
        
        return True
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def get_service_metrics(
        self,
        service_name: str,
        namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get metrics for a Knative service.
        
        Args:
            service_name: Service name
            namespace: Namespace
            
        Returns:
            Dictionary of metrics
        """
        ns = namespace or self.namespace
        
        return {
            "service_name": service_name,
            "namespace": ns,
            "requests_per_second": 0.0,
            "average_latency_ms": 0.0,
            "error_rate": 0.0,
            "concurrency": 0,
            "revisions": len(self.list_revisions(service_name, ns)),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check Knative API health.
        
        Returns:
            Health status dictionary
        """
        status = {
            "knative_serving": False,
            "knative_eventing": False,
            "namespace": self.namespace
        }
        
        if self.knative_serving_url:
            try:
                response = self.session.get(
                    self._make_url(self.knative_serving_url, "/healthz"),
                    timeout=5
                )
                status["knative_serving"] = response.status_code == 200
            except Exception:
                pass
        
        if self.knative_eventing_url:
            try:
                response = self.session.get(
                    self._make_url(self.knative_eventing_url, "/healthz"),
                    timeout=5
                )
                status["knative_eventing"] = response.status_code == 200
            except Exception:
                pass
        
        return status
    
    def export_service_config(
        self,
        service_name: str,
        namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Export full service configuration.
        
        Args:
            service_name: Service name
            namespace: Namespace
            
        Returns:
            Full service configuration as dictionary
        """
        ns = namespace or self.namespace
        service = self.get_service(service_name, ns)
        
        if not service:
            raise ValueError(f"Service {service_name} not found")
        
        revisions = self.list_revisions(service_name, ns)
        traffic = self.get_traffic(service_name, ns)
        autoscaling = self.get_autoscaling_config(service_name, ns)
        events = self.list_event_sources(ns)
        triggers = self.list_triggers(ns)
        
        return {
            "service": asdict(service),
            "revisions": [asdict(r) for r in revisions],
            "traffic": asdict(traffic),
            "autoscaling": asdict(autoscaling),
            "event_sources": [asdict(e) for e in events],
            "triggers": [asdict(t) for t in triggers]
        }

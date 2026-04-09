"""Kubernetes orchestration action for container management.

This module provides comprehensive Kubernetes API interactions including:
- Pod, Deployment, Service, ConfigMap, Secret management
- Rolling updates and rollbacks
- Resource monitoring and health checks
- Namespace operations
- Custom Resource Definitions (CRD) support

Author: rabai_autoclick
Version: 1.0.0
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

try:
    from kubernetes import client, config
    from kubernetes.client import ApiClient, CoreV1Api, AppsV1Api, NetworkingV1Api
    from kubernetes.client.rest import ApiException
    KUBERNETES_AVAILABLE = True
except ImportError:
    KUBERNETES_AVAILABLE = False
    client = None
    config = None

logger = logging.getLogger(__name__)


class ResourceKind(Enum):
    """Kubernetes resource kinds."""
    POD = "Pod"
    DEPLOYMENT = "Deployment"
    SERVICE = "Service"
    CONFIGMAP = "ConfigMap"
    SECRET = "Secret"
    INGRESS = "Ingress"
    PERSISTENT_VOLUME = "PersistentVolume"
    PERSISTENT_VOLUME_CLAIM = "PersistentVolumeClaim"
    NAMESPACE = "Namespace"
    JOB = "Job"
    CRONJOB = "CronJob"
    DAEMON_SET = "DaemonSet"
    STATEFUL_SET = "StatefulSet"
    CUSTOM = "CustomResourceDefinition"


class PodPhase(Enum):
    """Kubernetes pod phases."""
    PENDING = "Pending"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    UNKNOWN = "Unknown"


@dataclass
class ContainerSpec:
    """Container specification for pods."""
    name: str
    image: str
    image_pull_policy: str = "IfNotPresent"
    command: Optional[List[str]] = None
    args: Optional[List[str]] = None
    env: Optional[Dict[str, str]] = None
    env_from: Optional[List[Dict[str, str]]] = None
    ports: Optional[List[int]] = None
    resources: Optional[Dict[str, Any]] = None
    liveness_probe: Optional[Dict[str, Any]] = None
    readiness_probe: Optional[Dict[str, Any]] = None
    volume_mounts: Optional[List[Dict[str, str]]] = None
    image_pull_secrets: Optional[str] = None


@dataclass
class DeploymentSpec:
    """Deployment specification."""
    name: str
    namespace: str = "default"
    replicas: int = 1
    containers: List[ContainerSpec] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    service_account: Optional[str] = None
    strategy_type: str = "RollingUpdate"
    max_surge: int = 1
    max_unavailable: int = 0
    revision_history_limit: int = 3
    selector_match_labels: Optional[Dict[str, str]] = None


@dataclass
class ServiceSpec:
    """Service specification."""
    name: str
    namespace: str = "default"
    service_type: str = "ClusterIP"
    selector: Dict[str, str] = field(default_factory=dict)
    ports: List[Dict[str, Union[int, str]]] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    external_ips: Optional[List[str]] = None
    load_balancer_ip: Optional[str] = None


@dataclass
class KubernetesEvent:
    """Kubernetes event information."""
    type: str
    reason: str
    message: str
    involved_object: Dict[str, str]
    first_timestamp: Optional[float] = None
    last_timestamp: Optional[float] = None
    count: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResourceStatus:
    """Resource status information."""
    name: str
    kind: ResourceKind
    namespace: str
    ready: str
    status: str
    age: str
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    spec: Dict[str, Any] = field(default_factory=dict)


class KubernetesAction:
    """Kubernetes orchestration action handler.

    This class provides a high-level interface for Kubernetes operations
    with built-in error handling, retry logic, and event handling.

    Example:
        action = KubernetesAction(kubeconfig="/path/to/kubeconfig")
        await action.apply_deployment(deployment_spec)
        pods = await action.list_pods(namespace="default")
        await action.scale_deployment("my-deployment", replicas=3)
    """

    def __init__(
        self,
        kubeconfig: Optional[str] = None,
        context: Optional[str] = None,
        in_cluster: bool = False,
        default_namespace: str = "default",
        timeout: int = 300,
        retry_attempts: int = 3,
        retry_delay: float = 2.0,
    ):
        """Initialize Kubernetes action.

        Args:
            kubeconfig: Path to kubeconfig file
            context: Specific kubeconfig context to use
            in_cluster: Whether running inside a Kubernetes cluster
            default_namespace: Default namespace for operations
            timeout: Operation timeout in seconds
            retry_attempts: Number of retry attempts for failed operations
            retry_delay: Delay between retries in seconds
        """
        self.kubeconfig = kubeconfig
        self.context = context
        self.in_cluster = in_cluster
        self.default_namespace = default_namespace
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay

        self._core_api: Optional[CoreV1Api] = None
        self._apps_api: Optional[AppsV1Api] = None
        self._networking_api: Optional[NetworkingV1Api] = None
        self._api_client: Optional[ApiClient] = None
        self._initialized = False

        if not KUBERNETES_AVAILABLE:
            logger.warning("kubernetes library not available")

    def _initialize(self) -> bool:
        """Initialize Kubernetes client configuration.

        Returns:
            True if initialization successful, False otherwise
        """
        if self._initialized:
            return True

        try:
            if self.in_cluster:
                config.load_incluster_config()
            elif self.kubeconfig:
                config.load_kube_config(
                    config_file=self.kubeconfig,
                    context=self.context
                )
            else:
                config.load_kube_config(context=self.context)

            self._api_client = ApiClient()
            self._core_api = CoreV1Api()
            self._apps_api = AppsV1Api()
            self._networking_api = NetworkingV1Api()
            self._initialized = True
            logger.info("Kubernetes client initialized successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize Kubernetes client: {e}")
            return False

    def _is_initialized(func: Callable) -> Callable:
        """Decorator to ensure Kubernetes client is initialized."""
        def wrapper(self, *args, **kwargs):
            if not self._initialized:
                if not self._initialize():
                    raise RuntimeError("Kubernetes client not initialized")
            return func(self, *args, **kwargs)
        return wrapper

    async def _execute_with_retry(
        self,
        operation: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Execute operation with retry logic.

        Args:
            operation: Async operation to execute
            *args: Positional arguments for operation
            **kwargs: Keyword arguments for operation

        Returns:
            Operation result

        Raises:
            Exception: If all retry attempts fail
        """
        last_exception = None

        for attempt in range(self.retry_attempts):
            try:
                return await operation(*args, **kwargs)
            except ApiException as e:
                last_exception = e
                if e.status == 409:
                    logger.warning(f"Conflict on attempt {attempt + 1}, retrying...")
                elif e.status >= 500:
                    logger.warning(f"Server error {e.status} on attempt {attempt + 1}, retrying...")
                else:
                    raise
            except Exception as e:
                last_exception = e
                logger.warning(f"Operation failed on attempt {attempt + 1}: {e}")

            if attempt < self.retry_attempts - 1:
                await asyncio.sleep(self.retry_delay * (attempt + 1))

        raise last_exception

    @_is_initialized
    async def create_namespace(
        self,
        name: str,
        labels: Optional[Dict[str, str]] = None,
        annotations: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Create a namespace.

        Args:
            name: Namespace name
            labels: Optional labels for namespace
            annotations: Optional annotations for namespace

        Returns:
            Created namespace object
        """
        namespace_body = client.V1Namespace(
            metadata=client.V1ObjectMeta(
                name=name,
                labels=labels or {},
                annotations=annotations or {}
            )
        )

        async def _create():
            return self._core_api.create_namespace(body=namespace_body)

        result = await self._execute_with_retry(_create)
        return self._api_client.sanitize_for_serialization(result)

    @_is_initialized
    async def delete_namespace(self, name: str) -> bool:
        """Delete a namespace.

        Args:
            name: Namespace name

        Returns:
            True if deletion successful
        """
        try:
            async def _delete():
                self._core_api.delete_namespace(name=name)
            await self._execute_with_retry(_delete)
            return True
        except ApiException as e:
            if e.status == 404:
                logger.warning(f"Namespace {name} not found")
                return True
            raise

    @_is_initialized
    async def list_namespaces(
        self,
        labels_filter: Optional[Dict[str, str]] = None,
    ) -> List[ResourceStatus]:
        """List all namespaces.

        Args:
            labels_filter: Filter by labels

        Returns:
            List of namespace statuses
        """
        result = self._core_api.list_namespace()

        namespaces = []
        for ns in result.items:
            ns_labels = ns.metadata.labels or {}
            if labels_filter:
                if not all(ns_labels.get(k) == v for k, v in labels_filter.items()):
                    continue

            namespaces.append(ResourceStatus(
                name=ns.metadata.name,
                kind=ResourceKind.NAMESPACE,
                namespace=ns.metadata.name,
                ready="Active",
                status=str(ns.status.phase) if ns.status else "Unknown",
                age=self._get_age(ns.metadata.creation_timestamp),
                labels=ns_labels,
                annotations=ns.metadata.annotations or {}
            ))

        return namespaces

    @_is_initialized
    async def create_deployment(self, spec: DeploymentSpec) -> Dict[str, Any]:
        """Create a deployment.

        Args:
            spec: Deployment specification

        Returns:
            Created deployment object
        """
        if not spec.containers:
            raise ValueError("At least one container is required")

        labels = spec.labels or {"app": spec.name}
        selector_labels = spec.selector_match_labels or {"app": spec.name}

        container_specs = []
        for container in spec.containers:
            ports = []
            if container.ports:
                for port in container.ports:
                    ports.append(client.V1ContainerPort(container_port=port))

            env = []
            if container.env:
                for key, value in container.env.items():
                    env.append(client.V1EnvVar(name=key, value=value))

            resources = None
            if container.resources:
                resources = client.V1ResourceRequirements(
                    limits=container.resources.get("limits"),
                    requests=container.resources.get("requests")
                )

            liveness_probe = None
            if container.liveness_probe:
                liveness_probe = client.V1Probe(
                    http_get=client.V1HTTPGetAction(
                        path=container.liveness_probe.get("path", "/"),
                        port=container.liveness_probe.get("port", 8080)
                    )
                )

            readiness_probe = None
            if container.readiness_probe:
                readiness_probe = client.V1Probe(
                    http_get=client.V1HTTPGetAction(
                        path=container.readiness_probe.get("path", "/"),
                        port=container.readiness_probe.get("port", 8080)
                    )
                )

            volume_mounts = None
            if container.volume_mounts:
                volume_mounts = [
                    client.V1VolumeMount(
                        name=vm["name"],
                        mount_path=vm["mountPath"]
                    )
                    for vm in container.volume_mounts
                ]

            container_specs.append(client.V1Container(
                name=container.name,
                image=container.image,
                image_pull_policy=container.image_pull_policy,
                command=container.command,
                args=container.args,
                env=env,
                ports=ports,
                resources=resources,
                liveness_probe=liveness_probe,
                readiness_probe=readiness_probe,
                volume_mounts=volume_mounts
            ))

        deployment_body = client.V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=client.V1ObjectMeta(
                name=spec.name,
                namespace=spec.namespace,
                labels=labels,
                annotations=spec.annotations
            ),
            spec=client.V1DeploymentSpec(
                replicas=spec.replicas,
                selector=client.V1LabelSelector(
                    match_labels=selector_labels
                ),
                strategy=client.V1DeploymentStrategy(
                    type=spec.strategy_type,
                    rolling_update=client.V1RollingUpdateDeployment(
                        max_surge=spec.max_surge,
                        max_unavailable=spec.max_unavailable
                    )
                ),
                revision_history_limit=spec.revision_history_limit,
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels=selector_labels
                    ),
                    spec=client.V1PodSpec(
                        containers=container_specs,
                        service_account_name=spec.service_account
                    )
                )
            )
        )

        async def _create():
            return self._apps_api.create_namespaced_deployment(
                body=deployment_body,
                namespace=spec.namespace
            )

        result = await self._execute_with_retry(_create)
        return self._api_client.sanitize_for_serialization(result)

    @_is_initialized
    async def get_deployment(
        self,
        name: str,
        namespace: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get deployment details.

        Args:
            name: Deployment name
            namespace: Namespace (uses default if not specified)

        Returns:
            Deployment object or None if not found
        """
        ns = namespace or self.default_namespace
        try:
            result = self._apps_api.read_namespaced_deployment(name=name, namespace=ns)
            return self._api_client.sanitize_for_serialization(result)
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    @_is_initialized
    async def scale_deployment(
        self,
        name: str,
        replicas: int,
        namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """Scale a deployment.

        Args:
            name: Deployment name
            replicas: Number of replicas
            namespace: Namespace (uses default if not specified)

        Returns:
            Scaled deployment object
        """
        ns = namespace or self.default_namespace

        body = client.V1Scale(
            spec=client.V1ScaleSpec(replicas=replicas)
        )

        async def _scale():
            return self._apps_api.patch_namespaced_deployment_scale(
                name=name,
                namespace=ns,
                body=body
            )

        result = await self._execute_with_retry(_scale)
        return self._api_client.sanitize_for_serialization(result)

    @_is_initialized
    async def delete_deployment(
        self,
        name: str,
        namespace: Optional[str] = None,
        grace_period: int = 30
    ) -> bool:
        """Delete a deployment.

        Args:
            name: Deployment name
            namespace: Namespace (uses default if not specified)
            grace_period: Grace period for termination

        Returns:
            True if deletion successful
        """
        ns = namespace or self.default_namespace
        try:
            async def _delete():
                self._apps_api.delete_namespaced_deployment(
                    name=name,
                    namespace=ns,
                    grace_period_seconds=grace_period
                )
            await self._execute_with_retry(_delete)
            return True
        except ApiException as e:
            if e.status == 404:
                return True
            raise

    @_is_initialized
    async def list_deployments(
        self,
        namespace: Optional[str] = None,
        labels_filter: Optional[Dict[str, str]] = None,
    ) -> List[ResourceStatus]:
        """List deployments.

        Args:
            namespace: Namespace (uses default if not specified)
            labels_filter: Filter by labels

        Returns:
            List of deployment statuses
        """
        ns = namespace or self.default_namespace
        result = self._apps_api.list_namespaced_deployment(namespace=ns)

        deployments = []
        for deploy in result.items:
            deploy_labels = deploy.metadata.labels or {}
            if labels_filter:
                if not all(deploy_labels.get(k) == v for k, v in labels_filter.items()):
                    continue

            ready = deploy.status.ready_replicas or 0
            desired = deploy.spec.replicas or 0

            deployments.append(ResourceStatus(
                name=deploy.metadata.name,
                kind=ResourceKind.DEPLOYMENT,
                namespace=ns,
                ready=f"{ready}/{desired}",
                status="Available" if ready == desired else "Scaling",
                age=self._get_age(deploy.metadata.creation_timestamp),
                labels=deploy_labels,
                annotations=deploy.metadata.annotations or {}
            ))

        return deployments

    @_is_initialized
    async def rollout_undo(
        self,
        name: str,
        namespace: Optional[str] = None,
        revision: Optional[int] = None
    ) -> Dict[str, Any]:
        """Undo a deployment rollout.

        Args:
            name: Deployment name
            namespace: Namespace
            revision: Specific revision to rollback to

        Returns:
            Deployment object after rollback
        """
        ns = namespace or self.default_namespace

        async def _undo():
            return self._apps_api.rollback_namespaced_deployment(
                name=name,
                namespace=ns,
                body=client.V1DeploymentRollback(
                    name=name,
                    rollback_to=client.V1RollbackConfig(revision=revision or 0)
                ) if revision is None else client.V1DeploymentRollback(
                    name=name,
                    rollback_to=client.V1RollbackConfig(revision=revision)
                )
            )

        result = await self._execute_with_retry(_undo)
        return self._api_client.sanitize_for_serialization(result)

    @_is_initialized
    async def create_service(self, spec: ServiceSpec) -> Dict[str, Any]:
        """Create a service.

        Args:
            spec: Service specification

        Returns:
            Created service object
        """
        if not spec.ports:
            raise ValueError("At least one port is required")

        service_ports = []
        for port_spec in spec.ports:
            service_ports.append(client.V1ServicePort(
                name=port_spec.get("name", f"port-{port_spec['port']}"),
                port=port_spec["port"],
                target_port=port_spec.get("targetPort", port_spec["port"]),
                protocol=port_spec.get("protocol", "TCP")
            ))

        service_body = client.V1Service(
            api_version="v1",
            kind="Service",
            metadata=client.V1ObjectMeta(
                name=spec.name,
                namespace=spec.namespace,
                labels=spec.labels,
                annotations=spec.annotations
            ),
            spec=client.V1ServiceSpec(
                type=spec.service_type,
                selector=spec.selector,
                ports=service_ports,
                external_ips=spec.external_ips,
                load_balancer_ip=spec.load_balancer_ip
            )
        )

        async def _create():
            return self._core_api.create_namespaced_service(
                body=service_body,
                namespace=spec.namespace
            )

        result = await self._execute_with_retry(_create)
        return self._api_client.sanitize_for_serialization(result)

    @_is_initialized
    async def delete_service(
        self,
        name: str,
        namespace: Optional[str] = None
    ) -> bool:
        """Delete a service.

        Args:
            name: Service name
            namespace: Namespace

        Returns:
            True if deletion successful
        """
        ns = namespace or self.default_namespace
        try:
            async def _delete():
                self._core_api.delete_namespaced_service(name=name, namespace=ns)
            await self._execute_with_retry(_delete)
            return True
        except ApiException as e:
            if e.status == 404:
                return True
            raise

    @_is_initialized
    async def get_service(
        self,
        name: str,
        namespace: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get service details.

        Args:
            name: Service name
            namespace: Namespace

        Returns:
            Service object or None
        """
        ns = namespace or self.default_namespace
        try:
            result = self._core_api.read_namespaced_service(name=name, namespace=ns)
            return self._api_client.sanitize_for_serialization(result)
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    @_is_initialized
    async def list_services(
        self,
        namespace: Optional[str] = None,
    ) -> List[ResourceStatus]:
        """List services.

        Args:
            namespace: Namespace

        Returns:
            List of service statuses
        """
        ns = namespace or self.default_namespace
        result = self._core_api.list_namespaced_service(namespace=ns)

        services = []
        for svc in result.items:
            ports = []
            if svc.spec and svc.spec.ports:
                ports = [f"{p.port}:{p.target_port}" for p in svc.spec.ports]

            services.append(ResourceStatus(
                name=svc.metadata.name,
                kind=ResourceKind.SERVICE,
                namespace=ns,
                ready=",".join(ports) if ports else "None",
                status=str(svc.spec.type) if svc.spec else "Unknown",
                age=self._get_age(svc.metadata.creation_timestamp),
                labels=svc.metadata.labels or {}
            ))

        return services

    @_is_initialized
    async def create_configmap(
        self,
        name: str,
        data: Optional[Dict[str, str]] = None,
        namespace: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Create a ConfigMap.

        Args:
            name: ConfigMap name
            data: ConfigMap data
            namespace: Namespace
            labels: Labels

        Returns:
            Created ConfigMap
        """
        ns = namespace or self.default_namespace

        body = client.V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            metadata=client.V1ObjectMeta(
                name=name,
                namespace=ns,
                labels=labels or {}
            ),
            data=data or {}
        )

        async def _create():
            return self._core_api.create_namespaced_config_map(namespace=ns, body=body)

        result = await self._execute_with_retry(_create)
        return self._api_client.sanitize_for_serialization(result)

    @_is_initialized
    async def create_secret(
        self,
        name: str,
        data: Optional[Dict[str, str]] = None,
        string_data: Optional[Dict[str, str]] = None,
        secret_type: str = "Opaque",
        namespace: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a Secret.

        Args:
            name: Secret name
            data: Encoded data
            string_data: String data (will be base64 encoded)
            secret_type: Secret type
            namespace: Namespace

        Returns:
            Created Secret
        """
        ns = namespace or self.default_namespace

        body = client.V1Secret(
            api_version="v1",
            kind="Secret",
            metadata=client.V1ObjectMeta(
                name=name,
                namespace=ns
            ),
            type=secret_type,
            data=data or {},
            string_data=string_data or {}
        )

        async def _create():
            return self._core_api.create_namespaced_secret(namespace=ns, body=body)

        result = await self._execute_with_retry(_create)
        return self._api_client.sanitize_for_serialization(result)

    @_is_initialized
    async def list_pods(
        self,
        namespace: Optional[str] = None,
        labels_filter: Optional[Dict[str, str]] = None,
        field_selector: Optional[str] = None,
    ) -> List[ResourceStatus]:
        """List pods.

        Args:
            namespace: Namespace
            labels_filter: Filter by labels
            field_selector: Field selector

        Returns:
            List of pod statuses
        """
        ns = namespace or self.default_namespace

        async def _list():
            return self._core_api.list_namespaced_pod(
                namespace=ns,
                label_selector=self._labels_to_selector(labels_filter) if labels_filter else None,
                field_selector=field_selector
            )

        result = await self._execute_with_retry(_list)

        pods = []
        for pod in result.items:
            pod_labels = pod.metadata.labels or {}
            phase = str(pod.status.phase) if pod.status else "Unknown"

            ready_containers = 0
            total_containers = len(pod.spec.containers) if pod.spec else 0

            if pod.status and pod.status.container_statuses:
                ready_containers = sum(
                    1 for cs in pod.status.container_statuses if cs.ready
                )

            pods.append(ResourceStatus(
                name=pod.metadata.name,
                kind=ResourceKind.POD,
                namespace=ns,
                ready=f"{ready_containers}/{total_containers}",
                status=phase,
                age=self._get_age(pod.metadata.creation_timestamp),
                labels=pod_labels,
                annotations=pod.metadata.annotations or {}
            ))

        return pods

    @_is_initialized
    async def get_pod_logs(
        self,
        name: str,
        namespace: Optional[str] = None,
        container: Optional[str] = None,
        tail_lines: int = 100,
        follow: bool = False
    ) -> str:
        """Get pod logs.

        Args:
            name: Pod name
            namespace: Namespace
            container: Container name
            tail_lines: Number of lines to tail
            follow: Follow log output

        Returns:
            Log content
        """
        ns = namespace or self.default_namespace

        result = self._core_api.read_namespaced_pod_log(
            name=name,
            namespace=ns,
            container=container,
            tail_lines=tail_lines,
            follow=follow
        )

        return result

    @_is_initialized
    async def delete_pod(
        self,
        name: str,
        namespace: Optional[str] = None,
        grace_period: int = 30
    ) -> bool:
        """Delete a pod.

        Args:
            name: Pod name
            namespace: Namespace
            grace_period: Grace period

        Returns:
            True if deletion successful
        """
        ns = namespace or self.default_namespace
        try:
            self._core_api.delete_namespaced_pod(
                name=name,
                namespace=ns,
                grace_period_seconds=grace_period
            )
            return True
        except ApiException as e:
            if e.status == 404:
                return True
            raise

    @_is_initialized
    async def create_ingress(
        self,
        name: str,
        rules: List[Dict[str, Any]],
        namespace: str = "default",
        annotations: Optional[Dict[str, str]] = None,
        tls_hosts: Optional[List[str]] = None,
        tls_secret_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create an Ingress.

        Args:
            name: Ingress name
            rules: List of ingress rules
            namespace: Namespace
            annotations: Annotations
            tls_hosts: TLS hosts
            tls_secret_name: TLS secret name

        Returns:
            Created Ingress
        """
        ingress_rules = []
        for rule in rules:
            http_paths = []
            for path in rule.get("http_paths", []):
                http_paths.append(client.V1HTTPIngressPath(
                    path=path.get("path", "/"),
                    path_type=path.get("path_type", "Prefix"),
                    backend=client.V1IngressBackend(
                        service=client.V1IngressServiceBackend(
                            name=path["service_name"],
                            port=client.V1ServiceBackendPort(
                                number=path["service_port"]
                            )
                        )
                    )
                ))

            ingress_rules.append(client.V1IngressRule(
                host=rule.get("host"),
                http=client.V1HTTPIngressRuleValue(paths=http_paths)
            ))

        tls = None
        if tls_hosts:
            tls = [client.V1IngressTLS(
                hosts=tls_hosts,
                secret_name=tls_secret_name
            )]

        body = client.V1Ingress(
            api_version="networking.k8s.io/v1",
            kind="Ingress",
            metadata=client.V1ObjectMeta(
                name=name,
                namespace=namespace,
                annotations=annotations or {}
            ),
            spec=client.V1IngressSpec(
                rules=ingress_rules,
                tls=tls
            )
        )

        async def _create():
            return self._networking_api.create_namespaced_ingress(
                namespace=namespace,
                body=body
            )

        result = await self._execute_with_retry(_create)
        return self._api_client.sanitize_for_serialization(result)

    @_is_initialized
    async def get_events(
        self,
        namespace: Optional[str] = None,
        involved_object_name: Optional[str] = None,
        involved_object_kind: Optional[str] = None,
    ) -> List[KubernetesEvent]:
        """Get events.

        Args:
            namespace: Namespace
            involved_object_name: Filter by involved object name
            involved_object_kind: Filter by involved object kind

        Returns:
            List of events
        """
        ns = namespace or self.default_namespace

        field_selector = None
        if involved_object_name:
            field_selector = f"involvedObject.name={involved_object_name}"

        result = self._core_api.list_namespaced_event(
            namespace=ns,
            field_selector=field_selector
        )

        events = []
        for event in result.items:
            if involved_object_kind:
                if event.involved_object.kind != involved_object_kind:
                    continue

            events.append(KubernetesEvent(
                type=event.type or "Normal",
                reason=event.reason or "",
                message=event.message or "",
                involved_object={
                    "kind": event.involved_object.kind,
                    "name": event.involved_object.name,
                    "namespace": event.involved_object.namespace
                },
                first_timestamp=event.first_timestamp.timestamp() if event.first_timestamp else None,
                last_timestamp=event.last_timestamp.timestamp() if event.last_timestamp else None,
                count=event.count or 1,
                metadata={
                    "uid": event.metadata.uid if event.metadata else None
                }
            ))

        return events

    async def apply_from_yaml(
        self,
        yaml_content: str,
        namespace: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Apply resources from YAML.

        Args:
            yaml_content: YAML content
            namespace: Default namespace

        Returns:
            List of applied resources
        """
        if not YAML_AVAILABLE:
            raise ImportError("PyYAML required for YAML parsing")

        docs = list(yaml.safe_load_all(yaml_content))
        results = []

        for doc in docs:
            if not doc:
                continue

            kind = doc.get("kind")
            name = doc.get("metadata", {}).get("name")
            ns = doc.get("metadata", {}).get("namespace") or namespace or self.default_namespace

            if kind == "Namespace":
                result = await self.create_namespace(name)
            elif kind == "Deployment":
                spec = self._yaml_to_deployment_spec(doc)
                result = await self.create_deployment(spec)
            elif kind == "Service":
                spec = self._yaml_to_service_spec(doc)
                result = await self.create_service(spec)
            elif kind == "ConfigMap":
                result = await self.create_configmap(
                    name=name,
                    data=doc.get("data", {}),
                    namespace=ns
                )
            elif kind == "Secret":
                result = await self.create_secret(
                    name=name,
                    data=doc.get("data", {}),
                    string_data=doc.get("stringData", {}),
                    secret_type=doc.get("type", "Opaque"),
                    namespace=ns
                )
            else:
                logger.warning(f"Unsupported resource kind: {kind}")
                continue

            results.append(result)

        return results

    def _yaml_to_deployment_spec(self, doc: Dict[str, Any]) -> DeploymentSpec:
        """Convert YAML document to DeploymentSpec."""
        metadata = doc.get("metadata", {})
        spec = doc.get("spec", {})
        template = spec.get("template", {})

        containers = []
        for container in template.get("spec", {}).get("containers", []):
            containers.append(ContainerSpec(
                name=container["name"],
                image=container["image"],
                image_pull_policy=container.get("imagePullPolicy", "IfNotPresent"),
                command=container.get("command"),
                args=container.get("args"),
                env=container.get("env"),
                ports=container.get("ports")
            ))

        return DeploymentSpec(
            name=metadata.get("name", ""),
            namespace=metadata.get("namespace", "default"),
            replicas=spec.get("replicas", 1),
            containers=containers,
            labels=metadata.get("labels", {}),
            annotations=metadata.get("annotations", {}),
            strategy_type=spec.get("strategy", {}).get("type", "RollingUpdate")
        )

    def _yaml_to_service_spec(self, doc: Dict[str, Any]) -> ServiceSpec:
        """Convert YAML document to ServiceSpec."""
        metadata = doc.get("metadata", {})
        spec = doc.get("spec", {})

        ports = []
        for port in spec.get("ports", []):
            ports.append({
                "name": port.get("name", f"port-{port['port']}"),
                "port": port["port"],
                "targetPort": port.get("targetPort", port["port"]),
                "protocol": port.get("protocol", "TCP")
            })

        return ServiceSpec(
            name=metadata.get("name", ""),
            namespace=metadata.get("namespace", "default"),
            service_type=spec.get("type", "ClusterIP"),
            selector=spec.get("selector", {}),
            ports=ports,
            labels=metadata.get("labels", {}),
            annotations=metadata.get("annotations", {})
        )

    def _get_age(self, creation_timestamp: Optional[Any]) -> str:
        """Get age string from timestamp."""
        if not creation_timestamp:
            return "Unknown"

        delta = time.time() - creation_timestamp.timestamp()

        if delta < 60:
            return f"{int(delta)}s"
        elif delta < 3600:
            return f"{int(delta / 60)}m"
        elif delta < 86400:
            return f"{int(delta / 3600)}h"
        else:
            return f"{int(delta / 86400)}d"

    def _labels_to_selector(self, labels: Dict[str, str]) -> str:
        """Convert labels dict to selector string."""
        return ",".join(f"{k}={v}" for k, v in labels.items())


# Singleton instance
_kubernetes_action_instance: Optional[KubernetesAction] = None


def get_kubernetes_action(
    kubeconfig: Optional[str] = None,
    context: Optional[str] = None,
    in_cluster: bool = False,
    **kwargs
) -> KubernetesAction:
    """Get singleton Kubernetes action instance.

    Args:
        kubeconfig: Path to kubeconfig
        context: Specific context
        in_cluster: Running in cluster
        **kwargs: Additional arguments

    Returns:
        KubernetesAction instance
    """
    global _kubernetes_action_instance

    if _kubernetes_action_instance is None:
        _kubernetes_action_instance = KubernetesAction(
            kubeconfig=kubeconfig,
            context=context,
            in_cluster=in_cluster,
            **kwargs
        )

    return _kubernetes_action_instance

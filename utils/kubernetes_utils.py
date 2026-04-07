"""
Kubernetes utilities for cluster operations and workload management.

Provides pod management, deployment scaling, service discovery,
configmap/secret handling, job scheduling, and resource monitoring.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional

logger = logging.getLogger(__name__)


class PodStatus(Enum):
    """Kubernetes pod phases."""
    PENDING = auto()
    RUNNING = auto()
    SUCCEEDED = auto()
    FAILED = auto()
    UNKNOWN = auto()


@dataclass
class PodInfo:
    """Information about a Kubernetes pod."""
    name: str
    namespace: str
    status: PodStatus
    node: str
    ip: str
    labels: dict[str, str] = field(default_factory=dict)
    containers: list[str] = field(default_factory=list)
    restart_count: int = 0
    age_seconds: float = 0.0


@dataclass
class DeploymentConfig:
    """Configuration for a Kubernetes deployment."""
    name: str
    image: str
    namespace: str = "default"
    replicas: int = 1
    port: int = 80
    env_vars: dict[str, str] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)
    resource_limits: Optional[dict[str, str]] = None
    health_check_path: str = "/health"
    readiness_probe_initial_delay: int = 10


@dataclass
class ServiceConfig:
    """Configuration for a Kubernetes service."""
    name: str
    selector: dict[str, str]
    namespace: str = "default"
    port: int = 80
    target_port: int = 80
    service_type: str = "ClusterIP"  # ClusterIP, NodePort, LoadBalancer


class KubernetesClient:
    """High-level Kubernetes API client."""

    def __init__(
        self,
        kubeconfig_path: Optional[str] = None,
        context: Optional[str] = None,
        namespace: str = "default",
    ) -> None:
        self.kubeconfig_path = kubeconfig_path or os.path.expanduser("~/.kube/config")
        self.context = context
        self.namespace = namespace
        self._client: Any = None
        self._core_api: Any = None
        self._apps_api: Any = None

    def _get_client(self) -> Any:
        """Lazily initialize the Kubernetes client."""
        if self._client is None:
            try:
                from kubernetes import client, config
                if os.path.exists(self.kubeconfig_path):
                    config.load_kube_config(
                        config_file=self.kubeconfig_path,
                        context=self.context,
                    )
                else:
                    config.load_incluster_config()
                self._client = client
                self._core_api = client.CoreV1Api()
                self._apps_api = client.AppsV1Api()
            except ImportError:
                logger.warning("kubernetes package not installed")
                self._client = None
        return self._client

    def list_pods(self, label_selector: Optional[str] = None) -> list[PodInfo]:
        """List pods in the namespace."""
        client = self._get_client()
        if client is None:
            return []
        try:
            pods = self._core_api.list_namespaced_pod(
                self.namespace,
                label_selector=label_selector,
            )
            return [self._pod_to_info(p) for p in pods.items]
        except Exception as e:
            logger.error("Failed to list pods: %s", e)
            return []

    def _pod_to_info(self, pod: Any) -> PodInfo:
        from kubernetes.client import V1PodStatus
        phase = pod.status.phase if pod.status else "Unknown"
        status = PodStatus.UNKNOWN
        if phase == "Pending":
            status = PodStatus.PENDING
        elif phase == "Running":
            status = PodStatus.RUNNING
        elif phase == "Succeeded":
            status = PodStatus.SUCCEEDED
        elif phase == "Failed":
            status = PodStatus.FAILED

        return PodInfo(
            name=pod.metadata.name,
            namespace=pod.metadata.namespace,
            status=status,
            node=pod.spec.node_name or "",
            ip=pod.status.pod_ip if pod.status else "",
            labels=dict(pod.metadata.labels) if pod.metadata.labels else {},
            containers=[c.name for c in (pod.spec.containers or [])],
            restart_count=sum(c.restart_count for c in (pod.status.container_statuses or [])),
            age_seconds=(time.time() - pod.metadata.creation_timestamp.timestamp()) if pod.metadata.creation_timestamp else 0,
        )

    def get_pod_logs(
        self,
        pod_name: str,
        container: Optional[str] = None,
        tail_lines: int = 100,
    ) -> str:
        """Get logs from a pod."""
        client = self._get_client()
        if client is None:
            return ""
        try:
            return self._core_api.read_namespaced_pod_log(
                name=pod_name,
                namespace=self.namespace,
                container=container,
                tail_lines=tail_lines,
            )
        except Exception as e:
            logger.error("Failed to get pod logs: %s", e)
            return ""

    def create_deployment(self, config: DeploymentConfig) -> bool:
        """Create a Kubernetes deployment."""
        client = self._get_client()
        if client is None:
            return False
        try:
            container = client.V1Container(
                name=config.name,
                image=config.image,
                ports=[client.V1ContainerPort(container_port=config.port)],
                env=[client.V1EnvVar(name=k, value=v) for k, v in config.env_vars.items()],
                readiness_probe=client.V1Probe(
                    http_get=client.V1HTTPGetAction(
                        path=config.health_check_path,
                        port=config.port,
                    ),
                    initial_delay_seconds=config.readiness_probe_initial_delay,
                ),
            )
            spec = client.V1DeploymentSpec(
                replicas=config.replicas,
                selector=client.V1LabelSelector(match_labels={"app": config.name}),
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(labels={"app": config.name, **config.labels}),
                    spec=client.V1PodSpec(containers=[container]),
                ),
            )
            deployment = client.V1Deployment(
                api_version="apps/v1",
                kind="Deployment",
                metadata=client.V1ObjectMeta(name=config.name, namespace=config.namespace),
                spec=spec,
            )
            self._apps_api.create_namespaced_deployment(namespace=config.namespace, body=deployment)
            logger.info("Created deployment: %s", config.name)
            return True
        except Exception as e:
            logger.error("Failed to create deployment: %s", e)
            return False

    def scale_deployment(self, name: str, replicas: int) -> bool:
        """Scale a deployment."""
        client = self._get_client()
        if client is None:
            return False
        try:
            body = {"spec": {"replicas": replicas}}
            self._apps_api.patch_namespaced_deployment_scale(
                name=name,
                namespace=self.namespace,
                body=body,
            )
            logger.info("Scaled deployment %s to %d replicas", name, replicas)
            return True
        except Exception as e:
            logger.error("Failed to scale deployment: %s", e)
            return False

    def delete_deployment(self, name: str) -> bool:
        """Delete a deployment."""
        client = self._get_client()
        if client is None:
            return False
        try:
            self._apps_api.delete_namespaced_deployment(
                name=name,
                namespace=self.namespace,
                body=client.V1DeleteOptions(),
            )
            return True
        except Exception as e:
            logger.error("Failed to delete deployment: %s", e)
            return False

    def create_service(self, config: ServiceConfig) -> bool:
        """Create a Kubernetes service."""
        client = self._get_client()
        if client is None:
            return False
        try:
            spec = client.V1ServiceSpec(
                selector=config.selector,
                ports=[client.V1ServicePort(port=config.port, target_port=config.target_port)],
                type=config.service_type,
            )
            service = client.V1Service(
                api_version="v1",
                kind="Service",
                metadata=client.V1ObjectMeta(name=config.name, namespace=config.namespace),
                spec=spec,
            )
            self._core_api.create_namespaced_service(namespace=config.namespace, body=service)
            logger.info("Created service: %s", config.name)
            return True
        except Exception as e:
            logger.error("Failed to create service: %s", e)
            return False

    def create_configmap(self, name: str, data: dict[str, str]) -> bool:
        """Create or update a ConfigMap."""
        client = self._get_client()
        if client is None:
            return False
        try:
            configmap = client.V1ConfigMap(
                api_version="v1",
                kind="ConfigMap",
                metadata=client.V1ObjectMeta(name=name, namespace=self.namespace),
                data=data,
            )
            self._core_api.create_namespaced_config_map(namespace=self.namespace, body=configmap)
            return True
        except Exception as e:
            logger.error("Failed to create configmap: %s", e)
            return False

    def create_secret(self, name: str, data: dict[str, str]) -> bool:
        """Create or update a Secret."""
        import base64
        client = self._get_client()
        if client is None:
            return False
        try:
            encoded_data = {k: base64.b64encode(v.encode()).decode() for k, v in data.items()}
            secret = client.V1Secret(
                api_version="v1",
                kind="Secret",
                metadata=client.V1ObjectMeta(name=name, namespace=self.namespace),
                data=encoded_data,
                type="Opaque",
            )
            self._core_api.create_namespaced_secret(namespace=self.namespace, body=secret)
            return True
        except Exception as e:
            logger.error("Failed to create secret: %s", e)
            return False

    def wait_for_pod_ready(self, label_selector: str, timeout: int = 300) -> bool:
        """Wait for a pod matching the label selector to be ready."""
        start = time.time()
        while time.time() - start < timeout:
            pods = self.list_pods(label_selector=label_selector)
            if any(p.status == PodStatus.RUNNING for p in pods):
                return True
            time.sleep(2)
        return False


import os

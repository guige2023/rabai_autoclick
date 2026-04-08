"""Kubernetes integration for RabAI AutoClick.

Provides actions to manage K8s resources, pods, deployments, and execute commands in containers.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class KubernetesPodAction(BaseAction):
    """Manage Kubernetes pods - create, delete, exec, logs.

    Provides pod lifecycle management and container operations.
    """
    action_type = "k8s_pod"
    display_name = "K8s Pod管理"
    description = "管理Kubernetes Pod：创建、删除、日志、执行命令"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Kubernetes pods.

        Args:
            context: Execution context.
            params: Dict with keys:
                - kubeconfig: Path to kubeconfig (or use incluster)
                - api_server: API server URL
                - token: Service account token
                - namespace: Namespace (default: default)
                - operation: create | delete | get | list | logs | exec | describe
                - pod_name: Pod name (for get/delete/logs/exec)
                - container_name: Container name (for logs/exec)
                - image: Container image (for create)
                - command: Command to execute (list or string)
                - tail_lines: Number of log lines (default 100)
                - labels: Dict of labels (for create)

        Returns:
            ActionResult with pod data.
        """
        namespace = params.get('namespace', 'default')

        try:
            from kubernetes import client, config
            from kubernetes.client.rest import ApiException
        except ImportError:
            return ActionResult(success=False, message="kubernetes package not installed. Run: pip install kubernetes")

        try:
            if params.get('kubeconfig'):
                config.load_kube_config(config_file=params['kubeconfig'])
            else:
                try:
                    config.load_incluster_config()
                except Exception:
                    config.load_kube_config()

            v1 = client.CoreV1Api()
            operation = params.get('operation', 'list')

            if operation == 'create':
                pod_name = params.get('pod_name')
                if not pod_name:
                    return ActionResult(success=False, message="pod_name is required")

                container = client.V1Container(
                    name=pod_name,
                    image=params.get('image', 'nginx'),
                    command=params.get('command'),
                )

                metadata = client.V1ObjectMeta(
                    name=pod_name,
                    namespace=namespace,
                    labels=params.get('labels'),
                )

                pod_spec = client.V1PodSpec(containers=[container])
                pod = client.V1Pod(metadata=metadata, spec=pod_spec)

                result = v1.create_namespaced_pod(namespace, pod)
                return ActionResult(success=True, message=f"Pod {pod_name} created", data={'name': result.metadata.name})

            elif operation == 'delete':
                pod_name = params.get('pod_name')
                if not pod_name:
                    return ActionResult(success=False, message="pod_name is required")

                v1.delete_namespaced_pod(pod_name, namespace, client.V1DeleteOptions())
                return ActionResult(success=True, message=f"Pod {pod_name} deleted")

            elif operation == 'get':
                pod_name = params.get('pod_name')
                if not pod_name:
                    return ActionResult(success=False, message="pod_name is required")

                pod = v1.read_namespaced_pod(pod_name, namespace)
                return ActionResult(success=True, message="Pod retrieved", data={
                    'name': pod.metadata.name,
                    'status': pod.status.phase,
                    'namespace': pod.metadata.namespace,
                })

            elif operation == 'list':
                label_selector = params.get('label_selector', '')
                pods = v1.list_namespaced_pod(namespace, label_selector=label_selector)
                return ActionResult(
                    success=True,
                    message=f"Found {len(pods.items)} pods",
                    data={'pods': [{'name': p.metadata.name, 'status': p.status.phase} for p in pods.items]}
                )

            elif operation == 'logs':
                pod_name = params.get('pod_name')
                if not pod_name:
                    return ActionResult(success=False, message="pod_name is required")

                container_name = params.get('container_name')
                tail_lines = params.get('tail_lines', 100)

                logs = v1.read_namespaced_pod_log(
                    pod_name, namespace,
                    container=container_name,
                    tail_lines=tail_lines,
                )
                return ActionResult(success=True, message=f"Retrieved logs from {pod_name}", data={'logs': logs})

            elif operation == 'describe':
                pod_name = params.get('pod_name')
                if not pod_name:
                    return ActionResult(success=False, message="pod_name is required")

                import subprocess
                result = subprocess.run(
                    ['kubectl', 'describe', 'pod', pod_name, '-n', namespace],
                    capture_output=True, text=True, timeout=30
                )
                return ActionResult(success=True, message="Pod described", data={'output': result.stdout})

            elif operation == 'exec':
                pod_name = params.get('pod_name')
                if not pod_name:
                    return ActionResult(success=False, message="pod_name is required")

                command = params.get('command')
                if isinstance(command, list):
                    command = ' '.join(command)

                container_name = params.get('container_name')

                import subprocess
                cmd = ['kubectl', 'exec', pod_name, '-n', namespace]
                if container_name:
                    cmd.extend(['-c', container_name])
                cmd.extend(['--', 'sh', '-c', command])

                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                return ActionResult(
                    success=(result.returncode == 0),
                    message=f"Exec completed (exit {result.returncode})",
                    data={'stdout': result.stdout, 'stderr': result.stderr, 'returncode': result.returncode}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except ImportError:
            return ActionResult(success=False, message="kubernetes package not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"K8s error: {str(e)}")


class KubernetesDeploymentAction(BaseAction):
    """Manage Kubernetes deployments - create, scale, update, rollback.

    Handles deployment lifecycle and scaling operations.
    """
    action_type = "k8s_deployment"
    display_name = "K8s Deployment"
    description = "管理Kubernetes Deployment：创建、扩缩容、更新、回滚"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Kubernetes deployments.

        Args:
            context: Execution context.
            params: Dict with keys:
                - kubeconfig: Path to kubeconfig
                - namespace: Namespace
                - operation: create | scale | update | rollback | get | list | delete
                - name: Deployment name
                - image: Container image (for create/update)
                - replicas: Number of replicas (for create/scale)
                - labels: Dict of labels (for create)
                - strategy: RollingUpdate or Recreate

        Returns:
            ActionResult with deployment data.
        """
        namespace = params.get('namespace', 'default')

        try:
            from kubernetes import client, config
        except ImportError:
            return ActionResult(success=False, message="kubernetes package not installed")

        try:
            if params.get('kubeconfig'):
                config.load_kube_config(config_file=params['kubeconfig'])
            else:
                try:
                    config.load_incluster_config()
                except Exception:
                    config.load_kube_config()

            apps_v1 = client.AppsV1Api()
            operation = params.get('operation', 'list')

            if operation == 'create':
                name = params.get('name')
                if not name:
                    return ActionResult(success=False, message="name is required")

                container = client.V1Container(
                    name=name,
                    image=params.get('image', 'nginx'),
                )

                selector = client.V1LabelSelector(match_labels={'app': name})
                strategy = client.V1DeploymentStrategy(
                    type=params.get('strategy', 'RollingUpdate')
                )

                template = client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(labels={'app': name, **params.get('labels', {})}),
                    spec=client.V1PodSpec(containers=[container])
                )

                spec = client.V1DeploymentSpec(
                    replicas=params.get('replicas', 1),
                    selector=selector,
                    strategy=strategy,
                    template=template,
                )

                deployment = client.V1Deployment(
                    metadata=client.V1ObjectMeta(name=name, namespace=namespace),
                    spec=spec
                )

                result = apps_v1.create_namespaced_deployment(namespace, deployment)
                return ActionResult(success=True, message=f"Deployment {name} created", data={'name': result.metadata.name})

            elif operation == 'scale':
                name = params.get('name')
                replicas = params.get('replicas')
                if not name or replicas is None:
                    return ActionResult(success=False, message="name and replicas are required")

                result = apps_v1.read_namespaced_deployment(name, namespace)
                result.spec.replicas = replicas
                apps_v1.patch_namespaced_deployment_scale(name, namespace, client.V1Scale(spec=client.V1ScaleSpec(replicas=replicas)))
                return ActionResult(success=True, message=f"Deployment {name} scaled to {replicas}")

            elif operation == 'update':
                name = params.get('name')
                image = params.get('image')
                if not name or not image:
                    return ActionResult(success=False, message="name and image are required")

                body = {
                    'spec': {
                        'template': {
                            'spec': {
                                'containers': [{'name': name, 'image': image}]
                            }
                        }
                    }
                }
                apps_v1.patch_namespaced_deployment(name, namespace, body)
                return ActionResult(success=True, message=f"Deployment {name} image updated to {image}")

            elif operation == 'rollback':
                name = params.get('name')
                if not name:
                    return ActionResult(success=False, message="name is required")

                body = {'name': name}
                apps_v1.create_namespaced_deployment_rollback(name, namespace, body)
                return ActionResult(success=True, message=f"Deployment {name} rolled back")

            elif operation == 'get':
                name = params.get('name')
                if not name:
                    return ActionResult(success=False, message="name is required")

                deploy = apps_v1.read_namespaced_deployment(name, namespace)
                return ActionResult(success=True, message="Deployment retrieved", data={
                    'name': deploy.metadata.name,
                    'replicas': deploy.spec.replicas,
                    'ready_replicas': deploy.status.ready_replicas or 0,
                    'available_replicas': deploy.status.available_replicas or 0,
                })

            elif operation == 'list':
                deployments = apps_v1.list_namespaced_deployment(namespace)
                return ActionResult(
                    success=True,
                    message=f"Found {len(deployments.items)} deployments",
                    data={'deployments': [{'name': d.metadata.name, 'replicas': d.spec.replicas} for d in deployments.items]}
                )

            elif operation == 'delete':
                name = params.get('name')
                if not name:
                    return ActionResult(success=False, message="name is required")

                apps_v1.delete_namespaced_deployment(name, namespace, client.V1DeleteOptions())
                return ActionResult(success=True, message=f"Deployment {name} deleted")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except ImportError:
            return ActionResult(success=False, message="kubernetes package not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"K8s error: {str(e)}")


class KubernetesConfigMapAction(BaseAction):
    """Manage Kubernetes ConfigMaps and Secrets.

    Handles configuration data management.
    """
    action_type = "k8s_configmap"
    display_name = "K8s ConfigMap"
    description = "管理Kubernetes ConfigMap和Secret"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage K8s ConfigMaps and Secrets.

        Args:
            context: Execution context.
            params: Dict with keys:
                - kubeconfig: Path to kubeconfig
                - namespace: Namespace
                - operation: create_cm | get_cm | list_cm | delete_cm | create_secret | list_secret
                - name: ConfigMap/Secret name
                - data: Dict of key-value pairs
                - type: Secret type (for secrets)

        Returns:
            ActionResult with configmap data.
        """
        namespace = params.get('namespace', 'default')

        try:
            from kubernetes import client, config
        except ImportError:
            return ActionResult(success=False, message="kubernetes package not installed")

        try:
            if params.get('kubeconfig'):
                config.load_kube_config(config_file=params['kubeconfig'])
            else:
                try:
                    config.load_incluster_config()
                except Exception:
                    config.load_kube_config()

            core_v1 = client.CoreV1Api()
            operation = params.get('operation', 'list_cm')

            if operation == 'create_cm':
                name = params.get('name')
                if not name:
                    return ActionResult(success=False, message="name is required")

                cm = client.V1ConfigMap(
                    metadata=client.V1ObjectMeta(name=name, namespace=namespace),
                    data=params.get('data', {}),
                )
                core_v1.create_namespaced_config_map(namespace, cm)
                return ActionResult(success=True, message=f"ConfigMap {name} created")

            elif operation == 'get_cm':
                name = params.get('name')
                if not name:
                    return ActionResult(success=False, message="name is required")
                cm = core_v1.read_namespaced_config_map(name, namespace)
                return ActionResult(success=True, message="ConfigMap retrieved", data={'data': cm.data, 'metadata': {'name': cm.metadata.name}})

            elif operation == 'list_cm':
                cms = core_v1.list_namespaced_config_map(namespace)
                return ActionResult(success=True, message=f"Found {len(cms.items)} ConfigMaps", data={'configmaps': [c.metadata.name for c in cms.items]})

            elif operation == 'delete_cm':
                name = params.get('name')
                if not name:
                    return ActionResult(success=False, message="name is required")
                core_v1.delete_namespaced_config_map(name, namespace, client.V1DeleteOptions())
                return ActionResult(success=True, message=f"ConfigMap {name} deleted")

            elif operation == 'create_secret':
                name = params.get('name')
                if not name:
                    return ActionResult(success=False, message="name is required")

                secret = client.V1Secret(
                    metadata=client.V1ObjectMeta(name=name, namespace=namespace),
                    data={k: __import__('base64').b64encode(v.encode()).decode() for k, v in (params.get('data', {})).items()},
                    type=params.get('type', 'Opaque'),
                )
                core_v1.create_namespaced_secret(namespace, secret)
                return ActionResult(success=True, message=f"Secret {name} created")

            elif operation == 'list_secret':
                secrets = core_v1.list_namespaced_secret(namespace)
                return ActionResult(success=True, message=f"Found {len(secrets.items)} Secrets", data={'secrets': [s.metadata.name for s in secrets.items]})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except ImportError:
            return ActionResult(success=False, message="kubernetes package not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"K8s error: {str(e)}")

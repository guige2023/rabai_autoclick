"""
Kubernetes cluster interaction actions.
"""
from __future__ import annotations

import json
import subprocess
from typing import Dict, Any, Optional, List


def run_kubectl(
    args: List[str],
    namespace: Optional[str] = None,
    kubeconfig: Optional[str] = None
) -> Dict[str, Any]:
    """
    Execute a kubectl command.

    Args:
        args: kubectl arguments (e.g., ['get', 'pods']).
        namespace: Kubernetes namespace.
        kubeconfig: Path to kubeconfig file.

    Returns:
        Dictionary with 'stdout', 'stderr', 'returncode'.

    Raises:
        RuntimeError: If kubectl is not available.
    """
    cmd = ['kubectl']

    if kubeconfig:
        cmd.extend(['--kubeconfig', kubeconfig])

    if namespace:
        cmd.extend(['-n', namespace])

    cmd.extend(args)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )

        return {
            'stdout': result.stdout,
            'stderr': result.stderr,
            'returncode': result.returncode,
            'success': result.returncode == 0,
        }
    except FileNotFoundError:
        raise RuntimeError("kubectl not found. Is Kubernetes installed?")
    except subprocess.TimeoutExpired:
        raise RuntimeError("kubectl command timed out")
    except Exception as e:
        raise RuntimeError(f"kubectl failed: {e}")


def get_pods(namespace: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List pods in a namespace or cluster.

    Args:
        namespace: Kubernetes namespace (None for all namespaces).

    Returns:
        List of pod information dictionaries.
    """
    args = ['get', 'pods', '-o', 'json']

    result = run_kubectl(args, namespace=namespace)

    if not result['success']:
        raise RuntimeError(f"Failed to get pods: {result['stderr']}")

    data = json.loads(result['stdout'])
    pods = []

    for item in data.get('items', []):
        status = item.get('status', {})
        containers = item.get('spec', {}).get('containers', [])

        pods.append({
            'name': item['metadata']['name'],
            'namespace': item['metadata']['namespace'],
            'status': status.get('phase', 'Unknown'),
            'ready': _get_container_ready(status),
            'containers': [c['name'] for c in containers],
            'image': containers[0]['image'] if containers else None,
            'created': item['metadata'].get('creationTimestamp'),
            'pod_ip': status.get('podIP'),
            'host_ip': status.get('hostIP'),
        })

    return pods


def _get_container_ready(status: Dict[str, Any]) -> str:
    """Extract ready status from pod status."""
    conditions = status.get('conditions', [])
    for cond in conditions:
        if cond.get('type') == 'Ready':
            return 'True' if cond.get('status') == 'True' else 'False'
    return 'False'


def get_services(namespace: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List services in a namespace.

    Args:
        namespace: Kubernetes namespace.

    Returns:
        List of service information dictionaries.
    """
    args = ['get', 'services', '-o', 'json']
    result = run_kubectl(args, namespace=namespace)

    if not result['success']:
        raise RuntimeError(f"Failed to get services: {result['stderr']}")

    data = json.loads(result['stdout'])
    services = []

    for item in data.get('items', []):
        spec = item.get('spec', {})

        services.append({
            'name': item['metadata']['name'],
            'namespace': item['metadata']['namespace'],
            'type': spec.get('type', 'ClusterIP'),
            'cluster_ip': spec.get('clusterIP'),
            'external_ip': spec.get('externalIPs'),
            'ports': spec.get('ports', []),
            'selector': spec.get('selector', {}),
        })

    return services


def get_nodes() -> List[Dict[str, Any]]:
    """
    List all nodes in the cluster.

    Returns:
        List of node information dictionaries.
    """
    result = run_kubectl(['get', 'nodes', '-o', 'json'])

    if not result['success']:
        raise RuntimeError(f"Failed to get nodes: {result['stderr']}")

    data = json.loads(result['stdout'])
    nodes = []

    for item in data.get('items', []):
        status = item.get('status', {})
        allocatable = status.get('allocatable', {})
        capacity = status.get('capacity', {})

        nodes.append({
            'name': item['metadata']['name'],
            'status': _get_node_status(status),
            'roles': _get_node_roles(item),
            'cpu': capacity.get('cpu'),
            'memory': capacity.get('memory'),
            'allocatable_cpu': allocatable.get('cpu'),
            'allocatable_memory': allocatable.get('memory'),
            'pod_cidr': item.get('spec', {}).get('podCIDR'),
        })

    return nodes


def _get_node_status(status: Dict[str, Any]) -> str:
    """Get node ready status."""
    conditions = status.get('conditions', [])
    for cond in conditions:
        if cond.get('type') == 'Ready':
            return cond.get('status', 'Unknown')
    return 'Unknown'


def _get_node_roles(item: Dict[str, Any]) -> List[str]:
    """Extract node roles from labels."""
    labels = item.get('metadata', {}).get('labels', {})
    roles = []

    for key, value in labels.items():
        if key == 'node-role.kubernetes.io/master' or key.endswith('/master'):
            roles.append('master')
        elif key.startswith('node-role.kubernetes.io/'):
            roles.append(key.split('/')[-1])

    return roles if roles else ['worker']


def get_deployments(namespace: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List deployments in a namespace.

    Args:
        namespace: Kubernetes namespace.

    Returns:
        List of deployment information dictionaries.
    """
    args = ['get', 'deployments', '-o', 'json']
    result = run_kubectl(args, namespace=namespace)

    if not result['success']:
        raise RuntimeError(f"Failed to get deployments: {result['stderr']}")

    data = json.loads(result['stdout'])
    deployments = []

    for item in data.get('items', []):
        spec = item.get('spec', {})
        status = item.get('status', {})

        deployments.append({
            'name': item['metadata']['name'],
            'namespace': item['metadata']['namespace'],
            'replicas': spec.get('replicas', 0),
            'ready_replicas': status.get('readyReplicas', 0),
            'available_replicas': status.get('availableReplicas', 0),
            'updated_replicas': status.get('updatedReplicas', 0),
            'image': _get_deployment_image(spec),
            'selector': spec.get('selector', {}),
        })

    return deployments


def _get_deployment_image(spec: Dict[str, Any]) -> Optional[str]:
    """Extract container image from deployment spec."""
    containers = spec.get('template', {}).get('spec', {}).get('containers', [])
    return containers[0].get('image') if containers else None


def scale_deployment(
    name: str,
    replicas: int,
    namespace: Optional[str] = None
) -> Dict[str, Any]:
    """
    Scale a deployment to a specific number of replicas.

    Args:
        name: Deployment name.
        replicas: Target replica count.
        namespace: Kubernetes namespace.

    Returns:
        Result dictionary.
    """
    args = ['scale', f'deployment/{name}', f'--replicas={replicas}']
    result = run_kubectl(args, namespace=namespace)

    if not result['success']:
        raise RuntimeError(f"Failed to scale deployment: {result['stderr']}")

    return {
        'name': name,
        'replicas': replicas,
        'success': True,
    }


def get_pod_logs(
    pod_name: str,
    namespace: str,
    container: Optional[str] = None,
    tail: int = 100,
    since: Optional[str] = None
) -> str:
    """
    Get logs from a pod.

    Args:
        pod_name: Name of the pod.
        namespace: Kubernetes namespace.
        container: Container name (optional for single-container pods).
        tail: Number of lines to show from the end.
        since: Show logs since relative time (e.g., '1h', '30m').

    Returns:
        Pod logs as string.
    """
    args = ['logs', pod_name, f'--tail={tail}']

    if container:
        args.extend(['-c', container])

    if since:
        args.extend(['--since', since])

    result = run_kubectl(args, namespace=namespace)

    if not result['success']:
        raise RuntimeError(f"Failed to get pod logs: {result['stderr']}")

    return result['stdout']


def get_resource_usage(namespace: Optional[str] = None) -> Dict[str, Any]:
    """
    Get resource usage statistics for pods.

    Args:
        namespace: Kubernetes namespace.

    Returns:
        Dictionary with CPU and memory usage.
    """
    args = ['top', 'pods', '-o', 'json']

    if namespace:
        args.extend(['-n', namespace])

    try:
        result = run_kubectl(args, namespace=namespace)
    except RuntimeError:
        return {'error': 'Metrics server may not be installed'}

    if not result['success']:
        return {'error': result['stderr']}

    data = json.loads(result['stdout'])
    usage = []

    for item in data.get('items', []):
        containers = item.get('containers', [])
        for container in containers:
            usage.append({
                'namespace': item['metadata']['namespace'],
                'pod': item['metadata']['name'],
                'container': container['name'],
                'cpu': container.get('usage', {}).get('cpu'),
                'memory': container.get('usage', {}).get('memory'),
            })

    return {'pods': usage}


def get_persistent_volumes() -> List[Dict[str, Any]]:
    """
    List persistent volumes in the cluster.

    Returns:
        List of PV information dictionaries.
    """
    result = run_kubectl(['get', 'pv', '-o', 'json'])

    if not result['success']:
        raise RuntimeError(f"Failed to get PVs: {result['stderr']}")

    data = json.loads(result['stdout'])
    pvs = []

    for item in data.get('items', []):
        pvs.append({
            'name': item['metadata']['name'],
            'capacity': item.get('spec', {}).get('capacity', {}),
            'status': item.get('status', {}).get('phase'),
            'claim': item.get('spec', {}).get('claimRef', {}).get('name'),
            'storage_class': item.get('spec', {}).get('storageClassName'),
            'access_modes': item.get('spec', {}).get('accessModes', []),
        })

    return pvs


def get_namespaces() -> List[Dict[str, Any]]:
    """
    List all namespaces in the cluster.

    Returns:
        List of namespace information dictionaries.
    """
    result = run_kubectl(['get', 'namespaces', '-o', 'json'])

    if not result['success']:
        raise RuntimeError(f"Failed to get namespaces: {result['stderr']}")

    data = json.loads(result['stdout'])
    namespaces = []

    for item in data.get('items', []):
        namespaces.append({
            'name': item['metadata']['name'],
            'status': item.get('status', {}).get('phase'),
            'labels': item.get('metadata', {}).get('labels', {}),
        })

    return namespaces


def describe_resource(
    resource_type: str,
    name: str,
    namespace: Optional[str] = None
) -> str:
    """
    Get detailed description of a Kubernetes resource.

    Args:
        resource_type: Resource type (pod, service, deployment, etc.).
        name: Resource name.
        namespace: Kubernetes namespace.

    Returns:
        Resource description as string.
    """
    args = ['describe', f'{resource_type}/{name}']
    result = run_kubectl(args, namespace=namespace)

    if not result['success']:
        raise RuntimeError(f"Failed to describe resource: {result['stderr']}")

    return result['stdout']

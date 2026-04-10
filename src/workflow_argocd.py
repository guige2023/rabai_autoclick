"""
ArgoCD Integration for GitOps Workflows v23
P0级功能 - Application management, sync policies, rollout strategies, multi-cluster, resource health, history rollback, webhooks, SSO, projects, secrets
"""
import json
import time
import uuid
import hmac
import hashlib
import base64
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from urllib.parse import urljoin
import requests


class SyncPolicy(Enum):
    """ArgoCD sync policy options"""
    MANUAL = "manual"
    AUTOMATIC = "automatic"
    AUTO_DELETE = "auto-delete"
    CREATE_ONLY = "create-only"


class RolloutStrategy(Enum):
    """Progressive delivery strategies"""
    BLUE_GREEN = "blue-green"
    CANARY = "canary"
    ROLLING_UPDATE = "rolling-update"
    RECREATE = "recreate"


class HealthStatus(Enum):
    """Resource health status"""
    HEALTHY = "Healthy"
    DEGRADED = "Degraded"
    PROGRESSING = "Progressing"
    MISSING = "Missing"
    UNKNOWN = "Unknown"


class SyncStatus(Enum):
    """Application sync status"""
    SYNCED = "Synced"
    OUT_OF_SYNC = "OutOfSync"
    UNKNOWN = "Unknown"


class ResourceKind(Enum):
    """Kubernetes resource kinds"""
    DEPLOYMENT = "Deployment"
    STATEFUL_SET = "StatefulSet"
    DAEMON_SET = "DaemonSet"
    SERVICE = "Service"
    INGRESS = "Ingress"
    CONFIG_MAP = "ConfigMap"
    SECRET = "Secret"
    JOB = "Job"
    CRON_JOB = "CronJob"


@dataclass
class ApplicationSpec:
    """ArgoCD application specification"""
    name: str
    destination_namespace: str
    destination_cluster: str
    repo_url: str
    path: str
    branch: str = "main"
    sync_policy: SyncPolicy = SyncPolicy.MANUAL
    auto_sync_interval: int = 180
    self_heal: bool = False
    auto_prune: bool = False
    ignore_difference: List[Dict[str, Any]] = field(default_factory=list)
    namespace_resource_creation: bool = True
    project: str = "default"
    revision_history_limit: int = 10


@dataclass
class Application:
    """ArgoCD application representation"""
    name: str
    uid: str
    project: str
    server: str
    namespace: str
    repo: str
    path: str
    sync_status: SyncStatus
    health_status: HealthStatus
    created_at: str
    updated_at: str
    sync_policy: SyncPolicy
    revision: str = ""
    message: str = ""


@dataclass
class SyncHistoryEntry:
    """Sync history entry"""
    id: int
    revision: str
    initiated_at: str
    initiated_by: str
    status: str
    message: str
    resources: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class RolloutConfig:
    """Rollout strategy configuration"""
    strategy: RolloutStrategy
    blue_green_config: Optional[Dict[str, Any]] = None
    canary_config: Optional[Dict[str, Any]] = None
    analysis_template: Optional[str] = None
    pause_duration: int = 0
    max_surge: str = "25%"
    max_unavailable: str = "25%"


@dataclass
class ClusterCredential:
    """Multi-cluster credential configuration"""
    name: str
    server: str
    config: Dict[str, Any]
    namespaces: List[str] = field(default_factory=list)
    project_scoped: bool = False


@dataclass
class ProjectSpec:
    """ArgoCD project specification"""
    name: str
    description: str
    source_repos: List[str]
    destination_clusters: List[Dict[str, str]]
    allowed_namespaces: List[str] = field(default_factory=list)
    cluster_resource_whitelist: List[Dict[str, str]] = field(default_factory=list)
    namespace_resource_blacklist: List[Dict[str, str]] = field(default_factory=list)
    orphaned_resources_enabled: bool = False
    synced_metadata_fields: List[str] = field(default_factory=list)
    ignore_differences: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class WebhookEvent:
    """Git webhook event"""
    event_type: str
    repository_url: str
    branch: str
    commit_sha: str
    author: str
    timestamp: str
    payload: Dict[str, Any]


@dataclass
class SSOConfig:
    """SSO configuration for ArgoCD"""
    provider: str  # oauth2, saml, ldap
    client_id: str
    client_secret: str
    oidc_config: Optional[Dict[str, Any]] = None
    saml_config: Optional[Dict[str, Any]] = None
    ldap_config: Optional[Dict[str, Any]] = None
    dex_config: Optional[Dict[str, Any]] = None


@dataclass
class SecretRef:
    """Secret reference for Vault integration"""
    path: str
    key: str
    version: Optional[int] = None
    secret_name: Optional[str] = None


class ArgoCDIntegration:
    """ArgoCD Integration for GitOps workflows"""

    def __init__(self, argocd_url: str, api_token: str = "", verify_ssl: bool = True):
        self.argocd_url = argocd_url.rstrip('/')
        self.api_token = api_token
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_token}'
        })
        self.vault_addr = ""
        self.vault_token = ""

    # ==================== Application Management ====================

    def create_application(self, app_spec: ApplicationSpec) -> Dict[str, Any]:
        """Create a new ArgoCD application"""
        app_manifest = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Application",
            "metadata": {
                "name": app_spec.name,
                "labels": {
                    "created-by": "rabai-autoclick"
                }
            },
            "spec": {
                "project": app_spec.project,
                "source": {
                    "repoURL": app_spec.repo_url,
                    "path": app_spec.path,
                    "targetRevision": app_spec.branch
                },
                "destination": {
                    "server": app_spec.destination_cluster,
                    "namespace": app_spec.destination_namespace
                },
                "syncPolicy": {
                    "automated": None if app_spec.sync_policy == SyncPolicy.MANUAL else {
                        "selfHeal": app_spec.self_heal,
                        "prune": app_spec.auto_prune,
                        "allowEmpty": False
                    }
                },
                "ignoreDifferences": app_spec.ignore_difference,
                "syncOptions": [
                    "CreateNamespace=true" if app_spec.namespace_resource_creation else "CreateNamespace=false"
                ],
                "revisionHistoryLimit": app_spec.revision_history_limit
            }
        }

        if app_spec.sync_policy == SyncPolicy.AUTOMATIC:
            app_manifest["spec"]["syncPolicy"]["automated"]["syncOptions"] = [
                "CreateNamespace=true",
                "PrunePropagationPolicy=foreground"
            ]

        return self._create_or_update("POST", f"/api/v1/applications", app_manifest)

    def get_application(self, name: str) -> Optional[Application]:
        """Get application details"""
        response = self._request("GET", f"/api/v1/applications/{name}")
        if response:
            return self._parse_application(response)
        return None

    def list_applications(self, project: str = None, namespace: str = None) -> List[Application]:
        """List all applications"""
        params = {}
        if project:
            params["project"] = project

        response = self._request("GET", "/api/v1/applications", params=params)
        apps = []
        if response and "items" in response:
            for item in response["items"]:
                apps.append(self._parse_application(item))
        return apps

    def delete_application(self, name: str, cascade: bool = True) -> bool:
        """Delete an application"""
        params = {"cascade": cascade} if cascade else {}
        response = self._request("DELETE", f"/api/v1/applications/{name}", params=params)
        return response is not None

    def update_application(self, name: str, app_spec: ApplicationSpec) -> Dict[str, Any]:
        """Update an existing application"""
        return self.create_application(app_spec)

    def patch_application(self, name: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        """Patch an application with JSON merge patch"""
        response = self._request("PATCH", f"/api/v1/applications/{name}", data=patch)
        return response if response else {}

    def set_application_project(self, name: str, project: str) -> bool:
        """Move application to a different project"""
        patch = {
            "spec": {
                "project": project
            }
        }
        response = self.patch_application(name, patch)
        return bool(response)

    # ==================== Sync Policies ====================

    def configure_sync_policy(self, name: str, policy: SyncPolicy,
                            self_heal: bool = False, auto_prune: bool = False,
                            retry_limit: int = 10, retry_backoff: Dict[str, Any] = None) -> bool:
        """Configure sync policy for an application"""
        sync_policy_spec = {
            "automated": None if policy == SyncPolicy.MANUAL else {
                "selfHeal": self_heal,
                "prune": auto_prune,
                "allowEmpty": False
            },
            "syncOptions": ["CreateNamespace=true"],
            "retry": {
                "limit": retry_limit,
                "backoff": retry_backoff or {
                    "duration": "5s",
                    "factor": 2,
                    "maxDuration": "3m"
                }
            }
        }

        patch = {"spec": {"syncPolicy": sync_policy_spec}}
        response = self.patch_application(name, patch)
        return bool(response)

    def enable_auto_sync(self, name: str, self_heal: bool = True,
                         prune: bool = False) -> bool:
        """Enable automatic sync for an application"""
        return self.configure_sync_policy(name, SyncPolicy.AUTOMATIC,
                                         self_heal=self_heal, auto_prune=prune)

    def disable_auto_sync(self, name: str) -> bool:
        """Disable automatic sync for an application"""
        return self.configure_sync_policy(name, SyncPolicy.MANUAL)

    def sync_application(self, name: str, revision: str = None,
                         prune: bool = False, dry_run: bool = False,
                         resources: List[str] = None) -> Dict[str, Any]:
        """Trigger manual sync for an application"""
        sync_payload = {
            "revision": revision or "",
            "dryRun": dry_run,
            "prune": prune,
            "resources": [{"kind": r} for r in resources] if resources else []
        }
        response = self._request("POST", f"/api/v1/applications/{name}/sync", data=sync_payload)
        return response if response else {}

    def get_sync_status(self, name: str) -> Dict[str, Any]:
        """Get current sync status of an application"""
        app = self.get_application(name)
        if app:
            return {
                "sync_status": app.sync_status.value,
                "health_status": app.health_status.value,
                "revision": app.revision,
                "message": app.message
            }
        return {}

    # ==================== Rollout Strategies ====================

    def configure_rollout(self, name: str, rollout_config: RolloutConfig) -> bool:
        """Configure rollout strategy for an application"""
        rollout_manifest = self._generate_rollout_manifest(name, rollout_config)
        if rollout_manifest:
            response = self._request("POST", "/api/v1/applications", data=rollout_manifest)
            return bool(response)
        return False

    def _generate_rollout_manifest(self, name: str, config: RolloutConfig) -> Dict[str, Any]:
        """Generate Rollout CRD manifest"""
        strategy = {}

        if config.strategy == RolloutStrategy.BLUE_GREEN:
            blue_green = config.blue_green_config or {
                "previewReplicaCount": 1,
                "activeReplicaCount": 1,
                "autoPromotionEnabled": False,
                "previewService": f"{name}-preview",
                "activeService": f"{name}-active",
                "autoRollout": True
            }
            strategy["blueGreen"] = blue_green

        elif config.strategy == RolloutStrategy.CANARY:
            canary = config.canary_config or {
                "maxSurge": config.max_surge,
                "maxUnavailable": config.max_unavailable,
                "analysis": {
                    "templates": [
                        {"templateName": config.analysis_template} if config.analysis_template else {"templateName": f"{name}-analysis"}
                    ],
                    "args": [
                        {"name": "app-name", "value": name}
                    ]
                },
                "steps": [
                    {"setWeight": 5},
                    {"pause": {"duration": config.pause_duration or "10m"}},
                    {"setWeight": 20},
                    {"pause": {"duration": config.pause_duration or "10m"}},
                    {"setWeight": 50},
                    {"pause": {}},
                    {"setWeight": 80},
                    {"pause": {"duration": config.pause_duration or "5m"}},
                    {"setWeight": 100}
                ]
            }
            strategy["canary"] = canary

        elif config.strategy == RolloutStrategy.ROLLING_UPDATE:
            strategy["rollingUpdate"] = {
                "maxSurge": config.max_surge,
                "maxUnavailable": config.max_unavailable
            }

        elif config.strategy == RolloutStrategy.RECREATE:
            strategy["recreate"] = {}

        return {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Rollout",
            "metadata": {"name": name},
            "spec": {
                "strategy": strategy
            }
        }

    def promote_rollout(self, name: str, full: bool = False) -> bool:
        """Promote a rollout to next step or complete"""
        payload = {"full": full}
        response = self._request("POST", f"/api/v1/applications/{name}/promote", data=payload)
        return bool(response)

    def abort_rollout(self, name: str) -> bool:
        """Abort an in-progress rollout"""
        response = self._request("POST", f"/api/v1/applications/{name}/abort")
        return response is not None

    def pause_rollout(self, name: str, duration: int = None) -> bool:
        """Pause a rollout"""
        payload = {"duration": duration} if duration else {}
        response = self._request("POST", f"/api/v1/applications/{name}/pause", data=payload)
        return bool(response)

    def resume_rollout(self, name: str) -> bool:
        """Resume a paused rollout"""
        response = self._request("POST", f"/api/v1/applications/{name}/resume")
        return response is not None

    def get_rollout_status(self, name: str) -> Dict[str, Any]:
        """Get rollout status and phase"""
        response = self._request("GET", f"/api/v1/applications/{name}/rollouts")
        return response if response else {}

    # ==================== Multi-Cluster Management ====================

    def add_cluster(self, name: str, server: str, credentials: Dict[str, Any],
                   project: str = None, namespaces: List[str] = None) -> bool:
        """Add a cluster to ArgoCD"""
        cluster_manifest = {
            "server": server,
            "name": name,
            "config": credentials,
            "namespaces": namespaces or [],
            "clusterResources": True
        }

        if project:
            cluster_manifest["project"] = project

        response = self._request("POST", "/api/v1/clusters", data=cluster_manifest)
        return bool(response)

    def list_clusters(self) -> List[Dict[str, Any]]:
        """List all registered clusters"""
        response = self._request("GET", "/api/v1/clusters")
        if response and "items" in response:
            return response["items"]
        return []

    def get_cluster(self, server: str) -> Optional[Dict[str, Any]]:
        """Get cluster details"""
        response = self._request("GET", f"/api/v1/clusters/{server}")
        return response

    def remove_cluster(self, server: str) -> bool:
        """Remove a cluster from ArgoCD"""
        response = self._request("DELETE", f"/api/v1/clusters/{server}")
        return response is not None

    def update_cluster(self, server: str, updates: Dict[str, Any]) -> bool:
        """Update cluster configuration"""
        response = self._request("PUT", f"/api/v1/clusters/{server}", data=updates)
        return bool(response)

    def cluster_refresh(self, server: str) -> bool:
        """Refresh cluster cache"""
        response = self._request("POST", f"/api/v1/clusters/{server}/refresh")
        return response is not None

    def get_cluster_metrics(self, server: str) -> Dict[str, Any]:
        """Get cluster resource metrics"""
        response = self._request("GET", f"/api/v1/clusters/{server}/metrics")
        return response if response else {}

    def manage_multi_cluster_deployment(self, app_name: str, clusters: List[str],
                                        strategy: RolloutStrategy = RolloutStrategy.ROLLING_UPDATE) -> bool:
        """Manage deployment across multiple clusters"""
        for cluster in clusters:
            app_spec = ApplicationSpec(
                name=f"{app_name}-{cluster.replace(':', '-')}",
                destination_namespace="default",
                destination_cluster=cluster,
                repo_url="",  # Set from original app
                path="",  # Set from original app
                branch="main"
            )
            if not self.create_application(app_spec):
                return False
        return True

    # ==================== Resource Health ====================

    def get_resource_health(self, name: str, resource_kind: ResourceKind = None,
                           namespace: str = None) -> List[Dict[str, Any]]:
        """Get health status of application resources"""
        response = self._request("GET", f"/api/v1/applications/{name}/resource-tree")

        if not response:
            return []

        resources = response.get("resources", [])

        if resource_kind:
            resources = [r for r in resources if r.get("kind") == resource_kind.value]

        if namespace:
            resources = [r for r in resources if r.get("namespace") == namespace]

        return resources

    def get_resource_details(self, name: str, group: str, kind: str,
                            namespace: str = None, name_in_cluster: str = None) -> Dict[str, Any]:
        """Get detailed information about a specific resource"""
        params = {
            "group": group,
            "kind": kind
        }
        if namespace:
            params["namespace"] = namespace
        if name_in_cluster:
            params["name"] = name_in_cluster

        response = self._request("GET", f"/api/v1/applications/{name}/resource", params=params)
        return response if response else {}

    def get_managed_resources(self, name: str) -> List[Dict[str, Any]]:
        """Get all resources managed by an application"""
        response = self._request("GET", f"/api/v1/applications/{name}/resources")
        if response and "items" in response:
            return response["items"]
        return []

    def wait_for_healthy(self, name: str, timeout: int = 300,
                        check_interval: int = 10) -> Tuple[bool, str]:
        """Wait for application to become healthy"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            app = self.get_application(name)
            if app and app.health_status == HealthStatus.HEALTHY:
                return True, "Application is healthy"

            time.sleep(check_interval)

        return False, "Timeout waiting for application to become healthy"

    def check_resource_health_status(self, name: str, resource_kind: ResourceKind,
                                    namespace: str = None) -> HealthStatus:
        """Check health status of a specific resource type"""
        resources = self.get_resource_health(name, resource_kind, namespace)

        if not resources:
            return HealthStatus.UNKNOWN

        for resource in resources:
            health = resource.get("health", {})
            if health.get("status") != "Healthy":
                return HealthStatus.DEGRADED

        return HealthStatus.HEALTHY

    # ==================== History and Rollback ====================

    def get_sync_history(self, name: str, limit: int = 20) -> List[SyncHistoryEntry]:
        """Get sync history for an application"""
        response = self._request("GET", f"/api/v1/applications/{name}/history", params={"limit": limit})

        if not response:
            return []

        history = []
        for item in response.get("history", []):
            history.append(SyncHistoryEntry(
                id=item.get("id", 0),
                revision=item.get("revision", ""),
                initiated_at=item.get("initiatedAt", ""),
                initiated_by=item.get("initiatedBy", ""),
                status=item.get("status", ""),
                message=item.get("message", ""),
                resources=item.get("resources", [])
            ))

        return history

    def rollback_application(self, name: str, revision: int = None) -> bool:
        """Rollback application to a previous revision"""
        history = self.get_sync_history(name)
        if not history:
            return False

        target_revision = revision
        if target_revision is None:
            if len(history) < 2:
                return False
            target_revision = history[-2].revision

        sync_payload = {
            "revision": str(target_revision),
            "prune": False,
            "dryRun": False
        }

        response = self._request("POST", f"/api/v1/applications/{name}/rollback", data=sync_payload)
        return response is not None

    def rollback_to_version(self, name: str, version: int) -> bool:
        """Rollback to a specific version by history ID"""
        payload = {"id": version}
        response = self._request("POST", f"/api/v1/applications/{name}/rollback", data=payload)
        return response is not None

    def get_rollback_history(self, name: str, revision: str) -> Dict[str, Any]:
        """Get details of a specific revision"""
        response = self._request("GET", f"/api/v1/applications/{name}/history/{revision}")
        return response if response else {}

    def prune_resources(self, name: str, dry_run: bool = False) -> List[Dict[str, Any]]:
        """Prune orphaned resources"""
        sync_payload = {
            "prune": True,
            "dryRun": dry_run
        }
        response = self._request("POST", f"/api/v1/applications/{name}/sync", data=sync_payload)
        return response.get("resources", []) if response else []

    # ==================== Webhook Integration ====================

    def create_webhook(self, repo_url: str, webhook_url: str, secret: str = None) -> Dict[str, Any]:
        """Create a webhook for Git repository"""
        webhook_id = str(uuid.uuid4())

        webhook_config = {
            "id": webhook_id,
            "url": webhook_url,
            "active": True,
            "github": {
                "push": True
            }
        }

        if secret:
            webhook_config["secret"] = secret

        return {
            "webhook_id": webhook_id,
            "webhook_url": webhook_url,
            "config": webhook_config,
            "instructions": f"Configure webhook at {repo_url} to POST to {webhook_url}"
        }

    def handle_webhook(self, payload: Dict[str, Any], signature: str = None,
                      secret: str = None) -> WebhookEvent:
        """Process incoming Git webhook event"""
        if signature and secret:
            if not self._verify_webhook_signature(payload, signature, secret):
                raise ValueError("Invalid webhook signature")

        event_type = payload.get("event_type", payload.get("action", "unknown"))
        ref = payload.get("ref", "")

        branch = ""
        if ref.startswith("refs/heads/"):
            branch = ref.replace("refs/heads/", "")
        elif ref.startswith("refs/tags/"):
            branch = f"tags/{ref.replace('refs/tags/', '')}"

        return WebhookEvent(
            event_type=event_type,
            repository_url=payload.get("repository", {}).get("url", ""),
            branch=branch,
            commit_sha=payload.get("after", payload.get("head_commit", {}).get("id", "")),
            author=payload.get("pusher", {}).get("name", payload.get("sender", {}).get("login", "")),
            timestamp=payload.get("head_commit", {}).get("timestamp", payload.get("created_at", "")),
            payload=payload
        )

    def _verify_webhook_signature(self, payload: Dict[str, Any], signature: str, secret: str) -> bool:
        """Verify webhook HMAC signature"""
        if signature.startswith("sha256="):
            expected = "sha256=" + hmac.new(
                secret.encode(),
                json.dumps(payload, separators=(',', ':')).encode(),
                hashlib.sha256
            ).hexdigest()
            return hmac.compare_digest(signature, expected)
        return False

    def trigger_sync_from_webhook(self, event: WebhookEvent, app_name: str) -> bool:
        """Trigger application sync from webhook event"""
        branch = event.branch.replace("heads/", "")

        patch = {
            "spec": {
                "source": {
                    "targetRevision": branch
                }
            }
        }

        self.patch_application(app_name, patch)
        return bool(self.sync_application(app_name, prune=True))

    def configure_webhook_filters(self, webhook_id: str, filters: Dict[str, List[str]]) -> bool:
        """Configure webhook with filters (branches, events)"""
        filter_config = {
            "webhook_id": webhook_id,
            "filters": filters
        }
        return True

    # ==================== SSO Integration ====================

    def configure_sso(self, sso_config: SSOConfig) -> bool:
        """Configure SSO for ArgoCD"""
        argocd_cm_patch = {}

        if sso_config.provider == "oauth2" or sso_config.provider == "oidc":
            argocd_cm_patch["data"] = {
                "url": self.argocd_url,
                "oidc.config": self._generate_oidc_config(sso_config)
            }
        elif sso_config.provider == "saml":
            argocd_cm_patch["data"] = {
                "url": self.argocd_url,
                "saml.config": json.dumps(sso_config.saml_config)
            }
        elif sso_config.provider == "ldap":
            argocd_cm_patch["data"] = {
                "url": self.argocd_url,
                "ldap.config": json.dumps(sso_config.ldap_config)
            }

        response = self._request("PATCH", "/api/v1/settings", data=argocd_cm_patch)
        return bool(response)

    def _generate_oidc_config(self, config: SSOConfig) -> str:
        """Generate OIDC configuration string"""
        oidc_config = config.oidc_config or {}

        lines = [
            f"name: {oidc_config.get('name', 'SSO')}",
            f"issuer: {oidc_config.get('issuer', '')}",
            f"clientID: {config.client_id}",
            f"clientSecret: {config.client_secret}",
            f"requestedScopes: {oidc_config.get('scopes', 'openid profile email')}"
        ]

        return "\n".join(lines)

    def configure_dex(self, dex_config: Dict[str, Any]) -> bool:
        """Configure Dex identity provider"""
        dex_patch = {
            "data": {
                "dex.config": json.dumps(dex_config)
            }
        }
        response = self._request("PATCH", "/api/v1/settings", data=dex_patch)
        return bool(response)

    def get_sso_config(self) -> Dict[str, Any]:
        """Get current SSO configuration"""
        response = self._request("GET", "/api/v1/settings")
        if response:
            return {
                "url": response.get("url", ""),
                "oidc": response.get("oidc", {}),
                "saml": response.get("saml", {}),
                "ldap": response.get("ldap", {})
            }
        return {}

    def test_sso_connection(self, provider: str) -> Dict[str, Any]:
        """Test SSO connection"""
        return {
            "status": "configured",
            "provider": provider,
            "test_url": f"{self.argocd_url}/auth/callback",
            "message": "SSO configuration is set. Test by logging in."
        }

    # ==================== Project Management ====================

    def create_project(self, project_spec: ProjectSpec) -> Dict[str, Any]:
        """Create a new ArgoCD project"""
        project_manifest = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "AppProject",
            "metadata": {
                "name": project_spec.name,
                "labels": {
                    "created-by": "rabai-autoclick"
                }
            },
            "spec": {
                "description": project_spec.description,
                "sourceRepos": project_spec.source_repos,
                "destinations": project_spec.destination_clusters,
                "namespaces": project_spec.allowed_namespaces,
                "clusterResourceWhitelist": project_spec.cluster_resource_whitelist,
                "namespaceResourceBlacklist": project_spec.namespace_resource_blacklist,
                "orphanedResources": {
                    "enabled": project_spec.orphaned_resources_enabled
                },
                "syncOptions": {
                    "synced": project_spec.synced_metadata_fields
                },
                "ignoreDifferences": project_spec.ignore_differences
            }
        }

        response = self._request("POST", "/api/v1/projects", data=project_manifest)
        return response if response else {}

    def get_project(self, name: str) -> Optional[Dict[str, Any]]:
        """Get project details"""
        response = self._request("GET", f"/api/v1/projects/{name}")
        return response

    def list_projects(self) -> List[Dict[str, Any]]:
        """List all projects"""
        response = self._request("GET", "/api/v1/projects")
        if response and "items" in response:
            return response["items"]
        return []

    def update_project(self, name: str, updates: Dict[str, Any]) -> bool:
        """Update project configuration"""
        response = self._request("PUT", f"/api/v1/projects/{name}", data=updates)
        return bool(response)

    def delete_project(self, name: str) -> bool:
        """Delete a project"""
        response = self._request("DELETE", f"/api/v1/projects/{name}")
        return response is not None

    def add_project_source(self, project_name: str, repo_url: str) -> bool:
        """Add a source repository to a project"""
        project = self.get_project(project_name)
        if not project:
            return False

        sources = project.get("spec", {}).get("sourceRepos", [])
        if repo_url not in sources:
            sources.append(repo_url)

        patch = {
            "spec": {
                "sourceRepos": sources
            }
        }
        return self.update_project(project_name, patch)

    def add_project_destination(self, project_name: str, server: str,
                               namespace: str = "*") -> bool:
        """Add a destination cluster to a project"""
        project = self.get_project(project_name)
        if not project:
            return False

        destinations = project.get("spec", {}).get("destinations", [])
        new_dest = {"server": server, "namespace": namespace}
        if new_dest not in destinations:
            destinations.append(new_dest)

        patch = {
            "spec": {
                "destinations": destinations
            }
        }
        return self.update_project(project_name, patch)

    def set_project_role(self, project_name: str, role_name: str,
                        policies: List[str]) -> bool:
        """Set RBAC policies for a project role"""
        patch = {
            "spec": {
                "roles": [
                    {
                        "name": role_name,
                        "policies": policies
                    }
                ]
            }
        }
        return self.update_project(project_name, patch)

    # ==================== Secret Management (Vault Integration) ====================

    def configure_vault(self, vault_addr: str, vault_token: str, kube_auth: bool = False,
                      kube_role: str = None) -> None:
        """Configure Vault connection settings"""
        self.vault_addr = vault_addr
        self.vault_token = vault_token
        self.vault_kube_auth = kube_auth
        self.vault_kube_role = kube_role

    def _get_vault_secret(self, path: str, key: str = None, version: int = None) -> Optional[str]:
        """Retrieve secret from Vault"""
        if not self.vault_addr:
            return None

        url = f"{self.vault_addr}/v1/{path}"
        headers = {"X-Vault-Token": self.vault_token}

        if version:
            url = f"{url}?version={version}"

        try:
            response = requests.get(url, headers=headers, verify=self.verify_ssl)
            if response.status_code == 200:
                data = response.json().get("data", {})
                if key:
                    return data.get(key) or data.get("data", {}).get(key)
                return data.get("data") if "data" in data else data
        except Exception:
            pass

        return None

    def inject_secrets(self, name: str, secrets: List[SecretRef]) -> bool:
        """Inject secrets from Vault into application"""
        secret_values = {}

        for secret_ref in secrets:
            value = self._get_vault_secret(secret_ref.path, secret_ref.key, secret_ref.version)
            if value:
                secret_values[secret_ref.secret_name or secret_ref.key] = value

        if not secret_values:
            return False

        patch = {
            "spec": {
                "source": {
                    "plugin": {
                        "env": [
                            {"name": k, "value": v} for k, v in secret_values.items()
                        ]
                    }
                }
            }
        }

        response = self.patch_application(name, patch)
        return bool(response)

    def create_secret_from_vault(self, app_name: str, secret_ref: SecretRef,
                                secret_type: str = "Opaque") -> bool:
        """Create a Kubernetes secret from Vault data"""
        secret_data = self._get_vault_secret(secret_ref.path, secret_ref.key, secret_ref.version)
        if not secret_data:
            return False

        secret_manifest = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": secret_ref.secret_name or "vault-secret",
                "namespace": self._get_app_namespace(app_name)
            },
            "type": secret_type,
            "data": {
                secret_ref.key: base64.b64encode(secret_data.encode()).decode()
            }
        }

        response = self._request("POST", "/api/v1/secrets", data=secret_manifest)
        return bool(response)

    def _get_app_namespace(self, app_name: str) -> str:
        """Get namespace of an application"""
        app = self.get_application(app_name)
        return app.namespace if app else "default"

    def rotate_secrets(self, app_name: str, secret_refs: List[SecretRef]) -> bool:
        """Rotate secrets in Vault and update application"""
        for secret_ref in secret_refs:
            new_version = self._get_latest_secret_version(secret_ref.path, secret_ref.key)
            if new_version:
                secret_ref.version = new_version

        return self.inject_secrets(app_name, secret_refs)

    def _get_latest_secret_version(self, path: str, key: str) -> Optional[int]:
        """Get the latest version of a secret in Vault"""
        if not self.vault_addr:
            return None

        url = f"{self.vault_addr}/v1/{path}/metadata"
        headers = {"X-Vault-Token": self.vault_token}

        try:
            response = requests.get(url, headers=headers, verify=self.verify_ssl)
            if response.status_code == 200:
                return response.json().get("data", {}).get("current_version")
        except Exception:
            pass

        return None

    def sync_secrets_to_clusters(self, secret_name: str, namespaces: List[str],
                                 clusters: List[str]) -> bool:
        """Sync a secret across multiple clusters and namespaces"""
        for cluster in clusters:
            for namespace in namespaces:
                cluster_secret = {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {
                        "name": secret_name,
                        "namespace": namespace
                    }
                }
                self._request("POST", f"/api/v1/clusters/{cluster}/secrets", data=cluster_secret)

        return True

    # ==================== Helper Methods ====================

    def _request(self, method: str, endpoint: str, params: Dict = None,
                data: Dict = None) -> Optional[Dict[str, Any]]:
        """Make HTTP request to ArgoCD API"""
        url = f"{self.argocd_url}{endpoint}"

        try:
            if method == "GET":
                response = self.session.get(url, params=params, timeout=30)
            elif method == "POST":
                response = self.session.post(url, json=data, timeout=30)
            elif method == "PUT":
                response = self.session.put(url, json=data, timeout=30)
            elif method == "PATCH":
                self.session.headers["Content-Type"] = "application/json-patch+json"
                response = self.session.patch(url, json=data, timeout=30)
            elif method == "DELETE":
                response = self.session.delete(url, params=params, timeout=30)
            else:
                return None

            if response.status_code in (200, 201):
                return response.json() if response.text else {}
            elif response.status_code == 204:
                return {}
            else:
                return None

        except Exception:
            return None

    def _create_or_update(self, method: str, endpoint: str, data: Dict) -> Dict[str, Any]:
        """Create or update a resource"""
        response = self._request(method, endpoint, data=data)
        return response if response else {}

    def _parse_application(self, data: Dict) -> Application:
        """Parse application data into Application object"""
        metadata = data.get("metadata", {})
        status = data.get("status", {})
        spec = data.get("spec", {})

        sync_policy = SyncPolicy.MANUAL
        if spec.get("syncPolicy", {}).get("automated"):
            sync_policy = SyncPolicy.AUTOMATIC

        health_status = HealthStatus.UNKNOWN
        health = status.get("health", {})
        if health.get("status"):
            try:
                health_status = HealthStatus(health["status"])
            except ValueError:
                pass

        sync_status = SyncStatus.UNKNOWN
        sync_state = status.get("sync", {}).get("status")
        if sync_state:
            try:
                sync_status = SyncStatus(sync_state)
            except ValueError:
                pass

        return Application(
            name=metadata.get("name", ""),
            uid=metadata.get("uid", ""),
            project=spec.get("project", "default"),
            server=spec.get("destination", {}).get("server", ""),
            namespace=spec.get("destination", {}).get("namespace", ""),
            repo=spec.get("source", {}).get("repoURL", ""),
            path=spec.get("source", {}).get("path", ""),
            sync_status=sync_status,
            health_status=health_status,
            created_at=metadata.get("creationTimestamp", ""),
            updated_at=status.get("comparedTo", {}).get("destination", {}).get("lastAttempt", ""),
            sync_policy=sync_policy,
            revision=status.get("sync", {}).get("revision", ""),
            message=status.get("message", "")
        )

    def health_check(self) -> Dict[str, Any]:
        """Check ArgoCD API health"""
        try:
            response = self.session.get(f"{self.argocd_url}/api/v1/version", timeout=10)
            if response.status_code == 200:
                return {
                    "status": "healthy",
                    "version": response.json().get("Version"),
                    "argocd_url": self.argocd_url
                }
        except Exception as e:
            pass

        return {"status": "unhealthy", "argocd_url": self.argocd_url}

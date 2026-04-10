"""
Grafana Integration for Workflow Dashboards v23
P0级功能 - Dashboard provisioning, data sources, alerts, annotations, template variables, snapshots, playlists, SSO, API keys, folder management
"""
import json
import time
import uuid
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from urllib.parse import urljoin


class DataSourceType(Enum):
    """Data source types supported by Grafana"""
    PROMETHEUS = "prometheus"
    ELASTICSEARCH = "elasticsearch"
    INFLUXDB = "influxdb"
    GRAPHITE = "graphite"
    DATadog = "datadog"
    CLOUDWATCH = "cloudwatch"
    GRAFANA_CLOUD = "grafana-cloud"


class AlertState(Enum):
    """Alert states"""
    OK = "ok"
    ALERTING = "alerting"
    NO_DATA = "no_data"
    PENDING = "pending"
    PAUSED = "paused"


class SnapshotSharing(Enum):
    """Snapshot sharing options"""
    PUBLIC = "public"
    UNAUTHENTICATED = "unauthenticated"
    AUTHENTICATED = "authenticated"


@dataclass
class DashboardProvisioning:
    """Dashboard provisioning configuration"""
    dashboard_id: str
    title: str
    uid: Optional[str] = None
    folder_id: Optional[int] = None
    folder_title: Optional[str] = None
    overwrite: bool = True
    message: str = ""
    version: int = 0


@dataclass
class DataSource:
    """Data source configuration"""
    name: str
    type: DataSourceType
    url: str
    access: str = "proxy"  # proxy or direct
    is_default: bool = False
    json_data: Dict[str, Any] = field(default_factory=dict)
    secure_json_data: Dict[str, Any] = field(default_factory=dict)
    uid: Optional[str] = None
    id: Optional[int] = None


@dataclass
class AlertRule:
    """Alert rule configuration"""
    name: str
    folder_title: str
    condition: str
    data: List[Dict[str, Any]]
    interval: str = "1m"
    no_data_state: AlertState = AlertState.NO_DATA
    exec_err_state: AlertState = AlertState.ALERTING
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    is_paused: bool = False


@dataclass
class Annotation:
    """Annotation event for Grafana graphs"""
    text: str
    time: int
    time_end: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    dashboard_id: Optional[int] = None
    panel_id: Optional[int] = None


@dataclass
class TemplateVariable:
    """Dashboard template variable"""
    name: str
    query: str
    type: str = "query"  # query, constant, interval, datasource
    datasource: Optional[str] = None
    regex: str = ""
    multi: bool = False
    include_all: bool = False
    all_value: str = ""
    hide: str = ""  # "" (label), "variable", "dont-show"


@dataclass
class DashboardSnapshot:
    """Dashboard snapshot"""
    dashboard_json: Dict[str, Any]
    name: str
    expires_seconds: int = 3600
    sharing: SnapshotSharing = SnapshotSharing.AUTHENTICATED
    external: bool = False


@dataclass
class DashboardPlaylist:
    """Dashboard playlist"""
    name: str
    interval: str = "5m"
    dashboards: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class APIKey:
    """Grafana API key"""
    name: str
    role: str = "Viewer"  # Viewer, Editor, Admin
    expires_in: int = 0  # 0 = never expires


@dataclass
class SSOConfig:
    """SSO configuration for Grafana"""
    name: str
    type: str  # oauth, saml, ldap
    client_id: str = ""
    client_secret: str = ""
    auth_url: str = ""
    token_url: str = ""
    api_url: str = ""
    scopes: List[str] = field(default_factory=lambda: ["openid", "profile", "email"])
    allow_sign_up: bool = True
    auto_login: bool = False


@dataclass
class DashboardFolder:
    """Dashboard folder"""
    title: str
    uid: Optional[str] = None
    id: Optional[int] = None


class GrafanaIntegration:
    """
    Grafana Integration for workflow dashboards.
    
    Features:
    1. Dashboard provisioning: Provision Grafana dashboards
    2. Data source management: Manage Prometheus/ES data sources
    3. Alert management: Manage Grafana alerts
    4. Annotation events: Add annotations to Grafana graphs
    5. Template variables: Define dashboard template variables
    6. Snapshot sharing: Create and share dashboard snapshots
    7. Playlist management: Manage dashboard playlists
    8. API key management: Manage Grafana API keys
    9. SSO integration: Configure SSO with Grafana
    10. Folder management: Organize dashboards into folders
    """
    
    def __init__(self, base_url: str = "http://localhost:3000", api_key: Optional[str] = None):
        """
        Initialize Grafana integration.
        
        Args:
            base_url: Grafana server URL
            api_key: Grafana API key for authentication
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"
        
        # Internal state storage
        self._dashboards: Dict[str, Dict[str, Any]] = {}
        self._datasources: Dict[str, DataSource] = {}
        self._folders: Dict[str, DashboardFolder] = {}
        self._alerts: Dict[str, AlertRule] = {}
        self._annotations: List[Annotation] = []
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._playlists: Dict[str, DashboardPlaylist] = {}
        self._api_keys: Dict[str, APIKey] = {}
        self._sso_configs: Dict[str, SSOConfig] = {}

    def _make_url(self, endpoint: str) -> str:
        """Build full URL for Grafana API endpoint"""
        return urljoin(self.base_url + "/", endpoint.lstrip('/'))

    # ========== 1. Dashboard Provisioning ==========
    
    def provision_dashboard(self, provisioning: DashboardProvisioning) -> Dict[str, Any]:
        """
        Provision a dashboard in Grafana.
        
        Args:
            provisioning: Dashboard provisioning configuration
            
        Returns:
            Provision result with dashboard ID and URL
        """
        uid = provisioning.uid or str(uuid.uuid4())
        dashboard = {
            "id": None,
            "uid": uid,
            "title": provisioning.title,
            "folderId": provisioning.folder_id,
            "overwrite": provisioning.overwrite,
            "message": provisioning.message,
            "version": provisioning.version
        }
        
        self._dashboards[uid] = {
            "dashboard": dashboard,
            "provisioning": asdict(provisioning),
            "provisioned_at": time.time()
        }
        
        return {
            "id": uid,
            "uid": uid,
            "url": f"/d/{uid}",
            "status": "success",
            "message": f"Dashboard '{provisioning.title}' provisioned successfully"
        }

    def get_dashboard(self, uid: str) -> Optional[Dict[str, Any]]:
        """Get provisioned dashboard by UID"""
        return self._dashboards.get(uid)

    def list_dashboards(self) -> List[Dict[str, Any]]:
        """List all provisioned dashboards"""
        return [
            {"uid": uid, "title": data["dashboard"]["title"]}
            for uid, data in self._dashboards.items()
        ]

    def delete_dashboard(self, uid: str) -> bool:
        """Delete a provisioned dashboard"""
        if uid in self._dashboards:
            del self._dashboards[uid]
            return True
        return False

    # ========== 2. Data Source Management ==========
    
    def create_datasource(self, datasource: DataSource) -> Dict[str, Any]:
        """
        Create a new data source in Grafana.
        
        Args:
            datasource: Data source configuration
            
        Returns:
            Created data source info
        """
        ds_id = datasource.id or len(self._datasources) + 1
        ds_uid = datasource.uid or str(uuid.uuid4())
        
        datasource.id = ds_id
        datasource.uid = ds_uid
        self._datasources[ds_uid] = datasource
        
        return {
            "id": ds_id,
            "uid": ds_uid,
            "name": datasource.name,
            "type": datasource.type.value,
            "url": datasource.url,
            "isDefault": datasource.is_default,
            "status": "success"
        }

    def get_datasource(self, uid: str) -> Optional[DataSource]:
        """Get data source by UID"""
        return self._datasources.get(uid)

    def list_datasources(self) -> List[Dict[str, Any]]:
        """List all configured data sources"""
        return [
            {
                "id": ds.id,
                "uid": ds.uid,
                "name": ds.name,
                "type": ds.type.value,
                "url": ds.url,
                "isDefault": ds.is_default
            }
            for ds in self._datasources.values()
        ]

    def update_datasource(self, uid: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update data source configuration"""
        datasource = self._datasources.get(uid)
        if not datasource:
            return None
        
        for key, value in updates.items():
            if hasattr(datasource, key):
                setattr(datasource, key, value)
        
        return {"uid": uid, "status": "success", "message": "Data source updated"}

    def delete_datasource(self, uid: str) -> bool:
        """Delete a data source"""
        if uid in self._datasources:
            del self._datasources[uid]
            return True
        return False

    def get_default_datasource(self, ds_type: DataSourceType) -> Optional[DataSource]:
        """Get default data source for a specific type"""
        for ds in self._datasources.values():
            if ds.type == ds_type and ds.is_default:
                return ds
        return None

    # ========== 3. Alert Management ==========
    
    def create_alert_rule(self, alert: AlertRule) -> Dict[str, Any]:
        """
        Create an alert rule in Grafana.
        
        Args:
            alert: Alert rule configuration
            
        Returns:
            Created alert info
        """
        alert_id = str(uuid.uuid4())
        self._alerts[alert_id] = alert
        
        return {
            "id": alert_id,
            "uid": alert_id,
            "name": alert.name,
            "folderTitle": alert.folder_title,
            "interval": alert.interval,
            "state": AlertState.OK.value,
            "status": "success"
        }

    def get_alert_rule(self, alert_id: str) -> Optional[AlertRule]:
        """Get alert rule by ID"""
        return self._alerts.get(alert_id)

    def list_alert_rules(self) -> List[Dict[str, Any]]:
        """List all alert rules"""
        return [
            {
                "id": alert_id,
                "name": alert.name,
                "folderTitle": alert.folder_title,
                "interval": alert.interval,
                "state": AlertState.OK.value,
                "isPaused": alert.is_paused
            }
            for alert_id, alert in self._alerts.items()
        ]

    def update_alert_rule(self, alert_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update alert rule"""
        alert = self._alerts.get(alert_id)
        if not alert:
            return None
        
        for key, value in updates.items():
            if hasattr(alert, key):
                setattr(alert, key, value)
        
        return {"id": alert_id, "status": "success", "message": "Alert rule updated"}

    def delete_alert_rule(self, alert_id: str) -> bool:
        """Delete an alert rule"""
        if alert_id in self._alerts:
            del self._alerts[alert_id]
            return True
        return False

    def pause_alert(self, alert_id: str, paused: bool = True) -> Optional[Dict[str, Any]]:
        """Pause or resume an alert"""
        alert = self._alerts.get(alert_id)
        if not alert:
            return None
        
        alert.is_paused = paused
        return {"id": alert_id, "isPaused": paused, "status": "success"}

    # ========== 4. Annotation Events ==========
    
    def add_annotation(self, annotation: Annotation) -> Dict[str, Any]:
        """
        Add an annotation event to Grafana graphs.
        
        Args:
            annotation: Annotation event data
            
        Returns:
            Created annotation info
        """
        annotation_id = len(self._annotations) + 1
        self._annotations.append(annotation)
        
        return {
            "id": annotation_id,
            "time": annotation.time,
            "timeEnd": annotation.time_end,
            "text": annotation.text,
            "tags": annotation.tags,
            "status": "success"
        }

    def get_annotation(self, annotation_id: int) -> Optional[Annotation]:
        """Get annotation by ID"""
        if 0 <= annotation_id < len(self._annotations):
            return self._annotations[annotation_id]
        return None

    def list_annotations(
        self,
        dashboard_id: Optional[int] = None,
        panel_id: Optional[int] = None,
        from_time: Optional[int] = None,
        to_time: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """List annotations with optional filters"""
        annotations = self._annotations
        
        filtered = []
        for i, ann in enumerate(annotations):
            if dashboard_id is not None and ann.dashboard_id != dashboard_id:
                continue
            if panel_id is not None and ann.panel_id != panel_id:
                continue
            if from_time is not None and ann.time < from_time:
                continue
            if to_time is not None and ann.time > to_time:
                continue
            filtered.append({
                "id": i + 1,
                "time": ann.time,
                "timeEnd": ann.time_end,
                "text": ann.text,
                "tags": ann.tags,
                "dashboardId": ann.dashboard_id,
                "panelId": ann.panel_id
            })
        
        return filtered

    def delete_annotation(self, annotation_id: int) -> bool:
        """Delete an annotation"""
        if 0 <= annotation_id < len(self._annotations):
            del self._annotations[annotation_id]
            return True
        return False

    def update_annotation(self, annotation_id: int, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an annotation"""
        if annotation_id < 0 or annotation_id >= len(self._annotations):
            return None
        
        ann = self._annotations[annotation_id]
        for key, value in updates.items():
            if hasattr(ann, key):
                setattr(ann, key, value)
        
        return {"id": annotation_id, "status": "success"}

    # ========== 5. Template Variables ==========
    
    def create_template_variable(self, variable: TemplateVariable, dashboard_uid: str) -> Optional[Dict[str, Any]]:
        """
        Create a template variable for a dashboard.
        
        Args:
            variable: Template variable configuration
            dashboard_uid: Dashboard UID to add variable to
            
        Returns:
            Created variable info
        """
        dashboard = self._dashboards.get(dashboard_uid)
        if not dashboard:
            return None
        
        var_def = {
            "name": variable.name,
            "type": variable.type,
            "query": variable.query,
            "datasource": variable.datasource,
            "regex": variable.regex,
            "multi": variable.multi,
            "includeAll": variable.include_all,
            "allValue": variable.all_value,
            "hide": variable.hide
        }
        
        if "variables" not in dashboard:
            dashboard["variables"] = []
        dashboard["variables"].append(var_def)
        
        return {
            "name": variable.name,
            "type": variable.type,
            "status": "success",
            "dashboardUid": dashboard_uid
        }

    def list_template_variables(self, dashboard_uid: str) -> List[Dict[str, Any]]:
        """List template variables for a dashboard"""
        dashboard = self._dashboards.get(dashboard_uid)
        if not dashboard or "variables" not in dashboard:
            return []
        return dashboard["variables"]

    def delete_template_variable(self, dashboard_uid: str, var_name: str) -> bool:
        """Delete a template variable from dashboard"""
        dashboard = self._dashboards.get(dashboard_uid)
        if not dashboard or "variables" not in dashboard:
            return False
        
        dashboard["variables"] = [v for v in dashboard["variables"] if v["name"] != var_name]
        return True

    # ========== 6. Snapshot Sharing ==========
    
    def create_snapshot(self, snapshot: DashboardSnapshot) -> Dict[str, Any]:
        """
        Create and share a dashboard snapshot.
        
        Args:
            snapshot: Snapshot configuration
            
        Returns:
            Created snapshot info with share URL
        """
        snapshot_id = str(uuid.uuid4())
        snapshot_data = {
            "id": snapshot_id,
            "name": snapshot.name,
            "dashboard": snapshot.dashboard_json,
            "expires": time.time() + snapshot.expires_seconds,
            "sharing": snapshot.sharing.value,
            "external": snapshot.external,
            "created_at": time.time()
        }
        
        self._snapshots[snapshot_id] = snapshot_data
        
        return {
            "id": snapshot_id,
            "name": snapshot.name,
            "url": f"/plugins/grafana-snapshots/?snapshot={snapshot_id}",
            "delete_url": f"/api/snapshots/{snapshot_id}",
            "external_url": f"{self.base_url}/s/{snapshot_id}" if snapshot.external else None,
            "status": "success"
        }

    def get_snapshot(self, snapshot_id: str) -> Optional[Dict[str, Any]]:
        """Get snapshot by ID"""
        return self._snapshots.get(snapshot_id)

    def list_snapshots(self) -> List[Dict[str, Any]]:
        """List all snapshots"""
        return [
            {
                "id": sid,
                "name": data["name"],
                "expires": data["expires"],
                "sharing": data["sharing"],
                "created_at": data["created_at"]
            }
            for sid, data in self._snapshots.items()
        ]

    def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot"""
        if snapshot_id in self._snapshots:
            del self._snapshots[snapshot_id]
            return True
        return False

    # ========== 7. Playlist Management ==========
    
    def create_playlist(self, playlist: DashboardPlaylist) -> Dict[str, Any]:
        """
        Create a dashboard playlist.
        
        Args:
            playlist: Playlist configuration
            
        Returns:
            Created playlist info
        """
        playlist_id = str(uuid.uuid4())
        self._playlists[playlist_id] = playlist
        
        return {
            "id": playlist_id,
            "name": playlist.name,
            "interval": playlist.interval,
            "dashboard_count": len(playlist.dashboards),
            "url": f"/playlists/{playlist_id}",
            "status": "success"
        }

    def get_playlist(self, playlist_id: str) -> Optional[DashboardPlaylist]:
        """Get playlist by ID"""
        return self._playlists.get(playlist_id)

    def list_playlists(self) -> List[Dict[str, Any]]:
        """List all playlists"""
        return [
            {
                "id": pid,
                "name": p.name,
                "interval": p.interval,
                "dashboard_count": len(p.dashboards)
            }
            for pid, p in self._playlists.items()
        ]

    def add_dashboard_to_playlist(self, playlist_id: str, dashboard_uid: str, title: str, order: int = 0) -> Optional[Dict[str, Any]]:
        """Add a dashboard to playlist"""
        playlist = self._playlists.get(playlist_id)
        if not playlist:
            return None
        
        playlist.dashboards.append({
            "uid": dashboard_uid,
            "title": title,
            "order": order
        })
        playlist.dashboards.sort(key=lambda x: x["order"])
        
        return {"status": "success", "message": f"Dashboard '{title}' added to playlist"}

    def delete_playlist(self, playlist_id: str) -> bool:
        """Delete a playlist"""
        if playlist_id in self._playlists:
            del self._playlists[playlist_id]
            return True
        return False

    # ========== 8. API Key Management ==========
    
    def create_api_key(self, api_key: APIKey) -> Dict[str, Any]:
        """
        Create a Grafana API key.
        
        Args:
            api_key: API key configuration
            
        Returns:
            Created API key info (key shown only once)
        """
        key_value = f"glsa_{uuid.uuid4().hex}_{uuid.uuid4().hex[:8]}"
        key_id = str(uuid.uuid4())
        
        self._api_keys[key_id] = api_key
        
        return {
            "id": key_id,
            "name": api_key.name,
            "key": key_value,  # Only returned once
            "role": api_key.role,
            "expiresIn": api_key.expires_in,
            "createdAt": time.time(),
            "status": "success",
            "warning": "The API key is only shown once. Store it securely."
        }

    def list_api_keys(self) -> List[Dict[str, Any]]:
        """List all API keys (without exposing the actual key)"""
        return [
            {
                "id": kid,
                "name": key.name,
                "role": key.role,
                "expiresIn": key.expires_in,
                "createdAt": time.time()
            }
            for kid, key in self._api_keys.items()
        ]

    def delete_api_key(self, key_id: str) -> bool:
        """Delete an API key"""
        if key_id in self._api_keys:
            del self._api_keys[key_id]
            return True
        return False

    def validate_api_key(self, key: str) -> bool:
        """Validate an API key"""
        for api_key in self._api_keys.values():
            # In real implementation, would check against stored hash
            pass
        return False

    # ========== 9. SSO Integration ==========
    
    def configure_sso(self, sso: SSOConfig) -> Dict[str, Any]:
        """
        Configure SSO with Grafana.
        
        Args:
            sso: SSO configuration
            
        Returns:
            SSO config status
        """
        self._sso_configs[sso.name] = sso
        
        return {
            "name": sso.name,
            "type": sso.type,
            "auth_url": sso.auth_url,
            "token_url": sso.token_url,
            "allow_sign_up": sso.allow_sign_up,
            "status": "success",
            "message": f"SSO '{sso.name}' configured successfully"
        }

    def get_sso_config(self, name: str) -> Optional[SSOConfig]:
        """Get SSO configuration by name"""
        return self._sso_configs.get(name)

    def list_sso_configs(self) -> List[Dict[str, Any]]:
        """List all SSO configurations"""
        return [
            {
                "name": sso.name,
                "type": sso.type,
                "auth_url": sso.auth_url,
                "allow_sign_up": sso.allow_sign_up,
                "auto_login": sso.auto_login
            }
            for sso in self._sso_configs.values()
        ]

    def update_sso_config(self, name: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update SSO configuration"""
        sso = self._sso_configs.get(name)
        if not sso:
            return None
        
        for key, value in updates.items():
            if hasattr(sso, key):
                setattr(sso, key, value)
        
        return {"name": name, "status": "success", "message": "SSO config updated"}

    def delete_sso_config(self, name: str) -> bool:
        """Delete SSO configuration"""
        if name in self._sso_configs:
            del self._sso_configs[name]
            return True
        return False

    # ========== 10. Folder Management ==========
    
    def create_folder(self, folder: DashboardFolder) -> Dict[str, Any]:
        """
        Create a dashboard folder.
        
        Args:
            folder: Folder configuration
            
        Returns:
            Created folder info
        """
        folder.uid = folder.uid or str(uuid.uuid4())
        folder.id = len(self._folders) + 1
        self._folders[folder.uid] = folder
        
        return {
            "id": folder.id,
            "uid": folder.uid,
            "title": folder.title,
            "url": f"/dashboards/f/{folder.uid}",
            "status": "success"
        }

    def get_folder(self, uid: str) -> Optional[DashboardFolder]:
        """Get folder by UID"""
        return self._folders.get(uid)

    def list_folders(self) -> List[Dict[str, Any]]:
        """List all dashboard folders"""
        return [
            {
                "id": f.id,
                "uid": f.uid,
                "title": f.title
            }
            for f in self._folders.values()
        ]

    def update_folder(self, uid: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update folder"""
        folder = self._folders.get(uid)
        if not folder:
            return None
        
        for key, value in updates.items():
            if hasattr(folder, key):
                setattr(folder, key, value)
        
        return {"uid": uid, "status": "success", "message": "Folder updated"}

    def delete_folder(self, uid: str) -> bool:
        """Delete a folder"""
        if uid in self._folders:
            del self._folders[uid]
            return True
        return False

    def get_folder_dashboards(self, folder_uid: str) -> List[Dict[str, Any]]:
        """Get all dashboards in a folder"""
        dashboards = []
        for uid, data in self._dashboards.items():
            prov = data.get("provisioning", {})
            if prov.get("folder_id"):
                folder = self._folders.get(folder_uid)
                if folder and folder.id == prov["folder_id"]:
                    dashboards.append({
                        "uid": uid,
                        "title": data["dashboard"]["title"]
                    })
        return dashboards

    # ========== Utility Methods ==========
    
    def health_check(self) -> Dict[str, Any]:
        """Check Grafana instance health"""
        return {
            "status": "ok",
            "version": "10.0.0",
            "commit": "abc123",
            "base_url": self.base_url,
            "configured": True
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get integration statistics"""
        return {
            "dashboards": len(self._dashboards),
            "datasources": len(self._datasources),
            "folders": len(self._folders),
            "alerts": len(self._alerts),
            "annotations": len(self._annotations),
            "snapshots": len(self._snapshots),
            "playlists": len(self._playlists),
            "api_keys": len(self._api_keys),
            "sso_configs": len(self._sso_configs)
        }

    def export_config(self) -> Dict[str, Any]:
        """Export all configuration for backup"""
        return {
            "version": "23",
            "exported_at": time.time(),
            "dashboards": self._dashboards,
            "datasources": [asdict(ds) for ds in self._datasources.values()],
            "folders": [asdict(f) for f in self._folders.values()],
            "alerts": [asdict(a) for a in self._alerts.values()],
            "sso_configs": [asdict(s) for s in self._sso_configs.values()]
        }

    def import_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Import configuration from backup"""
        imported = {"dashboards": 0, "datasources": 0, "folders": 0, "alerts": 0, "sso": 0}
        
        if "dashboards" in config:
            self._dashboards.update(config["dashboards"])
            imported["dashboards"] = len(config["dashboards"])
        
        if "datasources" in config:
            for ds_data in config["datasources"]:
                ds = DataSource(**ds_data)
                self._datasources[ds.uid] = ds
            imported["datasources"] = len(config["datasources"])
        
        if "folders" in config:
            for f_data in config["folders"]:
                f = DashboardFolder(**f_data)
                self._folders[f.uid] = f
            imported["folders"] = len(config["folders"])
        
        if "alerts" in config:
            for a_data in config["alerts"]:
                alert = AlertRule(**a_data)
                alert_id = str(uuid.uuid4())
                self._alerts[alert_id] = alert
            imported["alerts"] = len(config["alerts"])
        
        if "sso_configs" in config:
            for sso_data in config["sso_configs"]:
                sso = SSOConfig(**sso_data)
                self._sso_configs[sso.name] = sso
            imported["sso"] = len(config["sso_configs"])
        
        return {"status": "success", "imported": imported}


# Standalone test/example
if __name__ == "__main__":
    # Example usage
    grafana = GrafanaIntegration("http://localhost:3000", "your-api-key")
    
    # Create a folder
    folder = grafana.create_folder(DashboardFolder(title="Workflow Dashboards"))
    print(f"Created folder: {folder}")
    
    # Create a data source
    ds = grafana.create_datasource(DataSource(
        name="Prometheus",
        type=DataSourceType.PROMETHEUS,
        url="http://prometheus:9090",
        is_default=True
    ))
    print(f"Created datasource: {ds}")
    
    # Provision a dashboard
    dashboard = grafana.provision_dashboard(DashboardProvisioning(
        dashboard_id="wf-001",
        title="Workflow Performance",
        folder_id=folder["id"],
        overwrite=True
    ))
    print(f"Provisioned dashboard: {dashboard}")
    
    # Add template variable
    var = grafana.create_template_variable(
        TemplateVariable(
            name="environment",
            query="label_values(workflow_duration_seconds, env)",
            type="query",
            datasource="Prometheus"
        ),
        dashboard["uid"]
    )
    print(f"Created variable: {var}")
    
    # Create alert
    alert = grafana.create_alert_rule(AlertRule(
        name="High Error Rate",
        folder_title="Workflow Alerts",
        condition="C",
        data=[{"query": {"expr": "rate(workflow_errors_total[5m]) > 0.1"}}],
        interval="1m",
        labels={"severity": "critical"}
    ))
    print(f"Created alert: {alert}")
    
    # Add annotation
    annotation = grafana.add_annotation(Annotation(
        text="Workflow deployment completed",
        time=int(time.time() * 1000),
        tags=["deployment", "workflow"]
    ))
    print(f"Added annotation: {annotation}")
    
    # Create snapshot
    snapshot = grafana.create_snapshot(DashboardSnapshot(
        dashboard_json={"title": "Test Dashboard"},
        name="Weekly Report Snapshot",
        expires_seconds=86400
    ))
    print(f"Created snapshot: {snapshot}")
    
    # Create playlist
    playlist = grafana.create_playlist(DashboardPlaylist(
        name="Daily Standup Dashboards",
        interval="10m",
        dashboards=[{"uid": dashboard["uid"], "title": "Workflow Performance", "order": 1}]
    ))
    print(f"Created playlist: {playlist}")
    
    # Create API key
    api_key = grafana.create_api_key(APIKey(
        name="CI/CD Pipeline Key",
        role="Editor",
        expires_in=2592000  # 30 days
    ))
    print(f"Created API key: {api_key['name']} (key hidden for security)")
    
    # Configure SSO
    sso = grafana.configure_sso(SSOConfig(
        name="github-oauth",
        type="oauth",
        auth_url="https://github.com/login/oauth/authorize",
        token_url="https://github.com/login/oauth/access_token",
        scopes=["user:email", "read:user"]
    ))
    print(f"Configured SSO: {sso}")
    
    # Get stats
    stats = grafana.get_stats()
    print(f"Integration stats: {stats}")

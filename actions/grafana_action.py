"""Grafana action module for RabAI AutoClick.

Provides Grafana operations including dashboard management,
alert configuration, and metric data retrieval.
"""

import os
import sys
import time
import json
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class AlertRule:
    """Represents a Grafana alert rule.
    
    Attributes:
        name: Rule name.
        condition: Alert condition (e.g., 'B' for threshold).
        data: Query/data source configuration.
        interval: Evaluation interval (e.g., '1m').
        no_data_state: State when no data ('NoData', 'Alerting', 'OK').
        exec_err_state: State on execution error.
        labels: Optional alert labels.
        annotations: Optional alert annotations.
    """
    name: str
    condition: str = "B"
    data: List[Dict[str, Any]] = field(default_factory=list)
    interval: str = "1m"
    no_data_state: str = "NoData"
    exec_err_state: str = "Alerting"
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)


class GrafanaClient:
    """Grafana client wrapper for dashboard and alert operations.
    
    Provides methods for interacting with Grafana API including
    dashboard management, alert rules, and data source queries.
    """
    
    def __init__(
        self,
        url: str = "http://localhost:3000",
        api_key: str = "",
        timeout: int = 30
    ) -> None:
        """Initialize Grafana client.
        
        Args:
            url: Grafana server URL.
            api_key: API key for authentication.
            timeout: Request timeout in seconds.
        """
        self.url = url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._session: Optional[Any] = None
    
    def connect(self) -> bool:
        """Test connection to Grafana server.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import requests
        except ImportError:
            raise ImportError("requests is required. Install with: pip install requests")
        
        try:
            self._session = requests.Session()
            self._session.headers.update({
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            })
            
            response = self._session.get(
                f"{self.url}/api/health",
                timeout=self.timeout
            )
            
            return response.status_code == 200
        
        except Exception:
            self._session = None
            return False
    
    def disconnect(self) -> None:
        """Close the Grafana session."""
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
    
    def get_dashboard(self, uid: str) -> Optional[Dict[str, Any]]:
        """Get a dashboard by UID.
        
        Args:
            uid: Dashboard UID.
            
        Returns:
            Dashboard JSON or None if not found.
        """
        if not self._session:
            raise RuntimeError("Not connected to Grafana")
        
        try:
            response = self._session.get(
                f"{self.url}/api/dashboards/uid/{uid}",
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return response.json()
            
            return None
        
        except Exception as e:
            raise Exception(f"Get dashboard failed: {str(e)}")
    
    def create_dashboard(
        self,
        dashboard: Dict[str, Any],
        message: str = "",
        overwrite: bool = True
    ) -> Dict[str, Any]:
        """Create or update a dashboard.
        
        Args:
            dashboard: Dashboard JSON object.
            message: Optional commit message.
            overwrite: Whether to overwrite existing.
            
        Returns:
            Response with dashboard URL and UID.
        """
        if not self._session:
            raise RuntimeError("Not connected to Grafana")
        
        try:
            payload = {
                "dashboard": dashboard,
                "message": message,
                "overwrite": overwrite
            }
            
            response = self._session.post(
                f"{self.url}/api/dashboards/db",
                json=payload,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return response.json()
            
            raise Exception(f"Create dashboard failed: {response.status_code} - {response.text}")
        
        except Exception as e:
            raise Exception(f"Create dashboard failed: {str(e)}")
    
    def delete_dashboard(self, uid: str) -> bool:
        """Delete a dashboard by UID.
        
        Args:
            uid: Dashboard UID.
            
        Returns:
            True if deleted successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Grafana")
        
        try:
            response = self._session.delete(
                f"{self.url}/api/dashboards/uid/{uid}",
                timeout=self.timeout
            )
            
            return response.status_code in (200, 204)
        
        except Exception:
            return False
    
    def list_dashboards(
        self,
        limit: int = 100,
        folder_ids: Optional[List[int]] = None
    ) -> List[Dict[str, Any]]:
        """List all dashboards.
        
        Args:
            limit: Maximum number of results.
            folder_ids: Optional folder ID filter.
            
        Returns:
            List of dashboard summaries.
        """
        if not self._session:
            raise RuntimeError("Not connected to Grafana")
        
        try:
            params: Dict[str, Any] = {"limit": limit}
            if folder_ids:
                params["folderIds"] = folder_ids
            
            response = self._session.get(
                f"{self.url}/api/search",
                params=params,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"List dashboards failed: {str(e)}")
    
    def get_alert_rules(self) -> List[Dict[str, Any]]:
        """Get all alert rules.
        
        Returns:
            List of alert rules.
        """
        if not self._session:
            raise RuntimeError("Not connected to Grafana")
        
        try:
            response = self._session.get(
                f"{self.url}/api/ruler/grafana/api/v1/rules",
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("groups", [])
            
            return []
        
        except Exception as e:
            raise Exception(f"Get alert rules failed: {str(e)}")
    
    def create_alert_rule(
        self,
        folder: str,
        rule: AlertRule,
        folder_uid: Optional[str] = None
    ) -> bool:
        """Create an alert rule.
        
        Args:
            folder: Folder name to store the rule.
            rule: AlertRule object.
            folder_uid: Optional folder UID.
            
        Returns:
            True if created successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Grafana")
        
        try:
            payload = {
                "name": rule.name,
                "condition": rule.condition,
                "data": rule.data,
                "interval": rule.interval,
                "noDataState": rule.no_data_state,
                "execErrState": rule.exec_err_state,
                "labels": rule.labels,
                "annotations": rule.annotations
            }
            
            response = self._session.post(
                f"{self.url}/api/ruler/grafana/api/v1/rules/{folder}",
                json=payload,
                timeout=self.timeout
            )
            
            return response.status_code in (200, 202)
        
        except Exception as e:
            raise Exception(f"Create alert rule failed: {str(e)}")
    
    def get_data_source(self, uid: str) -> Optional[Dict[str, Any]]:
        """Get a data source by UID.
        
        Args:
            uid: Data source UID.
            
        Returns:
            Data source configuration or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Grafana")
        
        try:
            response = self._session.get(
                f"{self.url}/api/datasources/uid/{uid}",
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return response.json()
            
            return None
        
        except Exception as e:
            raise Exception(f"Get data source failed: {str(e)}")
    
    def list_data_sources(self) -> List[Dict[str, Any]]:
        """List all data sources.
        
        Returns:
            List of data source configurations.
        """
        if not self._session:
            raise RuntimeError("Not connected to Grafana")
        
        try:
            response = self._session.get(
                f"{self.url}/api/datasources",
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"List data sources failed: {str(e)}")
    
    def query_metric(
        self,
        ds_uid: str,
        query: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute a metric query.
        
        Args:
            ds_uid: Data source UID.
            query: Query object with expr (PromQL) or other query fields.
            
        Returns:
            Query results.
        """
        if not self._session:
            raise RuntimeError("Not connected to Grafana")
        
        try:
            payload = {
                "queries": [query],
                "from": "now-1h",
                "to": "now"
            }
            
            response = self._session.post(
                f"{self.url}/api/ds/query",
                json=payload,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("results", {}).get("A", {}).get("frames", [])
            
            return []
        
        except Exception as e:
            raise Exception(f"Query metric failed: {str(e)}")
    
    def get_annotations(
        self,
        start: int,
        end: int,
        limit: int = 100,
        tags: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get annotations within a time range.
        
        Args:
            start: Start time (unix epoch seconds).
            end: End time (unix epoch seconds).
            limit: Maximum number of annotations.
            tags: Optional tag filter.
            
        Returns:
            List of annotations.
        """
        if not self._session:
            raise RuntimeError("Not connected to Grafana")
        
        try:
            params: Dict[str, Any] = {
                "from": start * 1000,
                "to": end * 1000,
                "limit": limit
            }
            if tags:
                params["tags"] = tags
            
            response = self._session.get(
                f"{self.url}/api/annotations",
                params=params,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"Get annotations failed: {str(e)}")
    
    def create_annotation(
        self,
        time: int,
        text: str,
        tags: Optional[List[str]] = None,
        dashboard_id: Optional[int] = None,
        panel_id: Optional[int] = None
    ) -> Optional[int]:
        """Create an annotation.
        
        Args:
            time: Annotation timestamp (unix epoch seconds).
            text: Annotation text content.
            tags: Optional tags.
            dashboard_id: Optional linked dashboard ID.
            panel_id: Optional linked panel ID.
            
        Returns:
            Annotation ID or None if failed.
        """
        if not self._session:
            raise RuntimeError("Not connected to Grafana")
        
        try:
            payload: Dict[str, Any] = {
                "time": time * 1000,
                "text": text
            }
            if tags:
                payload["tags"] = tags
            if dashboard_id:
                payload["dashboardId"] = dashboard_id
            if panel_id:
                payload["panelId"] = panel_id
            
            response = self._session.post(
                f"{self.url}/api/annotations",
                json=payload,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("id")
            
            return None
        
        except Exception as e:
            raise Exception(f"Create annotation failed: {str(e)}")
    
    def get_folders(self) -> List[Dict[str, Any]]:
        """Get all folders.
        
        Returns:
            List of folder configurations.
        """
        if not self._session:
            raise RuntimeError("Not connected to Grafana")
        
        try:
            response = self._session.get(
                f"{self.url}/api/folders",
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"Get folders failed: {str(e)}")
    
    def get_server_info(self) -> Dict[str, Any]:
        """Get Grafana server information.
        
        Returns:
            Server info dictionary.
        """
        if not self._session:
            raise RuntimeError("Not connected to Grafana")
        
        try:
            response = self._session.get(
                f"{self.url}/api/health",
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return response.json()
            
            return {}
        
        except Exception as e:
            raise Exception(f"Get server info failed: {str(e)}")


class GrafanaAction(BaseAction):
    """Grafana action for dashboard and alert operations.
    
    Supports dashboard CRUD, alert management, and annotations.
    """
    action_type: str = "grafana"
    display_name: str = "Grafana动作"
    description: str = "Grafana仪表盘和告警管理操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[GrafanaClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Grafana operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "disconnect":
                return self._disconnect(start_time)
            elif operation == "get_dashboard":
                return self._get_dashboard(params, start_time)
            elif operation == "create_dashboard":
                return self._create_dashboard(params, start_time)
            elif operation == "delete_dashboard":
                return self._delete_dashboard(params, start_time)
            elif operation == "list_dashboards":
                return self._list_dashboards(params, start_time)
            elif operation == "get_alert_rules":
                return self._get_alert_rules(start_time)
            elif operation == "create_alert_rule":
                return self._create_alert_rule(params, start_time)
            elif operation == "list_data_sources":
                return self._list_data_sources(start_time)
            elif operation == "query_metric":
                return self._query_metric(params, start_time)
            elif operation == "get_annotations":
                return self._get_annotations(params, start_time)
            elif operation == "create_annotation":
                return self._create_annotation(params, start_time)
            elif operation == "get_folders":
                return self._get_folders(start_time)
            elif operation == "server_info":
                return self._server_info(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except ImportError as e:
            return ActionResult(
                success=False,
                message=f"Import error: {str(e)}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Grafana operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Grafana server."""
        url = params.get("url", "http://localhost:3000")
        api_key = params.get("api_key", "")
        
        self._client = GrafanaClient(url=url, api_key=api_key)
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to Grafana at {url}" if success else "Failed to connect",
            duration=time.time() - start_time
        )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from Grafana server."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(
            success=True,
            message="Disconnected from Grafana",
            duration=time.time() - start_time
        )
    
    def _get_dashboard(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a dashboard by UID."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        uid = params.get("uid", "")
        if not uid:
            return ActionResult(success=False, message="uid is required", duration=time.time() - start_time)
        
        try:
            dashboard = self._client.get_dashboard(uid)
            return ActionResult(
                success=dashboard is not None,
                message=f"Found dashboard: {uid}" if dashboard else f"Dashboard not found: {uid}",
                data={"dashboard": dashboard},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_dashboard(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create or update a dashboard."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        dashboard = params.get("dashboard", {})
        message = params.get("message", "")
        overwrite = params.get("overwrite", True)
        
        if not dashboard:
            return ActionResult(success=False, message="dashboard is required", duration=time.time() - start_time)
        
        try:
            result = self._client.create_dashboard(dashboard=dashboard, message=message, overwrite=overwrite)
            return ActionResult(
                success=True,
                message=f"Dashboard created: {result.get('url', '')}",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_dashboard(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a dashboard."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        uid = params.get("uid", "")
        if not uid:
            return ActionResult(success=False, message="uid is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_dashboard(uid)
            return ActionResult(
                success=success,
                message=f"Deleted dashboard: {uid}" if success else f"Delete failed: {uid}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_dashboards(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all dashboards."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        limit = params.get("limit", 100)
        folder_ids = params.get("folder_ids")
        
        try:
            dashboards = self._client.list_dashboards(limit=limit, folder_ids=folder_ids)
            return ActionResult(
                success=True,
                message=f"Found {len(dashboards)} dashboards",
                data={"dashboards": dashboards, "count": len(dashboards)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_alert_rules(self, start_time: float) -> ActionResult:
        """Get all alert rules."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            rules = self._client.get_alert_rules()
            return ActionResult(
                success=True,
                message=f"Found {len(rules)} alert groups",
                data={"rules": rules},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_alert_rule(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create an alert rule."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        folder = params.get("folder", "")
        name = params.get("name", "")
        
        if not folder or not name:
            return ActionResult(success=False, message="folder and name are required", duration=time.time() - start_time)
        
        rule = AlertRule(
            name=name,
            condition=params.get("condition", "B"),
            data=params.get("data", []),
            interval=params.get("interval", "1m"),
            no_data_state=params.get("no_data_state", "NoData"),
            exec_err_state=params.get("exec_err_state", "Alerting"),
            labels=params.get("labels", {}),
            annotations=params.get("annotations", {})
        )
        
        try:
            success = self._client.create_alert_rule(folder=folder, rule=rule)
            return ActionResult(
                success=success,
                message=f"Created alert rule: {name}" if success else "Create alert rule failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_data_sources(self, start_time: float) -> ActionResult:
        """List all data sources."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            data_sources = self._client.list_data_sources()
            return ActionResult(
                success=True,
                message=f"Found {len(data_sources)} data sources",
                data={"data_sources": data_sources, "count": len(data_sources)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _query_metric(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a metric query."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        ds_uid = params.get("ds_uid", "")
        query = params.get("query", {})
        
        if not ds_uid or not query:
            return ActionResult(success=False, message="ds_uid and query are required", duration=time.time() - start_time)
        
        try:
            results = self._client.query_metric(ds_uid=ds_uid, query=query)
            return ActionResult(
                success=True,
                message=f"Query returned {len(results)} frames",
                data={"frames": results},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_annotations(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get annotations within a time range."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        start = params.get("start", int(time.time()) - 3600)
        end = params.get("end", int(time.time()))
        limit = params.get("limit", 100)
        tags = params.get("tags")
        
        try:
            annotations = self._client.get_annotations(start=start, end=end, limit=limit, tags=tags)
            return ActionResult(
                success=True,
                message=f"Found {len(annotations)} annotations",
                data={"annotations": annotations, "count": len(annotations)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_annotation(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create an annotation."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        time_val = params.get("time", int(time.time()))
        text = params.get("text", "")
        
        if not text:
            return ActionResult(success=False, message="text is required", duration=time.time() - start_time)
        
        try:
            annotation_id = self._client.create_annotation(
                time=time_val,
                text=text,
                tags=params.get("tags"),
                dashboard_id=params.get("dashboard_id"),
                panel_id=params.get("panel_id")
            )
            return ActionResult(
                success=annotation_id is not None,
                message=f"Created annotation: {annotation_id}" if annotation_id else "Create annotation failed",
                data={"annotation_id": annotation_id},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_folders(self, start_time: float) -> ActionResult:
        """Get all folders."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            folders = self._client.get_folders()
            return ActionResult(
                success=True,
                message=f"Found {len(folders)} folders",
                data={"folders": folders, "count": len(folders)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _server_info(self, start_time: float) -> ActionResult:
        """Get Grafana server information."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            info = self._client.get_server_info()
            return ActionResult(
                success=True,
                message=f"Grafana at {self._client.url}",
                data=info,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)

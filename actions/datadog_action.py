"""Datadog action module for RabAI AutoClick.

Provides Datadog operations including metrics querying,
alert management, and dashboard operations.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DatadogClient:
    """Datadog client for monitoring and alerting operations.
    
    Provides methods for querying metrics, managing monitors,
    and interacting with Datadog API.
    """
    
    def __init__(
        self,
        api_key: str = "",
        app_key: str = "",
        site: str = "datadoghq.com"
    ) -> None:
        """Initialize Datadog client.
        
        Args:
            api_key: Datadog API key.
            app_key: Datadog application key.
            site: Datadog site (datadoghq.com, datadoghq.eu, etc.).
        """
        self.api_key = api_key
        self.app_key = app_key
        self.site = site
        self._session: Optional[Any] = None
        self._base_url = f"https://api.{site}"
    
    def connect(self) -> bool:
        """Test connection to Datadog API.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import requests
        except ImportError:
            raise ImportError("requests is required. Install with: pip install requests")
        
        if not self.api_key or not self.app_key:
            return False
        
        try:
            self._session = requests.Session()
            self._session.headers.update({
                "DD-API-KEY": self.api_key,
                "DD-APPLICATION-KEY": self.app_key,
                "Content-Type": "application/json"
            })
            
            response = self._session.get(
                f"{self._base_url}/api/v1/check_run",
                params={"check": "datadog.api.health"},
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            self._session = None
            return False
    
    def disconnect(self) -> None:
        """Close the Datadog session."""
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
    
    def query_metrics(
        self,
        query: str,
        from_ts: int,
        to_ts: int
    ) -> Dict[str, Any]:
        """Query metrics from Datadog.
        
        Args:
            query: Metric query string.
            from_ts: Start timestamp (unix).
            to_ts: End timestamp (unix).
            
        Returns:
            Query results.
        """
        if not self._session:
            raise RuntimeError("Not connected to Datadog")
        
        try:
            response = self._session.get(
                f"{self._base_url}/api/v1/query",
                params={"query": query, "from": from_ts, "to": to_ts},
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return {}
        
        except Exception as e:
            raise Exception(f"Query metrics failed: {str(e)}")
    
    def list_monitors(self) -> List[Dict[str, Any]]:
        """List all monitors.
        
        Returns:
            List of monitor information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Datadog")
        
        try:
            response = self._session.get(
                f"{self._base_url}/api/v1/monitor",
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"List monitors failed: {str(e)}")
    
    def get_monitor(self, monitor_id: str) -> Optional[Dict[str, Any]]:
        """Get a monitor by ID.
        
        Args:
            monitor_id: Monitor ID.
            
        Returns:
            Monitor information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Datadog")
        
        try:
            response = self._session.get(
                f"{self._base_url}/api/v1/monitor/{monitor_id}",
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def create_monitor(
        self,
        monitor_type: str,
        query: str,
        name: str,
        message: str,
        tags: Optional[List[str]] = None
    ) -> Optional[str]:
        """Create a new monitor.
        
        Args:
            monitor_type: Monitor type (metric, service_check, event, etc.).
            query: Monitor query.
            name: Monitor name.
            message: Alert message.
            tags: Optional tags.
            
        Returns:
            Monitor ID or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Datadog")
        
        try:
            data = {
                "type": monitor_type,
                "query": query,
                "name": name,
                "message": message
            }
            
            if tags:
                data["tags"] = tags
            
            response = self._session.post(
                f"{self._base_url}/api/v1/monitor",
                json=data,
                timeout=30
            )
            
            if response.status_code in (200, 201, 202):
                return response.json().get("id")
            
            return None
        
        except Exception as e:
            raise Exception(f"Create monitor failed: {str(e)}")
    
    def update_monitor(
        self,
        monitor_id: str,
        query: Optional[str] = None,
        name: Optional[str] = None,
        message: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> bool:
        """Update a monitor.
        
        Args:
            monitor_id: Monitor ID.
            query: New query.
            name: New name.
            message: New message.
            tags: New tags.
            
        Returns:
            True if updated successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Datadog")
        
        try:
            data: Dict[str, Any] = {}
            
            if query is not None:
                data["query"] = query
            if name is not None:
                data["name"] = name
            if message is not None:
                data["message"] = message
            if tags is not None:
                data["tags"] = tags
            
            response = self._session.put(
                f"{self._base_url}/api/v1/monitor/{monitor_id}",
                json=data,
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            return False
    
    def delete_monitor(self, monitor_id: str) -> bool:
        """Delete a monitor.
        
        Args:
            monitor_id: Monitor ID.
            
        Returns:
            True if deleted successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Datadog")
        
        try:
            response = self._session.delete(
                f"{self._base_url}/api/v1/monitor/{monitor_id}",
                timeout=30
            )
            
            return response.status_code in (200, 204)
        
        except Exception:
            return False
    
    def mute_monitor(self, monitor_id: str, end_ts: Optional[int] = None) -> bool:
        """Mute a monitor.
        
        Args:
            monitor_id: Monitor ID.
            end_ts: Optional end timestamp.
            
        Returns:
            True if muted successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Datadog")
        
        try:
            data: Dict[str, Any] = {"new_group_delay": 0}
            if end_ts:
                data["end"] = end_ts
            
            response = self._session.post(
                f"{self._base_url}/api/v1/monitor/{monitor_id}/mute",
                json=data,
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            return False
    
    def unmute_monitor(self, monitor_id: str) -> bool:
        """Unmute a monitor.
        
        Args:
            monitor_id: Monitor ID.
            
        Returns:
            True if unmuted successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Datadog")
        
        try:
            response = self._session.post(
                f"{self._base_url}/api/v1/monitor/{monitor_id}/unmute",
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            return False
    
    def list_dashboards(self) -> List[Dict[str, Any]]:
        """List all dashboards.
        
        Returns:
            List of dashboard information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Datadog")
        
        try:
            response = self._session.get(
                f"{self._base_url}/api/v1/dashboard",
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json().get("dashboards", [])
            
            return []
        
        except Exception as e:
            raise Exception(f"List dashboards failed: {str(e)}")
    
    def get_dashboard(self, dashboard_id: str) -> Optional[Dict[str, Any]]:
        """Get a dashboard by ID.
        
        Args:
            dashboard_id: Dashboard ID.
            
        Returns:
            Dashboard information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Datadog")
        
        try:
            response = self._session.get(
                f"{self._base_url}/api/v1/dashboard/{dashboard_id}",
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def create_event(
        self,
        title: str,
        text: str,
        tags: Optional[List[str]] = None,
        alert_type: Optional[str] = None
    ) -> Optional[str]:
        """Create an event.
        
        Args:
            title: Event title.
            text: Event text.
            tags: Optional tags.
            alert_type: Optional alert type.
            
        Returns:
            Event ID or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Datadog")
        
        try:
            data: Dict[str, Any] = {"title": title, "text": text}
            
            if tags:
                data["tags"] = tags
            if alert_type:
                data["alert_type"] = alert_type
            
            response = self._session.post(
                f"{self._base_url}/api/v1/events",
                json=data,
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return str(response.json().get("id"))
            
            return None
        
        except Exception as e:
            raise Exception(f"Create event failed: {str(e)}")


class DatadogAction(BaseAction):
    """Datadog action for monitoring and alerting operations.
    
    Supports metrics queries, monitor management, and event creation.
    """
    action_type: str = "datadog"
    display_name: str = "Datadog动作"
    description: str = "Datadog监控指标查询和告警管理操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[DatadogClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Datadog operation.
        
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
            elif operation == "query_metrics":
                return self._query_metrics(params, start_time)
            elif operation == "list_monitors":
                return self._list_monitors(start_time)
            elif operation == "get_monitor":
                return self._get_monitor(params, start_time)
            elif operation == "create_monitor":
                return self._create_monitor(params, start_time)
            elif operation == "update_monitor":
                return self._update_monitor(params, start_time)
            elif operation == "delete_monitor":
                return self._delete_monitor(params, start_time)
            elif operation == "mute_monitor":
                return self._mute_monitor(params, start_time)
            elif operation == "unmute_monitor":
                return self._unmute_monitor(params, start_time)
            elif operation == "list_dashboards":
                return self._list_dashboards(start_time)
            elif operation == "get_dashboard":
                return self._get_dashboard(params, start_time)
            elif operation == "create_event":
                return self._create_event(params, start_time)
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
                message=f"Datadog operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Datadog."""
        api_key = params.get("api_key", "")
        app_key = params.get("app_key", "")
        site = params.get("site", "datadoghq.com")
        
        if not api_key or not app_key:
            return ActionResult(
                success=False,
                message="api_key and app_key are required",
                duration=time.time() - start_time
            )
        
        self._client = DatadogClient(api_key=api_key, app_key=app_key, site=site)
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to Datadog at {site}" if success else "Failed to connect",
            duration=time.time() - start_time
        )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from Datadog."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(
            success=True,
            message="Disconnected from Datadog",
            duration=time.time() - start_time
        )
    
    def _query_metrics(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Query metrics from Datadog."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        query = params.get("query", "")
        from_ts = params.get("from_ts", int(time.time()) - 3600)
        to_ts = params.get("to_ts", int(time.time()))
        
        if not query:
            return ActionResult(success=False, message="query is required", duration=time.time() - start_time)
        
        try:
            result = self._client.query_metrics(query, from_ts, to_ts)
            return ActionResult(
                success=True,
                message="Metrics query completed",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_monitors(self, start_time: float) -> ActionResult:
        """List all monitors."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            monitors = self._client.list_monitors()
            return ActionResult(
                success=True,
                message=f"Found {len(monitors)} monitors",
                data={"monitors": monitors, "count": len(monitors)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_monitor(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a monitor by ID."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        monitor_id = params.get("monitor_id", "")
        if not monitor_id:
            return ActionResult(success=False, message="monitor_id is required", duration=time.time() - start_time)
        
        try:
            monitor = self._client.get_monitor(monitor_id)
            return ActionResult(
                success=monitor is not None,
                message=f"Monitor found: {monitor_id}" if monitor else f"Monitor not found: {monitor_id}",
                data={"monitor": monitor},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_monitor(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new monitor."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        monitor_type = params.get("monitor_type", "")
        query = params.get("query", "")
        name = params.get("name", "")
        message = params.get("message", "")
        tags = params.get("tags")
        
        if not monitor_type or not query or not name:
            return ActionResult(success=False, message="monitor_type, query, and name are required", duration=time.time() - start_time)
        
        try:
            monitor_id = self._client.create_monitor(monitor_type, query, name, message, tags)
            return ActionResult(
                success=monitor_id is not None,
                message=f"Monitor created: {monitor_id}" if monitor_id else "Create monitor failed",
                data={"monitor_id": monitor_id},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _update_monitor(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Update a monitor."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        monitor_id = params.get("monitor_id", "")
        if not monitor_id:
            return ActionResult(success=False, message="monitor_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.update_monitor(
                monitor_id,
                query=params.get("query"),
                name=params.get("name"),
                message=params.get("message"),
                tags=params.get("tags")
            )
            return ActionResult(
                success=success,
                message=f"Monitor updated: {monitor_id}" if success else "Update failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_monitor(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a monitor."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        monitor_id = params.get("monitor_id", "")
        if not monitor_id:
            return ActionResult(success=False, message="monitor_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_monitor(monitor_id)
            return ActionResult(
                success=success,
                message=f"Monitor deleted: {monitor_id}" if success else "Delete failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _mute_monitor(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Mute a monitor."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        monitor_id = params.get("monitor_id", "")
        if not monitor_id:
            return ActionResult(success=False, message="monitor_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.mute_monitor(monitor_id, params.get("end_ts"))
            return ActionResult(
                success=success,
                message=f"Monitor muted: {monitor_id}" if success else "Mute failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _unmute_monitor(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Unmute a monitor."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        monitor_id = params.get("monitor_id", "")
        if not monitor_id:
            return ActionResult(success=False, message="monitor_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.unmute_monitor(monitor_id)
            return ActionResult(
                success=success,
                message=f"Monitor unmuted: {monitor_id}" if success else "Unmute failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_dashboards(self, start_time: float) -> ActionResult:
        """List all dashboards."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            dashboards = self._client.list_dashboards()
            return ActionResult(
                success=True,
                message=f"Found {len(dashboards)} dashboards",
                data={"dashboards": dashboards, "count": len(dashboards)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_dashboard(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a dashboard by ID."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        dashboard_id = params.get("dashboard_id", "")
        if not dashboard_id:
            return ActionResult(success=False, message="dashboard_id is required", duration=time.time() - start_time)
        
        try:
            dashboard = self._client.get_dashboard(dashboard_id)
            return ActionResult(
                success=dashboard is not None,
                message=f"Dashboard found: {dashboard_id}" if dashboard else f"Dashboard not found: {dashboard_id}",
                data={"dashboard": dashboard},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_event(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create an event."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        title = params.get("title", "")
        text = params.get("text", "")
        
        if not title or not text:
            return ActionResult(success=False, message="title and text are required", duration=time.time() - start_time)
        
        try:
            event_id = self._client.create_event(
                title=title,
                text=text,
                tags=params.get("tags"),
                alert_type=params.get("alert_type")
            )
            return ActionResult(
                success=event_id is not None,
                message=f"Event created: {event_id}" if event_id else "Create event failed",
                data={"event_id": event_id},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)

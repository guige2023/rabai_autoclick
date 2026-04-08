"""Prometheus action module for RabAI AutoClick.

Provides Prometheus monitoring operations including
metric queries, alert management, and target discovery.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class MetricFamily:
    """Represents a Prometheus metric family.
    
    Attributes:
        name: Metric name.
        help: Metric help text.
        type: Metric type (gauge, counter, histogram, summary).
        metrics: List of metric samples.
    """
    name: str
    help: str = ""
    type: str = "gauge"
    metrics: List[Dict[str, Any]] = field(default_factory=list)


class PrometheusClient:
    """Prometheus client for monitoring operations.
    
    Provides methods for querying Prometheus metrics,
    managing alerts, and discovering targets.
    """
    
    def __init__(
        self,
        url: str = "http://localhost:9090",
        timeout: int = 30
    ) -> None:
        """Initialize Prometheus client.
        
        Args:
            url: Prometheus server URL.
            timeout: Request timeout in seconds.
        """
        self.url = url.rstrip("/")
        self.timeout = timeout
        self._session: Optional[Any] = None
    
    def connect(self) -> bool:
        """Test connection to Prometheus server.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import requests
        except ImportError:
            raise ImportError("requests is required. Install with: pip install requests")
        
        try:
            self._session = requests.Session()
            response = self._session.get(
                f"{self.url}/-/ready",
                timeout=self.timeout
            )
            return response.status_code == 200
        except Exception:
            self._session = None
            return False
    
    def disconnect(self) -> None:
        """Close the Prometheus session."""
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
    
    def query(self, query: str, time: Optional[str] = None) -> List[Dict[str, Any]]:
        """Execute a PromQL instant query.
        
        Args:
            query: PromQL query string.
            time: Optional evaluation timestamp.
            
        Returns:
            List of query results.
        """
        if not self._session:
            raise RuntimeError("Not connected to Prometheus")
        
        try:
            import requests
            
            params: Dict[str, str] = {"query": query}
            if time:
                params["time"] = time
            
            response = self._session.get(
                f"{self.url}/api/v1/query",
                params=params,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"Query failed: HTTP {response.status_code}")
            
            data = response.json()
            
            if data.get("status") != "success":
                raise Exception(f"Query failed: {data.get('error', 'unknown')}")
            
            return data.get("data", {}).get("result", [])
        
        except Exception as e:
            raise Exception(f"Query failed: {str(e)}")
    
    def query_range(
        self,
        query: str,
        start: str,
        end: str,
        step: str = "15s"
    ) -> Dict[str, Any]:
        """Execute a PromQL range query.
        
        Args:
            query: PromQL query string.
            start: Start timestamp (RFC3339 or unix).
            end: End timestamp (RFC3339 or unix).
            step: Query resolution step.
            
        Returns:
            Range query results with timestamps.
        """
        if not self._session:
            raise RuntimeError("Not connected to Prometheus")
        
        try:
            import requests
            
            params = {
                "query": query,
                "start": start,
                "end": end,
                "step": step
            }
            
            response = self._session.get(
                f"{self.url}/api/v1/query_range",
                params=params,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"Query range failed: HTTP {response.status_code}")
            
            data = response.json()
            
            if data.get("status") != "success":
                raise Exception(f"Query range failed: {data.get('error', 'unknown')}")
            
            return data.get("data", {})
        
        except Exception as e:
            raise Exception(f"Query range failed: {str(e)}")
    
    def get_targets(self, state: Optional[str] = None) -> Dict[str, Any]:
        """Get all scrape targets.
        
        Args:
            state: Optional filter by state ('active', 'dropped').
            
        Returns:
            Target information dictionary.
        """
        if not self._session:
            raise RuntimeError("Not connected to Prometheus")
        
        try:
            import requests
            
            params = {}
            if state:
                params["state"] = state
            
            response = self._session.get(
                f"{self.url}/api/v1/targets",
                params=params,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"Get targets failed: HTTP {response.status_code}")
            
            return response.json()
        
        except Exception as e:
            raise Exception(f"Get targets failed: {str(e)}")
    
    def get_alerts(self) -> List[Dict[str, Any]]:
        """Get all alerts.
        
        Returns:
            List of alert information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Prometheus")
        
        try:
            import requests
            
            response = self._session.get(
                f"{self.url}/api/v1/alerts",
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"Get alerts failed: HTTP {response.status_code}")
            
            data = response.json()
            return data.get("data", {}).get("alerts", [])
        
        except Exception as e:
            raise Exception(f"Get alerts failed: {str(e)}")
    
    def get_rules(self) -> Dict[str, Any]:
        """Get all configured rules.
        
        Returns:
            Rules information including recording and alerting rules.
        """
        if not self._session:
            raise RuntimeError("Not connected to Prometheus")
        
        try:
            import requests
            
            response = self._session.get(
                f"{self.url}/api/v1/rules",
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"Get rules failed: HTTP {response.status_code}")
            
            return response.json()
        
        except Exception as e:
            raise Exception(f"Get rules failed: {str(e)}")
    
    def get_metric_metadata(
        self,
        metric: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get metric metadata.
        
        Args:
            metric: Optional metric name filter.
            
        Returns:
            Metric metadata dictionary.
        """
        if not self._session:
            raise RuntimeError("Not connected to Prometheus")
        
        try:
            import requests
            
            params = {}
            if metric:
                params["metric"] = metric
            
            response = self._session.get(
                f"{self.url}/api/v1/metadata",
                params=params,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"Get metadata failed: HTTP {response.status_code}")
            
            return response.json()
        
        except Exception as e:
            raise Exception(f"Get metadata failed: {str(e)}")
    
    def get_label_values(self, label: str) -> List[str]:
        """Get all values for a label.
        
        Args:
            label: Label name.
            
        Returns:
            List of label values.
        """
        if not self._session:
            raise RuntimeError("Not connected to Prometheus")
        
        try:
            import requests
            
            response = self._session.get(
                f"{self.url}/api/v1/label/{label}/values",
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"Get label values failed: HTTP {response.status_code}")
            
            data = response.json()
            return data.get("data", [])
        
        except Exception as e:
            raise Exception(f"Get label values failed: {str(e)}")
    
    def series(
        self,
        match: List[str],
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get series matching a selector.
        
        Args:
            match: List of metric selectors.
            start: Optional start time.
            end: Optional end time.
            
        Returns:
            List of series.
        """
        if not self._session:
            raise RuntimeError("Not connected to Prometheus")
        
        try:
            import requests
            
            params: Dict[str, Any] = {"match[]": match}
            if start:
                params["start"] = start
            if end:
                params["end"] = end
            
            response = self._session.get(
                f"{self.url}/api/v1/series",
                params=params,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"Series query failed: HTTP {response.status_code}")
            
            data = response.json()
            return data.get("data", [])
        
        except Exception as e:
            raise Exception(f"Series query failed: {str(e)}")
    
    def get_tsdb_stats(self) -> Dict[str, Any]:
        """Get TSDB statistics.
        
        Returns:
            TSDB stats including head stats.
        """
        if not self._session:
            raise RuntimeError("Not connected to Prometheus")
        
        try:
            import requests
            
            response = self._session.get(
                f"{self.url}/api/v1/status/tsdb",
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"Get TSDB stats failed: HTTP {response.status_code}")
            
            return response.json()
        
        except Exception as e:
            raise Exception(f"Get TSDB stats failed: {str(e)}")
    
    def get_wal_info(self) -> Dict[str, Any]:
        """Get WAL (Write-Ahead Log) information.
        
        Returns:
            WAL info dictionary.
        """
        if not self._session:
            raise RuntimeError("Not connected to Prometheus")
        
        try:
            import requests
            
            response = self._session.get(
                f"{self.url}/api/v1/status/walinfo",
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"Get WAL info failed: HTTP {response.status_code}")
            
            return response.json()
        
        except Exception as e:
            raise Exception(f"Get WAL info failed: {str(e)}")
    
    def get_snapshot(self, skip_head: bool = False) -> Dict[str, Any]:
        """Create or get a snapshot.
        
        Args:
            skip_head: Whether to skip the head block.
            
        Returns:
            Snapshot information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Prometheus")
        
        try:
            import requests
            
            params = {"skip_head": skip_head}
            
            response = self._session.post(
                f"{self.url}/api/v1/admin/tsdb/snapshot",
                params=params,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"Snapshot failed: HTTP {response.status_code}")
            
            return response.json()
        
        except Exception as e:
            raise Exception(f"Snapshot failed: {str(e)}")
    
    def delete_series(
        self,
        match: List[str],
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> bool:
        """Delete series matching a selector.
        
        Args:
            match: List of metric selectors.
            start: Optional start time.
            end: Optional end time.
            
        Returns:
            True if deletion successful.
        """
        if not self._session:
            raise RuntimeError("Not connected to Prometheus")
        
        try:
            import requests
            
            params: Dict[str, Any] = {"match[]": match}
            if start:
                params["start"] = start
            if end:
                params["end"] = end
            
            response = self._session.post(
                f"{self.url}/api/v1/admin/tsdb/delete_series",
                params=params,
                timeout=self.timeout
            )
            
            return response.status_code in (200, 204)
        
        except Exception:
            return False
    
    def clean_tombstones(self) -> bool:
        """Clean tombstones from the TSDB.
        
        Returns:
            True if successful.
        """
        if not self._session:
            raise RuntimeError("Not connected to Prometheus")
        
        try:
            import requests
            
            response = self._session.post(
                f"{self.url}/api/v1/admin/tsdb/clean_tombstones",
                timeout=self.timeout
            )
            
            return response.status_code in (200, 204)
        
        except Exception:
            return False


class PrometheusAction(BaseAction):
    """Prometheus action for monitoring operations.
    
    Supports PromQL queries, alert management, and target discovery.
    """
    action_type: str = "prometheus"
    display_name: str = "Prometheus动作"
    description: str = "Prometheus监控指标查询和告警管理"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[PrometheusClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Prometheus operation.
        
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
            elif operation == "query":
                return self._query(params, start_time)
            elif operation == "query_range":
                return self._query_range(params, start_time)
            elif operation == "get_targets":
                return self._get_targets(params, start_time)
            elif operation == "get_alerts":
                return self._get_alerts(start_time)
            elif operation == "get_rules":
                return self._get_rules(start_time)
            elif operation == "get_metadata":
                return self._get_metadata(params, start_time)
            elif operation == "get_label_values":
                return self._get_label_values(params, start_time)
            elif operation == "series":
                return self._series(params, start_time)
            elif operation == "tsdb_stats":
                return self._tsdb_stats(start_time)
            elif operation == "snapshot":
                return self._snapshot(params, start_time)
            elif operation == "delete_series":
                return self._delete_series(params, start_time)
            elif operation == "clean_tombstones":
                return self._clean_tombstones(start_time)
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
                message=f"Prometheus operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Prometheus server."""
        url = params.get("url", "http://localhost:9090")
        
        self._client = PrometheusClient(url=url)
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to Prometheus at {url}" if success else "Failed to connect",
            duration=time.time() - start_time
        )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from Prometheus server."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(
            success=True,
            message="Disconnected from Prometheus",
            duration=time.time() - start_time
        )
    
    def _query(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a PromQL instant query."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        query = params.get("query", "")
        if not query:
            return ActionResult(success=False, message="query is required", duration=time.time() - start_time)
        
        time_val = params.get("time")
        
        try:
            results = self._client.query(query, time_val)
            return ActionResult(
                success=True,
                message=f"Query returned {len(results)} results",
                data={"results": results, "count": len(results)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _query_range(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a PromQL range query."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        query = params.get("query", "")
        start = params.get("start", "")
        end = params.get("end", "now")
        step = params.get("step", "15s")
        
        if not query or not start:
            return ActionResult(success=False, message="query and start are required", duration=time.time() - start_time)
        
        try:
            result = self._client.query_range(query, start, end, step)
            return ActionResult(
                success=True,
                message="Range query executed",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_targets(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get all scrape targets."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        state = params.get("state")
        
        try:
            result = self._client.get_targets(state)
            data = result.get("data", {})
            active = data.get("active_targets", [])
            return ActionResult(
                success=True,
                message=f"Found {len(active)} active targets",
                data={"targets": active, "count": len(active)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_alerts(self, start_time: float) -> ActionResult:
        """Get all alerts."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            alerts = self._client.get_alerts()
            return ActionResult(
                success=True,
                message=f"Found {len(alerts)} alerts",
                data={"alerts": alerts, "count": len(alerts)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_rules(self, start_time: float) -> ActionResult:
        """Get all configured rules."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            result = self._client.get_rules()
            data = result.get("data", {})
            groups = data.get("groups", [])
            return ActionResult(
                success=True,
                message=f"Found {len(groups)} rule groups",
                data={"groups": groups},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_metadata(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get metric metadata."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        metric = params.get("metric")
        
        try:
            result = self._client.get_metric_metadata(metric)
            data = result.get("data", {})
            return ActionResult(
                success=True,
                message=f"Found metadata for {len(data)} metrics",
                data={"metadata": data},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_label_values(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get all values for a label."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        label = params.get("label", "")
        if not label:
            return ActionResult(success=False, message="label is required", duration=time.time() - start_time)
        
        try:
            values = self._client.get_label_values(label)
            return ActionResult(
                success=True,
                message=f"Found {len(values)} values for label {label}",
                data={"label": label, "values": values, "count": len(values)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _series(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get series matching a selector."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        match = params.get("match", [])
        if not match:
            return ActionResult(success=False, message="match list is required", duration=time.time() - start_time)
        
        start = params.get("start")
        end = params.get("end")
        
        try:
            series = self._client.series(match, start, end)
            return ActionResult(
                success=True,
                message=f"Found {len(series)} series",
                data={"series": series, "count": len(series)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _tsdb_stats(self, start_time: float) -> ActionResult:
        """Get TSDB statistics."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            result = self._client.get_tsdb_stats()
            return ActionResult(
                success=True,
                message="TSDB stats retrieved",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _snapshot(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create or get a snapshot."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        skip_head = params.get("skip_head", False)
        
        try:
            result = self._client.get_snapshot(skip_head)
            return ActionResult(
                success=True,
                message="Snapshot created",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_series(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete series matching a selector."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        match = params.get("match", [])
        if not match:
            return ActionResult(success=False, message="match list is required", duration=time.time() - start_time)
        
        start = params.get("start")
        end = params.get("end")
        
        try:
            success = self._client.delete_series(match, start, end)
            return ActionResult(
                success=success,
                message="Series deleted" if success else "Delete failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _clean_tombstones(self, start_time: float) -> ActionResult:
        """Clean tombstones from the TSDB."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            success = self._client.clean_tombstones()
            return ActionResult(
                success=success,
                message="Tombstones cleaned" if success else "Clean tombstones failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)

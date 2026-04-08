"""Airbyte action module for RabAI AutoClick.

Provides Airbyte data integration operations including
connection management, job monitoring, and source/destination control.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class JobInfo:
    """Represents an Airbyte sync job.
    
    Attributes:
        id: Job ID.
        status: Job status.
        created_at: Creation timestamp.
        updated_at: Last update timestamp.
    """
    id: int
    status: str
    created_at: int
    updated_at: int


class AirbyteClient:
    """Airbyte client for data integration operations.
    
    Provides methods for managing Airbyte connections,
    triggering syncs, and monitoring job status.
    """
    
    def __init__(
        self,
        host: str = "http://localhost:8000",
        api_key: str = ""
    ) -> None:
        """Initialize Airbyte client.
        
        Args:
            host: Airbyte server URL.
            api_key: API key for authentication.
        """
        self.host = host.rstrip("/")
        self.api_key = api_key
        self._session: Optional[Any] = None
    
    def connect(self) -> bool:
        """Test connection to Airbyte API.
        
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
                f"{self.host}/api/v1/health",
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            self._session = None
            return False
    
    def disconnect(self) -> None:
        """Close the Airbyte session."""
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
    
    def list_workspaces(self) -> List[Dict[str, Any]]:
        """List all workspaces.
        
        Returns:
            List of workspace information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Airbyte")
        
        try:
            response = self._session.get(
                f"{self.host}/api/v1/workspaces/list",
                timeout=30
            )
            
            if response.status_code in (200, 201):
                data = response.json()
                return data.get("workspaces", [])
            
            return []
        
        except Exception as e:
            raise Exception(f"List workspaces failed: {str(e)}")
    
    def list_sources(self, workspace_id: str) -> List[Dict[str, Any]]:
        """List all sources in a workspace.
        
        Args:
            workspace_id: Workspace ID.
            
        Returns:
            List of source information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Airbyte")
        
        try:
            response = self._session.post(
                f"{self.host}/api/v1/sources/list",
                json={"workspaceId": workspace_id},
                timeout=30
            )
            
            if response.status_code in (200, 201):
                data = response.json()
                return data.get("sources", [])
            
            return []
        
        except Exception as e:
            raise Exception(f"List sources failed: {str(e)}")
    
    def list_destinations(self, workspace_id: str) -> List[Dict[str, Any]]:
        """List all destinations in a workspace.
        
        Args:
            workspace_id: Workspace ID.
            
        Returns:
            List of destination information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Airbyte")
        
        try:
            response = self._session.post(
                f"{self.host}/api/v1/destinations/list",
                json={"workspaceId": workspace_id},
                timeout=30
            )
            
            if response.status_code in (200, 201):
                data = response.json()
                return data.get("destinations", [])
            
            return []
        
        except Exception as e:
            raise Exception(f"List destinations failed: {str(e)}")
    
    def list_connections(self, workspace_id: str) -> List[Dict[str, Any]]:
        """List all connections in a workspace.
        
        Args:
            workspace_id: Workspace ID.
            
        Returns:
            List of connection information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Airbyte")
        
        try:
            response = self._session.post(
                f"{self.host}/api/v1/connections/list",
                json={"workspaceId": workspace_id},
                timeout=30
            )
            
            if response.status_code in (200, 201):
                data = response.json()
                return data.get("connections", [])
            
            return []
        
        except Exception as e:
            raise Exception(f"List connections failed: {str(e)}")
    
    def get_connection(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Get a connection by ID.
        
        Args:
            connection_id: Connection ID.
            
        Returns:
            Connection information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Airbyte")
        
        try:
            response = self._session.post(
                f"{self.host}/api/v1/connections/get",
                json={"connectionId": connection_id},
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def sync_connection(self, connection_id: str) -> int:
        """Trigger a sync for a connection.
        
        Args:
            connection_id: Connection ID.
            
        Returns:
            Job ID.
        """
        if not self._session:
            raise RuntimeError("Not connected to Airbyte")
        
        try:
            response = self._session.post(
                f"{self.host}/api/v1/connections/sync",
                json={"connectionId": connection_id},
                timeout=30
            )
            
            if response.status_code in (200, 201):
                data = response.json()
                return data.get("job", {}).get("id", 0)
            
            return 0
        
        except Exception as e:
            raise Exception(f"Sync connection failed: {str(e)}")
    
    def reset_connection(self, connection_id: str) -> int:
        """Reset data for a connection.
        
        Args:
            connection_id: Connection ID.
            
        Returns:
            Job ID.
        """
        if not self._session:
            raise RuntimeError("Not connected to Airbyte")
        
        try:
            response = self._session.post(
                f"{self.host}/api/v1/connections/reset",
                json={"connectionId": connection_id},
                timeout=30
            )
            
            if response.status_code in (200, 201):
                data = response.json()
                return data.get("job", {}).get("id", 0)
            
            return 0
        
        except Exception as e:
            raise Exception(f"Reset connection failed: {str(e)}")
    
    def get_job_status(self, job_id: int) -> Optional[Dict[str, Any]]:
        """Get the status of a job.
        
        Args:
            job_id: Job ID.
            
        Returns:
            Job information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Airbyte")
        
        try:
            response = self._session.post(
                f"{self.host}/api/v1/jobs/get",
                json={"id": job_id},
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def get_last_sync_job(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Get the last sync job for a connection.
        
        Args:
            connection_id: Connection ID.
            
        Returns:
            Last job information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Airbyte")
        
        try:
            response = self._session.post(
                f"{self.host}/api/v1/jobs/list",
                json={
                    "configId": connection_id,
                    "configType": "sync",
                    "limit": 1
                },
                timeout=30
            )
            
            if response.status_code in (200, 201):
                data = response.json()
                jobs = data.get("jobs", [])
                return jobs[0] if jobs else None
            
            return None
        
        except Exception:
            return None
    
    def cancel_job(self, job_id: int) -> bool:
        """Cancel a running job.
        
        Args:
            job_id: Job ID.
            
        Returns:
            True if cancelled successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Airbyte")
        
        try:
            response = self._session.post(
                f"{self.host}/api/v1/jobs/cancel",
                json={"id": job_id},
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            return False
    
    def create_source(
        self,
        workspace_id: str,
        name: str,
        source_type: str,
        config: Dict[str, Any]
    ) -> Optional[str]:
        """Create a new source.
        
        Args:
            workspace_id: Workspace ID.
            name: Source name.
            source_type: Source type (e.g., 'postgres', 'mysql').
            config: Source configuration.
            
        Returns:
            Source ID or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Airbyte")
        
        try:
            response = self._session.post(
                f"{self.host}/api/v1/sources/create",
                json={
                    "workspaceId": workspace_id,
                    "name": name,
                    "sourceType": source_type.upper(),
                    "connectionConfiguration": config
                },
                timeout=30
            )
            
            if response.status_code in (200, 201):
                data = response.json()
                return data.get("sourceId")
            
            return None
        
        except Exception as e:
            raise Exception(f"Create source failed: {str(e)}")
    
    def create_destination(
        self,
        workspace_id: str,
        name: str,
        destination_type: str,
        config: Dict[str, Any]
    ) -> Optional[str]:
        """Create a new destination.
        
        Args:
            workspace_id: Workspace ID.
            name: Destination name.
            destination_type: Destination type (e.g., 'bigquery', 'snowflake').
            config: Destination configuration.
            
        Returns:
            Destination ID or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Airbyte")
        
        try:
            response = self._session.post(
                f"{self.host}/api/v1/destinations/create",
                json={
                    "workspaceId": workspace_id,
                    "name": name,
                    "destinationType": destination_type.upper(),
                    "connectionConfiguration": config
                },
                timeout=30
            )
            
            if response.status_code in (200, 201):
                data = response.json()
                return data.get("destinationId")
            
            return None
        
        except Exception as e:
            raise Exception(f"Create destination failed: {str(e)}")
    
    def delete_source(self, source_id: str) -> bool:
        """Delete a source.
        
        Args:
            source_id: Source ID.
            
        Returns:
            True if deleted successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Airbyte")
        
        try:
            response = self._session.post(
                f"{self.host}/api/v1/sources/delete",
                json={"sourceId": source_id},
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            return False
    
    def delete_destination(self, destination_id: str) -> bool:
        """Delete a destination.
        
        Args:
            destination_id: Destination ID.
            
        Returns:
            True if deleted successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Airbyte")
        
        try:
            response = self._session.post(
                f"{self.host}/api/v1/destinations/delete",
                json={"destinationId": destination_id},
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            return False


class AirbyteAction(BaseAction):
    """Airbyte action for data integration operations.
    
    Supports connection management, sync triggering, and job monitoring.
    """
    action_type: str = "airbyte"
    display_name: str = "Airbyte动作"
    description: str = "Airbyte数据集成和同步管理操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[AirbyteClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Airbyte operation.
        
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
            elif operation == "list_workspaces":
                return self._list_workspaces(start_time)
            elif operation == "list_sources":
                return self._list_sources(params, start_time)
            elif operation == "list_destinations":
                return self._list_destinations(params, start_time)
            elif operation == "list_connections":
                return self._list_connections(params, start_time)
            elif operation == "get_connection":
                return self._get_connection(params, start_time)
            elif operation == "sync":
                return self._sync(params, start_time)
            elif operation == "reset":
                return self._reset(params, start_time)
            elif operation == "get_job_status":
                return self._get_job_status(params, start_time)
            elif operation == "get_last_sync":
                return self._get_last_sync(params, start_time)
            elif operation == "cancel_job":
                return self._cancel_job(params, start_time)
            elif operation == "create_source":
                return self._create_source(params, start_time)
            elif operation == "create_destination":
                return self._create_destination(params, start_time)
            elif operation == "delete_source":
                return self._delete_source(params, start_time)
            elif operation == "delete_destination":
                return self._delete_destination(params, start_time)
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
                message=f"Airbyte operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Airbyte."""
        host = params.get("host", "http://localhost:8000")
        api_key = params.get("api_key", "")
        
        self._client = AirbyteClient(host=host, api_key=api_key)
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to Airbyte at {host}" if success else "Failed to connect",
            duration=time.time() - start_time
        )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from Airbyte."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(
            success=True,
            message="Disconnected from Airbyte",
            duration=time.time() - start_time
        )
    
    def _list_workspaces(self, start_time: float) -> ActionResult:
        """List all workspaces."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            workspaces = self._client.list_workspaces()
            return ActionResult(
                success=True,
                message=f"Found {len(workspaces)} workspaces",
                data={"workspaces": workspaces, "count": len(workspaces)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_sources(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all sources in a workspace."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        workspace_id = params.get("workspace_id", "")
        if not workspace_id:
            return ActionResult(success=False, message="workspace_id is required", duration=time.time() - start_time)
        
        try:
            sources = self._client.list_sources(workspace_id)
            return ActionResult(
                success=True,
                message=f"Found {len(sources)} sources",
                data={"sources": sources, "count": len(sources)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_destinations(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all destinations in a workspace."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        workspace_id = params.get("workspace_id", "")
        if not workspace_id:
            return ActionResult(success=False, message="workspace_id is required", duration=time.time() - start_time)
        
        try:
            destinations = self._client.list_destinations(workspace_id)
            return ActionResult(
                success=True,
                message=f"Found {len(destinations)} destinations",
                data={"destinations": destinations, "count": len(destinations)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_connections(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all connections in a workspace."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        workspace_id = params.get("workspace_id", "")
        if not workspace_id:
            return ActionResult(success=False, message="workspace_id is required", duration=time.time() - start_time)
        
        try:
            connections = self._client.list_connections(workspace_id)
            return ActionResult(
                success=True,
                message=f"Found {len(connections)} connections",
                data={"connections": connections, "count": len(connections)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_connection(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a connection by ID."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        connection_id = params.get("connection_id", "")
        if not connection_id:
            return ActionResult(success=False, message="connection_id is required", duration=time.time() - start_time)
        
        try:
            connection = self._client.get_connection(connection_id)
            return ActionResult(
                success=connection is not None,
                message=f"Found connection: {connection_id}" if connection else f"Connection not found: {connection_id}",
                data={"connection": connection},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _sync(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Trigger a sync for a connection."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        connection_id = params.get("connection_id", "")
        if not connection_id:
            return ActionResult(success=False, message="connection_id is required", duration=time.time() - start_time)
        
        try:
            job_id = self._client.sync_connection(connection_id)
            return ActionResult(
                success=job_id > 0,
                message=f"Sync started: job {job_id}" if job_id > 0 else "Sync failed",
                data={"job_id": job_id},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _reset(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Reset data for a connection."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        connection_id = params.get("connection_id", "")
        if not connection_id:
            return ActionResult(success=False, message="connection_id is required", duration=time.time() - start_time)
        
        try:
            job_id = self._client.reset_connection(connection_id)
            return ActionResult(
                success=job_id > 0,
                message=f"Reset started: job {job_id}" if job_id > 0 else "Reset failed",
                data={"job_id": job_id},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_job_status(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get the status of a job."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_id = params.get("job_id", 0)
        if not job_id:
            return ActionResult(success=False, message="job_id is required", duration=time.time() - start_time)
        
        try:
            job = self._client.get_job_status(job_id)
            return ActionResult(
                success=job is not None,
                message=f"Job status: {job.get('status', 'unknown')}" if job else f"Job not found: {job_id}",
                data={"job": job},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_last_sync(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get the last sync job for a connection."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        connection_id = params.get("connection_id", "")
        if not connection_id:
            return ActionResult(success=False, message="connection_id is required", duration=time.time() - start_time)
        
        try:
            job = self._client.get_last_sync_job(connection_id)
            return ActionResult(
                success=True,
                message="Last sync job retrieved",
                data={"job": job},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _cancel_job(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Cancel a running job."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_id = params.get("job_id", 0)
        if not job_id:
            return ActionResult(success=False, message="job_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.cancel_job(job_id)
            return ActionResult(
                success=success,
                message=f"Job cancelled: {job_id}" if success else "Cancel failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_source(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new source."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        workspace_id = params.get("workspace_id", "")
        name = params.get("name", "")
        source_type = params.get("source_type", "")
        config = params.get("config", {})
        
        if not workspace_id or not name or not source_type:
            return ActionResult(success=False, message="workspace_id, name, and source_type are required", duration=time.time() - start_time)
        
        try:
            source_id = self._client.create_source(workspace_id, name, source_type, config)
            return ActionResult(
                success=source_id is not None,
                message=f"Source created: {source_id}" if source_id else "Create source failed",
                data={"source_id": source_id},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_destination(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new destination."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        workspace_id = params.get("workspace_id", "")
        name = params.get("name", "")
        destination_type = params.get("destination_type", "")
        config = params.get("config", {})
        
        if not workspace_id or not name or not destination_type:
            return ActionResult(success=False, message="workspace_id, name, and destination_type are required", duration=time.time() - start_time)
        
        try:
            destination_id = self._client.create_destination(workspace_id, name, destination_type, config)
            return ActionResult(
                success=destination_id is not None,
                message=f"Destination created: {destination_id}" if destination_id else "Create destination failed",
                data={"destination_id": destination_id},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_source(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a source."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        source_id = params.get("source_id", "")
        if not source_id:
            return ActionResult(success=False, message="source_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_source(source_id)
            return ActionResult(
                success=success,
                message=f"Source deleted: {source_id}" if success else "Delete source failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_destination(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a destination."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        destination_id = params.get("destination_id", "")
        if not destination_id:
            return ActionResult(success=False, message="destination_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_destination(destination_id)
            return ActionResult(
                success=success,
                message=f"Destination deleted: {destination_id}" if success else "Delete destination failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)

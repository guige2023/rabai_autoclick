"""Consul action module for RabAI AutoClick.

Provides HashiCorp Consul operations for
service discovery, configuration, and mesh management.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ConsulClient:
    """Consul client for service discovery and configuration.
    
    Provides methods for managing Consul services,
    key-value store, and health checks.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8500,
        token: str = "",
        scheme: str = "http",
        datacenter: Optional[str] = None
    ) -> None:
        """Initialize Consul client.
        
        Args:
            host: Consul server host.
            port: Consul server port.
            token: ACL token.
            scheme: HTTP scheme (http or https).
            datacenter: Specific datacenter.
        """
        self.host = host
        self.port = port
        self.token = token
        self.scheme = scheme
        self.datacenter = datacenter
        self.base_url = f"{scheme}://{host}:{port}"
        self._session: Optional[Any] = None
    
    def connect(self) -> bool:
        """Test connection to Consul server.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import requests
        except ImportError:
            raise ImportError("requests is required")
        
        headers = {}
        if self.token:
            headers["X-Consul-Token"] = self.token
        
        try:
            self._session = requests.Session()
            self._session.headers.update(headers)
            
            response = self._session.get(
                f"{self.base_url}/v1/status/leader",
                timeout=30
            )
            
            return response.status_code == 200
        
        except Exception:
            self._session = None
            return False
    
    def disconnect(self) -> None:
        """Close the Consul session."""
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
    
    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None
    ) -> Any:
        """Make a request to Consul.
        
        Args:
            method: HTTP method.
            path: API path.
            params: Query parameters.
            data: Request body data.
            
        Returns:
            Response data.
        """
        if not self._session:
            raise RuntimeError("Not connected to Consul")
        
        url = f"{self.base_url}{path}"
        
        try:
            response = self._session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                timeout=30
            )
            
            if response.status_code in (200, 201):
                if response.text:
                    return response.json()
                return True
            
            return None
        
        except Exception as e:
            raise Exception(f"Consul request failed: {str(e)}")
    
    def get_services(self) -> List[Dict[str, Any]]:
        """Get all services.
        
        Returns:
            List of services.
        """
        try:
            result = self._request("GET", "/v1/agent/services")
            if result:
                return [
                    {"id": k, **v}
                    for k, v in result.items()
                ]
            return []
        except Exception:
            return []
    
    def get_service(self, service: str) -> List[Dict[str, Any]]:
        """Get service instances.
        
        Args:
            service: Service name.
            
        Returns:
            List of service instances.
        """
        try:
            params = {"filter": f'Service == "{service}"'}
            result = self._request("GET", "/v1/health/service/" + service, params)
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def register_service(
        self,
        name: str,
        id: str,
        address: str,
        port: int,
        tags: Optional[List[str]] = None,
        check: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Register a service.
        
        Args:
            name: Service name.
            id: Service ID.
            address: Service address.
            port: Service port.
            tags: Optional service tags.
            check: Optional health check.
            
        Returns:
            True if registration succeeded.
        """
        service_data = {
            "Name": name,
            "ID": id,
            "Address": address,
            "Port": port
        }
        
        if tags:
            service_data["Tags"] = tags
        
        if check:
            service_data["Check"] = check
        
        try:
            result = self._request(
                "PUT",
                "/v1/agent/service/register",
                data=service_data
            )
            return result is True
        except Exception:
            return False
    
    def deregister_service(self, service_id: str) -> bool:
        """Deregister a service.
        
        Args:
            service_id: Service ID to deregister.
            
        Returns:
            True if deregistration succeeded.
        """
        try:
            result = self._request(
                "PUT",
                f"/v1/agent/service/deregister/{service_id}"
            )
            return result is True
        except Exception:
            return False
    
    def get_health_checks(self, service: str) -> List[Dict[str, Any]]:
        """Get health checks for a service.
        
        Args:
            service: Service name.
            
        Returns:
            List of health checks.
        """
        try:
            result = self._request("GET", f"/v1/health/checks/{service}")
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def get_kv(self, key: str, recurse: bool = False) -> Optional[Any]:
        """Get key-value pair(s).
        
        Args:
            key: Key path.
            recurse: Get all keys under path.
            
        Returns:
            Value(s) or None.
        """
        try:
            params = {"recurse": "true"} if recurse else {}
            result = self._request("GET", f"/v1/kv/{key}", params)
            
            if isinstance(result, list):
                return [
                    {
                        "key": item["Key"],
                        "value": item.get("Value"),
                        "flags": item.get("Flags"),
                        "modify_index": item.get("ModifyIndex")
                    }
                    for item in result
                ]
            
            return result
        
        except Exception:
            return None
    
    def put_kv(
        self,
        key: str,
        value: Any,
        flags: Optional[int] = None
    ) -> bool:
        """Put a key-value pair.
        
        Args:
            key: Key path.
            value: Value to store.
            flags: Optional flags.
            
        Returns:
            True if put succeeded.
        """
        import json
        
        try:
            data = {"Value": json.dumps(value) if not isinstance(value, str) else value}
            
            if flags is not None:
                data["Flags"] = flags
            
            result = self._request("PUT", f"/v1/kv/{key}", data=data)
            return result is True
        
        except Exception:
            return False
    
    def delete_kv(self, key: str, recurse: bool = False) -> bool:
        """Delete key-value pair(s).
        
        Args:
            key: Key path.
            recurse: Delete all keys under path.
            
        Returns:
            True if delete succeeded.
        """
        try:
            params = {"recurse": "true"} if recurse else {}
            result = self._request("DELETE", f"/v1/kv/{key}", params)
            return result is True
        except Exception:
            return False
    
    def get_catalog_services(self) -> Dict[str, List[str]]:
        """Get all services from catalog.
        
        Returns:
            Dictionary of service names to tags.
        """
        try:
            result = self._request("GET", "/v1/catalog/services")
            return result if isinstance(result, dict) else {}
        except Exception:
            return {}
    
    def get_catalog_nodes(self) -> List[Dict[str, Any]]:
        """Get all nodes from catalog.
        
        Returns:
            List of nodes.
        """
        try:
            result = self._request("GET", "/v1/catalog/nodes")
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def get_catalog_node(self, node: str) -> Optional[Dict[str, Any]]:
        """Get node information.
        
        Args:
            node: Node name.
            
        Returns:
            Node information or None.
        """
        try:
            result = self._request("GET", f"/v1/catalog/node/{node}")
            return result if isinstance(result, dict) else None
        except Exception:
            return None
    
    def get_acl_tokens(self) -> List[Dict[str, Any]]:
        """Get all ACL tokens.
        
        Returns:
            List of ACL tokens.
        """
        if not self.token:
            return []
        
        try:
            result = self._request("GET", "/v1/acl/tokens")
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def create_acl_token(
        self,
        name: str,
        policies: Optional[List[Dict[str, str]]] = None
    ) -> Optional[str]:
        """Create an ACL token.
        
        Args:
            name: Token name.
            policies: Optional policies.
            
        Returns:
            Token accessor ID or None.
        """
        if not self.token:
            return None
        
        try:
            data = {"Name": name}
            
            if policies:
                data["Policies"] = policies
            
            result = self._request("POST", "/v1/acl/token", data=data)
            
            if isinstance(result, dict):
                return result.get("AccessorID")
            
            return None
        
        except Exception:
            return None
    
    def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session information.
        
        Args:
            session_id: Session ID.
            
        Returns:
            Session information or None.
        """
        try:
            result = self._request("GET", f"/v1/session/info/{session_id}")
            if isinstance(result, list) and len(result) > 0:
                return result[0]
            return None
        except Exception:
            return None
    
    def create_session(
        self,
        name: str,
        node: Optional[str] = None,
        ttl: Optional[str] = None
    ) -> Optional[str]:
        """Create a session.
        
        Args:
            name: Session name.
            node: Node to associate.
            ttl: Session TTL.
            
        Returns:
            Session ID or None.
        """
        try:
            data: Dict[str, Any] = {"Name": name}
            
            if node:
                data["Node"] = node
            
            if ttl:
                data["TTL"] = ttl
            
            result = self._request("POST", "/v1/session/create", data=data)
            
            if isinstance(result, dict):
                return result.get("ID")
            
            return None
        
        except Exception:
            return None
    
    def delete_session(self, session_id: str) -> bool:
        """Delete a session.
        
        Args:
            session_id: Session ID to delete.
            
        Returns:
            True if deletion succeeded.
        """
        try:
            result = self._request("PUT", f"/v1/session/destroy/{session_id}")
            return result is True
        except Exception:
            return False
    
    def get_coordinate_nodes(self) -> List[Dict[str, Any]]:
        """Get network coordinates for all nodes.
        
        Returns:
            List of node coordinates.
        """
        try:
            result = self._request("GET", "/v1/coordinate/nodes")
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def get_config_entries(
        self,
        kind: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get configuration entries.
        
        Args:
            kind: Optional entry kind filter.
            
        Returns:
            List of configuration entries.
        """
        try:
            params = {"kind": kind} if kind else {}
            result = self._request("GET", "/v1/config/entries", params)
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def write_config_entry(self, entry: Dict[str, Any]) -> bool:
        """Write a configuration entry.
        
        Args:
            entry: Configuration entry.
            
        Returns:
            True if write succeeded.
        """
        try:
            result = self._request("PUT", "/v1/config/entries", data=entry)
            return result is True
        except Exception:
            return False


class ConsulAction(BaseAction):
    """Consul action for service discovery and configuration.
    
    Supports service registration, KV store, and health checks.
    """
    action_type: str = "consul"
    display_name: str = "Consul动作"
    description: str = "HashiCorp Consul服务发现和配置管理"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[ConsulClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Consul operation.
        
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
            elif operation == "get_services":
                return self._get_services(start_time)
            elif operation == "get_service":
                return self._get_service(params, start_time)
            elif operation == "register_service":
                return self._register_service(params, start_time)
            elif operation == "deregister_service":
                return self._deregister_service(params, start_time)
            elif operation == "get_health_checks":
                return self._get_health_checks(params, start_time)
            elif operation == "get_kv":
                return self._get_kv(params, start_time)
            elif operation == "put_kv":
                return self._put_kv(params, start_time)
            elif operation == "delete_kv":
                return self._delete_kv(params, start_time)
            elif operation == "get_catalog_services":
                return self._get_catalog_services(start_time)
            elif operation == "get_catalog_nodes":
                return self._get_catalog_nodes(start_time)
            elif operation == "get_catalog_node":
                return self._get_catalog_node(params, start_time)
            elif operation == "get_acl_tokens":
                return self._get_acl_tokens(start_time)
            elif operation == "create_acl_token":
                return self._create_acl_token(params, start_time)
            elif operation == "get_session":
                return self._get_session(params, start_time)
            elif operation == "create_session":
                return self._create_session(params, start_time)
            elif operation == "delete_session":
                return self._delete_session(params, start_time)
            elif operation == "get_coordinates":
                return self._get_coordinates(start_time)
            elif operation == "get_config_entries":
                return self._get_config_entries(params, start_time)
            elif operation == "write_config_entry":
                return self._write_config_entry(params, start_time)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}", duration=time.time() - start_time)
        
        except ImportError as e:
            return ActionResult(success=False, message=f"Import error: {str(e)}", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=f"Consul operation failed: {str(e)}", duration=time.time() - start_time)
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Consul."""
        host = params.get("host", "localhost")
        port = params.get("port", 8500)
        token = params.get("token", "")
        scheme = params.get("scheme", "http")
        datacenter = params.get("datacenter")
        
        self._client = ConsulClient(host=host, port=port, token=token, scheme=scheme, datacenter=datacenter)
        success = self._client.connect()
        
        return ActionResult(success=success, message=f"Connected to Consul at {host}:{port}" if success else "Failed to connect", duration=time.time() - start_time)
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from Consul."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(success=True, message="Disconnected from Consul", duration=time.time() - start_time)
    
    def _get_services(self, start_time: float) -> ActionResult:
        """Get all services."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            services = self._client.get_services()
            return ActionResult(success=True, message=f"Found {len(services)} services", data={"services": services}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_service(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get service instances."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        service = params.get("service", "")
        if not service:
            return ActionResult(success=False, message="service is required", duration=time.time() - start_time)
        
        try:
            instances = self._client.get_service(service)
            return ActionResult(success=True, message=f"Found {len(instances)} instances", data={"instances": instances}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _register_service(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Register a service."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        name = params.get("name", "")
        id = params.get("id", "")
        address = params.get("address", "")
        port = params.get("port", 0)
        
        if not name or not id or not address or not port:
            return ActionResult(success=False, message="name, id, address, and port are required", duration=time.time() - start_time)
        
        try:
            success = self._client.register_service(
                name=name, id=id, address=address, port=port,
                tags=params.get("tags"), check=params.get("check")
            )
            return ActionResult(success=success, message=f"Service registered: {name}" if success else "Registration failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _deregister_service(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Deregister a service."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        service_id = params.get("service_id", "")
        if not service_id:
            return ActionResult(success=False, message="service_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.deregister_service(service_id)
            return ActionResult(success=success, message=f"Service deregistered: {service_id}" if success else "Deregistration failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_health_checks(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get health checks for a service."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        service = params.get("service", "")
        if not service:
            return ActionResult(success=False, message="service is required", duration=time.time() - start_time)
        
        try:
            checks = self._client.get_health_checks(service)
            return ActionResult(success=True, message=f"Found {len(checks)} checks", data={"checks": checks}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_kv(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get KV pair(s)."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        key = params.get("key", "")
        if not key:
            return ActionResult(success=False, message="key is required", duration=time.time() - start_time)
        
        try:
            result = self._client.get_kv(key, params.get("recurse", False))
            return ActionResult(success=result is not None, message="KV retrieved" if result else "Key not found", data={"result": result}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _put_kv(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Put KV pair."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        key = params.get("key", "")
        value = params.get("value")
        
        if not key or value is None:
            return ActionResult(success=False, message="key and value are required", duration=time.time() - start_time)
        
        try:
            success = self._client.put_kv(key, value, params.get("flags"))
            return ActionResult(success=success, message=f"KV stored: {key}" if success else "Store failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_kv(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete KV pair(s)."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        key = params.get("key", "")
        if not key:
            return ActionResult(success=False, message="key is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_kv(key, params.get("recurse", False))
            return ActionResult(success=success, message=f"KV deleted: {key}" if success else "Delete failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_catalog_services(self, start_time: float) -> ActionResult:
        """Get catalog services."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            services = self._client.get_catalog_services()
            return ActionResult(success=True, message=f"Found {len(services)} services", data={"services": services}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_catalog_nodes(self, start_time: float) -> ActionResult:
        """Get catalog nodes."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            nodes = self._client.get_catalog_nodes()
            return ActionResult(success=True, message=f"Found {len(nodes)} nodes", data={"nodes": nodes}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_catalog_node(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get catalog node."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        node = params.get("node", "")
        if not node:
            return ActionResult(success=False, message="node is required", duration=time.time() - start_time)
        
        try:
            info = self._client.get_catalog_node(node)
            return ActionResult(success=info is not None, message=f"Node info retrieved: {node}", data={"node": info}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_acl_tokens(self, start_time: float) -> ActionResult:
        """Get ACL tokens."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            tokens = self._client.get_acl_tokens()
            return ActionResult(success=True, message=f"Found {len(tokens)} tokens", data={"tokens": tokens}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_acl_token(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create ACL token."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            token_id = self._client.create_acl_token(name, params.get("policies"))
            return ActionResult(success=token_id is not None, message="Token created" if token_id else "Create failed", data={"token_id": token_id}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_session(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get session info."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        session_id = params.get("session_id", "")
        if not session_id:
            return ActionResult(success=False, message="session_id is required", duration=time.time() - start_time)
        
        try:
            info = self._client.get_session_info(session_id)
            return ActionResult(success=info is not None, message="Session info retrieved", data={"session": info}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_session(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a session."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            session_id = self._client.create_session(name, params.get("node"), params.get("ttl"))
            return ActionResult(success=session_id is not None, message="Session created" if session_id else "Create failed", data={"session_id": session_id}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_session(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a session."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        session_id = params.get("session_id", "")
        if not session_id:
            return ActionResult(success=False, message="session_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_session(session_id)
            return ActionResult(success=success, message=f"Session deleted: {session_id}" if success else "Delete failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_coordinates(self, start_time: float) -> ActionResult:
        """Get node coordinates."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            coords = self._client.get_coordinate_nodes()
            return ActionResult(success=True, message=f"Found {len(coords)} coordinates", data={"coordinates": coords}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_config_entries(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get config entries."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            entries = self._client.get_config_entries(params.get("kind"))
            return ActionResult(success=True, message=f"Found {len(entries)} entries", data={"entries": entries}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _write_config_entry(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Write config entry."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        entry = params.get("entry", {})
        if not entry:
            return ActionResult(success=False, message="entry is required", duration=time.time() - start_time)
        
        try:
            success = self._client.write_config_entry(entry)
            return ActionResult(success=success, message="Config entry written" if success else "Write failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)

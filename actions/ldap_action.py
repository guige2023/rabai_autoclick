"""LDAP action module for RabAI AutoClick.

Provides LDAP directory operations including user authentication,
search operations, and directory management.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LDAPClient:
    """LDAP client wrapper for directory operations.
    
    Provides methods for connecting to LDAP directories,
    binding, searching, and modifying entries.
    """
    
    def __init__(
        self,
        server: str = "ldap://localhost",
        port: int = 389,
        use_ssl: bool = False,
        timeout: int = 30
    ) -> None:
        """Initialize LDAP client.
        
        Args:
            server: LDAP server URL (ldap:// or ldaps://).
            port: LDAP port (389 for LDAP, 636 for LDAPS).
            use_ssl: Whether to use SSL/TLS.
            timeout: Connection timeout in seconds.
        """
        self.server = server
        self.port = port
        self.use_ssl = use_ssl
        self.timeout = timeout
        self._conn: Optional[Any] = None
        self._bound: bool = False
        self._server_url = server if "://" in server else f"{'ldaps' if use_ssl else 'ldap'}://{server}:{port}"
    
    def connect(self) -> bool:
        """Establish connection to LDAP server.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import ldap
        except ImportError:
            raise ImportError(
                "python-ldap is required for LDAP support. Install with: pip install python-ldap"
            )
        
        try:
            ldap.set_option(ldap.OPT_NETWORK_TIMEOUT, self.timeout)
            ldap.set_option(ldap.OPT_REFERRALS, 0)
            
            if self.use_ssl:
                ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_NEVER)
                self._conn = ldap.initialize(self._server_url)
                self._conn.start_tls_s()
            else:
                self._conn = ldap.initialize(self._server_url)
            
            self._conn.protocol_version = ldap.VERSION3
            
            return True
        
        except Exception:
            self._conn = None
            return False
    
    def bind(
        self,
        dn: str = "",
        password: str = "",
        method: str = "simple"
    ) -> bool:
        """Bind (authenticate) to the LDAP server.
        
        Args:
            dn: Distinguished Name to bind as.
            password: Password for authentication.
            method: Authentication method ('simple', 'SASL').
            
        Returns:
            True if bind successful, False otherwise.
        """
        if not self._conn:
            raise RuntimeError("Not connected to LDAP server")
        
        try:
            if method == "simple" and dn:
                self._conn.simple_bind_s(dn, password)
            else:
                self._conn.simple_bind_s()
            
            self._bound = True
            return True
        
        except Exception:
            self._bound = False
            return False
    
    def unbind(self) -> bool:
        """Unbind from the LDAP server.
        
        Returns:
            True if successful.
        """
        if self._conn:
            try:
                self._conn.unbind_s()
            except Exception:
                pass
            self._conn = None
            self._bound = False
        return True
    
    @property
    def is_bound(self) -> bool:
        """Check if currently bound."""
        return self._bound
    
    def search(
        self,
        base_dn: str,
        scope: str = "subtree",
        filter_str: str = "(objectClass=*)",
        attrs: Optional[List[str]] = None,
        size_limit: int = 0
    ) -> List[Dict[str, Any]]:
        """Search the LDAP directory.
        
        Args:
            base_dn: Base DN to search from.
            scope: Search scope ('base', 'onelevel', 'subtree').
            filter_str: LDAP filter string.
            attrs: List of attributes to return.
            size_limit: Maximum number of results (0 for unlimited).
            
        Returns:
            List of result dictionaries.
        """
        if not self._conn or not self._bound:
            raise RuntimeError("Not bound to LDAP server")
        
        import ldap
        
        scope_map = {
            "base": ldap.SCOPE_BASE,
            "onelevel": ldap.SCOPE_ONELEVEL,
            "subtree": ldap.SCOPE_SUBTREE
        }
        
        scope_enum = scope_map.get(scope, ldap.SCOPE_SUBTREE)
        
        try:
            results = self._conn.search_ext_s(
                base_dn,
                scope_enum,
                filter_str,
                attrs or None,
                sizelimit=size_limit
            )
            
            parsed_results: List[Dict[str, Any]] = []
            
            for dn, entry in results:
                if dn is None:
                    continue
                
                attrs_dict: Dict[str, List[str]] = {}
                if entry:
                    for attr, values in entry.items():
                        if values:
                            attr_name = attr.decode() if isinstance(attr, bytes) else attr
                            attr_values = [
                                v.decode() if isinstance(v, bytes) else v
                                for v in values
                            ]
                            attrs_dict[attr_name] = attr_values
                
                parsed_results.append({
                    "dn": dn,
                    "attributes": attrs_dict
                })
            
            return parsed_results
        
        except Exception as e:
            raise Exception(f"Search failed: {str(e)}")
    
    def get_user(
        self,
        username: str,
        search_base: str,
        username_attr: str = "sAMAccountName"
    ) -> Optional[Dict[str, Any]]:
        """Get a user by username.
        
        Args:
            username: Username to search for.
            search_base: Base DN for the search.
            username_attr: Attribute containing the username.
            
        Returns:
            User dictionary or None if not found.
        """
        filter_str = f"({username_attr}={username})"
        
        results = self.search(
            base_dn=search_base,
            filter_str=filter_str
        )
        
        return results[0] if results else None
    
    def authenticate_user(
        self,
        username: str,
        password: str,
        search_base: str,
        username_attr: str = "sAMAccountName"
    ) -> bool:
        """Authenticate a user against the directory.
        
        Args:
            username: Username to authenticate.
            password: Password to verify.
            search_base: Base DN for user search.
            username_attr: Attribute containing the username.
            
        Returns:
            True if authentication successful.
        """
        user = self.get_user(username, search_base, username_attr)
        
        if not user:
            return False
        
        user_dn = user["dn"]
        
        try:
            import ldap
            
            test_conn = ldap.initialize(self._server_url)
            test_conn.protocol_version = ldap.VERSION3
            test_conn.simple_bind_s(user_dn, password)
            test_conn.unbind_s()
            
            return True
        
        except Exception:
            return False
    
    def add(
        self,
        dn: str,
        attrs: Dict[str, List[str]]
    ) -> bool:
        """Add a new entry to the directory.
        
        Args:
            dn: Distinguished Name of the new entry.
            attrs: Attributes for the new entry.
            
        Returns:
            True if added successfully.
        """
        if not self._conn or not self._bound:
            raise RuntimeError("Not bound to LDAP server")
        
        try:
            encoded_attrs: Dict[bytes, List[bytes]]] = {}
            for key, values in attrs.items():
                k = key.encode() if isinstance(key, str) else key
                v = [
                    val.encode() if isinstance(val, str) else val
                    for val in values
                ]
                encoded_attrs[k] = v
            
            self._conn.add_s(dn.encode() if isinstance(dn, str) else dn, ldap.modlist.addModlist(encoded_attrs))
            return True
        
        except Exception as e:
            raise Exception(f"Add failed: {str(e)}")
    
    def modify(
        self,
        dn: str,
        modifications: List[tuple]
    ) -> bool:
        """Modify an existing entry.
        
        Args:
            dn: Distinguished Name of the entry.
            modifications: List of (operation, attribute, values) tuples.
                          Operations: ldap.MOD_ADD, ldap.MOD_DELETE, ldap.MOD_REPLACE.
            
        Returns:
            True if modified successfully.
        """
        if not self._conn or not self._bound:
            raise RuntimeError("Not bound to LDAP server")
        
        import ldap
        
        try:
            self._conn.modify_s(dn, modifications)
            return True
        
        except Exception as e:
            raise Exception(f"Modify failed: {str(e)}")
    
    def delete(self, dn: str) -> bool:
        """Delete an entry from the directory.
        
        Args:
            dn: Distinguished Name of the entry to delete.
            
        Returns:
            True if deleted successfully.
        """
        if not self._conn or not self._bound:
            raise RuntimeError("Not bound to LDAP server")
        
        try:
            self._conn.delete_s(dn)
            return True
        
        except Exception as e:
            raise Exception(f"Delete failed: {str(e)}")
    
    def rename(
        self,
        dn: str,
        new_rdn: str,
        delete_old_rdn: bool = True
    ) -> bool:
        """Rename (move) an entry.
        
        Args:
            dn: Current Distinguished Name.
            new_rdn: New Relative Distinguished Name.
            delete_old_rdn: Whether to delete the old RDN.
            
        Returns:
            True if renamed successfully.
        """
        if not self._conn or not self._bound:
            raise RuntimeError("Not bound to LDAP server")
        
        try:
            self._conn.rename_s(dn, new_rdn, delete_old_rdn=delete_old_rdn)
            return True
        
        except Exception as e:
            raise Exception(f"Rename failed: {str(e)}")


class LDAPAcion(BaseAction):
    """LDAP action for directory operations.
    
    Supports authentication, search, and directory management.
    """
    action_type: str = "ldap"
    display_name: str = "LDAP动作"
    description: str = "LDAP目录操作，支持认证、搜索和目录管理"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[LDAPClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute LDAP operation.
        
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
            elif operation == "bind":
                return self._bind(params, start_time)
            elif operation == "unbind":
                return self._unbind(start_time)
            elif operation == "search":
                return self._search(params, start_time)
            elif operation == "get_user":
                return self._get_user(params, start_time)
            elif operation == "authenticate":
                return self._authenticate(params, start_time)
            elif operation == "add":
                return self._add(params, start_time)
            elif operation == "modify":
                return self._modify(params, start_time)
            elif operation == "delete":
                return self._delete(params, start_time)
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
                message=f"LDAP operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to LDAP server."""
        server = params.get("server", "ldap://localhost")
        port = params.get("port", 389)
        use_ssl = params.get("use_ssl", False)
        
        self._client = LDAPClient(
            server=server,
            port=port,
            use_ssl=use_ssl
        )
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to {server}:{port}" if success else "Failed to connect",
            duration=time.time() - start_time
        )
    
    def _bind(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Bind to LDAP server."""
        if not self._client:
            return ActionResult(
                success=False,
                message="Not connected to LDAP server",
                duration=time.time() - start_time
            )
        
        dn = params.get("dn", "")
        password = params.get("password", "")
        method = params.get("method", "simple")
        
        success = self._client.bind(dn=dn, password=password, method=method)
        
        return ActionResult(
            success=success,
            message="Bind successful" if success else "Bind failed",
            duration=time.time() - start_time
        )
    
    def _unbind(self, start_time: float) -> ActionResult:
        """Unbind from LDAP server."""
        if self._client:
            self._client.unbind()
            self._client = None
        
        return ActionResult(
            success=True,
            message="Unbound from LDAP server",
            duration=time.time() - start_time
        )
    
    def _search(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Search LDAP directory."""
        if not self._client or not self._client.is_bound:
            return ActionResult(
                success=False,
                message="Not bound to LDAP server",
                duration=time.time() - start_time
            )
        
        base_dn = params.get("base_dn", "")
        filter_str = params.get("filter", "(objectClass=*)")
        scope = params.get("scope", "subtree")
        attrs = params.get("attrs")
        
        if not base_dn:
            return ActionResult(
                success=False,
                message="base_dn is required",
                duration=time.time() - start_time
            )
        
        try:
            results = self._client.search(
                base_dn=base_dn,
                scope=scope,
                filter_str=filter_str,
                attrs=attrs
            )
            
            return ActionResult(
                success=True,
                message=f"Found {len(results)} entries",
                data={"results": results, "count": len(results)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Search failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _get_user(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a user by username."""
        if not self._client or not self._client.is_bound:
            return ActionResult(
                success=False,
                message="Not bound to LDAP server",
                duration=time.time() - start_time
            )
        
        username = params.get("username", "")
        search_base = params.get("search_base", "")
        username_attr = params.get("username_attr", "sAMAccountName")
        
        if not username or not search_base:
            return ActionResult(
                success=False,
                message="username and search_base are required",
                duration=time.time() - start_time
            )
        
        try:
            user = self._client.get_user(
                username=username,
                search_base=search_base,
                username_attr=username_attr
            )
            
            return ActionResult(
                success=user is not None,
                message=f"Found user: {username}" if user else f"User not found: {username}",
                data={"user": user, "found": user is not None},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Get user failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _authenticate(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Authenticate a user."""
        if not self._client or not self._client.is_bound:
            return ActionResult(
                success=False,
                message="Not bound to LDAP server",
                duration=time.time() - start_time
            )
        
        username = params.get("username", "")
        password = params.get("password", "")
        search_base = params.get("search_base", "")
        username_attr = params.get("username_attr", "sAMAccountName")
        
        if not username or not password or not search_base:
            return ActionResult(
                success=False,
                message="username, password, and search_base are required",
                duration=time.time() - start_time
            )
        
        try:
            success = self._client.authenticate_user(
                username=username,
                password=password,
                search_base=search_base,
                username_attr=username_attr
            )
            
            return ActionResult(
                success=True,
                message="Authentication successful" if success else "Authentication failed",
                data={"authenticated": success, "username": username},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Authentication failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _add(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Add a new LDAP entry."""
        if not self._client or not self._client.is_bound:
            return ActionResult(
                success=False,
                message="Not bound to LDAP server",
                duration=time.time() - start_time
            )
        
        dn = params.get("dn", "")
        attrs = params.get("attrs", {})
        
        if not dn or not attrs:
            return ActionResult(
                success=False,
                message="dn and attrs are required",
                duration=time.time() - start_time
            )
        
        try:
            self._client.add(dn=dn, attrs=attrs)
            
            return ActionResult(
                success=True,
                message=f"Added entry: {dn}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Add failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _modify(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Modify an LDAP entry."""
        if not self._client or not self._client.is_bound:
            return ActionResult(
                success=False,
                message="Not bound to LDAP server",
                duration=time.time() - start_time
            )
        
        dn = params.get("dn", "")
        modifications = params.get("modifications", [])
        
        if not dn or not modifications:
            return ActionResult(
                success=False,
                message="dn and modifications are required",
                duration=time.time() - start_time
            )
        
        try:
            self._client.modify(dn=dn, modifications=modifications)
            
            return ActionResult(
                success=True,
                message=f"Modified entry: {dn}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Modify failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _delete(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete an LDAP entry."""
        if not self._client or not self._client.is_bound:
            return ActionResult(
                success=False,
                message="Not bound to LDAP server",
                duration=time.time() - start_time
            )
        
        dn = params.get("dn", "")
        
        if not dn:
            return ActionResult(
                success=False,
                message="dn is required",
                duration=time.time() - start_time
            )
        
        try:
            self._client.delete(dn=dn)
            
            return ActionResult(
                success=True,
                message=f"Deleted entry: {dn}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Delete failed: {str(e)}",
                duration=time.time() - start_time
            )

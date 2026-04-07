"""
LDAP directory operations actions.
"""
from __future__ import annotations

from typing import Dict, Any, Optional, List


class LDAPClient:
    """LDAP directory client."""

    def __init__(
        self,
        server: str,
        port: int = 389,
        use_ssl: bool = False,
        bind_dn: Optional[str] = None,
        bind_password: Optional[str] = None
    ):
        """
        Initialize LDAP client.

        Args:
            server: LDAP server hostname.
            port: LDAP port.
            use_ssl: Use LDAPS.
            bind_dn: Bind DN for authentication.
            bind_password: Bind password.
        """
        self.server = server
        self.port = port
        self.use_ssl = use_ssl
        self.bind_dn = bind_dn
        self.bind_password = bind_password
        self._conn = None

    def connect(self) -> Dict[str, Any]:
        """
        Connect to LDAP server.

        Returns:
            Connection result.
        """
        try:
            import ldap
        except ImportError:
            return {
                'success': False,
                'error': 'python-ldap not installed. Install with: pip install python-ldap',
            }

        protocol = 'ldaps' if self.use_ssl else 'ldap'
        uri = f'{protocol}://{self.server}:{self.port}'

        try:
            self._conn = ldap.initialize(uri)
            self._conn.protocol_version = ldap.VERSION3
            self._conn.set_option(ldap.OPT_REFERRALS, 0)

            if self.bind_dn and self.bind_password:
                self._conn.simple_bind_s(self.bind_dn, self.bind_password)

            return {'success': True, 'uri': uri}
        except ldap.LDAPError as e:
            return {'success': False, 'error': str(e)}

    def disconnect(self) -> None:
        """Disconnect from LDAP server."""
        if self._conn:
            self._conn.unbind_s()
            self._conn = None

    def search(
        self,
        base_dn: str,
        search_filter: str,
        attributes: Optional[List[str]] = None,
        scope: str = 'subtree'
    ) -> List[Dict[str, Any]]:
        """
        Search LDAP directory.

        Args:
            base_dn: Base DN for search.
            search_filter: LDAP search filter.
            attributes: Attributes to retrieve.
            scope: Search scope ('base', 'onelevel', 'subtree').

        Returns:
            List of search results.
        """
        if not self._conn:
            return []

        try:
            import ldap

            scope_map = {
                'base': ldap.SCOPE_BASE,
                'onelevel': ldap.SCOPE_ONELEVEL,
                'subtree': ldap.SCOPE_SUBTREE,
            }

            results = self._conn.search_s(
                base_dn,
                scope_map.get(scope, ldap.SCOPE_SUBTREE),
                search_filter,
                attributes
            )

            entries = []
            for dn, attrs in results:
                entry: Dict[str, Any] = {'dn': dn}
                for key, values in attrs.items():
                    entry[key] = values[0].decode('utf-8') if values else None
                entries.append(entry)

            return entries
        except ldap.LDAPError:
            return []

    def get_user(self, username: str, base_dn: str) -> Optional[Dict[str, Any]]:
        """
        Get a user by username.

        Args:
            username: Username to search.
            base_dn: Base DN for search.

        Returns:
            User entry or None.
        """
        search_filter = f'(sAMAccountName={username})'
        results = self.search(base_dn, search_filter)
        return results[0] if results else None

    def get_users(self, base_dn: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get all users from directory.

        Args:
            base_dn: Base DN for users.
            limit: Maximum number of results.

        Returns:
            List of user entries.
        """
        search_filter = '(objectClass=user)'
        return self.search(base_dn, search_filter)[:limit]

    def get_groups(self, base_dn: str) -> List[Dict[str, Any]]:
        """
        Get all groups from directory.

        Args:
            base_dn: Base DN for groups.

        Returns:
            List of group entries.
        """
        search_filter = '(objectClass=group)'
        return self.search(base_dn, search_filter)

    def get_group_members(
        self,
        group_dn: str,
        base_dn: str
    ) -> List[Dict[str, Any]]:
        """
        Get members of a group.

        Args:
            group_dn: Group DN.
            base_dn: Base DN for search.

        Returns:
            List of member entries.
        """
        search_filter = f'(memberOf={group_dn})'
        return self.search(base_dn, search_filter)

    def authenticate(
        self,
        user_dn: str,
        password: str
    ) -> bool:
        """
        Authenticate a user.

        Args:
            user_dn: User DN.
            password: Password.

        Returns:
            True if authentication successful.
        """
        try:
            import ldap
            conn = ldap.initialize(f'ldap://{self.server}:{self.port}')
            conn.simple_bind_s(user_dn, password)
            conn.unbind_s()
            return True
        except ldap.LDAPError:
            return False


def connect_ldap(
    server: str,
    bind_dn: str,
    bind_password: str,
    port: int = 389,
    use_ssl: bool = False
) -> Dict[str, Any]:
    """
    Connect to LDAP server.

    Args:
        server: LDAP server.
        bind_dn: Bind DN.
        bind_password: Bind password.
        port: LDAP port.
        use_ssl: Use SSL.

    Returns:
        Connection result.
    """
    client = LDAPClient(
        server=server,
        port=port,
        use_ssl=use_ssl,
        bind_dn=bind_dn,
        bind_password=bind_password
    )
    return client.connect()


def search_directory(
    server: str,
    base_dn: str,
    search_filter: str,
    bind_dn: Optional[str] = None,
    bind_password: Optional[str] = None,
    attributes: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    Search LDAP directory.

    Args:
        server: LDAP server.
        base_dn: Base DN.
        search_filter: LDAP filter.
        bind_dn: Bind DN.
        bind_password: Bind password.
        attributes: Attributes to retrieve.

    Returns:
        Search results.
    """
    client = LDAPClient(
        server=server,
        bind_dn=bind_dn,
        bind_password=bind_password
    )

    result = client.connect()
    if not result['success']:
        return []

    try:
        return client.search(base_dn, search_filter, attributes)
    finally:
        client.disconnect()


def find_user_by_email(
    server: str,
    email: str,
    base_dn: str,
    bind_dn: Optional[str] = None,
    bind_password: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Find a user by email address.

    Args:
        server: LDAP server.
        email: Email address.
        base_dn: Base DN.
        bind_dn: Bind DN.
        bind_password: Bind password.

    Returns:
        User entry or None.
    """
    search_filter = f'(mail={email})'

    results = search_directory(
        server=server,
        base_dn=base_dn,
        search_filter=search_filter,
        bind_dn=bind_dn,
        bind_password=bind_password
    )

    return results[0] if results else None


def find_users_by_department(
    server: str,
    department: str,
    base_dn: str,
    bind_dn: Optional[str] = None,
    bind_password: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Find all users in a department.

    Args:
        server: LDAP server.
        department: Department name.
        base_dn: Base DN.
        bind_dn: Bind DN.
        bind_password: Bind password.

    Returns:
        List of user entries.
    """
    search_filter = f'(department={department})'

    return search_directory(
        server=server,
        base_dn=base_dn,
        search_filter=search_filter,
        bind_dn=bind_dn,
        bind_password=bind_password
    )


def get_user_groups(
    server: str,
    username: str,
    base_dn: str,
    bind_dn: Optional[str] = None,
    bind_password: Optional[str] = None
) -> List[str]:
    """
    Get group memberships for a user.

    Args:
        server: LDAP server.
        username: Username.
        base_dn: Base DN.
        bind_dn: Bind DN.
        bind_password: Bind password.

    Returns:
        List of group DNs.
    """
    user = find_user_by_username(
        server=server,
        username=username,
        base_dn=base_dn,
        bind_dn=bind_dn,
        bind_password=bind_password
    )

    if not user:
        return []

    return [user.get('memberOf', [])] if isinstance(user.get('memberOf'), list) else []


def find_user_by_username(
    server: str,
    username: str,
    base_dn: str,
    bind_dn: Optional[str] = None,
    bind_password: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Find a user by username.

    Args:
        server: LDAP server.
        username: Username.
        base_dn: Base DN.
        bind_dn: Bind DN.
        bind_password: Bind password.

    Returns:
        User entry or None.
    """
    try:
        import ldap
    except ImportError:
        return None

    client = LDAPClient(
        server=server,
        bind_dn=bind_dn,
        bind_password=bind_password
    )

    result = client.connect()
    if not result['success']:
        return None

    try:
        entries = client.search(
            base_dn,
            f'(sAMAccountName={username})'
        )
        return entries[0] if entries else None
    finally:
        client.disconnect()


def test_ldap_connection(
    server: str,
    port: int = 389,
    bind_dn: Optional[str] = None,
    bind_password: Optional[str] = None
) -> Dict[str, Any]:
    """
    Test LDAP connection.

    Args:
        server: LDAP server.
        port: LDAP port.
        bind_dn: Bind DN.
        bind_password: Bind password.

    Returns:
        Test result.
    """
    try:
        import ldap
    except ImportError:
        return {
            'success': False,
            'error': 'python-ldap not installed',
        }

    uri = f'ldap://{server}:{port}'

    try:
        conn = ldap.initialize(uri)
        conn.protocol_version = ldap.VERSION3
        conn.set_option(ldap.OPT_REFERRALS, 0)

        if bind_dn and bind_password:
            conn.simple_bind_s(bind_dn, bind_password)
        else:
            conn.simple_bind_s()

        conn.unbind_s()

        return {
            'success': True,
            'server': server,
            'port': port,
        }
    except ldap.LDAPError as e:
        return {
            'success': False,
            'error': str(e),
            'server': server,
            'port': port,
        }

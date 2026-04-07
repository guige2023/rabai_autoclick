"""
Grafana dashboard and alerting actions.
"""
from __future__ import annotations

import requests
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin


class GrafanaClient:
    """Grafana API client."""

    def __init__(
        self,
        url: str = 'http://localhost:3000',
        api_key: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 10
    ):
        """
        Initialize Grafana client.

        Args:
            url: Grafana server URL.
            api_key: API key for authentication.
            username: Username for basic auth.
            password: Password for basic auth.
            timeout: Request timeout in seconds.
        """
        self.url = url.rstrip('/')
        self.timeout = timeout

        if api_key:
            self.auth = ('Authorization', f'Bearer {api_key}')
            self.session = requests.Session()
            self.session.headers.update({'Authorization': f'Bearer {api_key}'})
        elif username and password:
            self.auth = (username, password)
            self.session = requests.Session()
            self.session.auth = (username, password)
        else:
            self.auth = None
            self.session = requests.Session()

    def _request(
        self,
        method: str,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make an HTTP request to Grafana."""
        url = urljoin(self.url, path)

        try:
            response = self.session.request(
                method,
                url,
                json=data,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {'error': str(e)}

    def get_dashboards(self) -> List[Dict[str, Any]]:
        """
        Get all dashboards.

        Returns:
            List of dashboard summaries.
        """
        result = self._request('GET', '/api/search')
        if isinstance(result, dict) and 'error' in result:
            return []
        return result

    def get_dashboard(self, uid: str) -> Optional[Dict[str, Any]]:
        """
        Get a dashboard by UID.

        Args:
            uid: Dashboard UID.

        Returns:
            Dashboard data or None.
        """
        return self._request('GET', f'/api/dashboards/uid/{uid}')

    def create_dashboard(
        self,
        dashboard: Dict[str, Any],
        folder_id: int = 0,
        overwrite: bool = True
    ) -> Dict[str, Any]:
        """
        Create or update a dashboard.

        Args:
            dashboard: Dashboard JSON structure.
            folder_id: Folder ID to create in.
            overwrite: Overwrite existing dashboard.

        Returns:
            Creation result.
        """
        payload = {
            'dashboard': dashboard,
            'folderId': folder_id,
            'overwrite': overwrite,
        }
        return self._request('POST', '/api/dashboards/db', data=payload)

    def delete_dashboard(self, uid: str) -> Dict[str, Any]:
        """
        Delete a dashboard.

        Args:
            uid: Dashboard UID.

        Returns:
            Deletion result.
        """
        return self._request('DELETE', f'/api/dashboards/uid/{uid}')

    def get_alerts(self, dashboard_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get alerts.

        Args:
            dashboard_id: Filter by dashboard ID.

        Returns:
            List of alerts.
        """
        if dashboard_id:
            return self._request('GET', f'/api/alerts?dashboardId={dashboard_id}')
        return self._request('GET', '/api/alerts')

    def get_alert_groups(self) -> List[Dict[str, Any]]:
        """
        Get alert groups.

        Returns:
            List of alert groups.
        """
        return self._request('GET', '/api/alert-groups')

    def get_alerting_stats(self) -> Dict[str, Any]:
        """
        Get alerting statistics.

        Returns:
            Alerting stats.
        """
        return self._request('GET', '/api/alerting/stats')

    def create_alert(
        self,
        dashboard_id: int,
        panel_id: int,
        name: str,
        message: str,
        alert_rule: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create a new alert rule.

        Args:
            dashboard_id: Dashboard ID.
            panel_id: Panel ID.
            name: Alert name.
            message: Alert message.
            alert_rule: Alert rule configuration.

        Returns:
            Creation result.
        """
        payload = {
            'dashboardId': dashboard_id,
            'panelId': panel_id,
            'name': name,
            'message': message,
            'alertRule': alert_rule,
        }
        return self._request('POST', '/api/alerts', data=payload)

    def pause_alert(self, alert_id: int, pause: bool = True) -> Dict[str, Any]:
        """
        Pause or resume an alert.

        Args:
            alert_id: Alert ID.
            pause: True to pause, False to resume.

        Returns:
            Result.
        """
        payload = {'pause': pause}
        return self._request('POST', f'/api/alerts/{alert_id}/pause', data=payload)

    def get_folders(self) -> List[Dict[str, Any]]:
        """
        Get all folders.

        Returns:
            List of folders.
        """
        return self._request('GET', '/api/folders')

    def create_folder(self, title: str) -> Dict[str, Any]:
        """
        Create a folder.

        Args:
            title: Folder title.

        Returns:
            Creation result.
        """
        return self._request('POST', '/api/folders', data={'title': title})

    def get_datasource(self, uid: str) -> Optional[Dict[str, Any]]:
        """
        Get a datasource by UID.

        Args:
            uid: Datasource UID.

        Returns:
            Datasource data.
        """
        return self._request('GET', f'/api/datasources/uid/{uid}')

    def get_datasources(self) -> List[Dict[str, Any]]:
        """
        Get all datasources.

        Returns:
            List of datasources.
        """
        return self._request('GET', '/api/datasources')

    def create_datasource(self, datasource: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a datasource.

        Args:
            datasource: Datasource configuration.

        Returns:
            Creation result.
        """
        return self._request('POST', '/api/datasources', data=datasource)

    def get_organization(self) -> Dict[str, Any]:
        """
        Get current organization.

        Returns:
            Organization data.
        """
        return self._request('GET', '/api/org')

    def get_health(self) -> Dict[str, Any]:
        """
        Check Grafana health.

        Returns:
            Health status.
        """
        return self._request('GET', '/api/health')


def get_dashboard_widgets(grafana_url: str, uid: str, api_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Get all widgets (panels) from a dashboard.

    Args:
        grafana_url: Grafana server URL.
        uid: Dashboard UID.
        api_key: Grafana API key.

    Returns:
        List of panel information.
    """
    client = GrafanaClient(url=grafana_url, api_key=api_key)
    dashboard = client.get_dashboard(uid)

    if not dashboard or 'dashboard' not in dashboard:
        return []

    panels = dashboard.get('dashboard', {}).get('panels', [])
    return [
        {
            'id': p.get('id'),
            'title': p.get('title'),
            'type': p.get('type'),
            'grid_pos': p.get('gridPos'),
        }
        for p in panels
    ]


def query_grafana_datasource(
    grafana_url: str,
    datasource_uid: str,
    query: Dict[str, Any],
    api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Query a Grafana datasource directly.

    Args:
        grafana_url: Grafana server URL.
        datasource_uid: Datasource UID.
        query: Query JSON.
        api_key: Grafana API key.

    Returns:
        Query result.
    """
    client = GrafanaClient(url=grafana_url, api_key=api_key)

    payload = {
        'queries': [query],
        'from': 'now-1h',
        'to': 'now',
    }

    return client._request(
        'POST',
        f'/api/ds/query',
        data=payload,
        params={'uid': datasource_uid}
    )


def export_dashboard_json(grafana_url: str, uid: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Export a dashboard as JSON.

    Args:
        grafana_url: Grafana server URL.
        uid: Dashboard UID.
        api_key: Grafana API key.

    Returns:
        Dashboard JSON.
    """
    client = GrafanaClient(url=grafana_url, api_key=api_key)
    return client.get_dashboard(uid)


def get_alert_states(grafana_url: str, api_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Get all current alert states.

    Args:
        grafana_url: Grafana server URL.
        api_key: Grafana API key.

    Returns:
        List of alert states.
    """
    client = GrafanaClient(url=grafana_url, api_key=api_key)
    result = client._request('GET', '/api/alerts/states')
    if isinstance(result, list):
        return result
    return []


def check_grafana_version(grafana_url: str) -> Optional[str]:
    """
    Get Grafana version.

    Args:
        grafana_url: Grafana server URL.

    Returns:
        Version string or None.
    """
    client = GrafanaClient(url=grafana_url)
    health = client.get_health()
    return health.get('version')

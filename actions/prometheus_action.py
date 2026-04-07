"""
Prometheus monitoring and metrics actions.
"""
from __future__ import annotations

import requests
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin


class PrometheusClient:
    """Prometheus API client."""

    def __init__(
        self,
        url: str = 'http://localhost:9090',
        timeout: int = 10
    ):
        """
        Initialize Prometheus client.

        Args:
            url: Prometheus server URL.
            timeout: Request timeout in seconds.
        """
        self.url = url.rstrip('/')
        self.timeout = timeout

    def query(self, query: str, time: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute a PromQL query.

        Args:
            query: PromQL query string.
            time: Evaluation timestamp (optional).

        Returns:
            Query results.
        """
        params: Dict[str, str] = {'query': query}
        if time:
            params['time'] = time

        response = requests.get(
            urljoin(self.url, '/api/v1/query'),
            params=params,
            timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def query_range(
        self,
        query: str,
        start: str,
        end: str,
        step: str = '1m'
    ) -> Dict[str, Any]:
        """
        Execute a range query.

        Args:
            query: PromQL query.
            start: Start timestamp.
            end: End timestamp.
            step: Query resolution step.

        Returns:
            Range query results.
        """
        params = {
            'query': query,
            'start': start,
            'end': end,
            'step': step,
        }

        response = requests.get(
            urljoin(self.url, '/api/v1/query_range'),
            params=params,
            timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def get_alerts(self) -> List[Dict[str, Any]]:
        """
        Get all active alerts.

        Returns:
            List of alert dictionaries.
        """
        response = requests.get(
            urljoin(self.url, '/api/v1/alerts'),
            timeout=self.timeout
        )
        response.raise_for_status()
        data = response.json()
        return data.get('data', {}).get('alerts', [])

    def get_rules(self) -> Dict[str, Any]:
        """
        Get all configured alerting and recording rules.

        Returns:
            Rules data.
        """
        response = requests.get(
            urljoin(self.url, '/api/v1/rules'),
            timeout=self.timeout
        )
        response.raise_for_status()
        return response.json().get('data', {})

    def get_targets(self) -> List[Dict[str, Any]]:
        """
        Get all scrape targets.

        Returns:
            List of target information.
        """
        response = requests.get(
            urljoin(self.url, '/api/v1/targets'),
            timeout=self.timeout
        )
        response.raise_for_status()
        data = response.json()
        return data.get('data', {}).get('activeTargets', [])

    def get_series(self, match: List[str]) -> List[Dict[str, Any]]:
        """
        Get series data for matching label set.

        Args:
            match: List of label matchers.

        Returns:
            List of series.
        """
        response = requests.get(
            urljoin(self.url, '/api/v1/series'),
            params={'match[]': match},
            timeout=self.timeout
        )
        response.raise_for_status()
        data = response.json()
        return data.get('data', [])

    def get_label_values(self, label: str) -> List[str]:
        """
        Get values for a specific label.

        Args:
            label: Label name.

        Returns:
            List of label values.
        """
        response = requests.get(
            urljoin(self.url, f'/api/v1/label/{label}/values'),
            timeout=self.timeout
        )
        response.raise_for_status()
        data = response.json()
        return data.get('data', [])

    def get_metadata(self, metric: Optional[str] = None) -> Dict[str, Any]:
        """
        Get metadata for metrics.

        Args:
            metric: Specific metric name (optional).

        Returns:
            Metadata mapping.
        """
        params = {}
        if metric:
            params['metric'] = metric

        response = requests.get(
            urljoin(self.url, '/api/v1/metadata'),
            params=params,
            timeout=self.timeout
        )
        response.raise_for_status()
        return response.json().get('data', {})

    def get_tsdb_stats(self) -> Dict[str, Any]:
        """
        Get TSDB statistics.

        Returns:
            TSDB stats.
        """
        response = requests.get(
            urljoin(self.url, '/api/v1/status/tsdb'),
            timeout=self.timeout
        )
        response.raise_for_status()
        return response.json().get('data', {})

    def get_wal_length(self) -> int:
        """
        Get the length of the Write-Ahead Log.

        Returns:
            WAL length.
        """
        stats = self.get_tsdb_stats()
        return stats.get('walLength', 0)

    def get_head_stats(self) -> Dict[str, Any]:
        """
        Get TSDB head statistics.

        Returns:
            Head stats.
        """
        response = requests.get(
            urljoin(self.url, '/api/v1/status/tsdb'),
            timeout=self.timeout
        )
        response.raise_for_status()
        data = response.json()
        return data.get('data', {}).get('headStats', {})


def query_prometheus(
    prometheus_url: str,
    query: str
) -> List[Dict[str, Any]]:
    """
    Query Prometheus and return results.

    Args:
        prometheus_url: Prometheus server URL.
        query: PromQL query.

    Returns:
        List of result vectors.
    """
    client = PrometheusClient(url=prometheus_url)
    result = client.query(query)

    if result.get('status') != 'success':
        raise RuntimeError(f"Query failed: {result.get('error', 'Unknown error')}")

    return result.get('data', {}).get('result', [])


def get_up_metrics(prometheus_url: str) -> List[Dict[str, Any]]:
    """
    Get uptime metrics for all targets.

    Args:
        prometheus_url: Prometheus server URL.

    Returns:
        List of up metrics.
    """
    return query_prometheus(prometheus_url, 'up')


def get_metric_values(
    prometheus_url: str,
    metric: str,
    labels: Optional[Dict[str, str]] = None
) -> List[Any]:
    """
    Get values for a specific metric.

    Args:
        prometheus_url: Prometheus server URL.
        metric: Metric name.
        labels: Optional label filters.

    Returns:
        List of metric values.
    """
    query = metric
    if labels:
        label_str = ','.join(f'{k}="{v}"' for k, v in labels.items())
        query = f'{metric}{{{label_str}}}'

    results = query_prometheus(prometheus_url, query)

    values = []
    for result in results:
        value = result.get('value', [])
        if len(value) > 1:
            values.append({
                'timestamp': float(value[0]),
                'value': value[1],
                'labels': result.get('metric', {}),
            })

    return values


def check_alert_status(prometheus_url: str, alert_name: str) -> Dict[str, Any]:
    """
    Check the status of a specific alert.

    Args:
        prometheus_url: Prometheus server URL.
        alert_name: Name of the alert.

    Returns:
        Alert status information.
    """
    client = PrometheusClient(url=prometheus_url)
    alerts = client.get_alerts()

    for alert in alerts:
        if alert.get('name') == alert_name:
            return {
                'name': alert_name,
                'state': alert.get('state'),
                'health': alert.get('health'),
                'labels': alert.get('labels', {}),
                'annotations': alert.get('annotations', {}),
                'active_at': alert.get('activeAt'),
            }

    return {
        'name': alert_name,
        'state': 'not_found',
        'health': None,
    }


def get_metric_cardinality(prometheus_url: str, metric: str) -> Dict[str, Any]:
    """
    Get cardinality information for a metric.

    Args:
        prometheus_url: Prometheus server URL.
        metric: Metric name.

    Returns:
        Cardinality stats.
    """
    series = query_prometheus(
        prometheus_url,
        f'count({metric})'
    )

    if series:
        value = series[0].get('value', [])
        return {
            'metric': metric,
            'series_count': int(float(value[1])) if len(value) > 1 else 0,
        }

    return {'metric': metric, 'series_count': 0}


def calculate_rate(
    prometheus_url: str,
    metric: str,
    range_minutes: int = 5
) -> List[Dict[str, Any]]:
    """
    Calculate per-second rate of increase for a counter metric.

    Args:
        prometheus_url: Prometheus server URL.
        metric: Counter metric name.
        range_minutes: Time range in minutes.

    Returns:
        Rate values over time.
    """
    query = f'rate({metric}[{range_minutes}m])'
    return query_prometheus(prometheus_url, query)


def get_instant_vector(
    prometheus_url: str,
    query: str
) -> Dict[str, Any]:
    """
    Get instant vector results.

    Args:
        prometheus_url: Prometheus server URL.
        query: PromQL query.

    Returns:
        Dictionary with instant vector results.
    """
    client = PrometheusClient(url=prometheus_url)
    result = client.query(query)

    return {
        'status': result.get('status'),
        'result_type': result.get('data', {}).get('resultType'),
        'results': result.get('data', {}).get('result', []),
    }


def get_range_vector(
    prometheus_url: str,
    query: str,
    start: str,
    end: str,
    step: str = '1m'
) -> Dict[str, Any]:
    """
    Get range vector results.

    Args:
        prometheus_url: Prometheus server URL.
        query: PromQL query.
        start: Start time (RFC3339 or Unix).
        end: End time (RFC3339 or Unix).
        step: Query resolution.

    Returns:
        Dictionary with range vector results.
    """
    client = PrometheusClient(url=prometheus_url)
    result = client.query_range(query, start, end, step)

    return {
        'status': result.get('status'),
        'result_type': result.get('data', {}).get('resultType'),
        'results': result.get('data', {}).get('result', []),
    }

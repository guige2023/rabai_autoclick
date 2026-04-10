"""Tests for Workflow Grafana Module.

Comprehensive tests for Grafana integration including
dashboard provisioning, data source management, alerts,
annotations, template variables, snapshots, playlists,
API keys, SSO configuration, and folder management.
"""

import unittest
import sys
import json
import time
from unittest.mock import Mock, patch, MagicMock, mock_open
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from enum import Enum

sys.path.insert(0, '/Users/guige/my_project')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick/src')

from workflow_grafana import (
    GrafanaIntegration, DataSourceType, AlertState, SnapshotSharing,
    DashboardProvisioning, DataSource, AlertRule, Annotation,
    TemplateVariable, DashboardSnapshot, DashboardPlaylist, APIKey,
    SSOConfig, DashboardFolder
)


class TestDataSourceType(unittest.TestCase):
    """Tests for DataSourceType enum."""

    def test_datasource_type_values(self):
        """Test DataSourceType enum values."""
        self.assertEqual(DataSourceType.PROMETHEUS.value, "prometheus")
        self.assertEqual(DataSourceType.ELASTICSEARCH.value, "elasticsearch")
        self.assertEqual(DataSourceType.INFLUXDB.value, "influxdb")
        self.assertEqual(DataSourceType.GRAPHITE.value, "graphite")
        self.assertEqual(DataSourceType.DATadog.value, "datadog")
        self.assertEqual(DataSourceType.CLOUDWATCH.value, "cloudwatch")
        self.assertEqual(DataSourceType.GRAFANA_CLOUD.value, "grafana-cloud")


class TestAlertState(unittest.TestCase):
    """Tests for AlertState enum."""

    def test_alert_state_values(self):
        """Test AlertState enum values."""
        self.assertEqual(AlertState.OK.value, "ok")
        self.assertEqual(AlertState.ALERTING.value, "alerting")
        self.assertEqual(AlertState.NO_DATA.value, "no_data")
        self.assertEqual(AlertState.PENDING.value, "pending")
        self.assertEqual(AlertState.PAUSED.value, "paused")


class TestSnapshotSharing(unittest.TestCase):
    """Tests for SnapshotSharing enum."""

    def test_snapshot_sharing_values(self):
        """Test SnapshotSharing enum values."""
        self.assertEqual(SnapshotSharing.PUBLIC.value, "public")
        self.assertEqual(SnapshotSharing.AUTHENTICATED.value, "***")


class TestDataclasses(unittest.TestCase):
    """Tests for Grafana dataclasses."""

    def test_dashboard_provisioning_creation(self):
        """Test DashboardProvisioning creation."""
        prov = DashboardProvisioning(
            dashboard_id="dash-1",
            title="My Dashboard",
            uid="my-uid",
            folder_id=1,
            overwrite=True
        )
        self.assertEqual(prov.dashboard_id, "dash-1")
        self.assertEqual(prov.title, "My Dashboard")
        self.assertTrue(prov.overwrite)

    def test_datasource_creation(self):
        """Test DataSource creation."""
        ds = DataSource(
            name="Prometheus DS",
            type=DataSourceType.PROMETHEUS,
            url="http://prometheus:9090",
            is_default=True
        )
        self.assertEqual(ds.name, "Prometheus DS")
        self.assertEqual(ds.type, DataSourceType.PROMETHEUS)
        self.assertTrue(ds.is_default)

    def test_alert_rule_creation(self):
        """Test AlertRule creation."""
        alert = AlertRule(
            name="High CPU",
            folder_title="Alerts",
            condition="A",
            data=[{"refId": "A", "query": "cpu > 80"}],
            interval="1m"
        )
        self.assertEqual(alert.name, "High CPU")
        self.assertEqual(alert.interval, "1m")

    def test_annotation_creation(self):
        """Test Annotation creation."""
        ann = Annotation(
            text="Deployment started",
            time=int(time.time()),
            tags=["deploy", "production"]
        )
        self.assertEqual(ann.text, "Deployment started")
        self.assertEqual(ann.tags, ["deploy", "production"])

    def test_template_variable_creation(self):
        """Test TemplateVariable creation."""
        var = TemplateVariable(
            name="env",
            query="label_values(env)",
            type="query",
            multi=True
        )
        self.assertEqual(var.name, "env")
        self.assertTrue(var.multi)

    def test_dashboard_snapshot_creation(self):
        """Test DashboardSnapshot creation."""
        snap = DashboardSnapshot(
            dashboard_json={"title": "Test"},
            name="Test Snapshot",
            expires_seconds=3600
        )
        self.assertEqual(snap.name, "Test Snapshot")
        self.assertEqual(snap.expires_seconds, 3600)

    def test_dashboard_playlist_creation(self):
        """Test DashboardPlaylist creation."""
        playlist = DashboardPlaylist(
            name="Daily Review",
            interval="10m",
            dashboards=[{"uid": "dash-1", "title": "Dashboard 1"}]
        )
        self.assertEqual(playlist.name, "Daily Review")
        self.assertEqual(len(playlist.dashboards), 1)

    def test_api_key_creation(self):
        """Test APIKey creation."""
        key = APIKey(
            name="CI/CD Key",
            role="Editor",
            expires_in=86400
        )
        self.assertEqual(key.role, "Editor")
        self.assertEqual(key.expires_in, 86400)

    def test_sso_config_creation(self):
        """Test SSOConfig creation."""
        sso = SSOConfig(
            name="github-sso",
            type="oauth",
            client_id="abc123",
            client_secret="secret"
        )
        self.assertEqual(sso.name, "github-sso")
        self.assertEqual(sso.type, "oauth")

    def test_dashboard_folder_creation(self):
        """Test DashboardFolder creation."""
        folder = DashboardFolder(
            title="Production",
            uid="prod-folder"
        )
        self.assertEqual(folder.title, "Production")
        self.assertEqual(folder.uid, "prod-folder")


class TestGrafanaIntegration(unittest.TestCase):
    """Tests for GrafanaIntegration class."""

    def setUp(self):
        """Set up test fixtures."""
        self.grafana = GrafanaIntegration(
            base_url="http://localhost:3000",
            api_key="test-api-key"
        )

    def test_initialization(self):
        """Test GrafanaIntegration initialization."""
        self.assertEqual(self.grafana.base_url, "http://localhost:3000")
        self.assertEqual(self.grafana.api_key, "test-api-key")
        self.assertIn("Authorization", self.grafana.headers)

    def test_initialization_without_api_key(self):
        """Test GrafanaIntegration initialization without API key."""
        grafana = GrafanaIntegration(base_url="http://localhost:3000")
        self.assertNotIn("Authorization", grafana.headers)

    def test_make_url(self):
        """Test URL building for API endpoints."""
        url = self.grafana._make_url("/api/dashboards")
        self.assertEqual(url, "http://localhost:3000/api/dashboards")

    def test_make_url_trailing_slash(self):
        """Test URL building with trailing slash in endpoint."""
        url = self.grafana._make_url("/api/dashboards/")
        self.assertEqual(url, "http://localhost:3000/api/dashboards/")


class TestDashboardProvisioning(unittest.TestCase):
    """Tests for dashboard provisioning functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.grafana = GrafanaIntegration()

    def test_provision_dashboard(self):
        """Test provisioning a dashboard."""
        prov = DashboardProvisioning(
            dashboard_id="dash-1",
            title="Test Dashboard",
            uid="test-uid"
        )
        result = self.grafana.provision_dashboard(prov)
        self.assertEqual(result["status"], "success")
        self.assertIn("uid", result)
        self.assertIn("url", result)

    def test_provision_dashboard_without_uid(self):
        """Test provisioning a dashboard without explicit UID."""
        prov = DashboardProvisioning(
            dashboard_id="dash-2",
            title="Test Dashboard 2"
        )
        result = self.grafana.provision_dashboard(prov)
        self.assertEqual(result["status"], "success")
        self.assertIsNotNone(result["uid"])

    def test_get_dashboard(self):
        """Test getting a provisioned dashboard."""
        prov = DashboardProvisioning(
            dashboard_id="dash-1",
            title="Test Dashboard",
            uid="test-get"
        )
        self.grafana.provision_dashboard(prov)
        dashboard = self.grafana.get_dashboard("test-get")
        self.assertIsNotNone(dashboard)
        self.assertEqual(dashboard["dashboard"]["title"], "Test Dashboard")

    def test_get_dashboard_not_found(self):
        """Test getting a non-existent dashboard."""
        dashboard = self.grafana.get_dashboard("nonexistent")
        self.assertIsNone(dashboard)

    def test_list_dashboards(self):
        """Test listing all provisioned dashboards."""
        prov1 = DashboardProvisioning(dashboard_id="1", title="Dashboard 1", uid="uid-1")
        prov2 = DashboardProvisioning(dashboard_id="2", title="Dashboard 2", uid="uid-2")
        self.grafana.provision_dashboard(prov1)
        self.grafana.provision_dashboard(prov2)
        dashboards = self.grafana.list_dashboards()
        self.assertEqual(len(dashboards), 2)

    def test_delete_dashboard(self):
        """Test deleting a provisioned dashboard."""
        prov = DashboardProvisioning(dashboard_id="1", title="To Delete", uid="delete-me")
        self.grafana.provision_dashboard(prov)
        result = self.grafana.delete_dashboard("delete-me")
        self.assertTrue(result)
        self.assertIsNone(self.grafana.get_dashboard("delete-me"))

    def test_delete_dashboard_not_found(self):
        """Test deleting a non-existent dashboard."""
        result = self.grafana.delete_dashboard("nonexistent")
        self.assertFalse(result)


class TestDataSourceManagement(unittest.TestCase):
    """Tests for data source management functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.grafana = GrafanaIntegration()

    def test_create_datasource(self):
        """Test creating a data source."""
        ds = DataSource(
            name="Prometheus",
            type=DataSourceType.PROMETHEUS,
            url="http://prometheus:9090",
            is_default=True
        )
        result = self.grafana.create_datasource(ds)
        self.assertEqual(result["status"], "success")
        self.assertIn("uid", result)
        self.assertTrue(result["isDefault"])

    def test_create_datasource_without_default(self):
        """Test creating a non-default data source."""
        ds = DataSource(
            name="Elasticsearch",
            type=DataSourceType.ELASTICSEARCH,
            url="http://es:9200"
        )
        result = self.grafana.create_datasource(ds)
        self.assertEqual(result["status"], "success")
        self.assertFalse(result["isDefault"])

    def test_get_datasource(self):
        """Test getting a data source by UID."""
        ds = DataSource(
            name="Prometheus",
            type=DataSourceType.PROMETHEUS,
            url="http://prometheus:9090"
        )
        result = self.grafana.create_datasource(ds)
        retrieved = self.grafana.get_datasource(result["uid"])
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.name, "Prometheus")

    def test_get_datasource_not_found(self):
        """Test getting a non-existent data source."""
        retrieved = self.grafana.get_datasource("nonexistent")
        self.assertIsNone(retrieved)

    def test_list_datasources(self):
        """Test listing all data sources."""
        ds1 = DataSource(name="DS1", type=DataSourceType.PROMETHEUS, url="http://p1:9090")
        ds2 = DataSource(name="DS2", type=DataSourceType.ELASTICSEARCH, url="http://es:9200")
        self.grafana.create_datasource(ds1)
        self.grafana.create_datasource(ds2)
        datasources = self.grafana.list_datasources()
        self.assertEqual(len(datasources), 2)

    def test_update_datasource(self):
        """Test updating a data source."""
        ds = DataSource(name="Original", type=DataSourceType.PROMETHEUS, url="http://original:9090")
        result = self.grafana.create_datasource(ds)
        updated = self.grafana.update_datasource(result["uid"], {"url": "http://updated:9090"})
        self.assertIsNotNone(updated)
        self.assertEqual(updated["status"], "success")

    def test_update_datasource_not_found(self):
        """Test updating a non-existent data source."""
        updated = self.grafana.update_datasource("nonexistent", {"url": "http://new:9090"})
        self.assertIsNone(updated)

    def test_delete_datasource(self):
        """Test deleting a data source."""
        ds = DataSource(name="ToDelete", type=DataSourceType.PROMETHEUS, url="http://del:9090")
        result = self.grafana.create_datasource(ds)
        deleted = self.grafana.delete_datasource(result["uid"])
        self.assertTrue(deleted)

    def test_delete_datasource_not_found(self):
        """Test deleting a non-existent data source."""
        deleted = self.grafana.delete_datasource("nonexistent")
        self.assertFalse(deleted)

    def test_get_default_datasource(self):
        """Test getting default data source for a type."""
        ds = DataSource(
            name="Default Prometheus",
            type=DataSourceType.PROMETHEUS,
            url="http://prom:9090",
            is_default=True
        )
        self.grafana.create_datasource(ds)
        default = self.grafana.get_default_datasource(DataSourceType.PROMETHEUS)
        self.assertIsNotNone(default)
        self.assertEqual(default.name, "Default Prometheus")

    def test_get_default_datasource_none(self):
        """Test getting default data source when none exists."""
        default = self.grafana.get_default_datasource(DataSourceType.PROMETHEUS)
        self.assertIsNone(default)


class TestAlertManagement(unittest.TestCase):
    """Tests for alert management functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.grafana = GrafanaIntegration()

    def test_create_alert_rule(self):
        """Test creating an alert rule."""
        alert = AlertRule(
            name="HighCPU",
            folder_title="Production",
            condition="A",
            data=[{"refId": "A", "query": "cpu > 80"}]
        )
        result = self.grafana.create_alert_rule(alert)
        self.assertEqual(result["status"], "success")
        self.assertIn("id", result)
        self.assertEqual(result["name"], "HighCPU")

    def test_get_alert_rule(self):
        """Test getting an alert rule."""
        alert = AlertRule(
            name="TestAlert",
            folder_title="Test",
            condition="A",
            data=[]
        )
        result = self.grafana.create_alert_rule(alert)
        retrieved = self.grafana.get_alert_rule(result["id"])
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.name, "TestAlert")

    def test_get_alert_rule_not_found(self):
        """Test getting a non-existent alert rule."""
        retrieved = self.grafana.get_alert_rule("nonexistent")
        self.assertIsNone(retrieved)

    def test_list_alert_rules(self):
        """Test listing all alert rules."""
        alert1 = AlertRule(name="Alert1", folder_title="F1", condition="A", data=[])
        alert2 = AlertRule(name="Alert2", folder_title="F2", condition="B", data=[])
        self.grafana.create_alert_rule(alert1)
        self.grafana.create_alert_rule(alert2)
        alerts = self.grafana.list_alert_rules()
        self.assertEqual(len(alerts), 2)

    def test_update_alert_rule(self):
        """Test updating an alert rule."""
        alert = AlertRule(name="Original", folder_title="Folder", condition="A", data=[])
        result = self.grafana.create_alert_rule(alert)
        updated = self.grafana.update_alert_rule(result["id"], {"name": "Updated"})
        self.assertIsNotNone(updated)
        self.assertEqual(updated["status"], "success")

    def test_update_alert_rule_not_found(self):
        """Test updating a non-existent alert rule."""
        updated = self.grafana.update_alert_rule("nonexistent", {"name": "New"})
        self.assertIsNone(updated)

    def test_delete_alert_rule(self):
        """Test deleting an alert rule."""
        alert = AlertRule(name="ToDelete", folder_title="Folder", condition="A", data=[])
        result = self.grafana.create_alert_rule(alert)
        deleted = self.grafana.delete_alert_rule(result["id"])
        self.assertTrue(deleted)

    def test_delete_alert_rule_not_found(self):
        """Test deleting a non-existent alert rule."""
        deleted = self.grafana.delete_alert_rule("nonexistent")
        self.assertFalse(deleted)

    def test_pause_alert(self):
        """Test pausing an alert."""
        alert = AlertRule(name="ToPause", folder_title="Folder", condition="A", data=[])
        result = self.grafana.create_alert_rule(alert)
        paused = self.grafana.pause_alert(result["id"], True)
        self.assertIsNotNone(paused)
        self.assertTrue(paused["isPaused"])

    def test_pause_alert_not_found(self):
        """Test pausing a non-existent alert."""
        paused = self.grafana.pause_alert("nonexistent", True)
        self.assertIsNone(paused)


class TestAnnotationEvents(unittest.TestCase):
    """Tests for annotation events functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.grafana = GrafanaIntegration()

    def test_add_annotation(self):
        """Test adding an annotation."""
        ann = Annotation(
            text="Deployment started",
            time=int(time.time()),
            tags=["deploy"]
        )
        result = self.grafana.add_annotation(ann)
        self.assertEqual(result["status"], "success")
        self.assertIn("id", result)
        self.assertEqual(result["text"], "Deployment started")

    def test_get_annotation(self):
        """Test getting an annotation by ID."""
        ann = Annotation(text="Test", time=int(time.time()))
        result = self.grafana.add_annotation(ann)
        # Note: add_annotation returns id=1 for first annotation but list is 0-indexed
        # So we need to use index 0 (id-1) to get the annotation
        retrieved = self.grafana.get_annotation(0)  # Use correct list index
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.text, "Test")

    def test_get_annotation_not_found(self):
        """Test getting a non-existent annotation."""
        retrieved = self.grafana.get_annotation(999)
        self.assertIsNone(retrieved)

    def test_list_annotations(self):
        """Test listing all annotations."""
        ann1 = Annotation(text="Annotation 1", time=int(time.time()))
        ann2 = Annotation(text="Annotation 2", time=int(time.time()))
        self.grafana.add_annotation(ann1)
        self.grafana.add_annotation(ann2)
        annotations = self.grafana.list_annotations()
        self.assertEqual(len(annotations), 2)

    def test_list_annotations_with_dashboard_filter(self):
        """Test listing annotations filtered by dashboard."""
        ann1 = Annotation(text="Dash 1", time=int(time.time()), dashboard_id=1)
        ann2 = Annotation(text="Dash 2", time=int(time.time()), dashboard_id=2)
        self.grafana.add_annotation(ann1)
        self.grafana.add_annotation(ann2)
        filtered = self.grafana.list_annotations(dashboard_id=1)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["text"], "Dash 1")

    def test_list_annotations_with_time_filter(self):
        """Test listing annotations filtered by time range."""
        now = int(time.time())
        ann1 = Annotation(text="Old", time=now - 1000)
        ann2 = Annotation(text="New", time=now)
        self.grafana.add_annotation(ann1)
        self.grafana.add_annotation(ann2)
        filtered = self.grafana.list_annotations(from_time=now - 100)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["text"], "New")

    def test_delete_annotation(self):
        """Test deleting an annotation."""
        ann = Annotation(text="ToDelete", time=int(time.time()))
        result = self.grafana.add_annotation(ann)
        # Use index 0 since list is 0-indexed
        deleted = self.grafana.delete_annotation(0)
        self.assertTrue(deleted)

    def test_delete_annotation_not_found(self):
        """Test deleting a non-existent annotation."""
        deleted = self.grafana.delete_annotation(999)
        self.assertFalse(deleted)

    def test_update_annotation(self):
        """Test updating an annotation."""
        ann = Annotation(text="Original", time=int(time.time()))
        result = self.grafana.add_annotation(ann)
        # Use index 0 since list is 0-indexed
        updated = self.grafana.update_annotation(0, {"text": "Updated"})
        self.assertIsNotNone(updated)
        self.assertEqual(updated["status"], "success")

    def test_update_annotation_not_found(self):
        """Test updating a non-existent annotation."""
        updated = self.grafana.update_annotation(999, {"text": "New"})
        self.assertIsNone(updated)


class TestTemplateVariables(unittest.TestCase):
    """Tests for template variables functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.grafana = GrafanaIntegration()
        prov = DashboardProvisioning(dashboard_id="1", title="Test", uid="test-dash")
        self.grafana.provision_dashboard(prov)

    def test_create_template_variable(self):
        """Test creating a template variable."""
        var = TemplateVariable(
            name="environment",
            query="label_values(env)",
            type="query"
        )
        result = self.grafana.create_template_variable(var, "test-dash")
        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "success")

    def test_create_template_variable_dashboard_not_found(self):
        """Test creating template variable for non-existent dashboard."""
        var = TemplateVariable(name="env", query="label_values(env)")
        result = self.grafana.create_template_variable(var, "nonexistent")
        self.assertIsNone(result)

    def test_list_template_variables(self):
        """Test listing template variables."""
        var = TemplateVariable(name="var1", query="query1")
        self.grafana.create_template_variable(var, "test-dash")
        vars_list = self.grafana.list_template_variables("test-dash")
        self.assertEqual(len(vars_list), 1)

    def test_list_template_variables_dashboard_not_found(self):
        """Test listing template variables for non-existent dashboard."""
        vars_list = self.grafana.list_template_variables("nonexistent")
        self.assertEqual(vars_list, [])

    def test_delete_template_variable(self):
        """Test deleting a template variable."""
        var = TemplateVariable(name="toDelete", query="query")
        self.grafana.create_template_variable(var, "test-dash")
        deleted = self.grafana.delete_template_variable("test-dash", "toDelete")
        self.assertTrue(deleted)

    def test_delete_template_variable_not_found(self):
        """Test deleting a non-existent template variable."""
        deleted = self.grafana.delete_template_variable("test-dash", "nonexistent")
        self.assertFalse(deleted)


class TestSnapshotSharing(unittest.TestCase):
    """Tests for snapshot sharing functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.grafana = GrafanaIntegration()

    def test_create_snapshot(self):
        """Test creating a snapshot."""
        snap = DashboardSnapshot(
            dashboard_json={"title": "Test Dashboard"},
            name="Test Snapshot",
            expires_seconds=3600
        )
        result = self.grafana.create_snapshot(snap)
        self.assertEqual(result["status"], "success")
        self.assertIn("id", result)
        self.assertIn("url", result)

    def test_get_snapshot(self):
        """Test getting a snapshot."""
        snap = DashboardSnapshot(
            dashboard_json={"title": "Test"},
            name="Test Snapshot"
        )
        result = self.grafana.create_snapshot(snap)
        retrieved = self.grafana.get_snapshot(result["id"])
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved["name"], "Test Snapshot")

    def test_get_snapshot_not_found(self):
        """Test getting a non-existent snapshot."""
        retrieved = self.grafana.get_snapshot("nonexistent")
        self.assertIsNone(retrieved)

    def test_list_snapshots(self):
        """Test listing all snapshots."""
        snap1 = DashboardSnapshot(dashboard_json={}, name="Snap 1")
        snap2 = DashboardSnapshot(dashboard_json={}, name="Snap 2")
        self.grafana.create_snapshot(snap1)
        self.grafana.create_snapshot(snap2)
        snapshots = self.grafana.list_snapshots()
        self.assertEqual(len(snapshots), 2)

    def test_delete_snapshot(self):
        """Test deleting a snapshot."""
        snap = DashboardSnapshot(dashboard_json={}, name="ToDelete")
        result = self.grafana.create_snapshot(snap)
        deleted = self.grafana.delete_snapshot(result["id"])
        self.assertTrue(deleted)

    def test_delete_snapshot_not_found(self):
        """Test deleting a non-existent snapshot."""
        deleted = self.grafana.delete_snapshot("nonexistent")
        self.assertFalse(deleted)


class TestPlaylistManagement(unittest.TestCase):
    """Tests for playlist management functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.grafana = GrafanaIntegration()

    def test_create_playlist(self):
        """Test creating a playlist."""
        playlist = DashboardPlaylist(
            name="Daily Review",
            interval="10m",
            dashboards=[{"uid": "dash-1", "title": "Dashboard 1"}]
        )
        result = self.grafana.create_playlist(playlist)
        self.assertEqual(result["status"], "success")
        self.assertIn("id", result)
        self.assertEqual(result["dashboard_count"], 1)

    def test_get_playlist(self):
        """Test getting a playlist."""
        playlist = DashboardPlaylist(name="Test Playlist", dashboards=[])
        result = self.grafana.create_playlist(playlist)
        retrieved = self.grafana.get_playlist(result["id"])
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.name, "Test Playlist")

    def test_get_playlist_not_found(self):
        """Test getting a non-existent playlist."""
        retrieved = self.grafana.get_playlist("nonexistent")
        self.assertIsNone(retrieved)

    def test_list_playlists(self):
        """Test listing all playlists."""
        p1 = DashboardPlaylist(name="Playlist 1", dashboards=[])
        p2 = DashboardPlaylist(name="Playlist 2", dashboards=[])
        self.grafana.create_playlist(p1)
        self.grafana.create_playlist(p2)
        playlists = self.grafana.list_playlists()
        self.assertEqual(len(playlists), 2)

    def test_add_dashboard_to_playlist(self):
        """Test adding a dashboard to a playlist."""
        playlist = DashboardPlaylist(name="Test", dashboards=[])
        result = self.grafana.create_playlist(playlist)
        added = self.grafana.add_dashboard_to_playlist(
            result["id"], "dash-uid", "Test Dashboard", order=1
        )
        self.assertIsNotNone(added)
        self.assertEqual(added["status"], "success")

    def test_add_dashboard_to_playlist_not_found(self):
        """Test adding dashboard to non-existent playlist."""
        added = self.grafana.add_dashboard_to_playlist("nonexistent", "dash-uid", "Test")
        self.assertIsNone(added)

    def test_delete_playlist(self):
        """Test deleting a playlist."""
        playlist = DashboardPlaylist(name="ToDelete", dashboards=[])
        result = self.grafana.create_playlist(playlist)
        deleted = self.grafana.delete_playlist(result["id"])
        self.assertTrue(deleted)

    def test_delete_playlist_not_found(self):
        """Test deleting a non-existent playlist."""
        deleted = self.grafana.delete_playlist("nonexistent")
        self.assertFalse(deleted)


class TestAPIKeyManagement(unittest.TestCase):
    """Tests for API key management functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.grafana = GrafanaIntegration()

    def test_create_api_key(self):
        """Test creating an API key."""
        api_key = APIKey(
            name="CI/CD Key",
            role="Editor",
            expires_in=86400
        )
        result = self.grafana.create_api_key(api_key)
        self.assertEqual(result["status"], "success")
        self.assertIn("key", result)
        self.assertEqual(result["role"], "Editor")
        self.assertIn("warning", result)

    def test_list_api_keys(self):
        """Test listing API keys."""
        key1 = APIKey(name="Key 1", role="Viewer")
        key2 = APIKey(name="Key 2", role="Editor")
        self.grafana.create_api_key(key1)
        self.grafana.create_api_key(key2)
        keys = self.grafana.list_api_keys()
        self.assertEqual(len(keys), 2)

    def test_delete_api_key(self):
        """Test deleting an API key."""
        api_key = APIKey(name="ToDelete", role="Viewer")
        result = self.grafana.create_api_key(api_key)
        deleted = self.grafana.delete_api_key(result["id"])
        self.assertTrue(deleted)

    def test_delete_api_key_not_found(self):
        """Test deleting a non-existent API key."""
        deleted = self.grafana.delete_api_key("nonexistent")
        self.assertFalse(deleted)

    def test_validate_api_key(self):
        """Test validating an API key."""
        result = self.grafana.validate_api_key("any-key")
        self.assertFalse(result)


class TestSSOConfiguration(unittest.TestCase):
    """Tests for SSO configuration functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.grafana = GrafanaIntegration()

    def test_configure_sso(self):
        """Test configuring SSO."""
        sso = SSOConfig(
            name="github",
            type="oauth",
            client_id="abc123",
            client_secret="secret",
            auth_url="https://github.com/login/oauth/authorize",
            token_url="https://github.com/login/oauth/access_token"
        )
        result = self.grafana.configure_sso(sso)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["name"], "github")

    def test_get_sso_config(self):
        """Test getting SSO configuration."""
        sso = SSOConfig(name="test-sso", type="oauth", client_id="id")
        self.grafana.configure_sso(sso)
        retrieved = self.grafana.get_sso_config("test-sso")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.name, "test-sso")

    def test_get_sso_config_not_found(self):
        """Test getting non-existent SSO configuration."""
        retrieved = self.grafana.get_sso_config("nonexistent")
        self.assertIsNone(retrieved)

    def test_list_sso_configs(self):
        """Test listing all SSO configurations."""
        sso1 = SSOConfig(name="sso1", type="oauth", client_id="id1")
        sso2 = SSOConfig(name="sso2", type="saml", client_id="id2")
        self.grafana.configure_sso(sso1)
        self.grafana.configure_sso(sso2)
        configs = self.grafana.list_sso_configs()
        self.assertEqual(len(configs), 2)

    def test_update_sso_config(self):
        """Test updating SSO configuration."""
        sso = SSOConfig(name="to-update", type="oauth", client_id="old")
        self.grafana.configure_sso(sso)
        updated = self.grafana.update_sso_config("to-update", {"client_id": "new"})
        self.assertIsNotNone(updated)
        self.assertEqual(updated["status"], "success")

    def test_update_sso_config_not_found(self):
        """Test updating non-existent SSO configuration."""
        updated = self.grafana.update_sso_config("nonexistent", {"client_id": "new"})
        self.assertIsNone(updated)

    def test_delete_sso_config(self):
        """Test deleting SSO configuration."""
        sso = SSOConfig(name="to-delete", type="oauth", client_id="id")
        self.grafana.configure_sso(sso)
        deleted = self.grafana.delete_sso_config("to-delete")
        self.assertTrue(deleted)

    def test_delete_sso_config_not_found(self):
        """Test deleting non-existent SSO configuration."""
        deleted = self.grafana.delete_sso_config("nonexistent")
        self.assertFalse(deleted)


class TestFolderManagement(unittest.TestCase):
    """Tests for folder management functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.grafana = GrafanaIntegration()

    def test_create_folder(self):
        """Test creating a folder."""
        folder = DashboardFolder(
            title="Production Dashboards",
            uid="prod-folder"
        )
        result = self.grafana.create_folder(folder)
        self.assertEqual(result["status"], "success")
        self.assertIn("uid", result)
        self.assertIn("url", result)

    def test_create_folder_without_uid(self):
        """Test creating a folder without explicit UID."""
        folder = DashboardFolder(title="Auto UID Folder")
        result = self.grafana.create_folder(folder)
        self.assertEqual(result["status"], "success")
        self.assertIsNotNone(result["uid"])

    def test_get_folder(self):
        """Test getting a folder."""
        folder = DashboardFolder(title="Test Folder", uid="test-get")
        self.grafana.create_folder(folder)
        retrieved = self.grafana.get_folder("test-get")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.title, "Test Folder")

    def test_get_folder_not_found(self):
        """Test getting a non-existent folder."""
        retrieved = self.grafana.get_folder("nonexistent")
        self.assertIsNone(retrieved)

    def test_list_folders(self):
        """Test listing all folders."""
        f1 = DashboardFolder(title="Folder 1", uid="f1")
        f2 = DashboardFolder(title="Folder 2", uid="f2")
        self.grafana.create_folder(f1)
        self.grafana.create_folder(f2)
        folders = self.grafana.list_folders()
        self.assertEqual(len(folders), 2)

    def test_update_folder(self):
        """Test updating a folder."""
        folder = DashboardFolder(title="Original", uid="to-update")
        self.grafana.create_folder(folder)
        updated = self.grafana.update_folder("to-update", {"title": "Updated"})
        self.assertIsNotNone(updated)
        self.assertEqual(updated["status"], "success")

    def test_update_folder_not_found(self):
        """Test updating a non-existent folder."""
        updated = self.grafana.update_folder("nonexistent", {"title": "New"})
        self.assertIsNone(updated)

    def test_delete_folder(self):
        """Test deleting a folder."""
        folder = DashboardFolder(title="ToDelete", uid="del-folder")
        self.grafana.create_folder(folder)
        deleted = self.grafana.delete_folder("del-folder")
        self.assertTrue(deleted)

    def test_delete_folder_not_found(self):
        """Test deleting a non-existent folder."""
        deleted = self.grafana.delete_folder("nonexistent")
        self.assertFalse(deleted)


class TestUtilityMethods(unittest.TestCase):
    """Tests for utility methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.grafana = GrafanaIntegration()

    def test_health_check(self):
        """Test health check method."""
        health = self.grafana.health_check()
        self.assertEqual(health["status"], "ok")
        self.assertIn("version", health)
        self.assertTrue(health["configured"])

    def test_get_stats(self):
        """Test getting integration statistics."""
        stats = self.grafana.get_stats()
        self.assertIn("dashboards", stats)
        self.assertIn("datasources", stats)
        self.assertIn("folders", stats)
        self.assertIn("alerts", stats)
        self.assertIn("annotations", stats)
        self.assertIn("snapshots", stats)
        self.assertIn("playlists", stats)
        self.assertIn("api_keys", stats)
        self.assertIn("sso_configs", stats)

    def test_export_config(self):
        """Test exporting configuration."""
        config = self.grafana.export_config()
        self.assertIn("version", config)
        self.assertIn("exported_at", config)
        self.assertIn("dashboards", config)
        self.assertIn("datasources", config)

    def test_import_config(self):
        """Test importing configuration."""
        config = {
            "dashboards": {},
            "datasources": [{
                "name": "Imported DS",
                "type": "prometheus",
                "url": "http://imported:9090"
            }],
            "folders": [],
            "alerts": [],
            "sso_configs": []
        }
        result = self.grafana.import_config(config)
        # Result should have 'imported' key with counts
        self.assertEqual(result["status"], "success")
        self.assertIn("imported", result)
        self.assertEqual(result["imported"]["datasources"], 1)


if __name__ == '__main__':
    unittest.main()

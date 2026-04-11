"""
Tests for workflow_aws_guardduty module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import types

# Create mock boto3 module before importing workflow_aws_guardduty
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()
mock_boto3.resource = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions

# Import the module
import src.workflow_aws_guardduty as _guardduty_module

# Extract classes
GuardDutyIntegration = _guardduty_module.GuardDutyIntegration
Detector = _guardduty_module.Detector
IPSet = _guardduty_module.IPSet
ThreatIntelSet = _guardduty_module.ThreatIntelSet
Finding = _guardduty_module.Finding
Filter = _guardduty_module.Filter
MemberAccount = _guardduty_module.MemberAccount
AdminAccount = _guardduty_module.AdminAccount
FindingStatus = _guardduty_module.FindingStatus
DetectorStatus = _guardduty_module.DetectorStatus
IPSetStatus = _guardduty_module.IPSetStatus
ThreatIntelSetStatus = _guardduty_module.ThreatIntelSetStatus
FindingSeverity = _guardduty_module.FindingSeverity
FilterAction = _guardduty_module.FilterAction


class TestDetector(unittest.TestCase):
    """Test Detector dataclass"""

    def test_detector_creation(self):
        detector = Detector(
            detector_id="abc123",
            region="us-east-1",
            status=DetectorStatus.ENABLED
        )
        self.assertEqual(detector.detector_id, "abc123")
        self.assertEqual(detector.region, "us-east-1")
        self.assertEqual(detector.status, DetectorStatus.ENABLED)
        self.assertEqual(detector.finding_publishing_frequency, "FIFTEEN_MINUTES")
        self.assertEqual(detector.tags, {})

    def test_detector_with_tags(self):
        detector = Detector(
            detector_id="abc123",
            region="us-west-2",
            status=DetectorStatus.DISABLED,
            tags={"env": "test"}
        )
        self.assertEqual(detector.tags, {"env": "test"})


class TestIPSet(unittest.TestCase):
    """Test IPSet dataclass"""

    def test_ipset_creation(self):
        ipset = IPSet(
            ip_set_id="ipset-123",
            name="test-ipset",
            format="TXT",
            location="s3://bucket/ipset.txt"
        )
        self.assertEqual(ipset.ip_set_id, "ipset-123")
        self.assertEqual(ipset.name, "test-ipset")
        self.assertEqual(ipset.format, "TXT")
        self.assertEqual(ipset.status, IPSetStatus.INACTIVE)

    def test_ipset_active(self):
        ipset = IPSet(
            ip_set_id="ipset-456",
            name="active-ipset",
            status=IPSetStatus.ACTIVE
        )
        self.assertEqual(ipset.status, IPSetStatus.ACTIVE)


class TestThreatIntelSet(unittest.TestCase):
    """Test ThreatIntelSet dataclass"""

    def test_threat_intel_set_creation(self):
        tis = ThreatIntelSet(
            threat_intel_set_id="tis-123",
            name="test-threat-intel",
            format="TXT",
            location="s3://bucket/threats.txt"
        )
        self.assertEqual(tis.threat_intel_set_id, "tis-123")
        self.assertEqual(tis.name, "test-threat-intel")
        self.assertEqual(tis.status, ThreatIntelSetStatus.INACTIVE)

    def test_threat_intel_set_active(self):
        tis = ThreatIntelSet(
            threat_intel_set_id="tis-456",
            name="active-threats",
            status=ThreatIntelSetStatus.ACTIVE
        )
        self.assertEqual(tis.status, ThreatIntelSetStatus.ACTIVE)


class TestFinding(unittest.TestCase):
    """Test Finding dataclass"""

    def test_finding_creation(self):
        finding = Finding(
            finding_id="finding-123",
            detector_id="detector-123",
            severity=FindingSeverity.HIGH,
            status=FindingStatus.ACTIVE,
            title="Suspicious Login"
        )
        self.assertEqual(finding.finding_id, "finding-123")
        self.assertEqual(finding.severity, FindingSeverity.HIGH)
        self.assertEqual(finding.status, FindingStatus.ACTIVE)

    def test_finding_with_resource(self):
        finding = Finding(
            finding_id="finding-456",
            detector_id="detector-123",
            severity=FindingSeverity.CRITICAL,
            status=FindingStatus.ACTIVE,
            title="Compromised Instance",
            resource_type="EC2",
            resource_id="i-1234567890abcdef0"
        )
        self.assertEqual(finding.resource_type, "EC2")
        self.assertEqual(finding.resource_id, "i-1234567890abcdef0")


class TestFilter(unittest.TestCase):
    """Test Filter dataclass"""

    def test_filter_creation(self):
        filter_criteria = {"severity": [{"eq": ["HIGH"]}]}
        filter_obj = Filter(
            name="high-severity-filter",
            action=FilterAction.ARCHIVE,
            finding_criteria=filter_criteria,
            description="Archive high severity findings"
        )
        self.assertEqual(filter_obj.name, "high-severity-filter")
        self.assertEqual(filter_obj.action, FilterAction.ARCHIVE)


class TestMemberAccount(unittest.TestCase):
    """Test MemberAccount dataclass"""

    def test_member_account_creation(self):
        member = MemberAccount(
            account_id="123456789012",
            email="member@example.com"
        )
        self.assertEqual(member.account_id, "123456789012")
        self.assertEqual(member.email, "member@example.com")


class TestAdminAccount(unittest.TestCase):
    """Test AdminAccount dataclass"""

    def test_admin_account_creation(self):
        admin = AdminAccount(
            admin_account_id="999999999999",
            relationship_status="Enabled"
        )
        self.assertEqual(admin.admin_account_id, "999999999999")
        self.assertEqual(admin.relationship_status, "Enabled")


class TestGuardDutyIntegration(unittest.TestCase):
    """Test GuardDutyIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_guardduty = MagicMock()
        self.mock_cloudwatch = MagicMock()
        self.mock_events = MagicMock()
        self.mock_iam = MagicMock()
        self.mock_sts = MagicMock()
        self.mock_organizations = MagicMock()

        self.patcher = patch('src.workflow_aws_guardduty.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [
            self.mock_guardduty,
            self.mock_cloudwatch,
            self.mock_events,
            self.mock_iam,
            self.mock_sts,
            self.mock_organizations
        ]

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_initialization(self):
        """Test GuardDutyIntegration initialization"""
        integration = GuardDutyIntegration(region_name="us-east-1")
        self.assertEqual(integration.region_name, "us-east-1")
        self.assertIsNone(integration.profile_name)
        self.assertIsNone(integration.endpoint_url)

    def test_initialization_with_profile(self):
        """Test initialization with profile"""
        integration = GuardDutyIntegration(
            region_name="us-west-2",
            profile_name="my-profile"
        )
        self.assertEqual(integration.region_name, "us-west-2")
        self.assertEqual(integration.profile_name, "my-profile")


class TestGuardDutyDetectorManagement(unittest.TestCase):
    """Test GuardDuty detector management operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_guardduty = MagicMock()
        
        self.patcher = patch('src.workflow_aws_guardduty.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = self.mock_guardduty

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_create_detector(self):
        """Test create_detector method"""
        self.mock_guardduty.create_detector.return_value = {
            "detectorId": "detector-123"
        }
        
        integration = GuardDutyIntegration()
        detector = integration.create_detector(enable=True)
        
        self.assertEqual(detector.detector_id, "detector-123")
        self.assertEqual(detector.status, DetectorStatus.ENABLED)
        self.mock_guardduty.create_detector.assert_called_once()

    def test_get_detector(self):
        """Test get_detector method"""
        self.mock_guardduty.get_detector.return_value = {
            "status": "ENABLED",
            "findingPublishingFrequency": "ONE_HOUR",
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-02T00:00:00Z",
            "tags": {"env": "prod"}
        }
        
        integration = GuardDutyIntegration()
        detector = integration.get_detector("detector-123")
        
        self.assertIsNotNone(detector)
        self.assertEqual(detector.status, DetectorStatus.ENABLED)
        self.assertEqual(detector.finding_publishing_frequency, "ONE_HOUR")

    def test_list_detectors(self):
        """Test list_detectors method"""
        self.mock_guardduty.list_detectors.return_value = {
            "DetectorIds": ["detector-1", "detector-2"]
        }
        
        integration = GuardDutyIntegration()
        detectors = integration.list_detectors()
        
        self.assertEqual(len(detectors), 2)
        self.assertIn("detector-1", detectors)
        self.assertIn("detector-2", detectors)

    def test_update_detector(self):
        """Test update_detector method"""
        self.mock_guardduty.update_detector.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.update_detector("detector-123", enable=False)
        
        self.assertTrue(result)
        self.mock_guardduty.update_detector.assert_called_once()

    def test_delete_detector(self):
        """Test delete_detector method"""
        self.mock_guardduty.delete_detector.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.delete_detector("detector-123")
        
        self.assertTrue(result)

    def test_get_detector_status(self):
        """Test get_detector_status method"""
        self.mock_guardduty.get_detector.return_value = {
            "status": "ENABLED",
            "findingPublishingFrequency": "FIFTEEN_MINUTES"
        }
        
        integration = GuardDutyIntegration()
        status = integration.get_detector_status("detector-123")
        
        self.assertEqual(status, DetectorStatus.ENABLED)


class TestGuardDutyIPSets(unittest.TestCase):
    """Test GuardDuty IP set operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_guardduty = MagicMock()
        
        self.patcher = patch('src.workflow_aws_guardduty.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = self.mock_guardduty

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_create_ip_set(self):
        """Test create_ip_set method"""
        self.mock_guardduty.create_ip_set.return_value = {
            "ipSetId": "ipset-123"
        }
        
        integration = GuardDutyIntegration()
        ipset = integration.create_ip_set(
            detector_id="detector-123",
            name="test-ipset",
            format="TXT",
            location="s3://bucket/ipset.txt",
            activate=True
        )
        
        self.assertEqual(ipset.ip_set_id, "ipset-123")
        self.assertEqual(ipset.status, IPSetStatus.ACTIVE)

    def test_get_ip_set(self):
        """Test get_ip_set method"""
        self.mock_guardduty.get_ip_set.return_value = {
            "name": "my-ipset",
            "format": "TXT",
            "location": "s3://bucket/ipset.txt",
            "status": "ACTIVE",
            "tags": {"env": "test"}
        }
        
        integration = GuardDutyIntegration()
        ipset = integration.get_ip_set("detector-123", "ipset-123")
        
        self.assertIsNotNone(ipset)
        self.assertEqual(ipset.name, "my-ipset")
        self.assertEqual(ipset.status, IPSetStatus.ACTIVE)

    def test_list_ip_sets(self):
        """Test list_ip_sets method"""
        self.mock_guardduty.list_ip_sets.return_value = {
            "ipSets": [
                {"ipSetId": "ipset-1", "name": "ipset-one"},
                {"ipSetId": "ipset-2", "name": "ipset-two"}
            ]
        }
        
        integration = GuardDutyIntegration()
        ipsets = integration.list_ip_sets("detector-123")
        
        self.assertEqual(len(ipsets), 2)

    def test_update_ip_set(self):
        """Test update_ip_set method"""
        self.mock_guardduty.update_ip_set.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.update_ip_set(
            detector_id="detector-123",
            ip_set_id="ipset-123",
            activate=True
        )
        
        self.assertTrue(result)

    def test_delete_ip_set(self):
        """Test delete_ip_set method"""
        self.mock_guardduty.delete_ip_set.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.delete_ip_set("detector-123", "ipset-123")
        
        self.assertTrue(result)

    def test_create_allowlist(self):
        """Test create_allowlist method"""
        self.mock_guardduty.create_ip_set.return_value = {
            "ipSetId": "allowlist-123"
        }
        
        integration = GuardDutyIntegration()
        ipset = integration.create_allowlist(
            detector_id="detector-123",
            name="trusted-ips",
            ips=["1.2.3.4", "5.6.7.8"],
            activate=True
        )
        
        self.assertEqual(ipset.ip_set_id, "allowlist-123")

    def test_create_denylist(self):
        """Test create_denylist method"""
        self.mock_guardduty.create_ip_set.return_value = {
            "ipSetId": "denylist-123"
        }
        
        integration = GuardDutyIntegration()
        ipset = integration.create_denylist(
            detector_id="detector-123",
            name="blocked-ips",
            ips=["1.2.3.4"],
            activate=True
        )
        
        self.assertEqual(ipset.ip_set_id, "denylist-123")


class TestGuardDutyThreatIntelSets(unittest.TestCase):
    """Test GuardDuty threat intel set operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_guardduty = MagicMock()
        
        self.patcher = patch('src.workflow_aws_guardduty.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = self.mock_guardduty

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_create_threat_intel_set(self):
        """Test create_threat_intel_set method"""
        self.mock_guardduty.create_threat_intel_set.return_value = {
            "threatIntelSetId": "tis-123"
        }
        
        integration = GuardDutyIntegration()
        tis = integration.create_threat_intel_set(
            detector_id="detector-123",
            name="threat-intel-set",
            format="TXT",
            location="s3://bucket/threats.txt",
            activate=True
        )
        
        self.assertEqual(tis.threat_intel_set_id, "tis-123")
        self.assertEqual(tis.status, ThreatIntelSetStatus.ACTIVE)

    def test_get_threat_intel_set(self):
        """Test get_threat_intel_set method"""
        self.mock_guardduty.get_threat_intel_set.return_value = {
            "name": "my-threats",
            "format": "OTX_CSV",
            "location": "s3://bucket/threats.csv",
            "status": "INACTIVE",
            "tags": {}
        }
        
        integration = GuardDutyIntegration()
        tis = integration.get_threat_intel_set("detector-123", "tis-123")
        
        self.assertIsNotNone(tis)
        self.assertEqual(tis.name, "my-threats")
        self.assertEqual(tis.status, ThreatIntelSetStatus.INACTIVE)

    def test_list_threat_intel_sets(self):
        """Test list_threat_intel_sets method"""
        self.mock_guardduty.list_threat_intel_sets.return_value = {
            "threatIntelSets": [
                {"threatIntelSetId": "tis-1", "name": "threats-one"},
                {"threatIntelSetId": "tis-2", "name": "threats-two"}
            ]
        }
        
        integration = GuardDutyIntegration()
        sets = integration.list_threat_intel_sets("detector-123")
        
        self.assertEqual(len(sets), 2)

    def test_update_threat_intel_set(self):
        """Test update_threat_intel_set method"""
        self.mock_guardduty.update_threat_intel_set.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.update_threat_intel_set(
            detector_id="detector-123",
            threat_intel_set_id="tis-123",
            activate=True
        )
        
        self.assertTrue(result)

    def test_delete_threat_intel_set(self):
        """Test delete_threat_intel_set method"""
        self.mock_guardduty.delete_threat_intel_set.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.delete_threat_intel_set("detector-123", "tis-123")
        
        self.assertTrue(result)


class TestGuardDutyFindings(unittest.TestCase):
    """Test GuardDuty findings operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_guardduty = MagicMock()
        
        self.patcher = patch('src.workflow_aws_guardduty.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = self.mock_guardduty

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_get_findings(self):
        """Test get_findings method"""
        self.mock_guardduty.get_findings.return_value = {
            "Findings": [
                {
                    "id": "finding-1",  # lowercase 'id' to match actual code
                    "Severity": "HIGH",
                    "Title": "Test Finding",
                    "Description": "A test finding",
                    "AccountId": "123456789012",
                    "Region": "us-east-1",
                    "Service": {"Action": {"actionType": "OBSERVED"}},
                    "Resource": {"resourceType": "EC2", "resourceId": "i-123"},
                    "CreatedAt": "2024-01-01T00:00:00Z",
                    "UpdatedAt": "2024-01-02T00:00:00Z",
                    "Tags": {}
                }
            ]
        }
        
        integration = GuardDutyIntegration()
        findings = integration.get_findings("detector-123")
        
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].finding_id, "finding-1")
        self.assertEqual(findings[0].severity, FindingSeverity.HIGH)

    def test_list_findings(self):
        """Test list_findings method"""
        self.mock_guardduty.list_findings.return_value = {
            "findingIds": ["finding-1", "finding-2", "finding-3"]
        }
        
        integration = GuardDutyIntegration()
        finding_ids = integration.list_findings("detector-123")
        
        self.assertEqual(len(finding_ids), 3)

    def test_get_severity_statistics(self):
        """Test get_severity_statistics method"""
        self.mock_guardduty.get_severity_statistics.return_value = {
            "lowSeverity": 10,
            "mediumSeverity": 5,
            "highSeverity": 3,
            "criticalSeverity": 1
        }
        
        integration = GuardDutyIntegration()
        stats = integration.get_severity_statistics("detector-123")
        
        self.assertEqual(stats["low"], 10)
        self.assertEqual(stats["medium"], 5)
        self.assertEqual(stats["high"], 3)
        self.assertEqual(stats["critical"], 1)

    def test_findings_by_severity(self):
        """Test findings_by_severity method"""
        self.mock_guardduty.list_findings.return_value = {
            "findingIds": ["finding-1"]
        }
        self.mock_guardduty.get_findings.return_value = {
            "Findings": [
                {
                    "id": "finding-1",
                    "Severity": "HIGH",
                    "Title": "High Severity Finding",
                    "Description": "A high severity finding",
                    "AccountId": "123456789012",
                    "Region": "us-east-1",
                    "Service": {"Action": {"actionType": "OBSERVED"}},
                    "Resource": {"resourceType": "EC2", "resourceId": "i-123"},
                    "CreatedAt": "2024-01-01T00:00:00Z",
                    "UpdatedAt": "2024-01-02T00:00:00Z",
                    "Tags": {}
                }
            ]
        }
        
        integration = GuardDutyIntegration()
        findings = integration.findings_by_severity("detector-123", FindingSeverity.HIGH)
        
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].severity, FindingSeverity.HIGH)


class TestGuardDutyArchiveFindings(unittest.TestCase):
    """Test GuardDuty archive findings operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_guardduty = MagicMock()
        
        self.patcher = patch('src.workflow_aws_guardduty.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = self.mock_guardduty

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_archive_finding(self):
        """Test archive_finding method"""
        self.mock_guardduty.archive_findings.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.archive_finding("detector-123", "finding-456")
        
        self.assertTrue(result)
        self.mock_guardduty.archive_findings.assert_called_once()

    def test_unarchive_finding(self):
        """Test unarchive_finding method"""
        self.mock_guardduty.unarchive_findings.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.unarchive_finding("detector-123", "finding-456")
        
        self.assertTrue(result)
        self.mock_guardduty.unarchive_findings.assert_called_once()

    def test_archive_findings_by_filter(self):
        """Test archive_findings_by_filter method"""
        self.mock_guardduty.list_findings.return_value = {
            "findingIds": ["finding-1", "finding-2"]
        }
        self.mock_guardduty.archive_findings.return_value = {}
        
        integration = GuardDutyIntegration()
        count = integration.archive_findings_by_filter(
            "detector-123",
            {"severity": [{"eq": ["LOW"]}]}
        )
        
        self.assertEqual(count, 2)


class TestGuardDutyFilters(unittest.TestCase):
    """Test GuardDuty filter operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_guardduty = MagicMock()
        
        self.patcher = patch('src.workflow_aws_guardduty.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = self.mock_guardduty

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_create_filter(self):
        """Test create_filter method"""
        self.mock_guardduty.create_filter.return_value = {
            "filterId": "filter-123"
        }
        
        integration = GuardDutyIntegration()
        filter_criteria = {"severity": [{"eq": ["HIGH"]}]}
        filter_obj = integration.create_filter(
            detector_id="detector-123",
            name="high-severity-filter",
            action=FilterAction.ARCHIVE,
            finding_criteria=filter_criteria,
            description="Archive high severity findings"
        )
        
        self.assertEqual(filter_obj.name, "high-severity-filter")
        self.assertEqual(filter_obj.action, FilterAction.ARCHIVE)

    def test_get_filter(self):
        """Test get_filter method"""
        self.mock_guardduty.get_filter.return_value = {
            "name": "my-filter",
            "action": "ARCHIVE",
            "findingCriteria": {"severity": [{"eq": ["MEDIUM"]}]},
            "description": "Test filter",
            "tags": {"env": "test"}
        }
        
        integration = GuardDutyIntegration()
        filter_obj = integration.get_filter("detector-123", "filter-123")
        
        self.assertIsNotNone(filter_obj)
        self.assertEqual(filter_obj.name, "my-filter")
        self.assertEqual(filter_obj.action, FilterAction.ARCHIVE)

    def test_list_filters(self):
        """Test list_filters method"""
        self.mock_guardduty.list_filters.return_value = {
            "filterNames": ["filter-1", "filter-2"]  # lowercase key
        }
        
        integration = GuardDutyIntegration()
        filters = integration.list_filters("detector-123")
        
        self.assertEqual(len(filters), 2)

    def test_update_filter(self):
        """Test update_filter method"""
        self.mock_guardduty.update_filter.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.update_filter(
            detector_id="detector-123",
            filter_name="filter-123",  # correct parameter name
            finding_criteria={"severity": [{"eq": ["CRITICAL"]}]}
        )
        
        self.assertTrue(result)

    def test_delete_filter(self):
        """Test delete_filter method"""
        self.mock_guardduty.delete_filter.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.delete_filter("detector-123", "filter-123")
        
        self.assertTrue(result)

    def test_create_auto_archive_filter(self):
        """Test create_auto_archive_filter method"""
        self.mock_guardduty.create_filter.return_value = {
            "filterId": "auto-archive-123"
        }
        
        integration = GuardDutyIntegration()
        filter_obj = integration.create_auto_archive_filter(
            detector_id="detector-123",
            name="auto-archive-high",
            severity=FindingSeverity.HIGH,
            description="Auto archive high severity"
        )
        
        self.assertEqual(filter_obj.name, "auto-archive-high")
        self.assertEqual(filter_obj.action, FilterAction.ARCHIVE)


class TestGuardDutyMemberAccounts(unittest.TestCase):
    """Test GuardDuty member account operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_guardduty = MagicMock()
        
        self.patcher = patch('src.workflow_aws_guardduty.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = self.mock_guardduty

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_create_member(self):
        """Test create_member method"""
        self.mock_guardduty.create_members.return_value = {
            "UnprocessedAccounts": []
        }
        
        integration = GuardDutyIntegration()
        member = integration.create_member(
            detector_id="detector-123",
            account_id="123456789012",
            email="member@example.com"
        )
        
        self.assertEqual(member.account_id, "123456789012")
        self.assertEqual(member.email, "member@example.com")

    def test_get_member(self):
        """Test get_member method"""
        self.mock_guardduty.get_members.return_value = {
            "Members": [
                {
                    "AccountId": "123456789012",
                    "Email": "member@example.com",
                    "RelationshipStatus": "Created",
                    "DetectorId": "detector-123"
                }
            ]
        }
        
        integration = GuardDutyIntegration()
        member = integration.get_member("detector-123", "123456789012")
        
        self.assertIsNotNone(member)
        self.assertEqual(member.account_id, "123456789012")

    def test_list_members(self):
        """Test list_members method"""
        self.mock_guardduty.list_members.return_value = {
            "Members": [
                {"AccountId": "111111111111", "Email": "member1@example.com"},
                {"AccountId": "222222222222", "Email": "member2@example.com"}
            ],
            "UnprocessedAccounts": []
        }
        
        integration = GuardDutyIntegration()
        members = integration.list_members("detector-123")
        
        self.assertEqual(len(members), 2)

    def test_delete_member(self):
        """Test delete_member method"""
        self.mock_guardduty.delete_members.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.delete_member("detector-123", "123456789012")
        
        self.assertTrue(result)


class TestGuardDutyAdminAccount(unittest.TestCase):
    """Test GuardDuty administrator account operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_guardduty = MagicMock()
        self.mock_organizations = MagicMock()
        
        self.patcher = patch('src.workflow_aws_guardduty.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [self.mock_guardduty, MagicMock(), MagicMock(), MagicMock(), MagicMock(), self.mock_organizations]

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_enable_administrator(self):
        """Test enable_administrator method"""
        self.mock_guardduty.enable_organization_admin_account.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.enable_administrator("999999999999")
        
        self.assertTrue(result)
        self.mock_guardduty.enable_organization_admin_account.assert_called_once()

    def test_get_administrator(self):
        """Test get_administrator method"""
        self.mock_guardduty.get_administrator.return_value = {
            "Administrator": {
                "AccountId": "999999999999",
                "RelationshipStatus": "Enabled"
            }
        }
        
        integration = GuardDutyIntegration()
        admin = integration.get_administrator("detector-123")
        
        self.assertIsNotNone(admin)
        self.assertEqual(admin.admin_account_id, "999999999999")

    def test_list_administrators(self):
        """Test list_administrators method"""
        self.mock_guardduty.list_administrators.return_value = {
            "Administrators": [
                {"AccountId": "999999999999", "RelationshipStatus": "Enabled"}
            ]
        }
        
        integration = GuardDutyIntegration()
        admins = integration.list_administrators("detector-123")
        
        self.assertEqual(len(admins), 1)


class TestGuardDutyS3Protection(unittest.TestCase):
    """Test GuardDuty S3 protection operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_guardduty = MagicMock()
        
        self.patcher = patch('src.workflow_aws_guardduty.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = self.mock_guardduty

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_enable_s3_protection(self):
        """Test enable_s3_protection method"""
        self.mock_guardduty.update_detector.return_value = {}
        self.mock_guardduty.update_datasources.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.enable_s3_protection("detector-123")
        
        self.assertTrue(result)

    def test_get_s3_protection_status(self):
        """Test get_s3_protection_status method"""
        self.mock_guardduty.get_datasources.return_value = {
            "s3Logs": {
                "enable": True
            }
        }
        
        integration = GuardDutyIntegration()
        status = integration.get_s3_protection_status("detector-123")
        
        self.assertTrue(status)

    def test_disable_s3_protection(self):
        """Test disable_s3_protection method"""
        self.mock_guardduty.update_datasources.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.disable_s3_protection("detector-123")
        
        self.assertTrue(result)


class TestGuardDutyCloudWatchIntegration(unittest.TestCase):
    """Test GuardDuty CloudWatch integration"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_guardduty = MagicMock()
        self.mock_cloudwatch = MagicMock()
        self.mock_events = MagicMock()
        
        self.patcher = patch('src.workflow_aws_guardduty.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [self.mock_guardduty, self.mock_cloudwatch, self.mock_events]

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_setup_cloudwatch_integration(self):
        """Test setup_cloudwatch_integration method"""
        self.mock_events.put_rule.return_value = {}
        self.mock_events.put_targets.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.setup_cloudwatch_integration(
            detector_id="detector-123",
            sns_topic_arn="arn:aws:sns:us-east-1:123456789012:my-topic"
        )
        
        self.assertTrue(result)

    def test_put_guardduty_metric_alarm(self):
        """Test put_guardduty_metric_alarm method"""
        self.mock_cloudwatch.put_metric_alarm.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.put_guardduty_metric_alarm(
            alarm_name="guardduty-alarm",
            detector_id="detector-123",
            threshold=5
        )
        
        self.assertTrue(result)

    def test_get_guardduty_metrics(self):
        """Test get_guardduty_metrics method"""
        self.mock_cloudwatch.get_metric_statistics.return_value = {
            "Datapoints": [
                {"Timestamp": datetime(2024, 1, 1), "Sum": 10.0}
            ]
        }
        
        integration = GuardDutyIntegration()
        metrics = integration.get_guardduty_metrics()
        
        self.assertIsInstance(metrics, list)


class TestGuardDutyTagging(unittest.TestCase):
    """Test GuardDuty resource tagging operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_guardduty = MagicMock()
        
        self.patcher = patch('src.workflow_aws_guardduty.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = self.mock_guardduty

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_tag_resource(self):
        """Test tag_resource method"""
        self.mock_guardduty.tag_resource.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.tag_resource(
            resource_arn="arn:aws:guardduty:us-east-1:123456789012:detector/detector-123",
            tags={"env": "production"}
        )
        
        self.assertTrue(result)

    def test_untag_resource(self):
        """Test untag_resource method"""
        self.mock_guardduty.untag_resource.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.untag_resource(
            resource_arn="arn:aws:guardduty:us-east-1:123456789012:detector/detector-123",
            tag_keys=["env"]
        )
        
        self.assertTrue(result)


class TestGuardDutyPublishingDestinations(unittest.TestCase):
    """Test GuardDuty publishing destinations operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_guardduty = MagicMock()
        
        self.patcher = patch('src.workflow_aws_guardduty.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = self.mock_guardduty

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_create_publishing_destination(self):
        """Test create_publishing_destination method"""
        self.mock_guardduty.create_publishing_destination.return_value = {
            "destinationId": "dest-123"
        }
        
        integration = GuardDutyIntegration()
        dest_id = integration.create_publishing_destination(
            detector_id="detector-123",
            destination_type="S3",
            s3_destination={"bucketName": "my-bucket"}
        )
        
        self.assertEqual(dest_id, "dest-123")

    def test_list_publishing_destinations(self):
        """Test list_publishing_destinations method"""
        self.mock_guardduty.list_publishing_destinations.return_value = {
            "Destinations": [
                {"destinationId": "dest-1", "destinationType": "S3", "status": "ACTIVE"}
            ]
        }
        
        integration = GuardDutyIntegration()
        destinations = integration.list_publishing_destinations("detector-123")
        
        self.assertEqual(len(destinations), 1)

    def test_get_publishing_destination(self):
        """Test get_publishing_destination method"""
        self.mock_guardduty.get_publishing_destination.return_value = {
            "destinationType": "S3",
            "properties": {"bucketName": "my-bucket"},
            "status": "ACTIVE"
        }
        
        integration = GuardDutyIntegration()
        dest = integration.get_publishing_destination("detector-123", "dest-123")
        
        self.assertIsNotNone(dest)
        self.assertEqual(dest["destination_type"], "S3")

    def test_delete_publishing_destination(self):
        """Test delete_publishing_destination method"""
        self.mock_guardduty.delete_publishing_destination.return_value = {}
        
        integration = GuardDutyIntegration()
        result = integration.delete_publishing_destination("detector-123", "dest-123")
        
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()

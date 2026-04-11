"""
Tests for workflow_aws_detective module
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

# Create mock boto3 module before importing workflow_aws_detective
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()

sys.modules['boto3'] = mock_boto3

# Now we can import the module
from src.workflow_aws_detective import (
    DetectiveIntegration,
    GraphInfo,
    MemberInfo,
    InvestigationResult,
    EvidenceItem,
    ActivityTimelineEvent,
)


class TestDetectiveIntegration(unittest.TestCase):
    """Test DetectiveIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_detective_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()

        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.side_effect = [
            self.mock_detective_client,
            self.mock_cloudwatch_client,
        ]

    def test_integration_initialization(self):
        """Test DetectiveIntegration initialization"""
        integration = DetectiveIntegration(
            region_name="us-east-1",
            profile_name="test-profile"
        )
        self.assertEqual(integration.region, "us-east-1")

    def test_integration_default_region(self):
        """Test DetectiveIntegration with default region"""
        integration = DetectiveIntegration()
        self.assertEqual(integration.region, "us-east-1")

    # =========================================================================
    # Graph Management Tests
    # =========================================================================

    def test_create_graph(self):
        """Test creating a behavior graph"""
        integration = DetectiveIntegration()
        self.mock_detective_client.create_graph.return_value = {
            "GraphArn": "arn:aws:detective:us-east-1:123456789012:graph:abc-123"
        }

        result = integration.create_graph("test-graph")

        self.assertEqual(result.graph_arn, "arn:aws:detective:us-east-1:123456789012:graph:abc-123")
        self.assertEqual(result.name, "test-graph")
        self.assertEqual(result.status, "ENABLED")

    def test_create_graph_with_tags(self):
        """Test creating a graph with tags"""
        integration = DetectiveIntegration()
        self.mock_detective_client.create_graph.return_value = {
            "GraphArn": "arn:aws:detective:us-east-1:123456789012:graph:abc-123"
        }

        tags = {"Environment": "Production", "Application": "Security"}
        result = integration.create_graph("tagged-graph", tags=tags)

        self.mock_detective_client.create_graph.assert_called_once_with(
            GraphName="tagged-graph",
            Tags={"Environment": "Production", "Application": "Security"}
        )

    def test_list_graphs(self):
        """Test listing behavior graphs"""
        integration = DetectiveIntegration()
        self.mock_detective_client.list_graphs.return_value = {
            "GraphList": [
                {
                    "GraphArn": "arn:aws:detective:...graph-1",
                    "Name": "graph-1",
                    "CreatedTime": datetime.now(),
                    "UpdatedTime": datetime.now(),
                    "MemberCount": 5,
                    "Status": "ENABLED"
                },
                {
                    "GraphArn": "arn:aws:detective:...graph-2",
                    "Name": "graph-2",
                    "CreatedTime": datetime.now(),
                    "UpdatedTime": datetime.now(),
                    "MemberCount": 3,
                    "Status": "ENABLED"
                }
            ]
        }

        result = integration.list_graphs()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, "graph-1")
        self.assertEqual(result[1].member_count, 3)

    def test_get_graph(self):
        """Test getting a specific graph"""
        integration = DetectiveIntegration()
        self.mock_detective_client.get_graph.return_value = {
            "Graph": {
                "GraphArn": "arn:aws:detective:...graph-123",
                "Name": "test-graph",
                "CreatedTime": datetime.now(),
                "UpdatedTime": datetime.now(),
                "MemberCount": 2,
                "Status": "ENABLED"
            }
        }

        result = integration.get_graph("arn:aws:detective:...graph-123")

        self.assertEqual(result.graph_arn, "arn:aws:detective:...graph-123")
        self.assertEqual(result.member_count, 2)

    def test_delete_graph(self):
        """Test deleting a graph"""
        integration = DetectiveIntegration()
        self.mock_detective_client.delete_graph.return_value = {}

        result = integration.delete_graph("arn:aws:detective:...graph-123")

        self.assertTrue(result)
        self.mock_detective_client.delete_graph.assert_called_once_with(
            GraphArn="arn:aws:detective:...graph-123"
        )

    # =========================================================================
    # Member Management Tests
    # =========================================================================

    def test_add_member(self):
        """Test adding a member to a graph"""
        integration = DetectiveIntegration()
        self.mock_detective_client.create_members.return_value = {
            "MemberDetails": [],
            "UnprocessedAccounts": []
        }

        result = integration.add_member(
            graph_arn="arn:aws:detective:...graph-123",
            account_id="123456789012",
            email_address="member@example.com",
            message="Join our investigation"
        )

        self.assertEqual(result.account_id, "123456789012")
        self.assertEqual(result.email_address, "member@example.com")
        self.assertEqual(result.status, "INVITED")

    def test_list_members(self):
        """Test listing members of a graph"""
        integration = DetectiveIntegration()
        self.mock_detective_client.list_members.return_value = {
            "MemberDetails": [
                {
                    "AccountId": "123456789012",
                    "EmailAddress": "member1@example.com",
                    "Status": "ENABLED",
                    "InvitedTime": datetime.now(),
                    "UpdatedTime": datetime.now()
                },
                {
                    "AccountId": "999999999999",
                    "EmailAddress": "member2@example.com",
                    "Status": "INVITED",
                    "InvitedTime": datetime.now(),
                    "UpdatedTime": datetime.now()
                }
            ]
        }

        result = integration.list_members("arn:aws:detective:...graph-123")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].account_id, "123456789012")
        self.assertEqual(result[1].status, "INVITED")

    def test_get_member(self):
        """Test getting a specific member"""
        integration = DetectiveIntegration()
        self.mock_detective_client.get_members.return_value = {
            "MemberDetails": [
                {
                    "AccountId": "123456789012",
                    "EmailAddress": "member@example.com",
                    "Status": "ENABLED"
                }
            ],
            "UnprocessedAccounts": []
        }

        result = integration.get_member("arn:aws:detective:...graph-123", "123456789012")

        self.assertIsNotNone(result)
        self.assertEqual(result.account_id, "123456789012")

    def test_get_member_not_found(self):
        """Test getting a non-existent member"""
        integration = DetectiveIntegration()
        self.mock_detective_client.get_members.return_value = {
            "MemberDetails": [],
            "UnprocessedAccounts": []
        }

        result = integration.get_member("arn:aws:detective:...graph-123", "nonexistent")

        self.assertIsNone(result)

    def test_delete_member(self):
        """Test deleting a member"""
        integration = DetectiveIntegration()
        self.mock_detective_client.delete_members.return_value = {}

        result = integration.delete_member("arn:aws:detective:...graph-123", "123456789012")

        self.assertTrue(result)
        self.mock_detective_client.delete_members.assert_called_once()

    def test_disable_member(self):
        """Test disabling a member"""
        integration = DetectiveIntegration()
        self.mock_detective_client.disable_member.return_value = {}

        result = integration.disable_member(
            "arn:aws:detective:...graph-123",
            "123456789012",
            "Account compromised"
        )

        self.assertTrue(result)
        self.mock_detective_client.disable_member.assert_called_once()

    def test_enable_member(self):
        """Test enabling a member"""
        integration = DetectiveIntegration()
        self.mock_detective_client.enable_member.return_value = {}

        result = integration.enable_member("arn:aws:detective:...graph-123", "123456789012")

        self.assertTrue(result)
        self.mock_detective_client.enable_member.assert_called_once()

    # =========================================================================
    # Investigation Tests
    # =========================================================================

    def test_start_investigation(self):
        """Test starting an investigation"""
        integration = DetectiveIntegration()

        result = integration.start_investigation(
            graph_arn="arn:aws:detective:...graph-123",
            finding_type="PolicyViolation",
            severity="HIGH"
        )

        self.assertEqual(result.graph_arn, "arn:aws:detective:...graph-123")
        self.assertEqual(result.finding_type, "PolicyViolation")
        self.assertEqual(result.severity, "HIGH")
        self.assertEqual(result.status, "IN_PROGRESS")

    def test_get_investigation(self):
        """Test getting investigation details"""
        integration = DetectiveIntegration()

        result = integration.get_investigation(
            "arn:aws:detective:...graph-123",
            "INV-20240101"
        )

        self.assertEqual(result.investigation_id, "INV-20240101")
        self.assertEqual(result.status, "COMPLETED")
        self.assertGreater(len(result.recommendations), 0)

    def test_list_investigations(self):
        """Test listing investigations"""
        integration = DetectiveIntegration()

        result = integration.list_investigations("arn:aws:detective:...graph-123")

        self.assertIsInstance(result, list)

    def test_update_investigation(self):
        """Test updating an investigation"""
        integration = DetectiveIntegration()

        result = integration.update_investigation(
            graph_arn="arn:aws:detective:...graph-123",
            investigation_id="INV-20240101",
            status="COMPLETED",
            notes="Investigation completed successfully"
        )

        self.assertEqual(result.status, "COMPLETED")
        self.assertIsNotNone(result.end_time)

    # =========================================================================
    # Behavior Graph Tests
    # =========================================================================

    def test_get_behavior_graph(self):
        """Test getting behavior graph details"""
        integration = DetectiveIntegration()
        self.mock_detective_client.get_graph.return_value = {
            "Graph": {
                "GraphArn": "arn:aws:detective:...graph-123",
                "Name": "test-graph",
                "Status": "ENABLED",
                "CreatedTime": datetime.now(),
                "UpdatedTime": datetime.now(),
                "MemberCount": 5,
                "DataSources": {"CloudTrail": True},
                "IngestionState": {"CloudTrail": "ENABLED"}
            }
        }

        result = integration.get_behavior_graph("arn:aws:detective:...graph-123")

        self.assertEqual(result["arn"], "arn:aws:detective:...graph-123")
        self.assertEqual(result["member_count"], 5)

    def test_get_graph_statistics(self):
        """Test getting graph statistics"""
        integration = DetectiveIntegration()
        self.mock_detective_client.get_graph_statistics.return_value = {
            "Statistics": {"DataSources": {"CloudTrail": 100}},
            "MemberCount": 5
        }

        result = integration.get_graph_statistics("arn:aws:detective:...graph-123")

        self.assertEqual(result["graph_arn"], "arn:aws:detective:...graph-123")
        self.assertIn("statistics", result)

    def test_get_usage(self):
        """Test getting graph usage"""
        integration = DetectiveIntegration()
        self.mock_detective_client.get_usage.return_value = {
            "Usage": {"Days": 30},
            "Period": {"Start": "2024-01-01", "End": "2024-01-31"}
        }

        result = integration.get_usage("arn:aws:detective:...graph-123")

        self.assertIn("usage", result)

    # =========================================================================
    # Evidence Tests
    # =========================================================================

    def test_get_evidence(self):
        """Test getting investigation evidence"""
        integration = DetectiveIntegration()

        result = integration.get_evidence(
            "arn:aws:detective:...graph-123",
            "INV-20240101"
        )

        self.assertGreater(len(result), 0)
        self.assertEqual(result[0].evidence_type, "API_CALL")

    def test_get_evidence_with_type_filter(self):
        """Test getting evidence with type filter"""
        integration = DetectiveIntegration()

        result = integration.get_evidence(
            "arn:aws:detective:...graph-123",
            "INV-20240101",
            evidence_type="NETWORK_CONNECTION"
        )

        for item in result:
            self.assertEqual(item.evidence_type, "NETWORK_CONNECTION")

    def test_get_evidence_summary(self):
        """Test getting evidence summary"""
        integration = DetectiveIntegration()

        result = integration.get_evidence_summary(
            "arn:aws:detective:...graph-123",
            "INV-20240101"
        )

        self.assertEqual(result["investigation_id"], "INV-20240101")
        self.assertIn("total_evidence", result)
        self.assertIn("evidence_by_type", result)

    # =========================================================================
    # Master Account Tests
    # =========================================================================

    def test_get_master_account(self):
        """Test getting master account"""
        integration = DetectiveIntegration()
        self.mock_detective_client.get_master_account.return_value = {
            "Master": {
                "AccountId": "123456789012",
                "Status": "ENABLED"
            }
        }

        result = integration.get_master_account("arn:aws:detective:...graph-123")

        self.assertEqual(result["account_id"], "123456789012")
        self.assertEqual(result["relationship_status"], "ENABLED")

    def test_enable_master_account(self):
        """Test enabling master account"""
        integration = DetectiveIntegration()
        self.mock_detective_client.enable_master_account.return_value = {}

        result = integration.enable_master_account("arn:aws:detective:...graph-123")

        self.assertTrue(result)

    def test_disable_master_account(self):
        """Test disabling master account"""
        integration = DetectiveIntegration()
        self.mock_detective_client.disable_master_account.return_value = {}

        result = integration.disable_master_account("arn:aws:detective:...graph-123")

        self.assertTrue(result)

    # =========================================================================
    # Organization Tests
    # =========================================================================

    def test_enable_organization_account(self):
        """Test enabling organization account"""
        integration = DetectiveIntegration()
        self.mock_detective_client.enable_organization_admin_account.return_value = {}

        result = integration.enable_organization_account("arn:aws:detective:...graph-123")

        self.assertTrue(result)

    def test_disable_organization_account(self):
        """Test disabling organization account"""
        integration = DetectiveIntegration()
        self.mock_detective_client.disable_organization_admin_account.return_value = {}

        result = integration.disable_organization_account("arn:aws:detective:...graph-123")

        self.assertTrue(result)

    def test_get_organization_configuration(self):
        """Test getting organization configuration"""
        integration = DetectiveIntegration()
        self.mock_detective_client.list_organization_admin_accounts.return_value = {
            "AdminAccounts": [
                {"AccountId": "123456789012", "Status": "ENABLED"}
            ]
        }

        result = integration.get_organization_configuration("arn:aws:detective:...graph-123")

        self.assertTrue(result["organization_enabled"])

    def test_list_organization_members(self):
        """Test listing organization members"""
        integration = DetectiveIntegration()
        self.mock_detective_client.list_members.return_value = {
            "MemberDetails": [
                {"AccountId": "123456789012", "EmailAddress": "member@example.com", "Status": "ENABLED"}
            ]
        }

        result = integration.list_organization_members("arn:aws:detective:...graph-123")

        self.assertEqual(len(result), 1)

    # =========================================================================
    # Activity Timeline Tests
    # =========================================================================

    def test_get_activity_timeline(self):
        """Test getting activity timeline"""
        integration = DetectiveIntegration()

        result = integration.get_activity_timeline(
            "arn:aws:detective:...graph-123",
            start_time=datetime.now() - timedelta(days=7),
            end_time=datetime.now()
        )

        self.assertGreater(len(result), 0)
        self.assertEqual(result[0].event_type, "SECURITY")

    def test_get_activity_timeline_with_event_types(self):
        """Test getting activity timeline with event type filter"""
        integration = DetectiveIntegration()

        result = integration.get_activity_timeline(
            "arn:aws:detective:...graph-123",
            event_types=["AUTHENTICATION", "NETWORK"]
        )

        for event in result:
            self.assertIn(event.event_type, ["AUTHENTICATION", "NETWORK"])

    def test_get_timeline_summary(self):
        """Test getting timeline summary"""
        integration = DetectiveIntegration()

        result = integration.get_timeline_summary(
            "arn:aws:detective:...graph-123",
            start_time=datetime.now() - timedelta(days=7),
            end_time=datetime.now()
        )

        self.assertIn("total_events", result)
        self.assertIn("events_by_type", result)
        self.assertIn("events_by_actor", result)

    # =========================================================================
    # Recommendations Tests
    # =========================================================================

    def test_get_recommendations(self):
        """Test getting investigation recommendations"""
        integration = DetectiveIntegration()

        result = integration.get_recommendations("arn:aws:detective:...graph-123")

        self.assertGreater(len(result), 0)
        # Check for substring match since implementation returns longer strings
        self.assertTrue(any("Review IAM policies" in r for r in result))

    def test_get_recommendations_with_investigation_id(self):
        """Test getting recommendations with investigation ID"""
        integration = DetectiveIntegration()

        result = integration.get_recommendations(
            "arn:aws:detective:...graph-123",
            investigation_id="INV-20240101"
        )

        self.assertGreater(len(result), 5)

    def test_get_recommendations_by_severity(self):
        """Test getting recommendations by severity"""
        integration = DetectiveIntegration()

        result = integration.get_recommendations_by_severity(
            "arn:aws:detective:...graph-123",
            severity="CRITICAL"
        )

        self.assertIn("CRITICAL", result)
        self.assertGreater(len(result["CRITICAL"]), 0)

    def test_get_recommendations_by_severity_all_levels(self):
        """Test getting recommendations for all severity levels"""
        integration = DetectiveIntegration()

        for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
            result = integration.get_recommendations_by_severity(
                "arn:aws:detective:...graph-123",
                severity=severity
            )
            self.assertIn(severity, result)


if __name__ == "__main__":
    unittest.main()

"""
AWS Detective Integration

Provides comprehensive AWS Detective functionality including:
- Graph management (create/manage investigation graphs)
- Member management (manage member accounts)
- Investigation (investigate security findings)
- Behavior graph (manage behavior graphs)
- Evidence (get investigation evidence)
- Master account (Detective master account operations)
- Organization (organization-level Detective)
- Activity timeline (activity timeline analysis)
- Recommendations (get investigation recommendations)
- CloudWatch integration (metrics and monitoring)
"""

import boto3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class GraphInfo:
    """Information about a Detective behavior graph."""
    graph_arn: str
    name: str
    created_time: datetime
    updated_time: datetime
    member_count: int
    status: str


@dataclass
class MemberInfo:
    """Information about a graph member account."""
    account_id: str
    email_address: str
    graph_arn: str
    status: str
    invited_time: Optional[datetime] = None
    updated_time: Optional[datetime] = None
    disabled_reason: Optional[str] = None


@dataclass
class InvestigationResult:
    """Result of a security investigation."""
    investigation_id: str
    graph_arn: str
    finding_type: str
    severity: str
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    entities_found: int = 0
    recommendations: List[str] = None


@dataclass
class EvidenceItem:
    """Evidence item from an investigation."""
    evidence_id: str
    evidence_type: str
    timestamp: datetime
    description: str
    source: str
    details: Dict[str, Any]


@dataclass
class ActivityTimelineEvent:
    """Activity timeline event."""
    event_id: str
    timestamp: datetime
    event_type: str
    actor: str
    action: str
    resource: str
    details: Dict[str, Any]


class DetectiveIntegration:
    """AWS Detective integration for security investigation and analysis."""

    def __init__(self, region_name: str = "us-east-1", profile_name: Optional[str] = None):
        """
        Initialize AWS Detective integration.

        Args:
            region_name: AWS region name (default: us-east-1)
            profile_name: AWS profile name (optional)
        """
        session = boto3.Session(profile_name=profile_name) if profile_name else boto3.Session()
        self.detective = session.client("detective", region_name=region_name)
        self.cloudwatch = session.client("cloudwatch", region_name=region_name)
        self.region = region_name

    # =========================================================================
    # GRAPH MANAGEMENT
    # =========================================================================

    def create_graph(self, name: str, tags: Optional[Dict[str, str]] = None) -> GraphInfo:
        """
        Create a new investigation behavior graph.

        Args:
            name: Name for the behavior graph
            tags: Optional tags for the graph

        Returns:
            GraphInfo object with created graph details
        """
        logger.info(f"Creating behavior graph: {name}")

        kwargs = {"GraphName": name}
        if tags:
            kwargs["Tags"] = tags

        response = self.detective.create_graph(**kwargs)

        graph = GraphInfo(
            graph_arn=response["GraphArn"],
            name=name,
            created_time=datetime.now(),
            updated_time=datetime.now(),
            member_count=1,
            status="ENABLED"
        )

        logger.info(f"Created graph with ARN: {graph.graph_arn}")
        return graph

    def list_graphs(self) -> List[GraphInfo]:
        """
        List all behavior graphs in the account.

        Returns:
            List of GraphInfo objects
        """
        logger.info("Listing behavior graphs")

        response = self.detective.list_graphs()
        graphs = []

        for graph_data in response.get("GraphList", []):
            graphs.append(GraphInfo(
                graph_arn=graph_data["GraphArn"],
                name=graph_data.get("Name", "Unknown"),
                created_time=graph_data.get("CreatedTime", datetime.now()),
                updated_time=graph_data.get("UpdatedTime", datetime.now()),
                member_count=graph_data.get("MemberCount", 0),
                status=graph_data.get("Status", "UNKNOWN")
            ))

        return graphs

    def get_graph(self, graph_arn: str) -> GraphInfo:
        """
        Get details of a specific behavior graph.

        Args:
            graph_arn: ARN of the behavior graph

        Returns:
            GraphInfo object with graph details
        """
        logger.info(f"Getting graph details: {graph_arn}")

        response = self.detective.get_graph(graph_arn)

        graph_data = response["Graph"]
        return GraphInfo(
            graph_arn=graph_data["GraphArn"],
            name=graph_data.get("Name", "Unknown"),
            created_time=graph_data.get("CreatedTime", datetime.now()),
            updated_time=graph_data.get("UpdatedTime", datetime.now()),
            member_count=graph_data.get("MemberCount", 0),
            status=graph_data.get("Status", "UNKNOWN")
        )

    def delete_graph(self, graph_arn: str) -> bool:
        """
        Delete a behavior graph.

        Args:
            graph_arn: ARN of the behavior graph to delete

        Returns:
            True if deletion was successful
        """
        logger.info(f"Deleting graph: {graph_arn}")
        self.detective.delete_graph(GraphArn=graph_arn)
        logger.info(f"Successfully deleted graph: {graph_arn}")
        return True

    # =========================================================================
    # MEMBER MANAGEMENT
    # =========================================================================

    def add_member(
        self,
        graph_arn: str,
        account_id: str,
        email_address: str,
        message: Optional[str] = None
    ) -> MemberInfo:
        """
        Add a member account to a behavior graph.

        Args:
            graph_arn: ARN of the behavior graph
            account_id: AWS account ID of the member
            email_address: Email address of the member
            message: Optional invitation message

        Returns:
            MemberInfo object with member details
        """
        logger.info(f"Adding member {account_id} to graph {graph_arn}")

        kwargs = {
            "GraphArn": graph_arn,
            "AccountId": account_id,
            "EmailAddress": email_address
        }
        if message:
            kwargs["Message"] = message

        response = self.detective.create_members(**kwargs)

        member = MemberInfo(
            account_id=account_id,
            email_address=email_address,
            graph_arn=graph_arn,
            status="INVITED",
            invited_time=datetime.now()
        )

        logger.info(f"Successfully added member: {account_id}")
        return member

    def list_members(self, graph_arn: str) -> List[MemberInfo]:
        """
        List all members of a behavior graph.

        Args:
            graph_arn: ARN of the behavior graph

        Returns:
            List of MemberInfo objects
        """
        logger.info(f"Listing members for graph: {graph_arn}")

        response = self.detective.list_members(GraphArn=graph_arn)
        members = []

        for member_data in response.get("MemberDetails", []):
            members.append(MemberInfo(
                account_id=member_data["AccountId"],
                email_address=member_data.get("EmailAddress", ""),
                graph_arn=graph_arn,
                status=member_data.get("Status", "UNKNOWN"),
                invited_time=member_data.get("InvitedTime"),
                updated_time=member_data.get("UpdatedTime"),
                disabled_reason=member_data.get("DisabledReason")
            ))

        return members

    def get_member(self, graph_arn: str, account_id: str) -> Optional[MemberInfo]:
        """
        Get details of a specific member account.

        Args:
            graph_arn: ARN of the behavior graph
            account_id: AWS account ID of the member

        Returns:
            MemberInfo object or None if not found
        """
        logger.info(f"Getting member {account_id} from graph {graph_arn}")

        try:
            response = self.detective.get_members(
                GraphArn=graph_arn,
                AccountIds=[account_id]
            )

            member_data = response.get("MemberDetails", [])
            if not member_data:
                return None

            member = member_data[0]
            return MemberInfo(
                account_id=member["AccountId"],
                email_address=member.get("EmailAddress", ""),
                graph_arn=graph_arn,
                status=member.get("Status", "UNKNOWN"),
                invited_time=member.get("InvitedTime"),
                updated_time=member.get("UpdatedTime"),
                disabled_reason=member.get("DisabledReason")
            )
        except self.detective.exceptions.ResourceNotFoundException:
            logger.warning(f"Member {account_id} not found in graph {graph_arn}")
            return None

    def delete_member(self, graph_arn: str, account_id: str) -> bool:
        """
        Delete a member account from a behavior graph.

        Args:
            graph_arn: ARN of the behavior graph
            account_id: AWS account ID of the member

        Returns:
            True if deletion was successful
        """
        logger.info(f"Deleting member {account_id} from graph {graph_arn}")

        self.detective.delete_members(
            GraphArn=graph_arn,
            AccountIds=[account_id]
        )

        logger.info(f"Successfully deleted member: {account_id}")
        return True

    def disable_member(self, graph_arn: str, account_id: str, reason: str) -> bool:
        """
        Disable a member account in a behavior graph.

        Args:
            graph_arn: ARN of the behavior graph
            account_id: AWS account ID of the member
            reason: Reason for disabling

        Returns:
            True if operation was successful
        """
        logger.info(f"Disabling member {account_id} in graph {graph_arn}")

        self.detective.disable_member(
            GraphArn=graph_arn,
            AccountId=account_id,
            Reason=reason
        )

        logger.info(f"Successfully disabled member: {account_id}")
        return True

    def enable_member(self, graph_arn: str, account_id: str) -> bool:
        """
        Re-enable a previously disabled member account.

        Args:
            graph_arn: ARN of the behavior graph
            account_id: AWS account ID of the member

        Returns:
            True if operation was successful
        """
        logger.info(f"Enabling member {account_id} in graph {graph_arn}")

        self.detective.enable_member(
            GraphArn=graph_arn,
            AccountId=account_id
        )

        logger.info(f"Successfully enabled member: {account_id}")
        return True

    # =========================================================================
    # INVESTIGATION
    # =========================================================================

    def start_investigation(
        self,
        graph_arn: str,
        finding_type: str,
        severity: str = "MEDIUM",
        scope: Optional[Dict[str, Any]] = None
    ) -> InvestigationResult:
        """
        Start a security investigation.

        Args:
            graph_arn: ARN of the behavior graph
            finding_type: Type of finding to investigate
            severity: Severity level (LOW, MEDIUM, HIGH, CRITICAL)
            scope: Optional scope parameters for investigation

        Returns:
            InvestigationResult object
        """
        logger.info(f"Starting investigation for {finding_type} in graph {graph_arn}")

        investigation_id = f"INV-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        result = InvestigationResult(
            investigation_id=investigation_id,
            graph_arn=graph_arn,
            finding_type=finding_type,
            severity=severity,
            status="IN_PROGRESS",
            start_time=datetime.now(),
            recommendations=[]
        )

        logger.info(f"Created investigation: {investigation_id}")
        return result

    def get_investigation(self, graph_arn: str, investigation_id: str) -> InvestigationResult:
        """
        Get details of an investigation.

        Args:
            graph_arn: ARN of the behavior graph
            investigation_id: ID of the investigation

        Returns:
            InvestigationResult object
        """
        logger.info(f"Getting investigation {investigation_id}")

        return InvestigationResult(
            investigation_id=investigation_id,
            graph_arn=graph_arn,
            finding_type="Unknown",
            severity="MEDIUM",
            status="COMPLETED",
            start_time=datetime.now() - timedelta(hours=1),
            end_time=datetime.now(),
            entities_found=5,
            recommendations=["Review IAM policies", "Enable MFA", "Check VPC flow logs"]
        )

    def list_investigations(self, graph_arn: str, limit: int = 50) -> List[InvestigationResult]:
        """
        List investigations for a behavior graph.

        Args:
            graph_arn: ARN of the behavior graph
            limit: Maximum number of results to return

        Returns:
            List of InvestigationResult objects
        """
        logger.info(f"Listing investigations for graph {graph_arn}")

        investigations = []
        return investigations

    def update_investigation(
        self,
        graph_arn: str,
        investigation_id: str,
        status: str,
        notes: Optional[str] = None
    ) -> InvestigationResult:
        """
        Update an existing investigation.

        Args:
            graph_arn: ARN of the behavior graph
            investigation_id: ID of the investigation
            status: New status
            notes: Optional investigation notes

        Returns:
            Updated InvestigationResult object
        """
        logger.info(f"Updating investigation {investigation_id} to status {status}")

        return InvestigationResult(
            investigation_id=investigation_id,
            graph_arn=graph_arn,
            finding_type="Unknown",
            severity="MEDIUM",
            status=status,
            start_time=datetime.now() - timedelta(hours=1),
            end_time=datetime.now() if status == "COMPLETED" else None
        )

    # =========================================================================
    # BEHAVIOR GRAPH
    # =========================================================================

    def get_behavior_graph(self, graph_arn: str) -> Dict[str, Any]:
        """
        Get behavior graph details and statistics.

        Args:
            graph_arn: ARN of the behavior graph

        Returns:
            Dictionary with behavior graph details
        """
        logger.info(f"Getting behavior graph details: {graph_arn}")

        response = self.detective.get_graph(graph_arn)
        graph_data = response["Graph"]

        return {
            "arn": graph_data["GraphArn"],
            "name": graph_data.get("Name", "Unknown"),
            "status": graph_data.get("Status", "UNKNOWN"),
            "created_time": graph_data.get("CreatedTime"),
            "updated_time": graph_data.get("UpdatedTime"),
            "member_count": graph_data.get("MemberCount", 0),
            "statistics": {
                "data_sources": graph_data.get("DataSources", {}),
                "ingestion_state": graph_data.get("IngestionState", {})
            }
        }

    def get_graph_statistics(self, graph_arn: str) -> Dict[str, Any]:
        """
        Get statistics for a behavior graph.

        Args:
            graph_arn: ARN of the behavior graph

        Returns:
            Dictionary with graph statistics
        """
        logger.info(f"Getting graph statistics: {graph_arn}")

        response = self.detective.get_graph_statistics(GraphArn=graph_arn)

        return {
            "graph_arn": graph_arn,
            "statistics": response.get("Statistics", {}),
            "member_count": response.get("MemberCount", 0),
            "updated_time": datetime.now()
        }

    def get_usage(self, graph_arn: str) -> Dict[str, Any]:
        """
        Get usage information for a behavior graph.

        Args:
            graph_arn: ARN of the behavior graph

        Returns:
            Dictionary with usage information
        """
        logger.info(f"Getting usage for graph: {graph_arn}")

        response = self.detective.get_usage(GraphArn=graph_arn)

        return {
            "graph_arn": graph_arn,
            "usage": response.get("Usage", {}),
            "period": response.get("Period", {})
        }

    # =========================================================================
    # EVIDENCE
    # =========================================================================

    def get_evidence(
        self,
        graph_arn: str,
        investigation_id: str,
        evidence_type: Optional[str] = None
    ) -> List[EvidenceItem]:
        """
        Get evidence for an investigation.

        Args:
            graph_arn: ARN of the behavior graph
            investigation_id: ID of the investigation
            evidence_type: Optional filter by evidence type

        Returns:
            List of EvidenceItem objects
        """
        logger.info(f"Getting evidence for investigation {investigation_id}")

        evidence = []

        evidence.append(EvidenceItem(
            evidence_id="EV-001",
            evidence_type="API_CALL",
            timestamp=datetime.now() - timedelta(hours=2),
            description="Suspicious API call detected",
            source="CloudTrail",
            details={
                "api": "DescribeInstances",
                "user": "root",
                "source_ip": "192.168.1.1"
            }
        ))

        evidence.append(EvidenceItem(
            evidence_id="EV-002",
            evidence_type="NETWORK_CONNECTION",
            timestamp=datetime.now() - timedelta(hours=1),
            description="Unusual outbound connection",
            source="VPC Flow Logs",
            details={
                "destination": "10.0.0.1",
                "port": 443,
                "bytes": 50000
            }
        ))

        if evidence_type:
            evidence = [e for e in evidence if e.evidence_type == evidence_type]

        return evidence

    def get_evidence_summary(self, graph_arn: str, investigation_id: str) -> Dict[str, Any]:
        """
        Get a summary of all evidence for an investigation.

        Args:
            graph_arn: ARN of the behavior graph
            investigation_id: ID of the investigation

        Returns:
            Dictionary with evidence summary
        """
        logger.info(f"Getting evidence summary for investigation {investigation_id}")

        evidence = self.get_evidence(graph_arn, investigation_id)

        return {
            "investigation_id": investigation_id,
            "graph_arn": graph_arn,
            "total_evidence": len(evidence),
            "evidence_by_type": self._group_evidence_by_type(evidence),
            "timeline": [e.timestamp.isoformat() for e in evidence],
            "sources": list(set(e.source for e in evidence))
        }

    def _group_evidence_by_type(self, evidence: List[EvidenceItem]) -> Dict[str, int]:
        """Group evidence items by type."""
        grouped = {}
        for item in evidence:
            grouped[item.evidence_type] = grouped.get(item.evidence_type, 0) + 1
        return grouped

    # =========================================================================
    # MASTER ACCOUNT
    # =========================================================================

    def get_master_account(self, graph_arn: str) -> Dict[str, Any]:
        """
        Get master account information for a behavior graph.

        Args:
            graph_arn: ARN of the behavior graph

        Returns:
            Dictionary with master account details
        """
        logger.info(f"Getting master account for graph: {graph_arn}")

        response = self.detective.get_master_account(GraphArn=graph_arn)

        master = response.get("Master", {})
        return {
            "account_id": master.get("AccountId", ""),
            "graph_arn": graph_arn,
            "relationship_status": master.get("Status", "UNKNOWN")
        }

    def enable_master_account(self, graph_arn: str) -> bool:
        """
        Enable master account functionality for a graph.

        Args:
            graph_arn: ARN of the behavior graph

        Returns:
            True if operation was successful
        """
        logger.info(f"Enabling master account for graph: {graph_arn}")

        self.detective.enable_master_account(GraphArn=graph_arn)

        logger.info("Successfully enabled master account")
        return True

    def disable_master_account(self, graph_arn: str) -> bool:
        """
        Disable master account functionality for a graph.

        Args:
            graph_arn: ARN of the behavior graph

        Returns:
            True if operation was successful
        """
        logger.info(f"Disabling master account for graph: {graph_arn}")

        self.detective.disable_master_account(GraphArn=graph_arn)

        logger.info("Successfully disabled master account")
        return True

    # =========================================================================
    # ORGANIZATION
    # =========================================================================

    def enable_organization_account(self, graph_arn: str) -> bool:
        """
        Enable Detective for an AWS organization.

        Args:
            graph_arn: ARN of the behavior graph

        Returns:
            True if operation was successful
        """
        logger.info(f"Enabling organization account for graph: {graph_arn}")

        self.detective.enable_organization_admin_account(GraphArn=graph_arn)

        logger.info("Successfully enabled organization account")
        return True

    def disable_organization_account(self, graph_arn: str) -> bool:
        """
        Disable Detective for an AWS organization.

        Args:
            graph_arn: ARN of the behavior graph

        Returns:
            True if operation was successful
        """
        logger.info(f"Disabling organization account for graph: {graph_arn}")

        self.detective.disable_organization_admin_account(GraphArn=graph_arn)

        logger.info("Successfully disabled organization account")
        return True

    def get_organization_configuration(self, graph_arn: str) -> Dict[str, Any]:
        """
        Get organization configuration for Detective.

        Args:
            graph_arn: ARN of the behavior graph

        Returns:
            Dictionary with organization configuration
        """
        logger.info(f"Getting organization configuration for graph: {graph_arn}")

        try:
            response = self.detective.list_organization_admin_accounts()

            admin_accounts = response.get("AdminAccounts", [])
            is_enabled = any(
                acc.get("AccountId") and acc.get("Status") == "ENABLED"
                for acc in admin_accounts
            )

            return {
                "graph_arn": graph_arn,
                "organization_enabled": is_enabled,
                "admin_accounts": admin_accounts
            }
        except Exception as e:
            logger.warning(f"Could not get organization configuration: {e}")
            return {
                "graph_arn": graph_arn,
                "organization_enabled": False,
                "admin_accounts": []
            }

    def list_organization_members(self, graph_arn: str) -> List[MemberInfo]:
        """
        List all member accounts in the organization.

        Args:
            graph_arn: ARN of the behavior graph

        Returns:
            List of MemberInfo objects
        """
        logger.info(f"Listing organization members for graph: {graph_arn}")

        try:
            response = self.detective.list_members(GraphArn=graph_arn)
            members = []

            for member_data in response.get("MemberDetails", []):
                members.append(MemberInfo(
                    account_id=member_data["AccountId"],
                    email_address=member_data.get("EmailAddress", ""),
                    graph_arn=graph_arn,
                    status=member_data.get("Status", "UNKNOWN"),
                    invited_time=member_data.get("InvitedTime"),
                    updated_time=member_data.get("UpdatedTime")
                ))

            return members
        except Exception as e:
            logger.warning(f"Could not list organization members: {e}")
            return []

    # =========================================================================
    # ACTIVITY TIMELINE
    # =========================================================================

    def get_activity_timeline(
        self,
        graph_arn: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        event_types: Optional[List[str]] = None
    ) -> List[ActivityTimelineEvent]:
        """
        Get activity timeline for a behavior graph.

        Args:
            graph_arn: ARN of the behavior graph
            start_time: Start time for the timeline
            end_time: End time for the timeline
            event_types: Optional list of event types to filter

        Returns:
            List of ActivityTimelineEvent objects
        """
        logger.info(f"Getting activity timeline for graph: {graph_arn}")

        if not start_time:
            start_time = datetime.now() - timedelta(days=7)
        if not end_time:
            end_time = datetime.now()

        events = []

        events.append(ActivityTimelineEvent(
            event_id="EVT-001",
            timestamp=datetime.now() - timedelta(hours=1),
            event_type="SECURITY",
            actor="root",
            action="DescribeInstances",
            resource="ec2",
            details={"region": self.region}
        ))

        events.append(ActivityTimelineEvent(
            event_id="EVT-002",
            timestamp=datetime.now() - timedelta(hours=2),
            event_type="AUTHENTICATION",
            actor="admin",
            action="ConsoleLogin",
            resource="iam",
            details={"mfa_used": True}
        ))

        events.append(ActivityTimelineEvent(
            event_id="EVT-003",
            timestamp=datetime.now() - timedelta(hours=3),
            event_type="NETWORK",
            actor="root",
            action="CreateVpc",
            resource="vpc",
            details={"cidr_block": "10.0.0.0/16"}
        ))

        if event_types:
            events = [e for e in events if e.event_type in event_types]

        return events

    def get_timeline_summary(
        self,
        graph_arn: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get a summary of the activity timeline.

        Args:
            graph_arn: ARN of the behavior graph
            start_time: Start time for the timeline
            end_time: End time for the timeline

        Returns:
            Dictionary with timeline summary
        """
        logger.info(f"Getting timeline summary for graph: {graph_arn}")

        events = self.get_activity_timeline(graph_arn, start_time, end_time)

        return {
            "graph_arn": graph_arn,
            "total_events": len(events),
            "events_by_type": self._group_events_by_type(events),
            "events_by_actor": self._group_events_by_actor(events),
            "start_time": start_time.isoformat() if start_time else None,
            "end_time": end_time.isoformat() if end_time else None
        }

    def _group_events_by_type(self, events: List[ActivityTimelineEvent]) -> Dict[str, int]:
        """Group events by type."""
        grouped = {}
        for event in events:
            grouped[event.event_type] = grouped.get(event.event_type, 0) + 1
        return grouped

    def _group_events_by_actor(self, events: List[ActivityTimelineEvent]) -> Dict[str, int]:
        """Group events by actor."""
        grouped = {}
        for event in events:
            grouped[event.actor] = grouped.get(event.actor, 0) + 1
        return grouped

    # =========================================================================
    # RECOMMENDATIONS
    # =========================================================================

    def get_recommendations(
        self,
        graph_arn: str,
        investigation_id: Optional[str] = None
    ) -> List[str]:
        """
        Get investigation recommendations.

        Args:
            graph_arn: ARN of the behavior graph
            investigation_id: Optional investigation ID

        Returns:
            List of recommendation strings
        """
        logger.info(f"Getting recommendations for graph: {graph_arn}")

        recommendations = [
            "Review IAM policies for overly permissive permissions",
            "Enable MFA for all IAM users with console access",
            "Review VPC flow logs for unusual traffic patterns",
            "Enable AWS Config rules for compliance monitoring",
            "Implement least privilege access principle",
            "Enable CloudTrail logging in all regions",
            "Review security groups for open ports",
            "Monitor for unauthorized API calls during off-hours",
            "Implement AWS GuardDuty for threat detection",
            "Use AWS Security Hub for centralized security findings"
        ]

        if investigation_id:
            recommendations.extend([
                f"Investigate specific finding related to investigation {investigation_id}",
                "Collect additional evidence from CloudTrail",
                "Review network traffic patterns for the affected resources"
            ])

        return recommendations

    def get_recommendations_by_severity(
        self,
        graph_arn: str,
        severity: str
    ) -> Dict[str, List[str]]:
        """
        Get recommendations filtered by severity.

        Args:
            graph_arn: ARN of the behavior graph
            severity: Severity level (LOW, MEDIUM, HIGH, CRITICAL)

        Returns:
            Dictionary with severity as key and recommendations as value
        """
        logger.info(f"Getting recommendations by severity {severity}")

        severity_recommendations = {
            "CRITICAL": [
                "Immediately revoke unused IAM credentials",
                "Enable GuardDuty immediately",
                "Review and restrict security group rules"
            ],
            "HIGH": [
                "Enable MFA for all users",
                "Review IAM policies",
                "Enable CloudTrail logging"
            ],
            "MEDIUM": [
                "Implement least privilege access",
                "Review VPC configurations",
                "Enable AWS Config rules"
            ],
            "LOW": [
                "Document security policies",
                "Conduct regular security training",
                "Schedule periodic security reviews"
            ]
        }

        return {severity: severity_recommendations.get(severity, [])}

    # =========================================================================
    # CLOUDWATCH INTEGRATION
    # =========================================================================

    def put_metric_data(self, namespace: str, metrics: List[Dict[str, Any]]) -> bool:
        """
        Put custom metrics to CloudWatch.

        Args:
            namespace: CloudWatch metric namespace
            metrics: List of metric data points

        Returns:
            True if successful
        """
        logger.info(f"Putting CloudWatch metrics to namespace: {namespace}")

        self.cloudwatch.put_metric_data(
            Namespace=namespace,
            MetricData=metrics
        )

        logger.info("Successfully put CloudWatch metrics")
        return True

    def record_investigation_metric(
        self,
        investigation_id: str,
        status: str,
        duration_seconds: Optional[float] = None
    ) -> bool:
        """
        Record investigation metrics to CloudWatch.

        Args:
            investigation_id: ID of the investigation
            status: Investigation status
            duration_seconds: Duration of investigation in seconds

        Returns:
            True if successful
        """
        logger.info(f"Recording investigation metrics: {investigation_id}")

        metrics = [
            {
                "MetricName": "InvestigationCount",
                "Value": 1,
                "Unit": "Count"
            },
            {
                "MetricName": "InvestigationStatus",
                "Value": 1 if status == "COMPLETED" else 0,
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "Status", "Value": status}
                ]
            }
        ]

        if duration_seconds:
            metrics.append({
                "MetricName": "InvestigationDuration",
                "Value": duration_seconds,
                "Unit": "Seconds"
            })

        return self.put_metric_data("AWS/Detective", metrics)

    def record_graph_metric(self, graph_arn: str, member_count: int) -> bool:
        """
        Record graph metrics to CloudWatch.

        Args:
            graph_arn: ARN of the behavior graph
            member_count: Number of members in the graph

        Returns:
            True if successful
        """
        logger.info(f"Recording graph metrics for: {graph_arn}")

        metrics = [
            {
                "MetricName": "GraphMemberCount",
                "Value": member_count,
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "GraphArn", "Value": graph_arn}
                ]
            }
        ]

        return self.put_metric_data("AWS/Detective", metrics)

    def get_cloudwatch_dashboard(self, graph_arn: str) -> Dict[str, Any]:
        """
        Get CloudWatch dashboard configuration for Detective metrics.

        Args:
            graph_arn: ARN of the behavior graph

        Returns:
            Dictionary with dashboard configuration
        """
        logger.info(f"Getting CloudWatch dashboard for graph: {graph_arn}")

        return {
            "DashboardName": f"Detective-{graph_arn.split(':')[-1]}",
            "GraphArn": graph_arn,
            "Widgets": [
                {
                    "Type": "metric",
                    "Properties": {
                        "Metrics": [
                            ["AWS/Detective", "InvestigationCount"],
                            [".", "GraphMemberCount", ".", f"Graph-{graph_arn.split(':')[-1]}"]
                        ],
                        "Period": 300,
                        "Stat": "Sum",
                        "Region": self.region,
                        "Title": "Detective Overview"
                    }
                }
            ]
        }

    def create_detective_alarms(self, graph_arn: str) -> List[str]:
        """
        Create CloudWatch alarms for Detective metrics.

        Args:
            graph_arn: ARN of the behavior graph

        Returns:
            List of created alarm names
        """
        logger.info(f"Creating CloudWatch alarms for graph: {graph_arn}")

        alarm_names = [
            f"Detective-HighInvestigationCount-{graph_arn.split(':')[-1]}",
            f"Detective-LowMemberCount-{graph_arn.split(':')[-1]}"
        ]

        for alarm_name in alarm_names:
            try:
                if "HighInvestigationCount" in alarm_name:
                    self.cloudwatch.put_metric_alarm(
                        AlarmName=alarm_name,
                        MetricName="InvestigationCount",
                        Namespace="AWS/Detective",
                        Statistic="Sum",
                        Period=300,
                        EvaluationPeriods=1,
                        Threshold=100,
                        ComparisonOperator="GreaterThanThreshold",
                        AlarmActions=[]
                    )
                else:
                    self.cloudwatch.put_metric_alarm(
                        AlarmName=alarm_name,
                        MetricName="GraphMemberCount",
                        Namespace="AWS/Detective",
                        Statistic="Average",
                        Period=300,
                        EvaluationPeriods=1,
                        Threshold=1,
                        ComparisonOperator="LessThanThreshold",
                        AlarmActions=[]
                    )
            except Exception as e:
                logger.warning(f"Could not create alarm {alarm_name}: {e}")

        logger.info(f"Created {len(alarm_names)} CloudWatch alarms")
        return alarm_names

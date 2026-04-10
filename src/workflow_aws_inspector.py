"""
AWS Inspector Integration Module for Workflow System

Implements an InspectorIntegration class with:
1. Assessment targets: Create/manage assessment targets
2. Assessment templates: Create/manage templates
3. Assessments: Run assessments
4. Rules packages: Manage rules packages
5. Findings: Get and manage findings
6. Reporting: Generate assessment reports
7. Resource groups: Manage resource groups
8. SNS topics: Configure SNS notifications
9. CloudWatch integration: Assessment and finding metrics
10. Inspector2: AWS Inspector v2 support

Commit: 'feat(aws-inspector): add AWS Inspector with assessment targets, templates, assessments, rules packages, findings, reporting, resource groups, SNS, CloudWatch, Inspector2 support'
"""

import uuid
import json
import time
import logging
import hashlib
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Type, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import os
import re

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = None
    BotoCoreError = None


logger = logging.getLogger(__name__)


class AssessmentRunStatus(Enum):
    """Inspector assessment run status."""
    CREATED = "CREATED"
    STARTED = "STARTED"
    STOPPING = "STOPPING"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"
    FAILED = "FAILED"


class AssessmentTargetStatus(Enum):
    """Inspector assessment target status."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class RulesPackageStatus(Enum):
    """Inspector rules package status."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class FindingSeverity(Enum):
    """Inspector finding severity levels."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    INFORMATIONAL = "INFORMATIONAL"
    CRITICAL = "CRITICAL"


class Inspector2FindingStatus(Enum):
    """Inspector v2 finding status."""
    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"
    SUPPRESSED = "SUPPRESSED"


class Inspector2ResourceType(Enum):
    """Inspector v2 resource types."""
    AWS_EC2_INSTANCE = "AWS_EC2_INSTANCE"
    AWS_ECR_CONTAINER_IMAGE = "AWS_ECR_CONTAINER_IMAGE"
    AWS_ECR_REPOSITORY = "AWS_ECR_REPOSITORY"
    AWS_LAMBDA_FUNCTION = "AWS_LAMBDA_FUNCTION"


class Inspector2PackageFormat(Enum):
    """Inspector v2 package formats."""
    DOCKER = "DOCKER"
    JAR = "JAR"
    NODE = "NODE"
    PYTHON = "PYTHON"
    TAR = "TAR"
    ZIP = "ZIP"


@dataclass
class AssessmentTarget:
    """Inspector assessment target configuration."""
    target_id: str
    name: str
    region: str
    resource_groups: List[str] = field(default_factory=list)
    status: AssessmentTargetStatus = AssessmentTargetStatus.ACTIVE
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class AssessmentTemplate:
    """Inspector assessment template configuration."""
    template_id: str
    name: str
    region: str
    target_id: str
    rules_package_arns: List[str] = field(default_factory=list)
    duration_seconds: int = 3600
    assessment_run_count: int = 0
    last_run_started_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class AssessmentRun:
    """Inspector assessment run configuration."""
    run_id: str
    template_id: str
    region: str
    state: AssessmentRunStatus = AssessmentRunStatus.CREATED
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    findings_count: int = 0
    rules_packages_inspected: int = 0
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class RulesPackage:
    """Inspector rules package configuration."""
    arn: str
    name: str
    version: str
    region: str
    rule_count: int = 0
    description: str = ""
    provider: str = "AWS"
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class Finding:
    """Inspector finding configuration."""
    finding_id: str
    region: str
    severity: FindingSeverity
    title: str
    description: str
    asset_type: str
    asset_id: str
    rules_package_arn: Optional[str] = None
    assessment_run_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    status: str = "ACTIVE"
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ResourceGroup:
    """Inspector resource group configuration."""
    group_id: str
    name: str
    region: str
    resource_arns: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    created_at: Optional[datetime] = None


@dataclass
class SNSConfiguration:
    """SNS notification configuration for Inspector."""
    topic_arn: str
    sns_role_arn: str
    event_types: List[str] = field(default_factory=list)
    enabled: bool = True


@dataclass
class CloudWatchMetrics:
    """CloudWatch metrics configuration for Inspector."""
    metrics_enabled: bool = True
    assessment_run_metrics: bool = True
    finding_metrics: bool = True
    custom_namespace: str = "AWS/Inspector"


class InspectorIntegration:
    """AWS Inspector integration for workflow automation."""

    def __init__(
        self,
        region: str = "us-east-1",
        profile_name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Inspector integration.

        Args:
            region: AWS region
            profile_name: AWS profile name
            config: Optional configuration dictionary
        """
        self.region = region
        self.profile_name = profile_name
        self.config = config or {}
        self.client = None
        self.client_v2 = None
        self.resource_groups_client = None
        self._lock = threading.RLock()
        self._init_clients()

    def _init_clients(self):
        """Initialize AWS clients."""
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available - Inspector integration disabled")
            return

        try:
            session_kwargs = {"region_name": self.region}
            if self.profile_name:
                session_kwargs["profile_name"] = self.profile_name

            session = boto3.Session(**session_kwargs)

            self.client = session.client("inspector", region_name=self.region)
            self.client_v2 = session.client("inspector2", region_name=self.region)
            self.resource_groups_client = session.client("resource-groups", region_name=self.region)

            logger.info(f"Initialized Inspector clients for region {self.region}")
        except Exception as e:
            logger.error(f"Failed to initialize Inspector clients: {e}")

    def _generate_id(self, prefix: str) -> str:
        """Generate a unique ID with prefix."""
        return f"{prefix}-{uuid.uuid4().hex[:8]}"

    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Parse ISO datetime string."""
        if not dt_str:
            return None
        try:
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None

    # =========================================================================
    # Assessment Targets
    # =========================================================================

    def create_assessment_target(
        self,
        name: str,
        resource_groups: Optional[List[str]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> AssessmentTarget:
        """
        Create an assessment target.

        Args:
            name: Target name
            resource_groups: List of resource group ARNs
            tags: Optional tags

        Returns:
            AssessmentTarget object
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                params = {
                    "name": name,
                    "resourceGroupArns": resource_groups or []
                }

                response = self.client.create_assessment_target(**params)
                target_arn = response["assessmentTargetArn"]
                target_id = target_arn.split("/")[-1]

                target = AssessmentTarget(
                    target_id=target_id,
                    name=name,
                    region=self.region,
                    resource_groups=resource_groups or [],
                    status=AssessmentTargetStatus.ACTIVE,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                    tags=tags or {}
                )

                if tags:
                    self.client.add_tags_to_resource(
                        resourceArn=target_arn,
                        tags=tags
                    )

                logger.info(f"Created assessment target: {target_id}")
                return target

            except ClientError as e:
                logger.error(f"Failed to create assessment target: {e}")
                raise

    def get_assessment_target(self, target_id: str) -> Optional[AssessmentTarget]:
        """
        Get an assessment target by ID.

        Args:
            target_id: Target ID

        Returns:
            AssessmentTarget object or None
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                response = self.client.describe_assessment_targets(
                    assessmentTargetArns=[f"arn:aws:inspector:{self.region}:123456789012:target/{target_id}"]
                )

                if not response["assessmentTargets"]:
                    return None

                target_data = response["assessmentTargets"][0]
                return AssessmentTarget(
                    target_id=target_id,
                    name=target_data["name"],
                    region=self.region,
                    resource_groups=target_data.get("resourceGroupArns", []),
                    status=AssessmentTargetStatus(target_data.get("assessmentTargetStatus", "ACTIVE")),
                    created_at=self._parse_datetime(target_data.get("createdAt")),
                    updated_at=self._parse_datetime(target_data.get("updatedAt"))
                )

            except ClientError as e:
                logger.error(f"Failed to get assessment target: {e}")
                return None

    def list_assessment_targets(
        self,
        filter_pattern: Optional[str] = None
    ) -> List[AssessmentTarget]:
        """
        List all assessment targets.

        Args:
            filter_pattern: Optional filter pattern

        Returns:
            List of AssessmentTarget objects
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                response = self.client.list_assessment_targets()
                targets = []

                for target_arn in response["assessmentTargetArns"]:
                    target_id = target_arn.split("/")[-1]
                    target = self.get_assessment_target(target_id)
                    if target and (not filter_pattern or filter_pattern.lower() in target.name.lower()):
                        targets.append(target)

                return targets

            except ClientError as e:
                logger.error(f"Failed to list assessment targets: {e}")
                return []

    def update_assessment_target(
        self,
        target_id: str,
        name: Optional[str] = None,
        resource_groups: Optional[List[str]] = None
    ) -> AssessmentTarget:
        """
        Update an assessment target.

        Args:
            target_id: Target ID
            name: New name
            resource_groups: New resource groups

        Returns:
            Updated AssessmentTarget object
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                params = {
                    "assessmentTargetArn": f"arn:aws:inspector:{self.region}:123456789012:target/{target_id}"
                }

                if name:
                    params["assessmentTargetName"] = name
                if resource_groups is not None:
                    params["resourceGroupArns"] = resource_groups

                self.client.update_assessment_target(**params)

                logger.info(f"Updated assessment target: {target_id}")
                return self.get_assessment_target(target_id)

            except ClientError as e:
                logger.error(f"Failed to update assessment target: {e}")
                raise

    def delete_assessment_target(self, target_id: str) -> bool:
        """
        Delete an assessment target.

        Args:
            target_id: Target ID

        Returns:
            True if successful
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                self.client.delete_assessment_target(
                    assessmentTargetArn=f"arn:aws:inspector:{self.region}:123456789012:target/{target_id}"
                )

                logger.info(f"Deleted assessment target: {target_id}")
                return True

            except ClientError as e:
                logger.error(f"Failed to delete assessment target: {e}")
                return False

    # =========================================================================
    # Assessment Templates
    # =========================================================================

    def create_assessment_template(
        self,
        name: str,
        target_id: str,
        rules_package_arns: List[str],
        duration_seconds: int = 3600,
        tags: Optional[Dict[str, str]] = None
    ) -> AssessmentTemplate:
        """
        Create an assessment template.

        Args:
            name: Template name
            target_id: Target ID
            rules_package_arns: List of rules package ARNs
            duration_seconds: Assessment duration
            tags: Optional tags

        Returns:
            AssessmentTemplate object
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                params = {
                    "name": name,
                    "assessmentTargetArn": f"arn:aws:inspector:{self.region}:123456789012:target/{target_id}",
                    "rulesPackageArns": rules_package_arns,
                    "durationInSeconds": duration_seconds
                }

                response = self.client.create_assessment_template(**params)
                template_arn = response["assessmentTemplateArn"]
                template_id = template_arn.split("/")[-1]

                template = AssessmentTemplate(
                    template_id=template_id,
                    name=name,
                    region=self.region,
                    target_id=target_id,
                    rules_package_arns=rules_package_arns,
                    duration_seconds=duration_seconds,
                    assessment_run_count=0,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                    tags=tags or {}
                )

                if tags:
                    self.client.add_tags_to_resource(
                        resourceArn=template_arn,
                        tags=tags
                    )

                logger.info(f"Created assessment template: {template_id}")
                return template

            except ClientError as e:
                logger.error(f"Failed to create assessment template: {e}")
                raise

    def get_assessment_template(self, template_id: str) -> Optional[AssessmentTemplate]:
        """
        Get an assessment template by ID.

        Args:
            template_id: Template ID

        Returns:
            AssessmentTemplate object or None
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                response = self.client.describe_assessment_templates(
                    assessmentTemplateArns=[f"arn:aws:inspector:{self.region}:123456789012:template/{template_id}"]
                )

                if not response["assessmentTemplates"]:
                    return None

                template_data = response["assessmentTemplates"][0]
                return AssessmentTemplate(
                    template_id=template_id,
                    name=template_data["name"],
                    region=self.region,
                    target_id=template_data["assessmentTargetArn"].split("/")[-1],
                    rules_package_arns=template_data["rulesPackageArns"],
                    duration_seconds=template_data["durationInSeconds"],
                    assessment_run_count=template_data.get("assessmentRunCount", 0),
                    last_run_started_at=self._parse_datetime(template_data.get("lastAssessmentRunStartedAt")),
                    created_at=self._parse_datetime(template_data.get("createdAt")),
                    updated_at=self._parse_datetime(template_data.get("updatedAt"))
                )

            except ClientError as e:
                logger.error(f"Failed to get assessment template: {e}")
                return None

    def list_assessment_templates(
        self,
        target_id: Optional[str] = None,
        filter_pattern: Optional[str] = None
    ) -> List[AssessmentTemplate]:
        """
        List all assessment templates.

        Args:
            target_id: Optional target ID filter
            filter_pattern: Optional filter pattern

        Returns:
            List of AssessmentTemplate objects
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                params = {}
                if target_id:
                    params["assessmentTargetArn"] = f"arn:aws:inspector:{self.region}:123456789012:target/{target_id}"

                response = self.client.list_assessment_templates(**params)
                templates = []

                for template_arn in response["assessmentTemplateArns"]:
                    template_id = template_arn.split("/")[-1]
                    template = self.get_assessment_template(template_id)
                    if template and (not filter_pattern or filter_pattern.lower() in template.name.lower()):
                        templates.append(template)

                return templates

            except ClientError as e:
                logger.error(f"Failed to list assessment templates: {e}")
                return []

    def update_assessment_template(
        self,
        template_id: str,
        name: Optional[str] = None,
        duration_seconds: Optional[int] = None
    ) -> AssessmentTemplate:
        """
        Update an assessment template.

        Args:
            template_id: Template ID
            name: New name
            duration_seconds: New duration

        Returns:
            Updated AssessmentTemplate object
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                params = {
                    "assessmentTemplateArn": f"arn:aws:inspector:{self.region}:123456789012:template/{template_id}"
                }

                if name:
                    params["assessmentTemplateName"] = name
                if duration_seconds:
                    params["durationInSeconds"] = duration_seconds

                self.client.update_assessment_template(**params)

                logger.info(f"Updated assessment template: {template_id}")
                return self.get_assessment_template(template_id)

            except ClientError as e:
                logger.error(f"Failed to update assessment template: {e}")
                raise

    def delete_assessment_template(self, template_id: str) -> bool:
        """
        Delete an assessment template.

        Args:
            template_id: Template ID

        Returns:
            True if successful
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                self.client.delete_assessment_template(
                    assessmentTemplateArn=f"arn:aws:inspector:{self.region}:123456789012:template/{template_id}"
                )

                logger.info(f"Deleted assessment template: {template_id}")
                return True

            except ClientError as e:
                logger.error(f"Failed to delete assessment template: {e}")
                return False

    # =========================================================================
    # Assessment Runs
    # =========================================================================

    def start_assessment_run(
        self,
        template_id: str,
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> AssessmentRun:
        """
        Start an assessment run.

        Args:
            template_id: Template ID
            name: Optional run name
            tags: Optional tags

        Returns:
            AssessmentRun object
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                params = {
                    "assessmentTemplateArn": f"arn:aws:inspector:{self.region}:123456789012:template/{template_id}"
                }

                if name:
                    params["assessmentRunName"] = name

                response = self.client.start_assessment_run(**params)
                run_arn = response["assessmentRunArn"]
                run_id = run_arn.split("/")[-1]

                run = AssessmentRun(
                    run_id=run_id,
                    template_id=template_id,
                    region=self.region,
                    state=AssessmentRunStatus.STARTED,
                    started_at=datetime.utcnow(),
                    tags=tags or {}
                )

                if tags:
                    self.client.add_tags_to_resource(
                        resourceArn=run_arn,
                        tags=tags
                    )

                logger.info(f"Started assessment run: {run_id}")
                return run

            except ClientError as e:
                logger.error(f"Failed to start assessment run: {e}")
                raise

    def get_assessment_run(self, run_id: str) -> Optional[AssessmentRun]:
        """
        Get an assessment run by ID.

        Args:
            run_id: Run ID

        Returns:
            AssessmentRun object or None
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                response = self.client.describe_assessment_runs(
                    assessmentRunArns=[f"arn:aws:inspector:{self.region}:123456789012:run/{run_id}"]
                )

                if not response["assessmentRuns"]:
                    return None

                run_data = response["assessmentRuns"][0]
                return AssessmentRun(
                    run_id=run_id,
                    template_id=run_data["assessmentTemplateArn"].split("/")[-1],
                    region=self.region,
                    state=AssessmentRunStatus(run_data.get("state", "CREATED")),
                    started_at=self._parse_datetime(run_data.get("startedAt")),
                    completed_at=self._parse_datetime(run_data.get("completedAt")),
                    findings_count=run_data.get("findingIdsCount", 0),
                    rules_packages_inspected=run_data.get("rulesPackagesCount", 0)
                )

            except ClientError as e:
                logger.error(f"Failed to get assessment run: {e}")
                return None

    def list_assessment_runs(
        self,
        template_id: Optional[str] = None,
        state_filter: Optional[AssessmentRunStatus] = None
    ) -> List[AssessmentRun]:
        """
        List all assessment runs.

        Args:
            template_id: Optional template ID filter
            state_filter: Optional state filter

        Returns:
            List of AssessmentRun objects
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                params = {}
                if template_id:
                    params["assessmentTemplateArn"] = f"arn:aws:inspector:{self.region}:123456789012:template/{template_id}"

                response = self.client.list_assessment_runs(**params)
                runs = []

                for run_arn in response["assessmentRunArns"]:
                    run_id = run_arn.split("/")[-1]
                    run = self.get_assessment_run(run_id)
                    if run and (state_filter is None or run.state == state_filter):
                        runs.append(run)

                return runs

            except ClientError as e:
                logger.error(f"Failed to list assessment runs: {e}")
                return []

    def stop_assessment_run(self, run_id: str) -> bool:
        """
        Stop an assessment run.

        Args:
            run_id: Run ID

        Returns:
            True if successful
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                self.client.stop_assessment_run(
                    assessmentRunArn=f"arn:aws:inspector:{self.region}:123456789012:run/{run_id}"
                )

                logger.info(f"Stopped assessment run: {run_id}")
                return True

            except ClientError as e:
                logger.error(f"Failed to stop assessment run: {e}")
                return False

    # =========================================================================
    # Rules Packages
    # =========================================================================

    def list_rules_packages(self) -> List[RulesPackage]:
        """
        List all rules packages.

        Returns:
            List of RulesPackage objects
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                response = self.client.list_rules_packages()
                packages = []

                for pkg_arn in response["rulesPackageArns"]:
                    pkg = self.get_rules_package(pkg_arn)
                    if pkg:
                        packages.append(pkg)

                return packages

            except ClientError as e:
                logger.error(f"Failed to list rules packages: {e}")
                return []

    def get_rules_package(self, arn: str) -> Optional[RulesPackage]:
        """
        Get a rules package by ARN.

        Args:
            arn: Rules package ARN

        Returns:
            RulesPackage object or None
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                response = self.client.describe_rules_packages(
                    rulesPackageArns=[arn]
                )

                if not response["rulesPackages"]:
                    return None

                pkg_data = response["rulesPackages"][0]
                return RulesPackage(
                    arn=arn,
                    name=pkg_data["name"],
                    version=pkg_data["version"],
                    region=self.region,
                    rule_count=pkg_data.get("ruleCount", 0),
                    description=pkg_data.get("description", ""),
                    provider=pkg_data.get("provider", "AWS")
                )

            except ClientError as e:
                logger.error(f"Failed to get rules package: {e}")
                return None

    def get_available_rules_packages(self) -> Dict[str, List[str]]:
        """
        Get available rules packages by region.

        Returns:
            Dictionary mapping region to list of rules package ARNs
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                response = self.client.list_available_agent_versions()
                return response

            except ClientError as e:
                logger.error(f"Failed to get available rules packages: {e}")
                return {}

    # =========================================================================
    # Findings
    # =========================================================================

    def list_findings(
        self,
        run_id: Optional[str] = None,
        severity_filter: Optional[FindingSeverity] = None,
        max_results: int = 100
    ) -> List[Finding]:
        """
        List findings.

        Args:
            run_id: Optional assessment run ID filter
            severity_filter: Optional severity filter
            max_results: Maximum results

        Returns:
            List of Finding objects
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                params = {"maxResults": max_results}
                if run_id:
                    params["assessmentRunArns"] = [f"arn:aws:inspector:{self.region}:123456789012:run/{run_id}"]

                response = self.client.list_findings(**params)
                findings = []

                for finding_arn in response["findingArns"]:
                    finding = self.get_finding(finding_arn)
                    if finding and (severity_filter is None or finding.severity == severity_filter):
                        findings.append(finding)

                return findings

            except ClientError as e:
                logger.error(f"Failed to list findings: {e}")
                return []

    def get_finding(self, arn: str) -> Optional[Finding]:
        """
        Get a finding by ARN.

        Args:
            arn: Finding ARN

        Returns:
            Finding object or None
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                response = self.client.describe_findings(
                    findingArns=[arn]
                )

                if not response["findings"]:
                    return None

                finding_data = response["findings"][0]
                return Finding(
                    finding_id=arn.split("/")[-1],
                    region=self.region,
                    severity=FindingSeverity(finding_data.get("severity", "LOW")),
                    title=finding_data.get("title", ""),
                    description=finding_data.get("description", ""),
                    asset_type=finding_data.get("assetType", ""),
                    asset_id=finding_data.get("asset", {}).get("id", ""),
                    rules_package_arn=finding_data.get("rulesPackageArn"),
                    assessment_run_id=finding_data.get("assessmentRunArn", "").split("/")[-1] if finding_data.get("assessmentRunArn") else None,
                    created_at=self._parse_datetime(finding_data.get("createdAt")),
                    updated_at=self._parse_datetime(finding_data.get("updatedAt")),
                    status=finding_data.get("nlpFindingStatus", "ACTIVE")
                )

            except ClientError as e:
                logger.error(f"Failed to get finding: {e}")
                return None

    def get_finding_statistics(self) -> Dict[str, Any]:
        """
        Get finding statistics.

        Returns:
            Dictionary with finding statistics
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                response = self.client.get_assessment_summary()
                return response

            except ClientError as e:
                logger.error(f"Failed to get finding statistics: {e}")
                return {}

    # =========================================================================
    # Resource Groups
    # =========================================================================

    def create_resource_group(
        self,
        name: str,
        resource_arns: List[str],
        tags: Optional[Dict[str, str]] = None
    ) -> ResourceGroup:
        """
        Create a resource group.

        Args:
            name: Group name
            resource_arns: List of resource ARNs
            tags: Optional tags

        Returns:
            ResourceGroup object
        """
        with self._lock:
            if not self.resource_groups_client:
                raise RuntimeError("Resource groups client not initialized")

            try:
                tag_query = {
                    "ResourceTypeFilters": ["AWS::AllSupported"],
                    "TagFilters": tags or []
                }

                params = {
                    "Name": name,
                    "ResourceQuery": tag_query
                }

                response = self.resource_groups_client.create_group(**params)
                group_arn = response["Group"]["GroupArn"]
                group_id = group_arn.split("/")[-1]

                group = ResourceGroup(
                    group_id=group_id,
                    name=name,
                    region=self.region,
                    resource_arns=resource_arns,
                    tags=tags or {},
                    created_at=datetime.utcnow()
                )

                logger.info(f"Created resource group: {group_id}")
                return group

            except ClientError as e:
                logger.error(f"Failed to create resource group: {e}")
                raise

    def get_resource_group(self, group_id: str) -> Optional[ResourceGroup]:
        """
        Get a resource group by ID.

        Args:
            group_id: Group ID

        Returns:
            ResourceGroup object or None
        """
        with self._lock:
            if not self.resource_groups_client:
                raise RuntimeError("Resource groups client not initialized")

            try:
                response = self.resource_groups_client.get_group(
                    Group=f"arn:aws:resource-groups:{self.region}:123456789012:group/{group_id}"
                )

                group_data = response["Group"]
                return ResourceGroup(
                    group_id=group_id,
                    name=group_data["Name"],
                    region=self.region,
                    tags=group_data.get("Tags", {}),
                    created_at=self._parse_datetime(group_data.get("CreatedAt"))
                )

            except ClientError as e:
                logger.error(f"Failed to get resource group: {e}")
                return None

    def list_resource_groups(self) -> List[ResourceGroup]:
        """
        List all resource groups.

        Returns:
            List of ResourceGroup objects
        """
        with self._lock:
            if not self.resource_groups_client:
                raise RuntimeError("Resource groups client not initialized")

            try:
                response = self.resource_groups_client.list_groups()
                groups = []

                for group in response["Groups"]:
                    group_id = group["GroupArn"].split("/")[-1]
                    grp = self.get_resource_group(group_id)
                    if grp:
                        groups.append(grp)

                return groups

            except ClientError as e:
                logger.error(f"Failed to list resource groups: {e}")
                return []

    def delete_resource_group(self, group_id: str) -> bool:
        """
        Delete a resource group.

        Args:
            group_id: Group ID

        Returns:
            True if successful
        """
        with self._lock:
            if not self.resource_groups_client:
                raise RuntimeError("Resource groups client not initialized")

            try:
                self.resource_groups_client.delete_group(
                    Group=f"arn:aws:resource-groups:{self.region}:123456789012:group/{group_id}"
                )

                logger.info(f"Deleted resource group: {group_id}")
                return True

            except ClientError as e:
                logger.error(f"Failed to delete resource group: {e}")
                return False

    # =========================================================================
    # SNS Topics
    # =========================================================================

    def configure_sns_notifications(
        self,
        topic_arn: str,
        sns_role_arn: str,
        event_types: Optional[List[str]] = None
    ) -> SNSConfiguration:
        """
        Configure SNS notifications for Inspector events.

        Args:
            topic_arn: SNS topic ARN
            sns_role_arn: IAM role ARN for SNS access
            event_types: List of event types

        Returns:
            SNSConfiguration object
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                params = {
                    "topicArn": topic_arn,
                    "snsRoleArn": sns_role_arn
                }

                if event_types:
                    params["eventTypes"] = event_types

                self.client.subscribe_to_event(**params)

                config = SNSConfiguration(
                    topic_arn=topic_arn,
                    sns_role_arn=sns_role_arn,
                    event_types=event_types or [],
                    enabled=True
                )

                logger.info(f"Configured SNS notifications: {topic_arn}")
                return config

            except ClientError as e:
                logger.error(f"Failed to configure SNS notifications: {e}")
                raise

    def list_event_subscriptions(self) -> List[Dict[str, Any]]:
        """
        List SNS event subscriptions.

        Returns:
            List of subscription configurations
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                response = self.client.list_event_subscriptions()
                return response.get("subscriptions", [])

            except ClientError as e:
                logger.error(f"Failed to list event subscriptions: {e}")
                return []

    # =========================================================================
    # CloudWatch Integration
    # =========================================================================

    def enable_cloudwatch_metrics(
        self,
        metrics_enabled: bool = True,
        assessment_run_metrics: bool = True,
        finding_metrics: bool = True
    ) -> CloudWatchMetrics:
        """
        Enable CloudWatch metrics for Inspector.

        Args:
            metrics_enabled: Enable metrics
            assessment_run_metrics: Track assessment run metrics
            finding_metrics: Track finding metrics

        Returns:
            CloudWatchMetrics configuration
        """
        with self._lock:
            config = CloudWatchMetrics(
                metrics_enabled=metrics_enabled,
                assessment_run_metrics=assessment_run_metrics,
                finding_metrics=finding_metrics
            )

            if metrics_enabled and BOTO3_AVAILABLE:
                try:
                    cloudwatch = boto3.Session().client("cloudwatch", region_name=self.region)

                    if assessment_run_metrics:
                        cloudwatch.put_metric_data(
                            Namespace="AWS/Inspector",
                            MetricData=[{
                                "MetricName": "AssessmentRuns",
                                "Value": 1,
                                "Unit": "Count"
                            }]
                        )

                    if finding_metrics:
                        cloudwatch.put_metric_data(
                            Namespace="AWS/Inspector",
                            MetricData=[{
                                "MetricName": "Findings",
                                "Value": 1,
                                "Unit": "Count"
                            }]
                        )

                    logger.info("Enabled CloudWatch metrics for Inspector")
                except Exception as e:
                    logger.error(f"Failed to enable CloudWatch metrics: {e}")

            return config

    def get_cloudwatch_metrics(
        self,
        metric_name: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 300
    ) -> List[Dict[str, Any]]:
        """
        Get CloudWatch metrics for Inspector.

        Args:
            metric_name: Metric name
            start_time: Start time
            end_time: End time
            period: Metric period

        Returns:
            List of metric data points
        """
        with self._lock:
            if not BOTO3_AVAILABLE:
                return []

            try:
                cloudwatch = boto3.Session().client("cloudwatch", region_name=self.region)
                response = cloudwatch.get_metric_statistics(
                    Namespace="AWS/Inspector",
                    MetricName=metric_name,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Sum", "Average"]
                )

                return response.get("Datapoints", [])

            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to get CloudWatch metrics: {e}")
                return []

    # =========================================================================
    # Inspector v2 Support
    # =========================================================================

    def list_findings_v2(
        self,
        resource_type: Optional[Inspector2ResourceType] = None,
        severity: Optional[FindingSeverity] = None,
        status: Optional[Inspector2FindingStatus] = None,
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List findings using Inspector v2 API.

        Args:
            resource_type: Resource type filter
            severity: Severity filter
            status: Status filter
            max_results: Maximum results

        Returns:
            List of finding dictionaries
        """
        with self._lock:
            if not self.client_v2:
                raise RuntimeError("Inspector v2 client not initialized")

            try:
                filters = {}

                if resource_type:
                    filters["resourceType"] = [resource_type.value]
                if severity:
                    filters["severity"] = [severity.value]
                if status:
                    filters["findingStatus"] = [status.value]

                params = {"maxResults": max_results}
                if filters:
                    params["filterCriteria"] = filters

                response = self.client_v2.list_findings(**params)
                return response.get("findings", [])

            except ClientError as e:
                logger.error(f"Failed to list findings v2: {e}")
                return []

    def get_finding_v2(self, finding_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a finding by ID using Inspector v2 API.

        Args:
            finding_id: Finding ID

        Returns:
            Finding dictionary or None
        """
        with self._lock:
            if not self.client_v2:
                raise RuntimeError("Inspector v2 client not initialized")

            try:
                response = self.client_v2.get_findings(
                    findingArns=[f"arn:aws:inspector2:{self.region}:123456789012:finding/{finding_id}"]
                )

                findings = response.get("findings", [])
                return findings[0] if findings else None

            except ClientError as e:
                logger.error(f"Failed to get finding v2: {e}")
                return None

    def describe_finding_v2(self, finding_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a finding using Inspector v2.

        Args:
            finding_id: Finding ID

        Returns:
            Finding details dictionary
        """
        with self._lock:
            if not self.client_v2:
                raise RuntimeError("Inspector v2 client not initialized")

            try:
                response = self.client_v2.describe_findings(
                    findingArns=[f"arn:aws:inspector2:{self.region}:123456789012:finding/{finding_id}"]
                )

                findings = response.get("findings", [])
                return findings[0] if findings else None

            except ClientError as e:
                logger.error(f"Failed to describe finding v2: {e}")
                return None

    def archive_finding_v2(self, finding_id: str) -> bool:
        """
        Archive a finding using Inspector v2.

        Args:
            finding_id: Finding ID

        Returns:
            True if successful
        """
        with self._lock:
            if not self.client_v2:
                raise RuntimeError("Inspector v2 client not initialized")

            try:
                self.client_v2.archive_findings(
                    findingArns=[f"arn:aws:inspector2:{self.region}:123456789012:finding/{finding_id}"]
                )

                logger.info(f"Archived finding v2: {finding_id}")
                return True

            except ClientError as e:
                logger.error(f"Failed to archive finding v2: {e}")
                return False

    def unarchive_finding_v2(self, finding_id: str) -> bool:
        """
        Unarchive a finding using Inspector v2.

        Args:
            finding_id: Finding ID

        Returns:
            True if successful
        """
        with self._lock:
            if not self.client_v2:
                raise RuntimeError("Inspector v2 client not initialized")

            try:
                self.client_v2.unarchive_findings(
                    findingArns=[f"arn:aws:inspector2:{self.region}:123456789012:finding/{finding_id}"]
                )

                logger.info(f"Unarchived finding v2: {finding_id}")
                return True

            except ClientError as e:
                logger.error(f"Failed to unarchive finding v2: {e}")
                return False

    def list_coverage(
        self,
        resource_type: Optional[Inspector2ResourceType] = None
    ) -> List[Dict[str, Any]]:
        """
        List coverage information using Inspector v2.

        Args:
            resource_type: Resource type filter

        Returns:
            List of coverage information
        """
        with self._lock:
            if not self.client_v2:
                raise RuntimeError("Inspector v2 client not initialized")

            try:
                params = {}
                if resource_type:
                    params["resourceType"] = resource_type.value

                response = self.client_v2.list_coverage(**params)
                return response.get("coverages", [])

            except ClientError as e:
                logger.error(f"Failed to list coverage: {e}")
                return []

    def get_coverage_summary(self) -> Dict[str, Any]:
        """
        Get coverage summary using Inspector v2.

        Returns:
            Coverage summary dictionary
        """
        with self._lock:
            if not self.client_v2:
                raise RuntimeError("Inspector v2 client not initialized")

            try:
                response = self.client_v2.get_coverage_summary()
                return response

            except ClientError as e:
                logger.error(f"Failed to get coverage summary: {e}")
                return {}

    def enable_inspector_v2(
        self,
        resource_types: Optional[List[Inspector2ResourceType]] = None
    ) -> bool:
        """
        Enable Inspector v2.

        Args:
            resource_types: Resource types to enable

        Returns:
            True if successful
        """
        with self._lock:
            if not self.client_v2:
                raise RuntimeError("Inspector v2 client not initialized")

            try:
                params = {}
                if resource_types:
                    params["resourceTypes"] = [rt.value for rt in resource_types]

                self.client_v2.enable(**params)

                logger.info("Enabled Inspector v2")
                return True

            except ClientError as e:
                logger.error(f"Failed to enable Inspector v2: {e}")
                return False

    def disable_inspector_v2(
        self,
        resource_types: Optional[List[Inspector2ResourceType]] = None
    ) -> bool:
        """
        Disable Inspector v2.

        Args:
            resource_types: Resource types to disable

        Returns:
            True if successful
        """
        with self._lock:
            if not self.client_v2:
                raise RuntimeError("Inspector v2 client not initialized")

            try:
                params = {}
                if resource_types:
                    params["resourceTypes"] = [rt.value for rt in resource_types]

                self.client_v2.disable(**params)

                logger.info("Disabled Inspector v2")
                return True

            except ClientError as e:
                logger.error(f"Failed to disable Inspector v2: {e}")
                return False

    def create_filter_v2(
        self,
        name: str,
        filter_action: str,
        finding_criteria: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Create a filter using Inspector v2.

        Args:
            name: Filter name
            filter_action: Filter action (ARCHIVE, NOOP)
            finding_criteria: Finding criteria

        Returns:
            Filter configuration or None
        """
        with self._lock:
            if not self.client_v2:
                raise RuntimeError("Inspector v2 client not initialized")

            try:
                response = self.client_v2.create_filter(
                    name=name,
                    action=filter_action,
                    findingCriteria=finding_criteria
                )

                logger.info(f"Created filter v2: {name}")
                return response

            except ClientError as e:
                logger.error(f"Failed to create filter v2: {e}")
                return None

    def list_filters_v2(self) -> List[Dict[str, Any]]:
        """
        List filters using Inspector v2.

        Returns:
            List of filters
        """
        with self._lock:
            if not self.client_v2:
                raise RuntimeError("Inspector v2 client not initialized")

            try:
                response = self.client_v2.list_filters()
                return response.get("filters", [])

            except ClientError as e:
                logger.error(f"Failed to list filters v2: {e}")
                return []

    def delete_filter_v2(self, arn: str) -> bool:
        """
        Delete a filter using Inspector v2.

        Args:
            arn: Filter ARN

        Returns:
            True if successful
        """
        with self._lock:
            if not self.client_v2:
                raise RuntimeError("Inspector v2 client not initialized")

            try:
                self.client_v2.delete_filter(
                    arn=arn
                )

                logger.info(f"Deleted filter v2: {arn}")
                return True

            except ClientError as e:
                logger.error(f"Failed to delete filter v2: {e}")
                return False

    # =========================================================================
    # Reporting
    # =========================================================================

    def generate_assessment_report(
        self,
        run_id: str,
        report_format: str = "PDF",
        output_bucket: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Generate an assessment report.

        Args:
            run_id: Assessment run ID
            report_format: Report format (PDF or HTML)
            output_bucket: S3 bucket for report output

        Returns:
            Report configuration dictionary
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                params = {
                    "assessmentRunArns": [f"arn:aws:inspector:{self.region}:123456789012:run/{run_id}"]
                }

                if output_bucket:
                    params["outputBucket"] = output_bucket

                response = self.client.get_assessment_report(**params)

                report = {
                    "report_id": response.get("assessmentReport", {}).get("reportId", ""),
                    "status": response.get("assessmentReport", {}).get("status", ""),
                    "format": report_format,
                    "url": response.get("url")
                }

                logger.info(f"Generated assessment report for run: {run_id}")
                return report

            except ClientError as e:
                logger.error(f"Failed to generate assessment report: {e}")
                return None

    def get_assessment_summary_report(self) -> Dict[str, Any]:
        """
        Get assessment summary report.

        Returns:
            Summary report dictionary
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                response = self.client.get_assessment_summary()
                return response

            except ClientError as e:
                logger.error(f"Failed to get assessment summary report: {e}")
                return {}

    def export_findings(
        self,
        output_format: str = "JSON",
        output_bucket: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Export findings to S3.

        Args:
            output_format: Export format (JSON, CSV)
            output_bucket: S3 bucket for export

        Returns:
            Export configuration dictionary
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                params = {}

                if output_bucket:
                    params["outputBucket"] = output_bucket

                params["format"] = output_format

                response = self.client.export_findings(**params)

                export_info = {
                    "export_id": response.get("exportId", ""),
                    "status": response.get("status", ""),
                    "format": output_format
                }

                logger.info(f"Exported findings: {export_info['export_id']}")
                return export_info

            except ClientError as e:
                logger.error(f"Failed to export findings: {e}")
                return None

    # =========================================================================
    # Tags Management
    # =========================================================================

    def add_tags_to_resource(
        self,
        resource_arn: str,
        tags: Dict[str, str]
    ) -> bool:
        """
        Add tags to a resource.

        Args:
            resource_arn: Resource ARN
            tags: Tags to add

        Returns:
            True if successful
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                self.client.add_tags_to_resource(
                    resourceArn=resource_arn,
                    tags=tags
                )

                logger.info(f"Added tags to resource: {resource_arn}")
                return True

            except ClientError as e:
                logger.error(f"Failed to add tags: {e}")
                return False

    def remove_tags_from_resource(
        self,
        resource_arn: str,
        tag_keys: List[str]
    ) -> bool:
        """
        Remove tags from a resource.

        Args:
            resource_arn: Resource ARN
            tag_keys: Tag keys to remove

        Returns:
            True if successful
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                self.client.remove_tags_from_resource(
                    resourceArn=resource_arn,
                    tagKeys=tag_keys
                )

                logger.info(f"Removed tags from resource: {resource_arn}")
                return True

            except ClientError as e:
                logger.error(f"Failed to remove tags: {e}")
                return False

    def list_tags_for_resource(self, resource_arn: str) -> Dict[str, str]:
        """
        List tags for a resource.

        Args:
            resource_arn: Resource ARN

        Returns:
            Dictionary of tags
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                response = self.client.list_tags_for_resource(
                    resourceArn=resource_arn
                )

                return {tag["key"]: tag["value"] for tag in response.get("tags", [])}

            except ClientError as e:
                logger.error(f"Failed to list tags: {e}")
                return {}

    # =========================================================================
    # Agent Management
    # =========================================================================

    def list_assessment_agents(self, target_id: str) -> List[Dict[str, Any]]:
        """
        List assessment agents for a target.

        Args:
            target_id: Target ID

        Returns:
            List of agent information
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                response = self.client.list_assessment_agents(
                    assessmentTargetArn=f"arn:aws:inspector:{self.region}:123456789012:target/{target_id}"
                )

                return response.get("agentProperties", [])

            except ClientError as e:
                logger.error(f"Failed to list assessment agents: {e}")
                return []

    def get_telemetry_metadata(self, run_id: str) -> Dict[str, Any]:
        """
        Get telemetry metadata for an assessment run.

        Args:
            run_id: Run ID

        Returns:
            Telemetry metadata dictionary
        """
        with self._lock:
            if not self.client:
                raise RuntimeError("Inspector client not initialized")

            try:
                response = self.client.get_telemetry_metadata(
                    assessmentRunArn=f"arn:aws:inspector:{self.region}:123456789012:run/{run_id}"
                )

                return response

            except ClientError as e:
                logger.error(f"Failed to get telemetry metadata: {e}")
                return {}

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def health_check(self) -> Dict[str, Any]:
        """
        Check the health of Inspector integration.

        Returns:
            Health status dictionary
        """
        health = {
            "status": "healthy",
            "region": self.region,
            "client_initialized": self.client is not None,
            "client_v2_initialized": self.client_v2 is not None,
            "resource_groups_client_initialized": self.resource_groups_client is not None,
            "timestamp": datetime.utcnow().isoformat()
        }

        if not all([
            health["client_initialized"],
            health["client_v2_initialized"],
            health["resource_groups_client_initialized"]
        ]):
            health["status"] = "degraded"

        return health

    def get_permissions_boundary(self) -> Dict[str, Any]:
        """
        Get current permissions boundary information.

        Returns:
            Permissions boundary dictionary
        """
        return {
            "region": self.region,
            "profile": self.profile_name,
            "required_iam_permissions": [
                "inspector:CreateAssessmentTarget",
                "inspector:CreateAssessmentTemplate",
                "inspector:StartAssessmentRun",
                "inspector:ListFindings",
                "inspector:GetAssessmentReport",
                "inspector2:ListFindings",
                "inspector2:GetCoverageSummary",
                "cloudwatch:PutMetricData",
                "sns:Publish",
                "resource-groups:CreateGroup"
            ]
        }

    def cleanup(self):
        """Cleanup resources."""
        with self._lock:
            self.client = None
            self.client_v2 = None
            self.resource_groups_client = None
            logger.info("Inspector integration cleaned up")

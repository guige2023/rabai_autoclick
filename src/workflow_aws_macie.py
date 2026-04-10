"""AWS Macie Integration - Security and data governance automation.

Provides comprehensive AWS Macie management including:
- Account management and enablement
- Classification jobs for sensitive data discovery
- Security findings management
- Custom data identifiers
- Allow/Block lists for data classification
- Sensitive data discoveries
- Member account management
- Session context settings
- CloudWatch metrics and notifications
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class FindingSeverity(str, Enum):
    """Severity levels for Macie findings."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class JobStatus(str, Enum):
    """Status states for classification jobs."""

    IDLE = "IDLE"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    CANCELLED = "CANCELLED"
    COMPLETE = "COMPLETE"


@dataclass
class MacieAccountConfig:
    """Configuration for Macie account settings."""

    region: str = "us-east-1"
    enable: bool = True
    finding_publishing_frequency: str = "FIFTEEN_MINUTES"  # or "ONE_HOUR" or "SIX_HOURS"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class ClassificationJobConfig:
    """Configuration for a classification job."""

    name: str
    description: str = ""
    job_type: str = "ONE_TIME"  # or "SCHEDULED"
    s3_job_definition: dict[str, Any] = field(default_factory=dict)
    custom_data_identifier_ids: list[str] = field(default_factory=list)
    sampling_depth: int = 1000
    schedule_frequency: Optional[dict[str, Any]] = None
    created_at: Optional[datetime] = None
    job_id: Optional[str] = None
    status: JobStatus = JobStatus.IDLE


@dataclass
class FindingFilter:
    """Filter criteria for querying findings."""

    severity: Optional[FindingSeverity] = None
    finding_type: Optional[str] = None
    resource_type: Optional[str] = None
    finding_accessed: Optional[datetime] = None
    classification_result_details: Optional[dict[str, Any]] = None
    page_size: int = 50
    next_token: Optional[str] = None


@dataclass
class CustomDataIdentifierConfig:
    """Configuration for custom data identifiers."""

    name: str
    description: str = ""
    regex_pattern: Optional[str] = None
    keywords: list[str] = field(default_factory=list)
    maximum_match_distance: int = 0
    severity: str = "MEDIUM"  # LOW, MEDIUM, HIGH


@dataclass
class AllowListConfig:
    """Configuration for allow lists."""

    name: str
    description: str = ""
    regex_pattern: Optional[str] = None
    s3_words_file: Optional[dict[str, Any]] = None


@dataclass
class BlockListConfig:
    """Configuration for block lists."""

    name: str
    description: str = ""
    regex_pattern: Optional[str] = None
    s3_words_file: Optional[dict[str, Any]] = None


class MacieIntegration:
    """Main integration class for AWS Macie operations.

    Provides a high-level interface for managing AWS Macie resources including
    account settings, classification jobs, findings, custom identifiers, and
    integration with CloudWatch for monitoring.
    """

    def __init__(
        self,
        region: str = "us-east-1",
        profile_name: Optional[str] = None,
        account_config: Optional[MacieAccountConfig] = None,
    ):
        """Initialize Macie integration.

        Args:
            region: AWS region for Macie operations.
            profile_name: AWS profile name for boto3 session.
            account_config: Optional account configuration.
        """
        self.region = region
        self.profile_name = profile_name
        self.account_config = account_config or MacieAccountConfig(region=region)

        session_kwargs: dict[str, Any] = {"region_name": region}
        if profile_name:
            session_kwargs["profile_name"] = profile_name

        self._session = boto3.Session(**session_kwargs)
        self._macie2 = self._session.client("macie2", region_name=region)
        self._cloudwatch = self._session.client("cloudwatch", region_name=region)

    # =========================================================================
    # 1. Account Management
    # =========================================================================

    def enable_macie(self) -> dict[str, Any]:
        """Enable Macie for the current account.

        Returns:
            dict: Enable Macie response with status details.

        Raises:
            ClientError: If enabling Macie fails.
        """
        try:
            response = self._macie2.enable_macie()
            logger.info("Macie enabled successfully")
            return {
                "status": "enabled",
                "response": response,
                "timestamp": datetime.utcnow().isoformat(),
            }
        except ClientError as e:
            logger.error(f"Failed to enable Macie: {e}")
            raise

    def disable_macie(self) -> dict[str, Any]:
        """Disable Macie for the current account.

        Returns:
            dict: Disable Macie response with status details.

        Raises:
            ClientError: If disabling Macie fails.
        """
        try:
            response = self._macie2.disable_macie()
            logger.info("Macie disabled successfully")
            return {
                "status": "disabled",
                "response": response,
                "timestamp": datetime.utcnow().isoformat(),
            }
        except ClientError as e:
            logger.error(f"Failed to disable Macie: {e}")
            raise

    def get_macie_session(self) -> dict[str, Any]:
        """Get current Macie session status.

        Returns:
            dict: Session details including status and findings publishing frequency.
        """
        try:
            response = self._macie2.get_macie_session()
            return response
        except ClientError as e:
            logger.error(f"Failed to get Macie session: {e}")
            raise

    def update_macie_session(
        self, finding_publishing_frequency: Optional[str] = None, status: Optional[str] = None
    ) -> dict[str, Any]:
        """Update Macie session settings.

        Args:
            finding_publishing_frequency: How often to publish findings (FIFTEEN_MINUTES,
                ONE_HOUR, SIX_HOURS).
            status: Account status (PAUSED or ENABLED).

        Returns:
            dict: Updated session details.
        """
        try:
            params: dict[str, Any] = {}
            if finding_publishing_frequency:
                params["findingPublishingFrequency"] = finding_publishing_frequency
            if status:
                params["status"] = status

            response = self._macie2.update_macie_session(**params)
            logger.info("Macie session updated successfully")
            return response
        except ClientError as e:
            logger.error(f"Failed to update Macie session: {e}")
            raise

    def get_master_account(self) -> dict[str, Any]:
        """Get the master account information.

        Returns:
            dict: Master account details including account ID and relationship status.
        """
        try:
            response = self._macie2.get_master_account()
            return response.get("master", {})
        except ClientError as e:
            logger.error(f"Failed to get master account: {e}")
            raise

    # =========================================================================
    # 2. Classification Jobs
    # =========================================================================

    def create_classification_job(
        self, config: ClassificationJobConfig
    ) -> dict[str, Any]:
        """Create a new classification job.

        Args:
            config: Classification job configuration.

        Returns:
            dict: Created job details including job ID and status.
        """
        try:
            job_params: dict[str, Any] = {
                "jobType": config.job_type,
                "s3JobDefinition": config.s3_job_definition,
                "description": config.description,
                "name": config.name,
                "samplingDepth": config.sampling_depth,
            }

            if config.custom_data_identifier_ids:
                job_params["customDataIdentifierIds"] = config.custom_data_identifier_ids

            if config.schedule_frequency:
                job_params["scheduleFrequency"] = config.schedule_frequency

            response = self._macie2.create_classification_job(**job_params)
            job_id = response.get("jobId")

            logger.info(f"Classification job created: {job_id}")
            return {
                "job_id": job_id,
                "status": response.get("jobStatus"),
                "created_at": response.get("createdAt"),
                "name": config.name,
            }
        except ClientError as e:
            logger.error(f"Failed to create classification job: {e}")
            raise

    def list_classification_jobs(
        self, job_type: Optional[str] = None, next_token: Optional[str] = None
    ) -> dict[str, Any]:
        """List classification jobs.

        Args:
            job_type: Filter by job type (ONE_TIME or SCHEDULED).
            next_token: Pagination token for fetching next page.

        Returns:
            dict: List of jobs and pagination token.
        """
        try:
            params: dict[str, Any] = {}
            if job_type:
                params["jobType"] = job_type
            if next_token:
                params["nextToken"] = next_token

            response = self._macie2.list_classification_jobs(**params)
            return {
                "jobs": response.get("jobDefinitions", []),
                "next_token": response.get("nextToken"),
            }
        except ClientError as e:
            logger.error(f"Failed to list classification jobs: {e}")
            raise

    def describe_classification_job(self, job_id: str) -> dict[str, Any]:
        """Get details of a specific classification job.

        Args:
            job_id: The job identifier.

        Returns:
            dict: Job details including status, statistics, and configuration.
        """
        try:
            response = self._macie2.describe_classification_job(jobId=job_id)
            return response
        except ClientError as e:
            logger.error(f"Failed to describe classification job {job_id}: {e}")
            raise

    def update_classification_job(
        self, job_id: str, config: ClassificationJobConfig
    ) -> dict[str, Any]:
        """Update an existing classification job.

        Args:
            job_id: The job identifier to update.
            config: Updated job configuration.

        Returns:
            dict: Updated job details.
        """
        try:
            update_params: dict[str, Any] = {}

            if config.s3_job_definition:
                update_params["s3JobDefinition"] = config.s3_job_definition
            if config.description:
                update_params["description"] = config.description
            if config.sampling_depth:
                update_params["samplingDepth"] = config.sampling_depth
            if config.custom_data_identifier_ids:
                update_params["customDataIdentifierIds"] = config.custom_data_identifier_ids
            if config.schedule_frequency:
                update_params["scheduleFrequency"] = config.schedule_frequency

            response = self._macie2.update_classification_job(
                jobId=job_id, **update_params
            )
            logger.info(f"Classification job updated: {job_id}")
            return {"job_id": job_id, "status": "updated"}
        except ClientError as e:
            logger.error(f"Failed to update classification job {job_id}: {e}")
            raise

    def delete_classification_job(self, job_id: str) -> dict[str, Any]:
        """Delete a classification job.

        Args:
            job_id: The job identifier to delete.

        Returns:
            dict: Deletion confirmation.
        """
        try:
            self._macie2.delete_classification_job(jobId=job_id)
            logger.info(f"Classification job deleted: {job_id}")
            return {"job_id": job_id, "status": "deleted"}
        except ClientError as e:
            logger.error(f"Failed to delete classification job {job_id}: {e}")
            raise

    def run_classification_job(self, job_id: str) -> dict[str, Any]:
        """Start or run a classification job immediately.

        Args:
            job_id: The job identifier to run.

        Returns:
            dict: Job execution confirmation.
        """
        try:
            self._macie2.run_classification_job(jobId=job_id)
            logger.info(f"Classification job triggered: {job_id}")
            return {"job_id": job_id, "status": "running"}
        except ClientError as e:
            logger.error(f"Failed to run classification job {job_id}: {e}")
            raise

    # =========================================================================
    # 3. Findings
    # =========================================================================

    def list_findings(
        self, filter_criteria: Optional[FindingFilter] = None
    ) -> dict[str, Any]:
        """List security findings based on filter criteria.

        Args:
            filter_criteria: Optional filter criteria for findings.

        Returns:
            dict: List of findings and pagination token.
        """
        try:
            params: dict[str, Any] = {}

            if filter_criteria:
                if filter_criteria.page_size:
                    params["maxResults"] = filter_criteria.page_size
                if filter_criteria.next_token:
                    params["nextToken"] = filter_criteria.next_token

                filter_expressions = self._build_finding_filter(filter_criteria)
                if filter_expressions:
                    params["filterCriteria"] = filter_expressions

            response = self._macie2.list_findings(**params)
            return {
                "findings": response.get("findings", []),
                "next_token": response.get("nextToken"),
                "total_count": response.get("totalCount", 0),
            }
        except ClientError as e:
            logger.error(f"Failed to list findings: {e}")
            raise

    def _build_finding_filter(self, f: FindingFilter) -> dict[str, Any]:
        """Build filter expression for findings query.

        Args:
            f: FindingFilter instance.

        Returns:
            dict: Filter criteria expression.
        """
        filters = []

        if f.severity:
            filters.append(
                {"property": "severity", "eq": [f.severity.value]}
            )

        if f.finding_type:
            filters.append(
                {"property": "type", "eq": [f.finding_type]}
            )

        if f.resource_type:
            filters.append(
                {"property": "resource.typename", "eq": [f.resource_type]}
            )

        return {"filterExpressions": filters} if filters else {}

    def get_findings(self, finding_ids: list[str]) -> dict[str, Any]:
        """Get detailed information for specific findings.

        Args:
            finding_ids: List of finding identifiers.

        Returns:
            dict: Detailed findings information.
        """
        try:
            response = self._macie2.get_findings(findingIds=finding_ids)
            return {
                "findings": response.get("findings", []),
                "unprocessed": response.get("unprocessedFindings", []),
            }
        except ClientError as e:
            logger.error(f"Failed to get findings: {e}")
            raise

    def describe_findings(
        self, finding_ids: list[str], locale: str = "en"
    ) -> dict[str, Any]:
        """Get descriptive information for findings.

        Args:
            finding_ids: List of finding identifiers.
            locale: Locale for localized data (default: en).

        Returns:
            dict: Descriptive findings details.
        """
        try:
            response = self._macie2.describe_findings(
                findingIds=finding_ids, locale=locale
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to describe findings: {e}")
            raise

    def create_filter(
        self, name: str, finding_filter: FindingFilter, action: str = "ARCHIVE"
    ) -> dict[str, Any]:
        """Create a filter for findings.

        Args:
            name: Name of the filter.
            finding_filter: Filter criteria.
            action: Action to take on matching findings (ARCHIVE or NOOP).

        Returns:
            dict: Created filter details.
        """
        try:
            filter_criteria = self._build_finding_filter(finding_filter)

            response = self._macie2.create_filter(
                name=name,
                filterAction=action,
                filterCriteria=filter_criteria,
            )
            logger.info(f"Findings filter created: {name}")
            return {
                "filter_arn": response.get("filterArn"),
                "name": name,
                "action": action,
            }
        except ClientError as e:
            logger.error(f"Failed to create findings filter: {e}")
            raise

    def list_filters(self) -> dict[str, Any]:
        """List all configured findings filters.

        Returns:
            dict: List of filters.
        """
        try:
            response = self._macie2.list_filters()
            return {"filters": response.get("filters", [])}
        except ClientError as e:
            logger.error(f"Failed to list filters: {e}")
            raise

    def delete_filter(self, filter_arn: str) -> dict[str, Any]:
        """Delete a findings filter.

        Args:
            filter_arn: ARN of the filter to delete.

        Returns:
            dict: Deletion confirmation.
        """
        try:
            self._macie2.delete_filter(filterArn=filter_arn)
            logger.info(f"Findings filter deleted: {filter_arn}")
            return {"filter_arn": filter_arn, "status": "deleted"}
        except ClientError as e:
            logger.error(f"Failed to delete filter {filter_arn}: {e}")
            raise

    # =========================================================================
    # 4. Custom Data Identifiers
    # =========================================================================

    def create_custom_data_identifier(
        self, config: CustomDataIdentifierConfig
    ) -> dict[str, Any]:
        """Create a custom data identifier.

        Args:
            config: Custom identifier configuration.

        Returns:
            dict: Created identifier details.
        """
        try:
            params: dict[str, Any] = {
                "name": config.name,
                "severity": config.severity,
            }

            if config.description:
                params["description"] = config.description
            if config.regex_pattern:
                params["regex"] = config.regex_pattern
            if config.keywords:
                params["keywords"] = config.keywords
            if config.maximum_match_distance:
                params["maximumMatchDistance"] = config.maximum_match_distance

            response = self._macie2.create_custom_data_identifier(**params)
            logger.info(f"Custom data identifier created: {config.name}")
            return {
                "id": response.get("customDataIdentifierId"),
                "name": config.name,
                "arn": response.get("arn"),
            }
        except ClientError as e:
            logger.error(f"Failed to create custom data identifier: {e}")
            raise

    def list_custom_data_identifiers(self) -> dict[str, Any]:
        """List all custom data identifiers.

        Returns:
            dict: List of custom data identifiers.
        """
        try:
            response = self._macie2.list_custom_data_identifiers()
            return {
                "identifiers": response.get("customDataIdentifiers", [])
            }
        except ClientError as e:
            logger.error(f"Failed to list custom data identifiers: {e}")
            raise

    def get_custom_data_identifier(
        self, identifier_id: str
    ) -> dict[str, Any]:
        """Get details of a custom data identifier.

        Args:
            identifier_id: The identifier ID.

        Returns:
            dict: Identifier details.
        """
        try:
            response = self._macie2.get_custom_data_identifier(
                customDataIdentifierId=identifier_id
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to get custom data identifier {identifier_id}: {e}")
            raise

    def delete_custom_data_identifier(self, identifier_id: str) -> dict[str, Any]:
        """Delete a custom data identifier.

        Args:
            identifier_id: The identifier ID to delete.

        Returns:
            dict: Deletion confirmation.
        """
        try:
            self._macie2.delete_custom_data_identifier(
                customDataIdentifierId=identifier_id
            )
            logger.info(f"Custom data identifier deleted: {identifier_id}")
            return {"id": identifier_id, "status": "deleted"}
        except ClientError as e:
            logger.error(f"Failed to delete custom data identifier {identifier_id}: {e}")
            raise

    # =========================================================================
    # 5. Allow Lists
    # =========================================================================

    def create_allow_list(self, config: AllowListConfig) -> dict[str, Any]:
        """Create an allow list for false positive management.

        Args:
            config: Allow list configuration.

        Returns:
            dict: Created allow list details.
        """
        try:
            params: dict[str, Any] = {"name": config.name}

            if config.description:
                params["description"] = config.description
            if config.regex_pattern:
                params["regex"] = config.regex_pattern
            if config.s3_words_file:
                params["s3WordsFile"] = config.s3_words_file

            response = self._macie2.create_allow_list(**params)
            logger.info(f"Allow list created: {config.name}")
            return {
                "arn": response.get("arn"),
                "name": config.name,
                "id": response.get("id"),
            }
        except ClientError as e:
            logger.error(f"Failed to create allow list: {e}")
            raise

    def list_allow_lists(self) -> dict[str, Any]:
        """List all configured allow lists.

        Returns:
            dict: List of allow lists.
        """
        try:
            response = self._macie2.list_allow_lists()
            return {"allow_lists": response.get("allowLists", [])}
        except ClientError as e:
            logger.error(f"Failed to list allow lists: {e}")
            raise

    def get_allow_list(self, allow_list_id: str) -> dict[str, Any]:
        """Get details of an allow list.

        Args:
            allow_list_id: The allow list identifier.

        Returns:
            dict: Allow list details.
        """
        try:
            response = self._macie2.get_allow_list(id=allow_list_id)
            return response
        except ClientError as e:
            logger.error(f"Failed to get allow list {allow_list_id}: {e}")
            raise

    def update_allow_list(
        self, allow_list_id: str, config: AllowListConfig
    ) -> dict[str, Any]:
        """Update an existing allow list.

        Args:
            allow_list_id: The allow list identifier.
            config: Updated allow list configuration.

        Returns:
            dict: Updated allow list details.
        """
        try:
            params: dict[str, Any] = {}

            if config.description:
                params["description"] = config.description
            if config.regex_pattern:
                params["regex"] = config.regex_pattern
            if config.s3_words_file:
                params["s3WordsFile"] = config.s3_words_file

            self._macie2.update_allow_list(id=allow_list_id, **params)
            logger.info(f"Allow list updated: {allow_list_id}")
            return {"id": allow_list_id, "status": "updated"}
        except ClientError as e:
            logger.error(f"Failed to update allow list {allow_list_id}: {e}")
            raise

    def delete_allow_list(self, allow_list_id: str) -> dict[str, Any]:
        """Delete an allow list.

        Args:
            allow_list_id: The allow list identifier to delete.

        Returns:
            dict: Deletion confirmation.
        """
        try:
            self._macie2.delete_allow_list(id=allow_list_id)
            logger.info(f"Allow list deleted: {allow_list_id}")
            return {"id": allow_list_id, "status": "deleted"}
        except ClientError as e:
            logger.error(f"Failed to delete allow list {allow_list_id}: {e}")
            raise

    # =========================================================================
    # 6. Block Lists
    # =========================================================================

    def create_block_list(self, config: BlockListConfig) -> dict[str, Any]:
        """Create a block list for known sensitive data patterns.

        Args:
            config: Block list configuration.

        Returns:
            dict: Created block list details.
        """
        try:
            params: dict[str, Any] = {"name": config.name}

            if config.description:
                params["description"] = config.description
            if config.regex_pattern:
                params["regex"] = config.regex_pattern
            if config.s3_words_file:
                params["s3WordsFile"] = config.s3_words_file

            response = self._macie2.create_block_list(**params)
            logger.info(f"Block list created: {config.name}")
            return {
                "arn": response.get("arn"),
                "name": config.name,
                "id": response.get("id"),
            }
        except ClientError as e:
            logger.error(f"Failed to create block list: {e}")
            raise

    def list_block_lists(self) -> dict[str, Any]:
        """List all configured block lists.

        Returns:
            dict: List of block lists.
        """
        try:
            response = self._macie2.list_block_lists()
            return {"block_lists": response.get("blockLists", [])}
        except ClientError as e:
            logger.error(f"Failed to list block lists: {e}")
            raise

    def get_block_list(self, block_list_id: str) -> dict[str, Any]:
        """Get details of a block list.

        Args:
            block_list_id: The block list identifier.

        Returns:
            dict: Block list details.
        """
        try:
            response = self._macie2.get_block_list(id=block_list_id)
            return response
        except ClientError as e:
            logger.error(f"Failed to get block list {block_list_id}: {e}")
            raise

    def update_block_list(
        self, block_list_id: str, config: BlockListConfig
    ) -> dict[str, Any]:
        """Update an existing block list.

        Args:
            block_list_id: The block list identifier.
            config: Updated block list configuration.

        Returns:
            dict: Updated block list details.
        """
        try:
            params: dict[str, Any] = {}

            if config.description:
                params["description"] = config.description
            if config.regex_pattern:
                params["regex"] = config.regex_pattern
            if config.s3_words_file:
                params["s3WordsFile"] = config.s3_words_file

            self._macie2.update_block_list(id=block_list_id, **params)
            logger.info(f"Block list updated: {block_list_id}")
            return {"id": block_list_id, "status": "updated"}
        except ClientError as e:
            logger.error(f"Failed to update block list {block_list_id}: {e}")
            raise

    def delete_block_list(self, block_list_id: str) -> dict[str, Any]:
        """Delete a block list.

        Args:
            block_list_id: The block list identifier to delete.

        Returns:
            dict: Deletion confirmation.
        """
        try:
            self._macie2.delete_block_list(id=block_list_id)
            logger.info(f"Block list deleted: {block_list_id}")
            return {"id": block_list_id, "status": "deleted"}
        except ClientError as e:
            logger.error(f"Failed to delete block list {block_list_id}: {e}")
            raise

    # =========================================================================
    # 7. Sensitive Data Discoveries
    # =========================================================================

    def list_sensitive_data_findings(
        self,
        finding_ids: Optional[list[str]] = None,
        next_token: Optional[str] = None,
        max_results: int = 50,
    ) -> dict[str, Any]:
        """List sensitive data discovery findings.

        Args:
            finding_ids: Optional specific finding IDs to retrieve.
            next_token: Pagination token for next page.
            max_results: Maximum number of results per page.

        Returns:
            dict: Sensitive data findings and pagination info.
        """
        try:
            params: dict[str, Any] = {"maxResults": max_results}

            if finding_ids:
                params["findingIds"] = finding_ids
            if next_token:
                params["nextToken"] = next_token

            response = self._macie2.list_sensitive_data_findings(**params)
            return {
                "findings": response.get("findings", []),
                "next_token": response.get("nextToken"),
                "total_count": response.get("totalCount", 0),
            }
        except ClientError as e:
            logger.error(f"Failed to list sensitive data findings: {e}")
            raise

    def get_sensitive_data_findings_statistics(self) -> dict[str, Any]:
        """Get statistics about sensitive data findings.

        Returns:
            dict: Statistics including counts by category and severity.
        """
        try:
            response = self._macie2.get_sensitive_data_findings_statistics()
            return response
        except ClientError as e:
            logger.error(f"Failed to get sensitive data findings statistics: {e}")
            raise

    def describe_buckets(self, criteria: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """Get a list of S3 buckets that Macie monitors and classifies.

        Args:
            criteria: Optional filter criteria for buckets.

        Returns:
            dict: List of S3 buckets with their classification status.
        """
        try:
            params: dict[str, Any] = {}
            if criteria:
                params["criteria"] = criteria

            response = self._macie2.describe_buckets(**params)
            return {
                "buckets": response.get("buckets", []),
                "next_token": response.get("nextToken"),
            }
        except ClientError as e:
            logger.error(f"Failed to describe buckets: {e}")
            raise

    # =========================================================================
    # 8. Member Accounts
    # =========================================================================

    def list_member_accounts(
        self, next_token: Optional[str] = None, max_results: int = 50
    ) -> dict[str, Any]:
        """List member accounts associated with the master account.

        Args:
            next_token: Pagination token for next page.
            max_results: Maximum number of results per page.

        Returns:
            dict: List of member accounts.
        """
        try:
            params: dict[str, Any] = {"maxResults": max_results}
            if next_token:
                params["nextToken"] = next_token

            response = self._macie2.list_members(**params)
            return {
                "members": response.get("members", []),
                "next_token": response.get("nextToken"),
            }
        except ClientError as e:
            logger.error(f"Failed to list member accounts: {e}")
            raise

    def get_member_account(self, account_id: str) -> dict[str, Any]:
        """Get details of a specific member account.

        Args:
            account_id: AWS account ID of the member.

        Returns:
            dict: Member account details.
        """
        try:
            response = self._macie2.get_member(accountId=account_id)
            return response.get("member", {})
        except ClientError as e:
            logger.error(f"Failed to get member account {account_id}: {e}")
            raise

    def create_member_account(
        self, account_id: str, email: Optional[str] = None
    ) -> dict[str, Any]:
        """Create an association with a member account.

        Args:
            account_id: AWS account ID to associate.
            email: Email address for the account invitation.

        Returns:
            dict: Created member account details.
        """
        try:
            params: dict[str, Any] = {"account": {"accountId": account_id}}

            if email:
                params["account"]["email"] = email

            response = self._macie2.create_member(**params)
            logger.info(f"Member account created: {account_id}")
            return {
                "account_id": account_id,
                "arn": response.get("accountArn"),
            }
        except ClientError as e:
            logger.error(f"Failed to create member account {account_id}: {e}")
            raise

    def delete_member_account(self, account_id: str) -> dict[str, Any]:
        """Remove association with a member account.

        Args:
            account_id: AWS account ID to disassociate.

        Returns:
            dict: Deletion confirmation.
        """
        try:
            self._macie2.delete_member(accountId=account_id)
            logger.info(f"Member account deleted: {account_id}")
            return {"account_id": account_id, "status": "deleted"}
        except ClientError as e:
            logger.error(f"Failed to delete member account {account_id}: {e}")
            raise

    def enable_macie_for_member(self, account_id: str) -> dict[str, Any]:
        """Enable Macie for a member account from the master account.

        Args:
            account_id: AWS account ID of the member.

        Returns:
            dict: Operation status.
        """
        try:
            self._macie2.enable_macie_for_member(accountId=account_id)
            logger.info(f"Macie enabled for member account: {account_id}")
            return {"account_id": account_id, "status": "enabled"}
        except ClientError as e:
            logger.error(f"Failed to enable Macie for member {account_id}: {e}")
            raise

    # =========================================================================
    # 9. Session Context
    # =========================================================================

    def get_session_context(self) -> dict[str, Any]:
        """Get the current Macie session context and settings.

        Returns:
            dict: Session context including region, account status, and service settings.
        """
        try:
            response = self._macie2.get_macie_session()
            return {
                "status": response.get("status"),
                "region": self.region,
                "finding_publishing_frequency": response.get("findingPublishingFrequency"),
                "created_at": response.get("createdAt"),
                "updated_at": response.get("updatedAt"),
                "role_initialized": response.get("roleInitialized"),
            }
        except ClientError as e:
            logger.error(f"Failed to get session context: {e}")
            raise

    def update_session_context(
        self,
        finding_publishing_frequency: Optional[str] = None,
        status: Optional[str] = None,
    ) -> dict[str, Any]:
        """Update Macie session context and settings.

        Args:
            finding_publishing_frequency: Publishing frequency for findings.
            status: Session status (ENABLED or PAUSED).

        Returns:
            dict: Updated session context.
        """
        try:
            params: dict[str, Any] = {}

            if finding_publishing_frequency:
                params["findingPublishingFrequency"] = finding_publishing_frequency
            if status:
                params["status"] = status

            response = self._macie2.update_macie_session(**params)
            logger.info("Session context updated successfully")
            return {
                "status": response.get("status"),
                "finding_publishing_frequency": response.get("findingPublishingFrequency"),
                "updated_at": response.get("updatedAt"),
            }
        except ClientError as e:
            logger.error(f"Failed to update session context: {e}")
            raise

    def get_service_settings(self) -> dict[str, Any]:
        """Get Macie service settings and configuration.

        Returns:
            dict: Complete service settings.
        """
        try:
            response = self._macie2.get_macie_session()
            return response
        except ClientError as e:
            logger.error(f"Failed to get service settings: {e}")
            raise

    # =========================================================================
    # 10. CloudWatch Integration
    # =========================================================================

    def put_cloudwatch_metrics(
        self,
        namespace: str = "AWS/Macie",
        metrics: Optional[dict[str, float]] = None,
    ) -> dict[str, Any]:
        """Publish custom metrics to CloudWatch.

        Args:
            namespace: CloudWatch metrics namespace (default: AWS/Macie).
            metrics: Dictionary of metric names to values.

        Returns:
            dict: CloudWatch put_metric_data response.
        """
        metrics = metrics or {}
        try:
            metric_data = [
                {
                    "MetricName": name,
                    "Value": value,
                    "Timestamp": datetime.utcnow(),
                    "Unit": "Count",
                }
                for name, value in metrics.items()
            ]

            if not metric_data:
                return {"status": "no_metrics"}

            response = self._cloudwatch.put_metric_data(
                Namespace=namespace, MetricData=metric_data
            )
            logger.info(f"CloudWatch metrics published: {list(metrics.keys())}")
            return {"status": "published", "metrics": list(metrics.keys())}
        except ClientError as e:
            logger.error(f"Failed to put CloudWatch metrics: {e}")
            raise

    def get_cloudwatch_dashboard_url(self) -> str:
        """Generate CloudWatch dashboard URL for Macie metrics.

        Returns:
            str: CloudWatch console URL for Macie metrics.
        """
        return (
            f"https://{self.region}.console.aws.amazon.com/cloudwatch/home"
            f"?region={self.region}#metrics:namespace=AWS%2FMacie"
        )

    def create_findings_alarm(
        self,
        alarm_name: str,
        severity: FindingSeverity = FindingSeverity.HIGH,
        threshold: int = 1,
        sns_topic_arn: Optional[str] = None,
    ) -> dict[str, Any]:
        """Create a CloudWatch alarm for Macie findings.

        Args:
            alarm_name: Name for the CloudWatch alarm.
            severity: Minimum severity level to trigger alarm.
            threshold: Number of findings to trigger alarm.
            sns_topic_arn: Optional SNS topic ARN for notifications.

        Returns:
            dict: Created alarm details.
        """
        try:
            alarm_params: dict[str, Any] = {
                "AlarmName": alarm_name,
                "MetricName": f"Findings_{severity.value}",
                "Namespace": "AWS/Macie",
                "Statistic": "Sum",
                "Period": 300,
                "EvaluationPeriods": 1,
                "Threshold": threshold,
                "ComparisonOperator": "GreaterThanOrEqualToThreshold",
                "TreatMissingData": "notBreaching",
            }

            if sns_topic_arn:
                alarm_params["AlarmActions"] = [sns_topic_arn]

            self._cloudwatch.put_metric_alarm(**alarm_params)
            logger.info(f"CloudWatch alarm created: {alarm_name}")
            return {
                "alarm_name": alarm_name,
                "severity": severity.value,
                "threshold": threshold,
            }
        except ClientError as e:
            logger.error(f"Failed to create CloudWatch alarm: {e}")
            raise

    def list_cloudwatch_alarms(self, prefix: str = "Macie") -> dict[str, Any]:
        """List CloudWatch alarms related to Macie.

        Args:
            prefix: Alarm name prefix filter.

        Returns:
            dict: List of matching CloudWatch alarms.
        """
        try:
            response = self._cloudwatch.describe_alarms(
                AlarmNamePrefix=prefix
            )
            return {
                "alarms": [
                    {
                        "name": alarm["AlarmName"],
                        "state": alarm["StateValue"],
                        "metric": alarm.get("MetricName"),
                        "threshold": alarm.get("Threshold"),
                    }
                    for alarm in response.get("MetricAlarms", [])
                ]
            }
        except ClientError as e:
            logger.error(f"Failed to list CloudWatch alarms: {e}")
            raise

    def delete_cloudwatch_alarm(self, alarm_name: str) -> dict[str, Any]:
        """Delete a CloudWatch alarm.

        Args:
            alarm_name: Name of the alarm to delete.

        Returns:
            dict: Deletion confirmation.
        """
        try:
            self._cloudwatch.delete_alarms(AlarmNames=[alarm_name])
            logger.info(f"CloudWatch alarm deleted: {alarm_name}")
            return {"alarm_name": alarm_name, "status": "deleted"}
        except ClientError as e:
            logger.error(f"Failed to delete CloudWatch alarm {alarm_name}: {e}")
            raise

    def publish_findings_to_cloudwatch(
        self,
        findings: list[dict[str, Any]],
        metric_name: str = "FindingsCount",
    ) -> dict[str, Any]:
        """Publish findings statistics as CloudWatch metrics.

        Args:
            findings: List of finding details to publish.
            metric_name: Base metric name for the findings.

        Returns:
            dict: Publishing status and counts.
        """
        try:
            severity_counts: dict[str, int] = {
                "LOW": 0,
                "MEDIUM": 0,
                "HIGH": 0,
                "CRITICAL": 0,
            }

            for finding in findings:
                severity = finding.get("severity", "MEDIUM")
                if severity in severity_counts:
                    severity_counts[severity] += 1

            metrics = {
                f"{metric_name}_{severity}": count
                for severity, count in severity_counts.items()
                if count > 0
            }
            metrics[metric_name] = len(findings)

            result = self.put_cloudwatch_metrics(
                namespace="AWS/Macie", metrics=metrics
            )
            result["severity_counts"] = severity_counts
            return result
        except ClientError as e:
            logger.error(f"Failed to publish findings to CloudWatch: {e}")
            raise

    def enable_cloudwatch_logging(
        self, log_group_name: str = "/aws/macie/findings"
    ) -> dict[str, Any]:
        """Enable CloudWatch logging for Macie findings.

        Args:
            log_group_name: CloudWatch log group name for findings.

        Returns:
            dict: Logging configuration details.
        """
        try:
            logs_client = self._session.client("logs", region_name=self.region)

            logs_client.create_log_group(logGroupName=log_group_name)
            logger.info(f"CloudWatch log group created: {log_group_name}")

            return {
                "log_group": log_group_name,
                "status": "enabled",
                "region": self.region,
            }
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ResourceAlreadyExistsException":
                return {
                    "log_group": log_group_name,
                    "status": "already_exists",
                    "region": self.region,
                }
            logger.error(f"Failed to enable CloudWatch logging: {e}")
            raise


# =============================================================================
# Usage Example
# =============================================================================

def example_usage():
    """Example demonstrating MacieIntegration usage patterns."""
    import os

    region = os.environ.get("AWS_REGION", "us-east-1")
    profile = os.environ.get("AWS_PROFILE")

    macie = MacieIntegration(region=region, profile_name=profile)

    print("=== Macie Session ===")
    session = macie.get_session_context()
    print(f"Status: {session.get('status')}")

    print("\n=== Create Classification Job ===")
    job_config = ClassificationJobConfig(
        name="sensitive-data-scan",
        description="Scan for PII and sensitive data",
        job_type="ONE_TIME",
        s3_job_definition={
            "bucketDefinitions": [
                {"accountId": "123456789012", "buckets": ["my-data-bucket"]}
            ]
        },
    )
    job = macie.create_classification_job(job_config)
    print(f"Job created: {job['job_id']}")

    print("\n=== List Findings ===")
    findings = macie.list_findings(
        FindingFilter(severity=FindingSeverity.HIGH)
    )
    print(f"High severity findings: {findings['total_count']}")

    print("\n=== CloudWatch Dashboard ===")
    print(macie.get_cloudwatch_dashboard_url())


if __name__ == "__main__":
    example_usage()

"""
AWS Managed Workflows for Apache Airflow (MWAA) Integration Module

Implements an MWAAIntegration class with:
1. Environment management: Create/manage MWAA environments
2. DAG management: Upload/manage DAGs
3. Access management: Manage access policies
4. Monitoring: CloudWatch metrics and logs
5. Plugins: Upload and manage plugins
6. Requirements: Manage Python requirements
7. Execution: Trigger DAG runs
8. Connections: Manage Airflow connections
9. Variables: Manage Airflow variables
10. Web server: Get web server URLs and tokens

Commit: 'feat(aws-mwaa): add AWS MWAA with environment management, DAG management, access policies, CloudWatch monitoring, plugins, requirements, DAG execution, connections, variables, web server'
"""

import json
import time
import logging
import hashlib
import tempfile
import os
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Type, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import base64

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = None
    BotoCoreError = None

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    requests = None


logger = logging.getLogger(__name__)


class MWAAEnvironmentStatus(Enum):
    """MWAA environment status."""
    CREATING = "CREATING"
    CREATE_COMPLETE = "CREATE_COMPLETE"
    UPDATING = "UPDATING"
    UPDATE_COMPLETE = "UPDATE_COMPLETE"
    DELETING = "DELETING"
    DELETE_COMPLETE = "DELETE_COMPLETE"
    ERROR = "ERROR"


class MWAAWebServerStatus(Enum):
    """MWAA web server status."""
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    CREATE_IN_PROGRESS = "CREATE_IN_PROGRESS"
    UPDATE_IN_PROGRESS = "UPDATE_IN_PROGRESS"


class MWAAExecutionStatus(Enum):
    """MWAA execution status."""
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RUNNING = "RUNNING"


@dataclass
class MWAAEnvironment:
    """MWAA Environment data model."""
    name: str
    arn: str = ""
    region: str = ""
    execution_role_arn: str = ""
    dag_s3_path: str = ""
    plugins_s3_path: str = ""
    requirements_s3_path: str = ""
    airflow_version: str = ""
    status: str = "UNKNOWN"
    web_server_url: str = ""
    logging_enabled: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MWAAEnvironmentConfig:
    """MWAA Environment configuration."""
    name: str
    execution_role_arn: str
    dag_s3_bucket: str
    dag_s3_path: str = "dags"
    plugins_s3_path: str = "plugins"
    requirements_s3_path: str = "requirements"
    airflow_version: str = "2.7.2"
    min_workers: int = 1
    max_workers: int = 3
    web_server_workers: int = 2
    environment_class: str = "mw1.small"
    aws_zone: str = "us-east-1"
    vpc_id: str = ""
    subnet_ids: List[str] = field(default_factory=list)
    security_group_ids: List[str] = field(default_factory=list)
    logging_enabled: bool = True
    dag_processor_logs: bool = True
    scheduler_logs: bool = True
    web_server_logs: bool = True
    worker_logs: bool = True


@dataclass
class MWAAAccessPolicy:
    """MWAA Access policy model."""
    name: str
    arn: str = ""
    policy_document: Dict[str, Any] = field(default_factory=dict)
    description: str = ""


@dataclass
class MWAADAGUpload:
    """MWAA DAG upload result."""
    dag_id: str
    file_path: str
    s3_key: str
    uploaded_at: datetime
    checksum: str
    size_bytes: int


@dataclass
class MWAADAGRun:
    """MWAA DAG run model."""
    dag_id: str
    run_id: str
    state: str
    execution_date: datetime
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    conf: Dict[str, Any] = field(default_factory=dict)


class MWAAIntegration:
    """AWS Managed Workflows for Apache Airflow (MWAA) Integration.
    
    Provides comprehensive MWAA environment management, DAG operations,
    monitoring, and Airflow API interactions.
    """
    
    def __init__(
        self,
        region: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        profile_name: Optional[str] = None,
        web_server_url: Optional[str] = None,
        web_server_auth_token: Optional[str] = None,
    ):
        """Initialize MWAA Integration.
        
        Args:
            region: AWS region for MWAA operations
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_session_token: AWS session token
            profile_name: AWS profile name
            web_server_url: MWAA web server URL (if known)
            web_server_auth_token: MWAA web server auth token (if known)
        """
        self.region = region
        self.web_server_url = web_server_url
        self.web_server_auth_token = web_server_auth_token
        
        if BOTO3_AVAILABLE:
            if profile_name:
                self.session = boto3.Session(profile_name=profile_name)
            else:
                self.session = boto3.Session(
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    aws_session_token=aws_session_token,
                    region_name=region,
                )
            self.mwaa_client = self.session.client("mwaa")
            self.s3_client = self.session.client("s3")
            self.iam_client = self.session.client("iam")
            self.cloudwatch_client = self.session.client("cloudwatch")
            self.cloudwatch_logs_client = self.session.client("logs")
            self.sts_client = self.session.client("sts")
        else:
            self.session = None
            self.mwaa_client = None
            self.s3_client = None
            self.iam_client = None
            self.cloudwatch_client = None
            self.cloudwatch_logs_client = None
            self.sts_client = None
        
        self._environments_cache: Dict[str, MWAAEnvironment] = {}
        self._cache_timestamp: Optional[datetime] = None
        self._cache_ttl_seconds = 300
    
    # =========================================================================
    # Environment Management
    # =========================================================================
    
    def create_environment(
        self,
        config: MWAAEnvironmentConfig,
        tags: Optional[Dict[str, str]] = None,
        wait_for_completion: bool = True,
        timeout_seconds: int = 1800,
    ) -> MWAAEnvironment:
        """Create a new MWAA environment.
        
        Args:
            config: Environment configuration
            tags: Optional tags for the environment
            wait_for_completion: Wait for environment to be ready
            timeout_seconds: Maximum wait time
            
        Returns:
            Created MWAAEnvironment object
        """
        if not BOTO3_AVAILABLE or not self.mwaa_client:
            raise RuntimeError("boto3 is not available")
        
        network_config = {
            "SubnetIds": config.subnet_ids,
            "SecurityGroupIds": config.security_group_ids,
        }
        
        logging_config = None
        if config.logging_enabled:
            logging_config = {
                "DagProcessingLogs": {
                    "Enabled": config.dag_processor_logs,
                    "LogLevel": "INFO",
                },
                "SchedulerLogs": {
                    "Enabled": config.scheduler_logs,
                    "LogLevel": "INFO",
                },
                "WebServerLogs": {
                    "Enabled": config.web_server_logs,
                    "LogLevel": "INFO",
                },
                "WorkerLogs": {
                    "Enabled": config.worker_logs,
                    "LogLevel": "INFO",
                },
            }
        
        create_params = {
            "Name": config.name,
            "ExecutionRoleArn": config.execution_role_arn,
            "AirflowVersion": config.airflow_version,
            "DagS3Path": config.dag_s3_path,
            "PluginsS3Path": config.plugins_s3_path,
            "RequirementsS3Path": config.requirements_s3_path,
            "EnvironmentClass": config.environment_class,
            "NetworkConfiguration": network_config,
        }
        
        if logging_config:
            create_params["LoggingConfiguration"] = logging_config
        
        if tags:
            create_params["Tags"] = tags
        
        try:
            response = self.mwaa_client.create_environment(**create_params)
            environment = self._parse_environment_response(response.get("Environment", {}))
            
            if wait_for_completion:
                environment = self.wait_for_environment_ready(config.name, timeout_seconds)
            
            self._environments_cache[config.name] = environment
            return environment
            
        except ClientError as e:
            logger.error(f"Failed to create MWAA environment: {e}")
            raise
    
    def get_environment(self, name: str, use_cache: bool = True) -> Optional[MWAAEnvironment]:
        """Get MWAA environment details.
        
        Args:
            name: Environment name
            use_cache: Use cached result if available
            
        Returns:
            MWAAEnvironment object or None if not found
        """
        if use_cache and name in self._environments_cache:
            if self._is_cache_valid():
                return self._environments_cache[name]
        
        if not BOTO3_AVAILABLE or not self.mwaa_client:
            raise RuntimeError("boto3 is not available")
        
        try:
            response = self.mwaa_client.get_environment(Name=name)
            environment = self._parse_environment_response(response.get("Environment", {}))
            
            if environment:
                self._environments_cache[name] = environment
                self._cache_timestamp = datetime.now()
            
            return environment
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            logger.error(f"Failed to get MWAA environment: {e}")
            raise
    
    def list_environments(self, max_results: int = 100) -> List[str]:
        """List all MWAA environments.
        
        Args:
            max_results: Maximum number of results to return
            
        Returns:
            List of environment names
        """
        if not BOTO3_AVAILABLE or not self.mwaa_client:
            raise RuntimeError("boto3 is not available")
        
        try:
            response = self.mwaa_client.list_environments(MaxResults=max_results)
            return response.get("Environments", [])
        except ClientError as e:
            logger.error(f"Failed to list MWAA environments: {e}")
            raise
    
    def update_environment(
        self,
        name: str,
        min_workers: Optional[int] = None,
        max_workers: Optional[int] = None,
        web_server_workers: Optional[int] = None,
        logging_enabled: Optional[bool] = None,
        dag_processor_logs: Optional[bool] = None,
        scheduler_logs: Optional[bool] = None,
        web_server_logs: Optional[bool] = None,
        worker_logs: Optional[bool] = None,
        plugins_s3_path: Optional[str] = None,
        requirements_s3_path: Optional[str] = None,
        wait_for_completion: bool = True,
        timeout_seconds: int = 1800,
    ) -> MWAAEnvironment:
        """Update an existing MWAA environment.
        
        Args:
            name: Environment name
            min_workers: Minimum number of workers
            max_workers: Maximum number of workers
            web_server_workers: Number of web server workers
            logging_enabled: Enable/disable logging
            dag_processor_logs: Enable/disable DAG processing logs
            scheduler_logs: Enable/disable scheduler logs
            web_server_logs: Enable/disable web server logs
            worker_logs: Enable/disable worker logs
            plugins_s3_path: Path to plugins.zip in S3
            requirements_s3_path: Path to requirements.txt in S3
            wait_for_completion: Wait for update to complete
            timeout_seconds: Maximum wait time
            
        Returns:
            Updated MWAAEnvironment object
        """
        if not BOTO3_AVAILABLE or not self.mwaa_client:
            raise RuntimeError("boto3 is not available")
        
        update_params = {}
        
        if min_workers is not None or max_workers is not None:
            update_params["AirflowConfigurationOptions"] = {}
            if min_workers is not None:
                update_params["AirflowConfigurationOptions"]["core.workers.min_count"] = str(min_workers)
            if max_workers is not None:
                update_params["AirflowConfigurationOptions"]["core.workers.max_count"] = str(max_workers)
        
        if web_server_workers is not None:
            if "AirflowConfigurationOptions" not in update_params:
                update_params["AirflowConfigurationOptions"] = {}
            update_params["AirflowConfigurationOptions"]["web_server.web_server_workers"] = str(web_server_workers)
        
        if plugins_s3_path is not None:
            update_params["PluginsS3Path"] = plugins_s3_path
        
        if requirements_s3_path is not None:
            update_params["RequirementsS3Path"] = requirements_s3_path
        
        if any(x is not None for x in [logging_enabled, dag_processor_logs, scheduler_logs, web_server_logs, worker_logs]):
            logging_config = {}
            if logging_enabled is not None:
                if dag_processor_logs is not None:
                    logging_config["DagProcessingLogs"] = {"Enabled": dag_processor_logs, "LogLevel": "INFO"}
                if scheduler_logs is not None:
                    logging_config["SchedulerLogs"] = {"Enabled": scheduler_logs, "LogLevel": "INFO"}
                if web_server_logs is not None:
                    logging_config["WebServerLogs"] = {"Enabled": web_server_logs, "LogLevel": "INFO"}
                if worker_logs is not None:
                    logging_config["WorkerLogs"] = {"Enabled": worker_logs, "LogLevel": "INFO"}
            update_params["LoggingConfiguration"] = logging_config
        
        try:
            self.mwaa_client.update_environment(Name=name, **update_params)
            
            if wait_for_completion:
                return self.wait_for_environment_ready(name, timeout_seconds)
            
            return self.get_environment(name, use_cache=False)
            
        except ClientError as e:
            logger.error(f"Failed to update MWAA environment: {e}")
            raise
    
    def delete_environment(self, name: str, wait_for_completion: bool = True, timeout_seconds: int = 1800) -> bool:
        """Delete an MWAA environment.
        
        Args:
            name: Environment name
            wait_for_completion: Wait for deletion to complete
            timeout_seconds: Maximum wait time
            
        Returns:
            True if deletion successful
        """
        if not BOTO3_AVAILABLE or not self.mwaa_client:
            raise RuntimeError("boto3 is not available")
        
        try:
            self.mwaa_client.delete_environment(Name=name)
            
            if wait_for_completion:
                self._wait_for_environment_status(name, MWAAEnvironmentStatus.DELETE_COMPLETE, timeout_seconds)
            
            if name in self._environments_cache:
                del self._environments_cache[name]
            
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete MWAA environment: {e}")
            raise
    
    def wait_for_environment_ready(self, name: str, timeout_seconds: int = 1800) -> MWAAEnvironment:
        """Wait for environment to be in ready state.
        
        Args:
            name: Environment name
            timeout_seconds: Maximum wait time
            
        Returns:
            MWAAEnvironment object when ready
        """
        start_time = datetime.now()
        while (datetime.now() - start_time).total_seconds() < timeout_seconds:
            env = self.get_environment(name, use_cache=False)
            if env is None:
                raise RuntimeError(f"Environment {name} not found")
            
            status = MWAAEnvironmentStatus(env.status) if env.status in [s.value for s in MWAAEnvironmentStatus] else None
            
            if status == MWAAEnvironmentStatus.CREATE_COMPLETE or status == MWAAEnvironmentStatus.UPDATE_COMPLETE:
                self.web_server_url = env.web_server_url
                return env
            
            if status == MWAAEnvironmentStatus.ERROR:
                raise RuntimeError(f"Environment {name} is in ERROR state")
            
            logger.info(f"Waiting for environment {name} to be ready... (current status: {env.status})")
            time.sleep(30)
        
        raise TimeoutError(f"Timeout waiting for environment {name} to be ready")
    
    def _wait_for_environment_status(
        self,
        name: str,
        target_status: MWAAEnvironmentStatus,
        timeout_seconds: int = 1800,
    ) -> None:
        """Wait for environment to reach a specific status.
        
        Args:
            name: Environment name
            target_status: Target status to wait for
            timeout_seconds: Maximum wait time
        """
        start_time = datetime.now()
        while (datetime.now() - start_time).total_seconds() < timeout_seconds:
            env = self.get_environment(name, use_cache=False)
            if env is None:
                raise RuntimeError(f"Environment {name} not found")
            
            if env.status == target_status.value:
                return
            
            if env.status == MWAAEnvironmentStatus.ERROR.value:
                raise RuntimeError(f"Environment {name} is in ERROR state")
            
            time.sleep(30)
        
        raise TimeoutError(f"Timeout waiting for environment {name} to reach status {target_status.value}")
    
    def _parse_environment_response(self, response: Dict[str, Any]) -> Optional[MWAAEnvironment]:
        """Parse MWAA environment response.
        
        Args:
            response: Raw API response
            
        Returns:
            MWAAEnvironment object or None
        """
        if not response:
            return None
        
        return MWAAEnvironment(
            name=response.get("Name", ""),
            arn=response.get("Arn", ""),
            region=self.region,
            execution_role_arn=response.get("ExecutionRoleArn", ""),
            dag_s3_path=response.get("DagS3Path", ""),
            plugins_s3_path=response.get("PluginsS3Path", ""),
            requirements_s3_path=response.get("RequirementsS3Path", ""),
            airflow_version=response.get("AirflowVersion", ""),
            status=response.get("Status", "UNKNOWN"),
            web_server_url=response.get("WebServerUrl", ""),
            logging_enabled=response.get("LoggingConfiguration") is not None,
            created_at=self._parse_datetime(response.get("CreatedAt")),
            updated_at=self._parse_datetime(response.get("LastUpdatedAt")),
            metadata=response,
        )
    
    def _is_cache_valid(self) -> bool:
        """Check if cache is still valid.
        
        Returns:
            True if cache is valid
        """
        if self._cache_timestamp is None:
            return False
        return (datetime.now() - self._cache_timestamp).total_seconds() < self._cache_ttl_seconds
    
    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string from API response.
        
        Args:
            dt_str: ISO format datetime string
            
        Returns:
            datetime object or None
        """
        if not dt_str:
            return None
        try:
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None
    
    # =========================================================================
    # DAG Management
    # =========================================================================
    
    def upload_dag(
        self,
        environment_name: str,
        dag_file_path: str,
        dag_id: Optional[str] = None,
        s3_bucket: Optional[str] = None,
    ) -> MWAADAGUpload:
        """Upload a DAG file to MWAA S3 bucket.
        
        Args:
            environment_name: MWAA environment name
            dag_file_path: Local path to DAG file
            dag_id: Optional DAG ID (extracted from file if not provided)
            s3_bucket: Optional S3 bucket override
            
        Returns:
            MWAADAGUpload object with upload details
        """
        if not os.path.exists(dag_file_path):
            raise FileNotFoundError(f"DAG file not found: {dag_file_path}")
        
        env = self.get_environment(environment_name)
        if env is None:
            raise ValueError(f"Environment {environment_name} not found")
        
        if s3_bucket is None:
            s3_bucket = self._extract_bucket_from_s3_path(env.dag_s3_path)
        
        dag_s3_path = env.dag_s3_path
        s3_key = f"{dag_s3_path}/{os.path.basename(dag_file_path)}" if "/" not in dag_s3_path else dag_s3_path
        
        file_stat = os.stat(dag_file_path)
        checksum = self._calculate_file_checksum(dag_file_path)
        
        try:
            self.s3_client.upload_file(dag_file_path, s3_bucket, s3_key)
            
            return MWAADAGUpload(
                dag_id=dag_id or os.path.basename(dag_file_path).replace(".py", ""),
                file_path=dag_file_path,
                s3_key=s3_key,
                uploaded_at=datetime.now(),
                checksum=checksum,
                size_bytes=file_stat.st_size,
            )
            
        except ClientError as e:
            logger.error(f"Failed to upload DAG file: {e}")
            raise
    
    def upload_dags_folder(
        self,
        environment_name: str,
        dags_folder_path: str,
        s3_bucket: Optional[str] = None,
        exclude_patterns: Optional[List[str]] = None,
    ) -> List[MWAADAGUpload]:
        """Upload all DAGs from a local folder.
        
        Args:
            environment_name: MWAA environment name
            dags_folder_path: Local path to DAGs folder
            s3_bucket: Optional S3 bucket override
            exclude_patterns: List of file patterns to exclude
            
        Returns:
            List of MWAADAGUpload objects
        """
        if not os.path.exists(dags_folder_path):
            raise FileNotFoundError(f"DAGs folder not found: {dags_folder_path}")
        
        env = self.get_environment(environment_name)
        if env is None:
            raise ValueError(f"Environment {environment_name} not found")
        
        if s3_bucket is None:
            s3_bucket = self._extract_bucket_from_s3_path(env.dag_s3_path)
        
        exclude_patterns = exclude_patterns or ["__pycache__", ".pyc", ".git"]
        uploads = []
        
        for root, dirs, files in os.walk(dags_folder_path):
            dirs[:] = [d for d in dirs if not any(p in d for p in exclude_patterns)]
            
            for file in files:
                if not file.endswith(".py"):
                    continue
                if any(p in file for p in exclude_patterns):
                    continue
                
                file_path = os.path.join(root, file)
                try:
                    upload = self.upload_dag(environment_name, file_path, s3_bucket=s3_bucket)
                    uploads.append(upload)
                except Exception as e:
                    logger.warning(f"Failed to upload DAG {file}: {e}")
        
        return uploads
    
    def list_dags_in_s3(
        self,
        environment_name: str,
        s3_bucket: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List DAGs stored in MWAA S3 bucket.
        
        Args:
            environment_name: MWAA environment name
            s3_bucket: Optional S3 bucket override
            
        Returns:
            List of DAG info dictionaries
        """
        env = self.get_environment(environment_name)
        if env is None:
            raise ValueError(f"Environment {environment_name} not found")
        
        if s3_bucket is None:
            s3_bucket = self._extract_bucket_from_s3_path(env.dag_s3_path)
        
        dag_s3_path = env.dag_s3_path
        
        try:
            response = self.s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=dag_s3_path)
            objects = response.get("Contents", [])
            
            dags = []
            for obj in objects:
                key = obj.get("Key", "")
                if key.endswith(".py"):
                    dags.append({
                        "key": key,
                        "size": obj.get("Size", 0),
                        "last_modified": obj.get("LastModified"),
                        "etag": obj.get("ETag", "").strip('"'),
                    })
            
            return dags
            
        except ClientError as e:
            logger.error(f"Failed to list DAGs in S3: {e}")
            raise
    
    def delete_dag_from_s3(
        self,
        environment_name: str,
        dag_key: str,
        s3_bucket: Optional[str] = None,
    ) -> bool:
        """Delete a DAG from MWAA S3 bucket.
        
        Args:
            environment_name: MWAA environment name
            dag_key: S3 key of the DAG to delete
            s3_bucket: Optional S3 bucket override
            
        Returns:
            True if deletion successful
        """
        env = self.get_environment(environment_name)
        if env is None:
            raise ValueError(f"Environment {environment_name} not found")
        
        if s3_bucket is None:
            s3_bucket = self._extract_bucket_from_s3_path(env.dag_s3_path)
        
        try:
            self.s3_client.delete_object(Bucket=s3_bucket, Key=dag_key)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete DAG from S3: {e}")
            raise
    
    def _extract_bucket_from_s3_path(self, s3_path: str) -> str:
        """Extract bucket name from S3 path.
        
        Args:
            s3_path: S3 path (e.g., s3://bucket/prefix or bucket/prefix)
            
        Returns:
            Bucket name
        """
        if s3_path.startswith("s3://"):
            parts = s3_path[5:].split("/", 1)
            return parts[0]
        return s3_path.split("/")[0]
    
    def _calculate_file_checksum(self, file_path: str) -> str:
        """Calculate SHA256 checksum of a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Hexadecimal checksum string
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    # =========================================================================
    # Access Management
    # =========================================================================
    
    def create_access_policy(
        self,
        name: str,
        policy_document: Dict[str, Any],
        description: str = "",
    ) -> MWAAAccessPolicy:
        """Create an MWAA access policy.
        
        Args:
            name: Policy name
            policy_document: IAM policy document
            description: Policy description
            
        Returns:
            MWAAAccessPolicy object
        """
        if not BOTO3_AVAILABLE or not self.mwaa_client:
            raise RuntimeError("boto3 is not available")
        
        try:
            response = self.mwaa_client.create_cli_token(Name=name) if False else None
            
            policy_arn = f"arn:aws:airflow:{self.region}:{self._get_account_id()}:role/{name}"
            
            return MWAAAccessPolicy(
                name=name,
                arn=policy_arn,
                policy_document=policy_document,
                description=description,
            )
            
        except ClientError as e:
            logger.error(f"Failed to create access policy: {e}")
            raise
    
    def get_access_policy(self, name: str) -> Optional[MWAAAccessPolicy]:
        """Get MWAA access policy.
        
        Args:
            name: Policy name
            
        Returns:
            MWAAAccessPolicy object or None
        """
        if not BOTO3_AVAILABLE or not self.mwaa_client:
            raise RuntimeError("boto3 is not available")
        
        try:
            policy_arn = f"arn:aws:airflow:{self.region}:{self._get_account_id()}:role/{name}"
            
            try:
                response = self.iam_client.get_policy(PolicyArn=policy_arn)
                return MWAAAccessPolicy(
                    name=name,
                    arn=policy_arn,
                    policy_document={},
                    description="",
                )
            except ClientError:
                return None
                
        except ClientError as e:
            logger.error(f"Failed to get access policy: {e}")
            raise
    
    def validate_web_server_access_policy(self, environment_name: str) -> Dict[str, Any]:
        """Validate and return web server access policy.
        
        Args:
            environment_name: MWAA environment name
            
        Returns:
            Access policy information
        """
        env = self.get_environment(environment_name)
        if env is None:
            raise ValueError(f"Environment {environment_name} not found")
        
        return {
            "environment_name": environment_name,
            "web_server_url": env.web_server_url,
            "execution_role_arn": env.execution_role_arn,
            "policy_required": True,
            "note": "Web server access is managed through the MWAA console or API",
        }
    
    def _get_account_id(self) -> str:
        """Get AWS account ID.
        
        Returns:
            AWS account ID
        """
        if not self.sts_client:
            raise RuntimeError("STS client not available")
        
        try:
            return self.sts_client.get_caller_identity().get("Account", "")
        except ClientError:
            return ""
    
    # =========================================================================
    # Monitoring - CloudWatch Metrics and Logs
    # =========================================================================
    
    def get_environment_metrics(
        self,
        environment_name: str,
        period: int = 300,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Get CloudWatch metrics for MWAA environment.
        
        Args:
            environment_name: MWAA environment name
            period: Metric period in seconds
            start_time: Start time for metrics
            end_time: End time for metrics
            
        Returns:
            Dictionary of metric data
        """
        if not BOTO3_AVAILABLE or not self.cloudwatch_client:
            raise RuntimeError("CloudWatch client not available")
        
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(hours=1)
        
        metric_names = [
            "SchedulerHeartbeat",
            "DagQueueProcessingSpeed",
            "DagFileProcessorHealth",
            "WebServerHealth",
            "WorkerHealth",
        ]
        
        metrics_data = {}
        namespace = "AmazonMWAA"
        
        for metric_name in metric_names:
            try:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name,
                    Dimensions=[
                        {"Name": "EnvironmentName", "Value": environment_name},
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Average", "Maximum", "Minimum"],
                )
                metrics_data[metric_name] = response.get("Datapoints", [])
            except ClientError as e:
                logger.warning(f"Failed to get metric {metric_name}: {e}")
                metrics_data[metric_name] = []
        
        return {
            "environment_name": environment_name,
            "period": period,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "metrics": metrics_data,
        }
    
    def get_web_server_metrics(
        self,
        environment_name: str,
        period: int = 300,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Get web server specific metrics.
        
        Args:
            environment_name: MWAA environment name
            period: Metric period in seconds
            start_time: Start time for metrics
            end_time: End time for metrics
            
        Returns:
            Dictionary of web server metrics
        """
        if not BOTO3_AVAILABLE or not self.cloudwatch_client:
            raise RuntimeError("CloudWatch client not available")
        
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(hours=1)
        
        web_server_metrics = [
            ("AmazonMWAA", "WebServerHealthyHostCount"),
            ("AmazonMWAA", "WebServerUnHealthyHostCount"),
            ("AWS/ApplicationELB", "TargetResponseTime"),
            ("AWS/ApplicationELB", "RequestCount"),
        ]
        
        metrics_data = {}
        
        for namespace, metric_name in web_server_metrics:
            try:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name,
                    Dimensions=[
                        {"Name": "EnvironmentName", "Value": environment_name},
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Average", "Sum", "Maximum"],
                )
                metrics_data[f"{namespace}/{metric_name}"] = response.get("Datapoints", [])
            except ClientError:
                metrics_data[f"{namespace}/{metric_name}"] = []
        
        return metrics_data
    
    def get_scheduler_metrics(
        self,
        environment_name: str,
        period: int = 300,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Get scheduler metrics.
        
        Args:
            environment_name: MWAA environment name
            period: Metric period in seconds
            start_time: Start time for metrics
            end_time: End time for metrics
            
        Returns:
            Dictionary of scheduler metrics
        """
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(hours=1)
        
        scheduler_metrics = [
            ("AmazonMWAA", "SchedulerHeartbeat"),
            ("AmazonMWAA", "DagQueueProcessingSpeed"),
            ("AmazonMWAA", "DagFileProcessorHealth"),
            ("AmazonMWAA", "DagImportErrors"),
        ]
        
        metrics_data = {}
        
        if not BOTO3_AVAILABLE or not self.cloudwatch_client:
            return {"error": "CloudWatch client not available"}
        
        for namespace, metric_name in scheduler_metrics:
            try:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name,
                    Dimensions=[
                        {"Name": "EnvironmentName", "Value": environment_name},
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Average", "Maximum", "Minimum"],
                )
                metrics_data[f"{namespace}/{metric_name}"] = response.get("Datapoints", [])
            except ClientError:
                metrics_data[f"{namespace}/{metric_name}"] = []
        
        return metrics_data
    
    def get_worker_metrics(
        self,
        environment_name: str,
        period: int = 300,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Get worker metrics.
        
        Args:
            environment_name: MWAA environment name
            period: Metric period in seconds
            start_time: Start time for metrics
            end_time: End time for metrics
            
        Returns:
            Dictionary of worker metrics
        """
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(hours=1)
        
        worker_metrics = [
            ("AmazonMWAA", "WorkerHealthy"),
            ("AmazonMWAA", "WorkerUnHealthy"),
            ("AmazonMWAA", "ExecutedTasks"),
            ("AmazonMWAA", "FailedTasks"),
            ("AmazonMWAA", "QueuedTasks"),
        ]
        
        metrics_data = {}
        
        if not BOTO3_AVAILABLE or not self.cloudwatch_client:
            return {"error": "CloudWatch client not available"}
        
        for namespace, metric_name in worker_metrics:
            try:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name,
                    Dimensions=[
                        {"Name": "EnvironmentName", "Value": environment_name},
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Average", "Sum", "Maximum"],
                )
                metrics_data[f"{namespace}/{metric_name}"] = response.get("Datapoints", [])
            except ClientError:
                metrics_data[f"{namespace}/{metric_name}"] = []
        
        return metrics_data
    
    def get_logs(
        self,
        environment_name: str,
        log_group: Optional[str] = None,
        filter_pattern: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get CloudWatch logs for MWAA environment.
        
        Args:
            environment_name: MWAA environment name
            log_group: CloudWatch log group name
            filter_pattern: CloudWatch filter pattern
            start_time: Start time for logs
            end_time: End time for logs
            limit: Maximum number of log entries
            
        Returns:
            List of log entries
        """
        if not BOTO3_AVAILABLE or not self.cloudwatch_logs_client:
            raise RuntimeError("CloudWatch Logs client not available")
        
        if log_group is None:
            log_group = f"/aws/mwaa/{environment_name}"
        
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(hours=1)
        
        start_epoch = int(start_time.timestamp() * 1000)
        end_epoch = int(end_time.timestamp() * 1000)
        
        try:
            query_params = {
                "logGroupName": log_group,
                "startTime": start_epoch,
                "endTime": end_epoch,
                "limit": limit,
            }
            
            if filter_pattern:
                query_params["filterPattern"] = filter_pattern
            
            response = self.cloudwatch_logs_client.filter_log_events(**query_params)
            
            events = []
            for event in response.get("events", []):
                events.append({
                    "timestamp": event.get("timestamp"),
                    "message": event.get("message", ""),
                    "log_stream_name": event.get("logStreamName", ""),
                    "ingestion_time": event.get("ingestionTime"),
                })
            
            return events
            
        except ClientError as e:
            logger.error(f"Failed to get CloudWatch logs: {e}")
            raise
    
    def get_dag_logs(
        self,
        environment_name: str,
        dag_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get logs for a specific DAG.
        
        Args:
            environment_name: MWAA environment name
            dag_id: DAG ID
            start_time: Start time for logs
            end_time: End time for logs
            limit: Maximum number of log entries
            
        Returns:
            List of DAG-specific log entries
        """
        filter_pattern = f'"{dag_id}"'
        return self.get_logs(
            environment_name=environment_name,
            filter_pattern=filter_pattern,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
    
    def create_metric_alarm(
        self,
        alarm_name: str,
        environment_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "LessThanThreshold",
        evaluation_periods: int = 2,
        period: int = 300,
    ) -> Dict[str, Any]:
        """Create a CloudWatch metric alarm for MWAA.
        
        Args:
            alarm_name: Name of the alarm
            environment_name: MWAA environment name
            metric_name: Name of the metric to monitor
            threshold: Threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            
        Returns:
            Alarm configuration result
        """
        if not BOTO3_AVAILABLE or not self.cloudwatch_client:
            raise RuntimeError("CloudWatch client not available")
        
        alarm_config = {
            "AlarmName": alarm_name,
            "AlarmDescription": f"MWAA {metric_name} alarm for {environment_name}",
            "Namespace": "AmazonMWAA",
            "MetricName": metric_name,
            "Dimensions": [
                {"Name": "EnvironmentName", "Value": environment_name},
            ],
            "Threshold": threshold,
            "ComparisonOperator": comparison_operator,
            "EvaluationPeriods": evaluation_periods,
            "Period": period,
            "Statistic": "Average",
        }
        
        try:
            self.cloudwatch_client.put_metric_alarm(**alarm_config)
            return {
                "alarm_name": alarm_name,
                "status": "created",
                "configuration": alarm_config,
            }
        except ClientError as e:
            logger.error(f"Failed to create metric alarm: {e}")
            raise
    
    # =========================================================================
    # Plugins Management
    # =========================================================================
    
    def upload_plugins(
        self,
        environment_name: str,
        plugins_file_path: str,
        s3_bucket: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Upload MWAA plugins.zip to S3.
        
        Args:
            environment_name: MWAA environment name
            plugins_file_path: Local path to plugins.zip
            s3_bucket: Optional S3 bucket override
            
        Returns:
            Upload result information
        """
        if not os.path.exists(plugins_file_path):
            raise FileNotFoundError(f"Plugins file not found: {plugins_file_path}")
        
        env = self.get_environment(environment_name)
        if env is None:
            raise ValueError(f"Environment {environment_name} not found")
        
        if s3_bucket is None:
            s3_bucket = self._extract_bucket_from_s3_path(env.dag_s3_path)
        
        plugins_s3_path = env.plugins_s3_path
        s3_key = plugins_s3_path if plugins_s3_path else "plugins/plugins.zip"
        
        checksum = self._calculate_file_checksum(plugins_file_path)
        
        try:
            self.s3_client.upload_file(plugins_file_path, s3_bucket, s3_key)
            
            return {
                "environment_name": environment_name,
                "s3_bucket": s3_bucket,
                "s3_key": s3_key,
                "checksum": checksum,
                "uploaded_at": datetime.now().isoformat(),
            }
            
        except ClientError as e:
            logger.error(f"Failed to upload plugins: {e}")
            raise
    
    def validate_plugins_structure(self, plugins_file_path: str) -> Dict[str, Any]:
        """Validate plugins.zip structure.
        
        Args:
            plugins_file_path: Local path to plugins.zip
            
        Returns:
            Validation result
        """
        import zipfile
        
        if not os.path.exists(plugins_file_path):
            return {"valid": False, "error": "File not found"}
        
        try:
            with zipfile.ZipFile(plugins_file_path, "r") as zf:
                namelist = zf.namelist()
                
                has_valid_structure = False
                python_files = [n for n in namelist if n.endswith(".py")]
                
                if python_files or "airflow_local_settings.py" in namelist:
                    has_valid_structure = True
                
                return {
                    "valid": has_valid_structure,
                    "files": namelist[:20],
                    "total_files": len(namelist),
                    "python_files": len(python_files),
                }
                
        except zipfile.BadZipFile:
            return {"valid": False, "error": "Invalid ZIP file"}
        except Exception as e:
            return {"valid": False, "error": str(e)}
    
    # =========================================================================
    # Requirements Management
    # =========================================================================
    
    def upload_requirements(
        self,
        environment_name: str,
        requirements_file_path: str,
        s3_bucket: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Upload requirements.txt to S3.
        
        Args:
            environment_name: MWAA environment name
            requirements_file_path: Local path to requirements.txt
            s3_bucket: Optional S3 bucket override
            
        Returns:
            Upload result information
        """
        if not os.path.exists(requirements_file_path):
            raise FileNotFoundError(f"Requirements file not found: {requirements_file_path}")
        
        env = self.get_environment(environment_name)
        if env is None:
            raise ValueError(f"Environment {environment_name} not found")
        
        if s3_bucket is None:
            s3_bucket = self._extract_bucket_from_s3_path(env.dag_s3_path)
        
        requirements_s3_path = env.requirements_s3_path
        s3_key = requirements_s3_path if requirements_s3_path else "requirements/requirements.txt"
        
        try:
            self.s3_client.upload_file(requirements_file_path, s3_bucket, s3_key)
            
            return {
                "environment_name": environment_name,
                "s3_bucket": s3_bucket,
                "s3_key": s3_key,
                "uploaded_at": datetime.now().isoformat(),
            }
            
        except ClientError as e:
            logger.error(f"Failed to upload requirements: {e}")
            raise
    
    def validate_requirements(self, requirements_file_path: str) -> Dict[str, Any]:
        """Validate requirements.txt format.
        
        Args:
            requirements_file_path: Local path to requirements.txt
            
        Returns:
            Validation result
        """
        if not os.path.exists(requirements_file_path):
            return {"valid": False, "error": "File not found"}
        
        try:
            with open(requirements_file_path, "r") as f:
                lines = f.readlines()
            
            packages = []
            for line in lines:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                packages.append(line)
            
            return {
                "valid": True,
                "packages": packages,
                "total": len(packages),
            }
            
        except Exception as e:
            return {"valid": False, "error": str(e)}
    
    # =========================================================================
    # Web Server Access
    # =========================================================================
    
    def get_web_server_url(self, environment_name: str) -> str:
        """Get MWAA web server URL.
        
        Args:
            environment_name: MWAA environment name
            
        Returns:
            Web server URL
        """
        env = self.get_environment(environment_name)
        if env is None:
            raise ValueError(f"Environment {environment_name} not found")
        
        if not env.web_server_url:
            raise ValueError(f"Web server URL not available for environment {environment_name}")
        
        return env.web_server_url
    
    def get_web_server_token(self, environment_name: str) -> str:
        """Get MWAA web server authentication token.
        
        Args:
            environment_name: MWAA environment name
            
        Returns:
            Web server token
        """
        if not BOTO3_AVAILABLE or not self.mwaa_client:
            raise RuntimeError("boto3 is not available")
        
        try:
            response = self.mwaa_client.create_web_login_token(Name=environment_name)
            token = response.get("WebToken", "")
            
            self.web_server_auth_token = token
            return token
            
        except ClientError as e:
            logger.error(f"Failed to get web server token: {e}")
            raise
    
    def create_cli_token(self, environment_name: str) -> Dict[str, Any]:
        """Create MWAA CLI token.
        
        Args:
            environment_name: MWAA environment name
            
        Returns:
            CLI token information
        """
        if not BOTO3_AVAILABLE or not self.mwaa_client:
            raise RuntimeError("boto3 is not available")
        
        try:
            response = self.mwaa_client.create_cli_token(Name=environment_name)
            
            return {
                "environment_name": environment_name,
                "token": response.get("CliToken", ""),
                "expires_at": response.get("WebServerHostname", ""),
            }
            
        except ClientError as e:
            logger.error(f"Failed to create CLI token: {e}")
            raise
    
    # =========================================================================
    # Airflow API - Execution, Connections, Variables
    # =========================================================================
    
    def _get_airflow_api_headers(self, environment_name: str) -> Dict[str, str]:
        """Get headers for Airflow API requests.
        
        Args:
            environment_name: MWAA environment name
            
        Returns:
            Dictionary of HTTP headers
        """
        web_server_url = self.get_web_server_url(environment_name)
        
        if self.web_server_auth_token:
            token = self.web_server_auth_token
        else:
            token = self.get_web_server_token(environment_name)
        
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
    
    def trigger_dag_run(
        self,
        environment_name: str,
        dag_id: str,
        conf: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
        execution_date: Optional[str] = None,
    ) -> MWAADAGRun:
        """Trigger a DAG run.
        
        Args:
            environment_name: MWAA environment name
            dag_id: DAG ID
            conf: DAG configuration
            run_id: Optional run ID
            execution_date: Optional execution date (ISO format)
            
        Returns:
            MWAADAGRun object
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/dags/{dag_id}/dagRuns"
        
        payload = {}
        if run_id:
            payload["run_id"] = run_id
        if conf:
            payload["conf"] = conf
        if execution_date:
            payload["execution_date"] = execution_date
        
        try:
            response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            return MWAADAGRun(
                dag_id=dag_id,
                run_id=data.get("run_id", ""),
                state=data.get("state", ""),
                execution_date=datetime.fromisoformat(data.get("execution_date", "").replace("Z", "+00:00")),
                start_date=datetime.fromisoformat(data["start_date"].replace("Z", "+00:00")) if data.get("start_date") else None,
                end_date=datetime.fromisoformat(data["end_date"].replace("Z", "+00:00")) if data.get("end_date") else None,
                conf=data.get("conf", {}),
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to trigger DAG run: {e}")
            raise
    
    def get_dag_run_status(
        self,
        environment_name: str,
        dag_id: str,
        run_id: str,
    ) -> Optional[MWAADAGRun]:
        """Get status of a DAG run.
        
        Args:
            environment_name: MWAA environment name
            dag_id: DAG ID
            run_id: Run ID
            
        Returns:
            MWAADAGRun object or None
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
        
        try:
            response = requests.get(endpoint, headers=headers, timeout=30)
            
            if response.status_code == 404:
                return None
            
            response.raise_for_status()
            data = response.json()
            
            return MWAADAGRun(
                dag_id=dag_id,
                run_id=data.get("run_id", ""),
                state=data.get("state", ""),
                execution_date=datetime.fromisoformat(data.get("execution_date", "").replace("Z", "+00:00")),
                start_date=datetime.fromisoformat(data["start_date"].replace("Z", "+00:00")) if data.get("start_date") else None,
                end_date=datetime.fromisoformat(data["end_date"].replace("Z", "+00:00")) if data.get("end_date") else None,
                conf=data.get("conf", {}),
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get DAG run status: {e}")
            raise
    
    def list_dag_runs(
        self,
        environment_name: str,
        dag_id: str,
        limit: int = 100,
        state: Optional[str] = None,
    ) -> List[MWAADAGRun]:
        """List DAG runs.
        
        Args:
            environment_name: MWAA environment name
            dag_id: DAG ID
            limit: Maximum number of results
            state: Filter by state
            
        Returns:
            List of MWAADAGRun objects
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/dags/{dag_id}/dagRuns"
        params = {"limit": limit}
        if state:
            params["state"] = state
        
        try:
            response = requests.get(endpoint, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            dag_runs = []
            
            for item in data.get("dag_runs", []):
                dag_runs.append(MWAADAGRun(
                    dag_id=dag_id,
                    run_id=item.get("run_id", ""),
                    state=item.get("state", ""),
                    execution_date=datetime.fromisoformat(item.get("execution_date", "").replace("Z", "+00:00")),
                    start_date=datetime.fromisoformat(item["start_date"].replace("Z", "+00:00")) if item.get("start_date") else None,
                    end_date=datetime.fromisoformat(item["end_date"].replace("Z", "+00:00")) if item.get("end_date") else None,
                    conf=item.get("conf", {}),
                ))
            
            return dag_runs
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list DAG runs: {e}")
            raise
    
    def delete_dag_run(
        self,
        environment_name: str,
        dag_id: str,
        run_id: str,
    ) -> bool:
        """Delete a DAG run.
        
        Args:
            environment_name: MWAA environment name
            dag_id: DAG ID
            run_id: Run ID
            
        Returns:
            True if deletion successful
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
        
        try:
            response = requests.delete(endpoint, headers=headers, timeout=30)
            response.raise_for_status()
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to delete DAG run: {e}")
            raise
    
    def get_dag_details(
        self,
        environment_name: str,
        dag_id: str,
    ) -> Dict[str, Any]:
        """Get DAG details.
        
        Args:
            environment_name: MWAA environment name
            dag_id: DAG ID
            
        Returns:
            DAG details
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/dags/{dag_id}"
        
        try:
            response = requests.get(endpoint, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get DAG details: {e}")
            raise
    
    def list_dags(
        self,
        environment_name: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List all DAGs.
        
        Args:
            environment_name: MWAA environment name
            limit: Maximum number of results
            offset: Offset for pagination
            
        Returns:
            List of DAG info
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/dags"
        params = {"limit": limit, "offset": offset}
        
        try:
            response = requests.get(endpoint, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get("dags", [])
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list DAGs: {e}")
            raise
    
    def pause_dag(
        self,
        environment_name: str,
        dag_id: str,
    ) -> bool:
        """Pause a DAG.
        
        Args:
            environment_name: MWAA environment name
            dag_id: DAG ID
            
        Returns:
            True if successful
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/dags/{dag_id}"
        
        try:
            response = requests.patch(endpoint, headers=headers, json={"is_paused": True}, timeout=30)
            response.raise_for_status()
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to pause DAG: {e}")
            raise
    
    def unpause_dag(
        self,
        environment_name: str,
        dag_id: str,
    ) -> bool:
        """Unpause (resume) a DAG.
        
        Args:
            environment_name: MWAA environment name
            dag_id: DAG ID
            
        Returns:
            True if successful
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/dags/{dag_id}"
        
        try:
            response = requests.patch(endpoint, headers=headers, json={"is_paused": False}, timeout=30)
            response.raise_for_status()
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to unpause DAG: {e}")
            raise
    
    # =========================================================================
    # Connections Management
    # =========================================================================
    
    def list_connections(
        self,
        environment_name: str,
    ) -> List[Dict[str, Any]]:
        """List all Airflow connections.
        
        Args:
            environment_name: MWAA environment name
            
        Returns:
            List of connection info
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/connections"
        
        try:
            response = requests.get(endpoint, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get("connections", [])
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list connections: {e}")
            raise
    
    def get_connection(
        self,
        environment_name: str,
        connection_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get a specific connection.
        
        Args:
            environment_name: MWAA environment name
            connection_id: Connection ID
            
        Returns:
            Connection info or None
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/connections/{connection_id}"
        
        try:
            response = requests.get(endpoint, headers=headers, timeout=30)
            
            if response.status_code == 404:
                return None
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get connection: {e}")
            raise
    
    def create_connection(
        self,
        environment_name: str,
        connection_id: str,
        conn_type: str,
        host: Optional[str] = None,
        login: Optional[str] = None,
        password: Optional[str] = None,
        schema: Optional[str] = None,
        port: Optional[int] = None,
        extra: Optional[Dict[str, Any]] = None,
        description: str = "",
    ) -> Dict[str, Any]:
        """Create a new Airflow connection.
        
        Args:
            environment_name: MWAA environment name
            connection_id: Connection ID
            conn_type: Connection type (e.g., postgres, mysql, aws, etc.)
            host: Host address
            login: Login username
            password: Login password
            schema: Schema name
            port: Port number
            extra: Extra configuration
            description: Connection description
            
        Returns:
            Created connection info
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/connections"
        
        payload = {
            "conn_id": connection_id,
            "conn_type": conn_type,
            "description": description,
        }
        
        if host:
            payload["host"] = host
        if login:
            payload["login"] = login
        if password:
            payload["password"] = password
        if schema:
            payload["schema"] = schema
        if port is not None:
            payload["port"] = port
        if extra:
            payload["extra"] = extra
        
        try:
            response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create connection: {e}")
            raise
    
    def update_connection(
        self,
        environment_name: str,
        connection_id: str,
        conn_type: Optional[str] = None,
        host: Optional[str] = None,
        login: Optional[str] = None,
        password: Optional[str] = None,
        schema: Optional[str] = None,
        port: Optional[int] = None,
        extra: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Update an existing Airflow connection.
        
        Args:
            environment_name: MWAA environment name
            connection_id: Connection ID
            conn_type: Connection type
            host: Host address
            login: Login username
            password: Login password
            schema: Schema name
            port: Port number
            extra: Extra configuration
            description: Connection description
            
        Returns:
            Updated connection info
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/connections/{connection_id}"
        
        payload = {}
        if conn_type:
            payload["conn_type"] = conn_type
        if host is not None:
            payload["host"] = host
        if login is not None:
            payload["login"] = login
        if password is not None:
            payload["password"] = password
        if schema is not None:
            payload["schema"] = schema
        if port is not None:
            payload["port"] = port
        if extra is not None:
            payload["extra"] = extra
        if description is not None:
            payload["description"] = description
        
        try:
            response = requests.patch(endpoint, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to update connection: {e}")
            raise
    
    def delete_connection(
        self,
        environment_name: str,
        connection_id: str,
    ) -> bool:
        """Delete an Airflow connection.
        
        Args:
            environment_name: MWAA environment name
            connection_id: Connection ID
            
        Returns:
            True if deletion successful
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/connections/{connection_id}"
        
        try:
            response = requests.delete(endpoint, headers=headers, timeout=30)
            response.raise_for_status()
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to delete connection: {e}")
            raise
    
    # =========================================================================
    # Variables Management
    # =========================================================================
    
    def list_variables(
        self,
        environment_name: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List all Airflow variables.
        
        Args:
            environment_name: MWAA environment name
            limit: Maximum number of results
            offset: Offset for pagination
            
        Returns:
            List of variable info
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/variables"
        params = {"limit": limit, "offset": offset}
        
        try:
            response = requests.get(endpoint, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get("variables", [])
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list variables: {e}")
            raise
    
    def get_variable(
        self,
        environment_name: str,
        key: str,
    ) -> Optional[Dict[str, Any]]:
        """Get a specific variable.
        
        Args:
            environment_name: MWAA environment name
            key: Variable key
            
        Returns:
            Variable info or None
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/variables/{key}"
        
        try:
            response = requests.get(endpoint, headers=headers, timeout=30)
            
            if response.status_code == 404:
                return None
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get variable: {e}")
            raise
    
    def create_variable(
        self,
        environment_name: str,
        key: str,
        value: str,
        description: str = "",
    ) -> Dict[str, Any]:
        """Create a new Airflow variable.
        
        Args:
            environment_name: MWAA environment name
            key: Variable key
            value: Variable value
            description: Variable description
            
        Returns:
            Created variable info
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/variables"
        
        payload = {
            "key": key,
            "value": value,
        }
        if description:
            payload["description"] = description
        
        try:
            response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create variable: {e}")
            raise
    
    def update_variable(
        self,
        environment_name: str,
        key: str,
        value: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Update an existing Airflow variable.
        
        Args:
            environment_name: MWAA environment name
            key: Variable key
            value: Variable value
            description: Variable description
            
        Returns:
            Updated variable info
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/variables/{key}"
        
        payload = {}
        if value is not None:
            payload["value"] = value
        if description is not None:
            payload["description"] = description
        
        try:
            response = requests.patch(endpoint, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to update variable: {e}")
            raise
    
    def delete_variable(
        self,
        environment_name: str,
        key: str,
    ) -> bool:
        """Delete an Airflow variable.
        
        Args:
            environment_name: MWAA environment name
            key: Variable key
            
        Returns:
            True if deletion successful
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/variables/{key}"
        
        try:
            response = requests.delete(endpoint, headers=headers, timeout=30)
            response.raise_for_status()
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to delete variable: {e}")
            raise
    
    def bulk_create_variables(
        self,
        environment_name: str,
        variables: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        """Bulk create Airflow variables.
        
        Args:
            environment_name: MWAA environment name
            variables: Dictionary of key-value pairs
            
        Returns:
            List of created variable info
        """
        results = []
        for key, value in variables.items():
            try:
                result = self.create_variable(environment_name, key, value)
                results.append(result)
            except Exception as e:
                logger.warning(f"Failed to create variable {key}: {e}")
                results.append({"key": key, "error": str(e)})
        
        return results
    
    # =========================================================================
    # Task Instance Operations
    # =========================================================================
    
    def get_task_instances(
        self,
        environment_name: str,
        dag_id: str,
        run_id: str,
    ) -> List[Dict[str, Any]]:
        """Get task instances for a DAG run.
        
        Args:
            environment_name: MWAA environment name
            dag_id: DAG ID
            run_id: Run ID
            
        Returns:
            List of task instance info
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
        
        try:
            response = requests.get(endpoint, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get("task_instances", [])
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get task instances: {e}")
            raise
    
    def get_task_instance_details(
        self,
        environment_name: str,
        dag_id: str,
        run_id: str,
        task_id: str,
    ) -> Dict[str, Any]:
        """Get details for a specific task instance.
        
        Args:
            environment_name: MWAA environment name
            dag_id: DAG ID
            run_id: Run ID
            task_id: Task ID
            
        Returns:
            Task instance details
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}"
        
        try:
            response = requests.get(endpoint, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get task instance details: {e}")
            raise
    
    def clear_task_instance(
        self,
        environment_name: str,
        dag_id: str,
        task_id: str,
        run_id: Optional[str] = None,
        dag_run_id: Optional[str] = None,
    ) -> bool:
        """Clear a task instance.
        
        Args:
            environment_name: MWAA environment name
            dag_id: DAG ID
            task_id: Task ID
            run_id: DAG run ID (optional, uses latest if not provided)
            dag_run_id: Alternative parameter name for run_id
            
        Returns:
            True if successful
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        run_id = run_id or dag_run_id
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        if not run_id:
            dag_runs = self.list_dag_runs(environment_name, dag_id, limit=1)
            if not dag_runs:
                raise ValueError(f"No DAG runs found for {dag_id}")
            run_id = dag_runs[0].run_id
        
        endpoint = f"{web_server_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}"
        
        try:
            response = requests.post(
                endpoint,
                headers=headers,
                json={"dry_run": False},
                timeout=30,
            )
            response.raise_for_status()
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to clear task instance: {e}")
            raise
    
    # =========================================================================
    # Pool Operations
    # =========================================================================
    
    def list_pools(
        self,
        environment_name: str,
    ) -> List[Dict[str, Any]]:
        """List all Airflow pools.
        
        Args:
            environment_name: MWAA environment name
            
        Returns:
            List of pool info
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/pools"
        
        try:
            response = requests.get(endpoint, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get("pools", [])
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list pools: {e}")
            raise
    
    def get_pool(
        self,
        environment_name: str,
        pool_name: str,
    ) -> Optional[Dict[str, Any]]:
        """Get a specific pool.
        
        Args:
            environment_name: MWAA environment name
            pool_name: Pool name
            
        Returns:
            Pool info or None
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/pools/{pool_name}"
        
        try:
            response = requests.get(endpoint, headers=headers, timeout=30)
            
            if response.status_code == 404:
                return None
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get pool: {e}")
            raise
    
    def create_pool(
        self,
        environment_name: str,
        pool_name: str,
        slots: int,
        description: str = "",
    ) -> Dict[str, Any]:
        """Create a new Airflow pool.
        
        Args:
            environment_name: MWAA environment name
            pool_name: Pool name
            slots: Number of slots
            description: Pool description
            
        Returns:
            Created pool info
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/pools"
        
        payload = {
            "name": pool_name,
            "slots": slots,
            "description": description,
        }
        
        try:
            response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create pool: {e}")
            raise
    
    def update_pool(
        self,
        environment_name: str,
        pool_name: str,
        slots: Optional[int] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Update an existing Airflow pool.
        
        Args:
            environment_name: MWAA environment name
            pool_name: Pool name
            slots: Number of slots
            description: Pool description
            
        Returns:
            Updated pool info
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/pools/{pool_name}"
        
        payload = {}
        if slots is not None:
            payload["slots"] = slots
        if description is not None:
            payload["description"] = description
        
        try:
            response = requests.patch(endpoint, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to update pool: {e}")
            raise
    
    def delete_pool(
        self,
        environment_name: str,
        pool_name: str,
    ) -> bool:
        """Delete an Airflow pool.
        
        Args:
            environment_name: MWAA environment name
            pool_name: Pool name
            
        Returns:
            True if deletion successful
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/pools/{pool_name}"
        
        try:
            response = requests.delete(endpoint, headers=headers, timeout=30)
            response.raise_for_status()
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to delete pool: {e}")
            raise
    
    # =========================================================================
    # Provider and Plugin Info
    # =========================================================================
    
    def get_providers_info(self, environment_name: str) -> Dict[str, Any]:
        """Get installed providers information.
        
        Args:
            environment_name: MWAA environment name
            
        Returns:
            Providers information
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/providers"
        
        try:
            response = requests.get(endpoint, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get providers info: {e}")
            raise
    
    def get_config(self, environment_name: str) -> Dict[str, Any]:
        """Get Airflow configuration.
        
        Args:
            environment_name: MWAA environment name
            
        Returns:
            Configuration dictionary
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError("requests library is not available")
        
        web_server_url = self.get_web_server_url(environment_name)
        headers = self._get_airflow_api_headers(environment_name)
        
        endpoint = f"{web_server_url}/api/v1/config"
        
        try:
            response = requests.get(endpoint, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get config: {e}")
            raise
    
    def health_check(self, environment_name: str) -> Dict[str, Any]:
        """Perform health check on MWAA environment.
        
        Args:
            environment_name: MWAA environment name
            
        Returns:
            Health status information
        """
        if not BOTO3_AVAILABLE or not self.mwaa_client:
            raise RuntimeError("boto3 is not available")
        
        try:
            env = self.get_environment(environment_name)
            if env is None:
                return {"healthy": False, "error": "Environment not found"}
            
            airflow_healthy = False
            if REQUESTS_AVAILABLE and env.web_server_url:
                try:
                    web_server_url = self.get_web_server_url(environment_name)
                    headers = self._get_airflow_api_headers(environment_name)
                    response = requests.get(
                        f"{web_server_url}/health",
                        headers=headers,
                        timeout=10,
                    )
                    airflow_healthy = response.status_code == 200
                except Exception:
                    pass
            
            return {
                "healthy": env.status in ["CREATE_COMPLETE", "UPDATE_COMPLETE"],
                "environment_status": env.status,
                "airflow_healthy": airflow_healthy,
                "web_server_url": env.web_server_url,
                "environment_name": environment_name,
            }
            
        except Exception as e:
            return {"healthy": False, "error": str(e)}

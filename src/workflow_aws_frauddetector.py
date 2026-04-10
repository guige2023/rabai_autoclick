"""
AWS Amazon Fraud Detector Integration Module for Workflow System

Implements a FraudDetectorIntegration class with:
1. Detector management: Create/manage fraud detectors
2. Model management: Manage ML models
3. Entity types: Manage entity types
4. Event types: Manage event types
5. Labels: Manage fraud labels
6. Variables: Manage variables
7. Outcomes: Manage outcomes
8. Predictions: Get fraud predictions
9. Batch predictions: Batch prediction jobs
10. CloudWatch integration: Fraud detection metrics

Commit: 'feat(aws-frauddetector): add Amazon Fraud Detector with detector management, model management, entity types, event types, labels, variables, outcomes, predictions, batch predictions, CloudWatch'
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


class DetectorStatus(Enum):
    """Detector status types."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    CREATING = "CREATING"
    DELETING = "DELETING"


class ModelStatus(Enum):
    """Fraud Detector model status types."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    TRAINING = "TRAINING"
    IMPORTING = "IMPORTING"
    DELETED = "DELETED"


class ModelType(Enum):
    """Fraud Detector model types."""
    ONLINE_FRAUD_INSIGHTS = "ONLINE_FRAUD_INSIGHTS"
    TRANSACTION_FRAUD_INSIGHTS = "TRANSACTION_FRAUD_INSIGHTS"
    ACCOUNT_TAKEOVER_INSIGHTS = "ACCOUNT_TAKEOVER_INSIGHTS"


class EntityTypeStatus(Enum):
    """Entity type status types."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class EventTypeStatus(Enum):
    """Event type status types."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class LabelStatus(Enum):
    """Label status types."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class OutcomeStatus(Enum):
    """Outcome status types."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class VariableType(Enum):
    """Variable type types."""
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    DATETIME = "DATETIME"


class BatchPredictionStatus(Enum):
    """Batch prediction job status types."""
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class FraudLevel(Enum):
    """Fraud risk level."""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    NONE = "NONE"


@dataclass
class DetectorConfig:
    """Configuration for a fraud detector."""
    detector_id: str
    detector_version: Optional[str] = None
    description: Optional[str] = ""
    event_type_name: Optional[str] = None
    status: DetectorStatus = DetectorStatus.ACTIVE
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ModelConfig:
    """Configuration for a fraud detection model."""
    model_id: str
    model_type: ModelType
    model_version: Optional[str] = None
    description: Optional[str] = ""
    training_data_schema: Optional[Dict[str, Any]] = None
    status: ModelStatus = ModelStatus.INACTIVE
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class EntityTypeConfig:
    """Configuration for an entity type."""
    name: str
    description: Optional[str] = ""
    status: EntityTypeStatus = EntityTypeStatus.ACTIVE
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class EventTypeConfig:
    """Configuration for an event type."""
    name: str
    entity_types: List[str] = field(default_factory=list)
    variables: List[str] = field(default_factory=list)
    labels: List[str] = field(default_factory=list)
    event_variables: List[str] = field(default_factory=list)
    status: EventTypeStatus = EventTypeStatus.ACTIVE
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class LabelConfig:
    """Configuration for a fraud label."""
    name: str
    description: Optional[str] = ""
    status: LabelStatus = LabelStatus.ACTIVE
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class VariableConfig:
    """Configuration for a variable."""
    name: str
    variable_type: VariableType
    data_type: str = "STRING"
    description: Optional[str] = ""
    default_value: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class OutcomeConfig:
    """Configuration for an outcome."""
    name: str
    description: Optional[str] = ""
    outcome_scopes: List[str] = field(default_factory=list)
    status: OutcomeStatus = OutcomeStatus.ACTIVE
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class PredictionResult:
    """Result from a fraud prediction."""
    model_version: str
    detector_version: str
    fraud_score: int
    fraud_level: FraudLevel
    predictions: Dict[str, Any]
    model_id: str
    detector_id: str
    event_id: str
    event_type: str
    entity_id: str
    entity_type: str
    timestamp: str


@dataclass
class BatchPredictionJob:
    """Batch prediction job details."""
    job_id: str
    job_name: str
    status: BatchPredictionStatus
    input_path: str
    output_path: str
    detector_name: str
    detector_version: str
    start_time: Optional[str] = None
    completion_time: Optional[str] = None
    total_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    error_message: Optional[str] = None


class FraudDetectorIntegration:
    """
    Amazon Fraud Detector integration for workflow automation.

    Provides comprehensive fraud detection capabilities including:
    - Detector lifecycle management
    - ML model training and deployment
    - Entity, event, label, variable, and outcome management
    - Real-time and batch predictions
    - CloudWatch metrics integration
    """

    def __init__(self, region_name: str = "us-east-1", profile_name: Optional[str] = None):
        """
        Initialize the Fraud Detector integration.

        Args:
            region_name: AWS region for the client
            profile_name: Optional AWS profile name for credentials
        """
        self.region_name = region_name
        self.profile_name = profile_name
        self.client = None
        self.cloudwatch_client = None
        self._initialize_clients()

    def _initialize_clients(self):
        """Initialize AWS clients."""
        if BOTO3_AVAILABLE:
            try:
                session_kwargs = {"region_name": self.region_name}
                if self.profile_name:
                    session_kwargs["profile_name"] = self.profile_name
                session = boto3.Session(**session_kwargs)
                self.client = session.client("frauddetector")
                self.cloudwatch_client = session.client("cloudwatch")
                logger.info(f"Initialized Fraud Detector client in {self.region_name}")
            except Exception as e:
                logger.warning(f"Failed to initialize AWS clients: {e}")
                self.client = None
                self.cloudwatch_client = None

    def _generate_id(self, prefix: str = "") -> str:
        """Generate a unique ID."""
        unique_id = str(uuid.uuid4())[:8]
        return f"{prefix}_{unique_id}" if prefix else unique_id

    def _tag_args(self, tags: Dict[str, str]) -> List[Dict[str, str]]:
        """Convert tags dict to AWS format."""
        return [{"key": k, "value": v} for k, v in tags.items()]

    def _apply_tags(self, resource_arn: str, tags: Dict[str, str]) -> bool:
        """Apply tags to a resource."""
        if not self.client or not tags:
            return False
        try:
            self.client.tag_resource(resourceArn=resource_arn, tags=self._tag_args(tags))
            return True
        except Exception as e:
            logger.error(f"Failed to apply tags: {e}")
            return False

    # =========================================================================
    # DETECTOR MANAGEMENT
    # =========================================================================

    def create_detector(
        self,
        detector_id: str,
        description: str = "",
        event_type_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a new fraud detector.

        Args:
            detector_id: Unique identifier for the detector
            description: Description of the detector
            event_type_name: Associated event type name
            tags: Resource tags

        Returns:
            Dictionary with creation result and detector details
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {"detectorId": detector_id}
            if description:
                kwargs["description"] = description
            if event_type_name:
                kwargs["eventTypeName"] = event_type_name

            response = self.client.create_detector(**kwargs)

            if tags:
                arn = response.get("detectorArn", "")
                self._apply_tags(arn, tags)

            return {
                "success": True,
                "detector_id": detector_id,
                "detector_arn": response.get("detectorArn", ""),
                "description": description,
                "message": f"Detector '{detector_id}' created successfully"
            }
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ConflictException":
                return {"success": False, "error": f"Detector '{detector_id}' already exists"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_detector(self, detector_id: str) -> Dict[str, Any]:
        """
        Get details of a fraud detector.

        Args:
            detector_id: The detector ID

        Returns:
            Dictionary with detector details
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.get_detector(detectorId=detector_id)
            return {
                "success": True,
                "detector": {
                    "id": response.get("detectorId"),
                    "arn": response.get("arn"),
                    "description": response.get("description"),
                    "status": response.get("status"),
                    "created_time": response.get("createdTime"),
                    "last_updated_time": response.get("lastUpdatedTime")
                }
            }
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                return {"success": False, "error": f"Detector '{detector_id}' not found"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def list_detectors(
        self,
        max_results: int = 50,
        tag_filters: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        List all fraud detectors.

        Args:
            max_results: Maximum number of results to return
            tag_filters: Optional tag filters

        Returns:
            Dictionary with list of detectors
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {"maxResults": max_results}

            response = self.client.list_detectors(**kwargs)
            detectors = []

            for det in response.get("detectors", []):
                detectors.append({
                    "id": det.get("detectorId"),
                    "arn": det.get("arn"),
                    "status": det.get("status"),
                    "created_time": det.get("createdTime"),
                    "last_updated_time": det.get("lastUpdatedTime")
                })

            return {
                "success": True,
                "detectors": detectors,
                "count": len(detectors)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def update_detector(
        self,
        detector_id: str,
        description: Optional[str] = None,
        status: Optional[DetectorStatus] = None
    ) -> Dict[str, Any]:
        """
        Update a fraud detector.

        Args:
            detector_id: The detector ID
            description: New description
            status: New status

        Returns:
            Dictionary with update result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {"detectorId": detector_id}
            if description is not None:
                kwargs["description"] = description
            if status:
                kwargs["status"] = status.value

            self.client.update_detector(**kwargs)

            return {
                "success": True,
                "detector_id": detector_id,
                "message": f"Detector '{detector_id}' updated successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def delete_detector(self, detector_id: str) -> Dict[str, Any]:
        """
        Delete a fraud detector.

        Args:
            detector_id: The detector ID

        Returns:
            Dictionary with deletion result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            self.client.delete_detector(detectorId=detector_id)
            return {
                "success": True,
                "detector_id": detector_id,
                "message": f"Detector '{detector_id}' deleted successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def create_detector_version(
        self,
        detector_id: str,
        rules: List[Dict[str, Any]],
        model_versions: Optional[List[Dict[str, str]]] = None,
        description: str = ""
    ) -> Dict[str, Any]:
        """
        Create a version of a detector.

        Args:
            detector_id: The detector ID
            rules: List of rule configurations
            model_versions: List of model version associations
            description: Version description

        Returns:
            Dictionary with creation result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {
                "detectorId": detector_id,
                "rules": rules
            }
            if model_versions:
                kwargs["modelVersions"] = model_versions
            if description:
                kwargs["description"] = description

            response = self.client.create_detector_version(**kwargs)

            return {
                "success": True,
                "detector_id": detector_id,
                "version": response.get("detectorVersion"),
                "arn": response.get("arn"),
                "status": response.get("status"),
                "message": f"Detector version created successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_detector_version(
        self,
        detector_id: str,
        detector_version: str
    ) -> Dict[str, Any]:
        """
        Get a specific version of a detector.

        Args:
            detector_id: The detector ID
            detector_version: The version number

        Returns:
            Dictionary with detector version details
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.get_detector_version(
                detectorId=detector_id,
                detectorVersion=detector_version
            )

            return {
                "success": True,
                "detector_version": {
                    "detector_id": response.get("detectorId"),
                    "version": response.get("detectorVersion"),
                    "arn": response.get("arn"),
                    "status": response.get("status"),
                    "rules": response.get("rules", []),
                    "model_versions": response.get("modelVersions", []),
                    "description": response.get("description"),
                    "created_time": response.get("createdTime")
                }
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def list_detector_versions(
        self,
        detector_id: str,
        max_results: int = 50
    ) -> Dict[str, Any]:
        """
        List all versions of a detector.

        Args:
            detector_id: The detector ID
            max_results: Maximum number of results

        Returns:
            Dictionary with list of versions
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.list_detector_versions(
                detectorId=detector_id,
                maxResults=max_results
            )

            versions = []
            for v in response.get("detectorVersionSummaries", []):
                versions.append({
                    "version": v.get("detectorVersion"),
                    "status": v.get("status"),
                    "description": v.get("description"),
                    "created_time": v.get("createdTime")
                })

            return {
                "success": True,
                "detector_id": detector_id,
                "versions": versions,
                "count": len(versions)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def update_detector_version(
        self,
        detector_id: str,
        detector_version: str,
        rules: Optional[List[Dict[str, Any]]] = None,
        model_versions: Optional[List[Dict[str, str]]] = None,
        status: Optional[DetectorStatus] = None
    ) -> Dict[str, Any]:
        """
        Update a detector version.

        Args:
            detector_id: The detector ID
            detector_version: The version to update
            rules: New rules configuration
            model_versions: New model version associations
            status: New status

        Returns:
            Dictionary with update result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {
                "detectorId": detector_id,
                "detectorVersion": detector_version
            }
            if rules:
                kwargs["rules"] = rules
            if model_versions:
                kwargs["modelVersions"] = model_versions
            if status:
                kwargs["status"] = status.value

            self.client.update_detector_version(**kwargs)

            return {
                "success": True,
                "message": f"Detector version {detector_version} updated successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================================
    # RULE MANAGEMENT
    # =========================================================================

    def create_rule(
        self,
        detector_id: str,
        rule_id: str,
        expression: str,
        language: str = "DETECTORPL",
        outcomes: List[str] = None,
        description: str = ""
    ) -> Dict[str, Any]:
        """
        Create a rule for a detector.

        Args:
            detector_id: The detector ID
            rule_id: Unique rule identifier
            expression: Rule expression (e.g., "$model_fraud_score > 800")
            language: Rule language (default: DETECTORPL)
            outcomes: List of outcome names to trigger
            description: Rule description

        Returns:
            Dictionary with creation result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        if outcomes is None:
            outcomes = []

        try:
            kwargs = {
                "detectorId": detector_id,
                "rule": {
                    "ruleId": rule_id,
                    "expression": expression,
                    "language": language,
                    "outcomes": outcomes
                }
            }
            if description:
                kwargs["description"] = description

            response = self.client.create_rule(**kwargs)

            return {
                "success": True,
                "rule_id": rule_id,
                "detector_id": detector_id,
                "arn": response.get("ruleArn"),
                "message": f"Rule '{rule_id}' created successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_rule(self, rule_id: str) -> Dict[str, Any]:
        """
        Get a rule by ID.

        Args:
            rule_id: The rule ID

        Returns:
            Dictionary with rule details
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.get_rule(ruleId=rule_id)

            return {
                "success": True,
                "rule": {
                    "rule_id": response.get("rule").get("ruleId"),
                    "expression": response.get("rule").get("expression"),
                    "language": response.get("rule").get("language"),
                    "outcomes": response.get("rule").get("outcomes"),
                    "description": response.get("description")
                }
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================================
    # MODEL MANAGEMENT
    # =========================================================================

    def create_model(
        self,
        model_id: str,
        model_type: ModelType,
        description: str = "",
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a new fraud detection model.

        Args:
            model_id: Unique identifier for the model
            model_type: Type of fraud detection model
            description: Model description
            tags: Resource tags

        Returns:
            Dictionary with creation result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {
                "modelId": model_id,
                "modelType": model_type.value,
                "description": description
            }

            response = self.client.create_model(**kwargs)

            if tags:
                arn = response.get("modelArn", "")
                self._apply_tags(arn, tags)

            return {
                "success": True,
                "model_id": model_id,
                "model_arn": response.get("modelArn"),
                "model_type": model_type.value,
                "message": f"Model '{model_id}' created successfully"
            }
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ConflictException":
                return {"success": False, "error": f"Model '{model_id}' already exists"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_model(self, model_id: str, model_type: ModelType) -> Dict[str, Any]:
        """
        Get details of a fraud detection model.

        Args:
            model_id: The model ID
            model_type: The model type

        Returns:
            Dictionary with model details
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.get_model(
                modelId=model_id,
                modelType=model_type.value
            )

            return {
                "success": True,
                "model": {
                    "id": response.get("modelId"),
                    "type": response.get("modelType"),
                    "arn": response.get("arn"),
                    "description": response.get("description"),
                    "created_time": response.get("createdTime"),
                    "last_updated_time": response.get("lastUpdatedTime")
                }
            }
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                return {"success": False, "error": f"Model '{model_id}' not found"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def list_models(self, max_results: int = 50) -> Dict[str, Any]:
        """
        List all fraud detection models.

        Args:
            max_results: Maximum number of results

        Returns:
            Dictionary with list of models
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.list_models(maxResults=max_results)
            models = []

            for m in response.get("models", []):
                models.append({
                    "id": m.get("modelId"),
                    "type": m.get("modelType"),
                    "arn": m.get("arn"),
                    "created_time": m.get("createdTime")
                })

            return {
                "success": True,
                "models": models,
                "count": len(models)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def delete_model(self, model_id: str, model_type: ModelType) -> Dict[str, Any]:
        """
        Delete a fraud detection model.

        Args:
            model_id: The model ID
            model_type: The model type

        Returns:
            Dictionary with deletion result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            self.client.delete_model(
                modelId=model_id,
                modelType=model_type.value
            )
            return {
                "success": True,
                "model_id": model_id,
                "message": f"Model '{model_id}' deleted successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def create_model_version(
        self,
        model_id: str,
        model_type: ModelType,
        training_data_schema: Dict[str, Any],
        training_data_source: Dict[str, Any],
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a new version of a model.

        Args:
            model_id: The model ID
            model_type: The model type
            training_data_schema: Schema for training data
            training_data_source: S3 location of training data
            tags: Resource tags

        Returns:
            Dictionary with creation result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {
                "modelId": model_id,
                "modelType": model_type.value,
                "trainingDataSchema": training_data_schema,
                "trainingDataSource": training_data_source
            }

            response = self.client.create_model_version(**kwargs)

            if tags:
                arn = response.get("modelArn", "")
                self._apply_tags(arn, tags)

            return {
                "success": True,
                "model_id": model_id,
                "model_type": model_type.value,
                "model_version": response.get("modelVersionNumber"),
                "status": response.get("status"),
                "message": f"Model version created successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_model_version(
        self,
        model_id: str,
        model_type: ModelType,
        model_version: str
    ) -> Dict[str, Any]:
        """
        Get a specific version of a model.

        Args:
            model_id: The model ID
            model_type: The model type
            model_version: The version number

        Returns:
            Dictionary with model version details
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.get_model_version(
                modelId=model_id,
                modelType=model_type.value,
                modelVersion=model_version
            )

            return {
                "success": True,
                "model_version": {
                    "model_id": response.get("modelId"),
                    "model_type": response.get("modelType"),
                    "version": response.get("modelVersionNumber"),
                    "status": response.get("status"),
                    "training_result": response.get("trainingResult", {}),
                    "arn": response.get("arn")
                }
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def list_model_versions(
        self,
        model_id: str,
        model_type: ModelType,
        max_results: int = 50
    ) -> Dict[str, Any]:
        """
        List all versions of a model.

        Args:
            model_id: The model ID
            model_type: The model type
            max_results: Maximum number of results

        Returns:
            Dictionary with list of versions
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.list_model_versions(
                modelId=model_id,
                modelType=model_type.value,
                maxResults=max_results
            )

            versions = []
            for v in response.get("modelVersionSummaries", []):
                versions.append({
                    "model_id": v.get("modelId"),
                    "model_type": v.get("modelType"),
                    "version": v.get("modelVersionNumber"),
                    "status": v.get("status"),
                    "created_time": v.get("createdTime")
                })

            return {
                "success": True,
                "model_id": model_id,
                "versions": versions,
                "count": len(versions)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def update_model(
        self,
        model_id: str,
        model_type: ModelType,
        description: Optional[str] = None,
        status: Optional[ModelStatus] = None
    ) -> Dict[str, Any]:
        """
        Update a fraud detection model.

        Args:
            model_id: The model ID
            model_type: The model type
            description: New description
            status: New status

        Returns:
            Dictionary with update result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {
                "modelId": model_id,
                "modelType": model_type.value
            }
            if description is not None:
                kwargs["description"] = description
            if status:
                kwargs["modelStatus"] = status.value

            self.client.update_model(**kwargs)

            return {
                "success": True,
                "model_id": model_id,
                "message": f"Model '{model_id}' updated successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def update_model_version(
        self,
        model_id: str,
        model_type: ModelType,
        model_version: str,
        status: ModelStatus
    ) -> Dict[str, Any]:
        """
        Update a model version status.

        Args:
            model_id: The model ID
            model_type: The model type
            model_version: The version to update
            status: New status

        Returns:
            Dictionary with update result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            self.client.update_model_version(
                modelId=model_id,
                modelType=model_type.value,
                modelVersion=model_version,
                modelStatus=status.value
            )

            return {
                "success": True,
                "message": f"Model version {model_version} status updated to {status.value}"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================================
    # ENTITY TYPE MANAGEMENT
    # =========================================================================

    def create_entity_type(
        self,
        name: str,
        description: str = "",
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a new entity type.

        Args:
            name: Entity type name
            description: Description
            tags: Resource tags

        Returns:
            Dictionary with creation result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {"name": name}
            if description:
                kwargs["description"] = description

            response = self.client.create_entity_type(**kwargs)

            if tags:
                arn = response.get("entityTypeArn", "")
                self._apply_tags(arn, tags)

            return {
                "success": True,
                "name": name,
                "arn": response.get("entityTypeArn"),
                "message": f"Entity type '{name}' created successfully"
            }
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ConflictException":
                return {"success": False, "error": f"Entity type '{name}' already exists"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_entity_type(self, name: str) -> Dict[str, Any]:
        """
        Get an entity type by name.

        Args:
            name: Entity type name

        Returns:
            Dictionary with entity type details
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.get_entity_type(name=name)

            return {
                "success": True,
                "entity_type": {
                    "name": response.get("name"),
                    "arn": response.get("arn"),
                    "description": response.get("description"),
                    "created_time": response.get("createdTime"),
                    "last_updated_time": response.get("lastUpdatedTime")
                }
            }
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                return {"success": False, "error": f"Entity type '{name}' not found"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def list_entity_types(
        self,
        max_results: int = 50
    ) -> Dict[str, Any]:
        """
        List all entity types.

        Args:
            max_results: Maximum number of results

        Returns:
            Dictionary with list of entity types
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.list_entity_types(maxResults=max_results)
            entity_types = []

            for et in response.get("entityTypes", []):
                entity_types.append({
                    "name": et.get("name"),
                    "arn": et.get("arn"),
                    "description": et.get("description"),
                    "created_time": et.get("createdTime")
                })

            return {
                "success": True,
                "entity_types": entity_types,
                "count": len(entity_types)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def update_entity_type(
        self,
        name: str,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update an entity type.

        Args:
            name: Entity type name
            description: New description

        Returns:
            Dictionary with update result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {"name": name}
            if description is not None:
                kwargs["description"] = description

            self.client.update_entity_type(**kwargs)

            return {
                "success": True,
                "name": name,
                "message": f"Entity type '{name}' updated successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def delete_entity_type(self, name: str) -> Dict[str, Any]:
        """
        Delete an entity type.

        Args:
            name: Entity type name

        Returns:
            Dictionary with deletion result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            self.client.delete_entity_type(name=name)
            return {
                "success": True,
                "name": name,
                "message": f"Entity type '{name}' deleted successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================================
    # EVENT TYPE MANAGEMENT
    # =========================================================================

    def create_event_type(
        self,
        name: str,
        entity_types: List[str],
        event_variables: List[str],
        labels: Optional[List[str]] = None,
        description: str = "",
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a new event type.

        Args:
            name: Event type name
            entity_types: List of entity type names
            event_variables: List of variable names
            labels: List of label names
            description: Description
            tags: Resource tags

        Returns:
            Dictionary with creation result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        if labels is None:
            labels = []

        try:
            kwargs = {
                "name": name,
                "entityTypes": entity_types,
                "eventVariables": event_variables
            }
            if labels:
                kwargs["labels"] = labels
            if description:
                kwargs["description"] = description

            response = self.client.create_event_type(**kwargs)

            if tags:
                arn = response.get("eventTypeArn", "")
                self._apply_tags(arn, tags)

            return {
                "success": True,
                "name": name,
                "arn": response.get("eventTypeArn"),
                "message": f"Event type '{name}' created successfully"
            }
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ConflictException":
                return {"success": False, "error": f"Event type '{name}' already exists"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_event_type(self, name: str) -> Dict[str, Any]:
        """
        Get an event type by name.

        Args:
            name: Event type name

        Returns:
            Dictionary with event type details
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.get_event_type(name=name)

            return {
                "success": True,
                "event_type": {
                    "name": response.get("name"),
                    "arn": response.get("arn"),
                    "entity_types": response.get("entityTypes"),
                    "event_variables": response.get("eventVariables"),
                    "labels": response.get("labels", []),
                    "description": response.get("description"),
                    "created_time": response.get("createdTime"),
                    "last_updated_time": response.get("lastUpdatedTime")
                }
            }
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                return {"success": False, "error": f"Event type '{name}' not found"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def list_event_types(
        self,
        max_results: int = 50
    ) -> Dict[str, Any]:
        """
        List all event types.

        Args:
            max_results: Maximum number of results

        Returns:
            Dictionary with list of event types
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.list_event_types(maxResults=max_results)
            event_types = []

            for et in response.get("eventTypes", []):
                event_types.append({
                    "name": et.get("name"),
                    "arn": et.get("arn"),
                    "entity_types": et.get("entityTypes"),
                    "event_variables": et.get("eventVariables"),
                    "labels": et.get("labels", []),
                    "created_time": et.get("createdTime")
                })

            return {
                "success": True,
                "event_types": event_types,
                "count": len(event_types)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def update_event_type(
        self,
        name: str,
        entity_types: Optional[List[str]] = None,
        event_variables: Optional[List[str]] = None,
        labels: Optional[List[str]] = None,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update an event type.

        Args:
            name: Event type name
            entity_types: New list of entity types
            event_variables: New list of variables
            labels: New list of labels
            description: New description

        Returns:
            Dictionary with update result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {"name": name}
            if entity_types is not None:
                kwargs["entityTypes"] = entity_types
            if event_variables is not None:
                kwargs["eventVariables"] = event_variables
            if labels is not None:
                kwargs["labels"] = labels
            if description is not None:
                kwargs["description"] = description

            self.client.update_event_type(**kwargs)

            return {
                "success": True,
                "name": name,
                "message": f"Event type '{name}' updated successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def delete_event_type(self, name: str) -> Dict[str, Any]:
        """
        Delete an event type.

        Args:
            name: Event type name

        Returns:
            Dictionary with deletion result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            self.client.delete_event_type(name=name)
            return {
                "success": True,
                "name": name,
                "message": f"Event type '{name}' deleted successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================================
    # LABEL MANAGEMENT
    # =========================================================================

    def create_label(
        self,
        name: str,
        description: str = "",
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a new fraud label.

        Args:
            name: Label name (e.g., "fraud", "legit")
            description: Description
            tags: Resource tags

        Returns:
            Dictionary with creation result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {"name": name}
            if description:
                kwargs["description"] = description

            response = self.client.create_label(**kwargs)

            if tags:
                arn = response.get("labelArn", "")
                self._apply_tags(arn, tags)

            return {
                "success": True,
                "name": name,
                "arn": response.get("labelArn"),
                "message": f"Label '{name}' created successfully"
            }
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ConflictException":
                return {"success": False, "error": f"Label '{name}' already exists"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_label(self, name: str) -> Dict[str, Any]:
        """
        Get a label by name.

        Args:
            name: Label name

        Returns:
            Dictionary with label details
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.get_label(name=name)

            return {
                "success": True,
                "label": {
                    "name": response.get("name"),
                    "arn": response.get("arn"),
                    "description": response.get("description"),
                    "created_time": response.get("createdTime"),
                    "last_updated_time": response.get("lastUpdatedTime")
                }
            }
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                return {"success": False, "error": f"Label '{name}' not found"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def list_labels(
        self,
        max_results: int = 50
    ) -> Dict[str, Any]:
        """
        List all labels.

        Args:
            max_results: Maximum number of results

        Returns:
            Dictionary with list of labels
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.list_labels(maxResults=max_results)
            labels = []

            for label in response.get("labels", []):
                labels.append({
                    "name": label.get("name"),
                    "arn": label.get("arn"),
                    "description": label.get("description"),
                    "created_time": label.get("createdTime")
                })

            return {
                "success": True,
                "labels": labels,
                "count": len(labels)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def update_label(
        self,
        name: str,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update a label.

        Args:
            name: Label name
            description: New description

        Returns:
            Dictionary with update result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {"name": name}
            if description is not None:
                kwargs["description"] = description

            self.client.update_label(**kwargs)

            return {
                "success": True,
                "name": name,
                "message": f"Label '{name}' updated successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def delete_label(self, name: str) -> Dict[str, Any]:
        """
        Delete a label.

        Args:
            name: Label name

        Returns:
            Dictionary with deletion result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            self.client.delete_label(name=name)
            return {
                "success": True,
                "name": name,
                "message": f"Label '{name}' deleted successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================================
    # VARIABLE MANAGEMENT
    # =========================================================================

    def create_variable(
        self,
        name: str,
        variable_type: VariableType,
        data_source: str = "EVENT",
        data_type: str = "STRING",
        default_value: Optional[str] = None,
        description: str = "",
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a new variable.

        Args:
            name: Variable name
            variable_type: Type of variable
            data_source: Data source (EVENT, MODEL_SCORE, etc.)
            data_type: Data type (STRING, INTEGER, FLOAT, BOOLEAN)
            default_value: Default value for the variable
            description: Description
            tags: Resource tags

        Returns:
            Dictionary with creation result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {
                "name": name,
                "variableType": variable_type.value,
                "dataSource": data_source,
                "dataType": data_type
            }
            if default_value is not None:
                kwargs["defaultValue"] = default_value
            if description:
                kwargs["description"] = description

            response = self.client.create_variable(**kwargs)

            if tags:
                arn = response.get("variableArn", "")
                self._apply_tags(arn, tags)

            return {
                "success": True,
                "name": name,
                "arn": response.get("variableArn"),
                "message": f"Variable '{name}' created successfully"
            }
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ConflictException":
                return {"success": False, "error": f"Variable '{name}' already exists"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_variable(self, name: str) -> Dict[str, Any]:
        """
        Get a variable by name.

        Args:
            name: Variable name

        Returns:
            Dictionary with variable details
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.get_variable(name=name)

            return {
                "success": True,
                "variable": {
                    "name": response.get("name"),
                    "arn": response.get("arn"),
                    "variable_type": response.get("variableType"),
                    "data_source": response.get("dataSource"),
                    "data_type": response.get("dataType"),
                    "default_value": response.get("defaultValue"),
                    "description": response.get("description"),
                    "created_time": response.get("createdTime"),
                    "last_updated_time": response.get("lastUpdatedTime")
                }
            }
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                return {"success": False, "error": f"Variable '{name}' not found"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def list_variables(
        self,
        max_results: int = 50,
        variable_filter: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List all variables.

        Args:
            max_results: Maximum number of results
            variable_filter: Filter by variable type

        Returns:
            Dictionary with list of variables
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {"maxResults": max_results}
            if variable_filter:
                kwargs["filter"] = variable_filter

            response = self.client.list_variables(**kwargs)
            variables = []

            for v in response.get("variables", []):
                variables.append({
                    "name": v.get("name"),
                    "arn": v.get("arn"),
                    "variable_type": v.get("variableType"),
                    "data_source": v.get("dataSource"),
                    "data_type": v.get("dataType"),
                    "default_value": v.get("defaultValue"),
                    "created_time": v.get("createdTime")
                })

            return {
                "success": True,
                "variables": variables,
                "count": len(variables)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def update_variable(
        self,
        name: str,
        variable_type: Optional[VariableType] = None,
        default_value: Optional[str] = None,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update a variable.

        Args:
            name: Variable name
            variable_type: New variable type
            default_value: New default value
            description: New description

        Returns:
            Dictionary with update result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {"name": name}
            if variable_type is not None:
                kwargs["variableType"] = variable_type.value
            if default_value is not None:
                kwargs["defaultValue"] = default_value
            if description is not None:
                kwargs["description"] = description

            self.client.update_variable(**kwargs)

            return {
                "success": True,
                "name": name,
                "message": f"Variable '{name}' updated successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def delete_variable(self, name: str) -> Dict[str, Any]:
        """
        Delete a variable.

        Args:
            name: Variable name

        Returns:
            Dictionary with deletion result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            self.client.delete_variable(name=name)
            return {
                "success": True,
                "name": name,
                "message": f"Variable '{name}' deleted successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================================
    # OUTCOME MANAGEMENT
    # =========================================================================

    def create_outcome(
        self,
        name: str,
        description: str = "",
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a new outcome.

        Args:
            name: Outcome name
            description: Description
            tags: Resource tags

        Returns:
            Dictionary with creation result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {"name": name}
            if description:
                kwargs["description"] = description

            response = self.client.create_outcome(**kwargs)

            if tags:
                arn = response.get("outcomeArn", "")
                self._apply_tags(arn, tags)

            return {
                "success": True,
                "name": name,
                "arn": response.get("outcomeArn"),
                "message": f"Outcome '{name}' created successfully"
            }
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ConflictException":
                return {"success": False, "error": f"Outcome '{name}' already exists"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_outcome(self, name: str) -> Dict[str, Any]:
        """
        Get an outcome by name.

        Args:
            name: Outcome name

        Returns:
            Dictionary with outcome details
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.get_outcome(name=name)

            return {
                "success": True,
                "outcome": {
                    "name": response.get("name"),
                    "arn": response.get("arn"),
                    "description": response.get("description"),
                    "created_time": response.get("createdTime"),
                    "last_updated_time": response.get("lastUpdatedTime")
                }
            }
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                return {"success": False, "error": f"Outcome '{name}' not found"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def list_outcomes(
        self,
        max_results: int = 50
    ) -> Dict[str, Any]:
        """
        List all outcomes.

        Args:
            max_results: Maximum number of results

        Returns:
            Dictionary with list of outcomes
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.list_outcomes(maxResults=max_results)
            outcomes = []

            for o in response.get("outcomes", []):
                outcomes.append({
                    "name": o.get("name"),
                    "arn": o.get("arn"),
                    "description": o.get("description"),
                    "created_time": o.get("createdTime")
                })

            return {
                "success": True,
                "outcomes": outcomes,
                "count": len(outcomes)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def update_outcome(
        self,
        name: str,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update an outcome.

        Args:
            name: Outcome name
            description: New description

        Returns:
            Dictionary with update result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            kwargs = {"name": name}
            if description is not None:
                kwargs["description"] = description

            self.client.update_outcome(**kwargs)

            return {
                "success": True,
                "name": name,
                "message": f"Outcome '{name}' updated successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def delete_outcome(self, name: str) -> Dict[str, Any]:
        """
        Delete an outcome.

        Args:
            name: Outcome name

        Returns:
            Dictionary with deletion result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            self.client.delete_outcome(name=name)
            return {
                "success": True,
                "name": name,
                "message": f"Outcome '{name}' deleted successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================================
    # PREDICTIONS
    # =========================================================================

    def predict(
        self,
        detector_id: str,
        detector_version: str,
        event_id: str,
        event_type_name: str,
        entity_id: str,
        entity_type: str,
        event_variables: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get a fraud prediction for an event.

        Args:
            detector_id: The detector ID
            detector_version: The detector version
            event_id: Unique event identifier
            event_type_name: The event type name
            entity_id: Entity identifier
            entity_type: Entity type name
            event_variables: Dictionary of event variables

        Returns:
            Dictionary with prediction results
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.predict(
                detectorId=detector_id,
                detectorVersion=detector_version,
                eventId=event_id,
                eventTypeName=event_type_name,
                entityId=entity_id,
                entityType=entity_type,
                eventVariables=event_variables
            )

            scores = response.get("scores", {})
            fraud_score = int(scores.get("fraud_score", 0))

            if fraud_score >= 750:
                fraud_level = FraudLevel.HIGH
            elif fraud_score >= 500:
                fraud_level = FraudLevel.MEDIUM
            elif fraud_score >= 250:
                fraud_level = FraudLevel.LOW
            else:
                fraud_level = FraudLevel.NONE

            return {
                "success": True,
                "prediction": PredictionResult(
                    model_version=response.get("modelVersion", ""),
                    detector_version=response.get("detectorVersion", ""),
                    fraud_score=fraud_score,
                    fraud_level=fraud_level,
                    predictions=scores,
                    model_id=response.get("modelId", ""),
                    detector_id=detector_id,
                    event_id=event_id,
                    event_type=event_type_name,
                    entity_id=entity_id,
                    entity_type=entity_type,
                    timestamp=datetime.now().isoformat()
                )
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_prediction(
        self,
        event_id: str
    ) -> Dict[str, Any]:
        """
        Get prediction details for a past event.

        Args:
            event_id: The event ID

        Returns:
            Dictionary with prediction details
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.get_prediction(eventId=event_id)

            return {
                "success": True,
                "prediction": {
                    "event_id": response.get("eventId"),
                    "event_type": response.get("eventTypeName"),
                    "detector_id": response.get("detectorId"),
                    "detector_version": response.get("detectorVersion"),
                    "model_id": response.get("modelId"),
                    "model_version": response.get("modelVersion"),
                    "scores": response.get("scores", {}),
                    "timestamp": response.get("eventTimestamp")
                }
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================================
    # BATCH PREDICTIONS
    # =========================================================================

    def create_batch_prediction(
        self,
        job_id: str,
        detector_name: str,
        detector_version: str,
        input_path: str,
        output_path: str,
        event_type_name: str
    ) -> Dict[str, Any]:
        """
        Create a batch prediction job.

        Args:
            job_id: Unique job identifier
            detector_name: Detector name to use
            detector_version: Detector version to use
            input_path: S3 path to input data
            output_path: S3 path for output
            event_type_name: Event type name

        Returns:
            Dictionary with creation result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.create_batch_prediction(
                batchPredictionId=job_id,
                detectorName=detector_name,
                detectorVersion=detector_version,
                inputPath=input_path,
                outputPath=output_path,
                eventTypeName=event_type_name
            )

            return {
                "success": True,
                "job_id": job_id,
                "arn": response.get("batchPredictionArn"),
                "status": response.get("status"),
                "message": f"Batch prediction job '{job_id}' created successfully"
            }
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ConflictException":
                return {"success": False, "error": f"Batch prediction job '{job_id}' already exists"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_batch_prediction(self, job_id: str) -> Dict[str, Any]:
        """
        Get batch prediction job details.

        Args:
            job_id: The job ID

        Returns:
            Dictionary with job details
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.get_batch_prediction(batchPredictionId=job_id)

            job = response.get("batchPrediction", {})
            status_str = job.get("status", "UNKNOWN")
            try:
                status = BatchPredictionStatus(status_str)
            except ValueError:
                status = BatchPredictionStatus.RUNNING

            return {
                "success": True,
                "job": BatchPredictionJob(
                    job_id=job.get("batchPredictionId"),
                    job_name=job.get("batchPredictionName"),
                    status=status,
                    input_path=job.get("inputPath"),
                    output_path=job.get("outputPath"),
                    detector_name=job.get("detectorName"),
                    detector_version=job.get("detectorVersion"),
                    start_time=job.get("startTime"),
                    completion_time=job.get("completionTime"),
                    total_records=job.get("totalRecords", 0),
                    processed_records=job.get("processedRecords", 0),
                    failed_records=job.get("failedRecords", 0),
                    error_message=job.get("errorMessage")
                )
            }
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                return {"success": False, "error": f"Batch prediction job '{job_id}' not found"}
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def list_batch_predictions(
        self,
        max_results: int = 50
    ) -> Dict[str, Any]:
        """
        List all batch prediction jobs.

        Args:
            max_results: Maximum number of results

        Returns:
            Dictionary with list of jobs
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            response = self.client.list_batch_predictions(maxResults=max_results)
            jobs = []

            for job in response.get("batchPredictions", []):
                status_str = job.get("status", "UNKNOWN")
                try:
                    status = BatchPredictionStatus(status_str)
                except ValueError:
                    status = BatchPredictionStatus.RUNNING

                jobs.append({
                    "job_id": job.get("batchPredictionId"),
                    "job_name": job.get("batchPredictionName"),
                    "status": status.value,
                    "input_path": job.get("inputPath"),
                    "output_path": job.get("outputPath"),
                    "created_time": job.get("createdTime")
                })

            return {
                "success": True,
                "jobs": jobs,
                "count": len(jobs)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def cancel_batch_prediction(self, job_id: str) -> Dict[str, Any]:
        """
        Cancel a batch prediction job.

        Args:
            job_id: The job ID

        Returns:
            Dictionary with cancellation result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            self.client.cancel_batch_prediction(batchPredictionId=job_id)
            return {
                "success": True,
                "job_id": job_id,
                "message": f"Batch prediction job '{job_id}' cancelled successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def delete_batch_prediction(self, job_id: str) -> Dict[str, Any]:
        """
        Delete a batch prediction job.

        Args:
            job_id: The job ID

        Returns:
            Dictionary with deletion result
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        try:
            self.client.delete_batch_prediction(batchPredictionId=job_id)
            return {
                "success": True,
                "job_id": job_id,
                "message": f"Batch prediction job '{job_id}' deleted successfully"
            }
        except ClientError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================================
    # CLOUDWATCH INTEGRATION
    # =========================================================================

    def put_fraud_metrics(
        self,
        fraud_score: int,
        fraud_level: FraudLevel,
        model_id: str,
        detector_id: str,
        event_type: str,
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Put fraud detection metrics to CloudWatch.

        Args:
            fraud_score: The fraud score (0-1000)
            fraud_level: The fraud level (HIGH, MEDIUM, LOW, NONE)
            model_id: The model ID
            detector_id: The detector ID
            event_type: The event type name
            timestamp: Optional timestamp (defaults to now)

        Returns:
            Dictionary with operation result
        """
        if not self.cloudwatch_client:
            return {"success": False, "error": "CloudWatch client not initialized"}

        if timestamp is None:
            timestamp = datetime.now()

        try:
            metrics = [
                {
                    "MetricName": "FraudScore",
                    "Dimensions": [
                        {"Name": "ModelId", "Value": model_id},
                        {"Name": "DetectorId", "Value": detector_id},
                        {"Name": "EventType", "Value": event_type}
                    ],
                    "Value": fraud_score,
                    "Unit": "None"
                },
                {
                    "MetricName": "FraudLevel",
                    "Dimensions": [
                        {"Name": "ModelId", "Value": model_id},
                        {"Name": "DetectorId", "Value": detector_id},
                        {"Name": "EventType", "Value": event_type},
                        {"Name": "Level", "Value": fraud_level.value}
                    ],
                    "Value": 1,
                    "Unit": "Count"
                }
            ]

            self.cloudwatch_client.put_metric_data(
                Namespace="AWS/FraudDetector",
                MetricData=metrics
            )

            return {
                "success": True,
                "message": "Metrics published successfully"
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_fraud_metrics(
        self,
        model_id: str,
        detector_id: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 3600
    ) -> Dict[str, Any]:
        """
        Get fraud detection metrics from CloudWatch.

        Args:
            model_id: The model ID
            detector_id: The detector ID
            start_time: Start of time range
            end_time: End of time range
            period: Metric period in seconds

        Returns:
            Dictionary with metric data
        """
        if not self.cloudwatch_client:
            return {"success": False, "error": "CloudWatch client not initialized"}

        try:
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace="AWS/FraudDetector",
                MetricName="FraudScore",
                Dimensions=[
                    {"Name": "ModelId", "Value": model_id},
                    {"Name": "DetectorId", "Value": detector_id}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=["Average", "Maximum", "Minimum", "SampleCount"]
            )

            return {
                "success": True,
                "metrics": response.get("Datapoints", []),
                "count": len(response.get("Datapoints", []))
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def create_fraud_dashboard(
        self,
        dashboard_name: str,
        model_id: str,
        detector_id: str
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch dashboard for fraud metrics.

        Args:
            dashboard_name: Name of the dashboard
            model_id: The model ID
            detector_id: The detector ID

        Returns:
            Dictionary with creation result
        """
        if not self.cloudwatch_client:
            return {"success": False, "error": "CloudWatch client not initialized"}

        try:
            dashboard_body = json.dumps({
                "widgets": [
                    {
                        "type": "metric",
                        "properties": {
                            "title": "Fraud Score Average",
                            "region": self.region_name,
                            "metrics": [
                                ["AWS/FraudDetector", "FraudScore", "ModelId", model_id, "DetectorId", detector_id]
                            ],
                            "period": 300,
                            "stat": "Average",
                            "yAxis": {
                                "left": {"min": 0, "max": 1000}
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "properties": {
                            "title": "Fraud Level Distribution",
                            "region": self.region_name,
                            "metrics": [
                                ["AWS/FraudDetector", "FraudLevel", "ModelId", model_id, "DetectorId", detector_id, "Level", "HIGH"],
                                [".", "FraudLevel", ".", ".", ".", ".", ".", "MEDIUM"],
                                [".", "FraudLevel", ".", ".", ".", ".", ".", "LOW"],
                                [".", "FraudLevel", ".", ".", ".", ".", ".", "NONE"]
                            ],
                            "period": 300,
                            "stat": "Sum"
                        }
                    }
                ]
            })

            self.cloudwatch_client.put_dashboard(
                DashboardName=dashboard_name,
                DashboardBody=dashboard_body
            )

            return {
                "success": True,
                "dashboard_name": dashboard_name,
                "message": f"Dashboard '{dashboard_name}' created successfully"
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def create_fraud_alarm(
        self,
        alarm_name: str,
        model_id: str,
        detector_id: str,
        threshold: int = 750,
        evaluation_periods: int = 1
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for high fraud scores.

        Args:
            alarm_name: Name of the alarm
            model_id: The model ID
            detector_id: The detector ID
            threshold: Fraud score threshold
            evaluation_periods: Number of evaluation periods

        Returns:
            Dictionary with creation result
        """
        if not self.cloudwatch_client:
            return {"success": False, "error": "CloudWatch client not initialized"}

        try:
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription=f"Alarm when fraud score exceeds {threshold}",
                Namespace="AWS/FraudDetector",
                MetricName="FraudScore",
                Dimensions=[
                    {"Name": "ModelId", "Value": model_id},
                    {"Name": "DetectorId", "Value": detector_id}
                ],
                Statistic="Average",
                Period=300,
                EvaluationPeriods=evaluation_periods,
                Threshold=threshold,
                ComparisonOperator="GreaterThanThreshold",
                TreatMissingData="notBreaching"
            )

            return {
                "success": True,
                "alarm_name": alarm_name,
                "message": f"Alarm '{alarm_name}' created successfully"
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def get_detector_summary(self) -> Dict[str, Any]:
        """
        Get a comprehensive summary of all Fraud Detector resources.

        Returns:
            Dictionary with summary of all resources
        """
        if not self.client:
            return {"success": False, "error": "Fraud Detector client not initialized"}

        summary = {
            "detectors": [],
            "models": [],
            "entity_types": [],
            "event_types": [],
            "labels": [],
            "variables": [],
            "outcomes": [],
            "batch_predictions": []
        }

        try:
            detectors = self.list_detectors()
            if detectors.get("success"):
                summary["detectors"] = detectors.get("detectors", [])

            models = self.list_models()
            if models.get("success"):
                summary["models"] = models.get("models", [])

            entity_types = self.list_entity_types()
            if entity_types.get("success"):
                summary["entity_types"] = entity_types.get("entity_types", [])

            event_types = self.list_event_types()
            if event_types.get("success"):
                summary["event_types"] = event_types.get("event_types", [])

            labels = self.list_labels()
            if labels.get("success"):
                summary["labels"] = labels.get("labels", [])

            variables = self.list_variables()
            if variables.get("success"):
                summary["variables"] = variables.get("variables", [])

            outcomes = self.list_outcomes()
            if outcomes.get("success"):
                summary["outcomes"] = outcomes.get("outcomes", [])

            batch_predictions = self.list_batch_predictions()
            if batch_predictions.get("success"):
                summary["batch_predictions"] = batch_predictions.get("jobs", [])

            return {
                "success": True,
                "summary": summary,
                "total_resources": sum(len(v) for v in summary.values())
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the Fraud Detector integration.

        Returns:
            Dictionary with health status
        """
        health = {
            "fraud_detector_client": False,
            "cloudwatch_client": False,
            "boto3_available": BOTO3_AVAILABLE,
            "region": self.region_name
        }

        if self.client:
            try:
                self.client.list_detectors(maxResults=1)
                health["fraud_detector_client"] = True
            except Exception:
                pass

        if self.cloudwatch_client:
            try:
                self.cloudwatch_client.list_dashboards(Limit=1)
                health["cloudwatch_client"] = True
            except Exception:
                pass

        all_healthy = all([health["fraud_detector_client"], health["cloudwatch_client"]])

        return {
            "success": all_healthy,
            "health": health
        }

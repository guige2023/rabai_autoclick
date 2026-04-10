"""
Tests for workflow_aws_frauddetector module

Comprehensive tests for FraudDetectorIntegration class covering:
- Detector management
- Detector version management
- Rule management
- Model management
- Model version management
- Entity type management
- Event type management
- Label management
- Variable management
- Outcome management
- Prediction (get_prediction)
- Batch prediction
- CloudWatch integration
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

# Create mock boto3 module before importing workflow module
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions

# Import the module
import src.workflow_aws_frauddetector as frauddetector_module

FraudDetectorIntegration = frauddetector_module.FraudDetectorIntegration
DetectorStatus = frauddetector_module.DetectorStatus
ModelStatus = frauddetector_module.ModelStatus
ModelType = frauddetector_module.ModelType
EntityTypeStatus = frauddetector_module.EntityTypeStatus
EventTypeStatus = frauddetector_module.EventTypeStatus
LabelStatus = frauddetector_module.LabelStatus
OutcomeStatus = frauddetector_module.OutcomeStatus
VariableType = frauddetector_module.VariableType
BatchPredictionStatus = frauddetector_module.BatchPredictionStatus
FraudLevel = frauddetector_module.FraudLevel
DetectorConfig = frauddetector_module.DetectorConfig
ModelConfig = frauddetector_module.ModelConfig
EntityTypeConfig = frauddetector_module.EntityTypeConfig
EventTypeConfig = frauddetector_module.EventTypeConfig
LabelConfig = frauddetector_module.LabelConfig
VariableConfig = frauddetector_module.VariableConfig
OutcomeConfig = frauddetector_module.OutcomeConfig
PredictionResult = frauddetector_module.PredictionResult
BatchPredictionJob = frauddetector_module.BatchPredictionJob


class TestDetectorStatus(unittest.TestCase):
    """Test DetectorStatus enum"""

    def test_status_values(self):
        self.assertEqual(DetectorStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(DetectorStatus.INACTIVE.value, "INACTIVE")
        self.assertEqual(DetectorStatus.CREATING.value, "CREATING")
        self.assertEqual(DetectorStatus.DELETING.value, "DELETING")


class TestModelStatus(unittest.TestCase):
    """Test ModelStatus enum"""

    def test_status_values(self):
        self.assertEqual(ModelStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(ModelStatus.INACTIVE.value, "INACTIVE")
        self.assertEqual(ModelStatus.TRAINING.value, "TRAINING")
        self.assertEqual(ModelStatus.IMPORTING.value, "IMPORTING")
        self.assertEqual(ModelStatus.DELETED.value, "DELETED")


class TestModelType(unittest.TestCase):
    """Test ModelType enum"""

    def test_type_values(self):
        self.assertEqual(ModelType.ONLINE_FRAUD_INSIGHTS.value, "ONLINE_FRAUD_INSIGHTS")
        self.assertEqual(ModelType.TRANSACTION_FRAUD_INSIGHTS.value, "TRANSACTION_FRAUD_INSIGHTS")
        self.assertEqual(ModelType.ACCOUNT_TAKEOVER_INSIGHTS.value, "ACCOUNT_TAKEOVER_INSIGHTS")


class TestEntityTypeStatus(unittest.TestCase):
    """Test EntityTypeStatus enum"""

    def test_status_values(self):
        self.assertEqual(EntityTypeStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(EntityTypeStatus.INACTIVE.value, "INACTIVE")


class TestEventTypeStatus(unittest.TestCase):
    """Test EventTypeStatus enum"""

    def test_status_values(self):
        self.assertEqual(EventTypeStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(EventTypeStatus.INACTIVE.value, "INACTIVE")


class TestLabelStatus(unittest.TestCase):
    """Test LabelStatus enum"""

    def test_status_values(self):
        self.assertEqual(LabelStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(LabelStatus.INACTIVE.value, "INACTIVE")


class TestOutcomeStatus(unittest.TestCase):
    """Test OutcomeStatus enum"""

    def test_status_values(self):
        self.assertEqual(OutcomeStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(OutcomeStatus.INACTIVE.value, "INACTIVE")


class TestVariableType(unittest.TestCase):
    """Test VariableType enum"""

    def test_type_values(self):
        self.assertEqual(VariableType.INTEGER.value, "INTEGER")
        self.assertEqual(VariableType.FLOAT.value, "FLOAT")
        self.assertEqual(VariableType.STRING.value, "STRING")
        self.assertEqual(VariableType.BOOLEAN.value, "BOOLEAN")
        self.assertEqual(VariableType.DATETIME.value, "DATETIME")


class TestBatchPredictionStatus(unittest.TestCase):
    """Test BatchPredictionStatus enum"""

    def test_status_values(self):
        self.assertEqual(BatchPredictionStatus.RUNNING.value, "RUNNING")
        self.assertEqual(BatchPredictionStatus.COMPLETED.value, "COMPLETED")
        self.assertEqual(BatchPredictionStatus.FAILED.value, "FAILED")
        self.assertEqual(BatchPredictionStatus.CANCELLED.value, "CANCELLED")


class TestFraudLevel(unittest.TestCase):
    """Test FraudLevel enum"""

    def test_level_values(self):
        self.assertEqual(FraudLevel.HIGH.value, "HIGH")
        self.assertEqual(FraudLevel.MEDIUM.value, "MEDIUM")
        self.assertEqual(FraudLevel.LOW.value, "LOW")
        self.assertEqual(FraudLevel.NONE.value, "NONE")


class TestDetectorConfig(unittest.TestCase):
    """Test DetectorConfig dataclass"""

    def test_detector_config_creation(self):
        config = DetectorConfig(
            detector_id="test-detector",
            description="Test fraud detector",
            status=DetectorStatus.ACTIVE
        )
        self.assertEqual(config.detector_id, "test-detector")
        self.assertEqual(config.description, "Test fraud detector")
        self.assertEqual(config.status, DetectorStatus.ACTIVE)


class TestModelConfig(unittest.TestCase):
    """Test ModelConfig dataclass"""

    def test_model_config_creation(self):
        config = ModelConfig(
            model_id="test-model",
            model_type=ModelType.ONLINE_FRAUD_INSIGHTS,
            description="Test model"
        )
        self.assertEqual(config.model_id, "test-model")
        self.assertEqual(config.model_type, ModelType.ONLINE_FRAUD_INSIGHTS)


class TestEntityTypeConfig(unittest.TestCase):
    """Test EntityTypeConfig dataclass"""

    def test_entity_type_config_creation(self):
        config = EntityTypeConfig(
            name="test-entity",
            description="Test entity type",
            status=EntityTypeStatus.ACTIVE
        )
        self.assertEqual(config.name, "test-entity")
        self.assertEqual(config.status, EntityTypeStatus.ACTIVE)


class TestEventTypeConfig(unittest.TestCase):
    """Test EventTypeConfig dataclass"""

    def test_event_type_config_creation(self):
        config = EventTypeConfig(
            name="test-event",
            entity_types=["user"],
            variables=["amount", "ip_address"],
            labels=["fraud", "legit"]
        )
        self.assertEqual(config.name, "test-event")
        self.assertEqual(len(config.entity_types), 1)


class TestLabelConfig(unittest.TestCase):
    """Test LabelConfig dataclass"""

    def test_label_config_creation(self):
        config = LabelConfig(
            name="fraud",
            description="Fraud label"
        )
        self.assertEqual(config.name, "fraud")
        self.assertEqual(config.status, LabelStatus.ACTIVE)


class TestVariableConfig(unittest.TestCase):
    """Test VariableConfig dataclass"""

    def test_variable_config_creation(self):
        config = VariableConfig(
            name="ip_address",
            variable_type=VariableType.STRING,
            data_type="STRING"
        )
        self.assertEqual(config.name, "ip_address")
        self.assertEqual(config.variable_type, VariableType.STRING)


class TestOutcomeConfig(unittest.TestCase):
    """Test OutcomeConfig dataclass"""

    def test_outcome_config_creation(self):
        config = OutcomeConfig(
            name="block",
            description="Block transaction"
        )
        self.assertEqual(config.name, "block")
        self.assertEqual(config.status, OutcomeStatus.ACTIVE)


class TestPredictionResult(unittest.TestCase):
    """Test PredictionResult dataclass"""

    def test_prediction_result_creation(self):
        result = PredictionResult(
            model_version="1",
            detector_version="1",
            fraud_score=850,
            fraud_level=FraudLevel.HIGH,
            predictions={"score": 0.85},
            model_id="test-model",
            detector_id="test-detector",
            event_id="event-123",
            event_type="test-event",
            entity_id="entity-456",
            entity_type="user",
            timestamp="2024-01-01T00:00:00Z"
        )
        self.assertEqual(result.fraud_score, 850)
        self.assertEqual(result.fraud_level, FraudLevel.HIGH)


class TestBatchPredictionJob(unittest.TestCase):
    """Test BatchPredictionJob dataclass"""

    def test_batch_prediction_job_creation(self):
        job = BatchPredictionJob(
            job_id="job-123",
            job_name="test-batch-job",
            status=BatchPredictionStatus.RUNNING,
            input_path="s3://bucket/input",
            output_path="s3://bucket/output",
            detector_name="test-detector",
            detector_version="1"
        )
        self.assertEqual(job.job_id, "job-123")
        self.assertEqual(job.status, BatchPredictionStatus.RUNNING)


class TestFraudDetectorIntegration(unittest.TestCase):
    """Test FraudDetectorIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_frauddetector_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        
        # Patch boto3.Session and clients
        self.boto3_session_patcher = patch('boto3.Session')
        self.mock_session_class = self.boto3_session_patcher.start()
        self.mock_session = MagicMock()
        self.mock_session_class.return_value = self.mock_session
        self.mock_session.client.side_effect = [self.mock_frauddetector_client, self.mock_cloudwatch_client]
        
        # Create integration instance
        self.integration = FraudDetectorIntegration(region_name="us-east-1")

    def tearDown(self):
        self.boto3_session_patcher.stop()

    def test_init_with_region(self):
        """Test initialization with region"""
        integration = FraudDetectorIntegration(region_name="us-west-2")
        self.assertEqual(integration.region_name, "us-west-2")

    def test_init_with_profile(self):
        """Test initialization with profile"""
        integration = FraudDetectorIntegration(region_name="us-east-1", profile_name="myprofile")
        self.assertEqual(integration.profile_name, "myprofile")


class TestDetectorManagement(unittest.TestCase):
    """Test detector management methods"""

    def setUp(self):
        self.mock_frauddetector_client = MagicMock()
        self.boto3_session_patcher = patch('boto3.Session')
        self.mock_session_class = self.boto3_session_patcher.start()
        self.mock_session = MagicMock()
        self.mock_session_class.return_value = self.mock_session
        self.mock_session.client.side_effect = [self.mock_frauddetector_client, MagicMock()]
        self.integration = FraudDetectorIntegration()

    def tearDown(self):
        self.boto3_session_patcher.stop()

    def test_create_detector_success(self):
        """Test successful detector creation"""
        self.mock_frauddetector_client.create_detector.return_value = {
            "detectorArn": "arn:aws:frauddetector:us-east-1:123456789012:detector/test-detector"
        }
        
        result = self.integration.create_detector(
            detector_id="test-detector",
            description="Test detector"
        )
        
        self.assertTrue(result["success"])
        self.assertEqual(result["detector_id"], "test-detector")

    def test_create_detector_conflict(self):
        """Test detector creation with conflict"""
        error = Exception("Conflict")
        error.response = {"Error": {"Code": "ConflictException"}}
        self.mock_frauddetector_client.create_detector.side_effect = error
        
        result = self.integration.create_detector(detector_id="existing-detector")
        
        self.assertFalse(result["success"])
        self.assertIn("already exists", result["error"])

    def test_create_detector_client_error(self):
        """Test detector creation with client error"""
        self.mock_frauddetector_client.create_detector.side_effect = Exception("Client error")
        
        result = self.integration.create_detector(detector_id="test-detector")
        
        self.assertFalse(result["success"])

    def test_get_detector_success(self):
        """Test successful detector retrieval"""
        self.mock_frauddetector_client.get_detector.return_value = {
            "detectorId": "test-detector",
            "arn": "arn:aws:frauddetector:us-east-1:123456789012:detector/test-detector",
            "status": "ACTIVE"
        }
        
        result = self.integration.get_detector("test-detector")
        
        self.assertTrue(result["success"])
        self.assertEqual(result["detector"]["id"], "test-detector")

    def test_get_detector_not_found(self):
        """Test detector not found"""
        error = Exception("Not found")
        error.response = {"Error": {"Code": "ResourceNotFoundException"}}
        self.mock_frauddetector_client.get_detector.side_effect = error
        
        result = self.integration.get_detector("nonexistent")
        
        self.assertFalse(result["success"])

    def test_list_detectors_success(self):
        """Test listing detectors"""
        self.mock_frauddetector_client.list_detectors.return_value = {
            "detectors": [
                {"detectorId": "detector1", "status": "ACTIVE"},
                {"detectorId": "detector2", "status": "INACTIVE"}
            ]
        }
        
        result = self.integration.list_detectors()
        
        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 2)

    def test_update_detector_success(self):
        """Test successful detector update"""
        self.mock_frauddetector_client.update_detector.return_value = {}
        
        result = self.integration.update_detector(
            detector_id="test-detector",
            description="Updated description"
        )
        
        self.assertTrue(result["success"])

    def test_delete_detector_success(self):
        """Test successful detector deletion"""
        self.mock_frauddetector_client.delete_detector.return_value = {}
        
        result = self.integration.delete_detector("test-detector")
        
        self.assertTrue(result["success"])
        self.assertEqual(result["detector_id"], "test-detector")


class TestDetectorVersionManagement(unittest.TestCase):
    """Test detector version management methods"""

    def setUp(self):
        self.mock_frauddetector_client = MagicMock()
        self.boto3_session_patcher = patch('boto3.Session')
        self.mock_session_class = self.boto3_session_patcher.start()
        self.mock_session = MagicMock()
        self.mock_session_class.return_value = self.mock_session
        self.mock_session.client.side_effect = [self.mock_frauddetector_client, MagicMock()]
        self.integration = FraudDetectorIntegration()

    def tearDown(self):
        self.boto3_session_patcher.stop()

    def test_create_detector_version_success(self):
        """Test successful detector version creation"""
        self.mock_frauddetector_client.create_detector_version.return_value = {
            "detectorVersion": "1",
            "arn": "arn:aws:frauddetector:us-east-1:123456789012:detector/test/1",
            "status": "ACTIVE"
        }
        
        result = self.integration.create_detector_version(
            detector_id="test-detector",
            rules=[{"ruleId": "rule1", "expression": "$event.value > 100"}]
        )
        
        self.assertTrue(result["success"])
        self.assertEqual(result["version"], "1")

    def test_get_detector_version_success(self):
        """Test successful detector version retrieval"""
        self.mock_frauddetector_client.get_detector_version.return_value = {
            "detectorId": "test-detector",
            "detectorVersion": "1",
            "status": "ACTIVE",
            "rules": [],
            "modelVersions": []
        }
        
        result = self.integration.get_detector_version("test-detector", "1")
        
        self.assertTrue(result["success"])

    def test_list_detector_versions_success(self):
        """Test listing detector versions"""
        self.mock_frauddetector_client.list_detector_versions.return_value = {
            "detectorVersionSummaries": [
                {"detectorVersion": "1", "status": "ACTIVE"},
                {"detectorVersion": "2", "status": "DRAFT"}
            ]
        }
        
        result = self.integration.list_detector_versions("test-detector")
        
        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 2)

    def test_update_detector_version_success(self):
        """Test successful detector version update"""
        self.mock_frauddetector_client.update_detector_version.return_value = {}
        
        result = self.integration.update_detector_version(
            detector_id="test-detector",
            detector_version="1",
            status=DetectorStatus.ACTIVE
        )
        
        self.assertTrue(result["success"])


class TestRuleManagement(unittest.TestCase):
    """Test rule management methods"""

    def setUp(self):
        self.mock_frauddetector_client = MagicMock()
        self.boto3_session_patcher = patch('boto3.Session')
        self.mock_session_class = self.boto3_session_patcher.start()
        self.mock_session = MagicMock()
        self.mock_session_class.return_value = self.mock_session
        self.mock_session.client.side_effect = [self.mock_frauddetector_client, MagicMock()]
        self.integration = FraudDetectorIntegration()

    def tearDown(self):
        self.boto3_session_patcher.stop()

    def test_create_rule_success(self):
        """Test successful rule creation"""
        self.mock_frauddetector_client.create_rule.return_value = {
            "ruleArn": "arn:aws:frauddetector:us-east-1:123456789012:rule/test-detector/rule1"
        }
        
        result = self.integration.create_rule(
            detector_id="test-detector",
            rule_id="rule1",
            expression="$event.value > 100",
            outcomes=["block"]
        )
        
        self.assertTrue(result["success"])
        self.assertEqual(result["rule_id"], "rule1")

    def test_get_rule_success(self):
        """Test successful rule retrieval"""
        self.mock_frauddetector_client.get_rule.return_value = {
            "rule": {
                "ruleId": "rule1",
                "expression": "$event.value > 100",
                "language": "DETECTORPL",
                "outcomes": ["block"]
            }
        }
        
        result = self.integration.get_rule("rule1")
        
        self.assertTrue(result["success"])


class TestModelManagement(unittest.TestCase):
    """Test model management methods"""

    def setUp(self):
        self.mock_frauddetector_client = MagicMock()
        self.boto3_session_patcher = patch('boto3.Session')
        self.mock_session_class = self.boto3_session_patcher.start()
        self.mock_session = MagicMock()
        self.mock_session_class.return_value = self.mock_session
        self.mock_session.client.side_effect = [self.mock_frauddetector_client, MagicMock()]
        self.integration = FraudDetectorIntegration()

    def tearDown(self):
        self.boto3_session_patcher.stop()

    def test_create_model_success(self):
        """Test successful model creation"""
        self.mock_frauddetector_client.create_model.return_value = {
            "modelArn": "arn:aws:frauddetector:us-east-1:123456789012:model/test-model"
        }
        
        result = self.integration.create_model(
            model_id="test-model",
            model_type=ModelType.ONLINE_FRAUD_INSIGHTS,
            description="Test model"
        )
        
        self.assertTrue(result["success"])
        self.assertEqual(result["model_id"], "test-model")

    def test_create_model_conflict(self):
        """Test model creation with conflict"""
        error = Exception("Conflict")
        error.response = {"Error": {"Code": "ConflictException"}}
        self.mock_frauddetector_client.create_model.side_effect = error
        
        result = self.integration.create_model(
            model_id="existing-model",
            model_type=ModelType.ONLINE_FRAUD_INSIGHTS
        )
        
        self.assertFalse(result["success"])
        self.assertIn("already exists", result["error"])

    def test_get_model_success(self):
        """Test successful model retrieval"""
        self.mock_frauddetector_client.get_model.return_value = {
            "modelId": "test-model",
            "modelType": "ONLINE_FRAUD_INSIGHTS",
            "arn": "arn:aws:frauddetector:us-east-1:123456789012:model/test-model"
        }
        
        result = self.integration.get_model("test-model", ModelType.ONLINE_FRAUD_INSIGHTS)
        
        self.assertTrue(result["success"])

    def test_get_model_not_found(self):
        """Test model not found"""
        error = Exception("Not found")
        error.response = {"Error": {"Code": "ResourceNotFoundException"}}
        self.mock_frauddetector_client.get_model.side_effect = error
        
        result = self.integration.get_model("nonexistent", ModelType.ONLINE_FRAUD_INSIGHTS)
        
        self.assertFalse(result["success"])

    def test_list_models_success(self):
        """Test listing models"""
        self.mock_frauddetector_client.list_models.return_value = {
            "models": [
                {"modelId": "model1", "modelType": "ONLINE_FRAUD_INSIGHTS"},
                {"modelId": "model2", "modelType": "TRANSACTION_FRAUD_INSIGHTS"}
            ]
        }
        
        result = self.integration.list_models()
        
        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 2)

    def test_delete_model_success(self):
        """Test successful model deletion"""
        self.mock_frauddetector_client.delete_model.return_value = {}
        
        result = self.integration.delete_model("test-model", ModelType.ONLINE_FRAUD_INSIGHTS)
        
        self.assertTrue(result["success"])


class TestModelVersionManagement(unittest.TestCase):
    """Test model version management methods"""

    def setUp(self):
        self.mock_frauddetector_client = MagicMock()
        self.boto3_session_patcher = patch('boto3.Session')
        self.mock_session_class = self.boto3_session_patcher.start()
        self.mock_session = MagicMock()
        self.mock_session_class.return_value = self.mock_session
        self.mock_session.client.side_effect = [self.mock_frauddetector_client, MagicMock()]
        self.integration = FraudDetectorIntegration()

    def tearDown(self):
        self.boto3_session_patcher.stop()

    def test_create_model_version_success(self):
        """Test successful model version creation"""
        self.mock_frauddetector_client.create_model_version.return_value = {
            "modelVersionNumber": "1",
            "modelArn": "arn:aws:frauddetector:us-east-1:123456789012:model/test-model/1",
            "status": "TRAINING"
        }
        
        result = self.integration.create_model_version(
            model_id="test-model",
            model_type=ModelType.ONLINE_FRAUD_INSIGHTS,
            training_data_schema={"test": "schema"},
            training_data_source={"test": "source"}
        )
        
        self.assertTrue(result["success"])
        self.assertEqual(result["model_version"], "1")

    def test_get_model_version_success(self):
        """Test successful model version retrieval"""
        self.mock_frauddetector_client.get_model_version.return_value = {
            "modelId": "test-model",
            "modelType": "ONLINE_FRAUD_INSIGHTS",
            "modelVersionNumber": "1",
            "status": "ACTIVE"
        }
        
        result = self.integration.get_model_version("test-model", ModelType.ONLINE_FRAUD_INSIGHTS, "1")
        
        self.assertTrue(result["success"])

    def test_list_model_versions_success(self):
        """Test listing model versions"""
        self.mock_frauddetector_client.list_model_versions.return_value = {
            "modelVersionSummaries": [
                {"modelVersionNumber": "1", "status": "ACTIVE"},
                {"modelVersionNumber": "2", "status": "TRAINING"}
            ]
        }
        
        result = self.integration.list_model_versions("test-model", ModelType.ONLINE_FRAUD_INSIGHTS)
        
        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 2)


class TestEntityTypeManagement(unittest.TestCase):
    """Test entity type management methods"""

    def setUp(self):
        self.mock_frauddetector_client = MagicMock()
        self.boto3_session_patcher = patch('boto3.Session')
        self.mock_session_class = self.boto3_session_patcher.start()
        self.mock_session = MagicMock()
        self.mock_session_class.return_value = self.mock_session
        self.mock_session.client.side_effect = [self.mock_frauddetector_client, MagicMock()]
        self.integration = FraudDetectorIntegration()

    def tearDown(self):
        self.boto3_session_patcher.stop()

    def test_create_entity_type_success(self):
        """Test successful entity type creation"""
        self.mock_frauddetector_client.create_entity_type.return_value = {
            "entityTypeArn": "arn:aws:frauddetector:us-east-1:123456789012:entity-type/user"
        }
        
        result = self.integration.create_entity_type(
            name="user",
            description="User entity type"
        )
        
        self.assertTrue(result["success"])
        self.assertEqual(result["name"], "user")

    def test_get_entity_type_success(self):
        """Test successful entity type retrieval"""
        self.mock_frauddetector_client.get_entity_type.return_value = {
            "name": "user",
            "description": "User entity",
            "status": "ACTIVE"
        }
        
        result = self.integration.get_entity_type("user")
        
        self.assertTrue(result["success"])

    def test_list_entity_types_success(self):
        """Test listing entity types"""
        self.mock_frauddetector_client.list_entity_types.return_value = {
            "entityTypes": [
                {"name": "user", "status": "ACTIVE"},
                {"name": "device", "status": "ACTIVE"}
            ]
        }
        
        result = self.integration.list_entity_types()
        
        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 2)

    def test_update_entity_type_success(self):
        """Test successful entity type update"""
        self.mock_frauddetector_client.update_entity_type.return_value = {}
        
        result = self.integration.update_entity_type(
            name="user",
            description="Updated user entity"
        )
        
        self.assertTrue(result["success"])

    def test_delete_entity_type_success(self):
        """Test successful entity type deletion"""
        self.mock_frauddetector_client.delete_entity_type.return_value = {}
        
        result = self.integration.delete_entity_type("user")
        
        self.assertTrue(result["success"])


class TestEventTypeManagement(unittest.TestCase):
    """Test event type management methods"""

    def setUp(self):
        self.mock_frauddetector_client = MagicMock()
        self.boto3_session_patcher = patch('boto3.Session')
        self.mock_session_class = self.boto3_session_patcher.start()
        self.mock_session = MagicMock()
        self.mock_session_class.return_value = self.mock_session
        self.mock_session.client.side_effect = [self.mock_frauddetector_client, MagicMock()]
        self.integration = FraudDetectorIntegration()

    def tearDown(self):
        self.boto3_session_patcher.stop()

    def test_create_event_type_success(self):
        """Test successful event type creation"""
        self.mock_frauddetector_client.create_event.return_value = {
            "eventTypeArn": "arn:aws:frauddetector:us-east-1:123456789012:event-type/test-event"
        }
        
        result = self.integration.create_event_type(
            name="test-event",
            entity_types=["user"],
            variables=["amount"],
            labels=["fraud", "legit"]
        )
        
        self.assertTrue(result["success"])

    def test_get_event_type_success(self):
        """Test successful event type retrieval"""
        self.mock_frauddetector_client.get_event.return_value = {
            "name": "test-event",
            "entityTypes": ["user"],
            "status": "ACTIVE"
        }
        
        result = self.integration.get_event_type("test-event")
        
        self.assertTrue(result["success"])

    def test_list_event_types_success(self):
        """Test listing event types"""
        self.mock_frauddetector_client.list_events.return_value = {
            "eventTypes": [{"name": "event1"}, {"name": "event2"}]
        }
        
        result = self.integration.list_event_types()
        
        self.assertTrue(result["success"])

    def test_delete_event_type_success(self):
        """Test successful event type deletion"""
        self.mock_frauddetector_client.delete_event.return_value = {}
        
        result = self.integration.delete_event_type("test-event")
        
        self.assertTrue(result["success"])


class TestLabelManagement(unittest.TestCase):
    """Test label management methods"""

    def setUp(self):
        self.mock_frauddetector_client = MagicMock()
        self.boto3_session_patcher = patch('boto3.Session')
        self.mock_session_class = self.boto3_session_patcher.start()
        self.mock_session = MagicMock()
        self.mock_session_class.return_value = self.mock_session
        self.mock_session.client.side_effect = [self.mock_frauddetector_client, MagicMock()]
        self.integration = FraudDetectorIntegration()

    def tearDown(self):
        self.boto3_session_patcher.stop()

    def test_create_label_success(self):
        """Test successful label creation"""
        self.mock_frauddetector_client.create_label.return_value = {
            "labelArn": "arn:aws:frauddetector:us-east-1:123456789012:label/fraud"
        }
        
        result = self.integration.create_label(
            name="fraud",
            description="Fraud label"
        )
        
        self.assertTrue(result["success"])

    def test_get_label_success(self):
        """Test successful label retrieval"""
        self.mock_frauddetector_client.get_label.return_value = {
            "name": "fraud",
            "description": "Fraudulent transaction",
            "status": "ACTIVE"
        }
        
        result = self.integration.get_label("fraud")
        
        self.assertTrue(result["success"])

    def test_list_labels_success(self):
        """Test listing labels"""
        self.mock_frauddetector_client.list_labels.return_value = {
            "labels": [
                {"name": "fraud", "status": "ACTIVE"},
                {"name": "legit", "status": "ACTIVE"}
            ]
        }
        
        result = self.integration.list_labels()
        
        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 2)

    def test_update_label_success(self):
        """Test successful label update"""
        self.mock_frauddetector_client.update_label.return_value = {}
        
        result = self.integration.update_label(
            name="fraud",
            description="Updated fraud label"
        )
        
        self.assertTrue(result["success"])

    def test_delete_label_success(self):
        """Test successful label deletion"""
        self.mock_frauddetector_client.delete_label.return_value = {}
        
        result = self.integration.delete_label("fraud")
        
        self.assertTrue(result["success"])


class TestVariableManagement(unittest.TestCase):
    """Test variable management methods"""

    def setUp(self):
        self.mock_frauddetector_client = MagicMock()
        self.boto3_session_patcher = patch('boto3.Session')
        self.mock_session_class = self.boto3_session_patcher.start()
        self.mock_session = MagicMock()
        self.mock_session_class.return_value = self.mock_session
        self.mock_session.client.side_effect = [self.mock_frauddetector_client, MagicMock()]
        self.integration = FraudDetectorIntegration()

    def tearDown(self):
        self.boto3_session_patcher.stop()

    def test_create_variable_success(self):
        """Test successful variable creation"""
        self.mock_frauddetector_client.create_variable.return_value = {
            "variableArn": "arn:aws:frauddetector:us-east-1:123456789012:variable/ip_address"
        }
        
        result = self.integration.create_variable(
            name="ip_address",
            variable_type=VariableType.STRING,
            data_type="STRING"
        )
        
        self.assertTrue(result["success"])

    def test_get_variable_success(self):
        """Test successful variable retrieval"""
        self.mock_frauddetector_client.get_variable.return_value = {
            "name": "ip_address",
            "variableType": "STRING",
            "dataType": "STRING",
            "status": "ACTIVE"
        }
        
        result = self.integration.get_variable("ip_address")
        
        self.assertTrue(result["success"])

    def test_list_variables_success(self):
        """Test listing variables"""
        self.mock_frauddetector_client.list_variables.return_value = {
            "variables": [
                {"name": "ip_address", "variableType": "STRING"},
                {"name": "amount", "variableType": "FLOAT"}
            ]
        }
        
        result = self.integration.list_variables()
        
        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 2)

    def test_update_variable_success(self):
        """Test successful variable update"""
        self.mock_frauddetector_client.update_variable.return_value = {}
        
        result = self.integration.update_variable(
            name="ip_address",
            description="Updated IP address variable"
        )
        
        self.assertTrue(result["success"])

    def test_delete_variable_success(self):
        """Test successful variable deletion"""
        self.mock_frauddetector_client.delete_variable.return_value = {}
        
        result = self.integration.delete_variable("ip_address")
        
        self.assertTrue(result["success"])


class TestOutcomeManagement(unittest.TestCase):
    """Test outcome management methods"""

    def setUp(self):
        self.mock_frauddetector_client = MagicMock()
        self.boto3_session_patcher = patch('boto3.Session')
        self.mock_session_class = self.boto3_session_patcher.start()
        self.mock_session = MagicMock()
        self.mock_session_class.return_value = self.mock_session
        self.mock_session.client.side_effect = [self.mock_frauddetector_client, MagicMock()]
        self.integration = FraudDetectorIntegration()

    def tearDown(self):
        self.boto3_session_patcher.stop()

    def test_create_outcome_success(self):
        """Test successful outcome creation"""
        self.mock_frauddetector_client.create_outcome.return_value = {
            "outcomeArn": "arn:aws:frauddetector:us-east-1:123456789012:outcome/block"
        }
        
        result = self.integration.create_outcome(
            name="block",
            description="Block transaction"
        )
        
        self.assertTrue(result["success"])

    def test_get_outcome_success(self):
        """Test successful outcome retrieval"""
        self.mock_frauddetector_client.get_outcome.return_value = {
            "name": "block",
            "description": "Block the transaction",
            "status": "ACTIVE"
        }
        
        result = self.integration.get_outcome("block")
        
        self.assertTrue(result["success"])

    def test_list_outcomes_success(self):
        """Test listing outcomes"""
        self.mock_frauddetector_client.list_outcomes.return_value = {
            "outcomes": [
                {"name": "block", "status": "ACTIVE"},
                {"name": "allow", "status": "ACTIVE"}
            ]
        }
        
        result = self.integration.list_outcomes()
        
        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 2)

    def test_update_outcome_success(self):
        """Test successful outcome update"""
        self.mock_frauddetector_client.update_outcome.return_value = {}
        
        result = self.integration.update_outcome(
            name="block",
            description="Updated block outcome"
        )
        
        self.assertTrue(result["success"])

    def test_delete_outcome_success(self):
        """Test successful outcome deletion"""
        self.mock_frauddetector_client.delete_outcome.return_value = {}
        
        result = self.integration.delete_outcome("block")
        
        self.assertTrue(result["success"])


class TestPrediction(unittest.TestCase):
    """Test prediction methods"""

    def setUp(self):
        self.mock_frauddetector_client = MagicMock()
        self.boto3_session_patcher = patch('boto3.Session')
        self.mock_session_class = self.boto3_session_patcher.start()
        self.mock_session = MagicMock()
        self.mock_session_class.return_value = self.mock_session
        self.mock_session.client.side_effect = [self.mock_frauddetector_client, MagicMock()]
        self.integration = FraudDetectorIntegration()

    def tearDown(self):
        self.boto3_session_patcher.stop()

    def test_get_prediction_success(self):
        """Test successful prediction"""
        self.mock_frauddetector_client.get_prediction.return_value = {
            "modelVersion": {"modelId": "test-model", "modelType": "ONLINE_FRAUD_INSIGHTS", "modelVersionNumber": "1"},
            "detector": {"detectorId": "test-detector", "detectorVersion": "1"},
            "prediction": {
                "fraudScore": 850,
                "riskScore": 0.85,
                "riskLevel": "HIGH"
            }
        }
        
        result = self.integration.get_prediction(
            detector_id="test-detector",
            detector_version_id="1",
            event_type_name="test-event",
            entity_id="entity-123",
            entity_type="user",
            event_id="event-123",
            event_variables={"amount": "100", "ip_address": "192.168.1.1"}
        )
        
        self.assertTrue(result["success"])
        self.assertIn("prediction", result)

    def test_get_prediction_not_found(self):
        """Test prediction with not found"""
        error = Exception("Not found")
        error.response = {"Error": {"Code": "ResourceNotFoundException"}}
        self.mock_frauddetector_client.get_prediction.side_effect = error
        
        result = self.integration.get_prediction(
            detector_id="nonexistent",
            detector_version_id="1",
            event_type_name="test-event",
            entity_id="entity-123",
            entity_type="user",
            event_id="event-123",
            event_variables={}
        )
        
        self.assertFalse(result["success"])


class TestBatchPrediction(unittest.TestCase):
    """Test batch prediction methods"""

    def setUp(self):
        self.mock_frauddetector_client = MagicMock()
        self.boto3_session_patcher = patch('boto3.Session')
        self.mock_session_class = self.boto3_session_patcher.start()
        self.mock_session = MagicMock()
        self.mock_session_class.return_value = self.mock_session
        self.mock_session.client.side_effect = [self.mock_frauddetector_client, MagicMock()]
        self.integration = FraudDetectorIntegration()

    def tearDown(self):
        self.boto3_session_patcher.stop()

    def test_create_batch_prediction_success(self):
        """Test successful batch prediction creation"""
        self.mock_frauddetector_client.create_batch_prediction.return_value = {
            "batchPredictionId": "batch-123",
            "batchPredictionArn": "arn:aws:frauddetector:us-east-1:123456789012:batch-prediction/batch-123",
            "status": "RUNNING"
        }
        
        result = self.integration.create_batch_prediction(
            batch_prediction_name="test-batch",
            detector_name="test-detector",
            detector_version="1",
            input_path="s3://bucket/input.csv",
            output_path="s3://bucket/output/"
        )
        
        self.assertTrue(result["success"])
        self.assertEqual(result["job_id"], "batch-123")

    def test_get_batch_prediction_success(self):
        """Test successful batch prediction retrieval"""
        self.mock_frauddetector_client.get_batch_prediction.return_value = {
            "batchPredictionId": "batch-123",
            "batchPredictionName": "test-batch",
            "status": "COMPLETED",
            "inputPath": "s3://bucket/input.csv",
            "outputPath": "s3://bucket/output/"
        }
        
        result = self.integration.get_batch_prediction("batch-123")
        
        self.assertTrue(result["success"])

    def test_list_batch_predictions_success(self):
        """Test listing batch predictions"""
        self.mock_frauddetector_client.list_batch_predictions.return_value = {
            "batchPredictions": [
                {"batchPredictionId": "batch-1", "status": "COMPLETED"},
                {"batchPredictionId": "batch-2", "status": "RUNNING"}
            ]
        }
        
        result = self.integration.list_batch_predictions()
        
        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 2)

    def test_delete_batch_prediction_success(self):
        """Test successful batch prediction deletion"""
        self.mock_frauddetector_client.delete_batch_prediction.return_value = {}
        
        result = self.integration.delete_batch_prediction("batch-123")
        
        self.assertTrue(result["success"])


if __name__ == '__main__':
    unittest.main()

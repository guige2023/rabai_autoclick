"""
Tests for workflow_aws_sagemakerml module

Comprehensive tests for SageMakerMLIntegration class covering:
- Model management
- Endpoint configuration management
- Endpoint management
- Training jobs
- Processing jobs
- Notebook instances
- Hyperparameter tuning jobs
- Model registry
- SageMaker Pipelines
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
import src.workflow_aws_sagemakerml as sagemaker_module

SageMakerMLIntegration = sagemaker_module.SageMakerMLIntegration
SageMakerResourceState = sagemaker_module.SageMakerResourceState
TrainingJobStatus = sagemaker_module.TrainingJobStatus
ProcessingJobStatus = sagemaker_module.ProcessingJobStatus
TuningJobStatus = sagemaker_module.TuningJobStatus
PipelineExecutionStatus = sagemaker_module.PipelineExecutionStatus
InstanceType = sagemaker_module.InstanceType
SageMakerConfig = sagemaker_module.SageMakerConfig
ModelConfig = sagemaker_module.ModelConfig
EndpointConfigConfig = sagemaker_module.EndpointConfigConfig
EndpointConfig = sagemaker_module.EndpointConfig
TrainingJobConfig = sagemaker_module.TrainingJobConfig
ProcessingJobConfig = sagemaker_module.ProcessingJobConfig
NotebookInstanceConfig = sagemaker_module.NotebookInstanceConfig
HyperparameterTuningJobConfig = sagemaker_module.HyperparameterTuningJobConfig
ModelRegistryConfig = sagemaker_module.ModelRegistryConfig
PipelineConfig = sagemaker_module.PipelineConfig
CloudWatchMetricsConfig = sagemaker_module.CloudWatchMetricsConfig


class TestSageMakerResourceState(unittest.TestCase):
    """Test SageMakerResourceState enum"""

    def test_state_values(self):
        self.assertEqual(SageMakerResourceState.CREATE_IN_PROGRESS.value, "CreateInProgress")
        self.assertEqual(SageMakerResourceState.CREATE_COMPLETE.value, "CreateComplete")
        self.assertEqual(SageMakerResourceState.IN_SERVICE.value, "InService")
        self.assertEqual(SageMakerResourceState.FAILED.value, "Failed")
        self.assertEqual(SageMakerResourceState.DELETE_IN_PROGRESS.value, "DeleteInProgress")


class TestTrainingJobStatus(unittest.TestCase):
    """Test TrainingJobStatus enum"""

    def test_status_values(self):
        self.assertEqual(TrainingJobStatus.IN_PROGRESS.value, "InProgress")
        self.assertEqual(TrainingJobStatus.COMPLETED.value, "Completed")
        self.assertEqual(TrainingJobStatus.FAILED.value, "Failed")
        self.assertEqual(TrainingJobStatus.STOPPED.value, "Stopped")


class TestProcessingJobStatus(unittest.TestCase):
    """Test ProcessingJobStatus enum"""

    def test_status_values(self):
        self.assertEqual(ProcessingJobStatus.IN_PROGRESS.value, "InProgress")
        self.assertEqual(ProcessingJobStatus.COMPLETED.value, "Completed")
        self.assertEqual(ProcessingJobStatus.FAILED.value, "Failed")


class TestTuningJobStatus(unittest.TestCase):
    """Test TuningJobStatus enum"""

    def test_status_values(self):
        self.assertEqual(TuningJobStatus.IN_PROGRESS.value, "InProgress")
        self.assertEqual(TuningJobStatus.COMPLETED.value, "Completed")
        self.assertEqual(TuningJobStatus.FAILED.value, "Failed")
        self.assertEqual(TuningJobStatus.PENDING.value, "Pending")


class TestInstanceType(unittest.TestCase):
    """Test InstanceType enum"""

    def test_instance_type_values(self):
        self.assertEqual(InstanceType.ML_M4_XLARGE.value, "ml.m4.xlarge")
        self.assertEqual(InstanceType.ML_M5_XLARGE.value, "ml.m5.xlarge")
        self.assertEqual(InstanceType.ML_P3_2XLARGE.value, "ml.p3.2xlarge")
        self.assertEqual(InstanceType.ML_G4DN_XLARGE.value, "ml.g4dn.xlarge")


class TestSageMakerConfig(unittest.TestCase):
    """Test SageMakerConfig dataclass"""

    def test_config_defaults(self):
        config = SageMakerConfig()
        self.assertEqual(config.region_name, "us-east-1")
        self.assertIsNone(config.aws_access_key_id)
        self.assertIsNone(config.aws_secret_access_key)
        self.assertIsNone(config.profile_name)

    def test_config_custom(self):
        config = SageMakerConfig(
            region_name="us-west-2",
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            profile_name="myprofile"
        )
        self.assertEqual(config.region_name, "us-west-2")
        self.assertEqual(config.aws_access_key_id, "AKIAIOSFODNN7EXAMPLE")
        self.assertEqual(config.profile_name, "myprofile")


class TestModelConfig(unittest.TestCase):
    """Test ModelConfig dataclass"""

    def test_model_config_creation(self):
        config = ModelConfig(
            model_name="test-model",
            primary_container_image="123456789012.dkr.ecr.us-east-1.amazonaws.com/my-model:latest",
            execution_role_arn="arn:aws:iam::123456789012:role/test-role"
        )
        self.assertEqual(config.model_name, "test-model")
        self.assertEqual(config.primary_container_image, "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-model:latest")
        self.assertEqual(config.execution_role_arn, "arn:aws:iam::123456789012:role/test-role")
        self.assertFalse(config.enable_network_isolation)
        self.assertEqual(config.tags, {})


class TestEndpointConfigConfig(unittest.TestCase):
    """Test EndpointConfigConfig dataclass"""

    def test_endpoint_config_creation(self):
        config = EndpointConfigConfig(
            endpoint_config_name="test-endpoint-config",
            production_variants=[
                {
                    "VariantName": "variant1",
                    "InstanceType": "ml.m4.xlarge",
                    "InitialInstanceCount": 1
                }
            ]
        )
        self.assertEqual(config.endpoint_config_name, "test-endpoint-config")
        self.assertEqual(len(config.production_variants), 1)
        self.assertEqual(config.tags, {})


class TestEndpointConfig(unittest.TestCase):
    """Test EndpointConfig dataclass"""

    def test_endpoint_config_creation(self):
        config = EndpointConfig(
            endpoint_name="test-endpoint",
            endpoint_config_name="test-endpoint-config"
        )
        self.assertEqual(config.endpoint_name, "test-endpoint")
        self.assertEqual(config.endpoint_config_name, "test-endpoint-config")
        self.assertFalse(config.retain_all_variant_properties)


class TestTrainingJobConfig(unittest.TestCase):
    """Test TrainingJobConfig dataclass"""

    def test_training_job_config_creation(self):
        config = TrainingJobConfig(
            training_job_name="test-training-job",
            algorithm_specification={
                "TrainingImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/training:latest",
                "TrainingInputMode": "File"
            },
            output_data_config={
                "KmsKeyId": "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab",
                "S3OutputPath": "s3://my-bucket/output/"
            },
            resource_config={
                "InstanceType": "ml.m4.xlarge",
                "InstanceCount": 1,
                "VolumeSizeInGB": 50
            }
        )
        self.assertEqual(config.training_job_name, "test-training-job")
        self.assertIn("TrainingImage", config.algorithm_specification)
        self.assertFalse(config.enable_network_isolation)


class TestProcessingJobConfig(unittest.TestCase):
    """Test ProcessingJobConfig dataclass"""

    def test_processing_job_config_creation(self):
        config = ProcessingJobConfig(
            processing_job_name="test-processing-job",
            processing_resources={
                "ClusterConfig": {
                    "InstanceType": "ml.m4.xlarge",
                    "InstanceCount": 1,
                    "VolumeSizeInGB": 20
                }
            },
            processing_app_specification={
                "ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/processing:latest"
            }
        )
        self.assertEqual(config.processing_job_name, "test-processing-job")
        self.assertEqual(config.tags, {})


class TestNotebookInstanceConfig(unittest.TestCase):
    """Test NotebookInstanceConfig dataclass"""

    def test_notebook_instance_config_creation(self):
        config = NotebookInstanceConfig(
            notebook_instance_name="test-notebook",
            instance_type="ml.t3.medium",
            role_arn="arn:aws:iam::123456789012:role/test-role"
        )
        self.assertEqual(config.notebook_instance_name, "test-notebook")
        self.assertEqual(config.instance_type, "ml.t3.medium")
        self.assertEqual(config.root_access, "Enabled")


class TestHyperparameterTuningJobConfig(unittest.TestCase):
    """Test HyperparameterTuningJobConfig dataclass"""

    def test_tuning_job_config_creation(self):
        config = HyperparameterTuningJobConfig(
            tuning_job_name="test-tuning-job",
            hyperparameter_ranges={
                "IntegerParameterRanges": [
                    {"Name": "num_layers", "MinValue": "1", "MaxValue": "5"}
                ]
            },
            training_job_definition={
                "StaticHyperParameters": {},
                "AlgorithmSpecification": {},
                "RoleArn": "arn:aws:iam::123456789012:role/test-role"
            }
        )
        self.assertEqual(config.tuning_job_name, "test-tuning-job")
        self.assertEqual(config.tuning_strategy, "Bayesian")


class TestModelRegistryConfig(unittest.TestCase):
    """Test ModelRegistryConfig dataclass"""

    def test_model_registry_config_creation(self):
        config = ModelRegistryConfig(
            model_package_group_name="test-model-group",
            model_package_description="Test model package group"
        )
        self.assertEqual(config.model_package_group_name, "test-model-group")
        self.assertEqual(config.model_package_description, "Test model package group")


class TestPipelineConfig(unittest.TestCase):
    """Test PipelineConfig dataclass"""

    def test_pipeline_config_creation(self):
        config = PipelineConfig(
            pipeline_name="test-pipeline",
            role_arn="arn:aws:iam::123456789012:role/test-role",
            pipeline_description="Test pipeline"
        )
        self.assertEqual(config.pipeline_name, "test-pipeline")
        self.assertEqual(config.role_arn, "arn:aws:iam::123456789012:role/test-role")


class TestCloudWatchMetricsConfig(unittest.TestCase):
    """Test CloudWatchMetricsConfig dataclass"""

    def test_metrics_config_defaults(self):
        config = CloudWatchMetricsConfig()
        self.assertEqual(config.namespace, "SageMaker")
        self.assertEqual(config.training_metrics, [])
        self.assertEqual(config.custom_metrics, [])


class TestSageMakerMLIntegration(unittest.TestCase):
    """Test SageMakerMLIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_sagemaker_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        
        # Patch boto3 client
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_sagemaker_client

        # Create integration instance
        self.integration = SageMakerMLIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_init_with_config(self):
        """Test initialization with config"""
        config = SageMakerConfig(region_name="us-west-2")
        integration = SageMakerMLIntegration(config=config)
        self.assertEqual(integration.config.region_name, "us-west-2")

    def test_init_defaults(self):
        """Test initialization with defaults"""
        integration = SageMakerMLIntegration()
        self.assertEqual(integration.config.region_name, "us-east-1")

    def test_create_model_success(self):
        """Test successful model creation"""
        self.mock_sagemaker_client.create_model.return_value = {
            "ModelName": "test-model",
            "ModelArn": "arn:aws:sagemaker:us-east-1:123456789012:model/test-model"
        }
        
        config = ModelConfig(
            model_name="test-model",
            primary_container_image="123456789012.dkr.ecr.us-east-1.amazonaws.com/my-model:latest",
            execution_role_arn="arn:aws:iam::123456789012:role/test-role"
        )
        
        result = self.integration.create_model(config)
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["model"], "test-model")
        self.mock_sagemaker_client.create_model.assert_called_once()

    def test_create_model_client_error(self):
        """Test model creation with client error"""
        # Create a mock ClientError
        error_response = {'Error': {'Code': 'ValidationException', 'Message': 'Invalid input'}}
        mock_client_error = Exception("Client error")
        mock_client_error.response = error_response
        self.mock_sagemaker_client.create_model.side_effect = mock_client_error
        
        config = ModelConfig(
            model_name="test-model",
            execution_role_arn="arn:aws:iam::123456789012:role/test-role"
        )
        
        result = self.integration.create_model(config)
        
        self.assertEqual(result["status"], "error")

    def test_describe_model_success(self):
        """Test successful model description"""
        self.mock_sagemaker_client.describe_model.return_value = {
            "ModelName": "test-model",
            "ModelArn": "arn:aws:sagemaker:us-east-1:123456789012:model/test-model",
            "CreationTime": "2024-01-01T00:00:00Z"
        }
        
        result = self.integration.describe_model("test-model")
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["model"]["ModelName"], "test-model")

    def test_describe_model_error(self):
        """Test model description with error"""
        self.mock_sagemaker_client.describe_model.side_effect = Exception("Not found")
        
        result = self.integration.describe_model("nonexistent-model")
        
        self.assertEqual(result["status"], "error")

    def test_list_models_success(self):
        """Test listing models"""
        self.mock_sagemaker_client.list_models.return_value = {
            "Models": [
                {"ModelName": "model1"},
                {"ModelName": "model2"}
            ]
        }
        
        result = self.integration.list_models()
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["count"], 2)

    def test_list_models_with_prefix(self):
        """Test listing models with prefix filter"""
        self.mock_sagemaker_client.list_models.return_value = {
            "Models": [{"ModelName": "test-model"}]
        }
        
        result = self.integration.list_models(prefix="test")
        
        self.mock_sagemaker_client.list_models.assert_called_once()
        call_args = self.mock_sagemaker_client.list_models.call_args
        self.assertEqual(call_args[1]["NameContains"], "test")

    def test_delete_model_success(self):
        """Test successful model deletion"""
        self.mock_sagemaker_client.delete_model.return_value = {}
        
        result = self.integration.delete_model("test-model")
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["model"], "test-model")

    def test_delete_model_error(self):
        """Test model deletion with error"""
        self.mock_sagemaker_client.delete_model.side_effect = Exception("Delete failed")
        
        result = self.integration.delete_model("test-model")
        
        self.assertEqual(result["status"], "error")


class TestEndpointConfiguration(unittest.TestCase):
    """Test endpoint configuration methods"""

    def setUp(self):
        self.mock_sagemaker_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_sagemaker_client
        self.integration = SageMakerMLIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_create_endpoint_config_success(self):
        """Test successful endpoint configuration creation"""
        self.mock_sagemaker_client.create_endpoint_config.return_value = {
            "EndpointConfigArn": "arn:aws:sagemaker:us-east-1:123456789012:endpoint-config/test-config"
        }
        
        config = EndpointConfigConfig(
            endpoint_config_name="test-config",
            production_variants=[
                {"VariantName": "variant1", "InstanceType": "ml.m4.xlarge", "InitialInstanceCount": 1}
            ]
        )
        
        result = self.integration.create_endpoint_config(config)
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["endpoint_config"], "test-config")

    def test_describe_endpoint_config_success(self):
        """Test successful endpoint configuration description"""
        self.mock_sagemaker_client.describe_endpoint_config.return_value = {
            "EndpointConfigName": "test-config"
        }
        
        result = self.integration.describe_endpoint_config("test-config")
        
        self.assertEqual(result["status"], "success")

    def test_list_endpoint_configs_success(self):
        """Test listing endpoint configurations"""
        self.mock_sagemaker_client.list_endpoint_configs.return_value = {
            "EndpointConfigs": [{"EndpointConfigName": "config1"}]
        }
        
        result = self.integration.list_endpoint_configs()
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["count"], 1)

    def test_delete_endpoint_config_success(self):
        """Test successful endpoint configuration deletion"""
        self.mock_sagemaker_client.delete_endpoint_config.return_value = {}
        
        result = self.integration.delete_endpoint_config("test-config")
        
        self.assertEqual(result["status"], "success")


class TestEndpointManagement(unittest.TestCase):
    """Test endpoint management methods"""

    def setUp(self):
        self.mock_sagemaker_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_sagemaker_client
        self.integration = SageMakerMLIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_create_endpoint_success(self):
        """Test successful endpoint creation"""
        self.mock_sagemaker_client.create_endpoint.return_value = {
            "EndpointArn": "arn:aws:sagemaker:us-east-1:123456789012:endpoint/test-endpoint"
        }
        
        config = EndpointConfig(
            endpoint_name="test-endpoint",
            endpoint_config_name="test-config"
        )
        
        result = self.integration.create_endpoint(config)
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["endpoint"], "test-endpoint")

    def test_describe_endpoint_success(self):
        """Test successful endpoint description"""
        self.mock_sagemaker_client.describe_endpoint.return_value = {
            "EndpointName": "test-endpoint",
            "EndpointStatus": "InService"
        }
        
        result = self.integration.describe_endpoint("test-endpoint")
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["endpoint"]["EndpointStatus"], "InService")

    def test_list_endpoints_success(self):
        """Test listing endpoints"""
        self.mock_sagemaker_client.list_endpoints.return_value = {
            "Endpoints": [{"EndpointName": "ep1"}, {"EndpointName": "ep2"}]
        }
        
        result = self.integration.list_endpoints()
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["count"], 2)

    def test_list_endpoints_with_status_filter(self):
        """Test listing endpoints with status filter"""
        self.mock_sagemaker_client.list_endpoints.return_value = {
            "Endpoints": [{"EndpointName": "ep1", "EndpointStatus": "InService"}]
        }
        
        result = self.integration.list_endpoints(status_equals="InService")
        
        call_args = self.mock_sagemaker_client.list_endpoints.call_args
        self.assertEqual(call_args[1]["StatusEquals"], "InService")

    def test_update_endpoint_success(self):
        """Test successful endpoint update"""
        self.mock_sagemaker_client.update_endpoint.return_value = {
            "EndpointArn": "arn:aws:sagemaker:us-east-1:123456789012:endpoint/test-endpoint"
        }
        
        result = self.integration.update_endpoint("test-endpoint", "new-config")
        
        self.assertEqual(result["status"], "success")

    def test_delete_endpoint_success(self):
        """Test successful endpoint deletion"""
        self.mock_sagemaker_client.delete_endpoint.return_value = {}
        
        result = self.integration.delete_endpoint("test-endpoint")
        
        self.assertEqual(result["status"], "success")


class TestTrainingJobs(unittest.TestCase):
    """Test training job methods"""

    def setUp(self):
        self.mock_sagemaker_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_sagemaker_client
        self.integration = SageMakerMLIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_create_training_job_success(self):
        """Test successful training job creation"""
        self.mock_sagemaker_client.create_training_job.return_value = {
            "TrainingJobArn": "arn:aws:sagemaker:us-east-1:123456789012:training-job/test-job"
        }
        
        config = TrainingJobConfig(
            training_job_name="test-job",
            algorithm_specification={
                "TrainingImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/training:latest",
                "TrainingInputMode": "File"
            },
            output_data_config={
                "KmsKeyId": "key-id",
                "S3OutputPath": "s3://bucket/output/"
            },
            resource_config={
                "InstanceType": "ml.m4.xlarge",
                "InstanceCount": 1,
                "VolumeSizeInGB": 50
            }
        )
        
        result = self.integration.create_training_job(config)
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["training_job"], "test-job")

    def test_describe_training_job_success(self):
        """Test successful training job description"""
        self.mock_sagemaker_client.describe_training_job.return_value = {
            "TrainingJobName": "test-job",
            "TrainingJobStatus": "Completed"
        }
        
        result = self.integration.describe_training_job("test-job")
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["training_job"]["TrainingJobStatus"], "Completed")

    def test_list_training_jobs_success(self):
        """Test listing training jobs"""
        self.mock_sagemaker_client.list_training_jobs.return_value = {
            "TrainingJobSummaries": [
                {"TrainingJobName": "job1"},
                {"TrainingJobName": "job2"}
            ]
        }
        
        result = self.integration.list_training_jobs()
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["count"], 2)

    def test_stop_training_job_success(self):
        """Test successful training job stop"""
        self.mock_sagemaker_client.stop_training_job.return_value = {}
        
        result = self.integration.stop_training_job("test-job")
        
        self.assertEqual(result["status"], "success")


class TestProcessingJobs(unittest.TestCase):
    """Test processing job methods"""

    def setUp(self):
        self.mock_sagemaker_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_sagemaker_client
        self.integration = SageMakerMLIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_create_processing_job_success(self):
        """Test successful processing job creation"""
        self.mock_sagemaker_client.create_processing_job.return_value = {
            "ProcessingJobArn": "arn:aws:sagemaker:us-east-1:123456789012:processing-job/test-job"
        }
        
        config = ProcessingJobConfig(
            processing_job_name="test-job",
            processing_resources={
                "ClusterConfig": {
                    "InstanceType": "ml.m4.xlarge",
                    "InstanceCount": 1,
                    "VolumeSizeInGB": 20
                }
            },
            processing_app_specification={
                "ImageUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/processing:latest"
            }
        )
        
        result = self.integration.create_processing_job(config)
        
        self.assertEqual(result["status"], "success")

    def test_describe_processing_job_success(self):
        """Test successful processing job description"""
        self.mock_sagemaker_client.describe_processing_job.return_value = {
            "ProcessingJobName": "test-job",
            "ProcessingJobStatus": "Completed"
        }
        
        result = self.integration.describe_processing_job("test-job")
        
        self.assertEqual(result["status"], "success")

    def test_list_processing_jobs_success(self):
        """Test listing processing jobs"""
        self.mock_sagemaker_client.list_processing_jobs.return_value = {
            "ProcessingJobSummaries": [{"ProcessingJobName": "job1"}]
        }
        
        result = self.integration.list_processing_jobs()
        
        self.assertEqual(result["status"], "success")

    def test_stop_processing_job_success(self):
        """Test successful processing job stop"""
        self.mock_sagemaker_client.stop_processing_job.return_value = {}
        
        result = self.integration.stop_processing_job("test-job")
        
        self.assertEqual(result["status"], "success")


class TestNotebookInstances(unittest.TestCase):
    """Test notebook instance methods"""

    def setUp(self):
        self.mock_sagemaker_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_sagemaker_client
        self.integration = SageMakerMLIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_create_notebook_instance_success(self):
        """Test successful notebook instance creation"""
        self.mock_sagemaker_client.create_notebook_instance.return_value = {
            "NotebookInstanceArn": "arn:aws:sagemaker:us-east-1:123456789012:notebook-instance/test-notebook"
        }
        
        config = NotebookInstanceConfig(
            notebook_instance_name="test-notebook",
            instance_type="ml.t3.medium",
            role_arn="arn:aws:iam::123456789012:role/test-role"
        )
        
        result = self.integration.create_notebook_instance(config)
        
        self.assertEqual(result["status"], "success")

    def test_describe_notebook_instance_success(self):
        """Test successful notebook instance description"""
        self.mock_sagemaker_client.describe_notebook_instance.return_value = {
            "NotebookInstanceName": "test-notebook",
            "NotebookInstanceStatus": "InService"
        }
        
        result = self.integration.describe_notebook_instance("test-notebook")
        
        self.assertEqual(result["status"], "success")

    def test_list_notebook_instances_success(self):
        """Test listing notebook instances"""
        self.mock_sagemaker_client.list_notebook_instances.return_value = {
            "NotebookInstances": [{"NotebookInstanceName": "nb1"}]
        }
        
        result = self.integration.list_notebook_instances()
        
        self.assertEqual(result["status"], "success")

    def test_start_notebook_instance_success(self):
        """Test successful notebook instance start"""
        self.mock_sagemaker_client.start_notebook_instance.return_value = {}
        
        result = self.integration.start_notebook_instance("test-notebook")
        
        self.assertEqual(result["status"], "success")

    def test_stop_notebook_instance_success(self):
        """Test successful notebook instance stop"""
        self.mock_sagemaker_client.stop_notebook_instance.return_value = {}
        
        result = self.integration.stop_notebook_instance("test-notebook")
        
        self.assertEqual(result["status"], "success")

    def test_delete_notebook_instance_success(self):
        """Test successful notebook instance deletion"""
        self.mock_sagemaker_client.delete_notebook_instance.return_value = {}
        
        result = self.integration.delete_notebook_instance("test-notebook")
        
        self.assertEqual(result["status"], "success")


class TestHyperparameterTuningJobs(unittest.TestCase):
    """Test hyperparameter tuning job methods"""

    def setUp(self):
        self.mock_sagemaker_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_sagemaker_client
        self.integration = SageMakerMLIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_create_tuning_job_success(self):
        """Test successful tuning job creation"""
        self.mock_sagemaker_client.create_hyper_parameter_tuning_job.return_value = {
            "HyperParameterTuningJobArn": "arn:aws:sagemaker:us-east-1:123456789012:hyper-parameter-tuning-job/test-job"
        }
        
        config = HyperparameterTuningJobConfig(
            tuning_job_name="test-tuning-job",
            hyperparameter_ranges={
                "IntegerParameterRanges": [
                    {"Name": "num_layers", "MinValue": "1", "MaxValue": "5"}
                ]
            },
            training_job_definition={
                "StaticHyperParameters": {},
                "AlgorithmSpecification": {},
                "RoleArn": "arn:aws:iam::123456789012:role/test-role"
            }
        )
        
        result = self.integration.create_hyperparameter_tuning_job(config)
        
        self.assertEqual(result["status"], "success")

    def test_describe_tuning_job_success(self):
        """Test successful tuning job description"""
        self.mock_sagemaker_client.describe_hyper_parameter_tuning_job.return_value = {
            "HyperParameterTuningJobName": "test-job",
            "HyperParameterTuningJobStatus": "Completed"
        }
        
        result = self.integration.describe_hyperparameter_tuning_job("test-job")
        
        self.assertEqual(result["status"], "success")

    def test_list_tuning_jobs_success(self):
        """Test listing tuning jobs"""
        self.mock_sagemaker_client.list_hyper_parameter_tuning_jobs.return_value = {
            "HyperParameterTuningJobSummaries": [{"HyperParameterTuningJobName": "job1"}]
        }
        
        result = self.integration.list_hyperparameter_tuning_jobs()
        
        self.assertEqual(result["status"], "success")

    def test_stop_tuning_job_success(self):
        """Test successful tuning job stop"""
        self.mock_sagemaker_client.stop_hyper_parameter_tuning_job.return_value = {}
        
        result = self.integration.stop_hyperparameter_tuning_job("test-job")
        
        self.assertEqual(result["status"], "success")


class TestModelRegistry(unittest.TestCase):
    """Test model registry methods"""

    def setUp(self):
        self.mock_sagemaker_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_sagemaker_client
        self.integration = SageMakerMLIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_create_model_package_group_success(self):
        """Test successful model package group creation"""
        self.mock_sagemaker_client.create_model_package_group.return_value = {
            "ModelPackageGroupArn": "arn:aws:sagemaker:us-east-1:123456789012:model-package-group/test-group"
        }
        
        config = ModelRegistryConfig(
            model_package_group_name="test-group",
            model_package_description="Test group"
        )
        
        result = self.integration.create_model_package_group(config)
        
        self.assertEqual(result["status"], "success")

    def test_describe_model_package_group_success(self):
        """Test successful model package group description"""
        self.mock_sagemaker_client.describe_model_package_group.return_value = {
            "ModelPackageGroupName": "test-group"
        }
        
        result = self.integration.describe_model_package_group("test-group")
        
        self.assertEqual(result["status"], "success")

    def test_list_model_package_groups_success(self):
        """Test listing model package groups"""
        self.mock_sagemaker_client.list_model_package_groups.return_value = {
            "ModelPackageGroupSummaryList": [{"ModelPackageGroupName": "group1"}]
        }
        
        result = self.integration.list_model_package_groups()
        
        self.assertEqual(result["status"], "success")

    def test_register_model_version_success(self):
        """Test successful model version registration"""
        self.mock_sagemaker_client.create_model_package.return_value = {
            "ModelPackageArn": "arn:aws:sagemaker:us-east-1:123456789012:model-package/test-pkg/1"
        }
        
        result = self.integration.register_model_version(
            model_package_group_name="test-group",
            model_image_uri="123456789012.dkr.ecr.us-east-1.amazonaws.com/model:latest",
            model_data_url="s3://bucket/model.tar.gz"
        )
        
        self.assertEqual(result["status"], "success")

    def test_update_model_package_approval_status_success(self):
        """Test successful model package approval status update"""
        self.mock_sagemaker_client.update_model_package.return_value = {}
        
        result = self.integration.update_model_package_approval_status(
            model_package_arn="arn:aws:sagemaker:us-east-1:123456789012:model-package/test-pkg/1",
            approval_status="Approved"
        )
        
        self.assertEqual(result["status"], "success")


class TestSageMakerPipelines(unittest.TestCase):
    """Test SageMaker Pipelines methods"""

    def setUp(self):
        self.mock_sagemaker_client = MagicMock()
        self.boto3_patcher = patch('boto3.client')
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.mock_sagemaker_client
        self.integration = SageMakerMLIntegration()

    def tearDown(self):
        self.boto3_patcher.stop()

    def test_create_pipeline_success(self):
        """Test successful pipeline creation"""
        self.mock_sagemaker_client.create_pipeline.return_value = {
            "PipelineArn": "arn:aws:sagemaker:us-east-1:123456789012:pipeline/test-pipeline"
        }
        
        config = PipelineConfig(
            pipeline_name="test-pipeline",
            role_arn="arn:aws:iam::123456789012:role/test-role",
            pipeline_description="Test pipeline"
        )
        
        result = self.integration.create_pipeline(config)
        
        self.assertEqual(result["status"], "success")

    def test_describe_pipeline_success(self):
        """Test successful pipeline description"""
        self.mock_sagemaker_client.describe_pipeline.return_value = {
            "PipelineName": "test-pipeline",
            "PipelineArn": "arn:aws:sagemaker:us-east-1:123456789012:pipeline/test-pipeline"
        }
        
        result = self.integration.describe_pipeline("test-pipeline")
        
        self.assertEqual(result["status"], "success")


if __name__ == '__main__':
    unittest.main()

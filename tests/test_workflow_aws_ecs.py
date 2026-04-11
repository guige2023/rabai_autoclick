"""
Tests for workflow_aws_ecs module
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

# Create mock boto3 module before importing workflow_aws_ecs
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

# Now we can import the module
from src.workflow_aws_ecs import (
    ECSIntegration,
    ClusterStatus,
    TaskStatus,
    ServiceStatus,
    LaunchType,
    NetworkMode,
    LogDriver,
    HealthCheckType,
    PlacementStrategyType,
    SortOrder,
    ContainerDefinition,
    VolumeDefinition,
    TaskDefinition,
    ServiceDiscoveryConfig,
    AutoScalingConfig,
    LoadBalancerConfig,
    ClusterInfo,
    TaskInfo,
    ServiceInfo,
)


class TestClusterStatus(unittest.TestCase):
    """Test ClusterStatus enum"""

    def test_cluster_status_values(self):
        self.assertEqual(ClusterStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(ClusterStatus.PROVISIONING.value, "PROVISIONING")
        self.assertEqual(ClusterStatus.DEPROVISIONING.value, "DEPROVISIONING")
        self.assertEqual(ClusterStatus.FAILED.value, "FAILED")
        self.assertEqual(ClusterStatus.INACTIVE.value, "INACTIVE")


class TestTaskStatus(unittest.TestCase):
    """Test TaskStatus enum"""

    def test_task_status_values(self):
        self.assertEqual(TaskStatus.PROVISIONING.value, "PROVISIONING")
        self.assertEqual(TaskStatus.PENDING.value, "PENDING")
        self.assertEqual(TaskStatus.ACTIVATING.value, "ACTIVATING")
        self.assertEqual(TaskStatus.RUNNING.value, "RUNNING")
        self.assertEqual(TaskStatus.DEACTIVATING.value, "DEACTIVATING")
        self.assertEqual(TaskStatus.STOPPING.value, "STOPPING")
        self.assertEqual(TaskStatus.DEPROVISIONING.value, "DEPROVISIONING")
        self.assertEqual(TaskStatus.STOPPED.value, "STOPPED")


class TestServiceStatus(unittest.TestCase):
    """Test ServiceStatus enum"""

    def test_service_status_values(self):
        self.assertEqual(ServiceStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(ServiceStatus.PROVISIONING.value, "PROVISIONING")
        self.assertEqual(ServiceStatus.DEPROVISIONING.value, "DEPROVISIONING")
        self.assertEqual(ServiceStatus.FAILED.value, "FAILED")
        self.assertEqual(ServiceStatus.INACTIVE.value, "INACTIVE")


class TestLaunchType(unittest.TestCase):
    """Test LaunchType enum"""

    def test_launch_type_values(self):
        self.assertEqual(LaunchType.EC2.value, "EC2")
        self.assertEqual(LaunchType.FARGATE.value, "FARGATE")
        self.assertEqual(LaunchType.EXTERNAL.value, "EXTERNAL")


class TestNetworkMode(unittest.TestCase):
    """Test NetworkMode enum"""

    def test_network_mode_values(self):
        self.assertEqual(NetworkMode.NONE.value, "none")
        self.assertEqual(NetworkMode.BRIDGE.value, "bridge")
        self.assertEqual(NetworkMode.HOST.value, "host")
        self.assertEqual(NetworkMode.AWS_VPC.value, "awsvpc")
        self.assertEqual(NetworkMode.NATS.value, "nats")


class TestLogDriver(unittest.TestCase):
    """Test LogDriver enum"""

    def test_log_driver_values(self):
        self.assertEqual(LogDriver.JSON_FILE.value, "json-file")
        self.assertEqual(LogDriver.SYSLOG.value, "syslog")
        self.assertEqual(LogDriver.JOURNALD.value, "journald")
        self.assertEqual(LogDriver.GELF.value, "gelf")
        self.assertEqual(LogDriver.FLUENTD.value, "fluentd")
        self.assertEqual(LogDriver.AWSLOGS.value, "awslogs")
        self.assertEqual(LogDriver.SPLUNK.value, "splunk")
        self.assertEqual(LogDriver.NONE.value, "none")


class TestHealthCheckType(unittest.TestCase):
    """Test HealthCheckType enum"""

    def test_health_check_type_values(self):
        self.assertEqual(HealthCheckType.ECS.value, "ECS")
        self.assertEqual(HealthCheckType.ELB.value, "ELB")
        self.assertEqual(HealthCheckType.ALB.value, "ALB")
        self.assertEqual(HealthCheckType.NLB.value, "NLB")


class TestPlacementStrategyType(unittest.TestCase):
    """Test PlacementStrategyType enum"""

    def test_placement_strategy_type_values(self):
        self.assertEqual(PlacementStrategyType.RANDOM.value, "random")
        self.assertEqual(PlacementStrategyType.SPREAD.value, "spread")
        self.assertEqual(PlacementStrategyType.BINPACK.value, "binpack")


class TestSortOrder(unittest.TestCase):
    """Test SortOrder enum"""

    def test_sort_order_values(self):
        self.assertEqual(SortOrder.ASC.value, "ASC")
        self.assertEqual(SortOrder.DESC.value, "DESC")


class TestContainerDefinition(unittest.TestCase):
    """Test ContainerDefinition dataclass"""

    def test_container_definition_defaults(self):
        container = ContainerDefinition(
            name="test-container",
            image="nginx:latest"
        )
        self.assertEqual(container.name, "test-container")
        self.assertEqual(container.image, "nginx:latest")
        self.assertTrue(container.essential)
        self.assertEqual(container.cpu, 0)
        self.assertEqual(container.memory, 256)
        self.assertEqual(container.memory_reservation, 0)
        self.assertEqual(container.command, [])
        self.assertEqual(container.entry_point, [])
        self.assertEqual(container.environment, {})

    def test_container_definition_full(self):
        container = ContainerDefinition(
            name="app-container",
            image="my-app:latest",
            essential=True,
            cpu=1024,
            memory=2048,
            memory_reservation=1024,
            command=["python", "app.py"],
            entry_point=["/bin/bash"],
            working_directory="/app",
            environment={"ENV": "prod", "DEBUG": "false"},
            port_mappings=[{"containerPort": 8080, "hostPort": 80}],
            health_check={"command": ["CMD-SHELL", "curl -f http://localhost/"]},
            privileged=True,
            user="root",
            docker_labels={"version": "1.0"}
        )
        self.assertEqual(container.cpu, 1024)
        self.assertEqual(container.memory, 2048)
        self.assertEqual(container.working_directory, "/app")
        self.assertEqual(container.environment["ENV"], "prod")
        self.assertEqual(container.port_mappings[0]["containerPort"], 8080)
        self.assertTrue(container.privileged)


class TestVolumeDefinition(unittest.TestCase):
    """Test VolumeDefinition dataclass"""

    def test_volume_definition_creation(self):
        volume = VolumeDefinition(name="data-volume")
        self.assertEqual(volume.name, "data-volume")
        self.assertEqual(volume.host_path, "")

    def test_volume_definition_with_host_path(self):
        volume = VolumeDefinition(
            name="host-volume",
            host_path="/mnt/data"
        )
        self.assertEqual(volume.host_path, "/mnt/data")


class TestTaskDefinition(unittest.TestCase):
    """Test TaskDefinition dataclass"""

    def test_task_definition_defaults(self):
        container = ContainerDefinition(name="test", image="nginx")
        task_def = TaskDefinition(
            family="my-task",
            containers=[container]
        )
        self.assertEqual(task_def.family, "my-task")
        self.assertEqual(len(task_def.containers), 1)
        self.assertEqual(task_def.network_mode, NetworkMode.AWS_VPC)
        self.assertEqual(task_def.cpu, "256")
        self.assertEqual(task_def.memory, "512")
        self.assertEqual(task_def.volumes, [])

    def test_task_definition_full(self):
        container = ContainerDefinition(name="app", image="my-app:latest")
        volume = VolumeDefinition(name="data")
        task_def = TaskDefinition(
            family="full-task",
            containers=[container],
            volumes=[volume],
            network_mode=NetworkMode.AWS_VPC,
            execution_role_arn="arn:aws:iam::123456789:role/ecsTaskExecutionRole",
            task_role_arn="arn:aws:iam::123456789:role/ecsTaskRole",
            cpu="1024",
            memory="2048",
            requires_compatibilities=[LaunchType.FARGATE],
            tags={"Environment": "production"}
        )
        self.assertEqual(task_def.family, "full-task")
        self.assertEqual(task_def.cpu, "1024")
        self.assertEqual(task_def.memory, "2048")
        self.assertIn(LaunchType.FARGATE, task_def.requires_compatibilities)
        self.assertEqual(task_def.tags["Environment"], "production")


class TestServiceDiscoveryConfig(unittest.TestCase):
    """Test ServiceDiscoveryConfig dataclass"""

    def test_service_discovery_config_creation(self):
        config = ServiceDiscoveryConfig(
            name="my-service",
            dns_config={"dns_records": [{"type": "A", "ttl": 300}]}
        )
        self.assertEqual(config.name, "my-service")
        self.assertEqual(config.dns_config["dns_records"][0]["type"], "A")


class TestAutoScalingConfig(unittest.TestCase):
    """Test AutoScalingConfig dataclass"""

    def test_auto_scaling_config_defaults(self):
        config = AutoScalingConfig()
        self.assertEqual(config.min_capacity, 1)
        self.assertEqual(config.max_capacity, 10)
        self.assertEqual(config.target_cpu_utilization, 70.0)
        self.assertEqual(config.target_memory_utilization, 80.0)
        self.assertEqual(config.scale_in_cooldown, 300)
        self.assertEqual(config.scale_out_cooldown, 300)

    def test_auto_scaling_config_full(self):
        config = AutoScalingConfig(
            min_capacity=2,
            max_capacity=20,
            target_cpu_utilization=60.0,
            target_memory_utilization=75.0,
            scale_in_cooldown=600,
            scale_out_cooldown=600
        )
        self.assertEqual(config.min_capacity, 2)
        self.assertEqual(config.max_capacity, 20)
        self.assertEqual(config.target_cpu_utilization, 60.0)


class TestLoadBalancerConfig(unittest.TestCase):
    """Test LoadBalancerConfig dataclass"""

    def test_load_balancer_config_creation(self):
        config = LoadBalancerConfig(
            target_group_arn="arn:aws:elasticloadbalancing:us-east-1:123456789:targetgroup/my-tg/abc",
            container_name="web",
            container_port=8080
        )
        self.assertEqual(config.container_name, "web")
        self.assertEqual(config.container_port, 8080)
        self.assertEqual(config.load_balancer_type, "application")


class TestClusterInfo(unittest.TestCase):
    """Test ClusterInfo dataclass"""

    def test_cluster_info_creation(self):
        cluster = ClusterInfo(
            cluster_arn="arn:aws:ecs:us-east-1:123456789:cluster/my-cluster",
            cluster_name="my-cluster",
            status=ClusterStatus.ACTIVE
        )
        self.assertEqual(cluster.cluster_name, "my-cluster")
        self.assertEqual(cluster.status, ClusterStatus.ACTIVE)
        self.assertEqual(cluster.registered_container_instances, 0)
        self.assertEqual(cluster.running_tasks, 0)


class TestTaskInfo(unittest.TestCase):
    """Test TaskInfo dataclass"""

    def test_task_info_creation(self):
        task = TaskInfo(
            task_arn="arn:aws:ecs:us-east-1:123456789:task/my-cluster/task-id",
            task_definition_arn="arn:aws:ecs:us-east-1:123456789:task-definition/my-task:1",
            cluster_arn="arn:aws:ecs:us-east-1:123456789:cluster/my-cluster",
            status=TaskStatus.RUNNING
        )
        self.assertEqual(task.status, TaskStatus.RUNNING)
        self.assertEqual(task.desired_status, "RUNNING")
        self.assertEqual(task.launch_type, LaunchType.FARGATE)


class TestServiceInfo(unittest.TestCase):
    """Test ServiceInfo dataclass"""

    def test_service_info_creation(self):
        service = ServiceInfo(
            service_arn="arn:aws:ecs:us-east-1:123456789:service/my-cluster/my-service",
            service_name="my-service",
            cluster_arn="arn:aws:ecs:us-east-1:123456789:cluster/my-cluster",
            status=ServiceStatus.ACTIVE
        )
        self.assertEqual(service.service_name, "my-service")
        self.assertEqual(service.status, ServiceStatus.ACTIVE)
        self.assertEqual(service.desired_count, 1)
        self.assertEqual(service.running_count, 0)


class TestECSIntegration(unittest.TestCase):
    """Test ECSIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ecs_client = MagicMock()
        self.mock_elbv2_client = MagicMock()
        self.mock_iam_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        self.mock_servicediscovery_client = MagicMock()
        self.mock_application_autoscaling_client = MagicMock()

        # Create integration instance with mocked clients
        self.integration = ECSIntegration(region="us-east-1", cluster_name="test-cluster")
        self.integration.ecs_client = self.mock_ecs_client
        self.integration.elbv2_client = self.mock_elbv2_client
        self.integration.iam_client = self.mock_iam_client
        self.integration.cloudwatch_client = self.mock_cloudwatch_client
        self.integration.servicediscovery_client = self.mock_servicediscovery_client
        self.integration.application_autoscaling_client = self.mock_application_autoscaling_client

    def test_initialization(self):
        """Test ECSIntegration initialization"""
        integration = ECSIntegration(region="us-west-2", cluster_name="my-cluster")
        self.assertEqual(integration.region, "us-west-2")
        self.assertEqual(integration.cluster_name, "my-cluster")

    def test_create_cluster_success(self):
        """Test successful cluster creation"""
        self.mock_ecs_client.create_cluster.return_value = {
            "cluster": {
                "clusterArn": "arn:aws:ecs:us-east-1:123456789:cluster/test-cluster",
                "clusterName": "test-cluster",
                "status": "ACTIVE",
                "registeredContainerInstancesCount": 0,
                "runningTasksCount": 0,
                "pendingTasksCount": 0,
                "activeServicesCount": 0,
                "statistics": [],
                "settings": [{"name": "containerInsights", "value": "enabled"}],
                "capacityProviders": ["FARGATE"],
                "defaultCapacityProviderStrategy": []
            }
        }

        result = self.integration.create_cluster(
            cluster_name="test-cluster",
            settings={"containerInsights": True},
            capacity_providers=["FARGATE"]
        )

        self.assertEqual(result.cluster_name, "test-cluster")
        self.assertEqual(result.status, ClusterStatus.ACTIVE)
        self.mock_ecs_client.create_cluster.assert_called_once()

    def test_create_cluster_with_tags(self):
        """Test cluster creation with tags"""
        self.mock_ecs_client.create_cluster.return_value = {
            "cluster": {
                "clusterArn": "arn:aws:ecs:us-east-1:123456789:cluster/tagged-cluster",
                "clusterName": "tagged-cluster",
                "status": "ACTIVE",
                "registeredContainerInstancesCount": 0,
                "runningTasksCount": 0,
                "pendingTasksCount": 0,
                "activeServicesCount": 0,
                "statistics": [],
                "settings": [],
                "capacityProviders": [],
                "defaultCapacityProviderStrategy": []
            }
        }

        result = self.integration.create_cluster(
            cluster_name="tagged-cluster",
            tags={"Environment": "production", "Application": "my-app"}
        )

        self.assertEqual(result.cluster_name, "tagged-cluster")
        call_kwargs = self.mock_ecs_client.create_cluster.call_args[1]
        self.assertEqual(call_kwargs["tags"][0]["key"], "Environment")
        self.assertEqual(call_kwargs["tags"][0]["value"], "production")

    def test_describe_clusters(self):
        """Test describing clusters"""
        self.mock_ecs_client.describe_clusters.return_value = {
            "clusters": [
                {
                    "clusterArn": "arn:aws:ecs:us-east-1:123456789:cluster/my-cluster",
                    "clusterName": "my-cluster",
                    "status": "ACTIVE",
                    "registeredContainerInstancesCount": 3,
                    "runningTasksCount": 10,
                    "pendingTasksCount": 2,
                    "activeServicesCount": 5,
                    "statistics": [],
                    "settings": [{"name": "containerInsights", "value": "enabled"}],
                    "capacityProviders": ["FARGATE", "FARGATE_SPOT"],
                    "defaultCapacityProviderStrategy": [
                        {"base": 1, "weight": 100, "capacityProvider": "FARGATE"}
                    ]
                }
            ]
        }

        result = self.integration.describe_clusters(clusters=["my-cluster"])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].cluster_name, "my-cluster")
        self.assertEqual(result[0].running_tasks, 10)
        self.assertEqual(result[0].active_services, 5)
        self.assertEqual(result[0].capacity_providers, ["FARGATE", "FARGATE_SPOT"])

    def test_delete_cluster(self):
        """Test cluster deletion"""
        self.mock_ecs_client.delete_cluster.return_value = {}

        result = self.integration.delete_cluster(cluster="to-delete")

        self.assertTrue(result)
        self.mock_ecs_client.delete_cluster.assert_called_once_with(cluster="to-delete")

    def test_register_task_definition(self):
        """Test task definition registration"""
        container_def = ContainerDefinition(
            name="web",
            image="nginx:latest",
            cpu=256,
            memory=512,
            port_mappings=[{"containerPort": 80, "hostPort": 80}]
        )

        self.mock_ecs_client.register_task_definition.return_value = {
            "taskDefinition": {
                "taskDefinitionArn": "arn:aws:ecs:us-east-1:123456789:task-definition/my-task:1",
                "family": "my-task",
                "revision": 1,
                "containerDefinitions": [
                    {
                        "name": "web",
                        "image": "nginx:latest",
                        "cpu": 256,
                        "memory": 512,
                        "portMappings": [{"containerPort": 80, "hostPort": 80}]
                    }
                ],
                "volumes": [],
                "status": "ACTIVE",
                "compatibilities": ["EC2", "FARGATE"],
                "requiresCompatibilities": ["EC2", "FARGATE"]
            }
        }

        task_def = TaskDefinition(
            family="my-task",
            containers=[container_def]
        )
        result = self.integration.register_task_definition(task_def)

        self.assertEqual(result["family"], "my-task")
        self.assertEqual(result["revision"], 1)
        self.assertEqual(result["status"], "ACTIVE")

    def test_list_task_definitions(self):
        """Test listing task definitions - returns list of ARNs"""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {"taskDefinitionArns": [
                "arn:aws:ecs:us-east-1:123456789:task-definition/my-task:1",
                "arn:aws:ecs:us-east-1:123456789:task-definition/my-task:2"
            ]}
        ]
        self.mock_ecs_client.get_paginator.return_value = mock_paginator

        result = self.integration.list_task_definitions(family_prefix="my-task")

        self.assertEqual(len(result), 2)
        self.mock_ecs_client.get_paginator.assert_called_with("list_task_definitions")

    def test_describe_task_definition(self):
        """Test describing a task definition"""
        self.mock_ecs_client.describe_task_definition.return_value = {
            "taskDefinition": {
                "taskDefinitionArn": "arn:aws:ecs:us-east-1:123456789:task-definition/my-task:1",
                "family": "my-task",
                "revision": 1,
                "containerDefinitions": [
                    {"name": "web", "image": "nginx:latest", "cpu": 256, "memory": 512}
                ],
                "volumes": [],
                "status": "ACTIVE",
                "compatibilities": ["FARGATE"],
                "requiresCompatibilities": ["FARGATE"],
                "cpu": "256",
                "memory": "512"
            }
        }

        result = self.integration.describe_task_definition(task_definition="my-task:1")

        self.assertEqual(result["family"], "my-task")
        self.assertEqual(result["revision"], 1)
        self.assertEqual(result["containerDefinitions"][0]["name"], "web")

    def test_deregister_task_definition(self):
        """Test task definition deregistration - returns bool"""
        self.mock_ecs_client.deregister_task_definition.return_value = {}

        result = self.integration.deregister_task_definition(task_definition="my-task:1")

        self.assertTrue(result)
        self.mock_ecs_client.deregister_task_definition.assert_called_once_with(
            taskDefinition="my-task:1"
        )

    def test_run_task(self):
        """Test running a task"""
        self.mock_ecs_client.run_task.return_value = {
            "tasks": [
                {
                    "taskArn": "arn:aws:ecs:us-east-1:123456789:task/test-cluster/task-id-1",
                    "taskDefinitionArn": "arn:aws:ecs:us-east-1:123456789:task-definition/my-task:1",
                    "clusterArn": "arn:aws:ecs:us-east-1:123456789:cluster/test-cluster",
                    "desiredStatus": "RUNNING",
                    "lastStatus": "RUNNING",
                    "launchType": "FARGATE",
                    "containerInstanceArn": "",
                    "startedBy": "ecs-integration"
                }
            ],
            "failures": []
        }

        result = self.integration.run_task(
            task_definition="my-task:1",
            cluster="test-cluster",
            count=1,
            started_by="ecs-integration"
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, TaskStatus.RUNNING)
        self.assertEqual(result[0].launch_type, LaunchType.FARGATE)

    def test_list_tasks(self):
        """Test listing tasks - returns list of ARNs"""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {"taskArns": [
                "arn:aws:ecs:us-east-1:123456789:task/test-cluster/task-id-1",
                "arn:aws:ecs:us-east-1:123456789:task/test-cluster/task-id-2"
            ]}
        ]
        self.mock_ecs_client.get_paginator.return_value = mock_paginator

        result = self.integration.list_tasks(cluster="test-cluster")

        self.assertEqual(len(result), 2)

    def test_stop_task(self):
        """Test stopping a task"""
        self.mock_ecs_client.stop_task.return_value = {
            "task": {
                "taskArn": "arn:aws:ecs:us-east-1:123456789:task/test-cluster/task-id-1",
                "taskDefinitionArn": "arn:aws:ecs:us-east-1:123456789:task-definition/my-task:1",
                "clusterArn": "arn:aws:ecs:us-east-1:123456789:cluster/test-cluster",
                "desiredStatus": "STOPPED",
                "lastStatus": "STOPPING",
                "launchType": "FARGATE",
                "containerInstanceArn": "",
                "startedBy": ""
            }
        }

        result = self.integration.stop_task(
            task="task-id-1",
            cluster="test-cluster",
            reason="User requested stop"
        )

        self.assertEqual(result.desired_status, "STOPPED")
        self.mock_ecs_client.stop_task.assert_called_once()

    def test_create_service(self):
        """Test service creation"""
        self.mock_ecs_client.create_service.return_value = {
            "service": {
                "serviceArn": "arn:aws:ecs:us-east-1:123456789:service/test-cluster/my-service",
                "serviceName": "my-service",
                "clusterArn": "arn:aws:ecs:us-east-1:123456789:cluster/test-cluster",
                "status": "ACTIVE",
                "desiredCount": 2,
                "runningCount": 0,
                "pendingCount": 0,
                "taskDefinition": "arn:aws:ecs:us-east-1:123456789:task-definition/my-task:1",
                "deploymentController": {"type": "ECS"},
                "deployments": [],
                "loadBalancers": []
            }
        }

        result = self.integration.create_service(
            service_name="my-service",
            task_definition="my-task:1",
            cluster="test-cluster",
            desired_count=2
        )

        self.assertEqual(result.service_name, "my-service")
        self.assertEqual(result.desired_count, 2)
        self.assertEqual(result.status, ServiceStatus.ACTIVE)

    def test_update_service(self):
        """Test service update"""
        self.mock_ecs_client.update_service.return_value = {
            "service": {
                "serviceArn": "arn:aws:ecs:us-east-1:123456789:service/test-cluster/my-service",
                "serviceName": "my-service",
                "clusterArn": "arn:aws:ecs:us-east-1:123456789:cluster/test-cluster",
                "status": "ACTIVE",
                "desiredCount": 5,
                "runningCount": 3,
                "pendingCount": 2,
                "taskDefinition": "arn:aws:ecs:us-east-1:123456789:task-definition/my-task:2",
                "deploymentController": {"type": "ECS"},
                "deployments": [],
                "loadBalancers": []
            }
        }

        result = self.integration.update_service(
            service="my-service",
            cluster="test-cluster",
            desired_count=5,
            task_definition="my-task:2"
        )

        self.assertEqual(result.desired_count, 5)
        self.assertEqual(result.running_count, 3)

    def test_delete_service(self):
        """Test service deletion - returns bool"""
        self.mock_ecs_client.delete_service.return_value = {}

        result = self.integration.delete_service(
            service="to-delete",
            cluster="test-cluster"
        )

        self.assertTrue(result)

    def test_list_services(self):
        """Test listing services - returns list of ARNs"""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {"serviceArns": [
                "arn:aws:ecs:us-east-1:123456789:service/test-cluster/service-1",
                "arn:aws:ecs:us-east-1:123456789:service/test-cluster/service-2"
            ]}
        ]
        self.mock_ecs_client.get_paginator.return_value = mock_paginator

        result = self.integration.list_services(cluster="test-cluster")

        self.assertEqual(len(result), 2)

    def test_describe_services(self):
        """Test describing services"""
        self.mock_ecs_client.describe_services.return_value = {
            "services": [
                {
                    "serviceArn": "arn:aws:ecs:us-east-1:123456789:service/test-cluster/web",
                    "serviceName": "web",
                    "clusterArn": "arn:aws:ecs:us-east-1:123456789:cluster/test-cluster",
                    "status": "ACTIVE",
                    "desiredCount": 3,
                    "runningCount": 3,
                    "pendingCount": 0,
                    "taskDefinition": "arn:aws:ecs:us-east-1:123456789:task-definition/web-task:5",
                    "deploymentController": {"type": "ECS"},
                    "deployments": [],
                    "loadBalancers": [
                        {
                            "targetGroupArn": "arn:aws:elasticloadbalancing:us-east-1:123456789:targetgroup/my-tg/abc",
                            "containerName": "web",
                            "containerPort": 80
                        }
                    ]
                }
            ]
        }

        result = self.integration.describe_services(
            services=["web"],
            cluster="test-cluster"
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].service_name, "web")
        self.assertEqual(result[0].load_balancers[0]["containerPort"], 80)


class TestECSIntegrationAutoScaling(unittest.TestCase):
    """Test ECSIntegration auto scaling methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ecs_client = MagicMock()
        self.mock_application_autoscaling_client = MagicMock()

        self.integration = ECSIntegration(region="us-east-1", cluster_name="test-cluster")
        self.integration.ecs_client = self.mock_ecs_client
        self.integration.application_autoscaling_client = self.mock_application_autoscaling_client

    def test_configure_service_auto_scaling(self):
        """Test configuring service auto scaling"""
        self.mock_application_autoscaling_client.register_scalable_target.return_value = {}
        self.mock_application_autoscaling_client.put_scaling_policy.return_value = {
            "PolicyARN": "arn:aws:autoscaling:us-east-1:123456789:scalingPolicy:abc:service/my-service/TargetPolicy:scalable-target"
        }

        config = AutoScalingConfig(
            min_capacity=2,
            max_capacity=10,
            target_cpu_utilization=70.0
        )
        result = self.integration.configure_service_auto_scaling(
            service="my-service",
            cluster="test-cluster",
            config=config
        )

        self.assertTrue(result)
        self.mock_application_autoscaling_client.register_scalable_target.assert_called_once()
        self.mock_application_autoscaling_client.put_scaling_policy.assert_called()

    def test_deregister_scalable_target(self):
        """Test deregistering scalable target"""
        self.mock_application_autoscaling_client.deregister_scalable_target.return_value = {}

        result = self.integration.deregister_scalable_target(
            service="my-service",
            cluster="test-cluster"
        )

        self.assertTrue(result)

    def test_delete_scaling_policies(self):
        """Test deleting scaling policies"""
        self.mock_application_autoscaling_client.delete_scaling_policy.return_value = {}

        result = self.integration.delete_scaling_policies(
            service="my-service",
            cluster="test-cluster"
        )

        self.assertTrue(result)


class TestECSIntegrationLoadBalancer(unittest.TestCase):
    """Test ECSIntegration load balancer methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_elbv2_client = MagicMock()
        self.mock_ecs_client = MagicMock()

        self.integration = ECSIntegration(region="us-east-1", cluster_name="test-cluster")
        self.integration.elbv2_client = self.mock_elbv2_client
        self.integration.ecs_client = self.mock_ecs_client

    def test_create_load_balancer(self):
        """Test creating load balancer"""
        self.mock_elbv2_client.create_load_balancer.return_value = {
            "LoadBalancers": [
                {
                    "LoadBalancerArn": "arn:aws:elasticloadbalancing:us-east-1:123456789:loadbalancer/my-lb",
                    "LoadBalancerName": "my-lb",
                    "Type": "application",
                    "DNSName": "my-lb-123456789.us-east-1.elb.amazonaws.com",
                    "VpcId": "vpc-12345678",
                    "State": {"Code": "active"},
                    "AvailabilityZones": []
                }
            ]
        }

        result = self.integration.create_load_balancer(
            name="my-lb",
            subnets=["subnet-12345678", "subnet-87654321"],
            security_groups=["sg-12345678"]
        )

        self.assertEqual(result["LoadBalancerName"], "my-lb")
        self.assertEqual(result["Type"], "application")

    def test_create_target_group(self):
        """Test creating target group"""
        self.mock_elbv2_client.create_target_group.return_value = {
            "TargetGroups": [
                {
                    "TargetGroupArn": "arn:aws:elasticloadbalancing:us-east-1:123456789:targetgroup/my-tg/abc",
                    "TargetGroupName": "my-tg",
                    "Protocol": "HTTP",
                    "Port": 80,
                    "VpcId": "vpc-12345678",
                    "HealthCheckProtocol": "HTTP",
                    "HealthCheckPort": "traffic-port",
                    "TargetType": "ip"
                }
            ]
        }

        result = self.integration.create_target_group(
            name="my-tg",
            port=80,
            protocol="HTTP",
            vpc_id="vpc-12345678",
            health_check_path="/health"
        )

        self.assertEqual(result["TargetGroupName"], "my-tg")
        self.assertEqual(result["TargetType"], "ip")

    def test_delete_target_group(self):
        """Test deleting target group"""
        self.mock_elbv2_client.delete_target_group.return_value = {}

        result = self.integration.delete_target_group(
            target_group_arn="arn:aws:elasticloadbalancing:us-east-1:123456789:targetgroup/my-tg/abc"
        )

        self.assertTrue(result)

    def test_register_targets(self):
        """Test registering targets"""
        self.mock_elbv2_client.register_targets.return_value = {}

        result = self.integration.register_targets(
            target_group_arn="arn:aws:elasticloadbalancing:us-east-1:123456789:targetgroup/my-tg/abc",
            targets=[{"Id": "10.0.0.1", "Port": 80}]
        )

        self.assertTrue(result)


class TestECSIntegrationCloudWatch(unittest.TestCase):
    """Test ECSIntegration CloudWatch methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudwatch_client = MagicMock()

        self.integration = ECSIntegration(region="us-east-1", cluster_name="test-cluster")
        self.integration.cloudwatch_client = self.mock_cloudwatch_client

    def test_get_metric_statistics(self):
        """Test getting metric statistics"""
        self.mock_cloudwatch_client.get_metric_statistics.return_value = {
            "Label": "CPUUtilization",
            "Datapoints": [
                {"Timestamp": datetime(2024, 1, 1, 12, 0), "Average": 65.5, "Unit": "Percent"},
                {"Timestamp": datetime(2024, 1, 1, 12, 5), "Average": 70.2, "Unit": "Percent"}
            ]
        }

        result = self.integration.get_metric_statistics(
            namespace="AWS/ECS",
            metric_name="CPUUtilization",
            dimensions=[{"Name": "ClusterName", "Value": "test-cluster"}],
            start_time=datetime(2024, 1, 1, 11, 0),
            end_time=datetime(2024, 1, 1, 12, 0),
            period=300
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["Average"], 65.5)

    def test_put_cluster_metrics(self):
        """Test putting cluster metrics"""
        self.mock_ecs_client = MagicMock()
        self.integration.ecs_client = self.mock_ecs_client
        self.mock_ecs_client.put_cluster_capacity_settings.return_value = {}

        result = self.integration.put_cluster_metrics(cluster="test-cluster")

        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()

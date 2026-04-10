"""
Tests for workflow_aws_eks module
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

# Create mock boto3 module before importing workflow_aws_eks
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
from src.workflow_aws_eks import (
    EKSIntegration,
    ClusterStatus,
    NodeGroupStatus,
    FargateProfileStatus,
    AddonStatus,
    IpFamily,
    CapacityType,
    RemoteAccessPolicy,
    VpcConfig,
    KubernetesResource,
    NodeGroupConfig,
    FargateProfileConfig,
    AddonConfig,
    ClusterInfo,
    NodeGroupInfo,
    HelmChart,
)


class TestClusterStatus(unittest.TestCase):
    """Test ClusterStatus enum"""

    def test_cluster_status_values(self):
        self.assertEqual(ClusterStatus.CREATING.value, "CREATING")
        self.assertEqual(ClusterStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(ClusterStatus.DELETING.value, "DELETING")
        self.assertEqual(ClusterStatus.FAILED.value, "FAILED")
        self.assertEqual(ClusterStatus.UPDATING.value, "UPDATING")


class TestNodeGroupStatus(unittest.TestCase):
    """Test NodeGroupStatus enum"""

    def test_node_group_status_values(self):
        self.assertEqual(NodeGroupStatus.CREATING.value, "CREATING")
        self.assertEqual(NodeGroupStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(NodeGroupStatus.UPDATING.value, "UPDATING")
        self.assertEqual(NodeGroupStatus.DELETING.value, "DELETING")
        self.assertEqual(NodeGroupStatus.CREATE_FAILED.value, "CREATE_FAILED")
        self.assertEqual(NodeGroupStatus.DELETE_FAILED.value, "DELETE_FAILED")


class TestFargateProfileStatus(unittest.TestCase):
    """Test FargateProfileStatus enum"""

    def test_fargate_profile_status_values(self):
        self.assertEqual(FargateProfileStatus.CREATING.value, "CREATING")
        self.assertEqual(FargateProfileStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(FargateProfileStatus.DELETING.value, "DELETING")
        self.assertEqual(FargateProfileStatus.CREATE_FAILED.value, "CREATE_FAILED")
        self.assertEqual(FargateProfileStatus.DELETE_FAILED.value, "DELETE_FAILED")


class TestAddonStatus(unittest.TestCase):
    """Test AddonStatus enum"""

    def test_addon_status_values(self):
        self.assertEqual(AddonStatus.CREATING.value, "CREATING")
        self.assertEqual(AddonStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(AddonStatus.UPDATING.value, "UPDATING")
        self.assertEqual(AddonStatus.DELETING.value, "DELETING")
        self.assertEqual(AddonStatus.CREATE_FAILED.value, "CREATE_FAILED")
        self.assertEqual(AddonStatus.DELETE_FAILED.value, "DELETE_FAILED")


class TestIpFamily(unittest.TestCase):
    """Test IpFamily enum"""

    def test_ip_family_values(self):
        self.assertEqual(IpFamily.IPV4.value, "ipv4")
        self.assertEqual(IpFamily.IPV6.value, "ipv6")


class TestCapacityType(unittest.TestCase):
    """Test CapacityType enum"""

    def test_capacity_type_values(self):
        self.assertEqual(CapacityType.ON_DEMAND.value, "OnDemand")
        self.assertEqual(CapacityType.SPOT.value, "Spot")


class TestRemoteAccessPolicy(unittest.TestCase):
    """Test RemoteAccessPolicy enum"""

    def test_remote_access_policy_values(self):
        self.assertEqual(RemoteAccessPolicy.Ec2SecurityGroup.value, "Ec2SecurityGroup")


class TestVpcConfig(unittest.TestCase):
    """Test VpcConfig dataclass"""

    def test_vpc_config_creation(self):
        vpc = VpcConfig(
            subnet_ids=["subnet-1", "subnet-2"],
            security_group_ids=["sg-1"],
            vpc_id="vpc-123"
        )
        self.assertEqual(len(vpc.subnet_ids), 2)
        self.assertEqual(vpc.vpc_id, "vpc-123")
        self.assertTrue(vpc.endpoint_public_access)

    def test_vpc_config_defaults(self):
        vpc = VpcConfig(subnet_ids=["subnet-1"])
        self.assertEqual(vpc.endpoint_public_access, True)
        self.assertEqual(vpc.endpoint_private_access, False)


class TestKubernetesResource(unittest.TestCase):
    """Test KubernetesResource dataclass"""

    def test_kubernetes_resource_creation(self):
        resource = KubernetesResource(
            kind="Deployment",
            api_version="apps/v1",
            metadata={"name": "nginx", "namespace": "default"},
            spec={"replicas": 3}
        )
        self.assertEqual(resource.kind, "Deployment")
        self.assertEqual(resource.spec["replicas"], 3)


class TestNodeGroupConfig(unittest.TestCase):
    """Test NodeGroupConfig dataclass"""

    def test_node_group_config_defaults(self):
        config = NodeGroupConfig(
            name="worker-group",
            instance_types=["t3.medium"]
        )
        self.assertEqual(config.name, "worker-group")
        self.assertEqual(config.min_size, 1)
        self.assertEqual(config.max_size, 3)
        self.assertEqual(config.desired_size, 2)
        self.assertEqual(config.capacity_type, CapacityType.ON_DEMAND)

    def test_node_group_config_with_labels(self):
        config = NodeGroupConfig(
            name="worker-group",
            instance_types=["t3.medium"],
            labels={"node-type": "worker", "environment": "prod"}
        )
        self.assertEqual(config.labels["node-type"], "worker")

    def test_node_group_config_with_taints(self):
        config = NodeGroupConfig(
            name="worker-group",
            instance_types=["t3.medium"],
            taints=[{"key": "dedicated", "value": "gpu", "effect": "NoSchedule"}]
        )
        self.assertEqual(len(config.taints), 1)


class TestFargateProfileConfig(unittest.TestCase):
    """Test FargateProfileConfig dataclass"""

    def test_fargate_profile_config_creation(self):
        config = FargateProfileConfig(
            profile_name="fp-default",
            pod_execution_role_arn="arn:aws:iam::123456789:role/eks-fargate"
        )
        self.assertEqual(config.profile_name, "fp-default")

    def test_fargate_profile_config_with_selectors(self):
        config = FargateProfileConfig(
            profile_name="fp-default",
            pod_execution_role_arn="arn:aws:iam::123456789:role/eks-fargate",
            selectors=[
                {"namespace": "default", "labels": {"app": "nginx"}},
                {"namespace": "production"}
            ]
        )
        self.assertEqual(len(config.selectors), 2)


class TestAddonConfig(unittest.TestCase):
    """Test AddonConfig dataclass"""

    def test_addon_config_creation(self):
        addon = AddonConfig(
            addon_name="vpc-cni",
            addon_version="v1.15.0"
        )
        self.assertEqual(addon.addon_name, "vpc-cni")
        self.assertEqual(addon.addon_version, "v1.15.0")

    def test_addon_config_defaults(self):
        addon = AddonConfig(addon_name="core-dns")
        self.assertTrue(addon.preserve)


class TestClusterInfo(unittest.TestCase):
    """Test ClusterInfo dataclass"""

    def test_cluster_info_creation(self):
        cluster = ClusterInfo(
            arn="arn:aws:eks:us-east-1:123456789:cluster/test-cluster",
            name="test-cluster",
            status=ClusterStatus.ACTIVE,
            version="1.28"
        )
        self.assertEqual(cluster.name, "test-cluster")
        self.assertEqual(cluster.status, ClusterStatus.ACTIVE)
        self.assertEqual(cluster.version, "1.28")


class TestNodeGroupInfo(unittest.TestCase):
    """Test NodeGroupInfo dataclass"""

    def test_node_group_info_creation(self):
        ng = NodeGroupInfo(
            nodegroup_name="workers",
            cluster_name="test-cluster",
            status=NodeGroupStatus.ACTIVE
        )
        self.assertEqual(ng.nodegroup_name, "workers")
        self.assertEqual(ng.cluster_name, "test-cluster")


class TestHelmChart(unittest.TestCase):
    """Test HelmChart dataclass"""

    def test_helm_chart_defaults(self):
        chart = HelmChart(
            chart_name="nginx",
            release_name="my-nginx"
        )
        self.assertEqual(chart.namespace, "default")
        self.assertEqual(chart.timeout, 300)

    def test_helm_chart_with_values(self):
        chart = HelmChart(
            chart_name="nginx",
            release_name="my-nginx",
            namespace="production",
            values={"replicas": 5}
        )
        self.assertEqual(chart.values["replicas"], 5)


class TestEKSIntegration(unittest.TestCase):
    """Test EKSIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_eks = MagicMock()
        self.mock_ec2 = MagicMock()
        self.mock_iam = MagicMock()
        self.mock_cloudwatch = MagicMock()

        # Create integration instance with mocked clients
        self.integration = EKSIntegration(region="us-east-1")
        self.integration.eks_client = self.mock_eks
        self.integration.ec2_client = self.mock_ec2
        self.integration.iam_client = self.mock_iam
        self.integration.cloudwatch_client = self.mock_cloudwatch

    def test_create_iam_role_for_eks_cluster(self):
        """Test creating IAM role for EKS cluster"""
        mock_response = {
            'Role': {
                'RoleName': 'eks-cluster-role',
                'Arn': 'arn:aws:iam::123456789:role/eks-cluster-role'
            }
        }
        self.mock_iam.create_role.return_value = mock_response
        self.mock_iam.attach_role_policy.return_value = {}

        result = self.integration.create_iam_role_for_eks_cluster("eks-cluster-role")

        self.assertEqual(result, 'arn:aws:iam::123456789:role/eks-cluster-role')
        self.mock_iam.create_role.assert_called_once()
        self.mock_iam.attach_role_policy.assert_called_once()

    def test_create_iam_role_for_node_group(self):
        """Test creating IAM role for node group"""
        mock_response = {
            'Role': {
                'RoleName': 'node-group-role',
                'Arn': 'arn:aws:iam::123456789:role/node-group-role'
            }
        }
        self.mock_iam.create_role.return_value = mock_response
        self.mock_iam.attach_role_policy.return_value = {}

        result = self.integration.create_iam_role_for_node_group("node-group-role")

        self.assertEqual(result, 'arn:aws:iam::123456789:role/node-group-role')
        # Should attach 3 policies for node group
        self.assertEqual(self.mock_iam.attach_role_policy.call_count, 3)

    def test_list_clusters(self):
        """Test listing EKS clusters"""
        mock_response = {
            'clusters': [
                'dev-cluster',
                'staging-cluster',
                'prod-cluster'
            ]
        }
        self.mock_eks.list_clusters.return_value = mock_response

        result = self.integration.list_clusters()

        self.assertEqual(len(result), 3)
        self.assertIn('prod-cluster', result)

    def test_list_node_groups(self):
        """Test listing node groups"""
        mock_response = {
            'nodegroups': ['workers', 'gpu-workers']
        }
        self.mock_eks.list_nodegroups.return_value = mock_response

        result = self.integration.list_node_groups("test-cluster")

        self.assertEqual(len(result), 2)

    def test_list_fargate_profiles(self):
        """Test listing Fargate profiles"""
        mock_response = {
            'fargateProfileNames': ['fp-default', 'fp-production']
        }
        self.mock_eks.list_fargate_profiles.return_value = mock_response

        result = self.integration.list_fargate_profiles("test-cluster")

        self.assertEqual(len(result), 2)

    def test_client_error_handling(self):
        """Test ClientError handling"""
        mock_error = Exception("ResourceNotFound")
        mock_error.response = {'Error': {'Code': 'ResourceNotFoundException'}}
        self.mock_eks.list_clusters.side_effect = mock_error

        with self.assertRaises(Exception):
            self.integration.list_clusters()


class TestEKSIntegrationIntegration(unittest.TestCase):
    """Integration tests for EKSIntegration (with mocked boto3)"""

    @patch('src.workflow_aws_eks.boto3')
    def test_init_with_boto3(self, mock_boto3):
        """Test initialization with boto3"""
        mock_session = MagicMock()
        mock_eks = MagicMock()
        mock_ec2 = MagicMock()
        mock_iam = MagicMock()
        mock_cloudwatch = MagicMock()

        mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [mock_eks, mock_ec2, mock_iam, mock_cloudwatch]

        integration = EKSIntegration(region="us-west-2")

        mock_boto3.Session.assert_called_once()
        self.assertEqual(integration.region, 'us-west-2')

    @patch('src.workflow_aws_eks.boto3')
    def test_init_with_profile(self, mock_boto3):
        """Test initialization with AWS profile"""
        mock_session = MagicMock()

        mock_boto3.Session.return_value = mock_session

        integration = EKSIntegration(region="us-east-1", profile="my-profile")

        self.assertEqual(integration.profile, 'my-profile')


class TestEKSIntegrationClientErrors(unittest.TestCase):
    """Test EKSIntegration error handling"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_eks = MagicMock()
        self.mock_ec2 = MagicMock()
        self.mock_iam = MagicMock()
        self.mock_cloudwatch = MagicMock()

        self.integration = EKSIntegration(region="us-east-1")
        self.integration.eks_client = self.mock_eks
        self.integration.ec2_client = self.mock_ec2
        self.integration.iam_client = self.mock_iam
        self.integration.cloudwatch_client = self.mock_cloudwatch

    def test_client_error_handling(self):
        """Test ClientError handling"""
        mock_error = Exception("ResourceNotFound")
        mock_error.response = {'Error': {'Code': 'ResourceNotFoundException'}}
        self.mock_eks.list_clusters.side_effect = mock_error

        with self.assertRaises(Exception):
            self.integration.list_clusters()


if __name__ == '__main__':
    unittest.main()

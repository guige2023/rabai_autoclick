"""
Tests for workflow_aws_elasticache module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import types

# Create mock boto3 module before importing workflow_aws_elasticache
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

import src.workflow_aws_elasticache as _elasticache_module

if _elasticache_module is not None:
    ElastiCacheIntegration = _elasticache_module.ElastiCacheIntegration
    CacheEngine = _elasticache_module.CacheEngine
    CacheInstanceState = _elasticache_module.CacheInstanceState
    SnapshotState = _elasticache_module.SnapshotState
    ReplicationState = _elasticache_module.ReplicationState
    ServerlessCacheState = _elasticache_module.ServerlessCacheState
    CacheClusterConfig = _elasticache_module.CacheClusterConfig
    CacheNodeInfo = _elasticache_module.CacheNodeInfo
    SnapshotInfo = _elasticache_module.SnapshotInfo
    GlobalReplicationInfo = _elasticache_module.GlobalReplicationInfo
    ServerlessCacheInfo = _elasticache_module.ServerlessCacheInfo


class TestCacheEngine(unittest.TestCase):
    """Test CacheEngine enum"""

    def test_redis_value(self):
        self.assertEqual(CacheEngine.REDIS.value, "redis")

    def test_memcached_value(self):
        self.assertEqual(CacheEngine.MEMCACHED.value, "memcached")

    def test_valkey_value(self):
        self.assertEqual(CacheEngine.VALKEY.value, "valkey")


class TestCacheInstanceState(unittest.TestCase):
    """Test CacheInstanceState enum"""

    def test_states(self):
        self.assertEqual(CacheInstanceState.CREATING.value, "creating")
        self.assertEqual(CacheInstanceState.AVAILABLE.value, "available")
        self.assertEqual(CacheInstanceState.DELETING.value, "deleting")
        self.assertEqual(CacheInstanceState.DELETED.value, "deleted")
        self.assertEqual(CacheInstanceState.MODIFYING.value, "modifying")
        self.assertEqual(CacheInstanceState.REBOOTING.value, "rebooting")
        self.assertEqual(CacheInstanceState.FAILING.value, "failing")
        self.assertEqual(CacheInstanceState.FAILED.value, "failed")


class TestSnapshotState(unittest.TestCase):
    """Test SnapshotState enum"""

    def test_states(self):
        self.assertEqual(SnapshotState.CREATING.value, "creating")
        self.assertEqual(SnapshotState.AVAILABLE.value, "available")
        self.assertEqual(SnapshotState.DELETING.value, "deleting")
        self.assertEqual(SnapshotState.DELETED.value, "deleted")
        self.assertEqual(SnapshotState.FAILED.value, "failed")
        self.assertEqual(SnapshotState.RESTORING.value, "restoring")


class TestReplicationState(unittest.TestCase):
    """Test ReplicationState enum"""

    def test_states(self):
        self.assertEqual(ReplicationState.CREATING.value, "creating")
        self.assertEqual(ReplicationState.ACTIVE.value, "active")
        self.assertEqual(ReplicationState.DELETING.value, "deleting")
        self.assertEqual(ReplicationState.MODIFYING.value, "modifying")


class TestServerlessCacheState(unittest.TestCase):
    """Test ServerlessCacheState enum"""

    def test_states(self):
        self.assertEqual(ServerlessCacheState.CREATING.value, "creating")
        self.assertEqual(ServerlessCacheState.AVAILABLE.value, "available")
        self.assertEqual(ServerlessCacheState.DELETING.value, "deleting")
        self.assertEqual(ServerlessCacheState.FAILED.value, "failed")


class TestCacheClusterConfig(unittest.TestCase):
    """Test CacheClusterConfig dataclass"""

    def test_default_values(self):
        config = CacheClusterConfig(
            cluster_id="test-cluster",
            engine=CacheEngine.REDIS
        )
        self.assertEqual(config.cluster_id, "test-cluster")
        self.assertEqual(config.engine, CacheEngine.REDIS)
        self.assertEqual(config.node_type, "cache.t3.medium")
        self.assertEqual(config.num_nodes, 1)
        self.assertEqual(config.parameter_group_name, "default")
        self.assertEqual(config.subnet_group_name, "default")
        self.assertEqual(config.security_group_ids, [])
        self.assertEqual(config.port, 6379)
        self.assertEqual(config.maintenance_window, "mon:03:00-mon:04:00")
        self.assertEqual(config.snapshot_retention_limit, 0)
        self.assertEqual(config.snapshot_window, "06:00-07:00")
        self.assertEqual(config.auto_minor_version_upgrade, True)
        self.assertEqual(config.at_rest_encryption_enabled, False)
        self.assertEqual(config.transit_encryption_enabled, False)
        self.assertEqual(config.auth_token_enabled, False)
        self.assertEqual(config.tags, {})

    def test_custom_values(self):
        config = CacheClusterConfig(
            cluster_id="custom-cluster",
            engine=CacheEngine.MEMCACHED,
            node_type="cache.m5.large",
            num_nodes=3,
            port=11211,
            tags={"env": "prod"}
        )
        self.assertEqual(config.cluster_id, "custom-cluster")
        self.assertEqual(config.engine, CacheEngine.MEMCACHED)
        self.assertEqual(config.node_type, "cache.m5.large")
        self.assertEqual(config.num_nodes, 3)
        self.assertEqual(config.port, 11211)
        self.assertEqual(config.tags, {"env": "prod"})


class TestCacheNodeInfo(unittest.TestCase):
    """Test CacheNodeInfo dataclass"""

    def test_cache_node_info(self):
        from datetime import datetime
        now = datetime.now()
        node = CacheNodeInfo(
            node_id="node-001",
            node_type="cache.t3.medium",
            status="available",
            port=6379,
            availability_zone="us-east-1a",
            create_time=now,
            endpoint="node-001.cache.amazonaws.com"
        )
        self.assertEqual(node.node_id, "node-001")
        self.assertEqual(node.node_type, "cache.t3.medium")
        self.assertEqual(node.status, "available")
        self.assertEqual(node.port, 6379)
        self.assertEqual(node.availability_zone, "us-east-1a")
        self.assertEqual(node.create_time, now)
        self.assertEqual(node.endpoint, "node-001.cache.amazonaws.com")


class TestSnapshotInfo(unittest.TestCase):
    """Test SnapshotInfo dataclass"""

    def test_snapshot_info(self):
        from datetime import datetime
        now = datetime.now()
        snapshot = SnapshotInfo(
            snapshot_name="my-snapshot",
            cache_cluster_id="my-cluster",
            engine=CacheEngine.REDIS,
            snapshot_status=SnapshotState.AVAILABLE,
            create_time=now,
            node_type="cache.t3.medium",
            num_nodes=1,
            engine_version="7.0"
        )
        self.assertEqual(snapshot.snapshot_name, "my-snapshot")
        self.assertEqual(snapshot.cache_cluster_id, "my-cluster")
        self.assertEqual(snapshot.engine, CacheEngine.REDIS)
        self.assertEqual(snapshot.snapshot_status, SnapshotState.AVAILABLE)
        self.assertEqual(snapshot.node_type, "cache.t3.medium")
        self.assertEqual(snapshot.num_nodes, 1)
        self.assertEqual(snapshot.engine_version, "7.0")


class TestGlobalReplicationInfo(unittest.TestCase):
    """Test GlobalReplicationInfo dataclass"""

    def test_global_replication_info(self):
        info = GlobalReplicationInfo(
            global_replication_group_id="global-group-1",
            global_replication_group_description="Global replication group",
            status=ReplicationState.ACTIVE,
            primary_replication_group_id="primary-group",
            secondary_replication_group_ids=["secondary-group-1", "secondary-group-2"]
        )
        self.assertEqual(info.global_replication_group_id, "global-group-1")
        self.assertEqual(info.status, ReplicationState.ACTIVE)
        self.assertEqual(info.primary_replication_group_id, "primary-group")
        self.assertEqual(len(info.secondary_replication_group_ids), 2)


class TestServerlessCacheInfo(unittest.TestCase):
    """Test ServerlessCacheInfo dataclass"""

    def test_serverless_cache_info(self):
        from datetime import datetime
        now = datetime.now()
        info = ServerlessCacheInfo(
            cache_name="my-serverless-cache",
            status=ServerlessCacheState.AVAILABLE,
            create_time=now,
            endpoint="my-cache.serverless.amazonaws.com",
            port=6379,
            engine=CacheEngine.REDIS
        )
        self.assertEqual(info.cache_name, "my-serverless-cache")
        self.assertEqual(info.status, ServerlessCacheState.AVAILABLE)
        self.assertEqual(info.endpoint, "my-cache.serverless.amazonaws.com")
        self.assertEqual(info.port, 6379)
        self.assertEqual(info.engine, CacheEngine.REDIS)


class TestElastiCacheIntegrationInit(unittest.TestCase):
    """Test ElastiCacheIntegration initialization"""

    def test_default_init(self):
        integration = ElastiCacheIntegration()
        self.assertEqual(integration.region, "us-east-1")
        self.assertIsNone(integration.profile)
        self.assertEqual(integration._clients, {})
        self.assertEqual(integration._resource_cache, {})

    def test_custom_region(self):
        integration = ElastiCacheIntegration(region="us-west-2")
        self.assertEqual(integration.region, "us-west-2")

    def test_custom_profile(self):
        integration = ElastiCacheIntegration(profile="my-profile")
        self.assertEqual(integration.profile, "my-profile")


class TestElastiCacheIntegrationRedis(unittest.TestCase):
    """Test ElastiCacheIntegration Redis cluster methods"""

    def setUp(self):
        self.integration = ElastiCacheIntegration()
        self.mock_client = MagicMock()
        self.integration._clients['elasticache'] = self.mock_client

    def test_create_redis_cluster(self):
        config = CacheClusterConfig(
            cluster_id="redis-cluster",
            engine=CacheEngine.REDIS,
            node_type="cache.t3.medium",
            num_nodes=1
        )
        self.mock_client.create_cache_cluster.return_value = {
            "CacheCluster": {
                "CacheClusterId": "redis-cluster",
                "CacheClusterStatus": "creating"
            }
        }

        result = self.integration.create_redis_cluster(config)

        self.assertEqual(result["cluster_id"], "redis-cluster")
        self.assertEqual(result["status"], "creating")
        self.mock_client.create_cache_cluster.assert_called_once()

    def test_get_redis_cluster(self):
        self.mock_client.describe_cache_clusters.return_value = {
            "CacheClusters": [{
                "CacheClusterId": "redis-cluster",
                "CacheClusterStatus": "available"
            }]
        }

        result = self.integration.get_redis_cluster("redis-cluster")

        self.assertEqual(result["CacheClusterId"], "redis-cluster")
        self.mock_client.describe_cache_clusters.assert_called_once()

    def test_list_redis_clusters(self):
        mock_paginator = MagicMock()
        self.mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"CacheClusters": [{"CacheClusterId": "cluster-1"}]},
            {"CacheClusters": [{"CacheClusterId": "cluster-2"}]}
        ]

        result = self.integration.list_redis_clusters()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["CacheClusterId"], "cluster-1")
        self.assertEqual(result[1]["CacheClusterId"], "cluster-2")

    def test_delete_redis_cluster(self):
        self.integration._clusters["redis-cluster"] = {"CacheClusterId": "redis-cluster"}
        self.mock_client.delete_cache_cluster.return_value = {
            "CacheCluster": {"CacheClusterId": "redis-cluster", "CacheClusterStatus": "deleting"}
        }

        result = self.integration.delete_redis_cluster("redis-cluster")

        self.assertEqual(result["CacheClusterId"], "redis-cluster")
        self.assertNotIn("redis-cluster", self.integration._clusters)

    def test_modify_redis_cluster(self):
        self.mock_client.modify_cache_cluster.return_value = {
            "CacheCluster": {"CacheClusterId": "redis-cluster", "CacheClusterStatus": "modifying"}
        }

        modifications = {"node_type": "cache.m5.large", "num_nodes": 2}
        result = self.integration.modify_redis_cluster("redis-cluster", modifications)

        self.assertEqual(result["CacheClusterId"], "redis-cluster")
        self.mock_client.modify_cache_cluster.assert_called_once()

    def test_reboot_redis_cluster(self):
        self.mock_client.reboot_cache_cluster.return_value = {}

        result = self.integration.reboot_redis_cluster("redis-cluster")

        self.assertTrue(result)
        self.mock_client.reboot_cache_cluster.assert_called_once()


class TestElastiCacheIntegrationMemcached(unittest.TestCase):
    """Test ElastiCacheIntegration Memcached cluster methods"""

    def setUp(self):
        self.integration = ElastiCacheIntegration()
        self.mock_client = MagicMock()
        self.integration._clients['elasticache'] = self.mock_client

    def test_create_memcached_cluster(self):
        config = CacheClusterConfig(
            cluster_id="memcached-cluster",
            engine=CacheEngine.MEMCACHED,
            port=11211
        )
        self.mock_client.create_cache_cluster.return_value = {
            "CacheCluster": {
                "CacheClusterId": "memcached-cluster",
                "CacheClusterStatus": "creating"
            }
        }

        result = self.integration.create_memcached_cluster(config)

        self.assertEqual(result["cluster_id"], "memcached-cluster")
        self.assertEqual(result["status"], "creating")

    def test_get_memcached_cluster(self):
        self.mock_client.describe_cache_clusters.return_value = {
            "CacheClusters": [{
                "CacheClusterId": "memcached-cluster",
                "Engine": "memcached"
            }]
        }

        result = self.integration.get_memcached_cluster("memcached-cluster")

        self.assertEqual(result["CacheClusterId"], "memcached-cluster")

    def test_list_memcached_clusters(self):
        mock_paginator = MagicMock()
        self.mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"CacheClusters": [{"CacheClusterId": "mc-1", "Engine": "memcached"}]}
        ]

        result = self.integration.list_memcached_clusters()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["CacheClusterId"], "mc-1")

    def test_delete_memcached_cluster(self):
        self.mock_client.delete_cache_cluster.return_value = {
            "CacheCluster": {"CacheClusterId": "memcached-cluster"}
        }

        result = self.integration.delete_memcached_cluster("memcached-cluster")

        self.assertEqual(result["CacheClusterId"], "memcached-cluster")


class TestElastiCacheIntegrationParameterGroups(unittest.TestCase):
    """Test ElastiCacheIntegration parameter group methods"""

    def setUp(self):
        self.integration = ElastiCacheIntegration()
        self.mock_client = MagicMock()
        self.integration._clients['elasticache'] = self.mock_client

    def test_create_parameter_group(self):
        self.mock_client.create_cache_parameter_group.return_value = {
            "CacheParameterGroup": {
                "CacheParameterGroupName": "my-param-group",
                "CacheParameterGroupFamily": "redis7",
                "Description": "My parameter group"
            }
        }

        result = self.integration.create_parameter_group(
            "my-param-group",
            CacheEngine.REDIS,
            "My parameter group"
        )

        self.assertEqual(result["CacheParameterGroupName"], "my-param-group")

    def test_list_parameter_groups(self):
        mock_paginator = MagicMock()
        self.mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"CacheParameterGroups": [{"CacheParameterGroupName": "default.redis7"}]}
        ]

        result = self.integration.list_parameter_groups()

        self.assertEqual(len(result), 1)

    def test_get_parameter_group(self):
        self.mock_client.describe_cache_parameter_groups.return_value = {
            "CacheParameterGroups": [{
                "CacheParameterGroupName": "my-param-group"
            }]
        }

        result = self.integration.get_parameter_group("my-param-group")

        self.assertEqual(result["CacheParameterGroupName"], "my-param-group")


class TestElastiCacheIntegrationSubnetGroups(unittest.TestCase):
    """Test ElastiCacheIntegration subnet group methods"""

    def setUp(self):
        self.integration = ElastiCacheIntegration()
        self.mock_client = MagicMock()
        self.integration._clients['elasticache'] = self.mock_client

    def test_create_subnet_group(self):
        self.mock_client.create_cache_subnet_group.return_value = {
            "CacheSubnetGroup": {
                "CacheSubnetGroupName": "my-subnet-group",
                "CacheSubnetGroupDescription": "My subnet group"
            }
        }

        result = self.integration.create_subnet_group(
            "my-subnet-group",
            ["subnet-1", "subnet-2"],
            "My subnet group"
        )

        self.assertEqual(result["CacheSubnetGroupName"], "my-subnet-group")

    def test_list_subnet_groups(self):
        mock_paginator = MagicMock()
        self.mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"CacheSubnetGroups": [{"CacheSubnetGroupName": "default"}]}
        ]

        result = self.integration.list_subnet_groups()

        self.assertEqual(len(result), 1)

    def test_delete_subnet_group(self):
        self.mock_client.delete_cache_subnet_group.return_value = {}

        result = self.integration.delete_subnet_group("my-subnet-group")

        self.assertTrue(result)


class TestElastiCacheIntegrationSecurityGroups(unittest.TestCase):
    """Test ElastiCacheIntegration security group methods"""

    def setUp(self):
        self.integration = ElastiCacheIntegration()
        self.mock_ec2_client = MagicMock()
        self.integration._clients['ec2'] = self.mock_ec2_client

    def test_create_security_group(self):
        self.mock_ec2_client.create_security_group.return_value = {
            "GroupId": "sg-12345",
            "GroupName": "my-sec-group"
        }

        result = self.integration.create_security_group(
            "my-sec-group",
            "My security group"
        )

        self.assertEqual(result["GroupId"], "sg-12345")
        self.assertEqual(result["GroupName"], "my-sec-group")

    def test_list_security_groups(self):
        self.mock_ec2_client.describe_security_groups.return_value = {
            "SecurityGroups": [{"GroupId": "sg-12345", "GroupName": "default"}]
        }

        result = self.integration.list_security_groups()

        self.assertEqual(len(result), 1)


class TestElastiCacheIntegrationSnapshots(unittest.TestCase):
    """Test ElastiCacheIntegration snapshot methods"""

    def setUp(self):
        self.integration = ElastiCacheIntegration()
        self.mock_client = MagicMock()
        self.integration._clients['elasticache'] = self.mock_client

    def test_create_snapshot(self):
        self.mock_client.create_snapshot.return_value = {
            "Snapshot": {
                "SnapshotName": "my-snapshot",
                "CacheClusterId": "my-cluster",
                "SnapshotStatus": "creating"
            }
        }

        result = self.integration.create_snapshot(
            "my-snapshot",
            "my-cluster"
        )

        self.assertEqual(result["SnapshotName"], "my-snapshot")

    def test_list_snapshots(self):
        mock_paginator = MagicMock()
        self.mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"Snapshots": [{"SnapshotName": "snap-1", "Engine": "redis"}]}
        ]

        result = self.integration.list_snapshots()

        self.assertEqual(len(result), 1)

    def test_delete_snapshot(self):
        self.mock_client.delete_snapshot.return_value = {}

        result = self.integration.delete_snapshot("my-snapshot")

        self.assertTrue(result)


class TestElastiCacheIntegrationGlobalReplication(unittest.TestCase):
    """Test ElastiCacheIntegration global replication methods"""

    def setUp(self):
        self.integration = ElastiCacheIntegration()
        self.mock_client = MagicMock()
        self.integration._clients['elasticache'] = self.mock_client

    def test_create_global_replication(self):
        self.mock_client.create_global_replication_group.return_value = {
            "GlobalReplicationGroup": {
                "GlobalReplicationGroupId": "global-group-1",
                "Status": "creating"
            }
        }

        result = self.integration.create_global_replication(
            "primary-cluster",
            "My global group"
        )

        self.assertEqual(result["GlobalReplicationGroupId"], "global-group-1")

    def test_list_global_replications(self):
        mock_paginator = MagicMock()
        self.mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"GlobalReplicationGroups": [{
                "GlobalReplicationGroupId": "global-group-1"
            }]}
        ]

        result = self.integration.list_global_replications()

        self.assertEqual(len(result), 1)


class TestElastiCacheIntegrationServerless(unittest.TestCase):
    """Test ElastiCacheIntegration serverless cache methods"""

    def setUp(self):
        self.integration = ElastiCacheIntegration()
        self.mock_client = MagicMock()
        self.integration._clients['elasticache'] = self.mock_client

    def test_create_serverless_cache(self):
        self.mock_client.create_serverless_cache.return_value = {
            "ServerlessCache": {
                "ServerlessCacheName": "my-serverless",
                "Status": "creating"
            }
        }

        result = self.integration.create_serverless_cache(
            "my-serverless",
            CacheEngine.REDIS
        )

        self.assertEqual(result["ServerlessCacheName"], "my-serverless")

    def test_get_serverless_cache(self):
        self.mock_client.describe_serverless_caches.return_value = {
            "ServerlessCaches": [{
                "ServerlessCacheName": "my-serverless",
                "Status": "available"
            }]
        }

        result = self.integration.get_serverless_cache("my-serverless")

        self.assertEqual(result["ServerlessCacheName"], "my-serverless")

    def test_list_serverless_caches(self):
        mock_paginator = MagicMock()
        self.mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"ServerlessCaches": [{"ServerlessCacheName": "cache-1", "Engine": "redis"}]}
        ]

        result = self.integration.list_serverless_caches()

        self.assertEqual(len(result), 1)


class TestElastiCacheIntegrationValkey(unittest.TestCase):
    """Test ElastiCacheIntegration Valkey cluster methods"""

    def setUp(self):
        self.integration = ElastiCacheIntegration()
        self.mock_client = MagicMock()
        self.integration._clients['elasticache'] = self.mock_client

    def test_create_valkey_cluster(self):
        config = CacheClusterConfig(
            cluster_id="valkey-cluster",
            engine=CacheEngine.VALKEY
        )
        self.mock_client.create_cache_cluster.return_value = {
            "CacheCluster": {
                "CacheClusterId": "valkey-cluster",
                "Engine": "valkey"
            }
        }

        result = self.integration.create_valkey_cluster(config)

        self.assertEqual(result["cluster_id"], "valkey-cluster")

    def test_list_valkey_clusters(self):
        mock_paginator = MagicMock()
        self.mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"CacheClusters": [{"CacheClusterId": "valkey-1", "Engine": "valkey"}]}
        ]

        result = self.integration.list_valkey_clusters()

        self.assertEqual(len(result), 1)


class TestElastiCacheIntegrationCloudWatch(unittest.TestCase):
    """Test ElastiCacheIntegration CloudWatch monitoring methods"""

    def setUp(self):
        self.integration = ElastiCacheIntegration()
        self.mock_client = MagicMock()
        self.integration._clients['elasticache'] = self.mock_client
        self.mock_cw_client = MagicMock()
        self.integration._clients['cloudwatch'] = self.mock_cw_client

    def test_get_metrics(self):
        self.mock_cw_client.get_metric_statistics.return_value = {
            "Datapoints": [{"Average": 50.0}]
        }

        result = self.integration.get_metrics(
            "my-cluster",
            ["CPUUtilization"]
        )

        self.assertIn("CPUUtilization", result)

    def test_list_alarms(self):
        self.mock_cw_client.describe_alarms.return_value = {
            "MetricAlarms": [{"AlarmName": "CPUAlarm"}]
        }

        result = self.integration.list_alarms()

        self.assertEqual(len(result), 1)


if __name__ == "__main__":
    unittest.main()

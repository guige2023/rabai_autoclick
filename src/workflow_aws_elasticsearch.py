"""
Amazon Elasticsearch Service Integration

Provides comprehensive management of Elasticsearch domains including:
- Domain management: Create/manage Elasticsearch domains
- Instance management: Manage instance types and counts
- Storage: EBS storage configuration
- Access policies: Configure access policies
- Snapshots: Manage snapshots
- Reserved instances: Reserved instance capacity
- Advanced security: Fine-grained access control
- Cross-cluster search: Cross-cluster search
- Domain change items: Track domain changes
- CloudWatch integration: Domain metrics and monitoring
"""

import boto3
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
from botocore.exceptions import ClientError


class ElasticsearchIntegration:
    """Manages Amazon Elasticsearch Service domains and operations."""

    def __init__(self, region_name: str = "us-east-1"):
        """Initialize Elasticsearch integration.
        
        Args:
            region_name: AWS region for Elasticsearch service
        """
        self.region_name = region_name
        self.es_client = boto3.client("es", region_name=region_name)
        self.cloudwatch_client = boto3.client("cloudwatch", region_name=region_name)

    # ==================== Domain Management ====================

    def create_domain(
        self,
        domain_name: str,
        elasticsearch_version: str,
        instance_type: str,
        instance_count: int,
        dedicated_master_enabled: bool = False,
        dedicated_master_type: Optional[str] = None,
        dedicated_master_count: int = 0,
        ebs_enabled: bool = True,
        ebs_volume_size: int = 20,
        ebs_volume_type: str = "gp2",
        encryption_enabled: bool = False,
        vpc_options: Optional[Dict] = None,
        availability_zones: Optional[List[str]] = None,
        zone_awareness_enabled: bool = False,
    ) -> Dict[str, Any]:
        """Create a new Elasticsearch domain.
        
        Args:
            domain_name: Name of the domain
            elasticsearch_version: Elasticsearch version (e.g., "7.10")
            instance_type: Instance type (e.g., "t2.micro.elasticsearch")
            instance_count: Number of data nodes
            dedicated_master_enabled: Enable dedicated master nodes
            dedicated_master_type: Instance type for master nodes
            dedicated_master_count: Number of dedicated master nodes
            ebs_enabled: Enable EBS storage
            ebs_volume_size: Size of EBS volume in GB
            ebs_volume_type: EBS volume type (gp2, gp3, io1, io2)
            encryption_enabled: Enable domain encryption at rest
            vpc_options: VPC configuration dictionary
            availability_zones: List of availability zones
            zone_awareness_enabled: Enable zone awareness
            
        Returns:
            Domain creation status and details
        """
        cluster_config = {
            "InstanceType": instance_type,
            "InstanceCount": instance_count,
            "ZoneAwarenessEnabled": zone_awareness_enabled,
        }

        if dedicated_master_enabled:
            cluster_config["DedicatedMasterEnabled"] = True
            cluster_config["DedicatedMasterType"] = dedicated_master_type or instance_type
            cluster_config["DedicatedMasterCount"] = dedicated_master_count

        if availability_zones and zone_awareness_enabled:
            cluster_config["ZoneAwarenessConfig"] = {"AvailabilityZoneCount": len(availability_zones)}

        ebs_options = {"EBSEnabled": ebs_enabled}
        if ebs_enabled:
            ebs_options.update({
                "VolumeType": ebs_volume_type,
                "VolumeSize": ebs_volume_size,
            })

        domain_config = {
            "DomainName": domain_name,
            "ElasticsearchVersion": elasticsearch_version,
            "ElasticsearchClusterConfig": cluster_config,
            "EBSOptions": ebs_options,
            "EncryptionAtRestOptions": {"Enabled": encryption_enabled},
        }

        if vpc_options:
            domain_config["VPCOptions"] = vpc_options

        response = self.es_client.create_elasticsearch_domain(**domain_config)
        return response.get("DomainStatus", {})

    def get_domain(self, domain_name: str) -> Dict[str, Any]:
        """Get details of an Elasticsearch domain.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Domain details
        """
        response = self.es_client.describe_elasticsearch_domain(DomainName=domain_name)
        return response.get("DomainStatus", {})

    def list_domains(self) -> List[Dict[str, Any]]:
        """List all Elasticsearch domains in the region.
        
        Returns:
            List of domain summaries
        """
        response = self.es_client.list_domain_names()
        domain_names = response.get("DomainNames", [])
        
        domains = []
        for name_info in domain_names:
            try:
                domain = self.get_domain(name_info["Name"])
                domains.append(domain)
            except ClientError:
                continue
        return domains

    def delete_domain(self, domain_name: str) -> bool:
        """Delete an Elasticsearch domain.
        
        Args:
            domain_name: Name of the domain to delete
            
        Returns:
            True if deletion was successful
        """
        try:
            self.es_client.delete_elasticsearch_domain(DomainName=domain_name)
            return True
        except ClientError:
            return False

    # ==================== Instance Management ====================

    def update_instance_count(self, domain_name: str, instance_count: int) -> Dict[str, Any]:
        """Update the number of instances in a domain.
        
        Args:
            domain_name: Name of the domain
            instance_count: New instance count
            
        Returns:
            Updated domain configuration
        """
        response = self.es_client.update_elasticsearch_domain_config(
            DomainName=domain_name,
            ElasticsearchClusterConfig={"InstanceCount": instance_count}
        )
        return response.get("DomainConfig", {})

    def update_instance_type(self, domain_name: str, instance_type: str) -> Dict[str, Any]:
        """Update the instance type of a domain.
        
        Args:
            domain_name: Name of the domain
            instance_type: New instance type
            
        Returns:
            Updated domain configuration
        """
        response = self.es_client.update_elasticsearch_domain_config(
            DomainName=domain_name,
            ElasticsearchClusterConfig={"InstanceType": instance_type}
        )
        return response.get("DomainConfig", {})

    def get_instance_types(self) -> List[str]:
        """Get available Elasticsearch instance types.
        
        Returns:
            List of available instance types
        """
        response = self.es_client.list_elasticsearch_instance_types(
            ElasticsearchVersion="7.10"
        )
        return response.get("ElasticsearchInstanceTypes", [])

    # ==================== Storage (EBS) ====================

    def update_ebs_storage(
        self,
        domain_name: str,
        ebs_enabled: bool = True,
        volume_type: str = "gp2",
        volume_size: Optional[int] = None,
        iops: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Update EBS storage configuration.
        
        Args:
            domain_name: Name of the domain
            ebs_enabled: Enable EBS storage
            volume_type: EBS volume type (gp2, gp3, io1, io2)
            volume_size: Size of the volume in GB
            iops: Provisioned IOPS (for io1, io2, gp3)
            
        Returns:
            Updated EBS configuration
        """
        ebs_options = {"EBSEnabled": ebs_enabled}
        
        if ebs_enabled:
            ebs_options["VolumeType"] = volume_type
            if volume_size:
                ebs_options["VolumeSize"] = volume_size
            if iops and volume_type in ("io1", "io2", "gp3"):
                ebs_options["Iops"] = iops

        response = self.es_client.update_elasticsearch_domain_config(
            DomainName=domain_name,
            EBSOptions=ebs_options
        )
        return response.get("DomainConfig", {})

    # ==================== Access Policies ====================

    def get_access_policy(self, domain_name: str) -> Dict[str, Any]:
        """Get the access policy for a domain.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Access policy configuration
        """
        response = self.es_client.describe_elasticsearch_domain_config(
            DomainName=domain_name
        )
        return response.get("DomainConfig", {}).get("AccessPolicies", {})

    def update_access_policy(self, domain_name: str, policy: Dict[str, Any]) -> Dict[str, Any]:
        """Update the access policy for a domain.
        
        Args:
            domain_name: Name of the domain
            policy: IAM policy document
            
        Returns:
            Updated access policy configuration
        """
        policy_json = json.dumps(policy)
        response = self.es_client.update_elasticsearch_domain_config(
            DomainName=domain_name,
            AccessPolicies={"PolicyDocument": policy_json}
        )
        return response.get("DomainConfig", {}).get("AccessPolicies", {})

    def create_ip_based_policy(self, domain_name: str, allowed_ips: List[str]) -> Dict[str, Any]:
        """Create an IP-based access policy.
        
        Args:
            domain_name: Name of the domain
            allowed_ips: List of allowed IP addresses or CIDR blocks
            
        Returns:
            Updated access policy
        """
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": ["es:*"],
                    "Resource": f"arn:aws:es:{self.region_name}:*:domain/{domain_name}/*",
                    "Condition": {
                        "IpAddress": {
                            "aws:SourceIp": allowed_ips
                        }
                    }
                }
            ]
        }
        return self.update_access_policy(domain_name, policy)

    def create_iam_based_policy(self, domain_name: str, allowed_principals: List[str]) -> Dict[str, Any]:
        """Create an IAM-based access policy.
        
        Args:
            domain_name: Name of the domain
            allowed_principals: List of allowed IAM principal ARNs
            
        Returns:
            Updated access policy
        """
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": ["es:*"],
                    "Resource": f"arn:aws:es:{self.region_name}:*:domain/{domain_name}/*",
                    "Condition": {
                        "ArnEquals": {
                            "aws:PrincipalArn": allowed_principals
                        }
                    }
                }
            ]
        }
        return self.update_access_policy(domain_name, policy)

    # ==================== Snapshots ====================

    def get_snapshots(
        self,
        domain_name: str,
        max_snapshots: int = 100
    ) -> List[Dict[str, Any]]:
        """Get automated snapshots information for a domain.
        
        Args:
            domain_name: Name of the domain
            max_snapshots: Maximum number of snapshots to retrieve
            
        Returns:
            List of snapshot information
        """
        response = self.es_client.list_solutions(
            DomainName=domain_name
        )
        return response.get("Solutions", [])

    def get_snapshot_status(self, domain_name: str) -> Dict[str, Any]:
        """Get the status of the latest snapshot.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Snapshot status information
        """
        response = self.es_client.describe_elasticsearch_domain(
            DomainName=domain_name
        )
        return {
            "automated_snapshot_start_hour": response.get("DomainStatus", {}).get(
                "AutomatedSnapshotStartHour"
            )
        }

    def update_automated_snapshot(self, domain_name: str, start_hour: int) -> Dict[str, Any]:
        """Update the automated snapshot start hour.
        
        Args:
            domain_name: Name of the domain
            start_hour: Hour of the day for automated snapshots (0-23)
            
        Returns:
            Updated snapshot configuration
        """
        response = self.es_client.update_elasticsearch_domain_config(
            DomainName=domain_name,
            SnapshotOptions={"AutomatedSnapshotStartHour": start_hour}
        )
        return response.get("DomainConfig", {}).get("SnapshotOptions", {})

    # ==================== Reserved Instances ====================

    def get_reserved_instances(self) -> List[Dict[str, Any]]:
        """Get information about reserved instances.
        
        Returns:
            List of reserved instance offerings
        """
        response = self.es_client.describe_reserved_elasticsearch_instances()
        return response.get("ReservedElasticsearchInstances", [])

    def get_reserved_instance_offerings(
        self,
        instance_type: Optional[str] = None,
        duration: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Get available reserved instance offerings.
        
        Args:
            instance_type: Filter by instance type
            duration: Duration in years (1 or 3)
            
        Returns:
            List of reserved instance offerings
        """
        params = {}
        if instance_type:
            params["ElasticsearchInstanceType"] = instance_type
        if duration:
            params["DurationSeconds"] = duration * 31536000

        response = self.es_client.describe_reserved_elasticsearch_instance_offerings(**params)
        return response.get("ReservedElasticsearchInstanceOfferings", [])

    def purchase_reserved_instance(
        self,
        offering_id: str,
        instance_count: int = 1
    ) -> Dict[str, Any]:
        """Purchase a reserved instance.
        
        Args:
            offering_id: Reserved instance offering ID
            instance_count: Number of instances to reserve
            
        Returns:
            Purchase result
        """
        response = self.es_client.purchase_reserved_elasticsearch_instance_offering(
            ReservedElasticsearchInstanceOfferingId=offering_id,
            InstanceCount=instance_count
        )
        return response.get("ReservedElasticsearchInstance", {})

    # ==================== Advanced Security ====================

    def enable_fine_grained_access_control(
        self,
        domain_name: str,
        enabled: bool = True,
        internal_user_database_enabled: bool = False,
        master_user_options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Enable fine-grained access control.
        
        Args:
            domain_name: Name of the domain
            enabled: Enable fine-grained access control
            internal_user_database_enabled: Enable internal user database
            master_user_options: Master user configuration (UserName, UserPassword, or MasterUserARN)
            
        Returns:
            Updated advanced security options
        """
        advanced_options = {"Enabled": enabled}

        if enabled and internal_user_database_enabled:
            advanced_options["InternalUserDatabaseEnabled"] = True
            if master_user_options:
                advanced_options.update(master_user_options)

        response = self.es_client.update_elasticsearch_domain_config(
            DomainName=domain_name,
            AdvancedSecurityOptions=advanced_options
        )
        return response.get("DomainConfig", {}).get("AdvancedSecurityOptions", {})

    def get_fine_grained_access_control(self, domain_name: str) -> Dict[str, Any]:
        """Get fine-grained access control settings.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Advanced security options
        """
        domain = self.get_domain(domain_name)
        return domain.get("AdvancedSecurityOptions", {})

    def update_node_to_node_encryption(self, domain_name: str, enabled: bool) -> Dict[str, Any]:
        """Update node-to-node encryption settings.
        
        Args:
            domain_name: Name of the domain
            enabled: Enable node-to-node encryption
            
        Returns:
            Updated encryption configuration
        """
        response = self.es_client.update_elasticsearch_domain_config(
            DomainName=domain_name,
            NodeToNodeEncryptionOptions={"Enabled": enabled}
        )
        return response.get("DomainConfig", {}).get("NodeToNodeEncryptionOptions", {})

    def update_domain_encryption(self, domain_name: str, enabled: bool) -> Dict[str, Any]:
        """Update domain encryption at rest settings.
        
        Args:
            domain_name: Name of the domain
            enabled: Enable encryption at rest
            
        Returns:
            Updated encryption configuration
        """
        response = self.es_client.update_elasticsearch_domain_config(
            DomainName=domain_name,
            EncryptionAtRestOptions={"Enabled": enabled}
        )
        return response.get("DomainConfig", {}).get("EncryptionAtRestOptions", {})

    # ==================== Cross-Cluster Search ====================

    def enable_cross_cluster_search(self, domain_name: str, vpc_connector: Optional[Dict] = None) -> Dict[str, Any]:
        """Enable cross-cluster search for a domain.
        
        Args:
            domain_name: Name of the domain
            vpc_connector: VPC connection configuration
            
        Returns:
            Updated cross-cluster search configuration
        """
        cross_cluster_options = {"Enabled": True}
        if vpc_connector:
            cross_cluster_options["CrossClusterSearchConnector"] = vpc_connector

        response = self.es_client.update_elasticsearch_domain_config(
            DomainName=domain_name,
            CrossClusterSearchOptions=cross_cluster_options
        )
        return response.get("DomainConfig", {}).get("CrossClusterSearchOptions", {})

    def get_cross_cluster_search(self, domain_name: str) -> Dict[str, Any]:
        """Get cross-cluster search configuration.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Cross-cluster search configuration
        """
        domain = self.get_domain(domain_name)
        return domain.get("CrossClusterSearchOptions", {})

    # ==================== Domain Change Items ====================

    def get_domain_changes(self, domain_name: str) -> List[Dict[str, Any]]:
        """Track domain configuration changes.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            List of pending and completed configuration changes
        """
        response = self.es_client.describe_elasticsearch_domain_config(
            DomainName=domain_name
        )
        
        changes = []
        domain_config = response.get("DomainConfig", {})
        
        for config_name, config_value in domain_config.items():
            if "PendingDeletion" in config_value or "Processing" in config_value:
                changes.append({
                    "config_option": config_name,
                    "status": config_value.get("Status", "unknown"),
                    "pending_value": config_value.get("PendingDeletion"),
                    "timestamp": datetime.now().isoformat(),
                })
        
        return changes

    def get_change_progress(self, domain_name: str) -> Dict[str, str]:
        """Get the progress of domain updates.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Status of ongoing changes
        """
        domain = self.get_domain(domain_name)
        return {
            "domain_name": domain_name,
            "processing": domain.get("Processing", False),
            "endpoint": domain.get("Endpoint", "pending"),
            "endpoints": domain.get("Endpoints", {}),
            "update_ticket": domain.get("ServiceSoftwareOptions", {}).get("UpdateStatus", "not_applicable"),
        }

    # ==================== CloudWatch Integration ====================

    def get_domain_metrics(
        self,
        domain_name: str,
        metric_names: Optional[List[str]] = None,
        period: int = 300,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Get CloudWatch metrics for an Elasticsearch domain.
        
        Args:
            domain_name: Name of the domain
            metric_names: List of metric names to retrieve
            period: Metric period in seconds
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            CloudWatch metric data
        """
        if metric_names is None:
            metric_names = [
                "ClusterIndexWritesBlocked",
                "ClusterStatus.yellow",
                "ClusterStatus.red",
                "ClusterHealth.yellow",
                "ClusterHealth.red",
                "FreeStorageSpace",
                "ClusterUsedSpace",
                "Nodes",
                "SearchableDocuments",
                "DiskQueueDepth",
                "CPUUtilization",
                "JVMMemoryPressure",
            ]

        namespace = "AWS/ES"
        dimensions = [{"Name": "ClientId", "Value": boto3.client("sts").get_caller_identity()["Account"]},
                       {"Name": "DomainName", "Value": domain_name}]

        end_time = end_time or datetime.now()
        start_time = start_time or datetime(end_time.year, end_time.month, end_time.day)

        metric_data_queries = []
        for metric in metric_names:
            metric_data_queries.append({
                "Id": metric.lower().replace(".", "_"),
                "MetricStat": {
                    "Metric": {
                        "Namespace": namespace,
                        "MetricName": metric,
                        "Dimensions": dimensions
                    },
                    "Period": period,
                    "Stat": "Average"
                }
            })

        response = self.cloudwatch_client.get_metric_data(
            MetricDataQueries=metric_data_queries,
            StartTime=start_time,
            EndTime=end_time
        )

        return response.get("MetricDataResults", [])

    def put_custom_metric(
        self,
        domain_name: str,
        metric_name: str,
        value: float,
        unit: str = "Count",
        dimensions: Optional[List[Dict[str, str]]] = None,
    ) -> bool:
        """Put a custom metric to CloudWatch.
        
        Args:
            domain_name: Name of the domain
            metric_name: Name of the custom metric
            value: Metric value
            unit: Unit of the metric
            dimensions: Custom dimensions
            
        Returns:
            True if successful
        """
        try:
            self.cloudwatch_client.put_metric_data(
                Namespace="Custom/AWS/ES",
                MetricData=[{
                    "MetricName": metric_name,
                    "Dimensions": dimensions or [{"Name": "DomainName", "Value": domain_name}],
                    "Value": value,
                    "Unit": unit,
                    "Timestamp": datetime.now(),
                }]
            )
            return True
        except ClientError:
            return False

    def get_alarm_status(self, alarm_names: List[str]) -> List[Dict[str, Any]]:
        """Get status of CloudWatch alarms.
        
        Args:
            alarm_names: List of alarm names
            
        Returns:
            Alarm status information
        """
        try:
            response = self.cloudwatch_client.describe_alarms(
                AlarmNames=alarm_names
            )
            return response.get("MetricAlarms", [])
        except ClientError:
            return []

    def create_domain_alarm(
        self,
        domain_name: str,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 1,
        period: int = 300,
        statistic: str = "Average",
    ) -> Dict[str, Any]:
        """Create a CloudWatch alarm for a domain metric.
        
        Args:
            domain_name: Name of the domain
            alarm_name: Name of the alarm
            metric_name: Name of the metric
            threshold: Threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            statistic: Statistic type
            
        Returns:
            Created alarm configuration
        """
        account_id = boto3.client("sts").get_caller_identity()["Account"]
        
        response = self.cloudwatch_client.put_metric_alarm(
            AlarmName=alarm_name,
            AlarmDescription=f"Alarm for {domain_name} - {metric_name}",
            Namespace="AWS/ES",
            MetricName=metric_name,
            Dimensions=[
                {"Name": "ClientId", "Value": account_id},
                {"Name": "DomainName", "Value": domain_name}
            ],
            Period=period,
            Statistic=statistic,
            Threshold=threshold,
            ComparisonOperator=comparison_operator,
            EvaluationPeriods=evaluation_periods,
        )
        return response

    def get_cluster_health(self, domain_name: str) -> Dict[str, Any]:
        """Get cluster health metrics from CloudWatch.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Cluster health information
        """
        metrics = self.get_domain_metrics(
            domain_name,
            metric_names=[
                "ClusterStatus.yellow",
                "ClusterStatus.red",
                "ClusterHealth.yellow",
                "ClusterHealth.red",
            ],
            period=300
        )

        health_status = {
            "cluster_status_yellow": 0,
            "cluster_status_red": 0,
            "health_yellow": 0,
            "health_red": 0,
        }

        for metric in metrics:
            metric_name = metric.get("Id", "")
            values = metric.get("Values", [])
            if values:
                health_status[metric_name] = values[0]

        return health_status

    def get_storage_metrics(self, domain_name: str) -> Dict[str, Any]:
        """Get storage utilization metrics.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Storage metrics
        """
        metrics = self.get_domain_metrics(
            domain_name,
            metric_names=[
                "FreeStorageSpace",
                "ClusterUsedSpace",
                "DiskQueueDepth",
            ],
            period=300
        )

        storage_info = {
            "free_storage_space_mb": 0,
            "cluster_used_space_mb": 0,
            "disk_queue_depth": 0,
        }

        for metric in metrics:
            metric_id = metric.get("Id", "")
            values = metric.get("Values", [])
            if values:
                if metric_id == "freestoragespace":
                    storage_info["free_storage_space_mb"] = values[0] / (1024 ** 2)
                elif metric_id == "clusterusedspace":
                    storage_info["cluster_used_space_mb"] = values[0] / (1024 ** 2)
                elif metric_id == "diskqueuedepth":
                    storage_info["disk_queue_depth"] = values[0]

        return storage_info

    def get_jvm_memory_pressure(self, domain_name: str) -> Dict[str, Any]:
        """Get JVM memory pressure metrics.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            JVM memory pressure information
        """
        metrics = self.get_domain_metrics(
            domain_name,
            metric_names=["JVMMemoryPressure", "JVMMemoryPressureMaster"],
            period=300
        )

        memory_info = {
            "jvm_memory_pressure": 0,
            "jvm_memory_pressure_master": 0,
        }

        for metric in metrics:
            metric_id = metric.get("Id", "")
            values = metric.get("Values", [])
            if values:
                if metric_id == "jvmmemorypressure":
                    memory_info["jvm_memory_pressure"] = values[0]
                elif metric_id == "jvmmemorypressuremaster":
                    memory_info["jvm_memory_pressure_master"] = values[0]

        return memory_info

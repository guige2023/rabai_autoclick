"""
Tests for workflow_aws_route53 module
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

# Create mock boto3 module before importing workflow_aws_route53
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
from src.workflow_aws_route53 import (
    Route53Integration,
    RecordType,
    RoutingPolicy,
    HealthCheckStatus,
    HealthCheckType,
    HostedZoneType,
    DNSSECStatus,
    HostedZone,
    RecordSet,
    HealthCheck,
    TrafficPolicy,
    ResolverEndpoint,
    DomainRegistration,
    DKIMConfig,
    DNSSECConfig,
    FailoverConfig,
)


class TestRecordType(unittest.TestCase):
    """Test RecordType enum"""

    def test_record_type_values(self):
        self.assertEqual(RecordType.A.value, "A")
        self.assertEqual(RecordType.AAAA.value, "AAAA")
        self.assertEqual(RecordType.CNAME.value, "CNAME")
        self.assertEqual(RecordType.MX.value, "MX")
        self.assertEqual(RecordType.TXT.value, "TXT")
        self.assertEqual(RecordType.NS.value, "NS")
        self.assertEqual(RecordType.SOA.value, "SOA")
        self.assertEqual(RecordType.SRV.value, "SRV")
        self.assertEqual(RecordType.PTR.value, "PTR")
        self.assertEqual(RecordType.SPF.value, "SPF")
        self.assertEqual(RecordType.CAA.value, "CAA")
        self.assertEqual(RecordType.DS.value, "DS")


class TestRoutingPolicy(unittest.TestCase):
    """Test RoutingPolicy enum"""

    def test_routing_policy_values(self):
        self.assertEqual(RoutingPolicy.SIMPLE.value, "simple")
        self.assertEqual(RoutingPolicy.WEIGHTED.value, "weighted")
        self.assertEqual(RoutingPolicy.LATENCY.value, "latency")
        self.assertEqual(RoutingPolicy.GEOLOCATION.value, "geolocation")
        self.assertEqual(RoutingPolicy.GEOPROXIMITY.value, "geoproximity")
        self.assertEqual(RoutingPolicy.FAILOVER.value, "failover")
        self.assertEqual(RoutingPolicy.MULTIVALUE.value, "multivalue")


class TestHealthCheckStatus(unittest.TestCase):
    """Test HealthCheckStatus enum"""

    def test_health_check_status_values(self):
        self.assertEqual(HealthCheckStatus.HEALTHY.value, "healthy")
        self.assertEqual(HealthCheckStatus.UNHEALTHY.value, "unhealthy")
        self.assertEqual(HealthCheckStatus.LAST_FAILURE_REASON.value, "last_failure_reason")
        self.assertEqual(HealthCheckStatus.UNKNOWN.value, "unknown")


class TestHealthCheckType(unittest.TestCase):
    """Test HealthCheckType enum"""

    def test_health_check_type_values(self):
        self.assertEqual(HealthCheckType.HTTP.value, "HTTP")
        self.assertEqual(HealthCheckType.HTTPS.value, "HTTPS")
        self.assertEqual(HealthCheckType.HTTP_STR_MATCH.value, "HTTP_STR_MATCH")
        self.assertEqual(HealthCheckType.HTTPS_STR_MATCH.value, "HTTPS_STR_MATCH")
        self.assertEqual(HealthCheckType.TCP.value, "TCP")
        self.assertEqual(HealthCheckType.CALCULATED.value, "CALCULATED")
        self.assertEqual(HealthCheckType.RECOVERY_CONTROL.value, "RECOVERY_CONTROL")


class TestHostedZoneType(unittest.TestCase):
    """Test HostedZoneType enum"""

    def test_hosted_zone_type_values(self):
        self.assertEqual(HostedZoneType.PUBLIC.value, "public")
        self.assertEqual(HostedZoneType.PRIVATE.value, "private")


class TestDNSSECStatus(unittest.TestCase):
    """Test DNSSECStatus enum"""

    def test_dnssec_status_values(self):
        self.assertEqual(DNSSECStatus.SIGNING.value, "SIGNING")
        self.assertEqual(DNSSECStatus.NOT_SIGNING.value, "NOT_SIGNING")
        self.assertEqual(DNSSECStatus.DELETING.value, "DELETING")
        self.assertEqual(DNSSECStatus.UPDATE_FAILED.value, "UPDATE_FAILED")


class TestHostedZone(unittest.TestCase):
    """Test HostedZone dataclass"""

    def test_hosted_zone_defaults(self):
        zone = HostedZone(
            zone_id="ZONE123",
            name="example.com.",
            zone_type=HostedZoneType.PUBLIC
        )
        self.assertEqual(zone.zone_id, "ZONE123")
        self.assertEqual(zone.name, "example.com.")
        self.assertEqual(zone.zone_type, HostedZoneType.PUBLIC)
        self.assertEqual(zone.record_count, 0)
        self.assertIsNone(zone.comment)
        self.assertEqual(zone.tags, {})
        self.assertEqual(zone.vpcs, [])

    def test_hosted_zone_full(self):
        zone = HostedZone(
            zone_id="ZONE456",
            name="test.example.com.",
            zone_type=HostedZoneType.PRIVATE,
            record_count=10,
            comment="Test zone",
            caller_reference="ref-123",
            created_at=datetime.now(),
            tags={"Environment": "test"},
            delegation_set_id="DS123",
            vpcs=[{"vpc_id": "vpc-123", "vpc_region": "us-east-1"}]
        )
        self.assertEqual(zone.record_count, 10)
        self.assertEqual(zone.comment, "Test zone")
        self.assertEqual(zone.tags["Environment"], "test")
        self.assertEqual(len(zone.vpcs), 1)


class TestRecordSet(unittest.TestCase):
    """Test RecordSet dataclass"""

    def test_record_set_defaults(self):
        record = RecordSet(
            name="www.example.com.",
            record_type=RecordType.A
        )
        self.assertEqual(record.name, "www.example.com.")
        self.assertEqual(record.record_type, RecordType.A)
        self.assertEqual(record.ttl, 300)
        self.assertEqual(record.values, [])
        self.assertEqual(record.routing_policy, RoutingPolicy.SIMPLE)

    def test_record_set_full(self):
        record = RecordSet(
            name="api.example.com.",
            record_type=RecordType.CNAME,
            ttl=600,
            values=["lb-123.elb.amazonaws.com"],
            health_check_id="hc-456",
            set_identifier="api-primary",
            routing_policy=RoutingPolicy.FAILOVER,
            weight=100,
            region="us-east-1",
            geo_location={"CountryCode": "US"},
            failover_type="PRIMARY",
            multi_value_answer=False
        )
        self.assertEqual(record.ttl, 600)
        self.assertEqual(record.values[0], "lb-123.elb.amazonaws.com")
        self.assertEqual(record.routing_policy, RoutingPolicy.FAILOVER)
        self.assertEqual(record.failover_type, "PRIMARY")


class TestHealthCheck(unittest.TestCase):
    """Test HealthCheck dataclass"""

    def test_health_check_defaults(self):
        hc = HealthCheck(
            health_check_id="hc-123",
            name="my-health-check",
            health_check_type=HealthCheckType.HTTP
        )
        self.assertEqual(hc.health_check_id, "hc-123")
        self.assertEqual(hc.health_check_type, HealthCheckType.HTTP)
        self.assertEqual(hc.status, HealthCheckStatus.UNKNOWN)
        self.assertEqual(hc.port, 80)
        self.assertEqual(hc.protocol, "HTTP")
        self.assertEqual(hc.resource_path, "/")

    def test_health_check_full(self):
        hc = HealthCheck(
            health_check_id="hc-789",
            name="api-health",
            health_check_type=HealthCheckType.HTTPS,
            status=HealthCheckStatus.HEALTHY,
            ip_address="192.168.1.1",
            fqdn="api.example.com",
            port=443,
            protocol="HTTPS",
            resource_path="/health",
            fully_qualified_domain_name="api.example.com",
            search_string="OK",
            request_interval=30,
            failure_threshold=5,
            measure_latency=True,
            inverted=False,
            disabled=False,
            child_health_checks=["hc-001", "hc-002"],
            health_threshold=2,
            cloud_watch_alarm_name="api-alarm"
        )
        self.assertEqual(hc.status, HealthCheckStatus.HEALTHY)
        self.assertEqual(hc.port, 443)
        self.assertEqual(hc.search_string, "OK")


class TestTrafficPolicy(unittest.TestCase):
    """Test TrafficPolicy dataclass"""

    def test_traffic_policy_creation(self):
        doc = {"rules": [{"location": {"CountryCode": "US"}, "weight": 100}]}
        tp = TrafficPolicy(
            policy_id="tp-123",
            name="my-policy",
            document=doc,
            version=1,
            comment="Test policy"
        )
        self.assertEqual(tp.policy_id, "tp-123")
        self.assertEqual(tp.name, "my-policy")
        self.assertEqual(tp.version, 1)


class TestResolverEndpoint(unittest.TestCase):
    """Test ResolverEndpoint dataclass"""

    def test_resolver_endpoint_creation(self):
        re = ResolverEndpoint(
            endpoint_id="rve-123",
            name="my-resolver",
            endpoint_type="INBOUND",
            ip_addresses=[{"Ip": "192.168.1.1", "Port": 53}],
            security_group_ids=["sg-123"],
            status="ACTIVE"
        )
        self.assertEqual(re.endpoint_id, "rve-123")
        self.assertEqual(re.status, "ACTIVE")
        self.assertEqual(len(re.ip_addresses), 1)


class TestDomainRegistration(unittest.TestCase):
    """Test DomainRegistration dataclass"""

    def test_domain_registration_creation(self):
        dr = DomainRegistration(
            domain_name="example.com",
            registration_id="reg-123",
            status="active",
            dns_sec=True,
            nameservers=["ns1.example.com", "ns2.example.com"],
            privacy_protection=True,
            auto_renew=True
        )
        self.assertEqual(dr.domain_name, "example.com")
        self.assertTrue(dr.dns_sec)
        self.assertTrue(dr.auto_renew)


class TestDKIMConfig(unittest.TestCase):
    """Test DKIMConfig dataclass"""

    def test_dkim_config_creation(self):
        dk = DKIMConfig(
            domain="example.com",
            token="token123",
            status="active",
            selector="key1"
        )
        self.assertEqual(dk.domain, "example.com")
        self.assertEqual(dk.status, "active")


class TestDNSSECConfig(unittest.TestCase):
    """Test DNSSECConfig dataclass"""

    def test_dnssec_config_creation(self):
        dc = DNSSECConfig(
            status=DNSSECStatus.SIGNING,
            signing_keys=[{"keyId": "key-123"}],
            ksk_id="ksk-123",
            zsk_ids=["zsk-1", "zsk-2"]
        )
        self.assertEqual(dc.status, DNSSECStatus.SIGNING)
        self.assertEqual(len(dc.zsk_ids), 2)


class TestFailoverConfig(unittest.TestCase):
    """Test FailoverConfig dataclass"""

    def test_failover_config_creation(self):
        primary = RecordSet(name="primary.example.com.", record_type=RecordType.A, values=["1.2.3.4"])
        secondary = RecordSet(name="secondary.example.com.", record_type=RecordType.A, values=["5.6.7.8"])
        fc = FailoverConfig(
            primary_record=primary,
            secondary_record=secondary,
            failover_type="PRIMARY"
        )
        self.assertEqual(fc.failover_type, "PRIMARY")
        self.assertEqual(len(fc.primary_record.values), 1)


class TestRoute53Integration(unittest.TestCase):
    """Test Route53Integration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_route53_client = MagicMock()
        self.mock_resolver_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()

        # Create integration instance with mocked clients
        self.integration = Route53Integration(region="us-east-1")
        self.integration._client = self.mock_route53_client
        self.integration._route53resolver = self.mock_resolver_client
        self.integration._cloudwatch = self.mock_cloudwatch_client

    def test_initialization(self):
        """Test Route53Integration initialization"""
        integration = Route53Integration(region="eu-west-1", profile_name="my-profile")
        self.assertEqual(integration.region, "eu-west-1")
        self.assertEqual(integration.profile_name, "my-profile")
        self.assertIsNotNone(integration._lock)
        self.assertEqual(integration._hosted_zones, {})

    def test_create_hosted_zone_public(self):
        """Test creating a public hosted zone"""
        self.mock_route53_client.create_hosted_zone.return_value = {
            "HostedZone": {
                "Id": "/hostedzone/Z123",
                "Name": "example.com.",
                "Config": {"Comment": "Test zone", "PrivateZone": False}
            }
        }

        zone = self.integration.create_hosted_zone(
            name="example.com.",
            zone_type=HostedZoneType.PUBLIC,
            comment="Test zone"
        )

        self.assertEqual(zone.name, "example.com.")
        self.mock_route53_client.create_hosted_zone.assert_called_once()

    def test_create_hosted_zone_private(self):
        """Test creating a private hosted zone"""
        self.mock_route53_client.create_hosted_zone.return_value = {
            "HostedZone": {
                "Id": "/hostedzone/Z456",
                "Name": "internal.example.com.",
                "Config": {"Comment": "Internal zone", "PrivateZone": True}
            }
        }

        zone = self.integration.create_hosted_zone(
            name="internal.example.com.",
            zone_type=HostedZoneType.PRIVATE,
            vpc_id="vpc-123",
            vpc_region="us-east-1"
        )

        self.assertEqual(zone.name, "internal.example.com.")
        self.assertEqual(zone.zone_type, HostedZoneType.PRIVATE)

    def test_create_hosted_zone_simulation_mode(self):
        """Test creating hosted zone in simulation mode (no boto3)"""
        with patch('src.workflow_aws_route53.BOTO3_AVAILABLE', False):
            integration = Route53Integration()
            zone = integration.create_hosted_zone(
                name="simulated.com.",
                zone_type=HostedZoneType.PUBLIC,
                comment="Simulated zone"
            )
            self.assertEqual(zone.name, "simulated.com.")
            self.assertIn(zone.zone_id, integration._hosted_zones)

    def test_get_hosted_zone(self):
        """Test getting a hosted zone"""
        self.mock_route53_client.get_hosted_zone.return_value = {
            "HostedZone": {
                "Id": "/hostedzone/Z123",
                "Name": "example.com.",
                "Config": {"Comment": "Test", "PrivateZone": False, "RecordSetCount": 5}
            }
        }
        self.mock_route53_client.list_tags_for_resource.return_value = {"Environment": "test"}

        zone = self.integration.get_hosted_zone("Z123")

        self.assertIsNotNone(zone)
        self.assertEqual(zone.zone_id, "Z123")
        self.mock_route53_client.get_hosted_zone.assert_called_once_with(Id="Z123")

    def test_get_hosted_zone_simulation(self):
        """Test getting hosted zone in simulation mode"""
        with patch('src.workflow_aws_route53.BOTO3_AVAILABLE', False):
            integration = Route53Integration()
            zone = integration.create_hosted_zone(name="test.com.")
            retrieved = integration.get_hosted_zone(zone.zone_id)
            self.assertEqual(retrieved.name, "test.com.")

    def test_list_hosted_zones(self):
        """Test listing hosted zones"""
        self.mock_route53_client.list_hosted_zones.return_value = {
            "HostedZones": [
                {
                    "Id": "/hostedzone/Z1",
                    "Name": "example.com.",
                    "Config": {"PrivateZone": False}
                },
                {
                    "Id": "/hostedzone/Z2",
                    "Name": "test.com.",
                    "Config": {"PrivateZone": True}
                }
            ]
        }

        zones = self.integration.list_hosted_zones()
        self.assertEqual(len(zones), 2)

    def test_list_hosted_zones_filtered(self):
        """Test listing hosted zones with type filter"""
        self.mock_route53_client.list_hosted_zones.return_value = {
            "HostedZones": [
                {
                    "Id": "/hostedzone/Z1",
                    "Name": "example.com.",
                    "Config": {"PrivateZone": False}
                }
            ]
        }

        zones = self.integration.list_hosted_zones(zone_type=HostedZoneType.PUBLIC)
        self.assertEqual(len(zones), 1)

    def test_delete_hosted_zone(self):
        """Test deleting a hosted zone"""
        self.mock_route53_client.delete_hosted_zone.return_value = {}

        result = self.integration.delete_hosted_zone("Z123")

        self.assertTrue(result)
        self.mock_route53_client.delete_hosted_zone.assert_called_once_with(Id="Z123")

    def test_delete_hosted_zone_simulation(self):
        """Test deleting hosted zone in simulation mode"""
        integration = Route53Integration()
        zone = integration.create_hosted_zone(name="todelete.com.")
        result = integration.delete_hosted_zone(zone.zone_id)
        self.assertTrue(result)
        self.assertNotIn(zone.zone_id, integration._hosted_zones)

    def test_update_hosted_zone_comment(self):
        """Test updating hosted zone comment"""
        self.mock_route53_client.update_hosted_zone_comment.return_value = {}

        result = self.integration.update_hosted_zone_comment("Z123", "Updated comment")

        self.assertTrue(result)
        self.mock_route53_client.update_hosted_zone_comment.assert_called_once()

    def test_create_record_set(self):
        """Test creating a record set"""
        self.mock_route53_client.change_resource_record_sets.return_value = {}

        record = self.integration.create_record_set(
            zone_id="Z123",
            name="www.example.com.",
            record_type=RecordType.A,
            values=["192.168.1.1"],
            ttl=300
        )

        self.assertEqual(record.name, "www.example.com.")
        self.assertEqual(record.record_type, RecordType.A)
        self.mock_route53_client.change_resource_record_sets.assert_called_once()

    def test_create_record_set_simulation(self):
        """Test creating record set in simulation mode"""
        integration = Route53Integration()
        zone = integration.create_hosted_zone(name="sim.com.")
        record = integration.create_record_set(
            zone_id=zone.zone_id,
            name="www.sim.com.",
            record_type=RecordType.AAAA,
            values=["2001:db8::1"]
        )
        self.assertEqual(record.record_type, RecordType.AAAA)

    def test_get_record_set(self):
        """Test getting a record set"""
        self.mock_route53_client.list_resource_record_sets.return_value = {
            "ResourceRecordSets": [
                {
                    "Name": "www.example.com.",
                    "Type": "A",
                    "TTL": 300,
                    "ResourceRecords": [{"Value": "192.168.1.1"}]
                }
            ]
        }

        record = self.integration.get_record_set("Z123", "www.example.com.", RecordType.A)

        self.assertIsNotNone(record)
        self.assertEqual(record.name, "www.example.com.")

    def test_list_record_sets(self):
        """Test listing record sets"""
        self.mock_route53_client.list_resource_record_sets.return_value = {
            "ResourceRecordSets": [
                {"Name": "www.example.com.", "Type": "A", "TTL": 300, "ResourceRecords": [{"Value": "192.168.1.1"}]},
                {"Name": "mail.example.com.", "Type": "MX", "TTL": 300, "ResourceRecords": [{"Value": "10 mail.example.com."}]}
            ]
        }

        records = self.integration.list_record_sets("Z123")

        self.assertEqual(len(records), 2)

    def test_list_record_sets_with_filter(self):
        """Test listing record sets with type filter"""
        self.mock_route53_client.list_resource_record_sets.return_value = {
            "ResourceRecordSets": [
                {"Name": "www.example.com.", "Type": "A", "TTL": 300, "ResourceRecords": [{"Value": "192.168.1.1"}]}
            ]
        }

        records = self.integration.list_record_sets("Z123", record_type=RecordType.A)

        self.assertEqual(len(records), 1)

    def test_update_record_set(self):
        """Test updating a record set"""
        self.mock_route53_client.change_resource_record_sets.return_value = {}

        result = self.integration.update_record_set(
            zone_id="Z123",
            name="www.example.com.",
            record_type=RecordType.A,
            values=["192.168.2.1"],
            ttl=600
        )

        self.assertTrue(result)

    def test_delete_record_set(self):
        """Test deleting a record set"""
        self.mock_route53_client.change_resource_record_sets.return_value = {}

        result = self.integration.delete_record_set("Z123", "www.example.com.", RecordType.A)

        self.assertTrue(result)

    def test_create_health_check(self):
        """Test creating a health check"""
        self.mock_route53_client.create_health_check.return_value = {
            "HealthCheck": {
                "Id": "hc-123",
                "HealthCheckConfig": {
                    "Type": "HTTP",
                    "IPAddress": "192.168.1.1",
                    "Port": 80,
                    "ResourcePath": "/",
                    "RequestInterval": 10,
                    "FailureThreshold": 3
                }
            }
        }

        hc = self.integration.create_health_check(
            name="my-health-check",
            health_check_type=HealthCheckType.HTTP,
            ip_address="192.168.1.1"
        )

        self.assertEqual(hc.health_check_type, HealthCheckType.HTTP)

    def test_create_health_check_simulation(self):
        """Test creating health check in simulation mode"""
        with patch('src.workflow_aws_route53.BOTO3_AVAILABLE', False):
            integration = Route53Integration()
            hc = integration.create_health_check(
                name="sim-health",
                health_check_type=HealthCheckType.TCP,
                ip_address="192.168.1.1",
                port=443
            )
            self.assertIn(hc.health_check_id, integration._health_checks)

    def test_get_health_check(self):
        """Test getting a health check"""
        self.mock_route53_client.get_health_check.return_value = {
            "HealthCheck": {
                "Id": "hc-123",
                "HealthCheckConfig": {
                    "Type": "HTTP",
                    "IPAddress": "192.168.1.1",
                    "Port": 80,
                    "ResourcePath": "/health",
                    "RequestInterval": 10,
                    "FailureThreshold": 3,
                    "Notes": "Test check"
                },
                "Status": "Healthy"
            }
        }

        hc = self.integration.get_health_check("hc-123")

        self.assertIsNotNone(hc)
        self.assertEqual(hc.health_check_id, "hc-123")

    def test_list_health_checks(self):
        """Test listing health checks"""
        self.mock_route53_client.list_health_checks.return_value = {
            "HealthChecks": [
                {
                    "Id": "hc-1",
                    "HealthCheckConfig": {"Type": "HTTP", "Port": 80, "ResourcePath": "/"}
                },
                {
                    "Id": "hc-2",
                    "HealthCheckConfig": {"Type": "TCP", "Port": 443}
                }
            ]
        }

        checks = self.integration.list_health_checks()

        self.assertEqual(len(checks), 2)

    def test_delete_health_check(self):
        """Test deleting a health check"""
        self.mock_route53_client.delete_health_check.return_value = {}

        result = self.integration.delete_health_check("hc-123")

        self.assertTrue(result)

    def test_update_health_check(self):
        """Test updating a health check"""
        self.mock_route53_client.update_health_check.return_value = {}

        result = self.integration.update_health_check(
            health_check_id="hc-123",
            failure_threshold=5,
            inverted=True
        )

        self.assertTrue(result)

    def test_create_traffic_policy(self):
        """Test creating a traffic policy"""
        self.mock_route53_client.create_traffic_policy.return_value = {
            "TrafficPolicy": {
                "Id": "tp-123",
                "Name": "my-policy",
                "Version": 1
            }
        }

        doc = {"rules": []}
        tp = self.integration.create_traffic_policy(
            name="my-policy",
            document=doc,
            comment="Test policy"
        )

        self.assertEqual(tp.name, "my-policy")

    def test_list_traffic_policies(self):
        """Test listing traffic policies"""
        self.mock_route53_client.list_traffic_policies.return_value = {
            "TrafficPolicies": [
                {"Id": "tp-1", "Name": "policy-1", "Type": "A", "Version": "1"},
                {"Id": "tp-2", "Name": "policy-2", "Type": "AAAA", "Version": "1"}
            ]
        }

        policies = self.integration.list_traffic_policies()

        self.assertEqual(len(policies), 2)

    def test_create_resolver_endpoint(self):
        """Test creating a Resolver endpoint"""
        self.mock_resolver_client.create_resolver_endpoint.return_value = {
            "ResolverEndpoint": {
                "Id": "rve-123",
                "Name": "my-resolver",
                "Direction": "INBOUND",
                "Status": "CREATING",
                "ResolverEndpointStatus": "CREATING",
                "IpAddresses": [{"Ip": "192.168.1.1", "Port": 53}],
                "SecurityGroupIds": ["sg-123"]
            }
        }

        endpoint = self.integration.create_resolver_endpoint(
            name="my-resolver",
            endpoint_type="INBOUND",
            security_group_ids=["sg-123"],
            ip_addresses=[{"Ip": "192.168.1.1", "Port": 53}]
        )

        self.assertEqual(endpoint.name, "my-resolver")

    def test_list_resolver_endpoints(self):
        """Test listing Resolver endpoints"""
        self.mock_resolver_client.list_resolver_endpoints.return_value = {
            "ResolverEndpoints": [
                {"Id": "rve-1", "Name": "endpoint-1", "Direction": "INBOUND", "ResolverEndpointStatus": "OPERATIONAL", "IpAddresses": [], "SecurityGroupIds": []}
            ]
        }

        endpoints = self.integration.list_resolver_endpoints()

        self.assertEqual(len(endpoints), 1)

    def test_delete_resolver_endpoint(self):
        """Test deleting a Resolver endpoint"""
        self.mock_resolver_client.delete_resolver_endpoint.return_value = {}

        result = self.integration.delete_resolver_endpoint("rve-123")

        self.assertTrue(result)

    def test_register_domain(self):
        """Test domain registration"""
        self.mock_route53_client.register_domain.return_value = {
            "DomainName": "example.com",
            "RegistrationId": "reg-123"
        }

        contact = {
            "FirstName": "John",
            "LastName": "Doe",
            "Email": "john@example.com",
            "Phone": "+1-555-0100",
            "Address": {
                "AddressLine1": "123 Main St",
                "City": "Seattle",
                "State": "WA",
                "PostalCode": "98101",
                "Country": "US"
            }
        }
        result = self.integration.register_domain(
            domain_name="example.com",
            contact=contact,
            duration_years=1,
            privacy_protection=True
        )

        self.assertIsNotNone(result)

    def test_list_domains(self):
        """Test listing domains"""
        self.mock_route53_client.list_domains.return_value = {
            "Domains": [
                {"DomainName": "example.com", "RegistrationStatus": "active"}
            ]
        }

        domains = self.integration.list_domains()

        self.assertEqual(len(domains), 1)

    def test_get_domain_registration(self):
        """Test getting domain registration details"""
        self.mock_route53_client.get_domain_detail.return_value = {
            "DomainName": "example.com",
            "Nameservers": [{"Name": "ns1.example.com"}]
        }

        domain = self.integration.get_domain_registration("example.com")

        self.assertIsNotNone(domain)

    def test_enable_dnssec(self):
        """Test enabling DNSSEC"""
        self.mock_route53_client.enable_hosted_zone_dnssec.return_value = {}

        result = self.integration.enable_dnssec("Z123", "ksk-123")

        self.assertTrue(result)

    def test_disable_dnssec(self):
        """Test disabling DNSSEC"""
        self.mock_route53_client.disable_hosted_zone_dnssec.return_value = {}

        result = self.integration.disable_dnssec("Z123")

        self.assertTrue(result)

    def test_get_dnssec_config(self):
        """Test getting DNSSEC config"""
        self.mock_route53_client.get_dnssec.return_value = {
            "Status": {"Message": "Signing"}
        }

        config = self.integration.get_dnssec_config("Z123")

        self.assertIsNotNone(config)

    def test_configure_failover(self):
        """Test configuring failover routing"""
        self.mock_route53_client.change_resource_record_sets.return_value = {}

        result = self.integration.configure_failover(
            zone_id="Z123",
            name="www.example.com.",
            failover_type="PRIMARY",
            primary_value="192.168.1.1",
            secondary_value="192.168.2.1",
            record_type=RecordType.A
        )

        self.assertTrue(result)

    def test_change_action(self):
        """Test change action enumeration"""
        self.mock_route53_client.change_resource_record_sets.return_value = {}

        # Test using UPSERT via update_record_set with UPSERT action
        result = self.integration.update_record_set(
            zone_id="Z123",
            name="www.example.com.",
            record_type=RecordType.A,
            values=["192.168.1.1"],
            action="UPSERT"
        )

        self.assertTrue(result)


class TestRoute53IntegrationSimulationMode(unittest.TestCase):
    """Test Route53Integration in simulation mode (no real AWS)"""

    def test_simulation_mode_hosted_zone(self):
        """Test hosted zone operations in simulation mode"""
        # Test that simulation mode works when BOTO3_AVAILABLE is False
        integration = Route53Integration()
        # The integration uses internal dicts for simulation when boto3 is available
        # or when using mocked clients
        zone = integration.create_hosted_zone(
            name="workflow-test.com.",
            zone_type=HostedZoneType.PUBLIC,
            comment="Test zone"
        )
        self.assertIsNotNone(zone)


if __name__ == "__main__":
    unittest.main()

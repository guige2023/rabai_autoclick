"""
Tests for workflow_aws_waf module
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

# Create mock boto3 module before importing workflow_aws_waf
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

# Import the module
import src.workflow_aws_waf as waf_module

WAFIntegration = waf_module.WAFIntegration
WebACLScope = waf_module.WebACLScope
WebACLAction = waf_module.WebACLAction
RuleAction = waf_module.RuleAction
RuleType = waf_module.RuleType
IPSetAddressVersion = waf_module.IPSetAddressVersion
LoggingDestinationType = waf_module.LoggingDestinationType
BotControlCategory = waf_module.BotControlCategory
WebACL = waf_module.WebACL
RuleGroup = waf_module.RuleGroup
IPSet = waf_module.IPSet
RegexPatternSet = waf_module.RegexPatternSet
RateRule = waf_module.RateRule
BotControlRule = waf_module.BotControlRule
LoggingConfig = waf_module.LoggingConfig


class TestWAFEnums(unittest.TestCase):
    """Test WAF enums"""

    def test_web_acl_scope_values(self):
        self.assertEqual(WebACLScope.CLOUDFRONT.value, "CLOUDFRONT")
        self.assertEqual(WebACLScope.REGIONAL.value, "REGIONAL")

    def test_web_acl_action_values(self):
        self.assertEqual(WebACLAction.ALLOW.value, "ALLOW")
        self.assertEqual(WebACLAction.BLOCK.value, "BLOCK")
        self.assertEqual(WebACLAction.COUNT.value, "COUNT")
        self.assertEqual(WebACLAction.CAPTCHA.value, "CAPTCHA")
        self.assertEqual(WebACLAction.CHALLENGE.value, "CHALLENGE")

    def test_rule_action_values(self):
        self.assertEqual(RuleAction.ALLOW.value, "ALLOW")
        self.assertEqual(RuleAction.BLOCK.value, "BLOCK")
        self.assertEqual(RuleAction.COUNT.value, "COUNT")

    def test_rule_type_values(self):
        self.assertEqual(RuleType.REGULAR.value, "REGULAR")
        self.assertEqual(RuleType.RATE_BASED.value, "RATE_BASED")
        self.assertEqual(RuleType.GROUP.value, "GROUP")

    def test_ip_set_address_version_values(self):
        self.assertEqual(IPSetAddressVersion.IPV4.value, "IPV4")
        self.assertEqual(IPSetAddressVersion.IPV6.value, "IPV6")

    def test_logging_destination_type_values(self):
        self.assertEqual(LoggingDestinationType.S3.value, "S3")
        self.assertEqual(LoggingDestinationType.KINESIS_FIREHOSE.value, "KINESIS_FIREHOSE")
        self.assertEqual(LoggingDestinationType.CLOUDWATCH_LOG.value, "CLOUDWATCH_LOG")

    def test_bot_control_category_values(self):
        self.assertEqual(BotControlCategory.AUTOMATED.value, "AUTOMATED")
        self.assertEqual(BotControlCategory.BOT.value, "BOT")
        self.assertEqual(BotControlCategory.SCRAPER.value, "SCRAPER")


class TestWAFDataclasses(unittest.TestCase):
    """Test WAF dataclasses"""

    def test_web_acl_defaults(self):
        acl = WebACL(name="test-acl", scope=WebACLScope.REGIONAL)
        self.assertEqual(acl.name, "test-acl")
        self.assertEqual(acl.scope, WebACLScope.REGIONAL)
        self.assertEqual(acl.default_action, WebACLAction.ALLOW)
        self.assertEqual(len(acl.rules), 0)
        self.assertEqual(len(acl.tags), 0)

    def test_web_acl_custom(self):
        acl = WebACL(
            name="test-acl",
            scope=WebACLScope.CLOUDFRONT,
            description="Test ACL",
            default_action=WebACLAction.BLOCK,
            tags={"env": "prod"}
        )
        self.assertEqual(acl.name, "test-acl")
        self.assertEqual(acl.scope, WebACLScope.CLOUDFRONT)
        self.assertEqual(acl.default_action, WebACLAction.BLOCK)
        self.assertEqual(acl.tags["env"], "prod")

    def test_rule_group_defaults(self):
        rg = RuleGroup(name="test-rg", scope=WebACLScope.REGIONAL)
        self.assertEqual(rg.name, "test-rg")
        self.assertEqual(rg.capacity, 0)

    def test_rule_group_custom(self):
        rg = RuleGroup(
            name="test-rg",
            scope=WebACLScope.REGIONAL,
            capacity=100,
            description="Test rule group"
        )
        self.assertEqual(rg.capacity, 100)
        self.assertEqual(rg.description, "Test rule group")

    def test_ip_set_defaults(self):
        ip_set = IPSet(name="test-ipset", scope=WebACLScope.REGIONAL)
        self.assertEqual(ip_set.name, "test-ipset")
        self.assertEqual(ip_set.address_version, IPSetAddressVersion.IPV4)
        self.assertEqual(len(ip_set.addresses), 0)

    def test_ip_set_custom(self):
        ip_set = IPSet(
            name="test-ipset",
            scope=WebACLScope.REGIONAL,
            addresses=["192.168.1.1/32", "10.0.0.0/8"],
            address_version=IPSetAddressVersion.IPV4
        )
        self.assertEqual(len(ip_set.addresses), 2)
        self.assertEqual(ip_set.address_version, IPSetAddressVersion.IPV4)

    def test_regex_pattern_set_defaults(self):
        rps = RegexPatternSet(name="test-rps", scope=WebACLScope.REGIONAL)
        self.assertEqual(rps.name, "test-rps")
        self.assertEqual(len(rps.patterns), 0)

    def test_regex_pattern_set_custom(self):
        rps = RegexPatternSet(
            name="test-rps",
            scope=WebACLScope.REGIONAL,
            patterns=[r"\.evil\.com$", r"\.malware\.org$"]
        )
        self.assertEqual(len(rps.patterns), 2)

    def test_rate_rule_defaults(self):
        rr = RateRule(name="test-rr", rate_limit=1000)
        self.assertEqual(rr.name, "test-rr")
        self.assertEqual(rr.rate_limit, 1000)
        self.assertEqual(rr.action, RuleAction.BLOCK)
        self.assertEqual(rr.evaluation_window, 300)

    def test_rate_rule_custom(self):
        rr = RateRule(
            name="test-rr",
            rate_limit=5000,
            scope=WebACLScope.REGIONAL,
            action=RuleAction.COUNT
        )
        self.assertEqual(rr.rate_limit, 5000)
        self.assertEqual(rr.action, RuleAction.COUNT)

    def test_bot_control_rule_defaults(self):
        bcr = BotControlRule(name="test-bcr", category=BotControlCategory.BOT)
        self.assertEqual(bcr.name, "test-bcr")
        self.assertEqual(bcr.category, BotControlCategory.BOT)
        self.assertTrue(bcr.enable)
        self.assertEqual(bcr.action, RuleAction.BLOCK)

    def test_bot_control_rule_custom(self):
        bcr = BotControlRule(
            name="test-bcr",
            category=BotControlCategory.SCRAPER,
            sensitivity_level="HIGH"
        )
        self.assertEqual(bcr.category, BotControlCategory.SCRAPER)
        self.assertEqual(bcr.sensitivity_level, "HIGH")

    def test_logging_config(self):
        lc = LoggingConfig(
            acl_id="acl-123",
            log_destination_arn="arn:aws:firehose:us-east-1:123456789012:deliverystream/test",
            logging_destination_type=LoggingDestinationType.KINESIS_FIREHOSE
        )
        self.assertEqual(lc.acl_id, "acl-123")
        self.assertEqual(lc.logging_destination_type, LoggingDestinationType.KINESIS_FIREHOSE)
        self.assertTrue(lc.enabled)


class TestWAFIntegration(unittest.TestCase):
    """Test WAFIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_wafv2_client = MagicMock()
        self.mock_waf_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        self.mock_logs_client = MagicMock()
        self.mock_firehose_client = MagicMock()
        self.mock_fms_client = MagicMock()
        self.mock_s3_client = MagicMock()

        with patch.object(WAFIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = WAFIntegration()
            self.integration.region_name = "us-east-1"
            self.integration.profile_name = None
            self.integration.endpoint_url = None
            self.integration._clients = {
                'wafv2': self.mock_wafv2_client,
                'waf': self.mock_waf_client,
                'cloudwatch': self.mock_cloudwatch_client,
                'logs': self.mock_logs_client,
                'firehose': self.mock_firehose_client,
                'fms': self.mock_fms_client,
                's3': self.mock_s3_client
            }
            self.integration._resources = {}

    def test_wafv2_client_property(self):
        """Test wafv2_client property"""
        result = self.integration.wafv2_client
        self.assertEqual(result, self.mock_wafv2_client)

    def test_waf_classic_client_property(self):
        """Test waf_classic_client property"""
        result = self.integration.waf_classic_client
        self.assertEqual(result, self.mock_waf_client)

    def test_cloudwatch_client_property(self):
        """Test cloudwatch_client property"""
        result = self.integration.cloudwatch_client
        self.assertEqual(result, self.mock_cloudwatch_client)

    def test_logs_client_property(self):
        """Test logs_client property"""
        result = self.integration.logs_client
        self.assertEqual(result, self.mock_logs_client)

    def test_firehose_client_property(self):
        """Test firehose_client property"""
        result = self.integration.firehose_client
        self.assertEqual(result, self.mock_firehose_client)

    def test_fms_client_property(self):
        """Test fms_client property"""
        result = self.integration.fms_client
        self.assertEqual(result, self.mock_fms_client)

    def test_s3_client_property(self):
        """Test s3_client property"""
        result = self.integration.s3_client
        self.assertEqual(result, self.mock_s3_client)


class TestWAFWebACLManagement(unittest.TestCase):
    """Test WAF Web ACL management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_wafv2_client = MagicMock()

        with patch.object(WAFIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = WAFIntegration()
            self.integration._clients = {'wafv2': self.mock_wafv2_client}
            self.integration._resources = {}

    def test_create_web_acl(self):
        """Test creating a Web ACL"""
        mock_response = {
            'WebACL': {
                'Id': 'acl-123',
                'ARN': 'arn:aws:wafv2:us-east-1:123456789012:webacl/test-acl/acl-123',
                'Name': 'test-acl'
            }
        }
        self.mock_wafv2_client.create_web_acl.return_value = mock_response

        result = self.integration.create_web_acl(
            name="test-acl",
            scope=WebACLScope.REGIONAL,
            description="Test ACL"
        )

        self.assertEqual(result.name, "test-acl")
        self.assertEqual(result.acl_id, "acl-123")

    def test_create_web_acl_with_rules(self):
        """Test creating a Web ACL with rules"""
        mock_response = {
            'WebACL': {
                'Id': 'acl-123',
                'ARN': 'arn:aws:wafv2:us-east-1:123456789012:webacl/test-acl/acl-123',
                'Name': 'test-acl'
            }
        }
        self.mock_wafv2_client.create_web_acl.return_value = mock_response

        rules = [
            {
                'Name': 'rule-1',
                'Priority': 1,
                'Action': {'Allow': {}},
                'Statement': {
                    'ByteMatchStatement': {
                        'SearchString': 'test',
                        'FieldToMatch': {'SingleHeader': {'Name': 'user-agent'}},
                        'TextTransformations': [{'Priority': 0, 'Type': 'LOWERCASE'}],
                        'PositionalConstraint': 'CONTAINS'
                    }
                }
            }
        ]

        result = self.integration.create_web_acl(
            name="test-acl",
            scope=WebACLScope.REGIONAL,
            rules=rules
        )

        self.assertEqual(result.name, "test-acl")
        self.assertEqual(len(result.rules), 1)

    def test_get_web_acl(self):
        """Test getting a Web ACL"""
        mock_response = {
            'WebACL': {
                'Id': 'acl-123',
                'ARN': 'arn:aws:wafv2:us-east-1:123456789012:webacl/test-acl/acl-123',
                'Name': 'test-acl',
                'Scope': 'REGIONAL',
                'DefaultAction': {'Type': 'ALLOW'},
                'Rules': [],
                'CreatedAt': '2024-01-01T00:00:00Z',
                'UpdatedAt': '2024-01-01T00:00:00Z'
            }
        }
        self.mock_wafv2_client.get_web_acl.return_value = mock_response

        result = self.integration.get_web_acl("acl-123", WebACLScope.REGIONAL)

        self.assertEqual(result.acl_id, "acl-123")
        self.assertEqual(result.default_action, WebACLAction.ALLOW)

    def test_get_web_acl_not_found(self):
        """Test getting non-existent Web ACL"""
        error = Exception("WAFNonexistentItemException")
        error.response = {"Error": {"Code": "WAFNonexistentItemException"}}
        self.mock_wafv2_client.get_web_acl.side_effect = error

        result = self.integration.get_web_acl("non-existent", WebACLScope.REGIONAL)

        self.assertIsNone(result)

    def test_list_web_acls(self):
        """Test listing Web ACLs"""
        mock_response = {
            'WebACLs': [
                {'Id': 'acl-1', 'ARN': 'arn:aws:wafv2:us-east-1:123456789012:webacl/acl-1', 'Name': 'ACL 1'},
                {'Id': 'acl-2', 'ARN': 'arn:aws:wafv2:us-east-1:123456789012:webacl/acl-2', 'Name': 'ACL 2'}
            ]
        }
        self.mock_wafv2_client.list_web_acls.return_value = mock_response

        result = self.integration.list_web_acls(WebACLScope.REGIONAL)

        self.assertEqual(len(result), 2)

    def test_delete_web_acl(self):
        """Test deleting a Web ACL"""
        self.mock_wafv2_client.delete_web_acl.return_value = {}

        result = self.integration.delete_web_acl("acl-123", WebACLScope.REGIONAL)

        self.assertTrue(result)


class TestWAFRuleGroups(unittest.TestCase):
    """Test WAF rule group methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_wafv2_client = MagicMock()

        with patch.object(WAFIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = WAFIntegration()
            self.integration._clients = {'wafv2': self.mock_wafv2_client}
            self.integration._resources = {}

    def test_create_rule_group(self):
        """Test creating a rule group"""
        mock_response = {
            'RuleGroup': {
                'Id': 'rg-123',
                'ARN': 'arn:aws:wafv2:us-east-1:123456789012:rulegroup/test-rg/rg-123',
                'Name': 'test-rg',
                'Capacity': 50
            }
        }
        self.mock_wafv2_client.create_rule_group.return_value = mock_response

        result = self.integration.create_rule_group(
            name="test-rg",
            scope=WebACLScope.REGIONAL,
            capacity=50,
            description="Test rule group"
        )

        self.assertEqual(result.name, "test-rg")
        self.assertEqual(result.capacity, 50)

    def test_get_rule_group(self):
        """Test getting a rule group"""
        mock_response = {
            'RuleGroup': {
                'Id': 'rg-123',
                'ARN': 'arn:aws:wafv2:us-east-1:123456789012:rulegroup/test-rg/rg-123',
                'Name': 'test-rg',
                'Scope': 'REGIONAL',
                'Capacity': 50,
                'Rules': [],
                'CreatedAt': '2024-01-01T00:00:00Z',
                'UpdatedAt': '2024-01-01T00:00:00Z'
            }
        }
        self.mock_wafv2_client.get_rule_group.return_value = mock_response

        result = self.integration.get_rule_group("rg-123", WebACLScope.REGIONAL)

        self.assertEqual(result.rule_group_id, "rg-123")

    def test_list_rule_groups(self):
        """Test listing rule groups"""
        mock_response = {
            'RuleGroups': [
                {'Id': 'rg-1', 'ARN': 'arn:aws:wafv2:us-east-1:123456789012:rulegroup/rg-1', 'Name': 'RG 1'},
                {'Id': 'rg-2', 'ARN': 'arn:aws:wafv2:us-east-1:123456789012:rulegroup/rg-2', 'Name': 'RG 2'}
            ]
        }
        self.mock_wafv2_client.list_rule_groups.return_value = mock_response

        result = self.integration.list_rule_groups(WebACLScope.REGIONAL)

        self.assertEqual(len(result), 2)


class TestWAFIPSets(unittest.TestCase):
    """Test WAF IP set methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_wafv2_client = MagicMock()

        with patch.object(WAFIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = WAFIntegration()
            self.integration._clients = {'wafv2': self.mock_wafv2_client}
            self.integration._resources = {}

    def test_create_ip_set(self):
        """Test creating an IP set"""
        mock_response = {
            'IPSet': {
                'Id': 'ipset-123',
                'ARN': 'arn:aws:wafv2:us-east-1:123456789012:ipset/test-ipset/ipset-123',
                'Name': 'test-ipset',
                'Scope': 'REGIONAL',
                'IPAddressVersion': 'IPV4'
            }
        }
        self.mock_wafv2_client.create_ip_set.return_value = mock_response

        result = self.integration.create_ip_set(
            name="test-ipset",
            scope=WebACLScope.REGIONAL,
            addresses=["192.168.1.0/24"],
            description="Test IP set"
        )

        self.assertEqual(result.name, "test-ipset")
        self.assertEqual(len(result.addresses), 1)

    def test_get_ip_set(self):
        """Test getting an IP set"""
        mock_response = {
            'IPSet': {
                'Id': 'ipset-123',
                'ARN': 'arn:aws:wafv2:us-east-1:123456789012:ipset/test-ipset/ipset-123',
                'Name': 'test-ipset',
                'Scope': 'REGIONAL',
                'IPAddressVersion': 'IPV4',
                'Addresses': ['192.168.1.0/24']
            }
        }
        self.mock_wafv2_client.get_ip_set.return_value = mock_response

        result = self.integration.get_ip_set("ipset-123", WebACLScope.REGIONAL)

        self.assertEqual(result.ip_set_id, "ipset-123")

    def test_update_ip_set(self):
        """Test updating an IP set"""
        mock_response = {
            'IPSet': {
                'Id': 'ipset-123',
                'ARN': 'arn:aws:wafv2:us-east-1:123456789012:ipset/test-ipset/ipset-123',
                'Name': 'test-ipset',
                'Scope': 'REGIONAL'
            }
        }
        self.mock_wafv2_client.update_ip_set.return_value = mock_response

        result = self.integration.update_ip_set(
            ip_set_id="ipset-123",
            scope=WebACLScope.REGIONAL,
            addresses=["10.0.0.0/8"]
        )

        self.assertEqual(result.ip_set_id, "ipset-123")


class TestWAFRegexPatternSets(unittest.TestCase):
    """Test WAF regex pattern set methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_wafv2_client = MagicMock()

        with patch.object(WAFIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = WAFIntegration()
            self.integration._clients = {'wafv2': self.mock_wafv2_client}
            self.integration._resources = {}

    def test_create_regex_pattern_set(self):
        """Test creating a regex pattern set"""
        mock_response = {
            'RegexPatternSet': {
                'Id': 'rps-123',
                'ARN': 'arn:aws:wafv2:us-east-1:123456789012:regexpatternset/test-rps/rps-123',
                'Name': 'test-rps',
                'Scope': 'REGIONAL'
            }
        }
        self.mock_wafv2_client.create_regex_pattern_set.return_value = mock_response

        result = self.integration.create_regex_pattern_set(
            name="test-rps",
            scope=WebACLScope.REGIONAL,
            patterns=[r"\.evil\.com$"],
            description="Test regex pattern set"
        )

        self.assertEqual(result.name, "test-rps")

    def test_get_regex_pattern_set(self):
        """Test getting a regex pattern set"""
        mock_response = {
            'RegexPatternSet': {
                'Id': 'rps-123',
                'ARN': 'arn:aws:wafv2:us-east-1:123456789012:regexpatternset/test-rps/rps-123',
                'Name': 'test-rps',
                'Scope': 'REGIONAL',
                'RegularPatternList': [r"\.evil\.com$"]
            }
        }
        self.mock_wafv2_client.get_regex_pattern_set.return_value = mock_response

        result = self.integration.get_regex_pattern_set("rps-123", WebACLScope.REGIONAL)

        self.assertEqual(result.pattern_set_id, "rps-123")


class TestWAFLogging(unittest.TestCase):
    """Test WAF logging methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_wafv2_client = MagicMock()
        self.mock_logs_client = MagicMock()
        self.mock_firehose_client = MagicMock()

        with patch.object(WAFIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = WAFIntegration()
            self.integration._clients = {
                'wafv2': self.mock_wafv2_client,
                'logs': self.mock_logs_client,
                'firehose': self.mock_firehose_client
            }
            self.integration._resources = {}

    def test_associate_web_acl_with_resource(self):
        """Test associating a Web ACL with a resource"""
        self.mock_wafv2_client.associate_web_acl.return_value = {}

        result = self.integration.associate_web_acl(
            web_acl_id="acl-123",
            resource_arn="arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/test/abc",
            scope=WebACLScope.REGIONAL
        )

        self.assertTrue(result)

    def test_disassociate_web_acl(self):
        """Test disassociating a Web ACL"""
        self.mock_wafv2_client.disassociate_web_acl.return_value = {}

        result = self.integration.disassociate_web_acl(
            resource_arn="arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/test/abc",
            scope=WebACLScope.REGIONAL
        )

        self.assertTrue(result)

    def test_get_web_acl_for_resource(self):
        """Test getting Web ACL for a resource"""
        mock_response = {
            'WebACL': {
                'Id': 'acl-123',
                'ARN': 'arn:aws:wafv2:us-east-1:123456789012:webacl/test-acl/acl-123',
                'Name': 'test-acl'
            }
        }
        self.mock_wafv2_client.get_web_acl_for_resource.return_value = mock_response

        result = self.integration.get_web_acl_for_resource(
            resource_arn="arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/test/abc",
            scope=WebACLScope.REGIONAL
        )

        self.assertEqual(result.acl_id, "acl-123")


class TestWAFFirewallManager(unittest.TestCase):
    """Test WAF Firewall Manager integration methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_fms_client = MagicMock()

        with patch.object(WAFIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = WAFIntegration()
            self.integration._clients = {'fms': self.mock_fms_client}
            self.integration._resources = {}

    def test_get_admin_account(self):
        """Test getting Firewall Manager admin account"""
        mock_response = {
            'AdminAccount': {
                'AccountId': '123456789012',
                'RoleStatus': 'READY'
            }
        }
        self.mock_fms_client.get_admin_account.return_value = mock_response

        result = self.integration.get_firewall_manager_admin_account()

        self.assertEqual(result['AccountId'], '123456789012')

    def test_associate_web_acl_with_firewall_manager(self):
        """Test associating Web ACL with Firewall Manager"""
        self.mock_fms_client.put_web_acl.return_value = {}
        self.mock_fms_client associate_web_acl.return_value = {}

        result = self.integration.put_web_acl_for_firewall_manager(
            web_acl_id="acl-123",
            web_acl_arn="arn:aws:wafv2:us-east-1:123456789012:webacl/test-acl/acl-123",
            account_id="123456789012",
            resources=["arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/test/abc"]
        )

        self.assertTrue(result)


class TestWAFRateRules(unittest.TestCase):
    """Test WAF rate-based rule methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_wafv2_client = MagicMock()

        with patch.object(WAFIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = WAFIntegration()
            self.integration._clients = {'wafv2': self.mock_wafv2_client}
            self.integration._resources = {}

    def test_create_rate_rule(self):
        """Test creating a rate-based rule"""
        mock_response = {
            'Rule': {
                'Id': 'rate-rule-123',
                'ARN': 'arn:aws:wafv2:us-east-1:123456789012:rule/test-rule/rate-rule-123',
                'Name': 'test-rate-rule',
                'RuleId': 'rate-rule-123'
            }
        }
        self.mock_wafv2_client.create_rule.return_value = mock_response

        rule = RateRule(
            name="test-rate-rule",
            rate_limit=1000,
            scope=WebACLScope.REGIONAL,
            description="Rate limiting rule"
        )

        result = self.integration.create_rate_rule(rule)

        self.assertEqual(result.name, "test-rate-rule")


class TestWAFBotControl(unittest.TestCase):
    """Test WAF bot control methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_wafv2_client = MagicMock()

        with patch.object(WAFIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = WAFIntegration()
            self.integration._clients = {'wafv2': self.mock_wafv2_client}
            self.integration._resources = {}

    def test_create_bot_control_rule(self):
        """Test creating a bot control rule"""
        mock_response = {
            'Rule': {
                'Id': 'bc-rule-123',
                'ARN': 'arn:aws:wafv2:us-east-1:123456789012:rule/test-bc/bc-rule-123',
                'Name': 'test-bot-rule'
            }
        }
        self.mock_wafv2_client.create_rule.return_value = mock_response

        rule = BotControlRule(
            name="test-bot-rule",
            category=BotControlCategory.BOT,
            sensitivity_level="MEDIUM"
        )

        result = self.integration.create_bot_control_rule(rule, WebACLScope.REGIONAL)

        self.assertEqual(result.name, "test-bot-rule")


class TestWAFCloudWatchMonitoring(unittest.TestCase):
    """Test WAF CloudWatch monitoring methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudwatch_client = MagicMock()
        self.mock_wafv2_client = MagicMock()

        with patch.object(WAFIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = WAFIntegration()
            self.integration._clients = {
                'cloudwatch': self.mock_cloudwatch_client,
                'wafv2': self.mock_wafv2_client
            }
            self.integration._resources = {}

    def test_get_web_acl_metrics(self):
        """Test getting Web ACL metrics"""
        self.mock_cloudwatch_client.get_metric_statistics.return_value = {
            'Datapoints': [
                {'Average': 100.0, 'Sum': 1000.0, 'Timestamp': '2024-01-01T00:00:00Z'}
            ]
        }

        result = self.integration.get_web_acl_metrics("acl-123", WebACLScope.REGIONAL)

        self.assertIsNotNone(result)
        self.mock_cloudwatch_client.get_metric_statistics.assert_called()

    def test_list_web_acl_metrics(self):
        """Test listing Web ACL available metrics"""
        mock_response = {
            'Metrics': [
                {'MetricName': 'AllowedRequests'},
                {'MetricName': 'BlockedRequests'}
            ]
        }
        self.mock_wafv2_client.list_web_acl_metrics.return_value = mock_response

        result = self.integration.list_web_acl_metrics("acl-123", WebACLScope.REGIONAL)

        self.assertEqual(len(result), 2)


if __name__ == '__main__':
    unittest.main()

"""
Tests for workflow_aws_ses module
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

# Create mock boto3 module before importing workflow_aws_ses
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

# Now we can import the module
from src.workflow_aws_ses import (
    SESIntegration,
    IdentityType,
    VerificationStatus,
    EmailFormat,
    EmailIdentity,
    EmailTemplate,
    EmailMessage,
    ReceiptRule,
    ConfigurationSet,
    SNSSNotification,
)


class TestIdentityType(unittest.TestCase):
    """Test IdentityType enum"""

    def test_identity_type_values(self):
        self.assertEqual(IdentityType.EMAIL.value, "email")
        self.assertEqual(IdentityType.DOMAIN.value, "domain")

    def test_identity_type_count(self):
        self.assertEqual(len(IdentityType), 2)


class TestVerificationStatus(unittest.TestCase):
    """Test VerificationStatus enum"""

    def test_verification_status_values(self):
        self.assertEqual(VerificationStatus.PENDING.value, "Pending")
        self.assertEqual(VerificationStatus.SUCCESS.value, "Success")
        self.assertEqual(VerificationStatus.FAILED.value, "Failed")
        self.assertEqual(VerificationStatus.TEMPORARY_FAILURE.value, "TemporaryFailure")
        self.assertEqual(VerificationStatus.NOT_STARTED.value, "NotStarted")


class TestEmailFormat(unittest.TestCase):
    """Test EmailFormat enum"""

    def test_email_format_values(self):
        self.assertEqual(EmailFormat.PLAIN_TEXT.value, "Text")
        self.assertEqual(EmailFormat.HTML.value, "Html")


class TestEmailIdentity(unittest.TestCase):
    """Test EmailIdentity dataclass"""

    def test_email_identity_creation(self):
        identity = EmailIdentity(
            identity="test@example.com",
            identity_type=IdentityType.EMAIL,
            verification_status="Success",
            dkim_enabled=True
        )
        self.assertEqual(identity.identity, "test@example.com")
        self.assertTrue(identity.dkim_enabled)


class TestEmailTemplate(unittest.TestCase):
    """Test EmailTemplate dataclass"""

    def test_email_template_creation(self):
        template = EmailTemplate(
            name="welcome-template",
            subject="Welcome {{name}}",
            html_body="<h1>Hello {{name}}</h1>",
            text_body="Hello {{name}}"
        )
        self.assertEqual(template.name, "welcome-template")
        self.assertIn("{{name}}", template.subject)


class TestEmailMessage(unittest.TestCase):
    """Test EmailMessage dataclass"""

    def test_email_message_defaults(self):
        message = EmailMessage(
            source="sender@example.com",
            to_addresses=["recipient@example.com"],
            subject="Test Subject",
            body="Test Body"
        )
        self.assertEqual(message.body_format, EmailFormat.HTML)
        self.assertEqual(len(message.reply_to_addresses), 0)

    def test_email_message_with_attachments(self):
        message = EmailMessage(
            source="sender@example.com",
            to_addresses=["recipient@example.com"],
            subject="Test with Attachment",
            body="Test Body",
            attachments=[{"filename": "test.pdf", "data": "base64data"}]
        )
        self.assertEqual(len(message.attachments), 1)


class TestReceiptRule(unittest.TestCase):
    """Test ReceiptRule dataclass"""

    def test_receipt_rule_defaults(self):
        rule = ReceiptRule(name="test-rule", rule_set_name="test-ruleset")
        self.assertTrue(rule.enabled)
        self.assertEqual(rule.tls_policy, "Optional")
        self.assertEqual(rule.spam_threshold, 0.5)


class TestConfigurationSet(unittest.TestCase):
    """Test ConfigurationSet dataclass"""

    def test_configuration_set_defaults(self):
        config = ConfigurationSet(name="test-config")
        self.assertTrue(config.reputation_metrics_enabled)
        self.assertTrue(config.sending_enabled)


class TestSESIntegration(unittest.TestCase):
    """Test SESIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_ses_client = MagicMock()
        self.mock_sns_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()

        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.side_effect = [
            self.mock_ses_client,
            self.mock_sns_client,
            self.mock_cloudwatch_client,
        ]

    def test_integration_initialization(self):
        """Test SESIntegration initialization"""
        integration = SESIntegration(
            region_name="us-east-1"
        )
        self.assertEqual(integration.region_name, "us-east-1")

    def test_integration_with_credentials(self):
        """Test SESIntegration with explicit credentials"""
        integration = SESIntegration(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region_name="us-west-2"
        )
        self.assertEqual(integration.region_name, "us-west-2")

    def test_ses_client_property(self):
        """Test SES client property"""
        integration = SESIntegration()
        client = integration.ses_client
        self.assertIsNotNone(client)

    def test_sns_client_property(self):
        """Test SNS client property"""
        integration = SESIntegration()
        client = integration.sns_client
        self.assertIsNotNone(client)

    def test_cloudwatch_client_property(self):
        """Test CloudWatch client property"""
        integration = SESIntegration()
        client = integration.cloudwatch_client
        self.assertIsNotNone(client)

    # =========================================================================
    # Identity Management Tests
    # =========================================================================

    def test_verify_email_identity(self):
        """Test verifying an email identity"""
        integration = SESIntegration()
        self.mock_ses_client.verify_email_identity.return_value = {}

        result = integration.verify_email_identity("test@example.com")

        self.mock_ses_client.verify_email_identity.assert_called_once_with(
            EmailAddress="test@example.com"
        )

    def test_verify_domain_identity(self):
        """Test verifying a domain identity"""
        integration = SESIntegration()
        self.mock_ses_client.verify_domain_identity.return_value = {
            "VerificationToken": "token-123",
            "VerificationAttributes": {}
        }

        result = integration.verify_domain_identity("example.com")

        self.mock_ses_client.verify_domain_identity.assert_called_once_with(
            Domain="example.com"
        )

    def test_get_identity_verification_attributes(self):
        """Test getting identity verification attributes"""
        integration = SESIntegration()
        self.mock_ses_client.get_identity_verification_attributes.return_value = {
            "VerificationAttributes": {
                "test@example.com": {
                    "VerificationStatus": "Success",
                    "VerificationToken": "token-123"
                }
            }
        }

        result = integration.get_identity_verification_attributes(["test@example.com"])

        self.assertIn("test@example.com", result)

    def test_list_identities_all(self):
        """Test listing all identities"""
        integration = SESIntegration()
        self.mock_ses_client.list_identities.return_value = {
            "Identities": ["example.com", "test@example.com"]
        }

        result = integration.list_identities()

        self.assertEqual(len(result), 2)

    def test_list_identities_email_only(self):
        """Test listing email identities only"""
        integration = SESIntegration()
        self.mock_ses_client.list_identities.return_value = {
            "Identities": ["test@example.com"]
        }

        result = integration.list_identities(identity_type=IdentityType.EMAIL)

        call_kwargs = self.mock_ses_client.list_identities.call_args[1]
        self.assertEqual(call_kwargs["IdentityType"], "EmailAddress")

    def test_list_identities_domain_only(self):
        """Test listing domain identities only"""
        integration = SESIntegration()
        self.mock_ses_client.list_identities.return_value = {
            "Identities": ["example.com"]
        }

        result = integration.list_identities(identity_type=IdentityType.DOMAIN)

        call_kwargs = self.mock_ses_client.list_identities.call_args[1]
        self.assertEqual(call_kwargs["IdentityType"], "Domain")

    def test_delete_identity(self):
        """Test deleting an identity"""
        integration = SESIntegration()
        self.mock_ses_client.delete_identity.return_value = {}

        result = integration.delete_identity("test@example.com")

        self.assertTrue(result)
        self.mock_ses_client.delete_identity.assert_called_once_with(
            Identity="test@example.com"
        )

    def test_get_identity_attributes(self):
        """Test getting identity attributes"""
        integration = SESIntegration()
        self.mock_ses_client.get_identity_verification_attributes.return_value = {
            "VerificationAttributes": {
                "test@example.com": {
                    "VerificationStatus": "Success",
                    "DkimEnabled": True
                }
            }
        }

        result = integration.get_identity_attributes("test@example.com")

        self.assertIsNotNone(result)
        self.assertEqual(result.identity, "test@example.com")

    # =========================================================================
    # DKIM, SPF, DMARC Tests
    # =========================================================================

    def test_enable_dkim(self):
        """Test enabling DKIM"""
        integration = SESIntegration()
        self.mock_ses_client.enable_dkim.return_value = {
            "DkimTokens": ["token1", "token2", "token3"]
        }

        result = integration.enable_dkim("example.com")

        self.assertEqual(len(result), 3)
        self.mock_ses_client.enable_dkim.assert_called_once_with(Domain="example.com")

    def test_disable_dkim(self):
        """Test disabling DKIM"""
        integration = SESIntegration()
        self.mock_ses_client.disable_dkim.return_value = {}

        result = integration.disable_dkim("example.com")

        self.assertTrue(result)
        self.mock_ses_client.disable_dkim.assert_called_once_with(Domain="example.com")

    def test_get_dkim_attributes(self):
        """Test getting DKIM attributes"""
        integration = SESIntegration()
        self.mock_ses_client.get_dkim_attributes.return_value = {
            "DkimAttributes": {
                "example.com": {
                    "DkimEnabled": True,
                    "DkimVerificationStatus": "Success",
                    "DkimTokens": ["token1", "token2", "token3"]
                }
            }
        }

        result = integration.get_dkim_attributes("example.com")

        self.assertTrue(result["DkimEnabled"])

    def test_verify_dkim_signatures(self):
        """Test verifying DKIM signatures"""
        integration = SESIntegration()
        self.mock_ses_client.get_dkim_attributes.return_value = {
            "DkimAttributes": {
                "example.com": {
                    "DkimVerificationStatus": "Success"
                }
            }
        }

        result = integration.verify_dkim_signatures("example.com")

        self.assertEqual(result["DkimVerificationStatus"], "Success")

    def test_get_spf_attributes(self):
        """Test getting SPF attributes"""
        integration = SESIntegration()
        self.mock_ses_client.get_identity_mail_from_domain_attributes.return_value = {
            "Attributes": {
                "MXRecord": "feedback-smtp.us-east-1.amazonses.com",
                "SPFRecord": "v=spf1 include:amazonses.com ~all",
                "MailFromDomain": "mail.example.com",
                "IdentityMailFromDomainStatus": "Success"
            }
        }

        result = integration.get_spf_attributes("example.com")

        self.assertIn("SPFRecord", result)

    def test_set_mail_from_domain(self):
        """Test setting MAIL FROM domain"""
        integration = SESIntegration()
        self.mock_ses_client.set_identity_mail_from_domain.return_value = {}

        result = integration.set_mail_from_domain("example.com", "mail.example.com")

        self.assertTrue(result)
        self.mock_ses_client.set_identity_mail_from_domain.assert_called_once()

    def test_generate_dmarc_record(self):
        """Test generating DMARC record"""
        integration = SESIntegration()

        result = integration.generate_dmarc_record(
            domain="example.com",
            policy="quarantine",
            rua="mailto:rua@example.com",
            ruf="mailto:ruf@example.com",
            pct=100
        )

        self.assertEqual(result["name"], "_dmarc.example.com")
        self.assertIn("p=quarantine", result["value"])
        self.assertIn("rua=mailto:rua@example.com", result["value"])
        self.assertEqual(result["type"], "TXT")

    def test_setup_domain_for_ses(self):
        """Test complete domain setup for SES"""
        integration = SESIntegration()
        self.mock_ses_client.verify_domain_identity.return_value = {
            "VerificationToken": "token-123"
        }
        self.mock_ses_client.enable_dkim.return_value = {
            "DkimTokens": ["token1", "token2", "token3"]
        }
        self.mock_ses_client.set_identity_mail_from_domain.return_value = {}

        result = integration.setup_domain_for_ses("example.com")

        self.assertEqual(result["domain"], "example.com")
        self.assertEqual(len(result["dkim_tokens"]), 3)

    # =========================================================================
    # Email Templates Tests
    # =========================================================================

    def test_create_template(self):
        """Test creating an email template"""
        integration = SESIntegration()
        self.mock_ses_client.create_template.return_value = {}

        template = EmailTemplate(
            name="welcome-template",
            subject="Welcome {{name}}",
            html_body="<h1>Hello {{name}}</h1>"
        )

        result = integration.create_template(template)

        self.assertTrue(result)
        self.mock_ses_client.create_template.assert_called_once()

    def test_get_template(self):
        """Test getting an email template"""
        integration = SESIntegration()
        self.mock_ses_client.get_template.return_value = {
            "Template": {
                "TemplateName": "welcome-template",
                "Subject": "Welcome {{name}}",
                "Html": "<h1>Hello {{name}}</h1>",
                "Text": "Hello {{name}}"
            }
        }

        result = integration.get_template("welcome-template")

        self.assertIsNotNone(result)
        self.assertEqual(result.name, "welcome-template")

    def test_update_template(self):
        """Test updating an email template"""
        integration = SESIntegration()
        self.mock_ses_client.update_template.return_value = {}

        template = EmailTemplate(
            name="welcome-template",
            subject="Updated Welcome {{name}}",
            html_body="<h1>Updated Hello {{name}}</h1>"
        )

        result = integration.update_template(template)

        self.assertTrue(result)
        self.mock_ses_client.update_template.assert_called_once()

    def test_delete_template(self):
        """Test deleting an email template"""
        integration = SESIntegration()
        self.mock_ses_client.delete_template.return_value = {}

        result = integration.delete_template("welcome-template")

        self.assertTrue(result)
        self.mock_ses_client.delete_template.assert_called_once_with(
            TemplateName="welcome-template"
        )

    def test_list_templates(self):
        """Test listing email templates"""
        integration = SESIntegration()
        self.mock_ses_client.list_templates.return_value = {
            "TemplatesMetadata": [
                {"Name": "template-1", "CreatedTimestamp": datetime.now()},
                {"Name": "template-2", "CreatedTimestamp": datetime.now()}
            ]
        }

        result = integration.list_templates()

        self.assertEqual(len(result), 2)

    # =========================================================================
    # Email Sending Tests
    # =========================================================================

    def test_send_email(self):
        """Test sending an email"""
        integration = SESIntegration()
        self.mock_ses_client.send_email.return_value = {
            "MessageId": "msg-123456789"
        }

        message = EmailMessage(
            source="sender@example.com",
            to_addresses=["recipient@example.com"],
            subject="Test Email",
            body="<h1>Test</h1><p>This is a test email.</p>",
            body_format=EmailFormat.HTML
        )

        result = integration.send_email(message)

        self.assertEqual(result["MessageId"], "msg-123456789")

    def test_send_email_with_cc_bcc(self):
        """Test sending email with CC and BCC"""
        integration = SESIntegration()
        self.mock_ses_client.send_email.return_value = {
            "MessageId": "msg-123"
        }

        message = EmailMessage(
            source="sender@example.com",
            to_addresses=["recipient@example.com"],
            cc_addresses=["cc@example.com"],
            bcc_addresses=["bcc@example.com"],
            subject="Test with CC/BCC",
            body="Test body"
        )

        result = integration.send_email(message)

        call_kwargs = self.mock_ses_client.send_email.call_args[1]
        self.assertIn("cc@example.com", call_kwargs["Destination"]["CcAddresses"])
        self.assertIn("bcc@example.com", call_kwargs["Destination"]["BccAddresses"])

    def test_send_templated_email(self):
        """Test sending a templated email"""
        integration = SESIntegration()
        self.mock_ses_client.send_templated_email.return_value = {
            "MessageId": "msg-123456789"
        }

        result = integration.send_templated_email(
            source="sender@example.com",
            to_addresses=["recipient@example.com"],
            template_name="welcome-template",
            template_data={"name": "John"}
        )

        self.assertEqual(result["MessageId"], "msg-123456789")

    def test_send_raw_email(self):
        """Test sending a raw email"""
        integration = SESIntegration()
        self.mock_ses_client.send_raw_email.return_value = {
            "MessageId": "msg-raw-123"
        }

        raw_message = "From: sender@example.com\nTo: recipient@example.com\nSubject: Raw\n\nRaw email body"
        result = integration.send_raw_email(
            source="sender@example.com",
            raw_message=raw_message
        )

        self.assertEqual(result["MessageId"], "msg-raw-123")

    def test_send_bulk_templated_email(self):
        """Test sending bulk templated emails"""
        integration = SESIntegration()
        self.mock_ses_client.send_bulk_templated_email.return_value = {
            "Status": [
                {"Status": "Success", "MessageId": "msg-1"},
                {"Status": "Success", "MessageId": "msg-2"}
            ]
        }

        result = integration.send_bulk_templated_email(
            source="sender@example.com",
            template_name="welcome-template",
            default_template_data={"name": "Customer"},
            bulk_entries=[
                {"to_address": "customer1@example.com", "template_data": {"name": "Customer 1"}},
                {"to_address": "customer2@example.com", "template_data": {"name": "Customer 2"}}
            ]
        )

        self.assertEqual(len(result["Status"]), 2)

    # =========================================================================
    # Configuration Sets Tests
    # =========================================================================

    def test_create_configuration_set(self):
        """Test creating a configuration set"""
        integration = SESIntegration()
        self.mock_ses_client.create_configuration_set.return_value = {}

        config_set = ConfigurationSet(
            name="test-config",
            tracking_domain="track.example.com"
        )

        result = integration.create_configuration_set(config_set)

        self.assertTrue(result)

    def test_list_configuration_sets(self):
        """Test listing configuration sets"""
        integration = SESIntegration()
        self.mock_ses_client.list_configuration_sets.return_value = {
            "ConfigurationSets": [
                {"Name": "config-1"},
                {"Name": "config-2"}
            ]
        }

        result = integration.list_configuration_sets()

        self.assertEqual(len(result), 2)

    def test_delete_configuration_set(self):
        """Test deleting a configuration set"""
        integration = SESIntegration()
        self.mock_ses_client.delete_configuration_set.return_value = {}

        result = integration.delete_configuration_set("test-config")

        self.assertTrue(result)

    def test_set_configuration_set_tracking_options(self):
        """Test setting tracking options"""
        integration = SESIntegration()
        self.mock_ses_client.put_configuration_set_tracking_options.return_value = {}

        result = integration.set_configuration_set_tracking_options(
            "test-config",
            "track.example.com"
        )

        self.assertTrue(result)

    def test_set_reputation_metrics_enabled(self):
        """Test setting reputation metrics"""
        integration = SESIntegration()
        self.mock_ses_client.put_reputation_options.return_value = {}

        result = integration.set_reputation_metrics_enabled("test-config", True)

        self.assertTrue(result)

    def test_set_sending_enabled(self):
        """Test setting sending enabled"""
        integration = SESIntegration()
        self.mock_ses_client.put_account_sending_attributes.return_value = {}

        result = integration.set_sending_enabled("test-config", True)

        self.assertTrue(result)

    # =========================================================================
    # Receipt Rules Tests
    # =========================================================================

    def test_create_receipt_rule(self):
        """Test creating a receipt rule"""
        integration = SESIntegration()
        self.mock_ses_client.create_receipt_rule.return_value = {}

        rule = ReceiptRule(
            name="test-rule",
            rule_set_name="test-ruleset",
            recipients=["test@example.com"]
        )

        result = integration.create_receipt_rule(rule)

        self.assertTrue(result)

    def test_list_receipt_rules(self):
        """Test listing receipt rules"""
        integration = SESIntegration()
        self.mock_ses_client.describe_receipt_rule_set.return_value = {
            "Rules": [
                {"Name": "rule-1", "Enabled": True},
                {"Name": "rule-2", "Enabled": False}
            ]
        }

        result = integration.list_receipt_rules("test-ruleset")

        self.assertEqual(len(result), 2)

    def test_update_receipt_rule(self):
        """Test updating a receipt rule"""
        integration = SESIntegration()
        self.mock_ses_client.update_receipt_rule.return_value = {}

        rule = ReceiptRule(
            name="test-rule",
            rule_set_name="test-ruleset",
            enabled=False
        )

        result = integration.update_receipt_rule(rule)

        self.assertTrue(result)

    def test_delete_receipt_rule(self):
        """Test deleting a receipt rule"""
        integration = SESIntegration()
        self.mock_ses_client.delete_receipt_rule.return_value = {}

        result = integration.delete_receipt_rule("test-ruleset", "test-rule")

        self.assertTrue(result)

    # =========================================================================
    # SNS Notifications Tests
    # =========================================================================

    def test_set_sns_topic(self):
        """Test setting SNS topic for notifications"""
        integration = SESIntegration()
        self.mock_ses_client.set_identity_notification_topic.return_value = {}

        result = integration.set_sns_topic(
            identity="example.com",
            notification_type="Bounce",
            sns_topic_arn="arn:aws:sns:...:topic-123"
        )

        self.assertTrue(result)

    def test_set_sns_topic_without_topic(self):
        """Test disabling SNS notifications"""
        integration = SESIntegration()
        self.mock_ses_client.set_identity_notification_topic.return_value = {}

        result = integration.set_sns_topic(
            identity="example.com",
            notification_type="Complaint",
            sns_topic_arn=None
        )

        self.assertTrue(result)

    # =========================================================================
    # Statistics Tests
    # =========================================================================

    def test_get_send_statistics(self):
        """Test getting send statistics"""
        integration = SESIntegration()
        self.mock_ses_client.get_send_statistics.return_value = {
            "SendDataPoints": [
                {
                    "Timestamp": datetime.now(),
                    "DeliveryAttempts": 100,
                    "Bounces": 2,
                    "Complaints": 1,
                    "Rejects": 0
                }
            ]
        }

        result = integration.get_send_statistics()

        self.assertGreater(len(result), 0)

    def test_get_send_quota(self):
        """Test getting send quota"""
        integration = SESIntegration()
        self.mock_ses_client.get_send_quota.return_value = {
            "Max24HourSend": 100000.0,
            "MaxSendRate": 14.0,
            "SentLast24Hours": 5000.0
        }

        result = integration.get_send_quota()

        self.assertEqual(result["Max24HourSend"], 100000.0)

    # =========================================================================
    # Metrics Tests
    # =========================================================================

    def test_record_metric(self):
        """Test recording a metric"""
        integration = SESIntegration()

        integration._record_metric("TestMetric", 1.0, "Count", {"Key": "Value"})

        self.assertEqual(len(integration._metrics_buffer), 1)
        self.assertEqual(integration._metrics_buffer[0]["MetricName"], "TestMetric")

    def test_flush_metrics(self):
        """Test flushing metrics to CloudWatch"""
        integration = SESIntegration()
        self.mock_cloudwatch_client.put_metric_data.return_value = {}

        integration._record_metric("TestMetric", 1.0)
        integration.flush_metrics()

        self.assertEqual(len(integration._metrics_buffer), 0)
        self.mock_cloudwatch_client.put_metric_data.assert_called_once()


if __name__ == "__main__":
    unittest.main()

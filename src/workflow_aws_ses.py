"""
AWS SES (Simple Email Service) Integration Module for Workflow System

Implements an SESIntegration class with:
1. Identity management: Verify email domains and addresses
2. Email sending: Send templated and raw emails
3. Templates: Create and manage email templates
4. Receipt rules: Configure email receiving rules
5. Configuration sets: Manage configuration sets
6. Custom verified sender: Manage verified senders
7. Statistics: Get sending statistics
8. Domain verification: DKIM, SPF, DMARC setup
9. SNS notifications: Configure bounce/complaint notifications
10. CloudWatch integration: Sending metrics and monitoring

Commit: 'feat(aws-ses): add AWS SES with identity management, email sending, templates, receipt rules, configuration sets, domain verification, DKIM, SPF, DMARC, CloudWatch'
"""

import uuid
import json
import threading
import time
import logging
import email
import email.mime.multipart
import email.mime.text
import email.mime.base
import base64
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Type, Union
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum

try:
    import boto3
    from botocore.exceptions import (
        ClientError,
        BotoCoreError
    )
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None


logger = logging.getLogger(__name__)


class IdentityType(Enum):
    """SES identity types."""
    EMAIL = "email"
    DOMAIN = "domain"


class VerificationStatus(Enum):
    """Identity verification status."""
    PENDING = "Pending"
    SUCCESS = "Success"
    FAILED = "Failed"
    TEMPORARY_FAILURE = "TemporaryFailure"
    NOT_STARTED = "NotStarted"


class EmailFormat(Enum):
    """Email format types."""
    PLAIN_TEXT = "Text"
    HTML = "Html"


@dataclass
class EmailIdentity:
    """Email identity information."""
    identity: str
    identity_type: IdentityType
    verification_status: str
    verification_token: Optional[str] = None
    dkim_enabled: Optional[bool] = None
    dkim_verification_status: Optional[str] = None
    custom_verification_token: Optional[str] = None
    custom_verification_email_status: Optional[str] = None


@dataclass
class EmailTemplate:
    """Email template configuration."""
    name: str
    subject: str
    html_body: Optional[str] = None
    text_body: Optional[str] = None


@dataclass
class EmailMessage:
    """Configuration for sending an email."""
    source: str
    to_addresses: List[str]
    subject: str
    body: str
    body_format: EmailFormat = EmailFormat.HTML
    reply_to_addresses: List[str] = field(default_factory=list)
    cc_addresses: List[str] = field(default_factory=list)
    bcc_addresses: List[str] = field(default_factory=list)
    configuration_set_name: Optional[str] = None
    template_name: Optional[str] = None
    template_data: Dict[str, Any] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    attachments: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ReceiptRule:
    """Email receiving rule configuration."""
    name: str
    rule_set_name: str
    recipients: List[str] = field(default_factory=list)
    enabled: bool = True
    tls_policy: str = "Optional"
    spam_threshold: float = 0.5
    virus_scan_enabled: bool = False
    actions: List[Dict[str, Any]] = field(default_factory=list)
    scan_enabled: bool = False


@dataclass
class ConfigurationSet:
    """SES configuration set."""
    name: str
    tracking_domain: Optional[str] = None
    reputation_metrics_enabled: bool = True
    sending_enabled: bool = True
    last_fresh_start: Optional[str] = None


@dataclass
class SNSSNotification:
    """SNS notification configuration for bounces/complaints."""
    topic_arn: str
    notification_type: str
    include_original_headers: bool = False


class SESIntegration:
    """
    AWS SES integration class for email operations.
    
    Supports:
    - Email and domain identity verification
    - DKIM, SPF, DMARC setup and management
    - Templated and raw email sending
    - Email templates management
    - Receipt rules for incoming email handling
    - Configuration sets for tracking and metrics
    - SNS notifications for bounces and complaints
    - CloudWatch metrics and monitoring
    """
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        ses_client: Optional[Any] = None,
        sns_client: Optional[Any] = None,
        cloudwatch_client: Optional[Any] = None
    ):
        """
        Initialize SES integration.
        
        Args:
            aws_access_key_id: AWS access key ID (uses boto3 credentials if None)
            aws_secret_access_key: AWS secret access key (uses boto3 credentials if None)
            region_name: AWS region name
            endpoint_url: SES endpoint URL (for testing with LocalStack, etc.)
            ses_client: Pre-configured SES client (overrides boto3 creation)
            sns_client: Pre-configured SNS client for notifications
            cloudwatch_client: Pre-configured CloudWatch client for metrics
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SES integration. Install with: pip install boto3")
        
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self._ses_client = ses_client
        self._sns_client = sns_client
        self._cloudwatch_client = cloudwatch_client
        self._cloudwatch_namespace = "SES/Integration"
        self._lock = threading.RLock()
        self._verified_identities_cache: Dict[str, EmailIdentity] = {}
        
        session_kwargs = {
            "region_name": region_name
        }
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key
        
        self._session = boto3.Session(**session_kwargs)
        
        self._metrics_buffer: List[Dict[str, Any]] = []
        self._metrics_lock = threading.Lock()
    
    @property
    def ses_client(self):
        """Get or create SES client."""
        if self._ses_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._ses_client = self._session.client("ses", **kwargs)
        return self._ses_client
    
    @property
    def sns_client(self):
        """Get or create SNS client."""
        if self._sns_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._sns_client = self._session.client("sns", **kwargs)
        return self._sns_client
    
    @property
    def cloudwatch_client(self):
        """Get or create CloudWatch client."""
        if self._cloudwatch_client is None:
            kwargs = {"region_name": self.region_name}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._cloudwatch_client = self._session.client("cloudwatch", **kwargs)
        return self._cloudwatch_client
    
    def _record_metric(self, metric_name: str, value: float, unit: str = "Count", dimensions: Dict[str, str] = None):
        """Record a metric for CloudWatch."""
        metric = {
            "MetricName": metric_name,
            "Value": value,
            "Unit": unit,
            "Timestamp": datetime.utcnow().isoformat()
        }
        if dimensions:
            metric["Dimensions"] = [{"Name": k, "Value": v} for k, v in dimensions.items()]
        
        with self._metrics_lock:
            self._metrics_buffer.append(metric)
    
    def flush_metrics(self):
        """Flush buffered metrics to CloudWatch."""
        with self._metrics_lock:
            if not self._metrics_buffer:
                return
            
            try:
                self.cloudwatch_client.put_metric_data(
                    Namespace=self._cloudwatch_namespace,
                    MetricData=self._metrics_buffer
                )
                logger.info(f"Flushed {len(self._metrics_buffer)} metrics to CloudWatch")
                self._metrics_buffer.clear()
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to flush metrics to CloudWatch: {e}")
    
    # =========================================================================
    # Identity Management
    # =========================================================================
    
    def verify_email_identity(self, email_address: str) -> Dict[str, Any]:
        """
        Verify an email address identity.
        
        Args:
            email_address: Email address to verify
            
        Returns:
            Response containing verification token
        """
        try:
            response = self.ses_client.verify_email_identity(
                EmailAddress=email_address
            )
            logger.info(f"Verification email sent to: {email_address}")
            self._record_metric("IdentityVerificationsRequested", 1, "Count", {"Type": "Email"})
            return response
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to verify email identity {email_address}: {e}")
            raise
    
    def verify_domain_identity(self, domain: str) -> Dict[str, Any]:
        """
        Verify a domain identity.
        
        Args:
            domain: Domain name to verify
            
        Returns:
            Response containing verification tokens and DKIM tokens
        """
        try:
            response = self.ses_client.verify_domain_identity(
                Domain=domain
            )
            logger.info(f"Domain verification initiated for: {domain}")
            self._record_metric("IdentityVerificationsRequested", 1, "Count", {"Type": "Domain"})
            return response
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to verify domain identity {domain}: {e}")
            raise
    
    def get_identity_verification_attributes(self, identities: List[str]) -> Dict[str, Any]:
        """
        Get verification status for identities.
        
        Args:
            identities: List of email addresses or domains
            
        Returns:
            Verification attributes for each identity
        """
        try:
            response = self.ses_client.get_identity_verification_attributes(
                Identities=identities
            )
            return response.get("VerificationAttributes", {})
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get verification attributes: {e}")
            return {}
    
    def list_identities(self, identity_type: Optional[IdentityType] = None) -> List[str]:
        """
        List all identities (email addresses and domains).
        
        Args:
            identity_type: Filter by type (email or domain)
            
        Returns:
            List of identity strings
        """
        try:
            if identity_type == IdentityType.EMAIL:
                response = self.ses_client.list_identities(
                    IdentityType="EmailAddress"
                )
            elif identity_type == IdentityType.DOMAIN:
                response = self.ses_client.list_identities(
                    IdentityType="Domain"
                )
            else:
                response = self.ses_client.list_identities()
            
            return response.get("Identities", [])
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list identities: {e}")
            return []
    
    def delete_identity(self, identity: str) -> bool:
        """
        Delete an identity.
        
        Args:
            identity: Email address or domain to delete
            
        Returns:
            True if deleted successfully
        """
        try:
            self.ses_client.delete_identity(
                Identity=identity
            )
            with self._lock:
                if identity in self._verified_identities_cache:
                    del self._verified_identities_cache[identity]
            self._record_metric("IdentitiesDeleted", 1, "Count")
            logger.info(f"Deleted identity: {identity}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete identity {identity}: {e}")
            return False
    
    def get_identity_attributes(self, identity: str) -> Optional[EmailIdentity]:
        """
        Get attributes for an identity.
        
        Args:
            identity: Email address or domain
            
        Returns:
            EmailIdentity object or None
        """
        try:
            attrs = self.get_identity_verification_attributes([identity])
            if identity not in attrs:
                return None
            
            attr = attrs[identity]
            identity_type = IdentityType.DOMAIN if "@" not in identity else IdentityType.EMAIL
            
            return EmailIdentity(
                identity=identity,
                identity_type=identity_type,
                verification_status=attr.get("VerificationStatus", "Unknown"),
                verification_token=attr.get("VerificationToken"),
                dkim_enabled=attr.get("DkimEnabled"),
                dkim_verification_status=attr.get("DkimVerificationStatus")
            )
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get identity attributes for {identity}: {e}")
            return None
    
    # =========================================================================
    # DKIM, SPF, DMARC
    # =========================================================================
    
    def enable_dkim(self, domain: str) -> List[str]:
        """
        Enable DKIM signing for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            List of DKIM tokens (CNAME records to add)
        """
        try:
            response = self.ses_client.enable_dkim(
                Domain=domain
            )
            dkim_tokens = response.get("DkimTokens", [])
            logger.info(f"DKIM enabled for {domain}. Tokens: {dkim_tokens}")
            self._record_metric("DKIMEnabled", 1, "Count", {"Domain": domain})
            return dkim_tokens
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to enable DKIM for {domain}: {e}")
            raise
    
    def disable_dkim(self, domain: str) -> bool:
        """
        Disable DKIM signing for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            True if disabled successfully
        """
        try:
            self.ses_client.disable_dkim(
                Domain=domain
            )
            logger.info(f"DKIM disabled for {domain}")
            self._record_metric("DKIMDisabled", 1, "Count", {"Domain": domain})
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to disable DKIM for {domain}: {e}")
            return False
    
    def get_dkim_attributes(self, domain: str) -> Dict[str, Any]:
        """
        Get DKIM attributes for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            DKIM attributes including tokens and verification status
        """
        try:
            response = self.ses_client.get_dkim_attributes(
                Identities=[domain]
            )
            return response.get("DkimAttributes", {}).get(domain, {})
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get DKIM attributes for {domain}: {e}")
            return {}
    
    def verify_dkim_signatures(self, domain: str) -> Dict[str, Any]:
        """
        Verify DKIM signature setup (check DNS records).
        
        Args:
            domain: Domain name
            
        Returns:
            DKIM attributes showing verification status
        """
        return self.get_dkim_attributes(domain)
    
    def get_spf_attributes(self, domain: str) -> Dict[str, str]:
        """
        Get SPF verification attributes for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            SPF attributes including the SPF record value
        """
        try:
            response = self.ses_client.get_identity_mail_from_domain_attributes(
                Identity=domain
            )
            return {
                "MXRecord": response.get("Attributes", {}).get("MXRecord"),
                "SPFRecord": response.get("Attributes", {}).get("SPFRecord"),
                "MailFromDomain": response.get("Attributes", {}).get("MailFromDomain"),
                "Status": response.get("Attributes", {}).get("IdentityMailFromDomainStatus")
            }
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get SPF attributes for {domain}: {e}")
            return {}
    
    def set_mail_from_domain(
        self,
        domain: str,
        mail_from_domain: str,
        behavior_on_mx_failure: str = "UseDefaultValue"
    ) -> bool:
        """
        Set a custom MAIL FROM domain for SPF.
        
        Args:
            domain: Domain name
            mail_from_domain: Custom MAIL FROM subdomain
            behavior_on_mx_failure: Behavior when MX fails
            
        Returns:
            True if successful
        """
        try:
            self.ses_client.set_identity_mail_from_domain(
                Identity=domain,
                MailFromDomain=mail_from_domain,
                BehaviorOnMXFailure=behavior_on_mx_failure
            )
            logger.info(f"Set MAIL FROM domain to {mail_from_domain} for {domain}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to set MAIL FROM domain for {domain}: {e}")
            return False
    
    def generate_dmarc_record(
        self,
        domain: str,
        policy: str = "none",
        rua: Optional[str] = None,
        ruf: Optional[str] = None,
        pct: int = 100
    ) -> Dict[str, str]:
        """
        Generate DMARC DNS record for a domain.
        
        Args:
            domain: Domain name
            policy: DMARC policy (none, quarantine, reject)
            rua: URI for aggregate reports
            ruf: URI for forensic reports
            pct: Percentage of messages to apply policy to
            
        Returns:
            DMARC DNS record (name, value, type)
        """
        record = f"v=DMARC1; p={policy}; pct={pct}"
        
        if rua:
            record += f"; rua={rua}"
        if ruf:
            record += f"; ruf={ruf}"
        
        dmarc_domain = f"_dmarc.{domain}"
        
        return {
            "name": dmarc_domain,
            "value": record,
            "type": "TXT"
        }
    
    def setup_domain_for_ses(self, domain: str) -> Dict[str, Any]:
        """
        Complete domain setup for SES (verification + DKIM).
        
        Args:
            domain: Domain name
            
        Returns:
            Setup results including verification tokens and DKIM tokens
        """
        results = {
            "domain": domain,
            "verification_token": None,
            "dkim_tokens": [],
            "spf_record": None,
            "errors": []
        }
        
        try:
            verify_response = self.verify_domain_identity(domain)
            results["verification_token"] = verify_response.get("VerificationToken")
        except Exception as e:
            results["errors"].append(f"Domain verification failed: {e}")
        
        try:
            dkim_response = self.enable_dkim(domain)
            results["dkim_tokens"] = dkim_response
        except Exception as e:
            results["errors"].append(f"DKIM enable failed: {e}")
        
        try:
            mail_from = f"mail.{domain}"
            self.set_mail_from_domain(domain, mail_from)
            results["spf_record"] = f"v=spf1 include:amazonses.com ~all"
        except Exception as e:
            results["errors"].append(f"SPF setup failed: {e}")
        
        return results
    
    # =========================================================================
    # Email Templates
    # =========================================================================
    
    def create_template(self, template: EmailTemplate) -> bool:
        """
        Create an email template.
        
        Args:
            template: EmailTemplate configuration
            
        Returns:
            True if created successfully
        """
        try:
            kwargs = {
                "TemplateName": template.name,
                "Subject": template.subject
            }
            if template.html_body:
                kwargs["Html"] = template.html_body
            if template.text_body:
                kwargs["Text"] = template.text_body
            
            self.ses_client.create_template(Template=kwargs)
            logger.info(f"Created email template: {template.name}")
            self._record_metric("TemplatesCreated", 1, "Count")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create template {template.name}: {e}")
            return False
    
    def get_template(self, template_name: str) -> Optional[EmailTemplate]:
        """
        Get an email template.
        
        Args:
            template_name: Name of the template
            
        Returns:
            EmailTemplate object or None
        """
        try:
            response = self.ses_client.get_template(
                TemplateName=template_name
            )
            template_data = response.get("Template", {})
            return EmailTemplate(
                name=template_data.get("TemplateName"),
                subject=template_data.get("Subject"),
                html_body=template_data.get("Html"),
                text_body=template_data.get("Text")
            )
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get template {template_name}: {e}")
            return None
    
    def update_template(self, template: EmailTemplate) -> bool:
        """
        Update an email template.
        
        Args:
            template: EmailTemplate with updated values
            
        Returns:
            True if updated successfully
        """
        try:
            kwargs = {
                "TemplateName": template.name,
                "Subject": template.subject
            }
            if template.html_body:
                kwargs["Html"] = template.html_body
            if template.text_body:
                kwargs["Text"] = template.text_body
            
            self.ses_client.update_template(Template=kwargs)
            logger.info(f"Updated email template: {template.name}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to update template {template.name}: {e}")
            return False
    
    def delete_template(self, template_name: str) -> bool:
        """
        Delete an email template.
        
        Args:
            template_name: Name of the template
            
        Returns:
            True if deleted successfully
        """
        try:
            self.ses_client.delete_template(
                TemplateName=template_name
            )
            logger.info(f"Deleted email template: {template_name}")
            self._record_metric("TemplatesDeleted", 1, "Count")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete template {template_name}: {e}")
            return False
    
    def list_templates(self) -> List[Dict[str, Any]]:
        """
        List all email templates.
        
        Returns:
            List of template metadata
        """
        try:
            response = self.ses_client.list_templates()
            return response.get("TemplatesMetadata", [])
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list templates: {e}")
            return []
    
    # =========================================================================
    # Email Sending
    # =========================================================================
    
    def send_email(self, message: EmailMessage) -> Dict[str, Any]:
        """
        Send an email.
        
        Args:
            message: EmailMessage configuration
            
        Returns:
            Response containing MessageId
        """
        try:
            kwargs = {
                "Source": message.source,
                "Destination": {
                    "ToAddresses": message.to_addresses,
                },
                "Message": {
                    "Subject": {
                        "Data": message.subject,
                        "Charset": "UTF-8"
                    },
                    "Body": {}
                }
            }
            
            if message.body_format == EmailFormat.HTML:
                kwargs["Message"]["Body"]["Html"] = {
                    "Data": message.body,
                    "Charset": "UTF-8"
                }
            else:
                kwargs["Message"]["Body"]["Text"] = {
                    "Data": message.body,
                    "Charset": "UTF-8"
                }
            
            if message.reply_to_addresses:
                kwargs["ReplyToAddresses"] = message.reply_to_addresses
            
            if message.cc_addresses:
                kwargs["Destination"]["CcAddresses"] = message.cc_addresses
            
            if message.bcc_addresses:
                kwargs["Destination"]["BccAddresses"] = message.bcc_addresses
            
            if message.configuration_set_name:
                kwargs["ConfigurationSetName"] = message.configuration_set_name
            
            if message.template_name:
                kwargs["Template"] = message.template_name
                kwargs["TemplateData"] = json.dumps(message.template_data)
            
            if message.headers:
                kwargs["Headers"] = message.headers
            
            response = self.ses_client.send_email(**kwargs)
            
            self._record_metric("EmailsSent", 1, "Count", {"ConfigurationSet": message.configuration_set_name or "Default"})
            
            logger.info(f"Sent email from {message.source} to {message.to_addresses}")
            return response
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to send email from {message.source}: {e}")
            self._record_metric("EmailSendErrors", 1, "Count")
            raise
    
    def send_templated_email(
        self,
        source: str,
        to_addresses: List[str],
        template_name: str,
        template_data: Dict[str, Any],
        reply_to_addresses: List[str] = None,
        cc_addresses: List[str] = None,
        bcc_addresses: List[str] = None,
        configuration_set_name: str = None
    ) -> Dict[str, Any]:
        """
        Send a templated email.
        
        Args:
            source: Sender email address
            to_addresses: List of recipient addresses
            template_name: Name of the SES template
            template_data: Dictionary of template variables
            reply_to_addresses: Reply-to addresses
            cc_addresses: CC addresses
            bcc_addresses: BCC addresses
            configuration_set_name: Configuration set name
            
        Returns:
            Response containing MessageId
        """
        try:
            kwargs = {
                "Source": source,
                "Destination": {
                    "ToAddresses": to_addresses
                },
                "Template": template_name,
                "TemplateData": json.dumps(template_data)
            }
            
            if reply_to_addresses:
                kwargs["ReplyToAddresses"] = reply_to_addresses
            
            if cc_addresses:
                kwargs["Destination"]["CcAddresses"] = cc_addresses
            
            if bcc_addresses:
                kwargs["Destination"]["BccAddresses"] = bcc_addresses
            
            if configuration_set_name:
                kwargs["ConfigurationSetName"] = configuration_set_name
            
            response = self.ses_client.send_templated_email(**kwargs)
            
            self._record_metric("TemplatedEmailsSent", 1, "Count", {"Template": template_name})
            
            logger.info(f"Sent templated email using template {template_name}")
            return response
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to send templated email: {e}")
            self._record_metric("EmailSendErrors", 1, "Count")
            raise
    
    def send_raw_email(
        self,
        source: str,
        raw_message: Union[str, bytes],
        to_addresses: List[str] = None,
        configuration_set_name: str = None
    ) -> Dict[str, Any]:
        """
        Send a raw email (for custom MIME messages).
        
        Args:
            source: Sender email address
            raw_message: Raw email content (MIME message)
            to_addresses: Recipients (extracted from raw if not provided)
            configuration_set_name: Configuration set name
            
        Returns:
            Response containing MessageId
        """
        try:
            kwargs = {
                "Source": source,
                "RawMessage": {
                    "Data": raw_message if isinstance(raw_message, bytes) else raw_message.encode("utf-8")
                }
            }
            
            if to_addresses:
                kwargs["Destinations"] = to_addresses
            
            if configuration_set_name:
                kwargs["ConfigurationSetName"] = configuration_set_name
            
            response = self.ses_client.send_raw_email(**kwargs)
            
            self._record_metric("RawEmailsSent", 1, "Count")
            
            logger.info(f"Sent raw email from {source}")
            return response
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to send raw email: {e}")
            self._record_metric("EmailSendErrors", 1, "Count")
            raise
    
    def send_bulk_templated_email(
        self,
        source: str,
        template_name: str,
        default_template_data: Dict[str, Any],
        bulk_entries: List[Dict[str, Any]],
        configuration_set_name: str = None
    ) -> Dict[str, Any]:
        """
        Send bulk templated emails.
        
        Args:
            source: Sender email address
            template_name: Name of the SES template
            default_template_data: Default template variables
            bulk_entries: List of entries with destination and template data
            configuration_set_name: Configuration set name
            
        Returns:
            Response containing results for each recipient
        """
        try:
            kwargs = {
                "Source": source,
                "Template": template_name,
                "DefaultTemplateData": json.dumps(default_template_data),
                "BulkTemplatedEmailEntries": [
                    {
                        "Destination": {
                            "ToAddresses": [entry["to_address"]]
                        },
                        "ReplacementTemplateData": json.dumps(entry.get("template_data", {}))
                    }
                    for entry in bulk_entries
                ]
            }
            
            if configuration_set_name:
                kwargs["ConfigurationSetName"] = configuration_set_name
            
            response = self.ses_client.send_bulk_templated_email(**kwargs)
            
            self._record_metric("BulkEmailsSent", len(bulk_entries), "Count")
            
            logger.info(f"Sent bulk email to {len(bulk_entries)} recipients")
            return response
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to send bulk templated email: {e}")
            self._record_metric("EmailSendErrors", 1, "Count")
            raise
    
    # =========================================================================
    # Configuration Sets
    # =========================================================================
    
    def create_configuration_set(self, config_set: ConfigurationSet) -> bool:
        """
        Create a configuration set.
        
        Args:
            config_set: ConfigurationSet configuration
            
        Returns:
            True if created successfully
        """
        try:
            self.ses_client.create_configuration_set(
                ConfigurationSet={
                    "Name": config_set.name
                }
            )
            
            if config_set.tracking_domain:
                self.set_configuration_set_tracking_options(
                    config_set.name,
                    config_set.tracking_domain
                )
            
            if not config_set.reputation_metrics_enabled:
                self.set_reputation_metrics_enabled(config_set.name, False)
            
            if not config_set.sending_enabled:
                self.set_sending_enabled(config_set.name, False)
            
            logger.info(f"Created configuration set: {config_set.name}")
            self._record_metric("ConfigurationSetsCreated", 1, "Count")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create configuration set {config_set.name}: {e}")
            return False
    
    def delete_configuration_set(self, config_set_name: str) -> bool:
        """
        Delete a configuration set.
        
        Args:
            config_set_name: Name of the configuration set
            
        Returns:
            True if deleted successfully
        """
        try:
            self.ses_client.delete_configuration_set(
                ConfigurationSetName=config_set_name
            )
            logger.info(f"Deleted configuration set: {config_set_name}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete configuration set {config_set_name}: {e}")
            return False
    
    def list_configuration_sets(self) -> List[str]:
        """
        List all configuration sets.
        
        Returns:
            List of configuration set names
        """
        try:
            response = self.ses_client.list_configuration_sets()
            return [cs["Name"] for cs in response.get("ConfigurationSets", [])]
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list configuration sets: {e}")
            return []
    
    def set_configuration_set_tracking_options(
        self,
        config_set_name: str,
        tracking_domain: str
    ) -> bool:
        """
        Set custom tracking domain for a configuration set.
        
        Args:
            config_set_name: Configuration set name
            tracking_domain: Custom tracking subdomain
            
        Returns:
            True if successful
        """
        try:
            self.ses_client.set_configuration_set_tracking_options(
                ConfigurationSetName=config_set_name,
                TrackingOptions={
                    "CustomRedirectDomain": tracking_domain
                }
            )
            logger.info(f"Set tracking domain {tracking_domain} for {config_set_name}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to set tracking options: {e}")
            return False
    
    def set_reputation_metrics_enabled(
        self,
        config_set_name: str,
        enabled: bool = True
    ) -> bool:
        """
        Enable or disable reputation metrics for a configuration set.
        
        Args:
            config_set_name: Configuration set name
            enabled: Whether to enable metrics
            
        Returns:
            True if successful
        """
        try:
            self.ses_client.set_reputation_metrics_enabled(
                ConfigurationSetName=config_set_name,
                Enabled=enabled
            )
            logger.info(f"Set reputation metrics {'enabled' if enabled else 'disabled'} for {config_set_name}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to set reputation metrics: {e}")
            return False
    
    def set_sending_enabled(
        self,
        config_set_name: str,
        enabled: bool = True
    ) -> bool:
        """
        Enable or disable sending for a configuration set.
        
        Args:
            config_set_name: Configuration set name
            enabled: Whether to enable sending
            
        Returns:
            True if successful
        """
        try:
            self.ses_client.set_sending_enabled(
                ConfigurationSetName=config_set_name,
                Enabled=enabled
            )
            logger.info(f"Set sending {'enabled' if enabled else 'disabled'} for {config_set_name}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to set sending enabled: {e}")
            return False
    
    def add_event_destination(
        self,
        config_set_name: str,
        event_destination_name: str,
        enabled: bool = True,
        event_types: List[str] = None,
        sns_topic_arn: str = None,
        cloudwatch_destination: Dict[str, Any] = None,
        kinesis_firehose_destination: Dict[str, Any] = None
    ) -> bool:
        """
        Add an event destination to a configuration set.
        
        Args:
            config_set_name: Configuration set name
            event_destination_name: Name of the event destination
            enabled: Whether the destination is enabled
            event_types: List of event types (send, bounce, complaint, delivery, etc.)
            sns_topic_arn: SNS topic ARN for notifications
            cloudwatch_destination: CloudWatch destination configuration
            kinesis_firehose_destination: Kinesis Firehose destination configuration
            
        Returns:
            True if successful
        """
        try:
            if event_types is None:
                event_types = ["send", "renderFailure", "bounce", "complaint", "delivery"]
            
            event_destination = {
                "Name": event_destination_name,
                "Enabled": enabled,
                "MatchingEventTypes": event_types
            }
            
            if sns_topic_arn:
                event_destination["SnsDestination"] = {
                    "TopicARN": sns_topic_arn
                }
            
            if cloudwatch_destination:
                event_destination["CloudWatchDestination"] = cloudwatch_destination
            
            if kinesis_firehose_destination:
                event_destination["KinesisFirehoseDestination"] = kinesis_firehose_destination
            
            self.ses_client.create_configuration_set_event_destination(
                ConfigurationSetName=config_set_name,
                EventDestination=event_destination
            )
            
            logger.info(f"Added event destination {event_destination_name} to {config_set_name}")
            self._record_metric("EventDestinationsCreated", 1, "Count")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to add event destination: {e}")
            return False
    
    # =========================================================================
    # Receipt Rules
    # =========================================================================
    
    def create_receipt_rule_set(self, rule_set_name: str) -> bool:
        """
        Create a receipt rule set.
        
        Args:
            rule_set_name: Name of the rule set
            
        Returns:
            True if created successfully
        """
        try:
            self.ses_client.create_receipt_rule_set(
                RuleSetName=rule_set_name
            )
            logger.info(f"Created receipt rule set: {rule_set_name}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create receipt rule set {rule_set_name}: {e}")
            return False
    
    def delete_receipt_rule_set(self, rule_set_name: str) -> bool:
        """
        Delete a receipt rule set.
        
        Args:
            rule_set_name: Name of the rule set
            
        Returns:
            True if deleted successfully
        """
        try:
            self.ses_client.delete_receipt_rule_set(
                RuleSetName=rule_set_name
            )
            logger.info(f"Deleted receipt rule set: {rule_set_name}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete receipt rule set {rule_set_name}: {e}")
            return False
    
    def list_receipt_rule_sets(self) -> List[Dict[str, Any]]:
        """
        List all receipt rule sets.
        
        Returns:
            List of rule set metadata
        """
        try:
            response = self.ses_client.list_receipt_rule_sets()
            return response.get("RuleSets", [])
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list receipt rule sets: {e}")
            return []
    
    def create_receipt_rule(self, rule: ReceiptRule) -> bool:
        """
        Create a receipt rule.
        
        Args:
            rule: ReceiptRule configuration
            
        Returns:
            True if created successfully
        """
        try:
            rule_dict = {
                "Name": rule.name,
                "Enabled": rule.enabled,
                "TlsPolicy": rule.tls_policy,
                "Actions": rule.actions
            }
            
            if rule.recipients:
                rule_dict["Recipients"] = rule.recipients
            
            if hasattr(rule, 'scan_enabled'):
                rule_dict["ScanEnabled"] = rule.scan_enabled
            
            self.ses_client.create_receipt_rule(
                RuleSetName=rule.rule_set_name,
                Rule=rule_dict
            )
            
            logger.info(f"Created receipt rule: {rule.name}")
            self._record_metric("ReceiptRulesCreated", 1, "Count")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create receipt rule {rule.name}: {e}")
            return False
    
    def delete_receipt_rule(self, rule_name: str, rule_set_name: str) -> bool:
        """
        Delete a receipt rule.
        
        Args:
            rule_name: Name of the rule
            rule_set_name: Name of the rule set
            
        Returns:
            True if deleted successfully
        """
        try:
            self.ses_client.delete_receipt_rule(
                RuleName=rule_name,
                RuleSetName=rule_set_name
            )
            logger.info(f"Deleted receipt rule: {rule_name}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete receipt rule {rule_name}: {e}")
            return False
    
    def update_receipt_rule(
        self,
        rule_name: str,
        rule_set_name: str,
        rule: Dict[str, Any]
    ) -> bool:
        """
        Update a receipt rule.
        
        Args:
            rule_name: Name of the rule
            rule_set_name: Name of the rule set
            rule: Updated rule configuration
            
        Returns:
            True if updated successfully
        """
        try:
            self.ses_client.update_receipt_rule(
                RuleSetName=rule_set_name,
                Rule=rule
            )
            logger.info(f"Updated receipt rule: {rule_name}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to update receipt rule {rule_name}: {e}")
            return False
    
    def list_receipt_rules(self, rule_set_name: str) -> List[Dict[str, Any]]:
        """
        List all receipt rules in a rule set.
        
        Args:
            rule_set_name: Name of the rule set
            
        Returns:
            List of rule configurations
        """
        try:
            response = self.ses_client.list_receipt_rules(
                RuleSetName=rule_set_name
            )
            return response.get("Rules", [])
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list receipt rules for {rule_set_name}: {e}")
            return []
    
    def set_active_receipt_rule_set(self, rule_set_name: str) -> bool:
        """
        Set the active receipt rule set.
        
        Args:
            rule_set_name: Name of the rule set to make active
            
        Returns:
            True if successful
        """
        try:
            self.ses_client.set_active_receipt_rule_set(
                RuleSetName=rule_set_name
            )
            logger.info(f"Set active receipt rule set: {rule_set_name}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to set active receipt rule set {rule_set_name}: {e}")
            return False
    
    # =========================================================================
    # SNS Notifications
    # =========================================================================
    
    def configure_sns_notifications(
        self,
        topic_arn: str,
        notification_types: List[str] = None,
        include_original_headers: bool = False
    ) -> Dict[str, Any]:
        """
        Configure SNS notifications for bounce and complaint.
        
        Args:
            topic_arn: SNS topic ARN
            notification_types: Types of notifications (bounce, complaint, delivery)
            include_original_headers: Whether to include original headers
            
        Returns:
            Configuration results
        """
        if notification_types is None:
            notification_types = ["bounce", "complaint"]
        
        results = {}
        
        for notif_type in notification_types:
            try:
                self.ses_client.set_identity_notification_topic(
                    Identity=topic_arn.split(":")[5] if ":" in topic_arn else topic_arn,
                    NotificationType=notif_type.title(),
                    SnsTopic=topic_arn
                )
                results[notif_type] = {"success": True, "topic_arn": topic_arn}
            except Exception as e:
                results[notif_type] = {"success": False, "error": str(e)}
        
        if include_original_headers:
            try:
                self.ses_client.set_identity_headers_in_notifications_enabled(
                    Identity=topic_arn.split(":")[5] if ":" in topic_arn else topic_arn,
                    Enabled=True
                )
            except Exception as e:
                logger.warning(f"Failed to set headers in notifications: {e}")
        
        logger.info(f"Configured SNS notifications for topic: {topic_arn}")
        return results
    
    def configure_bounce_notifications(
        self,
        identity: str,
        topic_arn: str,
        include_original_headers: bool = False
    ) -> bool:
        """
        Configure SNS notifications for bounces.
        
        Args:
            identity: Email address or domain
            topic_arn: SNS topic ARN
            include_original_headers: Whether to include original headers
            
        Returns:
            True if successful
        """
        try:
            self.ses_client.set_identity_notification_topic(
                Identity=identity,
                NotificationType="Bounce",
                SnsTopic=topic_arn
            )
            
            self.ses_client.set_identity_headers_in_notifications_enabled(
                Identity=identity,
                Enabled=include_original_headers
            )
            
            logger.info(f"Configured bounce notifications for {identity}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to configure bounce notifications: {e}")
            return False
    
    def configure_complaint_notifications(
        self,
        identity: str,
        topic_arn: str,
        include_original_headers: bool = False
    ) -> bool:
        """
        Configure SNS notifications for complaints.
        
        Args:
            identity: Email address or domain
            topic_arn: SNS topic ARN
            include_original_headers: Whether to include original headers
            
        Returns:
            True if successful
        """
        try:
            self.ses_client.set_identity_notification_topic(
                Identity=identity,
                NotificationType="Complaint",
                SnsTopic=topic_arn
            )
            
            self.ses_client.set_identity_headers_in_notifications_enabled(
                Identity=identity,
                Enabled=include_original_headers
            )
            
            logger.info(f"Configured complaint notifications for {identity}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to configure complaint notifications: {e}")
            return False
    
    def configure_delivery_notifications(
        self,
        identity: str,
        topic_arn: str
    ) -> bool:
        """
        Configure SNS notifications for successful deliveries.
        
        Args:
            identity: Email address or domain
            topic_arn: SNS topic ARN
            
        Returns:
            True if successful
        """
        try:
            self.ses_client.set_identity_notification_topic(
                Identity=identity,
                NotificationType="Delivery",
                SnsTopic=topic_arn
            )
            
            logger.info(f"Configured delivery notifications for {identity}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to configure delivery notifications: {e}")
            return False
    
    # =========================================================================
    # Statistics
    # =========================================================================
    
    def get_send_statistics(self) -> Dict[str, Any]:
        """
        Get SES sending statistics.
        
        Returns:
            Send statistics data points
        """
        try:
            response = self.ses_client.get_send_statistics()
            return response.get("SendDataPoints", [])
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get send statistics: {e}")
            return {}
    
    def get_send_quota(self) -> Dict[str, Any]:
        """
        Get SES sending quota.
        
        Returns:
            Sending quota information
        """
        try:
            response = self.ses_client.get_send_quota()
            return {
                "max_24_hour_send": response.get("Max24HourSend"),
                "max_send_rate": response.get("MaxSendRate"),
                "sent_last_24_hours": response.get("SentLast24Hours")
            }
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get send quota: {e}")
            return {}
    
    def get_account_sending_enabled(self) -> bool:
        """
        Check if account sending is enabled.
        
        Returns:
            True if sending is enabled
        """
        try:
            response = self.ses_client.get_account_sending_enabled()
            return response.get("Enabled", False)
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get account sending enabled status: {e}")
            return False
    
    # =========================================================================
    # Custom Verified Senders
    # =========================================================================
    
    def verify_custom_verification_email(
        self,
        email_address: str,
        template_name: str,
        from_email_address: str
    ) -> bool:
        """
        Send a custom verification email with a custom template.
        
        Args:
            email_address: Email address to verify
            template_name: Name of the custom verification template
            from_email_address: From address for the verification email
            
        Returns:
            True if verification email sent successfully
        """
        try:
            self.ses_client.send_custom_verification_email(
                EmailAddress=email_address,
                TemplateName=template_name,
                ConfigurationSetName=None
            )
            logger.info(f"Sent custom verification email to {email_address}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to send custom verification email: {e}")
            return False
    
    def create_custom_verification_email_template(
        self,
        template_name: str,
        from_email_address: str,
        template_subject: str,
        template_content: str,
        success_redirect_url: str
    ) -> bool:
        """
        Create a custom verification email template.
        
        Args:
            template_name: Name of the template
            from_email_address: From email address
            template_subject: Email subject
            template_content: Email content (HTML)
            success_redirect_url: URL to redirect after successful verification
            
        Returns:
            True if created successfully
        """
        try:
            self.ses_client.create_custom_verification_email_template(
                TemplateName=template_name,
                FromEmailAddress=from_email_address,
                TemplateSubject=template_subject,
                TemplateContent=template_content,
                SuccessRedirectionURL=success_redirect_url
            )
            logger.info(f"Created custom verification template: {template_name}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to create custom verification template: {e}")
            return False
    
    def delete_custom_verification_email_template(self, template_name: str) -> bool:
        """
        Delete a custom verification email template.
        
        Args:
            template_name: Name of the template
            
        Returns:
            True if deleted successfully
        """
        try:
            self.ses_client.delete_custom_verification_email_template(
                TemplateName=template_name
            )
            logger.info(f"Deleted custom verification template: {template_name}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to delete custom verification template: {e}")
            return False
    
    def list_custom_verification_email_templates(self) -> List[Dict[str, Any]]:
        """
        List all custom verification email templates.
        
        Returns:
            List of template metadata
        """
        try:
            response = self.ses_client.list_custom_verification_email_templates()
            return response.get("CustomVerificationEmailTemplates", [])
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to list custom verification templates: {e}")
            return []
    
    # =========================================================================
    # CloudWatch Integration
    # =========================================================================
    
    def enable_cloudwatch_metrics(
        self,
        config_set_name: str,
        metrics: List[str] = None
    ) -> bool:
        """
        Enable CloudWatch metrics for a configuration set.
        
        Args:
            config_set_name: Configuration set name
            metrics: List of metrics to enable (default all)
            
        Returns:
            True if successful
        """
        if metrics is None:
            metrics = ["send", "renderFailure", "bounce", "complaint", "delivery", "open", "click"]
        
        try:
            cloudwatch_destination = {
                "DimensionConfigurations": [
                    {
                        "DimensionName": "ConfigurationSet",
                        "DimensionValueSource": "CONFIGURATION_SET",
                        "DefaultDimensionValue": config_set_name
                    }
                ]
            }
            
            self.add_event_destination(
                config_set_name=config_set_name,
                event_destination_name=f"{config_set_name}_cloudwatch",
                enabled=True,
                event_types=metrics,
                cloudwatch_destination=cloudwatch_destination
            )
            
            logger.info(f"Enabled CloudWatch metrics for configuration set: {config_set_name}")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to enable CloudWatch metrics: {e}")
            return False
    
    def get_cloudwatch_metrics(
        self,
        metric_name: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 300,
        dimensions: List[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Get CloudWatch metrics for SES.
        
        Args:
            metric_name: Name of the metric
            start_time: Start time for the query
            end_time: End time for the query
            period: Period in seconds
            dimensions: Metric dimensions
            
        Returns:
            Metric data points
        """
        try:
            kwargs = {
                "Namespace": self._cloudwatch_namespace,
                "MetricName": metric_name,
                "StartTime": start_time,
                "EndTime": end_time,
                "Period": period,
                "Statistics": ["Sum", "Average"]
            }
            
            if dimensions:
                kwargs["Dimensions"] = dimensions
            
            response = self.cloudwatch_client.get_metric_statistics(**kwargs)
            return response.get("Datapoints", [])
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get CloudWatch metrics: {e}")
            return {}
    
    def put_email_metric(
        self,
        metric_name: str,
        value: float,
        unit: str = "Count",
        configuration_set: str = None
    ) -> bool:
        """
        Put a custom email metric to CloudWatch.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: Unit type
            configuration_set: Configuration set name for dimension
            
        Returns:
            True if successful
        """
        try:
            dimensions = []
            if configuration_set:
                dimensions.append({"Name": "ConfigurationSet", "Value": configuration_set})
            
            self.cloudwatch_client.put_metric_data(
                Namespace=self._cloudwatch_namespace,
                MetricData=[
                    {
                        "MetricName": metric_name,
                        "Value": value,
                        "Unit": unit,
                        "Timestamp": datetime.utcnow().isoformat(),
                        "Dimensions": dimensions
                    }
                ]
            )
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to put email metric: {e}")
            return False
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def test_identity_verification(self, identity: str) -> bool:
        """
        Test if an identity is verified.
        
        Args:
            identity: Email address or domain
            
        Returns:
            True if verified
        """
        attrs = self.get_identity_verification_attributes([identity])
        if identity in attrs:
            status = attrs[identity].get("VerificationStatus")
            return status == "Success"
        return False
    
    def wait_for_identity_verification(
        self,
        identity: str,
        timeout: int = 300,
        poll_interval: int = 5
    ) -> bool:
        """
        Wait for an identity to be verified.
        
        Args:
            identity: Email address or domain
            timeout: Maximum wait time in seconds
            poll_interval: Poll interval in seconds
            
        Returns:
            True if verified within timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            attrs = self.get_identity_verification_attributes([identity])
            if identity in attrs:
                status = attrs[identity].get("VerificationStatus")
                if status == "Success":
                    logger.info(f"Identity {identity} verified successfully")
                    return True
                elif status == "Failed":
                    logger.error(f"Identity {identity} verification failed")
                    return False
            
            time.sleep(poll_interval)
        
        logger.warning(f"Timeout waiting for identity {identity} verification")
        return False
    
    def get_dns_records_for_domain(
        self,
        domain: str,
        include_dkim: bool = True,
        include_spf: bool = True,
        include_dmarc: bool = True,
        dmarc_policy: str = "none"
    ) -> List[Dict[str, str]]:
        """
        Get all DNS records needed to configure a domain for SES.
        
        Args:
            domain: Domain name
            include_dkim: Include DKIM records
            include_spf: Include SPF record
            include_dmarc: Include DMARC record
            dmarc_policy: DMARC policy (none, quarantine, reject)
            
        Returns:
            List of DNS records (name, value, type)
        """
        records = []
        
        domain_attrs = self.verify_domain_identity(domain)
        verification_token = domain_attrs.get("VerificationToken")
        
        if include_dkim:
            dkim_tokens = self.enable_dkim(domain)
            for i, token in enumerate(dkim_tokens):
                records.append({
                    "name": f"{token}._domainkey.{domain}",
                    "value": f"{token}.dkim.amazonses.com",
                    "type": "CNAME"
                })
        
        if include_spf:
            records.append({
                "name": domain,
                "value": "v=spf1 include:amazonses.com ~all",
                "type": "TXT"
            })
        
        if include_dmarc:
            dmarc_record = self.generate_dmarc_record(domain, policy=dmarc_policy)
            records.append(dmarc_record)
        
        records.append({
            "name": f"_amazonses.{domain}",
            "value": verification_token,
            "type": "TXT"
        })
        
        return records
    
    def close(self):
        """Flush any pending metrics and clean up resources."""
        self.flush_metrics()
        logger.info("SES integration closed")

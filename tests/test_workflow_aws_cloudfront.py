"""
Tests for workflow_aws_cloudfront module
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

# Create mock boto3 module before importing workflow_aws_cloudfront
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
from src.workflow_aws_cloudfront import (
    CloudFrontIntegration,
    PriceClass,
    ViewerProtocolPolicy,
    CachePolicy,
    GeoRestrictionType,
    HttpVersion,
    DefaultCacheBehaviorTarget,
    OriginConfig,
    CacheBehaviorConfig,
    DistributionConfig,
    InvalidationResult,
    SignedUrlResult,
)


class TestPriceClass(unittest.TestCase):
    """Test PriceClass enum"""

    def test_price_class_values(self):
        self.assertEqual(PriceClass.PRICE_CLASS_ALL.value, "PriceClass_All")
        self.assertEqual(PriceClass.PRICE_CLASS_100.value, "PriceClass_100")
        self.assertEqual(PriceClass.PRICE_CLASS_200.value, "PriceClass_200")


class TestViewerProtocolPolicy(unittest.TestCase):
    """Test ViewerProtocolPolicy enum"""

    def test_viewer_protocol_policy_values(self):
        self.assertEqual(ViewerProtocolPolicy.HTTP_ONLY.value, "http-only")
        self.assertEqual(ViewerProtocolPolicy.HTTPS_ONLY.value, "https-only")
        self.assertEqual(ViewerProtocolPolicy.REDIRECT_TO_HTTPS.value, "redirect-to-https")


class TestCachePolicy(unittest.TestCase):
    """Test CachePolicy enum"""

    def test_cache_policy_values(self):
        self.assertEqual(CachePolicy.ELIMINATE_PARAMETERS.value, "c基本化")
        self.assertEqual(CachePolicy.IMAGE_OPTIMIZATION.value, "cz7ur4j2")
        self.assertEqual(CachePolicy.MANAGEABLE_CACHE.value, "c83n9j0v")


class TestGeoRestrictionType(unittest.TestCase):
    """Test GeoRestrictionType enum"""

    def test_geo_restriction_type_values(self):
        self.assertEqual(GeoRestrictionType.NONE.value, "none")
        self.assertEqual(GeoRestrictionType.WHITELIST.value, "whitelist")
        self.assertEqual(GeoRestrictionType.BLACKLIST.value, "blacklist")


class TestHttpVersion(unittest.TestCase):
    """Test HttpVersion enum"""

    def test_http_version_values(self):
        self.assertEqual(HttpVersion.HTTP1_1.value, "http1.1")
        self.assertEqual(HttpVersion.HTTP2.value, "http2")
        self.assertEqual(HttpVersion.HTTP2_AND_3.value, "http2and3")


class TestDefaultCacheBehaviorTarget(unittest.TestCase):
    """Test DefaultCacheBehaviorTarget enum"""

    def test_default_cache_behavior_target_values(self):
        self.assertEqual(DefaultCacheBehaviorTarget.ORIGIN.value, "origin")


class TestOriginConfig(unittest.TestCase):
    """Test OriginConfig dataclass"""

    def test_origin_config_defaults(self):
        config = OriginConfig(origin_id="my-origin", domain_name="example.com")
        self.assertEqual(config.origin_id, "my-origin")
        self.assertEqual(config.domain_name, "example.com")
        self.assertEqual(config.origin_path, "")
        self.assertEqual(config.custom_headers, {})
        self.assertEqual(config.connection_attempts, 3)
        self.assertEqual(config.connection_timeout, 10)
        self.assertEqual(config.read_timeout, 30)
        self.assertEqual(config.keepalive_timeout, 5)
        self.assertIsNone(config.origin_shield)
        self.assertEqual(config.origin_ssl_protocols, ["TLSv1.2"])
        self.assertEqual(config.origin_protocol_policy, "match-viewer")
        self.assertEqual(config.http_port, 80)
        self.assertEqual(config.https_port, 443)

    def test_origin_config_custom(self):
        config = OriginConfig(
            origin_id="custom-origin",
            domain_name="custom.example.com",
            origin_path="/static",
            custom_headers={"X-Custom-Header": "value"},
            connection_attempts=5,
            origin_shield="shield.example.com"
        )
        self.assertEqual(config.origin_path, "/static")
        self.assertEqual(config.custom_headers, {"X-Custom-Header": "value"})
        self.assertEqual(config.connection_attempts, 5)


class TestCacheBehaviorConfig(unittest.TestCase):
    """Test CacheBehaviorConfig dataclass"""

    def test_cache_behavior_config_defaults(self):
        config = CacheBehaviorConfig(target_origin_id="my-origin")
        self.assertEqual(config.path_pattern, "/*")
        self.assertEqual(config.target_origin_id, "my-origin")
        self.assertEqual(config.viewer_protocol_policy, "https-only")
        self.assertEqual(config.allowed_methods, ["GET", "HEAD"])
        self.assertEqual(config.cached_methods, ["GET", "HEAD"])
        self.assertIsNone(config.cache_policy_id)
        self.assertEqual(config.min_ttl, 0)
        self.assertEqual(config.default_ttl, 86400)
        self.assertEqual(config.max_ttl, 31536000)
        self.assertTrue(config.compress)

    def test_cache_behavior_config_custom(self):
        config = CacheBehaviorConfig(
            path_pattern="/api/*",
            target_origin_id="api-origin",
            viewer_protocol_policy="redirect-to-https",
            allowed_methods=["GET", "POST", "PUT", "DELETE"],
            cached_methods=["GET", "HEAD"],
            min_ttl=60,
            default_ttl=3600,
            max_ttl=86400
        )
        self.assertEqual(config.path_pattern, "/api/*")
        self.assertEqual(config.viewer_protocol_policy, "redirect-to-https")
        self.assertEqual(config.allowed_methods, ["GET", "POST", "PUT", "DELETE"])


class TestDistributionConfig(unittest.TestCase):
    """Test DistributionConfig dataclass"""

    def test_distribution_config_defaults(self):
        config = DistributionConfig(origin_id="my-origin", domain_name="example.com")
        self.assertEqual(config.origin_id, "my-origin")
        self.assertEqual(config.domain_name, "example.com")
        self.assertTrue(config.caller_reference)
        self.assertEqual(config.comment, "")
        self.assertTrue(config.enabled)
        self.assertEqual(config.price_class, "PriceClass_All")
        self.assertEqual(config.aliases, [])
        self.assertEqual(config.ssl_certificate, "cloudfront.default")
        self.assertEqual(config.min_protocol_version, "TLSv1.2_2021")
        self.assertEqual(config.default_root_object, "index.html")
        self.assertIsNone(config.logs_bucket)
        self.assertEqual(config.logs_prefix, "cloudfront")

    def test_distribution_config_custom(self):
        config = DistributionConfig(
            origin_id="custom-origin",
            domain_name="custom.example.com",
            comment="Custom distribution",
            enabled=True,
            price_class="PriceClass_100",
            aliases=["cdn.example.com"],
            ssl_certificate="arn:aws:acm:us-east-1:123456789012:certificate/12345678-1234-1234-1234-123456789012",
            logs_bucket="logs.example.com",
            geo_restriction_type="whitelist",
            geo_restriction_locations=["US", "CA"]
        )
        self.assertEqual(config.comment, "Custom distribution")
        self.assertEqual(config.price_class, "PriceClass_100")
        self.assertEqual(config.aliases, ["cdn.example.com"])
        self.assertEqual(config.geo_restriction_type, "whitelist")
        self.assertEqual(config.geo_restriction_locations, ["US", "CA"])


class TestInvalidationResult(unittest.TestCase):
    """Test InvalidationResult dataclass"""

    def test_invalidation_result(self):
        now = datetime.utcnow()
        result = InvalidationResult(
            invalidation_id="KJFHHHWHW",
            status="Completed",
            create_time=now,
            caller_reference="test-ref",
            paths=["/images/*", "/css/*"]
        )
        self.assertEqual(result.invalidation_id, "KJFHHHWHW")
        self.assertEqual(result.status, "Completed")
        self.assertEqual(result.create_time, now)
        self.assertEqual(result.caller_reference, "test-ref")
        self.assertEqual(result.paths, ["/images/*", "/css/*"])


class TestSignedUrlResult(unittest.TestCase):
    """Test SignedUrlResult dataclass"""

    def test_signed_url_result(self):
        expires = datetime.utcnow() + timedelta(hours=1)
        result = SignedUrlResult(
            url="https://d123.cloudfront.net/files/doc.pdf?signature=...",
            expires=expires,
            policy="pol"
        )
        self.assertIn("d123.cloudfront.net", result.url)
        self.assertEqual(result.expires, expires)


class TestCloudFrontIntegration(unittest.TestCase):
    """Test CloudFrontIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.return_value = self.mock_client

    def test_init_without_boto3(self):
        """Test initialization when boto3 is not available"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration(region="us-east-1")
            self.assertEqual(cf.region, "us-east-1")
            self.assertEqual(cf._distribution_cache, {})
            self.assertEqual(cf._origin_cache, {})
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available

    def test_init_with_boto3(self):
        """Test initialization with boto3 available"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = True

        try:
            cf = CloudFrontIntegration(
                region="us-east-1",
                aws_access_key_id="test-key",
                aws_secret_access_key="test-secret"
            )
            self.assertEqual(cf.region, "us-east-1")
            mock_boto3.Session.assert_called()
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available

    def test_client_property(self):
        """Test client property"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = True

        try:
            cf = CloudFrontIntegration()
            # Accessing client property should work
            client = cf.client
            self.assertIsNotNone(client)
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available

    def test_mock_distribution_response(self):
        """Test _mock_distribution_response method"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration()
            result = cf._mock_distribution_response("origin-id", "example.com", "Test comment", True)

            self.assertIn("id", result)
            self.assertEqual(result["domain_name"], "d1234567890abc.cloudfront.net")
            self.assertTrue(result["enabled"])
            self.assertTrue(result["created"])
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available

    def test_create_distribution_without_boto3(self):
        """Test create_distribution when boto3 is not available"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration()
            result = cf.create_distribution(
                origin_id="my-origin",
                domain_name="source.example.com",
                comment="Test distribution"
            )

            self.assertIn("id", result)
            self.assertEqual(result["domain_name"], "d1234567890abc.cloudfront.net")
            self.assertTrue(result["created"])
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available

    def test_create_distribution_with_boto3(self):
        """Test create_distribution with boto3"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = True

        self.mock_client.create_distribution.return_value = {
            "Distribution": {
                "Id": "E1234567890ABC",
                "ARN": "arn:aws:cloudfront::123456789012:distribution/E1234567890ABC",
                "Status": "InProgress",
                "DomainName": "d1234567890abc.cloudfront.net",
                "DistributionConfig": {}
            }
        }

        try:
            cf = CloudFrontIntegration()
            result = cf.create_distribution(
                origin_id="my-origin",
                domain_name="source.example.com",
                comment="Test"
            )

            self.assertEqual(result["id"], "E1234567890ABC")
            self.assertTrue(result["created"])
            self.mock_client.create_distribution.assert_called_once()
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_distribution_from_cache(self):
        """Test get_distribution returns cached distribution"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration()
            cf._distribution_cache["E123456"] = {"Id": "E123456", "Status": "Deployed"}

            result = cf.get_distribution("E123456")
            self.assertEqual(result["Status"], "Deployed")
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_distribution_mock(self):
        """Test _get_mock_distribution method"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration()
            result = cf._get_mock_distribution("E123456")
            self.assertIsNone(result)  # Not in cache
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available

    def test_list_distributions_without_boto3(self):
        """Test list_distributions when boto3 is not available"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration()
            result = cf.list_distributions()
            self.assertEqual(result, [])
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available


class TestCloudFrontIntegrationOrigins(unittest.TestCase):
    """Test CloudFrontIntegration origin methods"""

    def test_add_origin_without_boto3(self):
        """Test add_origin when boto3 is not available"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration()
            config = OriginConfig(
                origin_id="new-origin",
                domain_name="new-source.example.com",
                origin_path="/static"
            )

            result = cf.add_origin("E123456", config)
            self.assertIn("id", result)
            self.assertEqual(result["origin_id"], "new-origin")
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available


class TestCloudFrontIntegrationCacheBehaviors(unittest.TestCase):
    """Test CloudFrontIntegration cache behavior methods"""

    def test_add_cache_behavior_without_boto3(self):
        """Test add_cache_behavior when boto3 is not available"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration()
            config = CacheBehaviorConfig(
                path_pattern="/api/*",
                target_origin_id="api-origin"
            )

            result = cf.add_cache_behavior("E123456", config)
            self.assertIn("path_pattern", result)
            self.assertEqual(result["path_pattern"], "/api/*")
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available


class TestCloudFrontIntegrationInvalidations(unittest.TestCase):
    """Test CloudFrontIntegration invalidation methods"""

    def test_create_invalidation_without_boto3(self):
        """Test create_invalidation when boto3 is not available"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration()
            result = cf.create_invalidation(
                distribution_id="E1234567890ABC",
                paths=["/images/*", "/css/*"]
            )

            self.assertIn("invalidation_id", result)
            self.assertEqual(result["status"], "Completed")
            self.assertEqual(result["paths"], ["/images/*", "/css/*"])
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_invalidation_without_boto3(self):
        """Test get_invalidation when boto3 is not available"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration()
            # First create an invalidation
            cf.create_invalidation("E123456", paths=["/*"])

            result = cf.get_invalidation("E123456", "KJFHHHWHW")
            self.assertIsNotNone(result)
            self.assertEqual(result.invalidation_id, "KJFHHHWHW")
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available


class TestCloudFrontIntegrationSignedUrls(unittest.TestCase):
    """Test CloudFrontIntegration signed URL methods"""

    def test_generate_signed_url_without_boto3(self):
        """Test generate_signed_url when boto3 is not available"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration()
            result = cf.generate_signed_url(
                distribution_url="https://d123.cloudfront.net/files/doc.pdf",
                key_pair_id="K123456789",
                private_key="mock-private-key",
                expires=3600
            )

            self.assertIn("url", result)
            self.assertIn("expires", result)
            self.assertIn("policy", result)
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available


class TestCloudFrontIntegrationGeoRestriction(unittest.TestCase):
    """Test CloudFrontIntegration geo restriction methods"""

    def test_configure_geo_restriction_without_boto3(self):
        """Test configure_geo_restriction when boto3 is not available"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration()
            result = cf.configure_geo_restriction(
                distribution_id="E123456",
                restriction_type="whitelist",
                locations=["US", "CA", "MX"]
            )

            self.assertIn("restriction_type", result)
            self.assertEqual(result["restriction_type"], "whitelist")
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available


class TestCloudFrontIntegrationAccessLogs(unittest.TestCase):
    """Test CloudFrontIntegration access log methods"""

    def test_enable_access_logs_without_boto3(self):
        """Test enable_access_logs when boto3 is not available"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration()
            result = cf.enable_access_logs(
                distribution_id="E123456",
                bucket="logs.example.com",
                prefix="cloudfront"
            )

            self.assertIn("logging", result)
            self.assertTrue(result["logging"]["enabled"])
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available


class TestCloudFrontIntegrationLambdaEdge(unittest.TestCase):
    """Test CloudFrontIntegration Lambda@Edge methods"""

    def test_add_lambda_edge_without_boto3(self):
        """Test add_lambda_edge when boto3 is not available"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration()
            result = cf.add_lambda_edge(
                distribution_id="E123456",
                function_arn="arn:aws:lambda:us-east-1:123456789012:function:my-function",
                event_type="viewer-request"
            )

            self.assertIn("function_arn", result)
            self.assertEqual(result["event_type"], "viewer-request")
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available


class TestCloudFrontIntegrationSSL(unittest.TestCase):
    """Test CloudFrontIntegration SSL/TLS methods"""

    def test_configure_ssl_without_boto3(self):
        """Test configure_ssl when boto3 is not available"""
        import src.workflow_aws_cloudfront as cloudfront_module
        original_boto3_available = cloudfront_module.BOTO3_AVAILABLE
        cloudfront_module.BOTO3_AVAILABLE = False

        try:
            cf = CloudFrontIntegration()
            result = cf.configure_ssl(
                distribution_id="E123456",
                certificate_arn="arn:aws:acm:us-east-1:123456789012:certificate/12345678-1234-1234-1234-123456789012",
                minimum_protocol="TLSv1.2_2021"
            )

            self.assertIn("viewer_certificate", result)
        finally:
            cloudfront_module.BOTO3_AVAILABLE = original_boto3_available


if __name__ == "__main__":
    unittest.main()

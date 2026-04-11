"""
Tests for workflow_aws_api_gateway module

Tests the actual implementation in src/workflow_aws_api_gateway.py
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

# Create mock boto3 module before importing workflow_aws_api_gateway
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
from src.workflow_aws_api_gateway import (
    APIGatewayIntegration,
    APIType,
    MethodType,
    IntegrationType,
    AuthorizationType,
    RESTAPIConfig,
    HTTPAPIConfig,
    WebSocketAPIConfig,
    ResourceConfig,
    MethodConfig,
    IntegrationConfig,
    StageConfig,
    CustomDomainConfig,
    APIKeyConfig,
    UsagePlanConfig,
    CloudWatchConfig,
)


class TestAPIType(unittest.TestCase):
    """Test APIType enum"""

    def test_api_type_values(self):
        self.assertEqual(APIType.REST.value, "REST")
        self.assertEqual(APIType.HTTP.value, "HTTP")
        self.assertEqual(APIType.WEBSOCKET.value, "WEBSOCKET")


class TestMethodType(unittest.TestCase):
    """Test MethodType enum"""

    def test_method_type_values(self):
        self.assertEqual(MethodType.GET.value, "GET")
        self.assertEqual(MethodType.POST.value, "POST")
        self.assertEqual(MethodType.PUT.value, "PUT")
        self.assertEqual(MethodType.DELETE.value, "DELETE")
        self.assertEqual(MethodType.PATCH.value, "PATCH")
        self.assertEqual(MethodType.OPTIONS.value, "OPTIONS")
        self.assertEqual(MethodType.HEAD.value, "HEAD")
        self.assertEqual(MethodType.ANY.value, "ANY")


class TestIntegrationType(unittest.TestCase):
    """Test IntegrationType enum"""

    def test_integration_type_values(self):
        self.assertEqual(IntegrationType.LAMBDA.value, "AWS")
        self.assertEqual(IntegrationType.HTTP.value, "HTTP")
        self.assertEqual(IntegrationType.HTTP_PROXY.value, "HTTP_PROXY")
        self.assertEqual(IntegrationType.AWS_PROXY.value, "AWS_PROXY")
        self.assertEqual(IntegrationType.MOCK.value, "MOCK")
        self.assertEqual(IntegrationType.HTTP_API.value, "HTTP_API")
        self.assertEqual(IntegrationType.AWS_PROXY_V2.value, "AWS_PROXY_V2")


class TestAuthorizationType(unittest.TestCase):
    """Test AuthorizationType enum"""

    def test_authorization_type_values(self):
        self.assertEqual(AuthorizationType.NONE.value, "NONE")
        self.assertEqual(AuthorizationType.IAM.value, "AWS_IAM")
        self.assertEqual(AuthorizationType.RESOURCE_POLICY.value, "RESOURCE_POLICY")
        self.assertEqual(AuthorizationType.CUSTOM.value, "CUSTOM")
        self.assertEqual(AuthorizationType.COGNITO_USER_POOLS.value, "COGNITO_USER_POOLS")


class TestRESTAPIConfig(unittest.TestCase):
    """Test RESTAPIConfig dataclass"""

    def test_rest_api_config_defaults(self):
        config = RESTAPIConfig(name="test-api")
        self.assertEqual(config.name, "test-api")
        self.assertIsNone(config.description)
        self.assertIsNone(config.version)
        self.assertEqual(config.binary_media_types, [])
        self.assertIsNone(config.minimum_compression_size)
        self.assertEqual(config.api_key_source, "HEADER")
        self.assertEqual(config.endpoint_configuration, {"types": ["REGIONAL"]})
        self.assertEqual(config.tags, {})

    def test_rest_api_config_custom(self):
        config = RESTAPIConfig(
            name="custom-api",
            description="Custom API",
            version="v1",
            binary_media_types=["application/pdf"],
            minimum_compression_size=1024,
            api_key_source="AUTHORIZER",
            endpoint_configuration={"types": ["EDGE"]},
            tags={"env": "prod"}
        )
        self.assertEqual(config.name, "custom-api")
        self.assertEqual(config.description, "Custom API")
        self.assertEqual(config.version, "v1")
        self.assertEqual(config.binary_media_types, ["application/pdf"])
        self.assertEqual(config.minimum_compression_size, 1024)
        self.assertEqual(config.api_key_source, "AUTHORIZER")
        self.assertEqual(config.endpoint_configuration, {"types": ["EDGE"]})
        self.assertEqual(config.tags, {"env": "prod"})


class TestHTTPAPIConfig(unittest.TestCase):
    """Test HTTPAPIConfig dataclass"""

    def test_http_api_config_defaults(self):
        config = HTTPAPIConfig(name="http-api")
        self.assertEqual(config.name, "http-api")
        self.assertIsNone(config.description)
        self.assertEqual(config.protocols, ["HTTP", "HTTPS"])
        self.assertIsNone(config.route_key)
        self.assertEqual(config.tags, {})

    def test_http_api_config_custom(self):
        config = HTTPAPIConfig(
            name="custom-http-api",
            description="Custom HTTP API",
            protocols=["HTTPS"],
            route_key="$default",
            tags={"env": "dev"}
        )
        self.assertEqual(config.name, "custom-http-api")
        self.assertEqual(config.protocols, ["HTTPS"])
        self.assertEqual(config.route_key, "$default")


class TestWebSocketAPIConfig(unittest.TestCase):
    """Test WebSocketAPIConfig dataclass"""

    def test_websocket_api_config_defaults(self):
        config = WebSocketAPIConfig(name="ws-api")
        self.assertEqual(config.name, "ws-api")
        self.assertEqual(config.route_selection_expression, "$request.body.action")
        self.assertEqual(config.tags, {})


class TestResourceConfig(unittest.TestCase):
    """Test ResourceConfig dataclass"""

    def test_resource_config_defaults(self):
        config = ResourceConfig(path="/users")
        self.assertEqual(config.path, "/users")
        self.assertIsNone(config.parent_id)
        self.assertEqual(config.methods, {})


class TestMethodConfig(unittest.TestCase):
    """Test MethodConfig dataclass"""

    def test_method_config_defaults(self):
        config = MethodConfig(http_method=MethodType.GET)
        self.assertEqual(config.http_method, MethodType.GET)
        self.assertEqual(config.authorization_type, AuthorizationType.NONE)
        self.assertFalse(config.api_key_required)
        self.assertEqual(config.request_parameters, {})
        self.assertEqual(config.request_models, {})


class TestIntegrationConfig(unittest.TestCase):
    """Test IntegrationConfig dataclass"""

    def test_integration_config_defaults(self):
        config = IntegrationConfig(integration_type=IntegrationType.LAMBDA)
        self.assertEqual(config.integration_type, IntegrationType.LAMBDA)
        self.assertIsNone(config.uri)
        self.assertEqual(config.passthrough_behavior, "WHEN_NO_MATCH")
        self.assertEqual(config.timeout_milliseconds, 29000)


class TestStageConfig(unittest.TestCase):
    """Test StageConfig dataclass"""

    def test_stage_config_defaults(self):
        config = StageConfig(stage_name="prod")
        self.assertEqual(config.stage_name, "prod")
        self.assertIsNone(config.description)
        self.assertEqual(config.variables, {})
        self.assertFalse(config.tracing_enabled)
        self.assertTrue(config.metrics_enabled)


class TestCustomDomainConfig(unittest.TestCase):
    """Test CustomDomainConfig dataclass"""

    def test_custom_domain_config_defaults(self):
        config = CustomDomainConfig(
            domain_name="api.example.com",
            certificate_arn="arn:aws:acm:us-east-1:123456789012:certificate/12345678-1234-1234-1234-123456789012"
        )
        self.assertEqual(config.domain_name, "api.example.com")
        self.assertEqual(config.security_policy, "TLS_1_2")
        self.assertEqual(config.endpoint_type, "REGIONAL")
        self.assertEqual(config.base_path_mapping, [])


class TestAPIKeyConfig(unittest.TestCase):
    """Test APIKeyConfig dataclass"""

    def test_api_key_config_defaults(self):
        config = APIKeyConfig(name="test-key")
        self.assertEqual(config.name, "test-key")
        self.assertTrue(config.enabled)
        self.assertTrue(config.generate_distinct_id)
        self.assertIsNone(config.value)


class TestUsagePlanConfig(unittest.TestCase):
    """Test UsagePlanConfig dataclass"""

    def test_usage_plan_config_defaults(self):
        config = UsagePlanConfig(name="basic-plan")
        self.assertEqual(config.name, "basic-plan")
        self.assertEqual(config.quota, {})
        self.assertEqual(config.throttle, {})


class TestCloudWatchConfig(unittest.TestCase):
    """Test CloudWatchConfig dataclass"""

    def test_cloudwatch_config_defaults(self):
        config = CloudWatchConfig()
        self.assertEqual(config.log_level, "INFO")
        self.assertEqual(config.log_format, "JSON")
        self.assertTrue(config.detailed_metrics)
        self.assertFalse(config.data_trace)
        self.assertEqual(config.logging_level, "OFF")
        self.assertTrue(config.metrics_enabled)


class TestAPIGatewayIntegration(unittest.TestCase):
    """Test APIGatewayIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.return_value = self.mock_client

    def test_init_without_boto3(self):
        """Test initialization when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration(region="us-west-2")
            self.assertEqual(integration.region, "us-west-2")
            self.assertEqual(integration._apis, {})
            self.assertEqual(integration._resources, {})
            self.assertEqual(integration._stages, {})
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_init_with_boto3(self):
        """Test initialization with boto3 available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = True

        try:
            integration = APIGatewayIntegration(
                region="us-east-1",
                aws_access_key_id="test-key",
                aws_secret_access_key="test-secret"
            )
            self.assertEqual(integration.region, "us-east-1")
            mock_boto3.Session.assert_called()
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_generate_id(self):
        """Test _generate_id method"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            generated_id = integration._generate_id("test-")
            self.assertTrue(generated_id.startswith("test-"))
            self.assertEqual(len(generated_id), len("test-") + 8)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_create_rest_api_without_boto3(self):
        """Test create_rest_api when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = RESTAPIConfig(name="test-rest-api", description="Test API")
            result = integration.create_rest_api(config)

            self.assertIn("id", result)
            self.assertEqual(result["name"], "test-rest-api")
            self.assertEqual(result["description"], "Test API")
            self.assertIn(result["id"], integration._apis)
            # Verify the stored API has correct structure
            stored_api = integration._apis[result["id"]]
            self.assertEqual(stored_api["name"], "test-rest-api")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_create_rest_api_with_boto3(self):
        """Test create_rest_api with boto3"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = True

        self.mock_client.create_rest_api.return_value = {
            "id": "abc123",
            "name": "test-api",
            "description": "Test",
            "version": "v1",
            "binaryMediaTypes": [],
            "minimumCompressionSize": None,
            "apiKeySource": "HEADER",
            "endpointConfiguration": {"types": ["REGIONAL"]},
            "createdTimestamp": datetime.utcnow()
        }

        try:
            integration = APIGatewayIntegration()
            config = RESTAPIConfig(name="test-api", description="Test")
            result = integration.create_rest_api(config)

            self.assertEqual(result["id"], "abc123")
            self.assertEqual(result["name"], "test-api")
            self.mock_client.create_rest_api.assert_called_once()
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_rest_api_from_cache(self):
        """Test get_rest_api returns cached API"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            integration._apis["test-id"] = {"id": "test-id", "name": "cached-api"}

            result = integration.get_rest_api("test-id")
            self.assertEqual(result["name"], "cached-api")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_rest_api_not_found(self):
        """Test get_rest_api raises error when not found"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            with self.assertRaises(ValueError):
                integration.get_rest_api("nonexistent-id")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_list_rest_apis_without_boto3(self):
        """Test list_rest_apis when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            integration._apis["api1"] = {"id": "api1", "name": "API 1"}
            integration._apis["api2"] = {"id": "api2", "name": "API 2"}

            result = integration.list_rest_apis()
            self.assertEqual(len(result["items"]), 2)
            self.assertIsNone(result["position"])
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_update_rest_api_without_boto3(self):
        """Test update_rest_api when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            integration._apis["test-id"] = {"id": "test-id", "name": "original"}

            result = integration.update_rest_api("test-id", [{"op": "replace", "path": "/name", "value": "updated"}])
            self.assertTrue(result.get("patched"))
            self.assertEqual(integration._apis["test-id"]["name"], "original")  # Original not modified
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_delete_rest_api_without_boto3(self):
        """Test delete_rest_api when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            integration._apis["test-id"] = {"id": "test-id", "name": "to-delete"}

            result = integration.delete_rest_api("test-id")
            self.assertTrue(result)
            self.assertNotIn("test-id", integration._apis)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_delete_rest_api_not_found(self):
        """Test delete_rest_api returns False when not found"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            result = integration.delete_rest_api("nonexistent")
            self.assertFalse(result)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available


class TestAPIGatewayIntegrationHTTPAPI(unittest.TestCase):
    """Test APIGatewayIntegration HTTP API methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.return_value = self.mock_client

    def test_create_http_api_without_boto3(self):
        """Test create_http_api when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = HTTPAPIConfig(name="test-http-api")
            result = integration.create_http_api(config)

            self.assertIn("apiId", result)
            self.assertEqual(result["name"], "test-http-api")
            self.assertEqual(result["protocols"], ["HTTP", "HTTPS"])
            # Verify stored with http- prefix
            stored_key = f"http-{result['apiId']}"
            self.assertIn(stored_key, integration._apis)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_http_api_without_boto3(self):
        """Test get_http_api when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = HTTPAPIConfig(name="test-http-api")
            created = integration.create_http_api(config)

            result = integration.get_http_api(created["apiId"])
            self.assertEqual(result["name"], "test-http-api")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_http_api_not_found(self):
        """Test get_http_api raises error when not found"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            with self.assertRaises(ValueError):
                integration.get_http_api("nonexistent-id")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_delete_http_api_without_boto3(self):
        """Test delete_http_api when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = HTTPAPIConfig(name="test-http-api")
            created = integration.create_http_api(config)
            api_id = created["apiId"]

            result = integration.delete_http_api(api_id)
            self.assertTrue(result)
            with self.assertRaises(ValueError):
                integration.get_http_api(api_id)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available


class TestAPIGatewayIntegrationWebSocket(unittest.TestCase):
    """Test APIGatewayIntegration WebSocket API methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.return_value = self.mock_client

    def test_create_websocket_api_without_boto3(self):
        """Test create_websocket_api when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = WebSocketAPIConfig(name="test-ws-api")
            result = integration.create_websocket_api(config)

            self.assertIn("apiId", result)
            self.assertEqual(result["name"], "test-ws-api")
            self.assertEqual(result["routeSelectionExpression"], "$request.body.action")
            # Verify stored with ws- prefix
            stored_key = f"ws-{result['apiId']}"
            self.assertIn(stored_key, integration._apis)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_add_websocket_route_without_boto3(self):
        """Test add_websocket_route when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = WebSocketAPIConfig(name="test-ws-api")
            api = integration.create_websocket_api(config)

            result = integration.add_websocket_route(
                api["apiId"],
                route_key="$connect",
                integration_uri="arn:aws:lambda:us-east-1:123456789012:function:my-function"
            )
            self.assertIn("routeKey", result)
            self.assertEqual(result["routeKey"], "$connect")
            self.assertEqual(result["apiId"], api["apiId"])
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_add_websocket_route_without_integration_uri(self):
        """Test add_websocket_route without integration_uri when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = WebSocketAPIConfig(name="test-ws-api")
            api = integration.create_websocket_api(config)

            result = integration.add_websocket_route(
                api["apiId"],
                route_key="message"
            )
            self.assertIn("routeKey", result)
            self.assertEqual(result["routeKey"], "message")
            # Note: in non-boto3 mode, integrationId is still generated for tracking
            self.assertIn("integrationId", result)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available


class TestAPIGatewayIntegrationResources(unittest.TestCase):
    """Test APIGatewayIntegration resource methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.return_value = self.mock_client

    def test_create_resource_without_boto3(self):
        """Test create_resource when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            integration._apis["test-api"] = {"id": "test-api"}

            result = integration.create_resource("test-api", "users")
            self.assertIn("id", result)
            self.assertEqual(result["path"], "/users")
            self.assertEqual(result["pathPart"], "users")
            self.assertEqual(result["apiId"], "test-api")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_create_resource_with_parent(self):
        """Test create_resource with parent_id when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            integration._apis["test-api"] = {"id": "test-api"}

            result = integration.create_resource("test-api", "users", parent_id="parent-123")
            self.assertEqual(result["parentId"], "parent-123")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_resource_from_cache(self):
        """Test get_resource returns cached resource"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            integration._resources["test-api/res-123"] = {
                "id": "res-123",
                "apiId": "test-api",
                "path": "/users",
                "pathPart": "users",
                "parentId": None
            }

            result = integration.get_resource("test-api", "res-123")
            self.assertEqual(result["path"], "/users")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_resource_not_found(self):
        """Test get_resource raises error when not found"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            with self.assertRaises(ValueError):
                integration.get_resource("test-api", "nonexistent")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available


class TestAPIGatewayIntegrationStages(unittest.TestCase):
    """Test APIGatewayIntegration stage methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.return_value = self.mock_client

    def test_create_stage_without_boto3(self):
        """Test create_stage when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = StageConfig(stage_name="prod", description="Production stage")

            result = integration.create_stage("test-api", config)
            self.assertIn("stageName", result)
            self.assertEqual(result["stageName"], "prod")
            self.assertEqual(result["description"], "Production stage")
            self.assertEqual(result["apiId"], "test-api")
            # Verify stored with correct key
            self.assertIn("test-api/prod", integration._stages)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_create_stage_with_variables(self):
        """Test create_stage with stage variables when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = StageConfig(
                stage_name="dev",
                variables={"env": "development", "version": "1.0"}
            )

            result = integration.create_stage("test-api", config)
            self.assertEqual(result["variables"]["env"], "development")
            self.assertEqual(result["variables"]["version"], "1.0")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_stage_without_boto3(self):
        """Test get_stage when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = StageConfig(stage_name="prod", description="Production stage")
            integration.create_stage("test-api", config)

            result = integration.get_stage("test-api", "prod")
            self.assertEqual(result["stageName"], "prod")
            self.assertEqual(result["description"], "Production stage")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_stage_not_found(self):
        """Test get_stage raises error when not found"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            with self.assertRaises(ValueError):
                integration.get_stage("test-api", "nonexistent")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_delete_stage_without_boto3(self):
        """Test delete_stage when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = StageConfig(stage_name="prod")
            integration.create_stage("test-api", config)

            result = integration.delete_stage("test-api", "prod")
            self.assertTrue(result)
            self.assertNotIn("test-api/prod", integration._stages)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available


class TestAPIGatewayIntegrationCustomDomains(unittest.TestCase):
    """Test APIGatewayIntegration custom domain methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.return_value = self.mock_client

    def test_create_custom_domain_without_boto3(self):
        """Test create_custom_domain when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = CustomDomainConfig(
                domain_name="api.example.com",
                certificate_arn="arn:aws:acm:us-east-1:123456789012:certificate/12345678-1234-1234-1234-123456789012"
            )

            result = integration.create_custom_domain(config)
            self.assertIn("domainName", result)
            self.assertEqual(result["domainName"], "api.example.com")
            self.assertEqual(result["securityPolicy"], "TLS_1_2")
            self.assertIn("domainId", result)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_custom_domain_from_cache(self):
        """Test get_custom_domain returns cached domain"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            integration._custom_domains["api.example.com"] = {
                "domainName": "api.example.com",
                "certificateArn": "arn:aws:acm:...",
                "securityPolicy": "TLS_1_2"
            }

            result = integration.get_custom_domain("api.example.com")
            self.assertEqual(result["domainName"], "api.example.com")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_delete_custom_domain_without_boto3(self):
        """Test delete_custom_domain when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = CustomDomainConfig(
                domain_name="api.example.com",
                certificate_arn="arn:aws:acm:us-east-1:123456789012:certificate/12345678-1234-1234-1234-123456789012"
            )
            integration.create_custom_domain(config)

            result = integration.delete_custom_domain("api.example.com")
            self.assertTrue(result)
            self.assertNotIn("api.example.com", integration._custom_domains)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available


class TestAPIGatewayIntegrationAPIKeys(unittest.TestCase):
    """Test APIGatewayIntegration API key methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.return_value = self.mock_client

    def test_create_api_key_without_boto3(self):
        """Test create_api_key when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = APIKeyConfig(name="test-key", description="Test API key")

            result = integration.create_api_key(config)
            self.assertIn("id", result)
            self.assertEqual(result["name"], "test-key")
            self.assertEqual(result["description"], "Test API key")
            self.assertTrue(result["enabled"])
            self.assertIn("value", result)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_api_key_from_cache(self):
        """Test get_api_key returns cached key"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            integration._api_keys["key-123"] = {
                "id": "key-123",
                "name": "test-key",
                "enabled": True
            }

            result = integration.get_api_key("key-123")
            self.assertEqual(result["name"], "test-key")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_delete_api_key_without_boto3(self):
        """Test delete_api_key when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = APIKeyConfig(name="test-key")
            created = integration.create_api_key(config)

            result = integration.delete_api_key(created["id"])
            self.assertTrue(result)
            self.assertNotIn(created["id"], integration._api_keys)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available


class TestAPIGatewayIntegrationUsagePlans(unittest.TestCase):
    """Test APIGatewayIntegration usage plan methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.return_value = self.mock_client

    def test_create_usage_plan_without_boto3(self):
        """Test create_usage_plan when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            config = UsagePlanConfig(
                name="basic-plan",
                quota={"limit": 1000, "period": "WEEK"},
                throttle={"rateLimit": 100}
            )

            result = integration.create_usage_plan(config)
            self.assertIn("id", result)
            self.assertEqual(result["name"], "basic-plan")
            self.assertEqual(result["quota"]["limit"], 1000)
            self.assertEqual(result["throttle"]["rateLimit"], 100)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_usage_plan_from_cache(self):
        """Test get_usage_plan returns cached plan"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            integration._usage_plans["up-123"] = {
                "id": "up-123",
                "name": "basic-plan",
                "quota": {"limit": 1000}
            }

            result = integration.get_usage_plan("up-123")
            self.assertEqual(result["name"], "basic-plan")
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available


class TestAPIGatewayIntegrationMethods(unittest.TestCase):
    """Test APIGatewayIntegration method management"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.return_value = self.mock_client

    def test_create_method_without_boto3(self):
        """Test create_method when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            result = integration.create_method(
                api_id="test-api",
                resource_id="res-123",
                http_method="GET",
                authorization_type="NONE",
                api_key_required=False
            )
            self.assertEqual(result["httpMethod"], "GET")
            self.assertEqual(result["resourceId"], "res-123")
            self.assertEqual(result["authorizationType"], "NONE")
            self.assertIn("methodId", result)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_create_integration_without_boto3(self):
        """Test create_integration when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            result = integration.create_integration(
                api_id="test-api",
                resource_id="res-123",
                http_method="GET",
                integration_type="AWS_PROXY",
                uri="arn:aws:lambda:us-east-1:123456789012:function:my-function"
            )
            self.assertEqual(result["httpMethod"], "GET")
            self.assertEqual(result["integrationType"], "AWS_PROXY")
            self.assertEqual(result["uri"], "arn:aws:lambda:us-east-1:123456789012:function:my-function")
            self.assertIn("integrationId", result)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_create_lambda_integration_without_boto3(self):
        """Test create_lambda_integration when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration(region="us-east-1")
            result = integration.create_lambda_integration(
                api_id="test-api",
                resource_id="res-123",
                http_method="POST",
                function_name="my-function"
            )
            self.assertEqual(result["httpMethod"], "POST")
            self.assertEqual(result["integrationType"], "AWS_PROXY")
            self.assertIn("my-function", result["uri"])
            self.assertIn("invocations", result["uri"])
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available


class TestAPIGatewayIntegrationDeployment(unittest.TestCase):
    """Test APIGatewayIntegration deployment methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.return_value = self.mock_client

    def test_create_deployment_without_boto3(self):
        """Test create_deployment when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            result = integration.create_deployment(
                api_id="test-api",
                description="Test deployment"
            )
            self.assertEqual(result["apiId"], "test-api")
            self.assertIn("deploymentId", result)
            self.assertEqual(result["description"], "Test deployment")
            self.assertIn("createdAt", result)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available


class TestAPIGatewayIntegrationCloudWatch(unittest.TestCase):
    """Test APIGatewayIntegration CloudWatch methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.return_value = self.mock_client

    def test_configure_access_logging_without_boto3(self):
        """Test configure_access_logging when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            result = integration.configure_access_logging(
                api_id="test-api",
                stage_name="prod",
                log_group_arn="arn:aws:logs:us-east-1:123456789012:log-group:/aws/apigateway/test-api"
            )
            self.assertEqual(result["apiId"], "test-api")
            self.assertEqual(result["stageName"], "prod")
            self.assertTrue(result["loggingEnabled"])
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_create_log_group_without_boto3(self):
        """Test create_log_group when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            result = integration.create_log_group(
                log_group_name="/aws/apigateway/test-api",
                retention_days=30
            )
            self.assertEqual(result["logGroupName"], "/aws/apigateway/test-api")
            self.assertEqual(result["retentionDays"], 30)
            self.assertTrue(result["created"])
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_set_throttling_without_boto3(self):
        """Test set_throttling when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            result = integration.set_throttling(
                api_id="test-api",
                stage_name="prod",
                burst_limit=1000,
                rate_limit=500
            )
            self.assertEqual(result["apiId"], "test-api")
            self.assertEqual(result["stageName"], "prod")
            self.assertEqual(result["burstLimit"], 1000)
            self.assertEqual(result["rateLimit"], 500)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_enable_cloudwatch_metrics_without_boto3(self):
        """Test enable_cloudwatch_metrics when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            result = integration.enable_cloudwatch_metrics(
                api_id="test-api",
                stage_name="prod"
            )
            self.assertEqual(result["apiId"], "test-api")
            self.assertEqual(result["stageName"], "prod")
            self.assertTrue(result["metricsEnabled"])
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_get_api_metrics_without_boto3(self):
        """Test get_api_metrics when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            result = integration.get_api_metrics(
                api_id="test-api",
                stage_name="prod",
                metric_names=["Count", "Latency"]
            )
            self.assertEqual(result["apiId"], "test-api")
            self.assertEqual(result["stageName"], "prod")
            self.assertIn("Count", result["metrics"])
            self.assertIn("Latency", result["metrics"])
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available


class TestAPIGatewayIntegrationUtility(unittest.TestCase):
    """Test APIGatewayIntegration utility methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.return_value = self.mock_client

    def test_export_api_without_boto3(self):
        """Test export_api when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            result = integration.export_api(
                api_id="test-api",
                export_type="swagger"
            )
            self.assertEqual(result["apiId"], "test-api")
            self.assertEqual(result["exportType"], "swagger")
            self.assertIn("data", result)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_import_api_without_boto3(self):
        """Test import_api when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            result = integration.import_api(
                body='{"swagger": "2.0", "info": {"title": "Test API"}}',
                mode="overwrite"
            )
            self.assertIn("apiId", result)
            self.assertTrue(result["imported"])
            self.assertEqual(result["warnings"], [])
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_flush_authorizer_cache_without_boto3(self):
        """Test flush_authorizer_cache when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            result = integration.flush_authorizer_cache(
                api_id="test-api",
                authorizer_id="auth-123"
            )
            self.assertTrue(result)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_flush_stage_cache_without_boto3(self):
        """Test flush_stage_cache when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            result = integration.flush_stage_cache(
                api_id="test-api",
                stage_name="prod"
            )
            self.assertTrue(result)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available

    def test_close_without_boto3(self):
        """Test close when boto3 is not available"""
        import src.workflow_aws_api_gateway as api_gateway_module
        original_boto3_available = api_gateway_module.BOTO3_AVAILABLE
        api_gateway_module.BOTO3_AVAILABLE = False

        try:
            integration = APIGatewayIntegration()
            integration.close()
            # Should clear internal state
            self.assertEqual(len(integration._clients), 0)
        finally:
            api_gateway_module.BOTO3_AVAILABLE = original_boto3_available


if __name__ == "__main__":
    unittest.main()

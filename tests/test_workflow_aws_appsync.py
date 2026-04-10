"""
Tests for workflow_aws_appsync module
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

# Create mock boto3 module before importing workflow_aws_appsync
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
import src.workflow_aws_appsync as appsync_module

AppSyncIntegration = appsync_module.AppSyncIntegration
APIAuthType = appsync_module.APIAuthType
AuthorizationMode = appsync_module.AuthorizationMode
DataSourceType = appsync_module.DataSourceType
RuntimeType = appsync_module.RuntimeType
GraphQLAPIConfig = appsync_module.GraphQLAPIConfig
SchemaConfig = appsync_module.SchemaConfig
ResolverConfig = appsync_module.ResolverConfig
DataSourceConfig = appsync_module.DataSourceConfig
FunctionConfig = appsync_module.FunctionConfig
APIKeyConfig = appsync_module.APIKeyConfig
CognitoAuthConfig = appsync_module.CognitoAuthConfig
LambdaAuthorizerConfig = appsync_module.LambdaAuthorizerConfig
SubscriptionConfig = appsync_module.SubscriptionConfig
CloudWatchConfig = appsync_module.CloudWatchConfig


class TestAppSyncEnums(unittest.TestCase):
    """Test AppSync enums"""

    def test_api_auth_type_values(self):
        self.assertEqual(APIAuthType.API_KEY.value, "API_KEY")
        self.assertEqual(APIAuthType.IAM.value, "IAM")
        self.assertEqual(APIAuthType.Cognito_USER_POOL.value, "AMAZON_COGNITO_USER_POOLS")
        self.assertEqual(APIAuthType.OPENID_CONNECT.value, "OPENID_CONNECT")
        self.assertEqual(APIAuthType.Lambda_AUTH.value, "AWS_LAMBDA")

    def test_authorization_mode_values(self):
        self.assertEqual(AuthorizationMode.API_KEY.value, "API_KEY")
        self.assertEqual(AuthorizationMode.IAM.value, "IAM")
        self.assertEqual(AuthorizationMode.AMAZON_COGNITO_USER_POOLS.value, "AMAZON_COGNITO_USER_POOLS")
        self.assertEqual(AuthorizationMode.OPENID_CONNECT.value, "OPENID_CONNECT")
        self.assertEqual(AuthorizationMode.AWS_LAMBDA.value, "AWS_LAMBDA")

    def test_data_source_type_values(self):
        self.assertEqual(DataSourceType.NONE.value, "NONE")
        self.assertEqual(DataSourceType.AWS_LAMBDA.value, "AWS_LAMBDA")
        self.assertEqual(DataSourceType.AMAZON_DYNAMODB.value, "AMAZON_DYNAMODB")
        self.assertEqual(DataSourceType.AMAZON_ELASTICSEARCH.value, "AMAZON_ELASTICSEARCH")
        self.assertEqual(DataSourceType.AMAZON_OPENSEARCH.value, "AMAZON_OPENSEARCH")
        self.assertEqual(DataSourceType.HTTP.value, "HTTP")
        self.assertEqual(DataSourceType.RELATIONAL_DATABASE.value, "RELATIONAL_DATABASE")
        self.assertEqual(DataSourceType.AMAZON_EVENTBRIDGE.value, "AMAZON_EVENTBRIDGE")

    def test_runtime_type_values(self):
        self.assertEqual(RuntimeType.APPSYNC_JS.value, "APPSYNC_JS")


class TestAppSyncDataclasses(unittest.TestCase):
    """Test AppSync dataclasses"""

    def test_graphql_api_config_defaults(self):
        config = GraphQLAPIConfig(name="test-api")
        self.assertEqual(config.name, "test-api")
        self.assertEqual(config.auth_type, APIAuthType.API_KEY)
        self.assertEqual(config.log_level, "ALL")
        self.assertEqual(len(config.additional_auth_types), 0)
        self.assertTrue(config.metrics_enabled)

    def test_graphql_api_config_custom(self):
        config = GraphQLAPIConfig(
            name="test-api",
            auth_type=APIAuthType.Cognito_USER_POOL,
            description="Test API",
            log_level="ERROR",
            user_pool_config={
                "userPoolId": "us-east-1_test",
                "awsRegion": "us-east-1",
                "defaultAction": "ALLOW"
            }
        )
        self.assertEqual(config.name, "test-api")
        self.assertEqual(config.auth_type, APIAuthType.Cognito_USER_POOL)
        self.assertEqual(config.log_level, "ERROR")
        self.assertIsNotNone(config.user_pool_config)

    def test_schema_config(self):
        config = SchemaConfig(
            definition="type Query { getUser(id: ID!): User }",
            description="Test schema"
        )
        self.assertIn("Query", config.definition)
        self.assertEqual(config.description, "Test schema")

    def test_resolver_config(self):
        config = ResolverConfig(
            type_name="Query",
            field_name="getUser",
            data_source_name="UserTable",
            request_template="$util.db.query($ctx.args.id)",
            response_template="$util.toJson($ctx.result)"
        )
        self.assertEqual(config.type_name, "Query")
        self.assertEqual(config.field_name, "getUser")
        self.assertEqual(config.data_source_name, "UserTable")

    def test_data_source_config(self):
        config = DataSourceConfig(
            name="TestDynamoDB",
            data_source_type=DataSourceType.AMAZON_DYNAMODB,
            description="Test data source",
            dynamodb_config={
                "tableName": "Users",
                "region": "us-east-1"
            }
        )
        self.assertEqual(config.name, "TestDynamoDB")
        self.assertEqual(config.data_source_type, DataSourceType.AMAZON_DYNAMODB)
        self.assertEqual(config.dynamodb_config["tableName"], "Users")

    def test_function_config(self):
        config = FunctionConfig(
            name="TestFunction",
            data_source_name="TestDS",
            request_mapping_template="{}",
            response_mapping_template="$util.toJson($ctx.result)"
        )
        self.assertEqual(config.name, "TestFunction")
        self.assertEqual(config.data_source_name, "TestDS")

    def test_api_key_config(self):
        config = APIKeyConfig(
            description="Test API Key",
            expires=datetime(2025, 12, 31)
        )
        self.assertEqual(config.description, "Test API Key")
        self.assertEqual(config.expires, datetime(2025, 12, 31))

    def test_cognito_auth_config(self):
        config = CognitoAuthConfig(
            user_pool_id="us-east-1_test",
            aws_region="us-east-1"
        )
        self.assertEqual(config.user_pool_id, "us-east-1_test")
        self.assertEqual(config.aws_region, "us-east-1")
        self.assertEqual(config.default_action, "ALLOW")

    def test_lambda_authorizer_config(self):
        config = LambdaAuthorizerConfig(
            authorizer_uri="arn:aws:lambda:us-east-1:123456789012:function/auth",
            lambda_arity=2,
            lambda_version="VARIABLE"
        )
        self.assertEqual(config.authorizer_uri, "arn:aws:lambda:us-east-1:123456789012:function/auth")
        self.assertEqual(config.lambda_arity, 2)
        self.assertEqual(config.lambda_version, "VARIABLE")

    def test_subscription_config(self):
        config = SubscriptionConfig(
            mutation_field="onCreateUser",
            topic="users/create",
            filter_scope="userId"
        )
        self.assertEqual(config.mutation_field, "onCreateUser")
        self.assertEqual(config.topic, "users/create")
        self.assertEqual(config.filter_scope, "userId")

    def test_cloud_watch_config_defaults(self):
        config = CloudWatchConfig()
        self.assertEqual(config.log_level, "ALL")
        self.assertEqual(config.field_log_level, "ALL")
        self.assertTrue(config.metrics_enabled)
        self.assertTrue(config.detailed_metrics)


class TestAppSyncIntegration(unittest.TestCase):
    """Test AppSyncIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_appsync_client = MagicMock()
        self.mock_logs_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        self.mock_lambda_client = MagicMock()
        self.mock_cognito_client = MagicMock()
        self.mock_dynamodb_client = MagicMock()
        self.mock_events_client = MagicMock()

        with patch.object(AppSyncIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = AppSyncIntegration()
            self.integration.region = "us-east-1"
            self.integration.role_arn = None
            self.integration.external_id = None
            self.integration._clients = {
                'appsync': self.mock_appsync_client,
                'logs': self.mock_logs_client,
                'cloudwatch': self.mock_cloudwatch_client,
                'lambda': self.mock_lambda_client,
                'cognito-idp': self.mock_cognito_client,
                'dynamodb': self.mock_dynamodb_client,
                'events': self.mock_events_client
            }
            self.integration._lock = MagicMock()
            self.integration._graphql_apis = {}
            self.integration._schemas = {}
            self.integration._resolvers = {}
            self.integration._data_sources = {}
            self.integration._functions = {}
            self.integration._api_keys = {}
            self.integration._cognito_configs = {}
            self.integration._lambda_authorizers = {}
            self.integration._subscriptions = {}
            self.integration._cloudwatch_configs = {}

    def test_generate_id(self):
        """Test ID generation"""
        id1 = self.integration._generate_id("api-")
        id2 = self.integration._generate_id("api-")
        self.assertTrue(id1.startswith("api-"))
        self.assertTrue(id2.startswith("api-"))
        self.assertNotEqual(id1, id2)


class TestAppSyncGraphQLAPIManagement(unittest.TestCase):
    """Test AppSync GraphQL API management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_appsync_client = MagicMock()

        with patch.object(AppSyncIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = AppSyncIntegration()
            self.integration.region = "us-east-1"
            self.integration._clients = {'appsync': self.mock_appsync_client}
            self.integration._lock = MagicMock()
            self.integration._graphql_apis = {}
            self.integration._schemas = {}
            self.integration._resolvers = {}
            self.integration._data_sources = {}
            self.integration._functions = {}
            self.integration._api_keys = {}
            self.integration._cognito_configs = {}
            self.integration._lambda_authorizers = {}
            self.integration._subscriptions = {}
            self.integration._cloudwatch_configs = {}

    def test_create_graphql_api(self):
        """Test creating a GraphQL API"""
        mock_response = {
            'graphqlApi': {
                'apiId': 'graphql-api-123',
                'name': 'test-api',
                'authenticationType': 'API_KEY',
                'arn': 'arn:aws:appsync:us-east-1:123456789012:apis/graphql-api-123',
                'createdAt': '2024-01-01T00:00:00Z',
                'updatedAt': '2024-01-01T00:00:00Z'
            }
        }
        self.mock_appsync_client.create_graphql_api.return_value = mock_response

        config = GraphQLAPIConfig(
            name="test-api",
            auth_type=APIAuthType.API_KEY,
            description="Test API"
        )

        result = self.integration.create_graphql_api(config)

        self.assertEqual(result['apiId'], 'graphql-api-123')
        self.assertEqual(result['name'], 'test-api')

    def test_get_graphql_api(self):
        """Test getting a GraphQL API"""
        mock_response = {
            'graphqlApi': {
                'apiId': 'graphql-api-123',
                'name': 'test-api',
                'authenticationType': 'API_KEY'
            }
        }
        self.mock_appsync_client.get_graphql_api.return_value = mock_response

        result = self.integration.get_graphql_api('graphql-api-123')

        self.assertEqual(result['apiId'], 'graphql-api-123')
        self.assertIn('graphql-api-123', self.integration._graphql_apis)

    def test_get_graphql_api_from_cache(self):
        """Test getting GraphQL API from cache"""
        self.integration._graphql_apis['graphql-api-123'] = {
            'apiId': 'graphql-api-123',
            'name': 'cached-api'
        }

        result = self.integration.get_graphql_api('graphql-api-123')

        self.assertEqual(result['name'], 'cached-api')

    def test_list_graphql_apis(self):
        """Test listing GraphQL APIs"""
        mock_response = {
            'graphqlApis': [
                {'apiId': 'api-1', 'name': 'API 1'},
                {'apiId': 'api-2', 'name': 'API 2'}
            ]
        }
        self.mock_appsync_client.list_graphql_apis.return_value = mock_response

        result = self.integration.list_graphql_apis()

        self.assertEqual(len(result), 2)

    def test_update_graphql_api(self):
        """Test updating a GraphQL API"""
        mock_response = {
            'graphqlApi': {
                'apiId': 'graphql-api-123',
                'name': 'updated-api',
                'updatedAt': '2024-01-01T00:00:00Z'
            }
        }
        self.mock_appsync_client.update_graphql_api.return_value = mock_response

        config = GraphQLAPIConfig(
            name="updated-api",
            auth_type=APIAuthType.API_KEY
        )

        result = self.integration.update_graphql_api('graphql-api-123', config)

        self.assertEqual(result['name'], 'updated-api')

    def test_delete_graphql_api(self):
        """Test deleting a GraphQL API"""
        self.mock_appsync_client.delete_graphql_api.return_value = {}

        result = self.integration.delete_graphql_api('graphql-api-123')

        self.assertTrue(result)


class TestAppSyncSchemaManagement(unittest.TestCase):
    """Test AppSync schema management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_appsync_client = MagicMock()

        with patch.object(AppSyncIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = AppSyncIntegration()
            self.integration.region = "us-east-1"
            self.integration._clients = {'appsync': self.mock_appsync_client}
            self.integration._lock = MagicMock()
            self.integration._schemas = {}

    def test_start_schema_creation(self):
        """Test starting schema creation"""
        mock_response = {
            'schemaValidationResult': {
                'status': 'PASS'
            }
        }
        self.mock_appsync_client.start_schema_creation.return_value = mock_response

        schema_definition = """
        type Query {
            hello: String
        }
        type Mutation {
            createUser(name: String!): User
        }
        """

        config = SchemaConfig(definition=schema_definition)
        result = self.integration.start_schema_creation('graphql-api-123', config)

        self.assertTrue(result)

    def test_get_schema(self):
        """Test getting schema"""
        mock_response = {
            'schema': {
                'definition': 'type Query { hello: String }',
                'status': 'ACTIVE'
            }
        }
        self.mock_appsync_client.get_schema.return_value = mock_response

        result = self.integration.get_schema('graphql-api-123')

        self.assertIn('definition', result)


class TestAppSyncDataSourceManagement(unittest.TestCase):
    """Test AppSync data source management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_appsync_client = MagicMock()

        with patch.object(AppSyncIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = AppSyncIntegration()
            self.integration.region = "us-east-1"
            self.integration._clients = {'appsync': self.mock_appsync_client}
            self.integration._lock = MagicMock()
            self.integration._data_sources = {}

    def test_create_data_source(self):
        """Test creating a data source"""
        mock_response = {
            'dataSource': {
                'dataSourceArn': 'arn:aws:appsync:us-east-1:123456789012:apis/graphql-api-123/datasources/TestDS',
                'name': 'TestDS',
                'type': 'AMAZON_DYNAMODB'
            }
        }
        self.mock_appsync_client.create_data_source.return_value = mock_response

        config = DataSourceConfig(
            name="TestDS",
            data_source_type=DataSourceType.AMAZON_DYNAMODB,
            dynamodb_config={
                "tableName": "Users",
                "region": "us-east-1"
            }
        )

        result = self.integration.create_data_source('graphql-api-123', config)

        self.assertEqual(result['name'], 'TestDS')

    def test_get_data_source(self):
        """Test getting a data source"""
        mock_response = {
            'dataSource': {
                'name': 'TestDS',
                'type': 'AMAZON_DYNAMODB',
                'dataSourceArn': 'arn:aws:appsync:us-east-1:123456789012:apis/graphql-api-123/datasources/TestDS'
            }
        }
        self.mock_appsync_client.get_data_source.return_value = mock_response

        result = self.integration.get_data_source('graphql-api-123', 'TestDS')

        self.assertEqual(result['name'], 'TestDS')

    def test_list_data_sources(self):
        """Test listing data sources"""
        mock_response = {
            'dataSources': [
                {'name': 'DS1', 'type': 'AMAZON_DYNAMODB'},
                {'name': 'DS2', 'type': 'AWS_LAMBDA'}
            ]
        }
        self.mock_appsync_client.list_data_sources.return_value = mock_response

        result = self.integration.list_data_sources('graphql-api-123')

        self.assertEqual(len(result), 2)

    def test_delete_data_source(self):
        """Test deleting a data source"""
        self.mock_appsync_client.delete_data_source.return_value = {}

        result = self.integration.delete_data_source('graphql-api-123', 'TestDS')

        self.assertTrue(result)


class TestAppSyncResolverManagement(unittest.TestCase):
    """Test AppSync resolver management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_appsync_client = MagicMock()

        with patch.object(AppSyncIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = AppSyncIntegration()
            self.integration.region = "us-east-1"
            self.integration._clients = {'appsync': self.mock_appsync_client}
            self.integration._lock = MagicMock()
            self.integration._resolvers = {}

    def test_create_resolver(self):
        """Test creating a resolver"""
        mock_response = {
            'resolver': {
                'typeName': 'Query',
                'fieldName': 'getUser',
                'resolverArn': 'arn:aws:appsync:us-east-1:123456789012:apis/graphql-api-123/resolvers/Query.getUser'
            }
        }
        self.mock_appsync_client.create_resolver.return_value = mock_response

        config = ResolverConfig(
            type_name="Query",
            field_name="getUser",
            data_source_name="UserTable",
            request_template="$util.db.get($ctx.args.id)",
            response_template="$util.toJson($ctx.result)"
        )

        result = self.integration.create_resolver('graphql-api-123', config)

        self.assertEqual(result['typeName'], 'Query')

    def test_get_resolver(self):
        """Test getting a resolver"""
        mock_response = {
            'resolver': {
                'typeName': 'Query',
                'fieldName': 'getUser',
                'runtime': {'name': 'APPSYNC_JS'}
            }
        }
        self.mock_appsync_client.get_resolver.return_value = mock_response

        result = self.integration.get_resolver('graphql-api-123', 'Query', 'getUser')

        self.assertEqual(result['typeName'], 'Query')
        self.assertEqual(result['fieldName'], 'getUser')

    def test_delete_resolver(self):
        """Test deleting a resolver"""
        self.mock_appsync_client.delete_resolver.return_value = {}

        result = self.integration.delete_resolver('graphql-api-123', 'Query', 'getUser')

        self.assertTrue(result)


class TestAppSyncFunctionManagement(unittest.TestCase):
    """Test AppSync function management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_appsync_client = MagicMock()

        with patch.object(AppSyncIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = AppSyncIntegration()
            self.integration.region = "us-east-1"
            self.integration._clients = {'appsync': self.mock_appsync_client}
            self.integration._lock = MagicMock()
            self.integration._functions = {}

    def test_create_function(self):
        """Test creating an AppSync function"""
        mock_response = {
            'functionConfiguration': {
                'functionId': 'func-123',
                'name': 'TestFunction',
                'functionArn': 'arn:aws:appsync:us-east-1:123456789012:apis/graphql-api-123/functions/func-123'
            }
        }
        self.mock_appsync_client.create_function.return_value = mock_response

        config = FunctionConfig(
            name="TestFunction",
            data_source_name="TestDS",
            request_mapping_template="{}",
            response_mapping_template="$util.toJson($ctx.result)"
        )

        result = self.integration.create_function('graphql-api-123', config)

        self.assertEqual(result['functionId'], 'func-123')

    def test_get_function(self):
        """Test getting an AppSync function"""
        mock_response = {
            'functionConfiguration': {
                'functionId': 'func-123',
                'name': 'TestFunction'
            }
        }
        self.mock_appsync_client.get_function.return_value = mock_response

        result = self.integration.get_function('graphql-api-123', 'func-123')

        self.assertEqual(result['functionId'], 'func-123')

    def test_list_functions(self):
        """Test listing AppSync functions"""
        mock_response = {
            'functions': [
                {'functionId': 'func-1', 'name': 'Function 1'},
                {'functionId': 'func-2', 'name': 'Function 2'}
            ]
        }
        self.mock_appsync_client.list_functions.return_value = mock_response

        result = self.integration.list_functions('graphql-api-123')

        self.assertEqual(len(result), 2)

    def test_delete_function(self):
        """Test deleting an AppSync function"""
        self.mock_appsync_client.delete_function.return_value = {}

        result = self.integration.delete_function('graphql-api-123', 'func-123')

        self.assertTrue(result)


class TestAppSyncAPIKeyManagement(unittest.TestCase):
    """Test AppSync API key management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_appsync_client = MagicMock()

        with patch.object(AppSyncIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = AppSyncIntegration()
            self.integration.region = "us-east-1"
            self.integration._clients = {'appsync': self.mock_appsync_client}
            self.integration._lock = MagicMock()
            self.integration._api_keys = {}

    def test_create_api_key(self):
        """Test creating an API key"""
        mock_response = {
            'apiKey': {
                'id': 'key-123',
                'expires': datetime(2025, 12, 31).isoformat()
            }
        }
        self.mock_appsync_client.create_api_key.return_value = mock_response

        config = APIKeyConfig(description="Test Key")

        result = self.integration.create_api_key('graphql-api-123', config)

        self.assertEqual(result['id'], 'key-123')

    def test_list_api_keys(self):
        """Test listing API keys"""
        mock_response = {
            'apiKeys': [
                {'id': 'key-1', 'description': 'Key 1'},
                {'id': 'key-2', 'description': 'Key 2'}
            ]
        }
        self.mock_appsync_client.list_api_keys.return_value = mock_response

        result = self.integration.list_api_keys('graphql-api-123')

        self.assertEqual(len(result), 2)

    def test_delete_api_key(self):
        """Test deleting an API key"""
        self.mock_appsync_client.delete_api_key.return_value = {}

        result = self.integration.delete_api_key('graphql-api-123', 'key-123')

        self.assertTrue(result)


class TestAppSyncCognitoAuthentication(unittest.TestCase):
    """Test AppSync Cognito authentication methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_appsync_client = MagicMock()
        self.mock_cognito_client = MagicMock()

        with patch.object(AppSyncIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = AppSyncIntegration()
            self.integration.region = "us-east-1"
            self.integration._clients = {
                'appsync': self.mock_appsync_client,
                'cognito-idp': self.mock_cognito_client
            }
            self.integration._lock = MagicMock()
            self.integration._cognito_configs = {}

    def test_configure_cognito_authentication(self):
        """Test configuring Cognito authentication"""
        self.mock_appsync_client.update_graphql_api.return_value = {}

        config = CognitoAuthConfig(
            user_pool_id="us-east-1_test",
            aws_region="us-east-1"
        )

        result = self.integration.configure_cognito_authentication('graphql-api-123', config)

        self.assertTrue(result)


class TestAppSyncLambdaAuthorizers(unittest.TestCase):
    """Test AppSync Lambda authorizer methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_appsync_client = MagicMock()
        self.mock_lambda_client = MagicMock()

        with patch.object(AppSyncIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = AppSyncIntegration()
            self.integration.region = "us-east-1"
            self.integration._clients = {
                'appsync': self.mock_appsync_client,
                'lambda': self.mock_lambda_client
            }
            self.integration._lock = MagicMock()
            self.integration._lambda_authorizers = {}

    def test_configure_lambda_authorizer(self):
        """Test configuring Lambda authorizer"""
        self.mock_appsync_client.update_graphql_api.return_value = {}

        config = LambdaAuthorizerConfig(
            authorizer_uri="arn:aws:lambda:us-east-1:123456789012:function/auth",
            lambda_arity=1
        )

        result = self.integration.configure_lambda_authorizer('graphql-api-123', config)

        self.assertTrue(result)


class TestAppSyncWebSocketSubscriptions(unittest.TestCase):
    """Test AppSync WebSocket subscription methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_appsync_client = MagicMock()

        with patch.object(AppSyncIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = AppSyncIntegration()
            self.integration.region = "us-east-1"
            self.integration._clients = {'appsync': self.mock_appsync_client}
            self.integration._lock = MagicMock()
            self.integration._subscriptions = {}

    def test_create_subscription(self):
        """Test creating a subscription"""
        self.mock_appsync_client.create_graphql_api.return_value = {
            'graphqlApi': {
                'apiId': 'graphql-api-123',
                'name': 'test-api'
            }
        }

        config = SubscriptionConfig(
            mutation_field="onCreateUser",
            filter_scope="userId"
        )

        result = self.integration.create_subscription('graphql-api-123', config)

        self.assertTrue(result)

    def test_get_subscription_info(self):
        """Test getting subscription info"""
        self.integration._subscriptions['graphql-api-123'] = [
            {'mutation_field': 'onCreateUser', 'filter_scope': 'userId'}
        ]

        result = self.integration.get_subscription_info('graphql-api-123')

        self.assertEqual(len(result), 1)


class TestAppSyncCloudWatchMonitoring(unittest.TestCase):
    """Test AppSync CloudWatch monitoring methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudwatch_client = MagicMock()
        self.mock_appsync_client = MagicMock()

        with patch.object(AppSyncIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = AppSyncIntegration()
            self.integration.region = "us-east-1"
            self.integration._clients = {
                'appsync': self.mock_appsync_client,
                'cloudwatch': self.mock_cloudwatch_client
            }
            self.integration._lock = MagicMock()
            self.integration._cloudwatch_configs = {}

    def test_get_api_metrics(self):
        """Test getting API metrics"""
        self.mock_cloudwatch_client.get_metric_statistics.return_value = {
            'Datapoints': [
                {'Average': 100.0, 'Maximum': 200.0, 'Minimum': 50.0, 'Timestamp': '2024-01-01T00:00:00Z'}
            ]
        }

        result = self.integration.get_api_metrics('graphql-api-123')

        self.assertIsNotNone(result)
        self.mock_cloudwatch_client.get_metric_statistics.assert_called()

    def test_configure_cloudwatch_logging(self):
        """Test configuring CloudWatch logging"""
        self.mock_appsync_client.update_graphql_api.return_value = {}

        config = CloudWatchConfig(
            log_level="ALL",
            field_log_level="ALL",
            metrics_enabled=True
        )

        result = self.integration.configure_cloudwatch_logging('graphql-api-123', config)

        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()

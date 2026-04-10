"""
Tests for workflow_aws_dynamodb module
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

# Create mock boto3 module before importing workflow_aws_dynamodb
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
from src.workflow_aws_dynamodb import (
    DynamoDBIntegration,
    TableStatus,
    CapacityMode,
    IndexStatus,
    StreamStatus,
    StreamViewType,
    KeyType,
    ProjectionType,
    AttributeDefinition,
    KeySchemaElement,
    GlobalSecondaryIndex,
    LocalSecondaryIndex,
    TableConfig,
    ItemOperation,
)


class TestTableStatus(unittest.TestCase):
    """Test TableStatus enum"""

    def test_table_status_values(self):
        self.assertEqual(TableStatus.CREATING.value, "CREATING")
        self.assertEqual(TableStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(TableStatus.UPDATING.value, "UPDATING")
        self.assertEqual(TableStatus.DELETING.value, "DELETING")
        self.assertEqual(TableStatus.DELETED.value, "DELETED")


class TestCapacityMode(unittest.TestCase):
    """Test CapacityMode enum"""

    def test_capacity_mode_values(self):
        self.assertEqual(CapacityMode.ON_DEMAND.value, "PROVISIONED")
        self.assertEqual(CapacityMode.PROVISIONED.value, "PROVISIONED")
        self.assertEqual(CapacityMode.PAY_PER_REQUEST.value, "PAY_PER_REQUEST")


class TestIndexStatus(unittest.TestCase):
    """Test IndexStatus enum"""

    def test_index_status_values(self):
        self.assertEqual(IndexStatus.CREATING.value, "CREATING")
        self.assertEqual(IndexStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(IndexStatus.UPDATING.value, "UPDATING")
        self.assertEqual(IndexStatus.DELETING.value, "DELETING")
        self.assertEqual(IndexStatus.DELETED.value, "DELETED")


class TestStreamStatus(unittest.TestCase):
    """Test StreamStatus enum"""

    def test_stream_status_values(self):
        self.assertEqual(StreamStatus.ENABLING.value, "ENABLING")
        self.assertEqual(StreamStatus.ENABLED.value, "ENABLED")
        self.assertEqual(StreamStatus.DISABLING.value, "DISABLING")
        self.assertEqual(StreamStatus.DISABLED.value, "DISABLED")


class TestStreamViewType(unittest.TestCase):
    """Test StreamViewType enum"""

    def test_stream_view_type_values(self):
        self.assertEqual(StreamViewType.NEW_IMAGE.value, "NEW_IMAGE")
        self.assertEqual(StreamViewType.OLD_IMAGE.value, "OLD_IMAGE")
        self.assertEqual(StreamViewType.NEW_AND_OLD_IMAGES.value, "NEW_AND_OLD_IMAGES")
        self.assertEqual(StreamViewType.KEYS_ONLY.value, "KEYS_ONLY")


class TestKeyType(unittest.TestCase):
    """Test KeyType enum"""

    def test_key_type_values(self):
        self.assertEqual(KeyType.HASH.value, "HASH")
        self.assertEqual(KeyType.RANGE.value, "RANGE")


class TestProjectionType(unittest.TestCase):
    """Test ProjectionType enum"""

    def test_projection_type_values(self):
        self.assertEqual(ProjectionType.ALL.value, "ALL")
        self.assertEqual(ProjectionType.KEYS_ONLY.value, "KEYS_ONLY")
        self.assertEqual(ProjectionType.INCLUDE.value, "INCLUDE")


class TestAttributeDefinition(unittest.TestCase):
    """Test AttributeDefinition dataclass"""

    def test_attribute_definition_creation(self):
        attr = AttributeDefinition(
            attribute_name="user_id",
            attribute_type="S"
        )
        self.assertEqual(attr.attribute_name, "user_id")
        self.assertEqual(attr.attribute_type, "S")


class TestKeySchemaElement(unittest.TestCase):
    """Test KeySchemaElement dataclass"""

    def test_key_schema_element_creation(self):
        key = KeySchemaElement(
            attribute_name="user_id",
            key_type="HASH"
        )
        self.assertEqual(key.attribute_name, "user_id")
        self.assertEqual(key.key_type, "HASH")


class TestGlobalSecondaryIndex(unittest.TestCase):
    """Test GlobalSecondaryIndex dataclass"""

    def test_global_secondary_index_creation(self):
        gsi = GlobalSecondaryIndex(
            index_name="email-index",
            key_schema=[
                KeySchemaElement(attribute_name="email", key_type="HASH")
            ],
            projection={"ProjectionType": "ALL"}
        )
        self.assertEqual(gsi.index_name, "email-index")
        self.assertEqual(len(gsi.key_schema), 1)
        self.assertEqual(gsi.projection["ProjectionType"], "ALL")


class TestLocalSecondaryIndex(unittest.TestCase):
    """Test LocalSecondaryIndex dataclass"""

    def test_local_secondary_index_creation(self):
        lsi = LocalSecondaryIndex(
            index_name="created-index",
            key_schema=[
                KeySchemaElement(attribute_name="user_id", key_type="HASH"),
                KeySchemaElement(attribute_name="created_at", key_type="RANGE")
            ],
            projection={"ProjectionType": "KEYS_ONLY"}
        )
        self.assertEqual(lsi.index_name, "created-index")
        self.assertEqual(len(lsi.key_schema), 2)


class TestTableConfig(unittest.TestCase):
    """Test TableConfig dataclass"""

    def test_table_config_defaults(self):
        config = TableConfig(
            table_name="test-table",
            partition_key="user_id"
        )
        self.assertEqual(config.table_name, "test-table")
        self.assertEqual(config.partition_key, "user_id")
        self.assertEqual(config.partition_key_type, "S")
        self.assertIsNone(config.sort_key)
        self.assertEqual(config.billing_mode, "PROVISIONED")
        self.assertEqual(config.read_capacity, 5)
        self.assertEqual(config.write_capacity, 5)

    def test_table_config_with_sort_key(self):
        config = TableConfig(
            table_name="test-table",
            partition_key="user_id",
            sort_key="created_at",
            sort_key_type="N"
        )
        self.assertEqual(config.sort_key, "created_at")
        self.assertEqual(config.sort_key_type, "N")

    def test_table_config_with_gsi(self):
        gsi = GlobalSecondaryIndex(
            index_name="email-index",
            key_schema=[KeySchemaElement(attribute_name="email", key_type="HASH")],
            projection={"ProjectionType": "ALL"}
        )
        config = TableConfig(
            table_name="test-table",
            partition_key="user_id",
            global_secondary_indexes=[gsi]
        )
        self.assertEqual(len(config.global_secondary_indexes), 1)
        self.assertEqual(config.global_secondary_indexes[0].index_name, "email-index")


class TestItemOperation(unittest.TestCase):
    """Test ItemOperation dataclass"""

    def test_item_operation_put(self):
        op = ItemOperation(
            operation_type="put",
            item={"user_id": "123", "name": "John"}
        )
        self.assertEqual(op.operation_type, "put")
        self.assertEqual(op.item["user_id"], "123")

    def test_item_operation_get(self):
        op = ItemOperation(
            operation_type="get",
            key={"user_id": "123"}
        )
        self.assertEqual(op.operation_type, "get")
        self.assertEqual(op.key["user_id"], "123")

    def test_item_operation_update(self):
        op = ItemOperation(
            operation_type="update",
            key={"user_id": "123"},
            update_expression="SET #name = :name",
            expression_attribute_names={"#name": "name"},
            expression_attribute_values={":name": "John"}
        )
        self.assertEqual(op.operation_type, "update")
        self.assertEqual(op.update_expression, "SET #name = :name")


class TestDynamoDBIntegration(unittest.TestCase):
    """Test DynamoDBIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_dynamodb = MagicMock()
        self.mock_dynamodbstreams = MagicMock()
        self.mock_cloudwatch = MagicMock()

        # Create integration instance with mocked clients
        self.integration = DynamoDBIntegration(region_name='us-east-1')
        self.integration._dynamodb = self.mock_dynamodb
        self.integration._dynamodbstreams = self.mock_dynamodbstreams
        self.integration._cloudwatch = self.mock_cloudwatch

    def test_is_available_with_clients(self):
        """Test is_available property when clients are initialized"""
        self.assertTrue(self.integration.is_available)

    def test_create_table(self):
        """Test creating a DynamoDB table"""
        mock_response = {
            'TableDescription': {
                'TableName': 'test-table',
                'TableStatus': 'ACTIVE',
                'KeySchema': [
                    {'AttributeName': 'user_id', 'KeyType': 'HASH'}
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'user_id', 'AttributeType': 'S'}
                ]
            }
        }
        self.mock_dynamodb.create_table.return_value = mock_response

        config = TableConfig(
            table_name="test-table",
            partition_key="user_id"
        )

        result = self.integration.create_table(config, wait_for_active=False)

        self.assertEqual(result['TableName'], 'test-table')
        self.mock_dynamodb.create_table.assert_called_once()

    def test_create_table_with_sort_key(self):
        """Test creating a table with sort key"""
        mock_response = {
            'TableDescription': {
                'TableName': 'test-table',
                'TableStatus': 'ACTIVE',
                'KeySchema': [
                    {'AttributeName': 'user_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'created_at', 'KeyType': 'RANGE'}
                ]
            }
        }
        self.mock_dynamodb.create_table.return_value = mock_response

        config = TableConfig(
            table_name="test-table",
            partition_key="user_id",
            sort_key="created_at",
            sort_key_type="N"
        )

        result = self.integration.create_table(config, wait_for_active=False)

        call_args = self.mock_dynamodb.create_table.call_args
        key_schema = call_args.kwargs['KeySchema']
        self.assertEqual(len(key_schema), 2)

    def test_create_table_with_gsi(self):
        """Test creating a table with global secondary index"""
        mock_response = {
            'TableDescription': {
                'TableName': 'test-table',
                'TableStatus': 'ACTIVE',
                'KeySchema': [
                    {'AttributeName': 'user_id', 'KeyType': 'HASH'}
                ],
                'GlobalSecondaryIndexes': [
                    {
                        'IndexName': 'email-index',
                        'KeySchema': [{'AttributeName': 'email', 'KeyType': 'HASH'}],
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                ]
            }
        }
        self.mock_dynamodb.create_table.return_value = mock_response

        gsi = GlobalSecondaryIndex(
            index_name="email-index",
            key_schema=[KeySchemaElement(attribute_name="email", key_type="HASH")],
            projection={"ProjectionType": "ALL"}
        )
        config = TableConfig(
            table_name="test-table",
            partition_key="user_id",
            global_secondary_indexes=[gsi]
        )

        result = self.integration.create_table(config, wait_for_active=False)

        call_args = self.mock_dynamodb.create_table.call_args
        self.assertIn('GlobalSecondaryIndexes', call_args.kwargs)

    def test_describe_table(self):
        """Test describing a table"""
        mock_response = {
            'Table': {
                'TableName': 'test-table',
                'TableStatus': 'ACTIVE',
                'ItemCount': 10
            }
        }
        self.mock_dynamodb.describe_table.return_value = mock_response

        result = self.integration.describe_table('test-table')

        self.assertEqual(result['TableName'], 'test-table')
        self.assertEqual(result['ItemCount'], 10)

    def test_list_tables_with_prefix(self):
        """Test listing tables with prefix filter"""
        mock_response = {
            'TableNames': ['dev-table1', 'dev-table2']
        }
        self.mock_dynamodb.list_tables.return_value = mock_response

        result = self.integration.list_tables(prefix='dev-')

        for table in result:
            self.assertTrue(table.startswith('dev-'))

    def test_delete_table(self):
        """Test deleting a table"""
        self.mock_dynamodb.delete_table.return_value = {}

        result = self.integration.delete_table('test-table', wait=False)

        self.assertTrue(result)
        self.mock_dynamodb.delete_table.assert_called_once_with(TableName='test-table')

    def test_update_table_billing_mode(self):
        """Test updating table billing mode"""
        mock_response = {
            'TableDescription': {
                'TableName': 'test-table',
                'TableStatus': 'ACTIVE',
                'BillingMode': 'PAY_PER_REQUEST'
            }
        }
        self.mock_dynamodb.update_table.return_value = mock_response

        result = self.integration.update_table(
            'test-table',
            billing_mode='PAY_PER_REQUEST',
            wait_for_active=False
        )

        call_args = self.mock_dynamodb.update_table.call_args
        self.assertEqual(call_args.kwargs['BillingMode'], 'PAY_PER_REQUEST')

    def test_put_item(self):
        """Test putting an item"""
        mock_response = {
            'Attributes': {
                'user_id': '123',
                'name': 'John',
                'email': 'john@example.com'
            }
        }
        self.mock_dynamodb.put_item.return_value = mock_response

        item = {
            'user_id': '123',
            'name': 'John',
            'email': 'john@example.com'
        }
        result = self.integration.put_item('test-table', item)

        self.assertEqual(result['Attributes']['user_id'], '123')
        self.mock_dynamodb.put_item.assert_called_once()

    def test_put_item_with_condition(self):
        """Test putting an item with condition"""
        mock_response = {'Attributes': {'user_id': '123'}}
        self.mock_dynamodb.put_item.return_value = mock_response

        item = {'user_id': '123', 'name': 'John'}
        result = self.integration.put_item(
            'test-table',
            item,
            condition="attribute_not_exists(user_id)"
        )

        call_args = self.mock_dynamodb.put_item.call_args
        self.assertIn('ConditionExpression', call_args.kwargs)

    def test_update_item(self):
        """Test updating an item"""
        mock_response = {
            'Attributes': {
                'user_id': '123',
                'name': 'John Updated',
                'email': 'john@example.com'
            }
        }
        self.mock_dynamodb.update_item.return_value = mock_response

        result = self.integration.update_item(
            'test-table',
            {'user_id': '123'},
            update_expression="SET #name = :name",
            expression_attribute_names={'#name': 'name'},
            expression_attribute_values={':name': 'John Updated'}
        )

        self.assertEqual(result['Attributes']['name'], 'John Updated')

    def test_delete_item(self):
        """Test deleting an item"""
        mock_response = {
            'Attributes': {
                'user_id': '123',
                'name': 'John'
            }
        }
        self.mock_dynamodb.delete_item.return_value = mock_response

        result = self.integration.delete_item('test-table', {'user_id': '123'})

        self.assertEqual(result['Attributes']['user_id'], '123')

    def test_enable_streams(self):
        """Test enabling streams"""
        mock_response = {
            'StreamSpecification': {
                'StreamEnabled': True,
                'StreamViewType': 'NEW_AND_OLD_IMAGES'
            }
        }
        self.mock_dynamodb.update_table.return_value = {
            'TableDescription': mock_response
        }

        result = self.integration.enable_streams('test-table', 'NEW_AND_OLD_IMAGES')

        call_args = self.mock_dynamodb.update_table.call_args
        self.assertTrue(call_args.kwargs['StreamSpecification']['StreamEnabled'])

    def test_enable_ttl(self):
        """Test enabling TTL"""
        mock_response = {
            'TimeToLiveSpecification': {
                'AttributeName': 'ttl',
                'Enabled': True
            }
        }
        self.mock_dynamodb.update_time_to_live.return_value = mock_response

        result = self.integration.enable_ttl('test-table', 'ttl')

        self.assertTrue(result)
        self.mock_dynamodb.update_time_to_live.assert_called_once()

    def test_wait_for_table_status(self):
        """Test waiting for table status"""
        mock_response = {
            'Table': {
                'TableName': 'test-table',
                'TableStatus': 'ACTIVE'
            }
        }
        self.mock_dynamodb.describe_table.return_value = mock_response

        # Should complete without timeout
        self.integration._wait_for_table_status('test-table', 'ACTIVE', timeout=5)

        self.mock_dynamodb.describe_table.assert_called()

    def test_client_error_handling(self):
        """Test ClientError handling"""
        error_response = {
            'Error': {
                'Code': 'ResourceNotFoundException',
                'Message': 'Table not found'
            }
        }
        mock_error = Exception("Not found")
        mock_error.response = error_response
        self.mock_dynamodb.describe_table.side_effect = mock_error

        with self.assertRaises(Exception):
            self.integration.describe_table('non-existent-table')

    def test_table_not_found_exception(self):
        """Test table not found exception handling"""
        error_response = {
            'Error': {
                'Code': 'ResourceNotFoundException',
                'Message': 'Table not found'
            }
        }
        mock_error = Exception("Not found")
        mock_error.response = error_response
        self.mock_dynamodb.delete_table.side_effect = mock_error

        # Should handle ResourceNotFoundException gracefully
        result = self.integration.delete_table('non-existent-table', wait=False)
        self.assertTrue(result)


class TestDynamoDBIntegrationIntegration(unittest.TestCase):
    """Integration tests for DynamoDBIntegration (with mocked boto3)"""

    @patch('src.workflow_aws_dynamodb.boto3')
    def test_init_with_boto3(self, mock_boto3):
        """Test initialization with boto3"""
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = mock_client

        integration = DynamoDBIntegration(region_name='us-west-2')

        mock_boto3.Session.assert_called_once()
        self.assertEqual(integration.region_name, 'us-west-2')

    @patch('src.workflow_aws_dynamodb.boto3')
    def test_init_with_custom_endpoint(self, mock_boto3):
        """Test initialization with custom endpoint"""
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = mock_client

        integration = DynamoDBIntegration(
            region_name='us-east-1',
            endpoint_url='http://localhost:8000'
        )

        self.assertEqual(integration.endpoint_url, 'http://localhost:8000')


if __name__ == '__main__':
    unittest.main()

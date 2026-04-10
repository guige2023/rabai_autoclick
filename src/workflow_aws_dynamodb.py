"""
AWS DynamoDB Integration Module for Workflow System

Implements a DynamoDBIntegration class with:
1. Table management: Create/manage DynamoDB tables
2. Item operations: Put/get/update/delete items
3. Batch operations: Batch get/write items
4. Query and scan: Query and scan tables
5. Indexes: Manage GSIs and LSIs
6. Streams: DynamoDB Streams integration
7. TTL: Configure TTL on items
8. On-demand: On-demand capacity mode
9. Global tables: Global table replication
10. CloudWatch integration: Monitoring and metrics

Commit: 'feat(aws-dynamodb): add AWS DynamoDB integration with table management, item operations, batch, query/scan, indexes, streams, TTL, on-demand, global tables, CloudWatch'
"""

import uuid
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import hashlib
import base64

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = None
    BotoCoreError = None


logger = logging.getLogger(__name__)


class TableStatus(Enum):
    """DynamoDB table statuses."""
    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    UPDATING = "UPDATING"
    DELETING = "DELETING"
    DELETED = "DELETED"


class CapacityMode(Enum):
    """DynamoDB capacity modes."""
    ON_DEMAND = "PROVISIONED"
    PROVISIONED = "PROVISIONED"
    PAY_PER_REQUEST = "PAY_PER_REQUEST"


class IndexStatus(Enum):
    """DynamoDB index statuses."""
    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    UPDATING = "UPDATING"
    DELETING = "DELETING"
    DELETED = "DELETED"


class StreamStatus(Enum):
    """DynamoDB Streams statuses."""
    ENABLING = "ENABLING"
    ENABLED = "ENABLED"
    DISABLING = "DISABLING"
    DISABLED = "DISABLED"


class StreamViewType(Enum):
    """DynamoDB Streams view types."""
    NEW_IMAGE = "NEW_IMAGE"
    OLD_IMAGE = "OLD_IMAGE"
    NEW_AND_OLD_IMAGES = "NEW_AND_OLD_IMAGES"
    KEYS_ONLY = "KEYS_ONLY"


class KeyType(Enum):
    """Key types for indexes."""
    HASH = "HASH"
    RANGE = "RANGE"


class ProjectionType(Enum):
    """Projection types for indexes."""
    ALL = "ALL"
    KEYS_ONLY = "KEYS_ONLY"
    INCLUDE = "INCLUDE"


@dataclass
class AttributeDefinition:
    """Attribute definition for table schema."""
    attribute_name: str
    attribute_type: str  # 'S' (String), 'N' (Number), 'B' (Binary)


@dataclass
class KeySchemaElement:
    """Key schema element."""
    attribute_name: str
    key_type: str  # 'HASH' or 'RANGE'


@dataclass
class GlobalSecondaryIndex:
    """Global Secondary Index definition."""
    index_name: str
    key_schema: List[KeySchemaElement]
    projection: Dict[str, Any]
    provisioned_throughput: Optional[Dict[str, Any]] = None
    non_key_attributes: Optional[List[str]] = None


@dataclass
class LocalSecondaryIndex:
    """Local Secondary Index definition."""
    index_name: str
    key_schema: List[KeySchemaElement]
    projection: Dict[str, Any]
    non_key_attributes: Optional[List[str]] = None


@dataclass
class TableConfig:
    """DynamoDB table configuration."""
    table_name: str
    partition_key: str
    partition_key_type: str = 'S'
    sort_key: Optional[str] = None
    sort_key_type: Optional[str] = None
    global_secondary_indexes: List[GlobalSecondaryIndex] = field(default_factory=list)
    local_secondary_indexes: List[LocalSecondaryIndex] = field(default_factory=list)
    stream_specification: Optional[Dict[str, Any]] = None
    ttl_attribute: Optional[str] = None
    billing_mode: str = 'PROVISIONED'
    read_capacity: int = 5
    write_capacity: int = 5
    point_in_time_recovery: bool = False
    server_side_encryption: bool = False
    kms_key_id: Optional[str] = None


@dataclass
class ItemOperation:
    """Represents a single item operation."""
    operation_type: str  # 'put', 'get', 'update', 'delete'
    item: Optional[Dict[str, Any]] = None
    key: Optional[Dict[str, Any]] = None
    condition: Optional[str] = None
    update_expression: Optional[str] = None
    expression_attribute_names: Optional[Dict[str, str]] = None
    expression_attribute_values: Optional[Dict[str, Any]] = None


class DynamoDBIntegration:
    """
    AWS DynamoDB Integration class providing comprehensive DynamoDB management.

    Features:
    1. Table management: Create/manage DynamoDB tables
    2. Item operations: Put/get/update/delete items
    3. Batch operations: Batch get/write items
    4. Query and scan: Query and scan tables
    5. Indexes: Manage GSIs and LSIs
    6. Streams: DynamoDB Streams integration
    7. TTL: Configure TTL on items
    8. On-demand: On-demand capacity mode
    9. Global tables: Global table replication
    10. CloudWatch integration: Monitoring and metrics
    """

    def __init__(
        self,
        region_name: str = 'us-east-1',
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        dynamodb_table_config: Optional[TableConfig] = None
    ):
        """
        Initialize DynamoDB integration.

        Args:
            region_name: AWS region name
            aws_access_key_id: AWS access key ID (uses default credentials if None)
            aws_secret_access_key: AWS secret access key (uses default credentials if None)
            endpoint_url: Custom endpoint URL (for local DynamoDB or custom endpoints)
            dynamodb_table_config: Default table configuration
        """
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        self.dynamodb_table_config = dynamodb_table_config

        self._dynamodb = None
        self._dynamodbstreams = None
        self._cloudwatch = None
        self._tables = {}
        self._table_locks = defaultdict(threading.RLock)
        self._streams_enabled = set()
        self._ttl_configured = set()

        if BOTO3_AVAILABLE:
            self._initialize_clients()

    def _initialize_clients(self):
        """Initialize boto3 clients."""
        try:
            session_kwargs = {
                'region_name': self.region_name
            }
            if self.aws_access_key_id:
                session_kwargs['aws_access_key_id'] = self.aws_access_key_id
            if self.aws_secret_access_key:
                session_kwargs['aws_secret_access_key'] = self.aws_secret_access_key

            session = boto3.Session(**session_kwargs)

            client_kwargs = {
                'region_name': self.region_name
            }
            if self.endpoint_url:
                client_kwargs['endpoint_url'] = self.endpoint_url

            self._dynamodb = session.client('dynamodb', **client_kwargs)
            self._dynamodbstreams = session.client('dynamodbstreams', **client_kwargs)
            self._cloudwatch = session.client('cloudwatch', **session_kwargs)

            logger.info(f"DynamoDB clients initialized for region {self.region_name}")
        except Exception as e:
            logger.error(f"Failed to initialize DynamoDB clients: {e}")

    @property
    def is_available(self) -> bool:
        """Check if boto3 is available and clients are initialized."""
        return BOTO3_AVAILABLE and self._dynamodb is not None

    # =========================================================================
    # Table Management
    # =========================================================================

    def create_table(
        self,
        table_config: TableConfig,
        wait_for_active: bool = True,
        timeout: int = 300
    ) -> Dict[str, Any]:
        """
        Create a DynamoDB table.

        Args:
            table_config: Table configuration
            wait_for_active: Wait for table to become active
            timeout: Timeout in seconds for waiting

        Returns:
            Table description dict
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized. boto3 may not be available.")

        with self._table_locks[table_config.table_name]:
            try:
                attribute_definitions = [
                    {'AttributeName': table_config.partition_key, 'AttributeType': table_config.partition_key_type}
                ]

                key_schema = [
                    {'AttributeName': table_config.partition_key, 'KeyType': 'HASH'}
                ]

                if table_config.sort_key:
                    attribute_definitions.append(
                        {'AttributeName': table_config.sort_key, 'AttributeType': table_config.sort_key_type or 'S'}
                    )
                    key_schema.append({'AttributeName': table_config.sort_key, 'KeyType': 'RANGE'})

                for gsi in table_config.global_secondary_indexes:
                    for key in gsi.key_schema:
                        attr_type = 'S'
                        for attr in attribute_definitions:
                            if attr['AttributeName'] == key.attribute_name:
                                attr_type = attr.get('AttributeType', 'S')
                                break
                        if not any(a['AttributeName'] == key.attribute_name for a in attribute_definitions):
                            attribute_definitions.append({
                                'AttributeName': key.attribute_name,
                                'AttributeType': attr_type
                            })

                for lsi in table_config.local_secondary_indexes:
                    for key in lsi.key_schema:
                        if key.key_type == 'RANGE':
                            attr_type = 'S'
                            for attr in attribute_definitions:
                                if attr['AttributeName'] == key.attribute_name:
                                    attr_type = attr.get('AttributeType', 'S')
                                    break
                            if not any(a['AttributeName'] == key.attribute_name for a in attribute_definitions):
                                attribute_definitions.append({
                                    'AttributeName': key.attribute_name,
                                    'AttributeType': attr_type
                                })

                table_params = {
                    'TableName': table_config.table_name,
                    'KeySchema': key_schema,
                    'AttributeDefinitions': attribute_definitions,
                    'BillingMode': table_config.billing_mode
                }

                if table_config.billing_mode == 'PROVISIONED':
                    table_params['ProvisionedThroughput'] = {
                        'ReadCapacityUnits': table_config.read_capacity,
                        'WriteCapacityUnits': table_config.write_capacity
                    }

                if table_config.global_secondary_indexes:
                    gsis = []
                    for gsi in table_config.global_secondary_indexes:
                        gsi_spec = {
                            'IndexName': gsi.index_name,
                            'KeySchema': [{'AttributeName': k.attribute_name, 'KeyType': k.key_type} for k in gsi.key_schema],
                            'Projection': gsi.projection
                        }
                        if gsi.provisioned_throughput:
                            gsi_spec['ProvisionedThroughput'] = gsi.provisioned_throughput
                        if gsi.non_key_attributes:
                            gsi_spec['NonKeyAttributes'] = gsi.non_key_attributes
                        gsis.append(gsi_spec)
                    table_params['GlobalSecondaryIndexes'] = gsis

                if table_config.local_secondary_indexes:
                    lsis = []
                    for lsi in table_config.local_secondary_indexes:
                        lsi_spec = {
                            'IndexName': lsi.index_name,
                            'KeySchema': [{'AttributeName': k.attribute_name, 'KeyType': k.key_type} for k in lsi.key_schema],
                            'Projection': lsi.projection
                        }
                        if lsi.non_key_attributes:
                            lsi_spec['NonKeyAttributes'] = lsi.non_key_attributes
                        lsis.append(lsi_spec)
                    table_params['LocalSecondaryIndexes'] = lsis

                if table_config.stream_specification:
                    table_params['StreamSpecification'] = table_config.stream_specification

                if table_config.point_in_time_recovery:
                    table_params['PointInTimeRecoverySpecification'] = {'PointInTimeRecoveryEnabled': True}

                if table_config.server_side_encryption:
                    sse_spec = {'Enabled': True}
                    if table_config.kms_key_id:
                        sse_spec['KMSMasterKeyId'] = table_config.kms_key_id
                    table_params['SSESpecification'] = sse_spec

                response = self._dynamodb.create_table(**table_params)
                self._tables[table_config.table_name] = response['TableDescription']

                if wait_for_active:
                    self._wait_for_table_status(table_config.table_name, 'ACTIVE', timeout)

                logger.info(f"Created DynamoDB table: {table_config.table_name}")
                return self._tables[table_config.table_name]

            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code == 'ResourceInUseException':
                    logger.info(f"Table {table_config.table_name} already exists")
                    return self.describe_table(table_config.table_name)
                logger.error(f"Failed to create table {table_config.table_name}: {e}")
                raise

    def _wait_for_table_status(
        self,
        table_name: str,
        target_status: str,
        timeout: int = 300
    ):
        """Wait for table to reach target status."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = self._dynamodb.describe_table(TableName=table_name)
                status = response['Table']['TableStatus']
                if status == target_status:
                    return
                if status in ['DELETING', 'DELETED']:
                    raise RuntimeError(f"Table {table_name} was deleted while waiting")
                time.sleep(2)
            except ClientError as e:
                if e.response.get('Error', {}).get('Code') == 'ResourceNotFoundException':
                    time.sleep(2)
                    continue
                raise
        raise TimeoutError(f"Timeout waiting for table {table_name} to reach status {target_status}")

    def describe_table(self, table_name: str) -> Dict[str, Any]:
        """
        Get table description.

        Args:
            table_name: Name of the table

        Returns:
            Table description dict
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            response = self._dynamodb.describe_table(TableName=table_name)
            self._tables[table_name] = response['Table']
            return response['Table']
        except ClientError as e:
            logger.error(f"Failed to describe table {table_name}: {e}")
            raise

    def list_tables(
        self,
        prefix: Optional[str] = None,
        limit: int = 100
    ) -> List[str]:
        """
        List DynamoDB tables.

        Args:
            prefix: Filter tables by prefix
            limit: Maximum number of tables to return

        Returns:
            List of table names
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            tables = []
            last_evaluated_table_name = None

            while len(tables) < limit:
                kwargs = {'Limit': limit}
                if last_evaluated_table_name:
                    kwargs['ExclusiveStartTableName'] = last_evaluated_table_name

                response = self._dynamodb.list_tables(**kwargs)
                table_names = response.get('TableNames', [])

                if prefix:
                    table_names = [t for t in table_names if t.startswith(prefix)]

                tables.extend(table_names)

                if len(tables) >= limit:
                    break

                last_evaluated_table_name = response.get('LastEvaluatedTableName')
                if not last_evaluated_table_name:
                    break

            return tables[:limit]

        except ClientError as e:
            logger.error(f"Failed to list tables: {e}")
            raise

    def delete_table(self, table_name: str, wait: bool = True) -> bool:
        """
        Delete a DynamoDB table.

        Args:
            table_name: Name of the table to delete
            wait: Wait for deletion to complete

        Returns:
            True if deleted successfully
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        with self._table_locks[table_name]:
            try:
                self._dynamodb.delete_table(TableName=table_name)
                logger.info(f"Deleted DynamoDB table: {table_name}")

                if table_name in self._tables:
                    del self._tables[table_name]

                if wait:
                    self._wait_for_table_deletion(table_name)

                return True

            except ClientError as e:
                if e.response.get('Error', {}).get('Code') == 'ResourceNotFoundException':
                    logger.info(f"Table {table_name} does not exist")
                    return True
                logger.error(f"Failed to delete table {table_name}: {e}")
                raise

    def _wait_for_table_deletion(self, table_name: str, timeout: int = 300):
        """Wait for table to be deleted."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                self._dynamodb.describe_table(TableName=table_name)
                time.sleep(2)
            except ClientError as e:
                if e.response.get('Error', {}).get('Code') == 'ResourceNotFoundException':
                    return
                raise
        raise TimeoutError(f"Timeout waiting for table {table_name} to be deleted")

    def update_table(
        self,
        table_name: str,
        billing_mode: Optional[str] = None,
        read_capacity: Optional[int] = None,
        write_capacity: Optional[int] = None,
        stream_specification: Optional[Dict[str, Any]] = None,
        wait_for_active: bool = True
    ) -> Dict[str, Any]:
        """
        Update table configuration.

        Args:
            table_name: Name of the table
            billing_mode: 'PROVISIONED' or 'PAY_PER_REQUEST'
            read_capacity: New read capacity (for provisioned mode)
            write_capacity: New write capacity (for provisioned mode)
            stream_specification: Stream specification
            wait_for_active: Wait for table to become active

        Returns:
            Updated table description
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        with self._table_locks[table_name]:
            try:
                update_params = {'TableName': table_name}

                if billing_mode:
                    update_params['BillingMode'] = billing_mode

                if billing_mode == 'PROVISIONED' and (read_capacity or write_capacity):
                    provisioned_throughput = {}
                    if read_capacity:
                        provisioned_throughput['ReadCapacityUnits'] = read_capacity
                    if write_capacity:
                        provisioned_throughput['WriteCapacityUnits'] = write_capacity
                    update_params['ProvisionedThroughput'] = provisioned_throughput

                if stream_specification is not None:
                    update_params['StreamSpecification'] = stream_specification

                response = self._dynamodb.update_table(**update_params)
                self._tables[table_name] = response['TableDescription']

                if wait_for_active:
                    self._wait_for_table_status(table_name, 'ACTIVE')

                logger.info(f"Updated DynamoDB table: {table_name}")
                return self._tables[table_name]

            except ClientError as e:
                logger.error(f"Failed to update table {table_name}: {e}")
                raise

    # =========================================================================
    # Item Operations
    # =========================================================================

    def put_item(
        self,
        table_name: str,
        item: Dict[str, Any],
        condition: Optional[str] = None,
        expression_attribute_names: Optional[Dict[str, str]] = None,
        expression_attribute_values: Optional[Dict[str, Any]] = None,
        return_values: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Put an item into a table.

        Args:
            table_name: Name of the table
            item: Item to put
            condition: Condition expression
            expression_attribute_names: Expression attribute names
            expression_attribute_values: Expression attribute values
            return_values: 'NONE', 'ALL_OLD', 'UPDATED_OLD', 'ALL_NEW', 'UPDATED_NEW'

        Returns:
            Response dict
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            params = {
                'TableName': table_name,
                'Item': self._serialize_item(item)
            }

            if condition:
                params['ConditionExpression'] = condition
            if expression_attribute_names:
                params['ExpressionAttributeNames'] = expression_attribute_names
            if expression_attribute_values:
                params['ExpressionAttributeValues'] = self._serialize_item(expression_attribute_values)
            if return_values:
                params['ReturnValues'] = return_values

            response = self._dynamodb.put_item(**params)
            logger.debug(f"Put item to {table_name}")
            return response

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ConditionalCheckFailedException':
                logger.warning(f"Condition check failed for item in {table_name}")
                return {'ConsumedCapacity': None, 'ItemCollectionMetrics': None}
            logger.error(f"Failed to put item to {table_name}: {e}")
            raise

    def get_item(
        self,
        table_name: str,
        key: Dict[str, Any],
        consistent_read: bool = False,
        projection_expression: Optional[str] = None,
        expression_attribute_names: Optional[Dict[str, str]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get an item from a table.

        Args:
            table_name: Name of the table
            key: Primary key of the item
            consistent_read: Use strongly consistent read
            projection_expression: Projection expression
            expression_attribute_names: Expression attribute names

        Returns:
            Item dict or None if not found
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            params = {
                'TableName': table_name,
                'Key': self._serialize_item(key)
            }

            if consistent_read:
                params['ConsistentRead'] = True
            if projection_expression:
                params['ProjectionExpression'] = projection_expression
            if expression_attribute_names:
                params['ExpressionAttributeNames'] = expression_attribute_names

            response = self._dynamodb.get_item(**params)
            item = response.get('Item')

            if item:
                return self._deserialize_item(item)
            return None

        except ClientError as e:
            logger.error(f"Failed to get item from {table_name}: {e}")
            raise

    def update_item(
        self,
        table_name: str,
        key: Dict[str, Any],
        update_expression: str,
        condition: Optional[str] = None,
        expression_attribute_names: Optional[Dict[str, str]] = None,
        expression_attribute_values: Optional[Dict[str, Any]] = None,
        return_values: Optional[str] = 'ALL_NEW'
    ) -> Dict[str, Any]:
        """
        Update an item in a table.

        Args:
            table_name: Name of the table
            key: Primary key of the item
            update_expression: Update expression (e.g., 'SET #attr = :val')
            condition: Condition expression
            expression_attribute_names: Expression attribute names for reserved words
            expression_attribute_values: Expression attribute values
            return_values: 'NONE', 'ALL_OLD', 'UPDATED_OLD', 'ALL_NEW', 'UPDATED_NEW'

        Returns:
            Response dict with updated item if return_values is set
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            params = {
                'TableName': table_name,
                'Key': self._serialize_item(key),
                'UpdateExpression': update_expression
            }

            if condition:
                params['ConditionExpression'] = condition
            if expression_attribute_names:
                params['ExpressionAttributeNames'] = expression_attribute_names
            if expression_attribute_values:
                params['ExpressionAttributeValues'] = self._serialize_item(expression_attribute_values)
            if return_values:
                params['ReturnValues'] = return_values

            response = self._dynamodb.update_item(**params)

            if 'Attributes' in response:
                response['Attributes'] = self._deserialize_item(response['Attributes'])

            return response

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ConditionalCheckFailedException':
                logger.warning(f"Condition check failed for update on {table_name}")
                return {'Attributes': None}
            logger.error(f"Failed to update item in {table_name}: {e}")
            raise

    def delete_item(
        self,
        table_name: str,
        key: Dict[str, Any],
        condition: Optional[str] = None,
        expression_attribute_names: Optional[Dict[str, str]] = None,
        expression_attribute_values: Optional[Dict[str, Any]] = None,
        return_values: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete an item from a table.

        Args:
            table_name: Name of the table
            key: Primary key of the item
            condition: Condition expression
            expression_attribute_names: Expression attribute names
            expression_attribute_values: Expression attribute values
            return_values: 'NONE', 'ALL_OLD', 'UPDATED_OLD', 'ALL_NEW', 'UPDATED_NEW'

        Returns:
            Response dict
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            params = {
                'TableName': table_name,
                'Key': self._serialize_item(key)
            }

            if condition:
                params['ConditionExpression'] = condition
            if expression_attribute_names:
                params['ExpressionAttributeNames'] = expression_attribute_names
            if expression_attribute_values:
                params['ExpressionAttributeValues'] = self._serialize_item(expression_attribute_values)
            if return_values:
                params['ReturnValues'] = return_values

            response = self._dynamodb.delete_item(**params)
            return response

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ConditionalCheckFailedException':
                logger.warning(f"Condition check failed for delete on {table_name}")
                return {}
            logger.error(f"Failed to delete item from {table_name}: {e}")
            raise

    # =========================================================================
    # Batch Operations
    # =========================================================================

    def batch_write_items(
        self,
        table_name: str,
        items: List[Dict[str, Any]],
        operation: str = 'put',
        chunk_size: int = 25
    ) -> Dict[str, Any]:
        """
        Batch write items to a table.

        Args:
            table_name: Name of the table
            items: List of items to write
            operation: 'put' or 'delete'
            chunk_size: Number of items per batch (max 25)

        Returns:
            Summary of unprocessed items
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        unprocessed_items = []
        total_consumed = 0

        for i in range(0, len(items), chunk_size):
            chunk = items[i:i + chunk_size]

            request_items = {
                table_name: [
                    {
                        operation.capitalize(): {
                            'Item' if operation == 'put' else 'Key': self._serialize_item(
                                item if operation == 'put' else item
                            )
                        }
                    }
                    for item in chunk
                ]
            }

            try:
                response = self._dynamodb.batch_write_item(RequestItems=request_items)

                consumed = response.get('ConsumedCapacity', [])
                if consumed:
                    total_consumed += sum(c.get('CapacityUnits', 0) for c in consumed if c)

                unprocessed = response.get('UnprocessedItems', {})
                if unprocessed.get(table_name):
                    unprocessed_items.extend(
                        u.get('Item', u.get('Key')) for u in unprocessed[table_name]
                    )

            except ClientError as e:
                logger.error(f"Batch write failed: {e}")
                unprocessed_items.extend(chunk)
                break

        result = {
            'total_processed': len(items) - len(unprocessed_items),
            'unprocessed_count': len(unprocessed_items),
            'unprocessed_items': unprocessed_items
        }

        logger.info(f"Batch write to {table_name}: {result['total_processed']} processed, "
                    f"{result['unprocessed_count']} unprocessed")

        return result

    def batch_get_items(
        self,
        table_name: str,
        keys: List[Dict[str, Any]],
        consistent_read: bool = False,
        chunk_size: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Batch get items from a table.

        Args:
            table_name: Name of the table
            keys: List of primary keys
            consistent_read: Use strongly consistent read
            chunk_size: Number of keys per batch (max 100)

        Returns:
            List of retrieved items
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        all_items = []
        unprocessed_keys = keys[:]

        while unprocessed_keys:
            batch = unprocessed_keys[:chunk_size]
            unprocessed_keys = unprocessed_keys[chunk_size:]

            request_items = {
                table_name: {
                    'Keys': [self._serialize_item(key) for key in batch]
                }
            }

            if consistent_read:
                request_items[table_name]['ConsistentRead'] = True

            try:
                response = self._dynamodb.batch_get_item(RequestItems=request_items)

                items = response.get('Responses', {}).get(table_name, [])
                all_items.extend([self._deserialize_item(item) for item in items])

                unprocessed = response.get('UnprocessedRequests', {})
                if unprocessed.get(table_name):
                    unprocessed_keys.extend(
                        u['Key'] for u in unprocessed[table_name].get('Keys', [])
                    )

            except ClientError as e:
                logger.error(f"Batch get failed: {e}")
                unprocessed_keys.extend(batch)
                break

        logger.info(f"Batch get from {table_name}: retrieved {len(all_items)} items")
        return all_items

    # =========================================================================
    # Query and Scan
    # =========================================================================

    def query(
        self,
        table_name: str,
        key_condition: Optional[str] = None,
        filter_expression: Optional[str] = None,
        expression_attribute_names: Optional[Dict[str, str]] = None,
        expression_attribute_values: Optional[Dict[str, Any]] = None,
        index_name: Optional[str] = None,
        consistent_read: bool = False,
        scan_forward: bool = True,
        limit: Optional[int] = None,
        exclusive_start_key: Optional[Dict[str, Any]] = None,
        select: str = 'ALL_ATTRIBUTES'
    ) -> Dict[str, Any]:
        """
        Query a table or index.

        Args:
            table_name: Name of the table
            key_condition: Key condition expression
            filter_expression: Filter expression
            expression_attribute_names: Expression attribute names
            expression_attribute_values: Expression attribute values
            index_name: Name of index to query
            consistent_read: Use strongly consistent read
            scan_forward: Scan forward (ascending) or backward
            limit: Maximum number of items to return
            exclusive_start_key: Key to start from
            select: 'ALL_ATTRIBUTES', 'ALL_PROJECTED_ATTRIBUTES', 'SPECIFIC_ATTRIBUTES', 'COUNT'

        Returns:
            Query results with items and metadata
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            params = {
                'TableName': table_name,
                'ScanIndexForward': scan_forward,
                'Select': select
            }

            if key_condition:
                params['KeyConditionExpression'] = key_condition
            if filter_expression:
                params['FilterExpression'] = filter_expression
            if expression_attribute_names:
                params['ExpressionAttributeNames'] = expression_attribute_names
            if expression_attribute_values:
                params['ExpressionAttributeValues'] = self._serialize_item(expression_attribute_values)
            if index_name:
                params['IndexName'] = index_name
            if consistent_read:
                params['ConsistentRead'] = True
            if limit:
                params['Limit'] = limit
            if exclusive_start_key:
                params['ExclusiveStartKey'] = self._serialize_item(exclusive_start_key)

            response = self._dynamodb.query(**params)

            items = [self._deserialize_item(item) for item in response.get('Items', [])]

            result = {
                'items': items,
                'count': response.get('Count', 0),
                'scanned_count': response.get('ScannedCount', 0)
            }

            if 'LastEvaluatedKey' in response:
                result['last_evaluated_key'] = self._deserialize_item(response['LastEvaluatedKey'])

            if 'ConsumedCapacity' in response:
                result['consumed_capacity'] = response['ConsumedCapacity']

            return result

        except ClientError as e:
            logger.error(f"Query failed for {table_name}: {e}")
            raise

    def scan(
        self,
        table_name: str,
        filter_expression: Optional[str] = None,
        expression_attribute_names: Optional[Dict[str, str]] = None,
        expression_attribute_values: Optional[Dict[str, Any]] = None,
        index_name: Optional[str] = None,
        segment: Optional[int] = None,
        total_segments: Optional[int] = None,
        limit: Optional[int] = None,
        exclusive_start_key: Optional[Dict[str, Any]] = None,
        select: str = 'ALL_ATTRIBUTES'
    ) -> Dict[str, Any]:
        """
        Scan a table or index.

        Args:
            table_name: Name of the table
            filter_expression: Filter expression
            expression_attribute_names: Expression attribute names
            expression_attribute_values: Expression attribute values
            index_name: Name of index to scan
            segment: Segment number for parallel scan
            total_segments: Total segments for parallel scan
            limit: Maximum number of items to return per page
            exclusive_start_key: Key to start from
            select: 'ALL_ATTRIBUTES', 'ALL_PROJECTED_ATTRIBUTES', 'SPECIFIC_ATTRIBUTES', 'COUNT'

        Returns:
            Scan results with items and metadata
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            params = {
                'TableName': table_name,
                'Select': select
            }

            if filter_expression:
                params['FilterExpression'] = filter_expression
            if expression_attribute_names:
                params['ExpressionAttributeNames'] = expression_attribute_names
            if expression_attribute_values:
                params['ExpressionAttributeValues'] = self._serialize_item(expression_attribute_values)
            if index_name:
                params['IndexName'] = index_name
            if segment is not None:
                params['Segment'] = segment
            if total_segments is not None:
                params['TotalSegments'] = total_segments
            if limit:
                params['Limit'] = limit
            if exclusive_start_key:
                params['ExclusiveStartKey'] = self._serialize_item(exclusive_start_key)

            response = self._dynamodb.scan(**params)

            items = [self._deserialize_item(item) for item in response.get('Items', [])]

            result = {
                'items': items,
                'count': response.get('Count', 0),
                'scanned_count': response.get('ScannedCount', 0)
            }

            if 'LastEvaluatedKey' in response:
                result['last_evaluated_key'] = self._deserialize_item(response['LastEvaluatedKey'])

            if 'ConsumedCapacity' in response:
                result['consumed_capacity'] = response['ConsumedCapacity']

            return result

        except ClientError as e:
            logger.error(f"Scan failed for {table_name}: {e}")
            raise

    def query_all(
        self,
        table_name: str,
        key_condition: Optional[str] = None,
        filter_expression: Optional[str] = None,
        expression_attribute_names: Optional[Dict[str, str]] = None,
        expression_attribute_values: Optional[Dict[str, Any]] = None,
        index_name: Optional[str] = None,
        consistent_read: bool = False,
        scan_forward: bool = True,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Query a table and retrieve all results (handles pagination).

        Args:
            table_name: Name of the table
            key_condition: Key condition expression
            filter_expression: Filter expression
            expression_attribute_names: Expression attribute names
            expression_attribute_values: Expression attribute values
            index_name: Name of index to query
            consistent_read: Use strongly consistent read
            scan_forward: Scan forward (ascending) or backward
            limit: Maximum total items to return

        Returns:
            All matching items
        """
        all_items = []
        last_evaluated_key = None
        total_limit = limit

        while True:
            query_params = {
                'table_name': table_name,
                'key_condition': key_condition,
                'filter_expression': filter_expression,
                'expression_attribute_names': expression_attribute_names,
                'expression_attribute_values': expression_attribute_values,
                'index_name': index_name,
                'consistent_read': consistent_read,
                'scan_forward': scan_forward
            }

            if total_limit:
                query_params['limit'] = min(limit or 100, total_limit - len(all_items))

            result = self.query(
                **query_params,
                exclusive_start_key=last_evaluated_key
            )

            all_items.extend(result['items'])

            last_evaluated_key = result.get('last_evaluated_key')

            if not last_evaluated_key:
                break

            if total_limit and len(all_items) >= total_limit:
                break

        return all_items

    def scan_all(
        self,
        table_name: str,
        filter_expression: Optional[str] = None,
        expression_attribute_names: Optional[Dict[str, str]] = None,
        expression_attribute_values: Optional[Dict[str, Any]] = None,
        index_name: Optional[str] = None,
        total_segments: int = 1,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Scan a table and retrieve all results (handles pagination).

        Args:
            table_name: Name of the table
            filter_expression: Filter expression
            expression_attribute_names: Expression attribute names
            expression_attribute_values: Expression attribute values
            index_name: Name of index to scan
            total_segments: Number of parallel segments
            limit: Maximum total items to return

        Returns:
            All matching items
        """
        all_items = []
        last_evaluated_key = None
        total_limit = limit

        while True:
            scan_params = {
                'table_name': table_name,
                'filter_expression': filter_expression,
                'expression_attribute_names': expression_attribute_names,
                'expression_attribute_values': expression_attribute_values,
                'index_name': index_name,
                'total_segments': total_segments
            }

            if total_segments == 1:
                if total_limit:
                    scan_params['limit'] = min(limit or 100, total_limit - len(all_items))
                result = self.scan(
                    **scan_params,
                    exclusive_start_key=last_evaluated_key
                )
            else:
                result = self._parallel_scan(scan_params)

            all_items.extend(result['items'])

            if total_segments == 1:
                last_evaluated_key = result.get('last_evaluated_key')
                if not last_evaluated_key:
                    break
            else:
                break

            if total_limit and len(all_items) >= total_limit:
                break

        return all_items

    def _parallel_scan(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute parallel scan across segments."""
        import concurrent.futures

        segments = params.pop('total_segments', 1)
        table_name = params['table_name']

        all_items = []
        count = 0
        scanned_count = 0

        def scan_segment(segment: int):
            return self.scan(table_name=table_name, segment=segment, total_segments=segments, **params)

        with concurrent.futures.ThreadPoolExecutor(max_workers=segments) as executor:
            futures = [executor.submit(scan_segment, i) for i in range(segments)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        for result in results:
            all_items.extend(result['items'])
            count += result.get('count', 0)
            scanned_count += result.get('scanned_count', 0)

        return {
            'items': all_items,
            'count': count,
            'scanned_count': scanned_count
        }

    # =========================================================================
    # Index Management
    # =========================================================================

    def create_gsi(
        self,
        table_name: str,
        index_name: str,
        partition_key: str,
        partition_key_type: str = 'S',
        sort_key: Optional[str] = None,
        sort_key_type: Optional[str] = 'S',
        projection_type: str = 'ALL',
        non_key_attributes: Optional[List[str]] = None,
        read_capacity: int = 5,
        write_capacity: int = 5
    ) -> Dict[str, Any]:
        """
        Create a Global Secondary Index.

        Args:
            table_name: Name of the base table
            index_name: Name of the GSI
            partition_key: Partition key attribute name
            partition_key_type: Partition key type ('S', 'N', 'B')
            sort_key: Sort key attribute name (optional)
            sort_key_type: Sort key type ('S', 'N', 'B')
            projection_type: 'ALL', 'KEYS_ONLY', or 'INCLUDE'
            non_key_attributes: Non-key attributes to include (for INCLUDE projection)
            read_capacity: Read capacity units
            write_capacity: Write capacity units

        Returns:
            Index description
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            key_schema = [
                {'AttributeName': partition_key, 'KeyType': 'HASH'}
            ]

            attribute_definitions = [
                {'AttributeName': partition_key, 'AttributeType': partition_key_type}
            ]

            if sort_key:
                key_schema.append({'AttributeName': sort_key, 'KeyType': 'RANGE'})
                attribute_definitions.append({'AttributeName': sort_key, 'AttributeType': sort_key_type})

            projection = {'ProjectionType': projection_type}
            if projection_type == 'INCLUDE' and non_key_attributes:
                projection['NonKeyAttributes'] = non_key_attributes

            response = self._dynamodb.update_table(
                TableName=table_name,
                AttributeDefinitions=attribute_definitions,
                GlobalSecondaryIndexUpdates=[
                    {
                        'Create': {
                            'IndexName': index_name,
                            'KeySchema': key_schema,
                            'Projection': projection,
                            'ProvisionedThroughput': {
                                'ReadCapacityUnits': read_capacity,
                                'WriteCapacityUnits': write_capacity
                            }
                        }
                    }
                ]
            )

            self._wait_for_gsi_status(table_name, index_name, 'ACTIVE')

            for gsi in response['Table']['GlobalSecondaryIndexes']:
                if gsi['IndexName'] == index_name:
                    logger.info(f"Created GSI {index_name} on table {table_name}")
                    return gsi

            return {}

        except ClientError as e:
            logger.error(f"Failed to create GSI {index_name} on {table_name}: {e}")
            raise

    def update_gsi(
        self,
        table_name: str,
        index_name: str,
        read_capacity: Optional[int] = None,
        write_capacity: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Update a Global Secondary Index.

        Args:
            table_name: Name of the base table
            index_name: Name of the GSI
            read_capacity: New read capacity units
            write_capacity: New write capacity units

        Returns:
            Updated index description
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            provisioned_throughput = {}
            if read_capacity:
                provisioned_throughput['ReadCapacityUnits'] = read_capacity
            if write_capacity:
                provisioned_throughput['WriteCapacityUnits'] = write_capacity

            if not provisioned_throughput:
                raise ValueError("At least one capacity value must be provided")

            response = self._dynamodb.update_table(
                TableName=table_name,
                GlobalSecondaryIndexUpdates=[
                    {
                        'Update': {
                            'IndexName': index_name,
                            'ProvisionedThroughput': provisioned_throughput
                        }
                    }
                ]
            )

            self._wait_for_gsi_status(table_name, index_name, 'ACTIVE')

            for gsi in response['Table']['GlobalSecondaryIndexes']:
                if gsi['IndexName'] == index_name:
                    logger.info(f"Updated GSI {index_name} on table {table_name}")
                    return gsi

            return {}

        except ClientError as e:
            logger.error(f"Failed to update GSI {index_name} on {table_name}: {e}")
            raise

    def delete_gsi(
        self,
        table_name: str,
        index_name: str
    ) -> bool:
        """
        Delete a Global Secondary Index.

        Args:
            table_name: Name of the base table
            index_name: Name of the GSI to delete

        Returns:
            True if deleted successfully
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            self._dynamodb.update_table(
                TableName=table_name,
                GlobalSecondaryIndexUpdates=[
                    {
                        'Delete': {
                            'IndexName': index_name
                        }
                    }
                ]
            )

            logger.info(f"Deleted GSI {index_name} from table {table_name}")
            return True

        except ClientError as e:
            logger.error(f"Failed to delete GSI {index_name} from {table_name}: {e}")
            raise

    def _wait_for_gsi_status(
        self,
        table_name: str,
        index_name: str,
        target_status: str,
        timeout: int = 300
    ):
        """Wait for GSI to reach target status."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = self._dynamodb.describe_table(TableName=table_name)
                for gsi in response['Table'].get('GlobalSecondaryIndexes', []):
                    if gsi['IndexName'] == index_name:
                        if gsi['IndexStatus'] == target_status:
                            return
                        if gsi['IndexStatus'] in ['DELETING', 'DELETED']:
                            raise RuntimeError(f"GSI {index_name} was deleted while waiting")
                        time.sleep(2)
                        break
                else:
                    time.sleep(2)
            except ClientError as e:
                if e.response.get('Error', {}).get('Code') == 'ResourceNotFoundException':
                    time.sleep(2)
                    continue
                raise
        raise TimeoutError(f"Timeout waiting for GSI {index_name} to reach status {target_status}")

    def describe_gsi(
        self,
        table_name: str,
        index_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get GSI description.

        Args:
            table_name: Name of the base table
            index_name: Name of the GSI

        Returns:
            GSI description dict
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            response = self._dynamodb.describe_table(TableName=table_name)
            for gsi in response['Table'].get('GlobalSecondaryIndexes', []):
                if gsi['IndexName'] == index_name:
                    return gsi
            return None

        except ClientError as e:
            logger.error(f"Failed to describe GSI {index_name}: {e}")
            raise

    # =========================================================================
    # DynamoDB Streams
    # =========================================================================

    def enable_streams(
        self,
        table_name: str,
        stream_view_type: str = 'NEW_AND_OLD_IMAGES'
    ) -> str:
        """
        Enable DynamoDB Streams on a table.

        Args:
            table_name: Name of the table
            stream_view_type: 'NEW_IMAGE', 'OLD_IMAGE', 'NEW_AND_OLD_IMAGES', 'KEYS_ONLY'

        Returns:
            Stream ARN
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            response = self._dynamodb.update_table(
                TableName=table_name,
                StreamSpecification={
                    'StreamEnabled': True,
                    'StreamViewType': stream_view_type
                }
            )

            stream_arn = response['TableDescription'].get('LatestStreamArn')
            self._streams_enabled.add(table_name)
            logger.info(f"Enabled streams on table {table_name}")
            return stream_arn

        except ClientError as e:
            logger.error(f"Failed to enable streams on {table_name}: {e}")
            raise

    def disable_streams(self, table_name: str) -> bool:
        """
        Disable DynamoDB Streams on a table.

        Args:
            table_name: Name of the table

        Returns:
            True if disabled successfully
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            self._dynamodb.update_table(
                TableName=table_name,
                StreamSpecification={
                    'StreamEnabled': False
                }
            )

            self._streams_enabled.discard(table_name)
            logger.info(f"Disabled streams on table {table_name}")
            return True

        except ClientError as e:
            logger.error(f"Failed to disable streams on {table_name}: {e}")
            raise

    def get_stream_arn(self, table_name: str) -> Optional[str]:
        """
        Get the stream ARN for a table.

        Args:
            table_name: Name of the table

        Returns:
            Stream ARN or None
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            response = self._dynamodb.describe_table(TableName=table_name)
            return response['Table'].get('LatestStreamArn')

        except ClientError as e:
            logger.error(f"Failed to get stream ARN for {table_name}: {e}")
            raise

    def describe_stream(self, stream_arn: str) -> Dict[str, Any]:
        """
        Describe a DynamoDB stream.

        Args:
            stream_arn: ARN of the stream

        Returns:
            Stream description
        """
        if not self.is_available or not self._dynamodbstreams:
            raise RuntimeError("DynamoDB Streams client not initialized")

        try:
            response = self._dynamodbstreams.describe_stream(StreamArn=stream_arn)
            return response['StreamDescription']

        except ClientError as e:
            logger.error(f"Failed to describe stream {stream_arn}: {e}")
            raise

    def get_shard_iterator(
        self,
        stream_arn: str,
        shard_id: str,
        iterator_type: str = 'LATEST'
    ) -> str:
        """
        Get a shard iterator.

        Args:
            stream_arn: ARN of the stream
            shard_id: Shard ID
            iterator_type: 'TRIM_HORIZON', 'LATEST', 'AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER'

        Returns:
            Shard iterator
        """
        if not self.is_available or not self._dynamodbstreams:
            raise RuntimeError("DynamoDB Streams client not initialized")

        try:
            response = self._dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType=iterator_type
            )
            return response['ShardIterator']

        except ClientError as e:
            logger.error(f"Failed to get shard iterator: {e}")
            raise

    def get_records(
        self,
        shard_iterator: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Get records from a shard.

        Args:
            shard_iterator: Shard iterator
            limit: Maximum number of records to retrieve

        Returns:
            Records response with next iterator
        """
        if not self.is_available or not self._dynamodbstreams:
            raise RuntimeError("DynamoDB Streams client not initialized")

        try:
            response = self._dynamodbstreams.get_records(
                ShardIterator=shard_iterator,
                Limit=limit
            )

            records = []
            for record in response.get('Records', []):
                records.append({
                    'event_id': record.get('eventID'),
                    'event_type': record.get('eventType'),
                    'event_source': record.get('eventSource'),
                    'aws_region': record.get('awsRegion'),
                    'dynamodb': record.get('dynamodb'),
                    'user_identity': record.get('userIdentity')
                })

            return {
                'records': records,
                'next_shard_iterator': response.get('NextShardIterator'),
                'millis_behind_latest': response.get('MillisBehindLatest')
            }

        except ClientError as e:
            logger.error(f"Failed to get records: {e}")
            raise

    def read_stream(
        self,
        stream_arn: str,
        iterator_type: str = 'LATEST',
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Read all available records from a stream.

        Args:
            stream_arn: ARN of the stream
            iterator_type: 'TRIM_HORIZON', 'LATEST', etc.
            limit: Maximum records per shard

        Returns:
            List of all records
        """
        if not self.is_available or not self._dynamodbstreams:
            raise RuntimeError("DynamoDB Streams client not initialized")

        try:
            stream_desc = self.describe_stream(stream_arn)
            all_records = []

            for shard in stream_desc.get('Shards', []):
                shard_id = shard['ShardId']
                iterator = self.get_shard_iterator(stream_arn, shard_id, iterator_type)

                while iterator:
                    result = self.get_records(iterator, limit)
                    all_records.extend(result['records'])
                    iterator = result['next_shard_iterator']

                    if result.get('millis_behind_latest', 0) == 0:
                        break

            return all_records

        except ClientError as e:
            logger.error(f"Failed to read stream {stream_arn}: {e}")
            raise

    # =========================================================================
    # TTL Management
    # =========================================================================

    def enable_ttl(
        self,
        table_name: str,
        ttl_attribute: str = 'ttl'
    ) -> bool:
        """
        Enable TTL on a table.

        Args:
            table_name: Name of the table
            ttl_attribute: Name of the TTL attribute

        Returns:
            True if enabled successfully
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            self._dynamodb.update_time_to_live(
                TableName=table_name,
                TimeToLiveSpecification={
                    'Enabled': True,
                    'AttributeName': ttl_attribute
                }
            )

            self._ttl_configured.add(table_name)
            logger.info(f"Enabled TTL on table {table_name} with attribute {ttl_attribute}")
            return True

        except ClientError as e:
            logger.error(f"Failed to enable TTL on {table_name}: {e}")
            raise

    def disable_ttl(self, table_name: str) -> bool:
        """
        Disable TTL on a table.

        Args:
            table_name: Name of the table

        Returns:
            True if disabled successfully
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            self._dynamodb.update_time_to_live(
                TableName=table_name,
                TimeToLiveSpecification={
                    'Enabled': False,
                    'AttributeName': 'ttl'
                }
            )

            self._ttl_configured.discard(table_name)
            logger.info(f"Disabled TTL on table {table_name}")
            return True

        except ClientError as e:
            logger.error(f"Failed to disable TTL on {table_name}: {e}")
            raise

    def describe_ttl(self, table_name: str) -> Dict[str, Any]:
        """
        Get TTL status for a table.

        Args:
            table_name: Name of the table

        Returns:
            TTL status information
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            response = self._dynamodb.describe_time_to_live(TableName=table_name)
            return response['TimeToLiveDescription']

        except ClientError as e:
            logger.error(f"Failed to describe TTL for {table_name}: {e}")
            raise

    def set_item_ttl(
        self,
        table_name: str,
        key: Dict[str, Any],
        ttl_timestamp: int
    ) -> Dict[str, Any]:
        """
        Set TTL on a specific item.

        Args:
            table_name: Name of the table
            key: Primary key of the item
            ttl_timestamp: Unix timestamp when item should expire

        Returns:
            Update response
        """
        return self.update_item(
            table_name=table_name,
            key=key,
            update_expression='SET #ttl = :ttl',
            expression_attribute_names={'#ttl': 'ttl'},
            expression_attribute_values={':ttl': ttl_timestamp},
            return_values='UPDATED_NEW'
        )

    def remove_item_ttl(
        self,
        table_name: str,
        key: Dict[str, Any],
        ttl_attribute: str = 'ttl'
    ) -> Dict[str, Any]:
        """
        Remove TTL from a specific item.

        Args:
            table_name: Name of the table
            key: Primary key of the item
            ttl_attribute: Name of the TTL attribute

        Returns:
            Update response
        """
        return self.update_item(
            table_name=table_name,
            key=key,
            update_expression='REMOVE #ttl',
            expression_attribute_names={'#ttl': ttl_attribute},
            return_values='UPDATED_NEW'
        )

    # =========================================================================
    # On-Demand and Capacity Mode
    # =========================================================================

    def enable_on_demand(self, table_name: str) -> Dict[str, Any]:
        """
        Switch table to on-demand capacity mode (PAY_PER_REQUEST).

        Args:
            table_name: Name of the table

        Returns:
            Updated table description
        """
        return self.update_table(
            table_name=table_name,
            billing_mode='PAY_PER_REQUEST',
            wait_for_active=True
        )

    def enable_provisioned(
        self,
        table_name: str,
        read_capacity: int = 5,
        write_capacity: int = 5
    ) -> Dict[str, Any]:
        """
        Switch table to provisioned capacity mode.

        Args:
            table_name: Name of the table
            read_capacity: Read capacity units
            write_capacity: Write capacity units

        Returns:
            Updated table description
        """
        return self.update_table(
            table_name=table_name,
            billing_mode='PROVISIONED',
            read_capacity=read_capacity,
            write_capacity=write_capacity,
            wait_for_active=True
        )

    def update_capacity(
        self,
        table_name: str,
        read_capacity: Optional[int] = None,
        write_capacity: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Update provisioned capacity.

        Args:
            table_name: Name of the table
            read_capacity: New read capacity units
            write_capacity: New write capacity units

        Returns:
            Updated table description
        """
        return self.update_table(
            table_name=table_name,
            read_capacity=read_capacity,
            write_capacity=write_capacity,
            wait_for_active=True
        )

    def describe_capacity(self, table_name: str) -> Dict[str, Any]:
        """
        Get current capacity settings.

        Args:
            table_name: Name of the table

        Returns:
            Capacity information
        """
        try:
            response = self._dynamodb.describe_table(TableName=table_name)
            table = response['Table']

            result = {
                'table_name': table_name,
                'table_status': table.get('TableStatus')
            }

            if 'BillingModeSummary' in table:
                result['billing_mode'] = table['BillingModeSummary'].get('BillingMode')
                result['billing_mode_source'] = table['BillingModeSummary'].get('BillingModeSource')

            if 'ProvisionedThroughput' in table:
                result['provisioned'] = {
                    'read_capacity': table['ProvisionedThroughput'].get('ReadCapacityUnits'),
                    'write_capacity': table['ProvisionedThroughput'].get('WriteCapacityUnits'),
                    'last_increased': table['ProvisionedThroughput'].get('LastIncreaseDateTime'),
                    'last_decreased': table['ProvisionedThroughput'].get('LastDecreaseDateTime'),
                    'number_of_decreases': table['ProvisionedThroughput'].get('NumberOfDecreasesToday')
                }

            return result

        except ClientError as e:
            logger.error(f"Failed to describe capacity for {table_name}: {e}")
            raise

    # =========================================================================
    # Global Tables (Multi-Region Replication)
    # =========================================================================

    def create_global_table(
        self,
        table_name: str,
        replica_regions: List[str]
    ) -> Dict[str, Any]:
        """
        Create a global table with replicas.

        Args:
            table_name: Name of the table (must exist in one region)
            replica_regions: List of AWS regions for replicas

        Returns:
            Global table description
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            replicas = [
                {'RegionName': region} for region in replica_regions
            ]

            response = self._dynamodb.create_global_table(
                GlobalTableName=table_name,
                ReplicationGroup=replicas
            )

            logger.info(f"Created global table {table_name} with replicas in {replica_regions}")
            return response['GlobalTableDescription']

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'GlobalTableAlreadyExistsException':
                logger.info(f"Global table {table_name} already exists")
                return self.describe_global_table(table_name)
            logger.error(f"Failed to create global table {table_name}: {e}")
            raise

    def describe_global_table(self, table_name: str) -> Dict[str, Any]:
        """
        Get global table description.

        Args:
            table_name: Name of the global table

        Returns:
            Global table description
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            response = self._dynamodb.describe_global_table(GlobalTableName=table_name)
            return response['GlobalTableDescription']

        except ClientError as e:
            logger.error(f"Failed to describe global table {table_name}: {e}")
            raise

    def add_replica(
        self,
        table_name: str,
        region: str
    ) -> Dict[str, Any]:
        """
        Add a replica to an existing global table.

        Args:
            table_name: Name of the global table
            region: AWS region to add replica in

        Returns:
            Updated global table description
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            response = self._dynamodb.update_global_table(
                GlobalTableName=table_name,
                ReplicaUpdates=[
                    {
                        'Create': {
                            'RegionName': region
                        }
                    }
                ]
            )

            logger.info(f"Added replica in {region} to global table {table_name}")
            return response['GlobalTableDescription']

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ReplicaAlreadyExistsException':
                logger.info(f"Replica already exists in {region}")
                return self.describe_global_table(table_name)
            logger.error(f"Failed to add replica to {table_name}: {e}")
            raise

    def remove_replica(self, table_name: str, region: str) -> Dict[str, Any]:
        """
        Remove a replica from a global table.

        Args:
            table_name: Name of the global table
            region: AWS region to remove replica from

        Returns:
            Updated global table description
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            response = self._dynamodb.update_global_table(
                GlobalTableName=table_name,
                ReplicaUpdates=[
                    {
                        'Delete': {
                            'RegionName': region
                        }
                    }
                ]
            )

            logger.info(f"Removed replica in {region} from global table {table_name}")
            return response['GlobalTableDescription']

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ReplicaNotFoundException':
                logger.info(f"Replica not found in {region}")
                return self.describe_global_table(table_name)
            logger.error(f"Failed to remove replica from {table_name}: {e}")
            raise

    def list_global_tables(
        self,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List global tables.

        Args:
            limit: Maximum number of tables to return

        Returns:
            List of global table info
        """
        if not self.is_available:
            raise RuntimeError("DynamoDB client not initialized")

        try:
            tables = []
            last_evaluated_table_name = None

            while len(tables) < limit:
                kwargs = {'Limit': limit}
                if last_evaluated_table_name:
                    kwargs['ExclusiveStartGlobalTableName'] = last_evaluated_table_name

                response = self._dynamodb.list_global_tables(**kwargs)
                tables.extend(response.get('GlobalTables', []))

                if len(tables) >= limit:
                    break

                last_evaluated_table_name = response.get('LastEvaluatedGlobalTableName')
                if not last_evaluated_table_name:
                    break

            return tables[:limit]

        except ClientError as e:
            logger.error(f"Failed to list global tables: {e}")
            raise

    # =========================================================================
    # CloudWatch Integration
    # =========================================================================

    def get_metric_statistics(
        self,
        table_name: str,
        metric_name: str,
        namespace: str = 'AWS/DynamoDB',
        period: int = 60,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        statistics: List[str] = None,
        unit: str = 'Count'
    ) -> List[Dict[str, Any]]:
        """
        Get CloudWatch metric statistics for a table.

        Args:
            table_name: Name of the table
            metric_name: Name of the metric
            namespace: CloudWatch namespace
            period: Period in seconds
            start_time: Start time (defaults to 1 hour ago)
            end_time: End time (defaults to now)
            statistics: List of statistics ('SampleCount', 'Average', 'Sum', 'Minimum', 'Maximum')
            unit: Unit of the metric

        Returns:
            List of metric data points
        """
        if not self.is_available or not self._cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")

        if statistics is None:
            statistics = ['Average', 'Sum']

        if start_time is None:
            start_time = datetime.utcnow() - timedelta(hours=1)
        if end_time is None:
            end_time = datetime.utcnow()

        try:
            response = self._cloudwatch.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=[
                    {'Name': 'TableName', 'Value': table_name}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=statistics,
                Unit=unit
            )

            return response.get('Datapoints', [])

        except ClientError as e:
            logger.error(f"Failed to get metric statistics: {e}")
            raise

    def get_consumed_capacity(
        self,
        table_name: str,
        period_minutes: int = 60
    ) -> Dict[str, Any]:
        """
        Get consumed read/write capacity metrics.

        Args:
            table_name: Name of the table
            period_minutes: Number of minutes to look back

        Returns:
            Capacity metrics summary
        """
        start_time = datetime.utcnow() - timedelta(minutes=period_minutes)

        read_capacity = self.get_metric_statistics(
            table_name=table_name,
            metric_name='ConsumedCapacity',
            start_time=start_time,
            statistics=['Sum'],
            unit='Count'
        )

        write_capacity = self.get_metric_statistics(
            table_name=table_name,
            metric_name='ConsumedWriteCapacity',
            start_time=start_time,
            statistics=['Sum'],
            unit='Count'
        )

        return {
            'read_capacity_consumed': sum(p.get('Sum', 0) for p in read_capacity),
            'write_capacity_consumed': sum(p.get('Sum', 0) for p in write_capacity),
            'period_minutes': period_minutes
        }

    def get_throttled_requests(
        self,
        table_name: str,
        period_minutes: int = 60
    ) -> Dict[str, Any]:
        """
        Get throttled request metrics.

        Args:
            table_name: Name of the table
            period_minutes: Number of minutes to look back

        Returns:
            Throttled requests summary
        """
        start_time = datetime.utcnow() - timedelta(minutes=period_minutes)

        read_throttle = self.get_metric_statistics(
            table_name=table_name,
            metric_name='ReadThrottleEvents',
            start_time=start_time,
            statistics=['Sum'],
            unit='Count'
        )

        write_throttle = self.get_metric_statistics(
            table_name=table_name,
            metric_name='WriteThrottleEvents',
            start_time=start_time,
            statistics=['Sum'],
            unit='Count'
        )

        return {
            'read_throttled_requests': sum(p.get('Sum', 0) for p in read_throttle),
            'write_throttled_requests': sum(p.get('Sum', 0) for p in write_throttle),
            'total_throttled_requests': sum(p.get('Sum', 0) for p in read_throttle) + sum(p.get('Sum', 0) for p in write_throttle),
            'period_minutes': period_minutes
        }

    def get_user_metrics(
        self,
        table_name: str,
        period_minutes: int = 60
    ) -> Dict[str, Any]:
        """
        Get comprehensive user metrics.

        Args:
            table_name: Name of the table
            period_minutes: Number of minutes to look back

        Returns:
            Comprehensive metrics summary
        """
        start_time = datetime.utcnow() - timedelta(minutes=period_minutes)

        metrics = {}

        metric_names = [
            ('SuccessfulRequestLatency', 'Count'),
            ('SuccessfulRequestCount', 'Count'),
            ('ThrottledRequests', 'Count'),
            ('PendingAsyncCount', 'Count')
        ]

        for metric_name, unit in metric_names:
            try:
                data = self.get_metric_statistics(
                    table_name=table_name,
                    metric_name=metric_name,
                    start_time=start_time,
                    statistics=['Average', 'Maximum', 'Sum'],
                    unit=unit
                )
                if data:
                    metrics[metric_name] = data
            except Exception:
                pass

        return metrics

    def put_metric_alarm(
        self,
        alarm_name: str,
        table_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = 'GreaterThanThreshold',
        evaluation_periods: int = 2,
        period: int = 60,
        statistic: str = 'Average'
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for a DynamoDB metric.

        Args:
            alarm_name: Name of the alarm
            table_name: Name of the table
            metric_name: Name of the metric
            threshold: Threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            statistic: Statistic to use

        Returns:
            Alarm configuration
        """
        if not self.is_available or not self._cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")

        try:
            response = self._cloudwatch.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription=f'DynamoDB {metric_name} alarm for {table_name}',
                Namespace='AWS/DynamoDB',
                MetricName=metric_name,
                Dimensions=[
                    {'Name': 'TableName', 'Value': table_name}
                ],
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                EvaluationPeriods=evaluation_periods,
                Period=period,
                Statistic=statistic
            )

            logger.info(f"Created CloudWatch alarm {alarm_name} for {table_name}")
            return response

        except ClientError as e:
            logger.error(f"Failed to create alarm {alarm_name}: {e}")
            raise

    def list_alarms(
        self,
        table_name: Optional[str] = None,
        state_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List CloudWatch alarms.

        Args:
            table_name: Filter by table name
            state_filter: Filter by alarm state

        Returns:
            List of alarms
        """
        if not self.is_available or not self._cloudwatch:
            raise RuntimeError("CloudWatch client not initialized")

        try:
            kwargs = {}
            if state_filter:
                kwargs['StateValue'] = state_filter

            response = self._cloudwatch.describe_alarms(**kwargs)

            alarms = response.get('MetricAlarms', [])

            if table_name:
                alarms = [
                    a for a in alarms
                    if any(d['Value'] == table_name for d in a.get('Dimensions', []))
                ]

            return alarms

        except ClientError as e:
            logger.error(f"Failed to list alarms: {e}")
            raise

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def _serialize_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize item for DynamoDB (convert types)."""
        serialized = {}
        for key, value in item.items():
            if value is None:
                continue
            elif isinstance(value, str):
                serialized[key] = {'S': value}
            elif isinstance(value, bool):
                serialized[key] = {'BOOL': value}
            elif isinstance(value, (int, float)):
                serialized[key] = {'N': str(value)}
            elif isinstance(value, list):
                if not value:
                    serialized[key] = {'L': []}
                elif isinstance(value[0], dict) and 'S' in value[0]:
                    serialized[key] = {'L': value}
                elif isinstance(value[0], dict) and 'N' in value[0]:
                    serialized[key] = {'L': value}
                else:
                    serialized[key] = {'L': [self._serialize_value(v) for v in value]}
            elif isinstance(value, dict):
                if any(k in value for k in ['S', 'N', 'B', 'BOOL', 'L', 'M', 'NULL']):
                    serialized[key] = value
                else:
                    serialized[key] = {'M': self._serialize_item(value)}
            else:
                serialized[key] = {'S': str(value)}
        return serialized

    def _serialize_value(self, value: Any) -> Dict[str, Any]:
        """Serialize a single value."""
        if value is None:
            return {'NULL': True}
        elif isinstance(value, str):
            return {'S': value}
        elif isinstance(value, bool):
            return {'BOOL': value}
        elif isinstance(value, (int, float)):
            return {'N': str(value)}
        elif isinstance(value, list):
            return {'L': [self._serialize_value(v) for v in value]}
        elif isinstance(value, dict):
            return {'M': self._serialize_item(value)}
        else:
            return {'S': str(value)}

    def _deserialize_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize item from DynamoDB format."""
        deserialized = {}
        for key, value in item.items():
            if isinstance(value, dict):
                if 'S' in value:
                    deserialized[key] = value['S']
                elif 'N' in value:
                    num = value['N']
                    deserialized[key] = int(num) if '.' not in num else float(num)
                elif 'BOOL' in value:
                    deserialized[key] = value['BOOL']
                elif 'NULL' in value:
                    deserialized[key] = None
                elif 'L' in value:
                    deserialized[key] = [self._deserialize_value(v) for v in value['L']]
                elif 'M' in value:
                    deserialized[key] = self._deserialize_item(value['M'])
                else:
                    deserialized[key] = value
            else:
                deserialized[key] = value
        return deserialized

    def _deserialize_value(self, value: Dict[str, Any]) -> Any:
        """Deserialize a single value."""
        if 'S' in value:
            return value['S']
        elif 'N' in value:
            num = value['N']
            return int(num) if '.' not in num else float(num)
        elif 'BOOL' in value:
            return value['BOOL']
        elif 'NULL' in value:
            return None
        elif 'L' in value:
            return [self._deserialize_value(v) for v in value['L']]
        elif 'M' in value:
            return self._deserialize_item(value['M'])
        return value

    def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on DynamoDB connection.

        Returns:
            Health status dict
        """
        status = {
            'service': 'dynamodb',
            'available': False,
            'region': self.region_name,
            'tables': []
        }

        if not BOTO3_AVAILABLE:
            status['error'] = 'boto3 not available'
            return status

        if not self._dynamodb:
            status['error'] = 'DynamoDB client not initialized'
            return status

        try:
            response = self._dynamodb.list_tables(Limit=1)
            status['available'] = True
            status['tables'] = self.list_tables(limit=10)
            status['table_count'] = len(status['tables'])
        except Exception as e:
            status['error'] = str(e)

        return status

    def close(self):
        """Close all client connections."""
        self._dynamodb = None
        self._dynamodbstreams = None
        self._cloudwatch = None
        self._tables.clear()
        logger.info("DynamoDB integration closed")

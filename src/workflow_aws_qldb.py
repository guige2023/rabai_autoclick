"""
AWS QLDB (Quantum Ledger Database) Integration Module for Workflow System

Implements a QLDBIntegration class with:
1. Ledger management: Create/manage ledgers
2. Table management: Create/manage tables
3. Document operations: Insert/update/delete documents
4. Query: Query using PartiQL
5. Journal exports: Export journal to S3
6. Backups: Create and manage backups
7. Point-in-time recovery: PITR for ledgers
8. Encryption: KMS encryption
9. Permissions: IAM permissions management
10. CloudWatch integration: Storage and I/O metrics

Commit: 'feat(aws-qldb): add Amazon QLDB with ledger management, table management, document operations, PartiQL query, journal exports, backups, PITR, encryption, CloudWatch'
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


class LedgerStatus(Enum):
    """QLDB ledger statuses."""
    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    DELETED = "DELETED"
    DELETING = "DELETING"


class TableStatus(Enum):
    """QLDB table statuses."""
    ACTIVE = "ACTIVE"


class IndexStatus(Enum):
    """QLDB index statuses."""
    CREATING = "CREATING"
    ACTIVE = "ACTIVE"


class ExportStatus(Enum):
    """QLDB journal export statuses."""
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"


class BackupStatus(Enum):
    """QLDB backup statuses."""
    CREATING = "CREATING"
    DELETED = "DELETED"
    ACTIVE = "ACTIVE"
    RESTORING = "RESTORING"


class PermissionMode(Enum):
    """QLDB permission modes."""
    ALLOW_ALL = "ALLOW_ALL"
    STANDARD = "STANDARD"


@dataclass
class LedgerConfig:
    """QLDB ledger configuration."""
    ledger_name: str
    permissions_mode: str = 'STANDARD'
    kms_key_id: Optional[str] = None
    deletion_protection: bool = True
    tags: Optional[Dict[str, str]] = None


@dataclass
class TableConfig:
    """QLDB table configuration."""
    table_name: str
    ledger_name: str
    indexes: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class DocumentOperation:
    """Represents a single document operation."""
    operation_type: str  # 'insert', 'update', 'delete'
    table_name: str
    document: Optional[Dict[str, Any]] = None
    document_id: Optional[str] = None
    condition: Optional[str] = None


@dataclass
class ExportConfig:
    """Journal export configuration."""
    export_id: str
    ledger_name: str
    s3_bucket: str
    s3_prefix: str
    export_time: datetime
    included_forms: List[str] = field(default_factory=lambda: ['JOURNAL'])


@dataclass
class BackupConfig:
    """QLDB backup configuration."""
    backup_id: str
    ledger_name: str
    creation_time: datetime
    tags: Optional[Dict[str, str]] = None


class QLDBIntegration:
    """
    AWS QLDB (Quantum Ledger Database) Integration class providing comprehensive ledger management.

    Features:
    1. Ledger management: Create/manage ledgers
    2. Table management: Create/manage tables
    3. Document operations: Insert/update/delete documents
    4. Query: Query using PartiQL
    5. Journal exports: Export journal to S3
    6. Backups: Create and manage backups
    7. Point-in-time recovery: PITR for ledgers
    8. Encryption: KMS encryption
    9. Permissions: IAM permissions management
    10. CloudWatch integration: Storage and I/O metrics
    """

    def __init__(
        self,
        region_name: str = 'us-east-1',
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        ledger_name: Optional[str] = None
    ):
        """
        Initialize QLDB integration.

        Args:
            region_name: AWS region name
            aws_access_key_id: AWS access key ID (uses default credentials if None)
            aws_secret_access_key: AWS secret access key (uses default credentials if None)
            ledger_name: Default ledger name to use
        """
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.default_ledger_name = ledger_name

        self._qldb = None
        self._qldb_session = None
        self._s3 = None
        self._cloudwatch = None
        self._iam = None
        self._ledgers = {}
        self._table_locks = defaultdict(threading.RLock)
        self._ledgers_lock = threading.RLock()

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

            self._qldb = session.client('qldb', **session_kwargs)
            self._qldb_session = session.client('qldb-session', **session_kwargs)
            self._s3 = session.client('s3', **session_kwargs)
            self._cloudwatch = session.client('cloudwatch', **session_kwargs)
            self._iam = session.client('iam', **session_kwargs)

            logger.info(f"QLDB clients initialized for region {self.region_name}")
        except Exception as e:
            logger.error(f"Failed to initialize QLDB clients: {e}")

    @property
    def is_available(self) -> bool:
        """Check if boto3 is available and clients are initialized."""
        return BOTO3_AVAILABLE and self._qldb is not None

    # =========================================================================
    # Ledger Management
    # =========================================================================

    def create_ledger(
        self,
        ledger_config: LedgerConfig,
        wait_for_active: bool = True,
        timeout: int = 300
    ) -> Dict[str, Any]:
        """
        Create a QLDB ledger.

        Args:
            ledger_config: Ledger configuration
            wait_for_active: Wait for ledger to become active
            timeout: Timeout in seconds for waiting

        Returns:
            Ledger description dict
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized. boto3 may not be available.")

        with self._ledgers_lock:
            try:
                params = {
                    'Name': ledger_config.ledger_name,
                    'PermissionsMode': ledger_config.permissions_mode,
                    'DeletionProtection': ledger_config.deletion_protection
                }

                if ledger_config.kms_key_id:
                    params['KmsKey'] = ledger_config.kms_key_id

                if ledger_config.tags:
                    params['Tags'] = [
                        {'Key': k, 'Value': v} for k, v in ledger_config.tags.items()
                    ]

                response = self._qldb.create_ledger(**params)
                
                if wait_for_active:
                    start_time = time.time()
                    while time.time() - start_time < timeout:
                        ledger_status = self.describe_ledger(ledger_config.ledger_name)
                        if ledger_status.get('Status') == 'ACTIVE':
                            break
                        time.sleep(5)
                    else:
                        logger.warning(f"Ledger {ledger_config.ledger_name} did not become active within {timeout}s")

                with self._ledgers_lock:
                    self._ledgers[ledger_config.ledger_name] = response

                logger.info(f"Created QLDB ledger: {ledger_config.ledger_name}")
                return response

            except ClientError as e:
                logger.error(f"Failed to create ledger {ledger_config.ledger_name}: {e}")
                raise

    def describe_ledger(self, ledger_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a QLDB ledger.

        Args:
            ledger_name: Name of the ledger

        Returns:
            Ledger description dict
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            response = self._qldb.describe_ledger(Name=ledger_name)
            with self._ledgers_lock:
                self._ledgers[ledger_name] = response
            return response
        except ClientError as e:
            logger.error(f"Failed to describe ledger {ledger_name}: {e}")
            raise

    def list_ledgers(
        self,
        max_results: int = 100,
        filter_status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List all QLDB ledgers.

        Args:
            max_results: Maximum number of results to return
            filter_status: Filter by status (ACTIVE, CREATING, DELETED, DELETING)

        Returns:
            List of ledger descriptions
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            ledgers = []
            paginator = self._qldb.get_paginator('list_ledgers')

            for page in paginator.paginate(PaginationMaxResults=max_results):
                for ledger in page.get('Ledgers', []):
                    if filter_status is None or ledger.get('Status') == filter_status:
                        ledgers.append(ledger)

            return ledgers
        except ClientError as e:
            logger.error(f"Failed to list ledgers: {e}")
            raise

    def update_ledger(
        self,
        ledger_name: str,
        deletion_protection: Optional[bool] = None,
        kms_key_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update a QLDB ledger.

        Args:
            ledger_name: Name of the ledger
            deletion_protection: Enable/disable deletion protection
            kms_key_id: KMS key ID for encryption

        Returns:
            Updated ledger description
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            params = {'Name': ledger_name}

            if deletion_protection is not None:
                params['DeletionProtection'] = deletion_protection

            if kms_key_id is not None:
                params['KmsKey'] = kms_key_id

            response = self._qldb.update_ledger(**params)

            with self._ledgers_lock:
                self._ledgers[ledger_name] = response

            logger.info(f"Updated QLDB ledger: {ledger_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to update ledger {ledger_name}: {e}")
            raise

    def delete_ledger(
        self,
        ledger_name: str,
        wait_for_deletion: bool = False,
        timeout: int = 300
    ) -> Dict[str, Any]:
        """
        Delete a QLDB ledger.

        Args:
            ledger_name: Name of the ledger
            wait_for_deletion: Wait for ledger to be fully deleted
            timeout: Timeout in seconds

        Returns:
            Deletion result
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            response = self._qldb.delete_ledger(Name=ledger_name)

            if wait_for_deletion:
                start_time = time.time()
                while time.time() - start_time < timeout:
                    try:
                        self.describe_ledger(ledger_name)
                        time.sleep(5)
                    except ClientError as e:
                        if e.response['Error']['Code'] == 'ResourceNotFoundException':
                            break
                        raise
                else:
                    logger.warning(f"Ledger {ledger_name} was not fully deleted within {timeout}s")

            with self._ledgers_lock:
                self._ledgers.pop(ledger_name, None)

            logger.info(f"Deleted QLDB ledger: {ledger_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete ledger {ledger_name}: {e}")
            raise

    # =========================================================================
    # Table Management
    # =========================================================================

    def create_table(
        self,
        table_name: str,
        ledger_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a table in a QLDB ledger usingPartiQL.

        Args:
            table_name: Name of the table to create
            ledger_name: Name of the ledger (uses default if None)

        Returns:
            Table creation result
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        ledger_name = ledger_name or self.default_ledger_name
        if not ledger_name:
            raise ValueError("Ledger name must be specified.")

        with self._table_locks[table_name]:
            try:
                statement = f"CREATE TABLE {table_name}"
                response = self._execute_statement(statement, ledger_name)
                logger.info(f"Created table '{table_name}' in ledger '{ledger_name}'")
                return response
            except ClientError as e:
                logger.error(f"Failed to create table {table_name}: {e}")
                raise

    def list_tables(self, ledger_name: Optional[str] = None) -> List[str]:
        """
        List all tables in a QLDB ledger.

        Args:
            ledger_name: Name of the ledger (uses default if None)

        Returns:
            List of table names
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        ledger_name = ledger_name or self.default_ledger_name
        if not ledger_name:
            raise ValueError("Ledger name must be specified.")

        try:
            statement = "SELECT table_name FROM information_schema.user_tables"
            response = self._execute_statement(statement, ledger_name)

            tables = []
            for row in response.get('FirstPage', {}).get('Results', []):
                if row:
                    tables.append(row[0].get('ScalarValue', ''))

            return tables
        except ClientError as e:
            logger.error(f"Failed to list tables in ledger {ledger_name}: {e}")
            raise

    def describe_table(
        self,
        table_name: str,
        ledger_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get detailed information about a table.

        Args:
            table_name: Name of the table
            ledger_name: Name of the ledger (uses default if None)

        Returns:
            Table information
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        ledger_name = ledger_name or self.default_ledger_name
        if not ledger_name:
            raise ValueError("Ledger name must be specified.")

        try:
            statement = f"SELECT * FROM information_schema.user_tables WHERE table_name = '{table_name}'"
            response = self._execute_statement(statement, ledger_name)
            return response
        except ClientError as e:
            logger.error(f"Failed to describe table {table_name}: {e}")
            raise

    def drop_table(self, table_name: str, ledger_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Drop a table from a QLDB ledger.

        Args:
            table_name: Name of the table to drop
            ledger_name: Name of the ledger (uses default if None)

        Returns:
            Drop table result
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        ledger_name = ledger_name or self.default_ledger_name
        if not ledger_name:
            raise ValueError("Ledger name must be specified.")

        with self._table_locks[table_name]:
            try:
                statement = f"DROP TABLE {table_name}"
                response = self._execute_statement(statement, ledger_name)
                logger.info(f"Dropped table '{table_name}' from ledger '{ledger_name}'")
                return response
            except ClientError as e:
                logger.error(f"Failed to drop table {table_name}: {e}")
                raise

    def create_index(
        self,
        table_name: str,
        column_name: str,
        ledger_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create an index on a table column.

        Args:
            table_name: Name of the table
            column_name: Name of the column to index
            ledger_name: Name of the ledger (uses default if None)

        Returns:
            Index creation result
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        ledger_name = ledger_name or self.default_ledger_name
        if not ledger_name:
            raise ValueError("Ledger name must be specified.")

        try:
            statement = f"CREATE INDEX ON {table_name} ({column_name})"
            response = self._execute_statement(statement, ledger_name)
            logger.info(f"Created index on column '{column_name}' in table '{table_name}'")
            return response
        except ClientError as e:
            logger.error(f"Failed to create index on {column_name}: {e}")
            raise

    # =========================================================================
    # Document Operations
    # =========================================================================

    def insert_document(
        self,
        table_name: str,
        document: Dict[str, Any],
        ledger_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Insert a document into a table.

        Args:
            table_name: Name of the table
            document: Document to insert
            ledger_name: Name of the ledger (uses default if None)

        Returns:
            Insert result with document ID
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        ledger_name = ledger_name or self.default_ledger_name
        if not ledger_name:
            raise ValueError("Ledger name must be specified.")

        try:
            doc_id = str(uuid.uuid4())
            document_with_id = {'id': doc_id, **document}

            statement = f"INSERT INTO {table_name} << ? >>"
            response = self._execute_statement(
                statement,
                ledger_name,
                parameters=[{'IonValue': self._dict_to_ion(document_with_id)}]
            )

            logger.info(f"Inserted document with ID '{doc_id}' into table '{table_name}'")
            return {
                'document_id': doc_id,
                'response': response
            }
        except ClientError as e:
            logger.error(f"Failed to insert document into {table_name}: {e}")
            raise

    def update_document(
        self,
        table_name: str,
        document_id: str,
        updates: Dict[str, Any],
        ledger_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update a document in a table.

        Args:
            table_name: Name of the table
            document_id: ID of the document to update
            updates: Fields to update
            ledger_name: Name of the ledger (uses default if None)

        Returns:
            Update result
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        ledger_name = ledger_name or self.default_ledger_name
        if not ledger_name:
            raise ValueError("Ledger name must be specified.")

        try:
            update_parts = []
            params = []
            for i, (key, value) in enumerate(updates.items()):
                update_parts.append(f"{key} = ?")
                params.append({'IonValue': self._dict_to_ion({key: value})})

            statement = f"UPDATE {table_name} AS t SET {', '.join(update_parts)} WHERE t.id = ?"
            params.append({'IonValue': self._dict_to_ion({'id': document_id})})

            response = self._execute_statement(statement, ledger_name, parameters=params)

            logger.info(f"Updated document with ID '{document_id}' in table '{table_name}'")
            return response
        except ClientError as e:
            logger.error(f"Failed to update document {document_id}: {e}")
            raise

    def delete_document(
        self,
        table_name: str,
        document_id: str,
        ledger_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete a document from a table.

        Args:
            table_name: Name of the table
            document_id: ID of the document to delete
            ledger_name: Name of the ledger (uses default if None)

        Returns:
            Delete result
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        ledger_name = ledger_name or self.default_ledger_name
        if not ledger_name:
            raise ValueError("Ledger name must be specified.")

        try:
            statement = f"DELETE FROM {table_name} AS t WHERE t.id = ?"
            response = self._execute_statement(
                statement,
                ledger_name,
                parameters=[{'IonValue': self._dict_to_ion({'id': document_id})}]
            )

            logger.info(f"Deleted document with ID '{document_id}' from table '{table_name}'")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete document {document_id}: {e}")
            raise

    def get_document(
        self,
        table_name: str,
        document_id: str,
        ledger_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get a document by ID.

        Args:
            table_name: Name of the table
            document_id: ID of the document
            ledger_name: Name of the ledger (uses default if None)

        Returns:
            Document data
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        ledger_name = ledger_name or self.default_ledger_name
        if not ledger_name:
            raise ValueError("Ledger name must be specified.")

        try:
            statement = f"SELECT * FROM {table_name} AS t WHERE t.id = ?"
            response = self._execute_statement(
                statement,
                ledger_name,
                parameters=[{'IonValue': self._dict_to_ion({'id': document_id})}]
            )

            results = response.get('FirstPage', {}).get('Results', [])
            if results:
                return self._ion_to_dict(results[0])
            return None
        except ClientError as e:
            logger.error(f"Failed to get document {document_id}: {e}")
            raise

    def get_revision_history(
        self,
        table_name: str,
        document_id: str,
        ledger_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get the revision history of a document.

        Args:
            table_name: Name of the table
            document_id: ID of the document
            ledger_name: Name of the ledger (uses default if None)

        Returns:
            List of document revisions
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        ledger_name = ledger_name or self.default_ledger_name
        if not ledger_name:
            raise ValueError("Ledger name must be specified.")

        try:
            statement = f"SELECT * FROM history({table_name}) AS h WHERE h.id = ?"
            response = self._execute_statement(
                statement,
                ledger_name,
                parameters=[{'IonValue': self._dict_to_ion({'id': document_id})}]
            )

            revisions = []
            for result in response.get('FirstPage', {}).get('Results', []):
                revisions.append(self._ion_to_dict(result))

            return revisions
        except ClientError as e:
            logger.error(f"Failed to get revision history for {document_id}: {e}")
            raise

    # =========================================================================
    # PartiQL Query
    # =========================================================================

    def _execute_statement(
        self,
        statement: str,
        ledger_name: str,
        parameters: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Execute a PartiQL statement.

        Args:
            statement: PartiQL statement to execute
            ledger_name: Name of the ledger
            parameters: Optional parameters for the statement

        Returns:
            Query result
        """
        try:
            params = {
                'TransactionContext': {'LedgerName': ledger_name},
                'Statement': statement
            }

            if parameters:
                params['Parameters'] = parameters

            response = self._qldb_session.execute_statement(**params)
            return response
        except ClientError as e:
            logger.error(f"Failed to execute statement: {statement} - {e}")
            raise

    def execute_query(
        self,
        query: str,
        ledger_name: Optional[str] = None,
        parameters: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Execute a PartiQL query.

        Args:
            query: PartiQL query to execute
            ledger_name: Name of the ledger (uses default if None)
            parameters: Optional parameters for the query

        Returns:
            Query result
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        ledger_name = ledger_name or self.default_ledger_name
        if not ledger_name:
            raise ValueError("Ledger name must be specified.")

        try:
            response = self._execute_statement(query, ledger_name, parameters)
            return response
        except ClientError as e:
            logger.error(f"Failed to execute query: {query}: {e}")
            raise

    def query_table(
        self,
        table_name: str,
        filter_condition: Optional[str] = None,
        select_columns: Optional[List[str]] = None,
        ledger_name: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Query a table with optional filters.

        Args:
            table_name: Name of the table to query
            filter_condition: Optional WHERE clause condition
            select_columns: Optional list of columns to select
            ledger_name: Name of the ledger (uses default if None)
            limit: Optional limit on results

        Returns:
            List of matching documents
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        ledger_name = ledger_name or self.default_ledger_name
        if not ledger_name:
            raise ValueError("Ledger name must be specified.")

        try:
            columns = ', '.join(select_columns) if select_columns else '*'
            query = f"SELECT {columns} FROM {table_name}"

            if filter_condition:
                query += f" WHERE {filter_condition}"

            if limit:
                query += f" LIMIT {limit}"

            response = self._execute_statement(query, ledger_name)

            results = []
            for result in response.get('FirstPage', {}).get('Results', []):
                results.append(self._ion_to_dict(result))

            return results
        except ClientError as e:
            logger.error(f"Failed to query table {table_name}: {e}")
            raise

    # =========================================================================
    # Journal Exports
    # =========================================================================

    def create_journal_export(
        self,
        ledger_name: str,
        s3_bucket: str,
        s3_prefix: str,
        export_time: Optional[datetime] = None,
        included_forms: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Create a journal export to S3.

        Args:
            ledger_name: Name of the ledger
            s3_bucket: S3 bucket name for export
            s3_prefix: S3 prefix/path for export
            export_time: Optional specific time to export up to
            included_forms: Optional list of forms to include

        Returns:
            Export description
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            params = {
                'LedgerName': ledger_name,
                'S3Bucket': s3_bucket,
                'S3Prefix': s3_prefix
            }

            if export_time:
                params['ExportCreationTime'] = export_time.isoformat()

            if included_forms:
                params['IncludedFormNames'] = included_forms
            else:
                params['IncludedFormNames'] = ['JOURNAL']

            response = self._qldb.create_journal_export(**params)

            export_id = response.get('ExportId')
            logger.info(f"Created journal export {export_id} for ledger {ledger_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create journal export for ledger {ledger_name}: {e}")
            raise

    def describe_journal_export(
        self,
        ledger_name: str,
        export_id: str
    ) -> Dict[str, Any]:
        """
        Get details of a journal export.

        Args:
            ledger_name: Name of the ledger
            export_id: ID of the export

        Returns:
            Export description
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            response = self._qldb.describe_journal_export(
                LedgerName=ledger_name,
                ExportId=export_id
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to describe journal export {export_id}: {e}")
            raise

    def list_journal_exports(
        self,
        ledger_name: str,
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List journal exports for a ledger.

        Args:
            ledger_name: Name of the ledger
            max_results: Maximum number of results

        Returns:
            List of export descriptions
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            response = self._qldb.list_journal_exports(
                LedgerName=ledger_name,
                MaxResults=max_results
            )
            return response.get('JournalExports', [])
        except ClientError as e:
            logger.error(f"Failed to list journal exports for ledger {ledger_name}: {e}")
            raise

    def cancel_journal_export(
        self,
        ledger_name: str,
        export_id: str
    ) -> Dict[str, Any]:
        """
        Cancel a journal export.

        Args:
            ledger_name: Name of the ledger
            export_id: ID of the export to cancel

        Returns:
            Cancellation result
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            response = self._qldb.cancel_journal_export(
                LedgerName=ledger_name,
                ExportId=export_id
            )
            logger.info(f"Cancelled journal export {export_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to cancel journal export {export_id}: {e}")
            raise

    # =========================================================================
    # Backups
    # =========================================================================

    def create_backup(
        self,
        ledger_name: str,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a backup of a ledger.

        Args:
            ledger_name: Name of the ledger to backup
            tags: Optional tags for the backup

        Returns:
            Backup description
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            params = {'LedgerName': ledger_name}

            if tags:
                params['Tags'] = {k: v for k, v in tags.items()}

            response = self._qldb.create_ledger_backup(**params)

            backup_id = response.get('BackupSummary', {}).get('BackupId')
            logger.info(f"Created backup {backup_id} for ledger {ledger_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create backup for ledger {ledger_name}: {e}")
            raise

    def describe_backup(
        self,
        backup_arn: str
    ) -> Dict[str, Any]:
        """
        Get details of a ledger backup.

        Args:
            backup_arn: ARN of the backup

        Returns:
            Backup description
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            response = self._qldb.describe_ledger_backup(BackupArn=backup_arn)
            return response
        except ClientError as e:
            logger.error(f"Failed to describe backup {backup_arn}: {e}")
            raise

    def list_backups(
        self,
        ledger_name: Optional[str] = None,
        max_results: int = 100,
        filter_status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List ledger backups.

        Args:
            ledger_name: Optional ledger name to filter by
            max_results: Maximum number of results
            filter_status: Optional status filter (ACTIVE, CREATING, DELETED)

        Returns:
            List of backup summaries
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            params = {'MaxResults': max_results}

            if ledger_name:
                params['LedgerName'] = ledger_name

            if filter_status:
                params['BackupStatus'] = filter_status

            response = self._qldb.list_ledger_backups(**params)
            return response.get('BackupSummaries', [])
        except ClientError as e:
            logger.error(f"Failed to list backups: {e}")
            raise

    def delete_backup(self, backup_arn: str) -> Dict[str, Any]:
        """
        Delete a ledger backup.

        Args:
            backup_arn: ARN of the backup to delete

        Returns:
            Deletion result
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            response = self._qldb.delete_ledger_backup(BackupArn=backup_arn)
            logger.info(f"Deleted backup {backup_arn}")
            return response
        except ClientError as e:
            logger.error(f"Failed to delete backup {backup_arn}: {e}")
            raise

    def restore_ledger(
        self,
        ledger_name: str,
        backup_arn: str,
        wait_for_active: bool = True,
        timeout: int = 600
    ) -> Dict[str, Any]:
        """
        Restore a ledger from a backup.

        Args:
            ledger_name: Name for the restored ledger
            backup_arn: ARN of the backup to restore from
            wait_for_active: Wait for ledger to become active
            timeout: Timeout in seconds

        Returns:
            Restored ledger description
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            response = self._qldb.restore_ledger_from_backup(
                LedgerName=ledger_name,
                BackupArn=backup_arn
            )

            if wait_for_active:
                start_time = time.time()
                while time.time() - start_time < timeout:
                    ledger_status = self.describe_ledger(ledger_name)
                    if ledger_status.get('Status') == 'ACTIVE':
                        break
                    time.sleep(10)
                else:
                    logger.warning(f"Restored ledger {ledger_name} did not become active within {timeout}s")

            logger.info(f"Restored ledger {ledger_name} from backup {backup_arn}")
            return response
        except ClientError as e:
            logger.error(f"Failed to restore ledger {ledger_name} from backup: {e}")
            raise

    # =========================================================================
    # Point-in-Time Recovery
    # =========================================================================

    def enable_pitr(self, ledger_name: str) -> Dict[str, Any]:
        """
        Enable point-in-time recovery for a ledger.

        Args:
            ledger_name: Name of the ledger

        Returns:
            Result of the operation
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            response = self._qldb.update_ledger(
                Name=ledger_name,
                PointInTimeRecoveryEnabled=True
            )
            logger.info(f"Enabled PITR for ledger {ledger_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to enable PITR for ledger {ledger_name}: {e}")
            raise

    def disable_pitr(self, ledger_name: str) -> Dict[str, Any]:
        """
        Disable point-in-time recovery for a ledger.

        Args:
            ledger_name: Name of the ledger

        Returns:
            Result of the operation
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            response = self._qldb.update_ledger(
                Name=ledger_name,
                PointInTimeRecoveryEnabled=False
            )
            logger.info(f"Disabled PITR for ledger {ledger_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to disable PITR for ledger {ledger_name}: {e}")
            raise

    def is_pitr_enabled(self, ledger_name: str) -> bool:
        """
        Check if PITR is enabled for a ledger.

        Args:
            ledger_name: Name of the ledger

        Returns:
            True if PITR is enabled, False otherwise
        """
        try:
            ledger = self.describe_ledger(ledger_name)
            return ledger.get('PointInTimeRecoveryEnabled', False)
        except ClientError as e:
            logger.error(f"Failed to check PITR status for ledger {ledger_name}: {e}")
            raise

    def restore_ledger_pitr(
        self,
        ledger_name: str,
        target_ledger_name: str,
        recovery_time: datetime,
        kms_key_id: Optional[str] = None,
        wait_for_active: bool = True,
        timeout: int = 600
    ) -> Dict[str, Any]:
        """
        Restore a ledger to a point in time.

        Args:
            ledger_name: Name of the source ledger
            target_ledger_name: Name for the restored ledger
            recovery_time: Point in time to restore to
            kms_key_id: Optional KMS key for encryption
            wait_for_active: Wait for ledger to become active
            timeout: Timeout in seconds

        Returns:
            Restored ledger description
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            params = {
                'LedgerName': target_ledger_name,
                'SourceLedgerName': ledger_name,
                'RestoreTimestamp': recovery_time.isoformat()
            }

            if kms_key_id:
                params['KmsKey'] = kms_key_id

            response = self._qldb.restore_ledger_to_point_in_time(**params)

            if wait_for_active:
                start_time = time.time()
                while time.time() - start_time < timeout:
                    try:
                        ledger_status = self.describe_ledger(target_ledger_name)
                        if ledger_status.get('Status') == 'ACTIVE':
                            break
                    except ClientError:
                        pass
                    time.sleep(10)
                else:
                    logger.warning(f"Restored ledger {target_ledger_name} did not become active within {timeout}s")

            logger.info(f"Restored ledger {target_ledger_name} to point in time {recovery_time}")
            return response
        except ClientError as e:
            logger.error(f"Failed to restore ledger {ledger_name} to point in time: {e}")
            raise

    # =========================================================================
    # Encryption (KMS)
    # =========================================================================

    def update_ledger_encryption(
        self,
        ledger_name: str,
        kms_key_id: str
    ) -> Dict[str, Any]:
        """
        Update the KMS encryption key for a ledger.

        Args:
            ledger_name: Name of the ledger
            kms_key_id: KMS key ID to use for encryption

        Returns:
            Updated ledger description
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            response = self._qldb.update_ledger(
                Name=ledger_name,
                KmsKey=kms_key_id
            )
            logger.info(f"Updated encryption key for ledger {ledger_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to update encryption for ledger {ledger_name}: {e}")
            raise

    def get_ledger_encryption_info(self, ledger_name: str) -> Dict[str, Any]:
        """
        Get encryption information for a ledger.

        Args:
            ledger_name: Name of the ledger

        Returns:
            Encryption information
        """
        try:
            ledger = self.describe_ledger(ledger_name)
            return {
                'encryption_status': ledger.get('EncryptionDescription', {}).get('EncryptionStatus'),
                'kms_key_arn': ledger.get('EncryptionDescription', {}).get('KmsKeyArn'),
                'encryption_description': ledger.get('EncryptionDescription', {})
            }
        except ClientError as e:
            logger.error(f"Failed to get encryption info for ledger {ledger_name}: {e}")
            raise

    # =========================================================================
    # IAM Permissions
    # =========================================================================

    def get_ledger_permissions(self, ledger_name: str) -> Dict[str, Any]:
        """
        Get the permissions mode for a ledger.

        Args:
            ledger_name: Name of the ledger

        Returns:
            Permissions information
        """
        try:
            ledger = self.describe_ledger(ledger_name)
            return {
                'permissions_mode': ledger.get('PermissionsMode'),
                'arn': ledger.get('Arn')
            }
        except ClientError as e:
            logger.error(f"Failed to get permissions for ledger {ledger_name}: {e}")
            raise

    def update_ledger_permissions(
        self,
        ledger_name: str,
        permissions_mode: str
    ) -> Dict[str, Any]:
        """
        Update the permissions mode for a ledger.

        Args:
            ledger_name: Name of the ledger
            permissions_mode: Permissions mode ('ALLOW_ALL' or 'STANDARD')

        Returns:
            Updated ledger description
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            response = self._qldb.update_ledger(
                Name=ledger_name,
                PermissionsMode=permissions_mode
            )
            logger.info(f"Updated permissions mode to {permissions_mode} for ledger {ledger_name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to update permissions for ledger {ledger_name}: {e}")
            raise

    def create_access_statement(
        self,
        ledger_name: str,
        actions: List[str],
        resources: List[str]
    ) -> Dict[str, Any]:
        """
        Create an IAM policy statement for QLDB access.

        Args:
            ledger_name: Name of the ledger
            actions: List of QLDB actions
            resources: List of resource ARNs

        Returns:
            IAM policy statement
        """
        statement = {
            "Effect": "Allow",
            "Action": actions,
            "Resource": resources + [f"arn:aws:qldb:{self.region_name}:*:ledger/{ledger_name}"]
        }
        return statement

    def generate_iam_policy(self, ledger_name: str) -> Dict[str, Any]:
        """
        Generate a comprehensive IAM policy for QLDB access.

        Args:
            ledger_name: Name of the ledger

        Returns:
            IAM policy document
        """
        ledger_arn = f"arn:aws:qldb:{self.region_name}:*:ledger/{ledger_name}"

        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "qldb:DescribeLedger",
                        "qldb:ListLedgers",
                        "qldb:ListTables",
                        "qldb:DescribeTable",
                        "qldb:GetBlock",
                        "qldb:GetDigest",
                        "qldb:GetRevision",
                        "qldb:Select"
                    ],
                    "Resource": ledger_arn
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "qldb:Insert",
                        "qldb:Update",
                        "qldb:Delete"
                    ],
                    "Resource": f"{ledger_arn}/table/*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "qldb:CreateLedger",
                        "qldb:DeleteLedger"
                    ],
                    "Resource": "*"
                }
            ]
        }

        return policy

    # =========================================================================
    # CloudWatch Integration
    # =========================================================================

    def get_storage_metrics(
        self,
        ledger_name: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 3600
    ) -> List[Dict[str, Any]]:
        """
        Get storage metrics from CloudWatch.

        Args:
            ledger_name: Name of the ledger
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Metric period in seconds

        Returns:
            List of storage metric data points
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            response = self._cloudwatch.get_metric_statistics(
                Namespace='AWS/QLDB',
                MetricName='JournalStorage',
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=['Average', 'Maximum', 'Minimum']
            )

            metrics = []
            for datapoint in response.get('Datapoints', []):
                metrics.append({
                    'timestamp': datapoint.get('Timestamp'),
                    'average': datapoint.get('Average'),
                    'maximum': datapoint.get('Maximum'),
                    'minimum': datapoint.get('Minimum'),
                    'unit': datapoint.get('Unit')
                })

            return metrics
        except ClientError as e:
            logger.error(f"Failed to get storage metrics: {e}")
            raise

    def get_io_metrics(
        self,
        ledger_name: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 3600
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get I/O metrics from CloudWatch.

        Args:
            ledger_name: Name of the ledger
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Metric period in seconds

        Returns:
            Dictionary of I/O metrics
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            metrics = {}
            metric_names = ['ReadIOs', 'WriteIOs', 'ReadLatency', 'WriteLatency']

            for metric_name in metric_names:
                response = self._cloudwatch.get_metric_statistics(
                    Namespace='AWS/QLDB',
                    MetricName=metric_name,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=['Average', 'Maximum', 'Minimum', 'Sum']
                )

                metrics[metric_name] = []
                for datapoint in response.get('Datapoints', []):
                    metrics[metric_name].append({
                        'timestamp': datapoint.get('Timestamp'),
                        'average': datapoint.get('Average'),
                        'maximum': datapoint.get('Maximum'),
                        'minimum': datapoint.get('Minimum'),
                        'sum': datapoint.get('Sum'),
                        'unit': datapoint.get('Unit')
                    })

            return metrics
        except ClientError as e:
            logger.error(f"Failed to get I/O metrics: {e}")
            raise

    def get_ledger_metrics_summary(self, ledger_name: str) -> Dict[str, Any]:
        """
        Get a summary of all QLDB metrics for a ledger.

        Args:
            ledger_name: Name of the ledger

        Returns:
            Metrics summary
        """
        try:
            ledger = self.describe_ledger(ledger_name)

            summary = {
                'ledger_name': ledger_name,
                'ledger_status': ledger.get('Status'),
                'creation_time': ledger.get('CreationDateTime'),
                'deletion_protection': ledger.get('DeletionProtection'),
                'encryption': ledger.get('EncryptionDescription', {}).get('EncryptionStatus'),
                'pitr_enabled': ledger.get('PointInTimeRecoveryEnabled', False),
                'permissions_mode': ledger.get('PermissionsMode')
            }

            return summary
        except ClientError as e:
            logger.error(f"Failed to get metrics summary for ledger {ledger_name}: {e}")
            raise

    def put_metric_data(
        self,
        metric_name: str,
        value: float,
        unit: str = 'Count',
        dimensions: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        Put custom metric data to CloudWatch.

        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: Unit of the metric
            dimensions: Optional dimensions

        Returns:
            Result of the put operation
        """
        if not self.is_available:
            raise RuntimeError("QLDB client not initialized.")

        try:
            params = {
                'Namespace': 'Custom/QLDB',
                'MetricData': [
                    {
                        'MetricName': metric_name,
                        'Value': value,
                        'Unit': unit,
                        'Timestamp': datetime.utcnow()
                    }
                ]
            }

            if dimensions:
                params['MetricData'][0]['Dimensions'] = dimensions

            response = self._cloudwatch.put_metric_data(**params)
            return response
        except ClientError as e:
            logger.error(f"Failed to put metric data: {e}")
            raise

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def _dict_to_ion(self, data: Dict[str, Any]) -> str:
        """
        Convert a Python dict to Ion format string.

        Args:
            data: Dictionary to convert

        Returns:
            Ion formatted string
        """
        try:
            import ion
            return ion.dumps(data, binary=False)
        except ImportError:
            return json.dumps(data)

    def _ion_to_dict(self, ion_data: Any) -> Dict[str, Any]:
        """
        Convert Ion data to Python dict.

        Args:
            ion_data: Ion data to convert

        Returns:
            Python dictionary
        """
        try:
            import ion
            return ion.loads(ion_data)
        except ImportError:
            if isinstance(ion_data, list) and ion_data:
                result = {}
                for item in ion_data:
                    if isinstance(item, dict) and 'FieldName' in item and 'ScalarValue' in item:
                        result[item['FieldName']] = item['ScalarValue']
                return result
            return ion_data if isinstance(ion_data, dict) else {}

    def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on the QLDB integration.

        Returns:
            Health check result
        """
        return {
            'service': 'qldb',
            'available': self.is_available,
            'region': self.region_name,
            'boto3_available': BOTO3_AVAILABLE,
            'ledgers_cached': len(self._ledgers)
        }

    def __repr__(self) -> str:
        return f"QLDBIntegration(region='{self.region_name}', ledger='{self.default_ledger_name}')"

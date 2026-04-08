"""Snowflake Snowpipe integration for RabAI AutoClick.

Provides actions to manage Snowpipe continuous data ingestion, 
warehouse operations, and Snowflake SQL queries.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SnowflakeConnectionAction(BaseAction):
    """Connect to Snowflake and manage warehouse operations.

    Provides connection management and basic warehouse lifecycle.
    """
    action_type = "snowflake_connection"
    display_name = "Snowflake连接"
    description = "连接Snowflake并管理仓库操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Connect to Snowflake and manage warehouse.

        Args:
            context: Execution context.
            params: Dict with keys:
                - account: Snowflake account identifier
                - user: Username
                - password: Password (or use private_key)
                - private_key: PEM private key (base64 encoded)
                - warehouse: Warehouse name
                - database: Database name
                - schema: Schema name
                - operation: connect | disconnect | use_warehouse | use_database | use_schema

        Returns:
            ActionResult with connection/schema result.
        """
        operation = params.get('operation', 'connect')

        try:
            import snowflake.connector
        except ImportError:
            return ActionResult(success=False, message="snowflake-connector-python not installed. Run: pip install snowflake-connector-python")

        # Store connection in context for reuse
        conn = getattr(context, '_snowflake_conn', None)

        try:
            if operation == 'connect':
                account = params.get('account') or os.environ.get('SNOWFLAKE_ACCOUNT')
                user = params.get('user') or os.environ.get('SNOWFLAKE_USER')
                password = params.get('password') or os.environ.get('SNOWFLAKE_PASSWORD')
                warehouse = params.get('warehouse') or os.environ.get('SNOWFLAKE_WAREHOUSE')
                database = params.get('database') or os.environ.get('SNOWFLAKE_DATABASE')
                schema = params.get('schema') or os.environ.get('SNOWFLAKE_SCHEMA')

                if not all([account, user]):
                    return ActionResult(success=False, message="account and user are required")

                conn_params = {
                    'account': account,
                    'user': user,
                    'warehouse': warehouse,
                    'database': database,
                    'schema': schema,
                }
                if params.get('private_key'):
                    from cryptography.hazmat.primitives import serialization
                    from cryptography.hazmat.backends import default_backend
                    import base64
                    pem_data = base64.b64decode(params['private_key'])
                    pkey = serialization.load_pem_private_key(pem_data, password=None, backend=default_backend())
                    conn_params['private_key'] = pkey
                else:
                    conn_params['password'] = password or os.environ.get('SNOWFLAKE_PASSWORD')

                conn = snowflake.connector.connect(**conn_params)
                context._snowflake_conn = conn
                return ActionResult(success=True, message="Connected to Snowflake", data={
                    'warehouse': warehouse, 'database': database, 'schema': schema
                })

            elif operation == 'disconnect':
                if conn:
                    conn.close()
                    context._snowflake_conn = None
                return ActionResult(success=True, message="Disconnected from Snowflake")

            elif operation == 'use_warehouse':
                warehouse = params.get('warehouse')
                if not warehouse:
                    return ActionResult(success=False, message="warehouse is required")
                if not conn:
                    return ActionResult(success=False, message="Not connected to Snowflake")
                conn.cursor().execute(f"USE WAREHOUSE {warehouse}")
                return ActionResult(success=True, message=f"Using warehouse {warehouse}")

            elif operation == 'use_database':
                database = params.get('database')
                if not database:
                    return ActionResult(success=False, message="database is required")
                if not conn:
                    return ActionResult(success=False, message="Not connected to Snowflake")
                conn.cursor().execute(f"USE DATABASE {database}")
                return ActionResult(success=True, message=f"Using database {database}")

            elif operation == 'use_schema':
                schema = params.get('schema')
                if not schema:
                    return ActionResult(success=False, message="schema is required")
                if not conn:
                    return ActionResult(success=False, message="Not connected to Snowflake")
                conn.cursor().execute(f"USE SCHEMA {schema}")
                return ActionResult(success=True, message=f"Using schema {schema}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Snowflake error: {str(e)}")


class SnowflakeQueryAction(BaseAction):
    """Execute SQL queries against Snowflake.

    Supports SELECT, INSERT, UPDATE, DELETE and stored procedure calls.
    """
    action_type = "snowflake_query"
    display_name = "Snowflake查询"
    description = "在Snowflake中执行SQL查询"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SQL on Snowflake.

        Args:
            context: Execution context.
            params: Dict with keys:
                - account: Snowflake account (if not using connection from context)
                - user: Username
                - password: Password
                - warehouse: Warehouse name
                - database: Database name
                - schema: Schema name
                - sql: SQL query string
                - bind_params: List of bind parameter values
                - fetch_size: Number of rows to fetch (default all)

        Returns:
            ActionResult with query results.
        """
        sql = params.get('sql')
        if not sql:
            return ActionResult(success=False, message="sql is required")

        try:
            import snowflake.connector
        except ImportError:
            return ActionResult(success=False, message="snowflake-connector-python not installed")

        conn = getattr(context, '_snowflake_conn', None)
        should_close = False

        try:
            if not conn:
                account = params.get('account') or os.environ.get('SNOWFLAKE_ACCOUNT')
                user = params.get('user') or os.environ.get('SNOWFLAKE_USER')
                password = params.get('password') or os.environ.get('SNOWFLAKE_PASSWORD')
                warehouse = params.get('warehouse') or os.environ.get('SNOWFLAKE_WAREHOUSE')
                database = params.get('database') or os.environ.get('SNOWFLAKE_DATABASE')
                schema = params.get('schema') or os.environ.get('SNOWFLAKE_SCHEMA')

                conn_params = {
                    'account': account,
                    'user': user,
                    'password': password,
                    'warehouse': warehouse,
                    'database': database,
                    'schema': schema,
                }
                conn = snowflake.connector.connect(**conn_params)
                should_close = True

            cursor = conn.cursor()

            if params.get('bind_params'):
                cursor.execute(sql, params['bind_params'])
            else:
                cursor.execute(sql)

            # Determine query type
            sql_upper = sql.strip().upper()
            if sql_upper.startswith('SELECT') or sql_upper.startswith('SHOW') or sql_upper.startswith('DESCRIBE'):
                if params.get('fetch_size'):
                    rows = cursor.fetchmany(params['fetch_size'])
                else:
                    rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                return ActionResult(
                    success=True,
                    message=f"Query returned {len(rows)} rows",
                    data={'columns': columns, 'rows': rows, 'row_count': len(rows)}
                )
            else:
                rowcount = cursor.rowcount
                return ActionResult(success=True, message=f"Query executed, {rowcount} rows affected", data={'rowcount': rowcount})

        except Exception as e:
            return ActionResult(success=False, message=f"Snowflake query error: {str(e)}")
        finally:
            if should_close and conn:
                conn.close()


class SnowflakeSnowpipeAction(BaseAction):
    """Manage Snowpipe continuous data ingestion.

    Handles pipe creation, listing, and manual ingestion triggers.
    """
    action_type = "snowflake_snowpipe"
    display_name = "Snowflake Snowpipe"
    description = "管理Snowflake Snowpipe持续数据摄取"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Snowpipe operations.

        Args:
            context: Execution context.
            params: Dict with keys:
                - account: Snowflake account
                - user: Username
                - password: Password
                - operation: create_pipe | list_pipes | trigger_ingest | get_ingest_status
                - pipe_name: Full pipe name (database.schema.pipe)
                - pipe_definition: CREATE PIPE SQL statement
                - stage_name: Stage name
                - table_name: Target table name
                - file_format: File format options

        Returns:
            ActionResult with snowpipe result.
        """
        try:
            import snowflake.connector
        except ImportError:
            return ActionResult(success=False, message="snowflake-connector-python not installed")

        account = params.get('account') or os.environ.get('SNOWFLAKE_ACCOUNT')
        user = params.get('user') or os.environ.get('SNOWFLAKE_USER')
        password = params.get('password') or os.environ.get('SNOWFLAKE_PASSWORD')
        warehouse = params.get('warehouse') or os.environ.get('SNOWFLAKE_WAREHOUSE')
        database = params.get('database') or os.environ.get('SNOWFLAKE_DATABASE')
        schema = params.get('schema') or os.environ.get('SNOWFLAKE_SCHEMA')

        operation = params.get('operation', 'list_pipes')

        conn = getattr(context, '_snowflake_conn', None)
        should_close = False

        try:
            if not conn:
                if not all([account, user, password]):
                    return ActionResult(success=False, message="account, user, and password are required")
                conn = snowflake.connector.connect(
                    account=account, user=user, password=password,
                    warehouse=warehouse, database=database, schema=schema
                )
                should_close = True

            cursor = conn.cursor()

            if operation == 'create_pipe':
                pipe_name = params.get('pipe_name')
                stage_name = params.get('stage_name')
                table_name = params.get('table_name')
                file_format = params.get('file_format', 'AUTO_DETECT')

                if not all([stage_name, table_name]):
                    return ActionResult(success=False, message="stage_name and table_name are required")

                sql = f"""CREATE OR REPLACE PIPE {pipe_name}
                AS COPY INTO {table_name}
                FROM @{stage_name}
                FILE_FORMAT = (TYPE = {file_format})"""

                cursor.execute(sql)
                return ActionResult(success=True, message=f"Pipe {pipe_name} created")

            elif operation == 'list_pipes':
                db = params.get('database') or database or ''
                sch = params.get('schema') or schema or ''
                query = f"SHOW PIPES IN {db}.{sch}" if db and sch else "SHOW PIPES"
                cursor.execute(query)
                pipes = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return ActionResult(
                    success=True,
                    message=f"Found {len(pipes)} pipes",
                    data={'columns': columns, 'pipes': pipes}
                )

            elif operation == 'trigger_ingest':
                pipe_name = params.get('pipe_name')
                if not pipe_name:
                    return ActionResult(success=False, message="pipe_name is required")

                sql = f"ALTER PIPE {pipe_name} REFRESH"
                cursor.execute(sql)
                return ActionResult(success=True, message=f"Ingest triggered for {pipe_name}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Snowpipe error: {str(e)}")
        finally:
            if should_close and conn:
                conn.close()


class SnowflakeStageAction(BaseAction):
    """Manage Snowflake stages (S3, Azure Blob, GCS).

    Handles stage creation, listing, and file operations.
    """
    action_type = "snowflake_stage"
    display_name = "Snowflake Stage"
    description = "管理Snowflake存储阶段(S3/Azure/GCS)"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Snowflake stages.

        Args:
            context: Execution context.
            params: Dict with keys:
                - account: Snowflake account
                - user: Username
                - password: Password
                - operation: create_stage | list_stages | list_files | put_file | remove_file
                - stage_name: Stage name
                - stage_type: S3 | AZURE | GCS | LOCAL
                - url: External stage URL (S3 bucket, Azure container, GCS bucket)
                - credentials: Dict with credentials for external stages
                - file_path: Local file path (for put_file)
                - remote_path: Path on stage (for put_file/remove_file)

        Returns:
            ActionResult with stage operation result.
        """
        try:
            import snowflake.connector
        except ImportError:
            return ActionResult(success=False, message="snowflake-connector-python not installed")

        account = params.get('account') or os.environ.get('SNOWFLAKE_ACCOUNT')
        user = params.get('user') or os.environ.get('SNOWFLAKE_USER')
        password = params.get('password') or os.environ.get('SNOWFLAKE_PASSWORD')
        warehouse = params.get('warehouse') or os.environ.get('SNOWFLAKE_WAREHOUSE')
        database = params.get('database') or os.environ.get('SNOWFLAKE_DATABASE')
        schema = params.get('schema') or os.environ.get('SNOWFLAKE_SCHEMA')

        operation = params.get('operation', 'list_stages')

        conn = getattr(context, '_snowflake_conn', None)
        should_close = False

        try:
            if not conn:
                if not all([account, user, password]):
                    return ActionResult(success=False, message="account, user, and password are required")
                conn = snowflake.connector.connect(
                    account=account, user=user, password=password,
                    warehouse=warehouse, database=database, schema=schema
                )
                should_close = True

            cursor = conn.cursor()

            if operation == 'create_stage':
                stage_name = params.get('stage_name')
                if not stage_name:
                    return ActionResult(success=False, message="stage_name is required")

                stage_type = params.get('stage_type', 'S3')
                url = params.get('url', '')
                credentials = params.get('credentials', {})

                if stage_type == 'S3':
                    sql = f"""CREATE OR REPLACE STAGE {stage_name}
                    URL = '{url}'
                    CREDENTIALS = (AWS_KEY_ID = '{credentials.get('aws_key_id', '')}' 
                                  AWS_SECRET_KEY = '{credentials.get('aws_secret_key', '')}')"""
                elif stage_type == 'AZURE':
                    sql = f"""CREATE OR REPLACE STAGE {stage_name}
                    URL = '{url}'
                    CREDENTIALS = (AZURE_SAS_TOKEN = '{credentials.get('azure_sas_token', '')}')"""
                elif stage_type == 'GCS':
                    sql = f"CREATE OR REPLACE STAGE {stage_name} URL = '{url}'"
                else:
                    sql = f"CREATE OR REPLACE STAGE {stage_name}"

                cursor.execute(sql)
                return ActionResult(success=True, message=f"Stage {stage_name} created")

            elif operation == 'list_stages':
                db = params.get('database') or database or ''
                sch = params.get('schema') or schema or ''
                query = f"SHOW STAGES IN {db}.{sch}" if db and sch else "SHOW STAGES"
                cursor.execute(query)
                stages = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return ActionResult(success=True, message=f"Found {len(stages)} stages", data={'stages': stages, 'columns': columns})

            elif operation == 'list_files':
                stage_name = params.get('stage_name')
                if not stage_name:
                    return ActionResult(success=False, message="stage_name is required")
                cursor.execute(f"LIST @{stage_name}")
                files = cursor.fetchall()
                return ActionResult(success=True, message=f"Found {len(files)} files", data={'files': files})

            elif operation == 'put_file':
                stage_name = params.get('stage_name')
                file_path = params.get('file_path')
                if not stage_name or not file_path:
                    return ActionResult(success=False, message="stage_name and file_path are required")
                cursor.execute(f"PUT file://{file_path} @{stage_name}")
                return ActionResult(success=True, message=f"File {file_path} uploaded to {stage_name}")

            elif operation == 'remove_file':
                stage_name = params.get('stage_name')
                remote_path = params.get('remote_path')
                if not stage_name or not remote_path:
                    return ActionResult(success=False, message="stage_name and remote_path are required")
                cursor.execute(f"REMOVE @{stage_name}/{remote_path}")
                return ActionResult(success=True, message=f"File removed from {stage_name}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Snowflake stage error: {str(e)}")
        finally:
            if should_close and conn:
                conn.close()

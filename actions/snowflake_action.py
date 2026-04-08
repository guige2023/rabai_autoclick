"""Snowflake action module for RabAI AutoClick.

Provides Snowflake data warehouse operations including
query execution, data loading, and account management.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class QueryResult:
    """Represents a Snowflake query result.
    
    Attributes:
        columns: List of column names.
        rows: List of row data.
        row_count: Number of rows returned.
        execution_time: Query execution time in seconds.
    """
    columns: List[str]
    rows: List[Dict[str, Any]]
    row_count: int = 0
    execution_time: float = 0.0


class SnowflakeClient:
    """Snowflake client for data warehouse operations.
    
    Provides methods for connecting to Snowflake, executing
    queries, and managing data loading operations.
    """
    
    def __init__(
        self,
        account: str = "",
        user: str = "",
        password: str = "",
        warehouse: str = "",
        database: str = "",
        schema: str = "",
        role: str = ""
    ) -> None:
        """Initialize Snowflake client.
        
        Args:
            account: Snowflake account identifier.
            user: Username.
            password: Password.
            warehouse: Default warehouse.
            database: Default database.
            schema: Default schema.
            role: Optional role.
        """
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self._conn: Optional[Any] = None
        self._cursor: Optional[Any] = None
    
    def connect(self) -> bool:
        """Connect to Snowflake.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import snowflake.connector
        except ImportError:
            raise ImportError(
                "snowflake-connector-python is required. Install with: pip install snowflake-connector-python"
            )
        
        try:
            self._conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                role=self.role
            )
            
            self._cursor = self._conn.cursor()
            
            self._cursor.execute("SELECT CURRENT_VERSION()")
            self._cursor.fetchone()
            
            return True
        
        except Exception:
            self._conn = None
            self._cursor = None
            return False
    
    def disconnect(self) -> None:
        """Disconnect from Snowflake."""
        if self._cursor:
            try:
                self._cursor.close()
            except Exception:
                pass
            self._cursor = None
        
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
    
    def execute_query(
        self,
        query: str,
        params: Optional[List[Any]] = None,
        timeout: int = 300
    ) -> QueryResult:
        """Execute a SQL query.
        
        Args:
            query: SQL query string.
            params: Optional query parameters.
            timeout: Query timeout in seconds.
            
        Returns:
            QueryResult with columns and rows.
        """
        if not self._cursor:
            raise RuntimeError("Not connected to Snowflake")
        
        try:
            start = time.time()
            
            if params:
                self._cursor.execute(query, params)
            else:
                self._cursor.execute(query)
            
            columns = [desc[0] for desc in self._cursor.description] if self._cursor.description else []
            rows = [dict(zip(columns, row)) for row in self._cursor.fetchall()]
            
            execution_time = time.time() - start
            
            return QueryResult(
                columns=columns,
                rows=rows,
                row_count=len(rows),
                execution_time=execution_time
            )
        
        except Exception as e:
            raise Exception(f"Query execution failed: {str(e)}")
    
    def execute_many(
        self,
        query: str,
        params_list: List[List[Any]]
    ) -> bool:
        """Execute a query with multiple parameter sets.
        
        Args:
            query: SQL query string with placeholders.
            params_list: List of parameter lists.
            
        Returns:
            True if successful.
        """
        if not self._cursor:
            raise RuntimeError("Not connected to Snowflake")
        
        try:
            self._cursor.executemany(query, params_list)
            return True
        
        except Exception as e:
            raise Exception(f"Execute many failed: {str(e)}")
    
    def list_databases(self) -> List[Dict[str, Any]]:
        """List all databases.
        
        Returns:
            List of database information.
        """
        result = self.execute_query("SHOW DATABASES")
        return result.rows
    
    def list_schemas(self, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all schemas in a database.
        
        Args:
            database: Optional database name (uses default if not provided).
            
        Returns:
            List of schema information.
        """
        db = database or self.database
        result = self.execute_query(f"SHOW SCHEMAS IN DATABASE {db}")
        return result.rows
    
    def list_tables(
        self,
        schema: Optional[str] = None,
        database: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List all tables in a schema.
        
        Args:
            schema: Optional schema name.
            database: Optional database name.
            
        Returns:
            List of table information.
        """
        schema_name = schema or self.schema
        db = database or self.database
        
        if not schema_name or not db:
            raise ValueError("schema and database are required")
        
        result = self.execute_query(f"SHOW TABLES IN SCHEMA {db}.{schema_name}")
        return result.rows
    
    def describe_table(
        self,
        table_name: str,
        schema: Optional[str] = None,
        database: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Describe a table's columns.
        
        Args:
            table_name: Table name.
            schema: Optional schema name.
            database: Optional database name.
            
        Returns:
            List of column definitions.
        """
        schema_name = schema or self.schema
        db = database or self.database
        
        if not schema_name or not db:
            raise ValueError("schema and database are required")
        
        result = self.execute_query(f"DESCRIBE TABLE {db}.{schema_name}.{table_name}")
        return result.rows
    
    def get_current_warehouse(self) -> str:
        """Get the current warehouse name.
        
        Returns:
            Warehouse name.
        """
        result = self.execute_query("SELECT CURRENT_WAREHOUSE() as WAREHOUSE")
        return result.rows[0].get("WAREHOUSE", "") if result.rows else ""
    
    def get_current_role(self) -> str:
        """Get the current role name.
        
        Returns:
            Role name.
        """
        result = self.execute_query("SELECT CURRENT_ROLE() as ROLE")
        return result.rows[0].get("ROLE", "") if result.rows else ""
    
    def use_warehouse(self, warehouse: str) -> bool:
        """Change the current warehouse.
        
        Args:
            warehouse: Warehouse name.
            
        Returns:
            True if successful.
        """
        try:
            self.execute_query(f"USE WAREHOUSE {warehouse}")
            self.warehouse = warehouse
            return True
        except Exception:
            return False
    
    def use_database(self, database: str) -> bool:
        """Change the current database.
        
        Args:
            database: Database name.
            
        Returns:
            True if successful.
        """
        try:
            self.execute_query(f"USE DATABASE {database}")
            self.database = database
            return True
        except Exception:
            return False
    
    def use_schema(self, schema: str, database: Optional[str] = None) -> bool:
        """Change the current schema.
        
        Args:
            schema: Schema name.
            database: Optional database name.
            
        Returns:
            True if successful.
        """
        try:
            if database:
                self.execute_query(f"USE SCHEMA {database}.{schema}")
            else:
                self.execute_query(f"USE SCHEMA {schema}")
            self.schema = schema
            return True
        except Exception:
            return False
    
    def create_database(self, name: str) -> bool:
        """Create a new database.
        
        Args:
            name: Database name.
            
        Returns:
            True if successful.
        """
        try:
            self.execute_query(f"CREATE DATABASE {name}")
            return True
        except Exception:
            return False
    
    def create_schema(
        self,
        name: str,
        database: Optional[str] = None
    ) -> bool:
        """Create a new schema.
        
        Args:
            name: Schema name.
            database: Optional database name.
            
        Returns:
            True if successful.
        """
        try:
            db = database or self.database
            if not db:
                raise ValueError("database is required")
            self.execute_query(f"CREATE SCHEMA {db}.{name}")
            return True
        except Exception:
            return False
    
    def copy_into(
        self,
        table: str,
        path: str,
        file_format: str = "auto",
        pattern: Optional[str] = None,
        schema: Optional[str] = None,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Load data from files into a table using COPY INTO.
        
        Args:
            table: Target table name.
            path: Staged file path or S3/Azure/GCS path.
            file_format: File format ('auto', 'csv', 'json', etc.).
            pattern: Optional file pattern regex.
            schema: Optional schema name.
            database: Optional database name.
            
        Returns:
            Copy operation result statistics.
        """
        schema_name = schema or self.schema
        db = database or self.database
        
        if not schema_name or not db:
            raise ValueError("schema and database are required")
        
        full_table = f"{db}.{schema_name}.{table}"
        
        copy_sql = f"COPY INTO {full_table} FROM @{path}"
        
        if file_format != "auto":
            copy_sql += f" FILE_FORMAT = '{file_format}'"
        
        if pattern:
            copy_sql += f" PATTERN = '{pattern}'"
        
        result = self.execute_query(copy_sql)
        return {
            "rows_loaded": result.row_count,
            "rows": result.rows
        }
    
    def get_query_history(
        self,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get recent query history.
        
        Args:
            limit: Maximum number of queries to return.
            
        Returns:
            List of query history records.
        """
        query = f"""
        SELECT QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME, USER_NAME,
               START_TIME, EXECUTION_TIME, STATUS, ROWS_PRODUCED
        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
        ORDER BY START_TIME DESC
        LIMIT {limit}
        """
        result = self.execute_query(query)
        return result.rows
    
    def get_server_info(self) -> Dict[str, Any]:
        """Get Snowflake account information.
        
        Returns:
            Account information dictionary.
        """
        if not self._cursor:
            raise RuntimeError("Not connected to Snowflake")
        
        try:
            self._cursor.execute("SELECT CURRENT_VERSION() as VERSION")
            version = self._cursor.fetchone()[0]
            
            self._cursor.execute("SELECT CURRENT_ACCOUNT() as ACCOUNT")
            account = self._cursor.fetchone()[0]
            
            return {
                "account": account,
                "version": version,
                "warehouse": self.get_current_warehouse(),
                "role": self.get_current_role(),
                "database": self.database,
                "schema": self.schema
            }
        
        except Exception as e:
            raise Exception(f"Get server info failed: {str(e)}")


class SnowflakeAction(BaseAction):
    """Snowflake action for data warehouse operations.
    
    Supports SQL queries, data loading, and account management.
    """
    action_type: str = "snowflake"
    display_name: str = "Snowflake动作"
    description: str = "Snowflake数据仓库查询和数据加载操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[SnowflakeClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Snowflake operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "disconnect":
                return self._disconnect(start_time)
            elif operation == "query":
                return self._query(params, start_time)
            elif operation == "execute_many":
                return self._execute_many(params, start_time)
            elif operation == "list_databases":
                return self._list_databases(start_time)
            elif operation == "list_schemas":
                return self._list_schemas(params, start_time)
            elif operation == "list_tables":
                return self._list_tables(params, start_time)
            elif operation == "describe_table":
                return self._describe_table(params, start_time)
            elif operation == "use_warehouse":
                return self._use_warehouse(params, start_time)
            elif operation == "use_database":
                return self._use_database(params, start_time)
            elif operation == "use_schema":
                return self._use_schema(params, start_time)
            elif operation == "create_database":
                return self._create_database(params, start_time)
            elif operation == "create_schema":
                return self._create_schema(params, start_time)
            elif operation == "copy_into":
                return self._copy_into(params, start_time)
            elif operation == "query_history":
                return self._query_history(params, start_time)
            elif operation == "server_info":
                return self._server_info(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except ImportError as e:
            return ActionResult(
                success=False,
                message=f"Import error: {str(e)}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Snowflake operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Snowflake."""
        account = params.get("account", "")
        user = params.get("user", "")
        password = params.get("password", "")
        warehouse = params.get("warehouse", "")
        database = params.get("database", "")
        schema = params.get("schema", "")
        role = params.get("role", "")
        
        if not account or not user or not password:
            return ActionResult(
                success=False,
                message="account, user, and password are required",
                duration=time.time() - start_time
            )
        
        self._client = SnowflakeClient(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role
        )
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to Snowflake account {account}" if success else "Failed to connect",
            duration=time.time() - start_time
        )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from Snowflake."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(
            success=True,
            message="Disconnected from Snowflake",
            duration=time.time() - start_time
        )
    
    def _query(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a SQL query."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        query = params.get("query", "")
        if not query:
            return ActionResult(success=False, message="query is required", duration=time.time() - start_time)
        
        try:
            result = self._client.execute_query(query)
            return ActionResult(
                success=True,
                message=f"Query returned {result.row_count} rows in {result.execution_time:.2f}s",
                data={
                    "columns": result.columns,
                    "rows": result.rows,
                    "row_count": result.row_count,
                    "execution_time": result.execution_time
                },
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _execute_many(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a query with multiple parameter sets."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        query = params.get("query", "")
        params_list = params.get("params_list", [])
        
        if not query or not params_list:
            return ActionResult(success=False, message="query and params_list are required", duration=time.time() - start_time)
        
        try:
            success = self._client.execute_many(query, params_list)
            return ActionResult(
                success=success,
                message=f"Executed {len(params_list)} batches",
                data={"batches": len(params_list)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_databases(self, start_time: float) -> ActionResult:
        """List all databases."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            databases = self._client.list_databases()
            return ActionResult(
                success=True,
                message=f"Found {len(databases)} databases",
                data={"databases": databases, "count": len(databases)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_schemas(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all schemas in a database."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        database = params.get("database")
        
        try:
            schemas = self._client.list_schemas(database=database)
            return ActionResult(
                success=True,
                message=f"Found {len(schemas)} schemas",
                data={"schemas": schemas, "count": len(schemas)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_tables(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all tables in a schema."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        schema = params.get("schema")
        database = params.get("database")
        
        try:
            tables = self._client.list_tables(schema=schema, database=database)
            return ActionResult(
                success=True,
                message=f"Found {len(tables)} tables",
                data={"tables": tables, "count": len(tables)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _describe_table(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Describe a table's columns."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        table_name = params.get("table_name", "")
        schema = params.get("schema")
        database = params.get("database")
        
        if not table_name:
            return ActionResult(success=False, message="table_name is required", duration=time.time() - start_time)
        
        try:
            columns = self._client.describe_table(
                table_name=table_name,
                schema=schema,
                database=database
            )
            return ActionResult(
                success=True,
                message=f"Table has {len(columns)} columns",
                data={"columns": columns, "count": len(columns)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _use_warehouse(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Change the current warehouse."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        warehouse = params.get("warehouse", "")
        if not warehouse:
            return ActionResult(success=False, message="warehouse is required", duration=time.time() - start_time)
        
        try:
            success = self._client.use_warehouse(warehouse)
            return ActionResult(
                success=success,
                message=f"Changed warehouse to {warehouse}" if success else "Change warehouse failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _use_database(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Change the current database."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        database = params.get("database", "")
        if not database:
            return ActionResult(success=False, message="database is required", duration=time.time() - start_time)
        
        try:
            success = self._client.use_database(database)
            return ActionResult(
                success=success,
                message=f"Changed database to {database}" if success else "Change database failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _use_schema(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Change the current schema."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        schema = params.get("schema", "")
        database = params.get("database")
        
        if not schema:
            return ActionResult(success=False, message="schema is required", duration=time.time() - start_time)
        
        try:
            success = self._client.use_schema(schema, database)
            return ActionResult(
                success=success,
                message=f"Changed schema to {schema}" if success else "Change schema failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_database(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new database."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.create_database(name)
            return ActionResult(
                success=success,
                message=f"Created database: {name}" if success else "Create database failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_schema(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new schema."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        name = params.get("name", "")
        database = params.get("database")
        
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.create_schema(name, database)
            return ActionResult(
                success=success,
                message=f"Created schema: {name}" if success else "Create schema failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _copy_into(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Load data into a table using COPY INTO."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        table = params.get("table", "")
        path = params.get("path", "")
        
        if not table or not path:
            return ActionResult(success=False, message="table and path are required", duration=time.time() - start_time)
        
        try:
            result = self._client.copy_into(
                table=table,
                path=path,
                file_format=params.get("file_format", "auto"),
                pattern=params.get("pattern"),
                schema=params.get("schema"),
                database=params.get("database")
            )
            return ActionResult(
                success=True,
                message=f"Loaded {result.get('rows_loaded', 0)} rows",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _query_history(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get recent query history."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        limit = params.get("limit", 10)
        
        try:
            history = self._client.get_query_history(limit=limit)
            return ActionResult(
                success=True,
                message=f"Found {len(history)} queries",
                data={"history": history, "count": len(history)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _server_info(self, start_time: float) -> ActionResult:
        """Get Snowflake account information."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            info = self._client.get_server_info()
            return ActionResult(
                success=True,
                message=f"Snowflake {info.get('version', '')}",
                data=info,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)

"""InfluxDB action module for RabAI AutoClick.

Provides InfluxDB time-series database operations including
writing data points, querying, and database management.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class DataPoint:
    """Represents a single InfluxDB data point.
    
    Attributes:
        measurement: Measurement name.
        tags: Optional tags for the point.
        fields: Fields (values) for the point.
        timestamp: Optional timestamp (unix nanoseconds).
    """
    measurement: str
    fields: Dict[str, Union[int, float, str, bool]]
    tags: Dict[str, str] = field(default_factory=dict)
    timestamp: Optional[int] = None


class InfluxDBClient:
    """InfluxDB client wrapper for time-series operations.
    
    Provides methods for connecting to InfluxDB, writing data points,
    and executing queries.
    """
    
    def __init__(
        self,
        url: str = "http://localhost:8086",
        token: str = "",
        org: str = "",
        timeout: int = 30
    ) -> None:
        """Initialize InfluxDB client.
        
        Args:
            url: InfluxDB server URL.
            token: Authentication token.
            org: Organization name.
            timeout: Connection timeout in seconds.
        """
        self.url = url
        self.token = token
        self.org = org
        self.timeout = timeout
        self._client: Optional[Any] = None
    
    def connect(self) -> bool:
        """Connect to InfluxDB server.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            from influxdb_client import InfluxDBClient
        except ImportError:
            raise ImportError(
                "influxdb-client is required. Install with: pip install influxdb-client"
            )
        
        try:
            self._client = InfluxDBClient(
                url=self.url,
                token=self.token,
                org=self.org,
                timeout=self.timeout * 1000
            )
            
            health = self._client.health()
            return health.status == "pass"
        
        except Exception:
            self._client = None
            return False
    
    def disconnect(self) -> None:
        """Disconnect from InfluxDB server."""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
    
    def write_points(
        self,
        bucket: str,
        points: List[DataPoint],
        precision: str = "ns"
    ) -> bool:
        """Write data points to InfluxDB.
        
        Args:
            bucket: Target bucket name.
            points: List of DataPoint objects.
            precision: Timestamp precision ('ns', 'us', 'ms', 's').
            
        Returns:
            True if write successful.
        """
        if not self._client:
            raise RuntimeError("Not connected to InfluxDB")
        
        try:
            from influxdb_client import Point
            from influxdb_client.client.write_api import SYNCHRONOUS
            
            write_api = self._client.write_api(write_options=SYNCHRONOUS)
            
            influx_points = []
            for pt in points:
                point = Point(pt.measurement)
                
                for key, value in pt.fields.items():
                    point.field(key, value)
                
                for key, value in pt.tags.items():
                    point.tag(key, value)
                
                if pt.timestamp:
                    point.time(pt.timestamp, precision)
                
                influx_points.append(point)
            
            write_api.write(bucket=bucket, org=self.org, record=influx_points)
            write_api.close()
            
            return True
        
        except Exception as e:
            raise Exception(f"Write failed: {str(e)}")
    
    def query(
        self,
        query_str: str,
        bucket: str = "",
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a Flux query.
        
        Args:
            query_str: Flux query string.
            bucket: Optional bucket override.
            params: Optional query parameters.
            
        Returns:
            List of result records.
        """
        if not self._client:
            raise RuntimeError("Not connected to InfluxDB")
        
        try:
            from influxdb_client.client.flux_table import FluxTable
            
            query_api = self._client.query_api()
            
            if bucket:
                tables = query_api.query(query_str, bucket=bucket, params=params)
            else:
                tables = query_api.query(query_str, params=params)
            
            results: List[Dict[str, Any]] = []
            
            for table in tables:
                for record in table.records:
                    results.append({
                        "measurement": record.get_measurement(),
                        "field": record.get_field(),
                        "value": record.get_value(),
                        "time": record.get_time(),
                        "tags": dict(record.values) if hasattr(record, 'values') else {}
                    })
            
            return results
        
        except Exception as e:
            raise Exception(f"Query failed: {str(e)}")
    
    def query_range(
        self,
        measurement: str,
        start: str = "-1h",
        end: str = "now()",
        bucket: str = "",
        fields: Optional[List[str]] = None,
        where_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query a measurement over a time range.
        
        Args:
            measurement: Measurement name.
            start: Start time (Flux duration or timestamp).
            end: End time (Flux duration or timestamp).
            bucket: Bucket name.
            fields: Optional list of fields to select.
            where_filter: Optional additional filter.
            
        Returns:
            List of data records.
        """
        field_str = ", ".join(fields) if fields else "*"
        
        query = f'''
        from(bucket: "{bucket}")
            |> range(start: {start}, stop: {end})
            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
        '''
        
        if fields:
            query += f'\n            |> filter(fn: (r) => contains(value: r["_field"], set: [{", ".join(f\'"{f}\"' for f in fields)}]))'
        
        if where_filter:
            query += f'\n            |> filter(fn: (r) => {where_filter})'
        
        query += '\n            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
        
        return self.query(query, bucket=bucket)
    
    def create_bucket(
        self,
        name: str,
        description: str = "",
        retention_rules: Optional[List[Dict[str, Any]]] = None
    ) -> bool:
        """Create a new bucket.
        
        Args:
            name: Bucket name.
            description: Optional description.
            retention_rules: Optional retention rules.
            
        Returns:
            True if created successfully.
        """
        if not self._client:
            raise RuntimeError("Not connected to InfluxDB")
        
        try:
            from influxdb_client import Bucket, BucketRetentionRules
            
            buckets_api = self._client.buckets_api()
            
            retention = []
            if retention_rules:
                for rule in retention_rules:
                    r = BucketRetentionRules(
                        every_seconds=rule.get("every_seconds", 0),
                        shard_group_duration_seconds=rule.get("shard_group_duration_seconds", 0)
                    )
                    retention.append(r)
            
            bucket = Bucket(
                name=name,
                org=self.org,
                description=description,
                retention_rules=retention or None
            )
            
            buckets_api.create_bucket(bucket=bucket)
            return True
        
        except Exception as e:
            raise Exception(f"Create bucket failed: {str(e)}")
    
    def delete_bucket(self, bucket: str) -> bool:
        """Delete a bucket.
        
        Args:
            bucket: Bucket name to delete.
            
        Returns:
            True if deleted successfully.
        """
        if not self._client:
            raise RuntimeError("Not connected to InfluxDB")
        
        try:
            buckets_api = self._client.buckets_api()
            bucket_obj = buckets_api.find_bucket_by_name(bucket)
            
            if bucket_obj:
                buckets_api.delete_bucket(bucket_obj)
                return True
            
            return False
        
        except Exception as e:
            raise Exception(f"Delete bucket failed: {str(e)}")
    
    def list_buckets(self) -> List[Dict[str, Any]]:
        """List all buckets.
        
        Returns:
            List of bucket information dictionaries.
        """
        if not self._client:
            raise RuntimeError("Not connected to InfluxDB")
        
        try:
            buckets_api = self._client.buckets_api()
            buckets = buckets_api.find_buckets().buckets
            
            return [
                {
                    "name": b.name,
                    "id": b.id,
                    "org_id": b.org_id,
                    "retention_rules": [
                        {"every_seconds": r.every_seconds}
                        for r in b.retention_rules
                    ] if b.retention_rules else []
                }
                for b in buckets
            ]
        
        except Exception as e:
            raise Exception(f"List buckets failed: {str(e)}")
    
    def list_measurements(self, bucket: str) -> List[str]:
        """List all measurements in a bucket.
        
        Args:
            bucket: Bucket name.
            
        Returns:
            List of measurement names.
        """
        query = f'''
        import "influxdata/influxdb/schema"
        schema.measurements(bucket: "{bucket}")
        '''
        
        results = self.query(query, bucket=bucket)
        return list(set(r.get("value", "") for r in results if r.get("value")))
    
    def get_server_info(self) -> Dict[str, Any]:
        """Get InfluxDB server information.
        
        Returns:
            Server information dictionary.
        """
        if not self._client:
            raise RuntimeError("Not connected to InfluxDB")
        
        try:
            health = self._client.health()
            version = self._client.version()
            
            return {
                "status": health.status,
                "version": version,
                "url": self.url,
                "org": self.org
            }
        
        except Exception as e:
            raise Exception(f"Get server info failed: {str(e)}")


class InfluxDBAction(BaseAction):
    """InfluxDB action for time-series data operations.
    
    Supports writing data points, querying, and bucket management.
    """
    action_type: str = "influxdb"
    display_name: str = "InfluxDB动作"
    description: str = "InfluxDB时序数据库操作，数据写入和查询"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[InfluxDBClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute InfluxDB operation.
        
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
            elif operation == "write":
                return self._write_points(params, start_time)
            elif operation == "query":
                return self._query(params, start_time)
            elif operation == "query_range":
                return self._query_range(params, start_time)
            elif operation == "create_bucket":
                return self._create_bucket(params, start_time)
            elif operation == "delete_bucket":
                return self._delete_bucket(params, start_time)
            elif operation == "list_buckets":
                return self._list_buckets(start_time)
            elif operation == "list_measurements":
                return self._list_measurements(params, start_time)
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
                message=f"InfluxDB operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to InfluxDB server."""
        url = params.get("url", "http://localhost:8086")
        token = params.get("token", "")
        org = params.get("org", "")
        
        self._client = InfluxDBClient(url=url, token=token, org=org)
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to InfluxDB at {url}" if success else "Failed to connect",
            duration=time.time() - start_time
        )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from InfluxDB server."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(
            success=True,
            message="Disconnected from InfluxDB",
            duration=time.time() - start_time
        )
    
    def _write_points(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Write data points to InfluxDB."""
        if not self._client:
            return ActionResult(
                success=False,
                message="Not connected to InfluxDB",
                duration=time.time() - start_time
            )
        
        bucket = params.get("bucket", "")
        points_data = params.get("points", [])
        precision = params.get("precision", "ns")
        
        if not bucket:
            return ActionResult(
                success=False,
                message="bucket is required",
                duration=time.time() - start_time
            )
        
        if not points_data:
            return ActionResult(
                success=False,
                message="points are required",
                duration=time.time() - start_time
            )
        
        try:
            points = []
            for pt in points_data:
                point = DataPoint(
                    measurement=pt.get("measurement", ""),
                    fields=pt.get("fields", {}),
                    tags=pt.get("tags", {}),
                    timestamp=pt.get("timestamp")
                )
                points.append(point)
            
            self._client.write_points(bucket=bucket, points=points, precision=precision)
            
            return ActionResult(
                success=True,
                message=f"Wrote {len(points)} points to {bucket}",
                data={"points_written": len(points), "bucket": bucket},
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Write failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _query(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a Flux query."""
        if not self._client:
            return ActionResult(
                success=False,
                message="Not connected to InfluxDB",
                duration=time.time() - start_time
            )
        
        query_str = params.get("query", "")
        bucket = params.get("bucket", "")
        
        if not query_str:
            return ActionResult(
                success=False,
                message="query is required",
                duration=time.time() - start_time
            )
        
        try:
            results = self._client.query(query_str, bucket=bucket)
            
            return ActionResult(
                success=True,
                message=f"Query returned {len(results)} records",
                data={"results": results, "count": len(results)},
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Query failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _query_range(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Query a measurement over a time range."""
        if not self._client:
            return ActionResult(
                success=False,
                message="Not connected to InfluxDB",
                duration=time.time() - start_time
            )
        
        measurement = params.get("measurement", "")
        bucket = params.get("bucket", "")
        start = params.get("start", "-1h")
        end = params.get("end", "now()")
        fields = params.get("fields")
        where_filter = params.get("where_filter")
        
        if not measurement or not bucket:
            return ActionResult(
                success=False,
                message="measurement and bucket are required",
                duration=time.time() - start_time
            )
        
        try:
            results = self._client.query_range(
                measurement=measurement,
                start=start,
                end=end,
                bucket=bucket,
                fields=fields,
                where_filter=where_filter
            )
            
            return ActionResult(
                success=True,
                message=f"Query range returned {len(results)} records",
                data={"results": results, "count": len(results)},
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Query range failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _create_bucket(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new bucket."""
        if not self._client:
            return ActionResult(
                success=False,
                message="Not connected to InfluxDB",
                duration=time.time() - start_time
            )
        
        name = params.get("name", "")
        description = params.get("description", "")
        retention_rules = params.get("retention_rules")
        
        if not name:
            return ActionResult(
                success=False,
                message="name is required",
                duration=time.time() - start_time
            )
        
        try:
            self._client.create_bucket(
                name=name,
                description=description,
                retention_rules=retention_rules
            )
            
            return ActionResult(
                success=True,
                message=f"Created bucket: {name}",
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Create bucket failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _delete_bucket(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a bucket."""
        if not self._client:
            return ActionResult(
                success=False,
                message="Not connected to InfluxDB",
                duration=time.time() - start_time
            )
        
        bucket = params.get("bucket", "")
        
        if not bucket:
            return ActionResult(
                success=False,
                message="bucket is required",
                duration=time.time() - start_time
            )
        
        try:
            success = self._client.delete_bucket(bucket=bucket)
            
            return ActionResult(
                success=success,
                message=f"Deleted bucket: {bucket}" if success else f"Bucket not found: {bucket}",
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Delete bucket failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _list_buckets(self, start_time: float) -> ActionResult:
        """List all buckets."""
        if not self._client:
            return ActionResult(
                success=False,
                message="Not connected to InfluxDB",
                duration=time.time() - start_time
            )
        
        try:
            buckets = self._client.list_buckets()
            
            return ActionResult(
                success=True,
                message=f"Found {len(buckets)} buckets",
                data={"buckets": buckets, "count": len(buckets)},
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"List buckets failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _list_measurements(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all measurements in a bucket."""
        if not self._client:
            return ActionResult(
                success=False,
                message="Not connected to InfluxDB",
                duration=time.time() - start_time
            )
        
        bucket = params.get("bucket", "")
        
        if not bucket:
            return ActionResult(
                success=False,
                message="bucket is required",
                duration=time.time() - start_time
            )
        
        try:
            measurements = self._client.list_measurements(bucket=bucket)
            
            return ActionResult(
                success=True,
                message=f"Found {len(measurements)} measurements",
                data={"measurements": measurements, "count": len(measurements)},
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"List measurements failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _server_info(self, start_time: float) -> ActionResult:
        """Get InfluxDB server information."""
        if not self._client:
            return ActionResult(
                success=False,
                message="Not connected to InfluxDB",
                duration=time.time() - start_time
            )
        
        try:
            info = self._client.get_server_info()
            
            return ActionResult(
                success=True,
                message=f"InfluxDB {info.get('version')} at {info.get('url')}",
                data=info,
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Get server info failed: {str(e)}",
                duration=time.time() - start_time
            )

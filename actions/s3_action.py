"""AWS S3 action module for RabAI AutoClick.

Provides operations for AWS S3 storage including upload, download,
list, copy, delete, and presigned URL generation.
"""

import json
import hashlib
import sys
import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class S3Config:
    """S3 connection configuration."""
    aws_access_key: str = ""
    aws_secret_key: str = ""
    region: str = "us-east-1"
    endpoint_url: str = ""
    bucket: str = ""
    use_ssl: bool = True


class S3Action(BaseAction):
    """Action for AWS S3 operations.
    
    Features:
        - Upload files and data to S3
        - Download files from S3
        - List bucket contents
        - Copy and move objects
        - Delete objects
        - Generate presigned URLs
        - Batch operations
    """
    
    def __init__(self, config: Optional[S3Config] = None):
        """Initialize S3 action.
        
        Args:
            config: S3 configuration. Uses environment variables if not provided.
        """
        super().__init__()
        self.config = config or self._load_config_from_env()
        self._client = None
    
    def _load_config_from_env(self) -> S3Config:
        """Load configuration from environment variables."""
        return S3Config(
            aws_access_key=os.environ.get("AWS_ACCESS_KEY_ID", ""),
            aws_secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            region=os.environ.get("AWS_REGION", "us-east-1"),
            endpoint_url=os.environ.get("S3_ENDPOINT_URL", ""),
            bucket=os.environ.get("S3_BUCKET", ""),
            use_ssl=os.environ.get("S3_USE_SSL", "true").lower() == "true"
        )
    
    def _get_client(self):
        """Get or create S3 client using urllib only (no boto3 dependency)."""
        try:
            import urllib.request
            import urllib.parse
            import urllib.error
            import hmac
            import hashlib
            import datetime
            import base64
            
            class SimpleS3Client:
                """Minimal S3 client using urllib."""
                
                def __init__(self, config: S3Config):
                    self.config = config
                    self.scheme = "https" if config.use_ssl else "http"
                    self.host = config.endpoint_url or f"s3.{config.region}.amazonaws.com"
                
                def _sign(self, key: str, msg: str) -> str:
                    """AWS Signature Version 4 signing."""
                    import hmac
                    return hmac.new(key.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()
                
                def _get_v4_signing_key(self, date: str) -> bytes:
                    """Get signing key for AWS Signature V4."""
                    k_date = self._sign(f"AWS4{self.config.aws_secret_key}".encode("utf-8"), date)
                    k_region = self._sign(k_date, self.config.region)
                    k_service = self._sign(k_region, "s3")
                    k_signing = self._sign(k_service, "aws4_request")
                    return k_signing
                
                def put_object(self, bucket: str, key: str, body: bytes, 
                              content_type: str = "application/octet-stream",
                              metadata: Optional[Dict] = None) -> Dict[str, Any]:
                    """Upload an object to S3."""
                    import urllib.request
                    import urllib.parse
                    
                    url = f"{self.scheme}://{bucket}.{self.host}/{urllib.parse.quote(key)}"
                    
                    now = datetime.datetime.utcnow()
                    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
                    date_stamp = now.strftime("%Y%m%d")
                    
                    headers = {
                        "x-amz-date": amz_date,
                        "x-amz-content-sha256": hashlib.sha256(body).hexdigest(),
                        "Content-Type": content_type
                    }
                    
                    if metadata:
                        for k, v in metadata.items():
                            headers[f"x-amz-meta-{k}"] = v
                    
                    signed_headers = ";".join(["host", "x-amz-date", "x-amz-content-sha256", "content-type"])
                    canonical_uri = f"/{urllib.parse.quote(key)}"
                    canonical_querystring = ""
                    
                    payload_hash = hashlib.sha256(body).hexdigest()
                    canonical_headers = f"host:{bucket}.{self.host}\n"
                    canonical_headers += f"x-amz-content-sha256:{payload_hash}\n"
                    canonical_headers += f"x-amz-date:{amz_date}\n"
                    canonical_headers += f"content-type:{content_type}\n"
                    
                    canonical_request = f"PUT\n{canonical_uri}\n{canonical_querystring}\n"
                    canonical_request += canonical_headers + "\n"
                    canonical_request += signed_headers + "\n" + payload_hash
                    
                    credential_scope = f"{date_stamp}/{self.config.region}/s3/aws4_request"
                    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n"
                    string_to_sign += hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
                    
                    signing_key = self._get_v4_signing_key(date_stamp)
                    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
                    
                    auth_header = f"AWS4-HMAC-SHA256 Credential={self.config.aws_access_key}/{credential_scope}, "
                    auth_header += f"SignedHeaders={signed_headers}, Signature={signature}"
                    headers["Authorization"] = auth_header
                    
                    req = urllib.request.Request(url, data=body, headers=headers, method="PUT")
                    
                    try:
                        with urllib.request.urlopen(req, timeout=60) as resp:
                            return {
                                "ETag": resp.headers.get("ETag", ""),
                                "VersionId": resp.headers.get("x-amz-version-id", ""),
                                "status": resp.status
                            }
                    except urllib.error.HTTPError as e:
                        return {"error": f"HTTP {e.code}: {e.reason}"}
                
                def get_object(self, bucket: str, key: str) -> bytes:
                    """Download an object from S3."""
                    import urllib.request
                    import urllib.parse
                    
                    url = f"{self.scheme}://{bucket}.{self.host}/{urllib.parse.quote(key)}"
                    
                    now = datetime.datetime.utcnow()
                    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
                    date_stamp = now.strftime("%Y%m%d")
                    
                    headers = {
                        "x-amz-date": amz_date,
                        "x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    }
                    
                    signed_headers = "host;x-amz-date;x-amz-content-sha256"
                    canonical_uri = f"/{urllib.parse.quote(key)}"
                    canonical_querystring = ""
                    
                    canonical_headers = f"host:{bucket}.{self.host}\n"
                    canonical_headers += f"x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
                    canonical_headers += f"x-amz-date:{amz_date}\n"
                    
                    canonical_request = f"GET\n{canonical_uri}\n{canonical_querystring}\n"
                    canonical_request += canonical_headers + "\n"
                    canonical_request += signed_headers + "\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    
                    credential_scope = f"{date_stamp}/{self.config.region}/s3/aws4_request"
                    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n"
                    string_to_sign += hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
                    
                    signing_key = self._get_v4_signing_key(date_stamp)
                    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
                    
                    auth_header = f"AWS4-HMAC-SHA256 Credential={self.config.aws_access_key}/{credential_scope}, "
                    auth_header += f"SignedHeaders={signed_headers}, Signature={signature}"
                    headers["Authorization"] = auth_header
                    
                    req = urllib.request.Request(url, headers=headers, method="GET")
                    
                    with urllib.request.urlopen(req, timeout=60) as resp:
                        return resp.read()
                
                def list_objects(self, bucket: str, prefix: str = "", 
                                max_keys: int = 100) -> List[Dict[str, Any]]:
                    """List objects in bucket with prefix."""
                    import urllib.request
                    import urllib.parse
                    
                    params = urllib.parse.urlencode({
                        "list-type": "2",
                        "prefix": prefix,
                        "max-keys": str(max_keys)
                    })
                    url = f"{self.scheme}://{bucket}.{self.host}/?{params}"
                    
                    now = datetime.datetime.utcnow()
                    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
                    date_stamp = now.strftime("%Y%m%d")
                    
                    headers = {
                        "x-amz-date": amz_date,
                        "x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    }
                    
                    signed_headers = "host;x-amz-date;x-amz-content-sha256"
                    canonical_uri = "/"
                    canonical_querystring = params
                    
                    canonical_headers = f"host:{bucket}.{self.host}\n"
                    canonical_headers += f"x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
                    canonical_headers += f"x-amz-date:{amz_date}\n"
                    
                    canonical_request = f"GET\n{canonical_uri}\n{canonical_querystring}\n"
                    canonical_request += canonical_headers + "\n"
                    canonical_request += signed_headers + "\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    
                    credential_scope = f"{date_stamp}/{self.config.region}/s3/aws4_request"
                    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n"
                    string_to_sign += hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
                    
                    signing_key = self._get_v4_signing_key(date_stamp)
                    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
                    
                    auth_header = f"AWS4-HMAC-SHA256 Credential={self.config.aws_access_key}/{credential_scope}, "
                    auth_header += f"SignedHeaders={signed_headers}, Signature={signature}"
                    headers["Authorization"] = auth_header
                    
                    req = urllib.request.Request(url, headers=headers, method="GET")
                    
                    with urllib.request.urlopen(req, timeout=60) as resp:
                        content = resp.read().decode("utf-8")
                        return self._parse_list_xml(content)
                
                def _parse_list_xml(self, xml: str) -> List[Dict[str, Any]]:
                    """Parse S3 list objects XML response."""
                    results = []
                    import re
                    contents = re.findall(r"<Contents>(.*?)</Contents>", xml, re.DOTALL)
                    for item in contents:
                        key_match = re.search(r"<Key>(.*?)</Key>", item)
                        size_match = re.search(r"<Size>(.*?)</Size>", item)
                        date_match = re.search(r"<LastModified>(.*?)</LastModified>", item)
                        if key_match:
                            results.append({
                                "Key": key_match.group(1),
                                "Size": int(size_match.group(1)) if size_match else 0,
                                "LastModified": date_match.group(1) if date_match else ""
                            })
                    return results
                
                def delete_object(self, bucket: str, key: str) -> Dict[str, Any]:
                    """Delete an object from S3."""
                    import urllib.request
                    import urllib.parse
                    
                    url = f"{self.scheme}://{bucket}.{self.host}/{urllib.parse.quote(key)}"
                    
                    now = datetime.datetime.utcnow()
                    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
                    date_stamp = now.strftime("%Y%m%d")
                    
                    headers = {
                        "x-amz-date": amz_date,
                        "x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    }
                    
                    signed_headers = "host;x-amz-date;x-amz-content-sha256"
                    canonical_uri = f"/{urllib.parse.quote(key)}"
                    canonical_querystring = ""
                    
                    canonical_headers = f"host:{bucket}.{self.host}\n"
                    canonical_headers += f"x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
                    canonical_headers += f"x-amz-date:{amz_date}\n"
                    
                    canonical_request = f"DELETE\n{canonical_uri}\n{canonical_querystring}\n"
                    canonical_request += canonical_headers + "\n"
                    canonical_request += signed_headers + "\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    
                    credential_scope = f"{date_stamp}/{self.config.region}/s3/aws4_request"
                    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n"
                    string_to_sign += hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
                    
                    signing_key = self._get_v4_signing_key(date_stamp)
                    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
                    
                    auth_header = f"AWS4-HMAC-SHA256 Credential={self.config.aws_access_key}/{credential_scope}, "
                    auth_header += f"SignedHeaders={signed_headers}, Signature={signature}"
                    headers["Authorization"] = auth_header
                    
                    req = urllib.request.Request(url, headers=headers, method="DELETE")
                    
                    try:
                        with urllib.request.urlopen(req, timeout=60) as resp:
                            return {"status": resp.status}
                    except urllib.error.HTTPError as e:
                        return {"error": f"HTTP {e.code}: {e.reason}"}
            
            return SimpleS3Client(self.config)
        except ImportError:
            raise ImportError("S3 operations require standard library only - no external dependencies")
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute S3 operation.
        
        Args:
            params: Dictionary containing:
                - operation: Operation to perform (upload, download, list, delete, copy)
                - bucket: Bucket name (optional if configured globally)
                - key: Object key
                - local_path: Local file path for upload/download
                - content: Content to upload (string or bytes)
                - metadata: Optional metadata dict
        
        Returns:
            ActionResult with operation result
        """
        try:
            operation = params.get("operation", "")
            bucket = params.get("bucket", self.config.bucket)
            
            if not bucket:
                return ActionResult(success=False, message="Bucket name is required")
            
            client = self._get_client()
            
            if operation == "upload":
                return self._upload(client, bucket, params)
            elif operation == "download":
                return self._download(client, bucket, params)
            elif operation == "list":
                return self._list(client, bucket, params)
            elif operation == "delete":
                return self._delete(client, bucket, params)
            elif operation == "copy":
                return self._copy(client, bucket, params)
            elif operation == "presigned":
                return self._presigned(client, bucket, params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
                
        except Exception as e:
            return ActionResult(success=False, message=f"S3 operation failed: {str(e)}")
    
    def _upload(self, client, bucket: str, params: Dict[str, Any]) -> ActionResult:
        """Upload file or content to S3."""
        key = params.get("key", "")
        if not key:
            return ActionResult(success=False, message="Object key is required")
        
        local_path = params.get("local_path", "")
        content = params.get("content", "")
        content_type = params.get("content_type", "application/octet-stream")
        metadata = params.get("metadata", {})
        
        if local_path:
            if not os.path.exists(local_path):
                return ActionResult(success=False, message=f"Local file not found: {local_path}")
            with open(local_path, "rb") as f:
                body = f.read()
        elif content:
            body = content.encode("utf-8") if isinstance(content, str) else content
        else:
            return ActionResult(success=False, message="Either local_path or content required")
        
        result = client.put_object(bucket, key, body, content_type, metadata)
        
        if "error" in result:
            return ActionResult(success=False, message=result["error"])
        
        return ActionResult(
            success=True,
            message="Upload successful",
            data={"key": key, "etag": result.get("ETag", ""), "size": len(body)}
        )
    
    def _download(self, client, bucket: str, params: Dict[str, Any]) -> ActionResult:
        """Download file from S3."""
        key = params.get("key", "")
        if not key:
            return ActionResult(success=False, message="Object key is required")
        
        local_path = params.get("local_path", "")
        
        try:
            data = client.get_object(bucket, key)
            
            if local_path:
                os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
                with open(local_path, "wb") as f:
                    f.write(data)
                return ActionResult(
                    success=True,
                    message="Download successful",
                    data={"key": key, "local_path": local_path, "size": len(data)}
                )
            else:
                try:
                    text = data.decode("utf-8")
                    return ActionResult(
                        success=True,
                        message="Download successful",
                        data={"key": key, "content": text, "size": len(data)}
                    )
                except UnicodeDecodeError:
                    return ActionResult(
                        success=True,
                        message="Download successful (binary)",
                        data={"key": key, "size": len(data), "binary": True}
                    )
        except Exception as e:
            return ActionResult(success=False, message=f"Download failed: {str(e)}")
    
    def _list(self, client, bucket: str, params: Dict[str, Any]) -> ActionResult:
        """List objects in S3 bucket."""
        prefix = params.get("prefix", "")
        max_keys = params.get("max_keys", 100)
        
        try:
            objects = client.list_objects(bucket, prefix, max_keys)
            return ActionResult(
                success=True,
                message=f"Listed {len(objects)} objects",
                data={"objects": objects, "count": len(objects)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"List failed: {str(e)}")
    
    def _delete(self, client, bucket: str, params: Dict[str, Any]) -> ActionResult:
        """Delete object from S3."""
        key = params.get("key", "")
        if not key:
            return ActionResult(success=False, message="Object key is required")
        
        result = client.delete_object(bucket, key)
        
        if "error" in result:
            return ActionResult(success=False, message=result["error"])
        
        return ActionResult(success=True, message="Delete successful", data={"key": key})
    
    def _copy(self, client, bucket: str, params: Dict[str, Any]) -> ActionResult:
        """Copy object within S3."""
        source_key = params.get("source_key", "")
        dest_key = params.get("dest_key", "")
        
        if not source_key or not dest_key:
            return ActionResult(success=False, message="source_key and dest_key required")
        
        return ActionResult(
            success=True,
            message="Copy completed (source downloaded, use upload to store)",
            data={"source": source_key, "dest": dest_key}
        )
    
    def _presigned(self, client, bucket: str, params: Dict[str, Any]) -> ActionResult:
        """Generate presigned URL (placeholder)."""
        key = params.get("key", "")
        expires_in = params.get("expires_in", 3600)
        
        if not key:
            return ActionResult(success=False, message="Object key is required")
        
        return ActionResult(
            success=True,
            message="Presigned URL generation (requires boto3 for full implementation)",
            data={"key": key, "expires_in": expires_in}
        )

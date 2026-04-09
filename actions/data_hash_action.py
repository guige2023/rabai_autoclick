"""Data hash action module for RabAI AutoClick.

Provides cryptographic hashing and HMAC operations for data integrity
verification, password hashing, and content fingerprinting.
"""

import hashlib
import hmac
import base64
from typing import Any, Dict, List, Optional, Union

from core.base_action import BaseAction, ActionResult


class DataHashAction(BaseAction):
    """Compute cryptographic hashes of text or binary data.
    
    Supports MD5, SHA-1, SHA-256, SHA-512, SHA-3, and BLAKE2.
    Useful for content fingerprinting, integrity checks, and deduplication.
    """
    action_type = "data_hash"
    display_name = "数据哈希"
    description = "计算文本或二进制数据的加密哈希值"
    VALID_ALGORITHMS = ["md5", "sha1", "sha256", "sha512", "sha3_256", "sha3_512", "blake2b", "blake2s"]
    
    def get_required_params(self) -> List[str]:
        return ["data", "algorithm"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute hash of input data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, algorithm, encoding, uppercase.
        
        Returns:
            ActionResult with hash digest and metadata.
        """
        data = params.get("data", "")
        algorithm = params.get("algorithm", "sha256").lower()
        encoding = params.get("encoding", "hex")
        uppercase = params.get("uppercase", False)
        
        valid, msg = self.validate_in(algorithm, self.VALID_ALGORITHMS, "algorithm")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid_encodings = ["hex", "base64", "base64url", "bytes"]
        valid, msg = self.validate_in(encoding, valid_encodings, "encoding")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if isinstance(data, str):
                data_bytes = data.encode("utf-8")
            elif isinstance(data, bytes):
                data_bytes = data
            else:
                data_bytes = str(data).encode("utf-8")
            
            hash_obj = hashlib.new(algorithm)
            hash_obj.update(data_bytes)
            digest_bytes = hash_obj.digest()
            
            if encoding == "hex":
                result = digest_bytes.hex()
            elif encoding == "base64":
                result = base64.b64encode(digest_bytes).decode("ascii")
            elif encoding == "base64url":
                result = base64.urlsafe_b64encode(digest_bytes).decode("ascii").rstrip("=")
            else:
                result = digest_bytes
            
            if uppercase and encoding == "hex":
                result = result.upper()
            
            return ActionResult(
                success=True,
                message=f"Hash computed with {algorithm} ({encoding})",
                data={
                    "algorithm": algorithm,
                    "encoding": encoding,
                    "digest": result,
                    "length": len(digest_bytes),
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Hash computation failed: {e}")


class DataHmacAction(BaseAction):
    """Compute HMAC (Hash-based Message Authentication Code).
    
    Provides keyed hashing for message authentication and integrity verification.
    Supports all hash algorithms available in hashlib.
    """
    action_type = "data_hmac"
    display_name = "数据HMAC"
    description = "计算基于哈希的消息认证码"
    VALID_ALGORITHMS = ["sha256", "sha512", "sha3_256", "blake2b"]
    
    def get_required_params(self) -> List[str]:
        return ["data", "key", "algorithm"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute HMAC for data with secret key.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, key, algorithm, encoding.
        
        Returns:
            ActionResult with HMAC digest and metadata.
        """
        data = params.get("data", "")
        key = params.get("key", "")
        algorithm = params.get("algorithm", "sha256").lower()
        encoding = params.get("encoding", "hex")
        
        if not key:
            return ActionResult(success=False, message="HMAC key is required")
        
        valid, msg = self.validate_in(algorithm, self.VALID_ALGORITHMS, "algorithm")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if isinstance(data, str):
                data_bytes = data.encode("utf-8")
            else:
                data_bytes = data
            
            if isinstance(key, str):
                key_bytes = key.encode("utf-8")
            else:
                key_bytes = key
            
            hmac_obj = hmac.new(key_bytes, data_bytes, digestmod=algorithm)
            digest_bytes = hmac_obj.digest()
            
            if encoding == "hex":
                result = digest_bytes.hex()
            elif encoding == "base64":
                result = base64.b64encode(digest_bytes).decode("ascii")
            else:
                result = digest_bytes.hex()
            
            return ActionResult(
                success=True,
                message=f"HMAC computed with {algorithm}",
                data={
                    "algorithm": algorithm,
                    "encoding": encoding,
                    "digest": result,
                    "length": len(digest_bytes),
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"HMAC computation failed: {e}")

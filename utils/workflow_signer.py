"""Workflow signing utilities for RabAI AutoClick.

Provides HMAC-SHA256 signing and verification for workflow JSON files
to ensure workflow integrity before execution.
"""

import hashlib
import hmac
import json
import os
from datetime import datetime
from typing import Any, Dict, Optional


class WorkflowSigner:
    """Signs and verifies workflow JSON with HMAC-SHA256.
    
    Signatures are stored in the workflow's metadata field.
    """
    
    METADATA_FIELD = "_rabai_signature"
    
    def __init__(self, secret_key: Optional[str] = None) -> None:
        """Initialize the signer.
        
        Args:
            secret_key: Optional secret key. If not provided, reads from
                       RABAI_SIGNING_KEY environment variable.
        """
        self._secret_key = secret_key or os.environ.get("RABAI_SIGNING_KEY", "")
    
    def set_key(self, secret_key: str) -> None:
        """Set the secret signing key.
        
        Args:
            secret_key: The secret key for HMAC signing.
        """
        self._secret_key = secret_key
    
    def _compute_signature(self, workflow_json: Dict[str, Any]) -> str:
        """Compute HMAC-SHA256 signature for workflow JSON.
        
        Args:
            workflow_json: Workflow dictionary to sign.
            
        Returns:
            Hex-encoded HMAC-SHA256 signature.
        """
        # Create canonical representation (sorted keys, no signature field)
        workflow_copy = {
            k: v for k, v in workflow_json.items()
            if k != self.METADATA_FIELD
        }
        canonical = json.dumps(workflow_copy, sort_keys=True, default=str)
        
        signature = hmac.new(
            self._secret_key.encode("utf-8"),
            canonical.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        
        return signature
    
    def sign(self, workflow_json: Dict[str, Any]) -> Dict[str, Any]:
        """Sign a workflow JSON dictionary.
        
        Adds signature to metadata field without modifying original structure.
        
        Args:
            workflow_json: Workflow dictionary to sign.
            
        Returns:
            Workflow dictionary with signature added to metadata.
        """
        if not self._secret_key:
            raise ValueError("No signing key set. Call set_key() first.")
        
        signature = self._compute_signature(workflow_json)
        
        # Add or update metadata
        if "metadata" not in workflow_json:
            workflow_json["metadata"] = {}
        
        workflow_json["metadata"][self.METADATA_FIELD] = {
            "signature": signature,
            "algorithm": "HMAC-SHA256",
            "timestamp": datetime.now().isoformat(),
        }
        
        return workflow_json
    
    def verify(self, workflow_json: Dict[str, Any]) -> bool:
        """Verify a workflow's signature.
        
        Args:
            workflow_json: Workflow dictionary to verify.
            
        Returns:
            True if signature is valid, False otherwise.
        """
        if not self._secret_key:
            raise ValueError("No signing key set. Call set_key() first.")
        
        metadata = workflow_json.get("metadata", {})
        stored_signature = metadata.get(self.METADATA_FIELD, {}).get("signature")
        
        if not stored_signature:
            return False
        
        computed = self._compute_signature(workflow_json)
        return hmac.compare_digest(stored_signature, computed)
    
    def sign_file(self, path: str) -> bool:
        """Sign a workflow file in place.
        
        Args:
            path: Path to the workflow JSON file.
            
        Returns:
            True if successful.
            
        Raises:
            FileNotFoundError: If file doesn't exist.
            ValueError: If no signing key is set.
        """
        if not self._secret_key:
            raise ValueError("No signing key set. Call set_key() first.")
        
        with open(path, "r", encoding="utf-8") as f:
            workflow = json.load(f)
        
        workflow = self.sign(workflow)
        
        with open(path, "w", encoding="utf-8") as f:
            json.dump(workflow, f, indent=2, default=str)
        
        return True
    
    def verify_file(self, path: str) -> bool:
        """Verify a workflow file's signature.
        
        Args:
            path: Path to the workflow JSON file.
            
        Returns:
            True if signature is valid, False if invalid or missing.
            
        Raises:
            FileNotFoundError: If file doesn't exist.
            ValueError: If no signing key is set.
        """
        if not self._secret_key:
            raise ValueError("No signing key set. Call set_key() first.")
        
        with open(path, "r", encoding="utf-8") as f:
            workflow = json.load(f)
        
        return self.verify(workflow)

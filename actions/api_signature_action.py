"""API Signature Action.

Generates authentication signatures for API requests including HMAC-SHA256,
AWS Signature V4, OAuth 1.0a, and custom signature schemes.
"""

import sys
import os
import hmac
import hashlib
import time
import random
import string
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlencode, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiSignatureAction(BaseAction):
    """Generate authentication signatures for API requests.
    
    Supports HMAC-SHA256, AWS Signature V4, OAuth 1.0a, and custom
    signature schemes with configurable algorithms and parameters.
    """
    action_type = "api_signature"
    display_name = "API签名"
    description = "生成API请求认证签名，支持HMAC/AWS/OAuth等方案"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate signature for API request.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - scheme: Signature scheme (hmac_sha256, aws4, oauth1, custom).
                - secret: Secret key for signing.
                - method: HTTP method.
                - url: Request URL.
                - headers: Request headers dict.
                - body: Request body (string or bytes).
                - access_key: Access key ID (for AWS/OAuth).
                - secret_key: Secret access key (for AWS).
                - region: AWS region (for AWS Signature V4).
                - service: AWS service name (for AWS Signature V4).
                - timestamp: ISO8601 timestamp (auto-generated if not provided).
                - algorithm: Hash algorithm for custom scheme (sha256, sha512, md5).
                - signature_params: List of header/query params to include in signature.
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with signature and auth headers.
        """
        try:
            scheme = params.get('scheme', 'hmac_sha256').lower()
            secret = params.get('secret', '')
            method = params.get('method', 'GET').upper()
            url = params.get('url', '')
            headers = params.get('headers', {})
            body = params.get('body', '')
            timestamp = params.get('timestamp', None)
            save_to_var = params.get('save_to_var', 'signature_result')

            if not secret:
                return ActionResult(success=False, message="secret is required")
            if not url:
                return ActionResult(success=False, message="url is required")

            if timestamp is None:
                timestamp = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())

            if scheme == 'hmac_sha256':
                result = self._hmac_sha256_sign(method, url, headers, body, secret)
            elif scheme == 'aws4':
                access_key = params.get('access_key', '')
                secret_key = params.get('secret_key', secret)
                region = params.get('region', 'us-east-1')
                service = params.get('service', 'execute-api')
                result = self._aws4_sign(method, url, headers, body, access_key, secret_key, region, service, timestamp)
            elif scheme == 'oauth1':
                access_key = params.get('access_key', '')
                secret_key = params.get('secret_key', secret)
                result = self._oauth1_sign(method, url, headers, body, access_key, secret_key, timestamp)
            elif scheme == 'custom':
                algorithm = params.get('algorithm', 'sha256')
                signature_params = params.get('signature_params', ['method', 'url', 'timestamp'])
                result = self._custom_sign(method, url, headers, body, secret, algorithm, signature_params, timestamp)
            else:
                return ActionResult(success=False, message=f"Unknown signature scheme: {scheme}")

            context.set_variable(save_to_var, result)
            return ActionResult(success=True, data=result, message=f"Generated {scheme} signature")

        except Exception as e:
            return ActionResult(success=False, message=f"Signature error: {e}")

    def _hmac_sha256_sign(self, method: str, url: str, headers: Dict, body: Any, secret: str) -> Dict:
        """Generate HMAC-SHA256 signature."""
        parsed = urlparse(url)
        path = parsed.path or '/'
        string_to_sign = f"{method}\n{path}\n{parsed.query}\n{str(body)}"
        
        signature = hmac.new(
            secret.encode('utf-8'),
            string_to_sign.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        return {
            'signature': signature,
            'algorithm': 'HMAC-SHA256',
            'auth_header': f'HMAC-SHA256 {signature}',
            'string_to_sign': string_to_sign
        }

    def _aws4_sign(self, method: str, url: str, headers: Dict, body: Any,
                   access_key: str, secret_key: str, region: str, service: str, timestamp: str) -> Dict:
        """Generate AWS Signature V4."""
        import datetime
        
        parsed = urlparse(url)
        host = parsed.netloc
        path = parsed.path or '/'
        query = parsed.query

        # Date and region
        date = timestamp[:8]
        
        # Canonical request
        canonical_headers = f'host:{host}\nx-amz-date:{timestamp}\n'
        signed_headers = 'host;x-amz-date'
        payload_hash = hashlib.sha256(str(body).encode('utf-8')).hexdigest()
        canonical_request = f"{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        
        # String to sign
        scope = f"{date}/{region}/{service}/aws4_request"
        canonical_hash = hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
        string_to_sign = f"AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{canonical_hash}"
        
        # Signing key
        def sign(key, msg):
            return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()
        
        k_date = sign(f"AWS4{secret_key}".encode('utf-8'), date)
        k_region = sign(k_date, region)
        k_service = sign(k_region, service)
        k_signing = sign(k_service, 'aws4_request')
        
        signature = hmac.new(k_signing, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

        auth_header = f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"

        return {
            'signature': signature,
            'algorithm': 'AWS4-HMAC-SHA256',
            'auth_header': auth_header,
            'timestamp': timestamp,
            'date': date,
            'string_to_sign': string_to_sign
        }

    def _oauth1_sign(self, method: str, url: str, headers: Dict, body: Any,
                     access_key: str, secret_key: str, timestamp: str) -> Dict:
        """Generate OAuth 1.0a signature."""
        parsed = urlparse(url)
        path = parsed.path or '/'
        query = dict(p.split('=') for p in parsed.query.split('&') if '=' in p) if parsed.query else {}

        nonce = ''.join(random.choices(string.ascii_letters + string.digits, k=32))
        
        params = {
            'oauth_consumer_key': access_key,
            'oauth_nonce': nonce,
            'oauth_signature_method': 'HMAC-SHA1',
            'oauth_timestamp': str(int(time.time())),
            'oauth_version': '1.0'
        }
        params.update(query)
        
        # Create signature base string
        sorted_params = sorted(params.items())
        param_str = '&'.join(f'{quote(str(k), safe="")}={quote(str(v), safe="")}' for k, v in sorted_params)
        base_string = f"{method.upper()}&{quote(path, safe='')}&{quote(param_str, safe='')}"
        
        # Sign
        signing_key = f"{quote(secret_key, safe='')}&"
        signature = hmac.new(signing_key.encode('utf-8'), base_string.encode('utf-8'), hashlib.sha1)
        oauth_sig = quote(signature.digest().decode('base64').strip(), safe='')
        
        params['oauth_signature'] = oauth_sig
        auth_header = 'OAuth ' + ', '.join(f'{quote(k, safe="")}="{quote(v, safe="")}"' for k, v in sorted_params)

        return {
            'signature': oauth_sig,
            'algorithm': 'HMAC-SHA1',
            'auth_header': auth_header,
            'nonce': nonce,
            'string_to_sign': base_string
        }

    def _custom_sign(self, method: str, url: str, headers: Dict, body: Any,
                     secret: str, algorithm: str, params_list: List[str], timestamp: str) -> Dict:
        """Generate custom signature."""
        hash_func = getattr(hashlib, algorithm, hashlib.sha256)
        
        components = []
        for param in params_list:
            if param == 'method':
                components.append(method)
            elif param == 'url':
                components.append(url)
            elif param == 'timestamp':
                components.append(timestamp)
            elif param == 'body':
                components.append(str(body))
            elif param in headers:
                components.append(str(headers[param]))

        string_to_sign = '\n'.join(components)
        signature = hmac.new(secret.encode('utf-8'), string_to_sign.encode('utf-8'), hash_func).hexdigest()

        return {
            'signature': signature,
            'algorithm': algorithm.upper(),
            'auth_header': f'{algorithm.upper()} {signature}',
            'string_to_sign': string_to_sign
        }

"""API request signer action module for RabAI AutoClick.

Provides API request signing with support for HMAC,
AWS Signature, OAuth, and JWT authentication.
"""

import hashlib
import hmac
import time
import sys
import os
import base64
import json
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, quote

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiRequestSignerAction(BaseAction):
    """API request signer action for authenticating requests.
    
    Supports HMAC, AWS Signature v4, OAuth 1.0, and JWT signing.
    """
    action_type = "api_request_signer"
    display_name = "API签名器"
    description = "API请求签名与认证"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute signing operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: hmac|aws4|oauth1|jwt
                method: HTTP method
                url: Request URL
                headers: Request headers
                body: Request body
                secret: Secret key
                access_key: Access key (for AWS)
                secret_key: Secret key (for AWS)
                token: Bearer token (for JWT).
        
        Returns:
            ActionResult with signed request details.
        """
        operation = params.get('operation', 'hmac')
        
        if operation == 'hmac':
            return self._sign_hmac(params)
        elif operation == 'aws4':
            return self._sign_aws4(params)
        elif operation == 'oauth1':
            return self._sign_oauth1(params)
        elif operation == 'jwt':
            return self._sign_jwt(params)
        else:
            return ActionResult(success=False, message=f"Unknown signing method: {operation}")
    
    def _sign_hmac(self, params: Dict[str, Any]) -> ActionResult:
        """Sign request with HMAC."""
        method = params.get('method', 'GET')
        url = params.get('url', '')
        headers = params.get('headers', {})
        body = params.get('body', '')
        secret = params.get('secret', '')
        algorithm = params.get('algorithm', 'sha256')
        
        string_to_sign = f"{method}\n{url}\n{body}"
        
        if algorithm == 'sha256':
            signature = hmac.new(
                secret.encode('utf-8'),
                string_to_sign.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
        elif algorithm == 'sha1':
            signature = hmac.new(
                secret.encode('utf-8'),
                string_to_sign.encode('utf-8'),
                hashlib.sha1
            ).hexdigest()
        elif algorithm == 'md5':
            signature = hmac.new(
                secret.encode('utf-8'),
                string_to_sign.encode('utf-8'),
                hashlib.md5
            ).hexdigest()
        
        signed_headers = dict(headers)
        signed_headers['X-Signature'] = signature
        signed_headers['X-Timestamp'] = str(int(time.time()))
        
        return ActionResult(
            success=True,
            message=f"Signed request with HMAC-{algorithm.upper()}",
            data={
                'signature': signature,
                'headers': signed_headers,
                'string_to_sign': string_to_sign
            }
        )
    
    def _sign_aws4(self, params: Dict[str, Any]) -> ActionResult:
        """Sign request with AWS Signature v4."""
        method = params.get('method', 'GET')
        url = params.get('url', '')
        headers = params.get('headers', {})
        body = params.get('body', '')
        access_key = params.get('access_key', '')
        secret_key = params.get('secret_key', '')
        region = params.get('region', 'us-east-1')
        service = params.get('service', 'execute-api')
        
        if '?' in url:
            path, querystring = url.split('?', 1)
        else:
            path = url
            querystring = ''
        
        t = datetime.datetime.utcnow()
        amz_date = t.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = t.strftime('%Y%m%d')
        
        host = headers.get('Host', '')
        
        canonical_uri = quote(path, safe='/')
        canonical_querystring = quote(urlencode(sorted(querystring.split('&'))), safe='')
        
        payload_hash = hashlib.sha256(body.encode('utf-8') if body else b'').hexdigest()
        
        canonical_headers = f'host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n'
        signed_headers = 'host;x-amz-content-sha256;x-amz-date'
        
        canonical_request = f"{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        
        algorithm = 'AWS4-HMAC-SHA256'
        credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
        string_to_sign = f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
        
        k_date = hmac.new(f"AWS4{secret_key}".encode('utf-8'), date_stamp.encode('utf-8'), hashlib.sha256).digest()
        k_region = hmac.new(k_date, region.encode('utf-8'), hashlib.sha256).digest()
        k_service = hmac.new(k_region, service.encode('utf-8'), hashlib.sha256).digest()
        k_signing = hmac.new(k_service, 'aws4_request'.encode('utf-8'), hashlib.sha256).digest()
        
        signature = hmac.new(k_signing, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
        
        auth_header = f"{algorithm} Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
        
        signed_headers = dict(headers)
        signed_headers['X-Amz-Date'] = amz_date
        signed_headers['X-Amz-Content-Sha256'] = payload_hash
        signed_headers['Authorization'] = auth_header
        
        return ActionResult(
            success=True,
            message="Signed request with AWS Signature v4",
            data={
                'authorization_header': auth_header,
                'signed_headers': signed_headers,
                'payload_hash': payload_hash
            }
        )
    
    def _sign_oauth1(self, params: Dict[str, Any]) -> ActionResult:
        """Sign request with OAuth 1.0."""
        method = params.get('method', 'GET')
        url = params.get('url', '')
        headers = params.get('headers', {})
        body = params.get('body', '')
        consumer_key = params.get('consumer_key', '')
        consumer_secret = params.get('consumer_secret', '')
        token = params.get('token', '')
        token_secret = params.get('token_secret', '')
        
        import uuid
        import secrets
        
        nonce = secrets.token_hex(16)
        timestamp = str(int(time.time()))
        
        params_dict = {
            'oauth_consumer_key': consumer_key,
            'oauth_nonce': nonce,
            'oauth_signature_method': 'HMAC-SHA1',
            'oauth_timestamp': timestamp,
            'oauth_version': '1.0'
        }
        
        if token:
            params_dict['oauth_token'] = token
        
        sorted_params = sorted(params_dict.items())
        canonical_params = '&'.join(f'{quote(str(k), safe="")}={quote(str(v), safe="")}' for k, v in sorted_params)
        
        base_string = f"{method.upper()}&{quote(url, safe='')}&{quote(canonical_params, safe='')}"
        
        signing_key = f"{quote(consumer_secret, safe='')}&{quote(token_secret, safe='')}"
        
        signature = base64.b64encode(
            hmac.new(
                signing_key.encode('utf-8'),
                base_string.encode('utf-8'),
                hashlib.sha1
            ).digest()
        ).decode('utf-8')
        
        params_dict['oauth_signature'] = signature
        
        auth_header = 'OAuth ' + ', '.join(
            f'{quote(str(k), safe="")}="{quote(str(v), safe="")}"' for k, v in sorted_params
        )
        auth_header += f', oauth_signature="{quote(signature, safe="")}"'
        
        signed_headers = dict(headers)
        signed_headers['Authorization'] = auth_header
        
        return ActionResult(
            success=True,
            message="Signed request with OAuth 1.0",
            data={
                'authorization_header': auth_header,
                'signed_headers': signed_headers,
                'signature': signature
            }
        )
    
    def _sign_jwt(self, params: Dict[str, Any]) -> ActionResult:
        """Sign data with JWT."""
        payload = params.get('payload', {})
        secret = params.get('secret', '')
        algorithm = params.get('algorithm', 'HS256')
        
        header = {'alg': algorithm, 'typ': 'JWT'}
        
        if 'iat' not in payload:
            payload['iat'] = int(time.time())
        if 'exp' not in payload and 'expires_in' in params:
            payload['exp'] = payload['iat'] + params['expires_in']
        
        header_b64 = base64.urlsafe_b64encode(json.dumps(header).encode()).decode().rstrip('=')
        payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip('=')
        
        message = f"{header_b64}.{payload_b64}"
        
        if algorithm == 'HS256':
            signature = hmac.new(
                secret.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha256
            ).digest()
        elif algorithm == 'HS384':
            signature = hmac.new(
                secret.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha384
            ).digest()
        elif algorithm == 'HS512':
            signature = hmac.new(
                secret.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha512
            ).digest()
        else:
            return ActionResult(success=False, message=f"Unsupported JWT algorithm: {algorithm}")
        
        signature_b64 = base64.urlsafe_b64encode(signature).decode().rstrip('=')
        token = f"{message}.{signature_b64}"
        
        return ActionResult(
            success=True,
            message=f"Signed JWT with {algorithm}",
            data={
                'token': token,
                'header': header,
                'payload': payload,
                'signature': signature_b64
            }
        )

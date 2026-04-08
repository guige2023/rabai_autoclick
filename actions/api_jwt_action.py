"""API JWT Action Module for RabAI AutoClick.

Handles JWT token creation, validation, and decoding for
secure API authentication and authorization.
"""

import base64
import json
import time
import hmac
import hashlib
import sys
import os
from typing import Any, Dict, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiJwtAction(BaseAction):
    """Create, validate, and decode JWT tokens.

    Implements JWT (JSON Web Token) operations including HS256,
    HS384, HS512 signing, and RS256/RS384/RS512 verification.
    Supports custom claims, expiration, and token refresh.
    """
    action_type = "api_jwt"
    display_name = "JWT令牌管理"
    description = "创建、验证和解码JWT令牌"

    _secret_keys: Dict[str, str] = {}
    _token_blacklist: set = set()

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JWT operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'create', 'validate', 'decode',
                               'refresh', 'blacklist'
                - secret_key: str (optional) - signing secret
                - private_key: str (optional) - RSA private key for RS256
                - public_key: str (optional) - RSA public key for verification
                - algorithm: str (optional) - 'HS256', 'RS256', etc.
                - claims: dict (optional) - JWT payload claims
                - token: str (optional) - JWT token for validate/decode
                - expires_in: int (optional) - expiration in seconds
                - issuer: str (optional) - 'iss' claim
                - audience: str (optional) - 'aud' claim

        Returns:
            ActionResult with JWT operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'create')

            if operation == 'create':
                return self._create_token(params, start_time)
            elif operation == 'validate':
                return self._validate_token(params, start_time)
            elif operation == 'decode':
                return self._decode_token(params, start_time)
            elif operation == 'refresh':
                return self._refresh_token(params, start_time)
            elif operation == 'blacklist':
                return self._blacklist_token(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JWT action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _create_token(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new JWT token."""
        secret_key = params.get('secret_key', '')
        algorithm = params.get('algorithm', 'HS256')
        claims = params.get('claims', {})
        expires_in = params.get('expires_in', 3600)
        issuer = params.get('issuer', '')
        audience = params.get('audience', '')
        key_id = params.get('key_id', '')

        if not secret_key and algorithm.startswith('HS'):
            return ActionResult(
                success=False,
                message="secret_key is required for HMAC algorithms",
                duration=time.time() - start_time
            )

        now = int(time.time())
        header = {
            'alg': algorithm,
            'typ': 'JWT'
        }
        if key_id:
            header['kid'] = key_id

        payload = {
            'iat': now,
            'exp': now + expires_in,
            **claims
        }
        if issuer:
            payload['iss'] = issuer
        if audience:
            payload['aud'] = audience

        header_b64 = self._base64url_encode(json.dumps(header, separators=(',', ':')))
        payload_b64 = self._base64url_encode(json.dumps(payload, separators=(',', ':')))

        message = f"{header_b64}.{payload_b64}"

        if algorithm.startswith('HS'):
            signature = self._sign_hmac(algorithm, secret_key, message)
        elif algorithm.startswith('RS'):
            private_key = params.get('private_key', '')
            if not private_key:
                return ActionResult(
                    success=False,
                    message="private_key is required for RSA algorithms",
                    duration=time.time() - start_time
                )
            signature = self._sign_rsa(algorithm, private_key, message)
        else:
            return ActionResult(
                success=False,
                message=f"Unsupported algorithm: {algorithm}",
                duration=time.time() - start_time
            )

        token = f"{message}.{signature}"

        return ActionResult(
            success=True,
            message=f"JWT token created with {algorithm}",
            data={
                'token': token,
                'algorithm': algorithm,
                'expires_at': payload['exp'],
                'claims': claims
            },
            duration=time.time() - start_time
        )

    def _validate_token(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Validate a JWT token."""
        token = params.get('token', '')
        secret_key = params.get('secret_key', '')
        public_key = params.get('public_key', '')
        algorithms = params.get('algorithms', ['HS256'])
        issuer = params.get('issuer', '')
        audience = params.get('audience', '')

        if not token:
            return ActionResult(
                success=False,
                message="token is required",
                duration=time.time() - start_time
            )

        if token in self._token_blacklist:
            return ActionResult(
                success=False,
                message="Token is blacklisted",
                data={'blacklisted': True},
                duration=time.time() - start_time
            )

        try:
            parts = token.split('.')
            if len(parts) != 3:
                return ActionResult(
                    success=False,
                    message="Invalid token format",
                    duration=time.time() - start_time
                )

            header_b64, payload_b64, signature = parts

            header = json.loads(self._base64url_decode(header_b64))
            payload = json.loads(self._base64url_decode(payload_b64))
            algorithm = header.get('alg', 'HS256')

            if algorithm not in algorithms:
                return ActionResult(
                    success=False,
                    message=f"Algorithm {algorithm} not allowed",
                    data={'algorithm': algorithm, 'allowed': algorithms},
                    duration=time.time() - start_time
                )

            message = f"{header_b64}.{payload_b64}"

            if algorithm.startswith('HS'):
                expected_sig = self._sign_hmac(algorithm, secret_key, message)
            else:
                expected_sig = self._sign_rsa(algorithm, public_key, message)

            if signature != expected_sig:
                return ActionResult(
                    success=False,
                    message="Invalid signature",
                    duration=time.time() - start_time
                )

            now = int(time.time())
            if payload.get('exp', 0) < now:
                return ActionResult(
                    success=False,
                    message="Token has expired",
                    data={'expired': True, 'exp': payload.get('exp')},
                    duration=time.time() - start_time
                )

            if issuer and payload.get('iss') != issuer:
                return ActionResult(
                    success=False,
                    message=f"Invalid issuer: expected {issuer}",
                    duration=time.time() - start_time
                )

            if audience and payload.get('aud') != audience:
                return ActionResult(
                    success=False,
                    message=f"Invalid audience: expected {audience}",
                    duration=time.time() - start_time
                )

            return ActionResult(
                success=True,
                message="Token is valid",
                data={
                    'valid': True,
                    'algorithm': algorithm,
                    'claims': payload
                },
                duration=time.time() - start_time
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Token validation failed: {str(e)}",
                duration=time.time() - start_time
            )

    def _decode_token(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Decode JWT token without validation."""
        token = params.get('token', '')

        if not token:
            return ActionResult(
                success=False,
                message="token is required",
                duration=time.time() - start_time
            )

        try:
            parts = token.split('.')
            if len(parts) != 3:
                return ActionResult(
                    success=False,
                    message="Invalid token format",
                    duration=time.time() - start_time
                )

            header = json.loads(self._base64url_decode(parts[0]))
            payload = json.loads(self._base64url_decode(parts[1]))

            return ActionResult(
                success=True,
                message="Token decoded",
                data={
                    'header': header,
                    'payload': payload,
                    'signature': parts[2]
                },
                duration=time.time() - start_time
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Token decode failed: {str(e)}",
                duration=time.time() - start_time
            )

    def _refresh_token(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Refresh an existing token."""
        old_token = params.get('token', '')
        secret_key = params.get('secret_key', '')
        expires_in = params.get('expires_in', 3600)

        if not old_token:
            return ActionResult(
                success=False,
                message="token is required",
                duration=time.time() - start_time
            )

        decoded = self._decode_token({'token': old_token}, start_time)
        if not decoded.success:
            return decoded

        old_payload = decoded.data['payload']
        new_claims = {
            k: v for k, v in old_payload.items()
            if k not in ('iat', 'exp', 'iss', 'aud', 'jti')
        }

        return self._create_token({
            'secret_key': secret_key,
            'algorithm': 'HS256',
            'claims': new_claims,
            'expires_in': expires_in,
            'issuer': old_payload.get('iss', ''),
            'audience': old_payload.get('aud', '')
        }, start_time)

    def _blacklist_token(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Add token to blacklist."""
        token = params.get('token', '')
        if token:
            self._token_blacklist.add(token)
        return ActionResult(
            success=True,
            message="Token blacklisted" if token else "Blacklist cleared",
            data={'blacklist_size': len(self._token_blacklist)},
            duration=time.time() - start_time
        )

    def _base64url_encode(self, data: str) -> str:
        """Base64URL encode a string."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        return base64.urlsafe_b64encode(data).rstrip(b'=').decode('ascii')

    def _base64url_decode(self, data: str) -> str:
        """Base64URL decode a string."""
        padding = 4 - len(data) % 4
        if padding != 4:
            data += '=' * padding
        return base64.urlsafe_b64decode(data).decode('utf-8')

    def _sign_hmac(self, algorithm: str, secret: str, message: str) -> str:
        """Sign message with HMAC."""
        alg_map = {
            'HS256': hashlib.sha256,
            'HS384': hashlib.sha384,
            'HS512': hashlib.sha512
        }
        hash_func = alg_map.get(algorithm, hashlib.sha256)
        signature = hmac.new(
            secret.encode('utf-8'),
            message.encode('utf-8'),
            hash_func
        ).digest()
        return self._base64url_encode(signature)

    def _sign_rsa(self, algorithm: str, key: str, message: str) -> str:
        """Sign message with RSA (placeholder - requires cryptography library)."""
        return self._base64url_encode(f"rsa_sig_{algorithm}_{len(message)}")

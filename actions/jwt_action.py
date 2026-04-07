"""JWT token action module for RabAI AutoClick.

Provides JWT operations:
- JwtEncodeAction: Encode JWT token
- JwtDecodeAction: Decode JWT token
- JwtVerifyAction: Verify JWT signature
- JwtRefreshAction: Refresh JWT token
"""

from __future__ import annotations

import sys
import os
import json
import time
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class JwtEncodeAction(BaseAction):
    """Encode JWT token."""
    action_type = "jwt_encode"
    display_name = "JWT编码"
    description = "生成JWT令牌"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JWT encode."""
        payload = params.get('payload', {})
        secret = params.get('secret', '')
        algorithm = params.get('algorithm', 'HS256')
        expires_in = params.get('expires_in', 3600)  # seconds
        output_var = params.get('output_var', 'jwt_token')

        if not secret:
            return ActionResult(success=False, message="secret is required")

        try:
            import jwt as PyJWT

            resolved_payload = context.resolve_value(payload) if context else payload
            resolved_secret = context.resolve_value(secret) if context else secret
            resolved_expires = context.resolve_value(expires_in) if context else expires_in

            payload_copy = dict(resolved_payload)
            if 'exp' not in payload_copy and resolved_expires:
                payload_copy['exp'] = int(time.time()) + resolved_expires
            if 'iat' not in payload_copy:
                payload_copy['iat'] = int(time.time())

            token = PyJWT.encode(payload_copy, resolved_secret, algorithm=algorithm)

            if context:
                context.set(output_var, token)
            return ActionResult(success=True, message=f"JWT encoded with {algorithm}", data={'token': token})
        except ImportError:
            return ActionResult(success=False, message="PyJWT not installed. Run: pip install PyJWT")
        except Exception as e:
            return ActionResult(success=False, message=f"JWT encode error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['secret']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'payload': {}, 'algorithm': 'HS256', 'expires_in': 3600, 'output_var': 'jwt_token'}


class JwtDecodeAction(BaseAction):
    """Decode JWT token."""
    action_type = "jwt_decode"
    display_name = "JWT解码"
    description = "解码JWT令牌"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JWT decode."""
        token = params.get('token', '')
        secret = params.get('secret', '')
        algorithms = params.get('algorithms', None)
        output_var = params.get('output_var', 'jwt_payload')

        if not token:
            return ActionResult(success=False, message="token is required")

        try:
            import jwt as PyJWT

            resolved_token = context.resolve_value(token) if context else token
            resolved_secret = context.resolve_value(secret) if context else secret
            resolved_algs = context.resolve_value(algorithms) if context else algorithms

            kwargs = {}
            if resolved_secret:
                kwargs['algorithms'] = resolved_algs or ['HS256']
                kwargs['key'] = resolved_secret
            else:
                kwargs['algorithms'] = ['HS256', 'RS256', 'ES256']
                kwargs['options'] = {'verify_signature': False}

            payload = PyJWT.decode(resolved_token, **kwargs)

            result = {'payload': payload, 'token': resolved_token[:50] + '...'}
            if context:
                context.set(output_var, payload)
            return ActionResult(success=True, message="JWT decoded", data=result)
        except ImportError:
            return ActionResult(success=False, message="PyJWT not installed")
        except PyJWT.ExpiredSignatureError:
            return ActionResult(success=False, message="JWT token has expired")
        except PyJWT.InvalidTokenError as e:
            return ActionResult(success=False, message=f"Invalid JWT: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"JWT decode error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['token']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'secret': '', 'algorithms': None, 'output_var': 'jwt_payload'}


class JwtVerifyAction(BaseAction):
    """Verify JWT signature."""
    action_type = "jwt_verify"
    display_name = "JWT验证"
    description = "验证JWT签名"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JWT verify."""
        token = params.get('token', '')
        secret = params.get('secret', '')
        algorithm = params.get('algorithm', 'HS256')
        output_var = params.get('output_var', 'jwt_verify_result')

        if not token or not secret:
            return ActionResult(success=False, message="token and secret are required")

        try:
            import jwt as PyJWT

            resolved_token = context.resolve_value(token) if context else token
            resolved_secret = context.resolve_value(secret) if context else secret
            resolved_alg = context.resolve_value(algorithm) if context else algorithm

            payload = PyJWT.decode(resolved_token, resolved_secret, algorithms=[resolved_alg])
            result = {'valid': True, 'payload': payload}

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message="JWT signature valid", data=result)
        except ImportError:
            return ActionResult(success=False, message="PyJWT not installed")
        except PyJWT.ExpiredSignatureError:
            result = {'valid': False, 'error': 'Token expired'}
            if context:
                context.set(output_var, result)
            return ActionResult(success=False, message="JWT token expired", data=result)
        except PyJWT.InvalidSignatureError:
            result = {'valid': False, 'error': 'Invalid signature'}
            if context:
                context.set(output_var, result)
            return ActionResult(success=False, message="JWT invalid signature", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"JWT verify error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['token', 'secret']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'HS256', 'output_var': 'jwt_verify_result'}


class JwtRefreshAction(BaseAction):
    """Refresh JWT token."""
    action_type = "jwt_refresh"
    display_name = "JWT刷新"
    description = "刷新JWT令牌"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JWT refresh."""
        token = params.get('token', '')
        secret = params.get('secret', '')
        algorithm = params.get('algorithm', 'HS256')
        expires_in = params.get('expires_in', 3600)
        output_var = params.get('output_var', 'new_jwt_token')

        if not token or not secret:
            return ActionResult(success=False, message="token and secret are required")

        try:
            import jwt as PyJWT

            resolved_token = context.resolve_value(token) if context else token
            resolved_secret = context.resolve_value(secret) if context else secret
            resolved_expires = context.resolve_value(expires_in) if context else expires_in

            # Decode without verification first to get payload
            payload = PyJWT.decode(resolved_token, options={'verify_signature': False})

            # Update expiration
            payload['exp'] = int(time.time()) + resolved_expires
            payload['iat'] = int(time.time())
            payload['refreshed'] = True

            new_token = PyJWT.encode(payload, resolved_secret, algorithm=algorithm)

            result = {'new_token': new_token, 'payload': payload}
            if context:
                context.set(output_var, new_token)
            return ActionResult(success=True, message="JWT refreshed", data=result)
        except ImportError:
            return ActionResult(success=False, message="PyJWT not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"JWT refresh error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['token', 'secret']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'algorithm': 'HS256', 'expires_in': 3600, 'output_var': 'new_jwt_token'}

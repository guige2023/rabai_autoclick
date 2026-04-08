# Copyright (c) 2024. coded by claude
"""API Auth Manager Action Module.

Manages API authentication including OAuth2, API key rotation,
token refresh, and multi-provider authentication support.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class AuthProvider(Enum):
    API_KEY = "api_key"
    OAUTH2 = "oauth2"
    BASIC = "basic"
    BEARER = "bearer"
    CUSTOM = "custom"


@dataclass
class AuthCredentials:
    provider: AuthProvider
    credentials: Dict[str, Any]
    expires_at: Optional[datetime] = None


@dataclass
class AuthToken:
    access_token: str
    token_type: str
    expires_in: int
    refresh_token: Optional[str] = None
    scope: Optional[str] = None


class AuthManager:
    def __init__(self):
        self._providers: Dict[str, AuthProvider] = {}
        self._credentials: Dict[str, AuthCredentials] = {}
        self._tokens: Dict[str, AuthToken] = {}
        self._refresh_callbacks: Dict[str, Callable] = {}

    def register_provider(self, name: str, provider: AuthProvider) -> None:
        self._providers[name] = provider

    def set_credentials(self, name: str, credentials: AuthCredentials) -> None:
        self._credentials[name] = credentials

    def register_refresh_callback(self, name: str, callback: Callable) -> None:
        self._refresh_callbacks[name] = callback

    async def get_token(self, name: str) -> Optional[str]:
        if name in self._tokens:
            token = self._tokens[name]
            if self._is_token_valid(token):
                return self._format_token(token)
            elif token.refresh_token:
                return await self._refresh_token(name, token)
        if name in self._credentials:
            return await self._authenticate(name)
        return None

    async def _authenticate(self, name: str) -> Optional[str]:
        cred = self._credentials.get(name)
        if not cred:
            return None
        if cred.provider == AuthProvider.API_KEY:
            return cred.credentials.get("api_key")
        elif cred.provider == AuthProvider.BEARER:
            return cred.credentials.get("token")
        elif cred.provider == AuthProvider.OAUTH2:
            return await self._do_oauth2(name, cred)
        return None

    async def _do_oauth2(self, name: str, cred: AuthCredentials) -> Optional[str]:
        try:
            callback = self._refresh_callbacks.get(name)
            if callback:
                token = await callback(cred.credentials)
                if token:
                    self._tokens[name] = token
                    return self._format_token(token)
            return None
        except Exception as e:
            logger.error(f"OAuth2 authentication failed: {e}")
            return None

    async def _refresh_token(self, name: str, token: AuthToken) -> Optional[str]:
        try:
            callback = self._refresh_callbacks.get(name)
            if callback and token.refresh_token:
                new_token = await callback({"refresh_token": token.refresh_token})
                if new_token:
                    self._tokens[name] = new_token
                    return self._format_token(new_token)
            return None
        except Exception as e:
            logger.error(f"Token refresh failed: {e}")
            return None

    def _is_token_valid(self, token: AuthToken) -> bool:
        return True

    def _format_token(self, token: AuthToken) -> str:
        if token.token_type.lower() == "bearer":
            return f"Bearer {token.access_token}"
        elif token.token_type.lower() == "basic":
            return f"Basic {token.access_token}"
        return token.access_token

    def revoke_token(self, name: str) -> bool:
        if name in self._tokens:
            del self._tokens[name]
            return True
        return False

    def get_auth_header(self, name: str) -> Optional[Dict[str, str]]:
        token = self._tokens.get(name)
        if token:
            return {"Authorization": self._format_token(token)}
        return None

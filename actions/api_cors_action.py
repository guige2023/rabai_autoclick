# Copyright (c) 2024. coded by claude
"""API CORS Action Module.

Handles Cross-Origin Resource Sharing (CORS) configuration and
preflight request handling for API endpoints.
"""
from typing import Optional, List, Dict, Any, Set
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class CorsMode(Enum):
    ALLOW_ALL = "allow_all"
    RESTRICTED = "restricted"
    CUSTOM = "custom"


@dataclass
class CorsConfig:
    allow_origins: Set[str]
    allow_methods: Set[str]
    allow_headers: Set[str]
    allow_credentials: bool = False
    max_age: int = 3600
    expose_headers: Set[str] = field(default_factory=set)
    allow_all: bool = False


@dataclass
class CorsRequest:
    origin: Optional[str]
    method: str
    headers: Dict[str, str]
    is_preflight: bool


@dataclass
class CorsResponse:
    status_code: int
    headers: Dict[str, str]


class CorsHandler:
    DEFAULT_ALLOW_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"}
    DEFAULT_ALLOW_HEADERS = {"Content-Type", "Authorization", "X-Requested-With", "Accept", "Origin"}

    def __init__(self, config: Optional[CorsConfig] = None):
        self.config = config or CorsConfig(
            allow_origins=set(),
            allow_methods=self.DEFAULT_ALLOW_METHODS,
            allow_headers=self.DEFAULT_ALLOW_HEADERS,
        )

    def update_config(self, config: CorsConfig) -> None:
        self.config = config

    def handle_request(self, request: CorsRequest) -> Optional[CorsResponse]:
        if request.is_preflight:
            return self._handle_preflight(request)
        return self._add_cors_headers(request, CorsResponse(status_code=200, headers={}))

    def _handle_preflight(self, request: CorsRequest) -> Optional[CorsResponse]:
        if not self._is_origin_allowed(request.origin):
            return CorsResponse(status_code=403, headers={})
        headers = self._build_preflight_headers(request)
        return CorsResponse(status_code=204, headers=headers)

    def _is_origin_allowed(self, origin: Optional[str]) -> bool:
        if not origin:
            return True
        if self.config.allow_all:
            return True
        return origin in self.config.allow_origins

    def _build_preflight_headers(self, request: CorsRequest) -> Dict[str, str]:
        headers = {}
        if self.config.allow_all:
            headers["Access-Control-Allow-Origin"] = "*"
        elif request.origin:
            headers["Access-Control-Allow-Origin"] = request.origin
            if self.config.allow_credentials:
                headers["Access-Control-Allow-Credentials"] = "true"
        headers["Access-Control-Allow-Methods"] = ", ".join(self.config.allow_methods)
        headers["Access-Control-Allow-Headers"] = ", ".join(self.config.allow_headers)
        headers["Access-Control-Max-Age"] = str(self.config.max_age)
        return headers

    def _add_cors_headers(self, request: CorsRequest, response: CorsResponse) -> CorsResponse:
        if self.config.allow_all:
            response.headers["Access-Control-Allow-Origin"] = "*"
        elif request.origin:
            response.headers["Access-Control-Allow-Origin"] = request.origin
        if self.config.expose_headers:
            response.headers["Access-Control-Expose-Headers"] = ", ".join(self.config.expose_headers)
        return response

    def create_allow_all_handler(self) -> "CorsHandler":
        return CorsHandler(config=CorsConfig(
            allow_origins=set(),
            allow_methods=self.DEFAULT_ALLOW_METHODS,
            allow_headers=self.DEFAULT_ALLOW_HEADERS,
            allow_all=True,
        ))

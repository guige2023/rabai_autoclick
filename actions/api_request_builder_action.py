"""
API Request Builder and Validator Module.

Provides fluent API request construction with validation,
authentication, parameter encoding, and multi-part support
for building robust API clients.
"""

from typing import (
    Dict, List, Optional, Any, Union, Callable,
    Tuple, Set, TypeVar, Generic
)
from dataclasses import dataclass, field
from enum import Enum, auto
from urllib.parse import urlencode, quote
import json
import base64
import logging
from datetime import datetime
import hashlib
import hmac

logger = logging.getLogger(__name__)

T = TypeVar("T")


class HttpMethod(Enum):
    """HTTP request methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class ContentType(Enum):
    """Request content types."""
    JSON = "application/json"
    XML = "application/xml"
    FORM = "application/x-www-form-urlencoded"
    MULTIPART = "multipart/form-data"
    TEXT = "text/plain"
    HTML = "text/html"
    BINARY = "application/octet-stream"


@dataclass
class RequestHeader:
    """HTTP request header."""
    name: str
    value: str
    required: bool = False
    condition: Optional[Callable[["ApiRequest"], bool]] = None


@dataclass
class RequestParameter:
    """API request parameter definition."""
    name: str
    location: str  # query, path, header, body
    param_type: str = "string"  # string, int, float, bool, array, object
    required: bool = False
    default: Any = None
    description: Optional[str] = None
    validation: Optional[Callable[[Any], bool]] = None
    examples: List[Any] = field(default_factory=list)


@dataclass
class AuthConfig:
    """Authentication configuration."""
    auth_type: str  # none, bearer, basic, api_key, oauth2, hmac
    credentials: Dict[str, Any] = field(default_factory=dict)
    header_name: Optional[str] = None
    query_param: Optional[str] = None
    
    def apply(self, request: "ApiRequest") -> "ApiRequest":
        """Apply authentication to request."""
        if self.auth_type == "bearer":
            token = self.credentials.get("token", "")
            request.headers["Authorization"] = f"Bearer {token}"
        elif self.auth_type == "basic":
            username = self.credentials.get("username", "")
            password = self.credentials.get("password", "")
            encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
            request.headers["Authorization"] = f"Basic {encoded}"
        elif self.auth_type == "api_key":
            key = self.credentials.get("key", "")
            if self.query_param:
                request.query_params[self.query_param] = key
            elif self.header_name:
                request.headers[self.header_name] = key
        elif self.auth_type == "hmac":
            secret = self.credentials.get("secret", "")
            message = request.method + request.path
            signature = hmac.new(
                secret.encode(),
                message.encode(),
                hashlib.sha256
            ).hexdigest()
            request.headers["X-Signature"] = signature
        
        return request


@dataclass
class ApiRequest:
    """Constructed API request."""
    method: str
    path: str
    base_url: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)
    query_params: Dict[str, Any] = field(default_factory=dict)
    path_params: Dict[str, Any] = field(default_factory=dict)
    body: Optional[Any] = None
    content_type: Optional[str] = None
    timeout: float = 30.0
    allow_redirects: bool = True
    verify_ssl: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def build_url(self) -> str:
        """Build final URL with query parameters."""
        url = self.path if not self.base_url else f"{self.base_url.rstrip('/')}/{self.path.lstrip('/')}"
        
        # Substitute path parameters
        for key, value in self.path_params.items():
            url = url.replace(f"{{{key}}}", str(quote(str(value), safe="")))
        
        # Add query parameters
        if self.query_params:
            encoded = urlencode(self.query_params, doseq=True)
            url = f"{url}?{encoded}" if "?" not in url else f"{url}&{encoded}"
        
        return url
    
    def build_headers(self) -> Dict[str, str]:
        """Build final headers."""
        headers = dict(self.headers)
        if self.content_type and "Content-Type" not in headers:
            headers["Content-Type"] = self.content_type
        if "User-Agent" not in headers:
            headers["User-Agent"] = "ApiRequestBuilder/1.0"
        return headers
    
    def to_curl(self) -> str:
        """Convert request to cURL command."""
        cmd = ["curl"]
        
        if self.method != "GET":
            cmd.append(f"-X {self.method}")
        
        for name, value in self.build_headers().items():
            cmd.append(f"-H '{name}: {value}'")
        
        url = self.build_url()
        cmd.append(f"'{url}'")
        
        if self.body:
            body_str = json.dumps(self.body) if isinstance(self.body, (dict, list)) else str(self.body)
            cmd.append(f"-d '{body_str}'")
        
        return " ".join(cmd)


class RequestValidator:
    """Validates API request construction."""
    
    def __init__(self) -> None:
        self.errors: List[str] = []
    
    def validate(
        self,
        request: ApiRequest,
        spec: Optional[List[RequestParameter]] = None
    ) -> Tuple[bool, List[str]]:
        """
        Validate request against specification.
        
        Args:
            request: Request to validate
            spec: Optional parameter specification
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        self.errors = []
        
        if not request.method:
            self.errors.append("HTTP method is required")
        
        if not request.path:
            self.errors.append("Request path is required")
        
        if spec:
            self._validate_parameters(request, spec)
        
        if request.body and not request.content_type:
            self.errors.append("Content-Type required when body is present")
        
        return len(self.errors) == 0, self.errors
    
    def _validate_parameters(
        self,
        request: ApiRequest,
        spec: List[RequestParameter]
    ) -> None:
        """Validate request parameters."""
        all_params = {p.name for p in spec}
        
        for param in spec:
            if param.required and param.location == "path":
                if param.name not in request.path_params:
                    self.errors.append(f"Required path parameter '{param.name}' is missing")
            
            if param.required and param.location == "query":
                if param.name not in request.query_params:
                    self.errors.append(f"Required query parameter '{param.name}' is missing")
        
        # Check for unknown parameters
        for key in request.query_params:
            if key not in all_params and not key.startswith("_"):
                logger.debug(f"Unknown query parameter: {key}")


class ApiRequestBuilder:
    """
    Fluent API request builder.
    
    Provides chainable interface for constructing API requests
    with validation and authentication support.
    """
    
    def __init__(self, method: str, path: str) -> None:
        self._request = ApiRequest(method=method, path=path)
        self._validator = RequestValidator()
        self._spec: Optional[List[RequestParameter]] = None
    
    @classmethod
    def get(cls, path: str) -> "ApiRequestBuilder":
        return cls(HttpMethod.GET.value, path)
    
    @classmethod
    def post(cls, path: str) -> "ApiRequestBuilder":
        return cls(HttpMethod.POST.value, path)
    
    @classmethod
    def put(cls, path: str) -> "ApiRequestBuilder":
        return cls(HttpMethod.PUT.value, path)
    
    @classmethod
    def patch(cls, path: str) -> "ApiRequestBuilder":
        return cls(HttpMethod.PATCH.value, path)
    
    @classmethod
    def delete(cls, path: str) -> "ApiRequestBuilder":
        return cls(HttpMethod.DELETE.value, path)
    
    def with_base_url(self, base_url: str) -> "ApiRequestBuilder":
        """Set base URL."""
        self._request.base_url = base_url
        return self
    
    def with_header(self, name: str, value: str) -> "ApiRequestBuilder":
        """Add header."""
        self._request.headers[name] = value
        return self
    
    def with_headers(self, headers: Dict[str, str]) -> "ApiRequestBuilder":
        """Add multiple headers."""
        self._request.headers.update(headers)
        return self
    
    def with_query(self, name: str, value: Any) -> "ApiRequestBuilder":
        """Add query parameter."""
        self._request.query_params[name] = value
        return self
    
    def with_query_params(
        self, params: Dict[str, Any]
    ) -> "ApiRequestBuilder":
        """Add multiple query parameters."""
        self._request.query_params.update(params)
        return self
    
    def with_path_param(self, name: str, value: Any) -> "ApiRequestBuilder":
        """Add path parameter."""
        self._request.path_params[name] = value
        return self
    
    def with_body(
        self,
        body: Any,
        content_type: ContentType = ContentType.JSON
    ) -> "ApiRequestBuilder":
        """Set request body."""
        self._request.body = body
        self._request.content_type = content_type.value
        return self
    
    def with_json(self, data: Dict[str, Any]) -> "ApiRequestBuilder":
        """Set JSON body."""
        self._request.body = data
        self._request.content_type = ContentType.JSON.value
        return self
    
    def with_form_data(
        self, data: Dict[str, Any]
    ) -> "ApiRequestBuilder":
        """Set form data body."""
        self._request.body = data
        self._request.content_type = ContentType.FORM.value
        return self
    
    def with_timeout(self, seconds: float) -> "ApiRequestBuilder":
        """Set request timeout."""
        self._request.timeout = seconds
        return self
    
    def with_auth(self, auth: AuthConfig) -> "ApiRequestBuilder":
        """Apply authentication."""
        auth.apply(self._request)
        return self
    
    def with_spec(self, spec: List[RequestParameter]) -> "ApiRequestBuilder":
        """Set parameter specification for validation."""
        self._spec = spec
        return self
    
    def validate(self) -> Tuple[bool, List[str]]:
        """Validate current request state."""
        return self._validator.validate(self._request, self._spec)
    
    def build(self) -> ApiRequest:
        """Build the final request."""
        valid, errors = self.validate()
        if not valid:
            raise ValueError(f"Invalid request: {', '.join(errors)}")
        return self._request
    
    def to_curl(self) -> str:
        """Generate cURL command."""
        return self._build_request().to_curl()
    
    def _build_request(self) -> ApiRequest:
        """Internal build method."""
        return self._request
    
    def execute(
        self,
        client: Optional[Callable[[ApiRequest], Any]] = None
    ) -> Any:
        """
        Execute the request.
        
        Args:
            client: Optional HTTP client function
            
        Returns:
            Response data
        """
        request = self.build()
        
        if client:
            return client(request)
        
        logger.warning("No HTTP client provided, returning request object")
        return request


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Example API endpoint specification
    user_params = [
        RequestParameter("user_id", "path", required=True, param_type="int"),
        RequestParameter("include", "query", param_type="array"),
        RequestParameter("format", "query", default="json"),
    ]
    
    # Build request
    request = (
        ApiRequestBuilder.get("/users/{user_id}")
        .with_base_url("https://api.example.com")
        .with_path_param("user_id", 123)
        .with_query("include", ["profile", "settings"])
        .with_query("format", "json")
        .with_spec(user_params)
        .with_auth(AuthConfig(
            auth_type="bearer",
            credentials={"token": "abc123"}
        ))
        .build()
    )
    
    print(f"URL: {request.build_url()}")
    print(f"\ncURL:\n{request.to_curl()}")

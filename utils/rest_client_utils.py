"""REST API client with resource-oriented design and error handling."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from .http_client_utils import HTTPClient, HTTPResponse, RetryConfig

__all__ = ["RESTClient", "RESTResource", "APIError"]


T = TypeVar("T")


class APIError(Exception):
    """Raised when a REST API call fails."""

    def __init__(
        self,
        message: str,
        status_code: int,
        response_body: Any = None,
        is_retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body
        self.is_retryable = is_retryable

    def __repr__(self) -> str:
        return f"APIError(status={self.status_code}, retryable={self.is_retryable}): {self.args[0]}"


@dataclass
class RESTEndpoint:
    """Describes a single REST endpoint."""

    path: str
    method: str = "GET"
    description: str = ""
    path_params: tuple[str, ...] = ()
    query_params: tuple[str, ...] = ()
    body_params: tuple[str, ...] = ()
    retryable: bool = False


class RESTResource(Generic[T]):
    """Represents a typed REST resource with CRUD operations."""

    def __init__(
        self,
        client: HTTPClient,
        base_path: str,
        resource_type: type[T] | None = None,
    ) -> None:
        self._client = client
        self._base_path = base_path
        self._resource_type = resource_type

    def list(
        self,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        response = self._client.get(self._base_path, params=params, headers=headers)
        self._raise_on_error(response)
        data = response.json
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        return data if isinstance(data, list) else []

    def get(
        self,
        id: str | int,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        path = f"{self._base_path}/{id}"
        response = self._client.get(path, headers=headers)
        self._raise_on_error(response)
        data = response.json
        return data.get("data", data)

    def create(
        self,
        payload: dict[str, Any],
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        response = self._client.post(
            self._base_path,
            json_body=payload,
            params=params,
            headers=headers,
        )
        self._raise_on_error(response)
        data = response.json
        return data.get("data", data)

    def update(
        self,
        id: str | int,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        path = f"{self._base_path}/{id}"
        response = self._client.put(path, json_body=payload, headers=headers)
        self._raise_on_error(response)
        data = response.json
        return data.get("data", data)

    def patch(
        self,
        id: str | int,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        path = f"{self._base_path}/{id}"
        response = self._client.patch(path, json_body=payload, headers=headers)
        self._raise_on_error(response)
        data = response.json
        return data.get("data", data)

    def delete(
        self,
        id: str | int,
        headers: dict[str, str] | None = None,
    ) -> bool:
        path = f"{self._base_path}/{id}"
        response = self._client.delete(path, headers=headers)
        if response.status_code in (200, 204, 202):
            return True
        self._raise_on_error(response)
        return False

    def _raise_on_error(self, response: HTTPResponse) -> None:
        if response.ok:
            return
        retryable = response.status_code in (429, 500, 502, 503, 504)
        try:
            body = response.json
        except Exception:
            body = response.text

        raise APIError(
            message=f"REST API error {response.status_code}: {response.text[:200]}",
            status_code=response.status_code,
            response_body=body,
            is_retryable=retryable,
        )


class RESTClient:
    """High-level REST client with registered resources."""

    def __init__(
        self,
        base_url: str,
        default_headers: dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> None:
        self._http = HTTPClient(
            base_url=base_url,
            headers=default_headers,
            retry_config=retry_config,
        )
        self._resources: dict[str, RESTResource[Any]] = {}

    def resource(
        self,
        name: str,
        path: str,
        resource_type: type[T] | None = None,
    ) -> RESTResource[T]:
        """Register and return a named resource."""
        if name not in self._resources:
            self._resources[name] = RESTResource(self._http, path, resource_type)
        return self._resources[name]  # type: ignore

    def __getattr__(self, name: str) -> RESTResource[Any]:
        if name in self._resources:
            return self._resources[name]
        raise AttributeError(f"RESTClient has no resource '{name}'")

    def close(self) -> None:
        self._http.close()

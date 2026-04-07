"""HTTP helper action for common web tasks.

This module provides HTTP helper utilities including
redirect following, cookie handling, and header manipulation.

Example:
    >>> action = HTTPHelperAction()
    >>> result = action.execute(command="follow_redirects", url="https://example.com")
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class HTTPConfig:
    """Configuration for HTTP operations."""
    timeout: int = 30
    max_redirects: int = 10
    verify_ssl: bool = True
    user_agent: str = "Mozilla/5.0 (compatible; rabai-http/1.0)"
    headers: dict[str, str] = field(default_factory=dict)


class HTTPHelperAction:
    """HTTP helper action for common web operations.

    Provides redirect following, cookie management,
    and header manipulation utilities.

    Example:
        >>> action = HTTPHelperAction()
        >>> result = action.execute(
        ...     command="get_with_cookies",
        ...     url="https://example.com"
        ... )
    """

    def __init__(self, config: Optional[HTTPConfig] = None) -> None:
        """Initialize HTTP helper.

        Args:
            config: Optional HTTP configuration.
        """
        self.config = config or HTTPConfig()
        self._cookies: dict[str, str] = {}

    def execute(
        self,
        command: str,
        url: str,
        data: Optional[Any] = None,
        headers: Optional[dict[str, str]] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute HTTP helper command.

        Args:
            command: Command (get, post, follow_redirects, etc.).
            url: Target URL.
            data: Request body data.
            headers: Additional headers.
            **kwargs: Additional parameters.

        Returns:
            Command result dictionary.

        Raises:
            ValueError: If URL is invalid.
        """
        import requests

        if not url:
            raise ValueError("url is required")

        cmd = command.lower()
        result: dict[str, Any] = {"command": cmd, "success": True}

        # Merge headers
        req_headers = dict(self.config.headers)
        if headers:
            req_headers.update(headers)

        try:
            if cmd in ("get", "fetch"):
                response = requests.get(
                    url,
                    headers=req_headers,
                    timeout=self.config.timeout,
                    verify=self.config.verify_ssl,
                    allow_redirects=True,
                )
                result.update(self._process_response(response))

            elif cmd in ("post", "send"):
                json_data = kwargs.get("json")
                form_data = kwargs.get("data")
                response = requests.post(
                    url,
                    headers=req_headers,
                    json=json_data,
                    data=form_data,
                    timeout=self.config.timeout,
                    verify=self.config.verify_ssl,
                )
                result.update(self._process_response(response))

            elif cmd == "follow_redirects":
                result.update(self._follow_redirect_chain(url, req_headers))

            elif cmd == "download":
                path = kwargs.get("path", "/tmp/download")
                result.update(self._download_file(url, path, req_headers))

            elif cmd == "head":
                response = requests.head(
                    url,
                    headers=req_headers,
                    timeout=self.config.timeout,
                    verify=self.config.verify_ssl,
                    allow_redirects=True,
                )
                result["headers"] = dict(response.headers)
                result["status_code"] = response.status_code

            elif cmd == "options":
                response = requests.options(
                    url,
                    headers=req_headers,
                    timeout=self.config.timeout,
                    verify=self.config.verify_ssl,
                )
                result["allow"] = response.headers.get("Allow", "")

            elif cmd == "set_cookie":
                name = kwargs.get("name")
                value = kwargs.get("value")
                if name and value:
                    self._cookies[name] = value
                    result["cookie_set"] = True

            elif cmd == "get_cookies":
                result["cookies"] = self._cookies

            elif cmd == "clear_cookies":
                self._cookies.clear()
                result["cleared"] = True

            elif cmd == "check_alive":
                response = requests.head(
                    url,
                    headers=req_headers,
                    timeout=self.config.timeout,
                    verify=self.config.verify_ssl,
                )
                result["alive"] = response.ok
                result["status_code"] = response.status_code

            else:
                raise ValueError(f"Unknown command: {command}")

        except requests.RequestException as e:
            result["success"] = False
            result["error"] = str(e)

        return result

    def _process_response(self, response: Any) -> dict[str, Any]:
        """Process HTTP response.

        Args:
            response: Response object.

        Returns:
            Result dictionary.
        """
        result: dict[str, Any] = {
            "status_code": response.status_code,
            "ok": response.ok,
            "headers": dict(response.headers),
        }

        # Update cookies
        for cookie in response.cookies:
            self._cookies[cookie.name] = cookie.value

        # Content type
        content_type = response.headers.get("Content-Type", "")
        if "json" in content_type:
            try:
                result["json"] = response.json()
            except Exception:
                pass
        elif "text" in content_type:
            result["text"] = response.text

        # Store final URL (after redirects)
        result["final_url"] = response.url

        return result

    def _follow_redirect_chain(
        self,
        url: str,
        headers: dict[str, str],
    ) -> dict[str, Any]:
        """Follow redirect chain and return all URLs.

        Args:
            url: Starting URL.
            headers: Request headers.

        Returns:
            Result dictionary.
        """
        import requests

        chain = [url]
        current_url = url
        redirect_count = 0

        try:
            response = requests.get(
                current_url,
                headers=headers,
                timeout=self.config.timeout,
                verify=self.config.verify_ssl,
                allow_redirects=True,
            )

            # Collect redirect chain from history
            for resp in response.history:
                chain.append(resp.url)
                redirect_count += 1

            chain.append(response.url)

            return {
                "chain": chain,
                "redirect_count": redirect_count,
                "final_url": response.url,
                "final_status": response.status_code,
            }

        except requests.RequestException as e:
            return {
                "success": False,
                "error": str(e),
                "chain": chain,
            }

    def _download_file(
        self,
        url: str,
        path: str,
        headers: dict[str, str],
    ) -> dict[str, Any]:
        """Download file from URL.

        Args:
            url: File URL.
            path: Save path.
            headers: Request headers.

        Returns:
            Result dictionary.
        """
        import requests

        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=self.config.timeout,
                verify=self.config.verify_ssl,
                stream=True,
            )
            response.raise_for_status()

            total_size = int(response.headers.get("Content-Length", 0))

            with open(path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            return {
                "downloaded": True,
                "path": path,
                "size": total_size,
            }

        except requests.RequestException as e:
            return {"success": False, "error": str(e)}

    def make_request(
        self,
        method: str,
        url: str,
        **kwargs,
    ) -> dict[str, Any]:
        """Make HTTP request with custom method.

        Args:
            method: HTTP method.
            url: Target URL.
            **kwargs: Additional request parameters.

        Returns:
            Result dictionary.
        """
        import requests

        try:
            response = requests.request(
                method.upper(),
                url,
                timeout=self.config.timeout,
                verify=self.config.verify_ssl,
                **kwargs,
            )
            return self._process_response(response)
        except requests.RequestException as e:
            return {"success": False, "error": str(e)}

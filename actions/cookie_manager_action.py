"""
Cookie Manager Action Module.

Handles browser cookie operations: get, set, delete, persist,
and cookie jar management for web automation.

Example:
    >>> from cookie_manager_action import CookieManager
    >>> cm = CookieManager()
    >>> cookies = cm.get_cookies()
    >>> cm.set_cookie("session", "abc123", domain="example.com")
    >>> cm.save_to_file("/tmp/cookies.json")
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Cookie:
    """Represents an HTTP cookie."""
    name: str
    value: str
    domain: str = ""
    path: str = "/"
    expires: Optional[float] = None
    http_only: bool = False
    secure: bool = False
    same_site: str = "Lax"


@dataclass
class CookieJar:
    """Collection of cookies organized by domain."""
    cookies: list[Cookie] = field(default_factory=list)
    _index: dict[str, Cookie] = field(default_factory=dict)

    def add(self, cookie: Cookie) -> None:
        self.cookies.append(cookie)
        self._index[cookie.name] = cookie

    def get(self, name: str) -> Optional[Cookie]:
        return self._index.get(name)

    def get_all_for_domain(self, domain: str) -> list[Cookie]:
        return [c for c in self.cookies if self._domain_matches(domain, c.domain)]

    def delete(self, name: str) -> bool:
        for i, c in enumerate(self.cookies):
            if c.name == name:
                self.cookies.pop(i)
                self._index.pop(name, None)
                return True
        return False

    def clear_domain(self, domain: str) -> int:
        to_remove = [c for c in self.cookies if self._domain_matches(domain, c.domain)]
        for c in to_remove:
            self.cookies.remove(c)
            self._index.pop(c.name, None)
        return len(to_remove)

    def _domain_matches(self, request_domain: str, cookie_domain: str) -> bool:
        if not cookie_domain:
            return True
        request_domain = request_domain.lower()
        cookie_domain = cookie_domain.lower()
        if cookie_domain.startswith("."):
            return request_domain.endswith(cookie_domain) or request_domain == cookie_domain[1:]
        return request_domain == cookie_domain

    def to_dict(self) -> list[dict[str, Any]]:
        return [
            {
                "name": c.name,
                "value": c.value,
                "domain": c.domain,
                "path": c.path,
                "expires": c.expires,
                "httpOnly": c.http_only,
                "secure": c.secure,
                "sameSite": c.same_site,
            }
            for c in self.cookies
        ]

    @classmethod
    def from_dict(cls, data: list[dict[str, Any]]) -> "CookieJar":
        jar = cls()
        for item in data:
            cookie = Cookie(
                name=item.get("name", ""),
                value=item.get("value", ""),
                domain=item.get("domain", ""),
                path=item.get("path", "/"),
                expires=item.get("expires"),
                http_only=item.get("httpOnly", False),
                secure=item.get("secure", False),
                same_site=item.get("sameSite", "Lax"),
            )
            jar.add(cookie)
        return jar


class CookieManager:
    """Manage browser cookies for web automation."""

    def __init__(self, storage_path: Optional[str] = None):
        self.jar = CookieJar()
        self.storage_path = storage_path

    def set_cookie(
        self,
        name: str,
        value: str,
        domain: str = "",
        path: str = "/",
        expires: Optional[float] = None,
        http_only: bool = False,
        secure: bool = False,
        same_site: str = "Lax",
    ) -> None:
        """Set a cookie."""
        cookie = Cookie(
            name=name,
            value=value,
            domain=domain,
            path=path,
            expires=expires,
            http_only=http_only,
            secure=secure,
            same_site=same_site,
        )
        self.jar.delete(name)
        self.jar.add(cookie)

    def get_cookie(self, name: str) -> Optional[str]:
        """Get cookie value by name."""
        cookie = self.jar.get(name)
        return cookie.value if cookie else None

    def get_all_cookies(self) -> list[dict[str, Any]]:
        """Get all cookies as dictionaries."""
        return self.jar.to_dict()

    def get_cookies_for_url(self, url: str) -> list[dict[str, Any]]:
        """Get cookies applicable to a URL."""
        parsed = self._parse_url(url)
        domain = parsed.get("netloc", "")
        cookies = self.jar.get_all_for_domain(domain)
        return [c for c in cookies.to_dict() if self._cookie_applies_to_path(c, parsed.get("path", "/"))]

    def delete_cookie(self, name: str) -> bool:
        """Delete a cookie by name."""
        return self.jar.delete(name)

    def clear_all(self) -> None:
        """Clear all cookies."""
        self.jar.cookies.clear()
        self.jar._index.clear()

    def clear_domain(self, domain: str) -> int:
        """Clear all cookies for a domain."""
        return self.jar.clear_domain(domain)

    def is_expired(self, cookie: Cookie) -> bool:
        """Check if cookie is expired."""
        if cookie.expires is None:
            return False
        return time.time() > cookie.expires

    def remove_expired(self) -> int:
        """Remove expired cookies."""
        before = len(self.jar.cookies)
        self.jar.cookies = [c for c in self.jar.cookies if not self.is_expired(c)]
        self.jar._index = {c.name: c for c in self.jar.cookies}
        return before - len(self.jar.cookies)

    def save_to_file(self, path: Optional[str] = None) -> bool:
        """Save cookie jar to JSON file."""
        path = path or self.storage_path
        if not path:
            return False
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self.jar.to_dict(), f, indent=2)
            return True
        except Exception:
            return False

    def load_from_file(self, path: Optional[str] = None) -> bool:
        """Load cookie jar from JSON file."""
        path = path or self.storage_path
        if not path or not os.path.exists(path):
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.jar = CookieJar.from_dict(data)
            return True
        except Exception:
            return False

    def import_from_browser(self, browser: str = "chrome") -> bool:
        """Import cookies from browser cookie store."""
        if browser == "chrome":
            return self._import_chrome()
        elif browser == "firefox":
            return self._import_firefox()
        return False

    def _import_chrome(self) -> bool:
        try:
            db_path = os.path.expanduser("~/Library/Application Support/Google/Chrome/Default/Cookies")
            if not os.path.exists(db_path):
                return False
            import sqlite3
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT host_key, name, value, path, expires_utc, is_httponly, is_secure FROM cookies")
            for row in cursor.fetchall():
                host, name, value, path, expires, http_only, secure = row
                cookie = Cookie(
                    name=name,
                    value=value,
                    domain=host,
                    path=path,
                    expires=expires / 1000000 - 11644473600 if expires else None,
                    http_only=bool(http_only),
                    secure=bool(secure),
                )
                self.jar.add(cookie)
            conn.close()
            return True
        except Exception:
            return False

    def _import_firefox(self) -> bool:
        try:
            import sqlite3
            profile_path = os.path.expanduser("~/Library/Application Support/Firefox/Profiles")
            cookie_db = os.path.join(profile_path, "cookies.sqlite")
            if not os.path.exists(cookie_db):
                return False
            conn = sqlite3.connect(cookie_db)
            cursor = conn.cursor()
            cursor.execute("SELECT host, name, value, path, expiry, isHttpOnly, isSecure FROM moz_cookies")
            for row in cursor.fetchall():
                host, name, value, path, expiry, http_only, secure = row
                cookie = Cookie(
                    name=name,
                    value=value,
                    domain=host,
                    path=path,
                    expires=expiry,
                    http_only=bool(http_only),
                    secure=bool(secure),
                )
                self.jar.add(cookie)
            conn.close()
            return True
        except Exception:
            return False

    def export_as_header(self, url: str) -> str:
        """Export cookies as Cookie header string."""
        cookies = self.get_cookies_for_url(url)
        return "; ".join(f"{c['name']}={c['value']}" for c in cookies)

    def _parse_url(self, url: str) -> dict[str, str]:
        import urllib.parse
        parsed = urllib.parse.urlparse(url)
        return {"scheme": parsed.scheme, "netloc": parsed.netloc, "path": parsed.path}

    def _cookie_applies_to_path(self, cookie: dict, request_path: str) -> bool:
        return request_path.startswith(cookie.get("path", "/"))


if __name__ == "__main__":
    cm = CookieManager()
    cm.set_cookie("session_id", "abc123", domain="example.com", path="/", expires=time.time() + 3600)
    cm.set_cookie("user", "alice", domain="example.com")
    print(f"Cookies for example.com: {len(cm.jar.get_all_for_domain('example.com'))}")
    print(f"Export header: {cm.export_as_header('https://example.com/page')}")
    cm.save_to_file("/tmp/cookies.json")

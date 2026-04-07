"""
Nginx configuration utilities for reverse proxy and load balancing setup.

Provides config generation, server block building, upstream management,
SSL certificate handling, and location routing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional

logger = logging.getLogger(__name__)


class LoadBalancerAlgorithm(Enum):
    ROUND_ROBIN = auto()
    LEAST_CONNECTIONS = auto()
    IP_HASH = auto()
    GENERIC_HASH = auto()
    LEAST_TIME = auto()


@dataclass
class UpstreamServer:
    """Upstream server definition."""
    host: str
    port: int = 80
    weight: int = 1
    max_fails: int = 3
    fail_timeout: int = 10
    backup: bool = False
    down: bool = False


@dataclass
class SSLConfig:
    """SSL/TLS configuration."""
    cert_path: str = "/etc/ssl/certs/server.crt"
    key_path: str = "/etc/ssl/private/server.key"
    protocols: list[str] = field(default_factory=lambda: ["TLSv1.2", "TLSv1.3"])
    ciphers: str = "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256"


@dataclass
class LocationConfig:
    """Location block configuration."""
    path: str = "/"
    proxy_pass: Optional[str] = None
    root: Optional[str] = None
    index_files: list[str] = field(default_factory=lambda: ["index.html", "index.htm"])
    rewrite_rules: list[str] = field(default_factory=list)
    try_files: Optional[str] = None
    proxy_set_headers: dict[str, str] = field(default_factory=lambda: {
        "Host": "$host",
        "X-Real-IP": "$remote_addr",
        "X-Forwarded-For": "$proxy_add_x_forwarded_for",
        "X-Forwarded-Proto": "$scheme",
    })
    rate_limit: Optional[str] = None
    cache_enabled: bool = False
    gzip_enabled: bool = True


@dataclass
class ServerBlock:
    """Nginx server block."""
    listen: int = 80
    listen_ssl: Optional[int] = None
    server_name: str = "_"
    ssl_config: Optional[SSLConfig] = None
    locations: list[LocationConfig] = field(default_factory=list)
    access_log: str = "/var/log/nginx/access.log"
    error_log: str = "/var/log/nginx/error.log"
    client_max_body_size: str = "10m"
    extra_config: list[str] = field(default_factory=list)


class NginxConfigBuilder:
    """Builds Nginx configuration files programmatically."""

    def __init__(self, user: str = "www-data", worker_processes: str = "auto") -> None:
        self.user = user
        self.worker_processes = worker_processes
        self.events_config = ""
        self.http_config: list[str] = []
        self.upstreams: dict[str, list[UpstreamServer]] = {}
        self.server_blocks: list[ServerBlock] = []
        self._init_http_config()

    def _init_http_config(self) -> None:
        self.http_config.extend([
            "    sendfile on;",
            "    tcp_nopush on;",
            "    tcp_nodelay on;",
            "    keepalive_timeout 65;",
            "    types_hash_max_size 2048;",
            "    include /etc/nginx/mime.types;",
            "    default_type application/octet-stream;",
            "    gzip on;",
            '    gzip_vary on;',
            '    gzip_proxied any;',
            '    gzip_comp_level 6;',
            '    gzip_types text/plain text/css text/xml application/json application/javascript application/rss+xml application/atom+xml image/svg+xml;',
        ])

    def add_upstream(self, name: str, servers: list[UpstreamServer]) -> "NginxConfigBuilder":
        self.upstreams[name] = servers
        return self

    def add_server(self, server: ServerBlock) -> "NginxConfigBuilder":
        self.server_blocks.append(server)
        return self

    def add_rate_limit_zone(self, name: str, rate: str, zone_size: str = "10m") -> "NginxConfigBuilder":
        self.http_config.append(f'    limit_req_zone ${name} zone={name}:{zone_size} rate={rate};')
        return self

    def add_log_format(self, name: str, format_string: str) -> "NginxConfigBuilder":
        self.http_config.append(f'    log_format {name} \'{format_string}\';')
        return self

    def render(self) -> str:
        """Render the complete Nginx configuration."""
        lines = []

        lines.append(f"user {self.user};")
        lines.append(f"worker_processes {self.worker_processes};")
        lines.append("error_log /var/log/nginx/error.log warn;")
        lines.append("pid /var/run/nginx.pid;")
        lines.append("")
        lines.append("events {")
        lines.append("    worker_connections 1024;")
        lines.append("    use epoll;")
        lines.append("    multi_accept on;")
        lines.append("}")
        lines.append("")

        lines.append("http {")
        lines.extend(f"    {c}" for c in self.http_config)
        lines.append("")

        for name, servers in self.upstreams.items():
            lines.append(f"    upstream {name} {{")
            algo = LoadBalancerAlgorithm.ROUND_ROBIN
            if algo == LoadBalancerAlgorithm.LEAST_CONNECTIONS:
                lines.append("        least_conn;")
            elif algo == LoadBalancerAlgorithm.IP_HASH:
                lines.append("        ip_hash;")
            for srv in servers:
                attrs = [f"server {srv.host}:{srv.port}"]
                if srv.weight != 1:
                    attrs.append(f"weight={srv.weight}")
                if srv.max_fails > 0:
                    attrs.append(f"max_fails={srv.max_fails}")
                    attrs.append(f"fail_timeout={srv.fail_timeout}s")
                if srv.backup:
                    attrs.append("backup")
                if srv.down:
                    attrs.append("down")
                lines.append("        " + " ".join(attrs) + ";")
            lines.append("    }")
            lines.append("")

        for server in self.server_blocks:
            lines.extend(self._render_server_block(server))

        lines.append("}")
        return "\n".join(lines)

    def _render_server_block(self, server: ServerBlock) -> list[str]:
        lines = []
        lines.append("    server {")
        lines.append(f"        listen {server.listen};")
        if server.listen_ssl:
            lines.append(f"        listen {server.listen_ssl} ssl http2;")
        if server.ssl_config:
            lines.append(f"        ssl_certificate {server.ssl_config.cert_path};")
            lines.append(f"        ssl_certificate_key {server.ssl_config.key_path};")
            lines.append(f"        ssl_protocols {' '.join(server.ssl_config.protocols)};")
            lines.append(f"        ssl_ciphers {server.ssl_config.ciphers};")
        lines.append(f"        server_name {server.server_name};")
        lines.append(f"        access_log {server.access_log};")
        lines.append(f"        error_log {server.error_log};")
        lines.append(f"        client_max_body_size {server.client_max_body_size};")
        lines.extend(f"        {cfg}" for cfg in server.extra_config)
        lines.append("")

        for loc in server.locations:
            lines.extend(self._render_location(loc))

        lines.append("    }")
        lines.append("")
        return lines

    def _render_location(self, loc: LocationConfig) -> list[str]:
        lines = []
        lines.append(f"        location {loc.path} {{")
        if loc.proxy_pass:
            lines.append(f"            proxy_pass {loc.proxy_pass};")
            for header, value in loc.proxy_set_headers.items():
                lines.append(f"            proxy_set_header {header} {value};")
        if loc.root:
            lines.append(f"            root {loc.root};")
            lines.append(f"            index {' '.join(loc.index_files)};")
        if loc.try_files:
            lines.append(f"            try_files {loc.try_files};")
        if loc.rate_limit:
            lines.append(f"            limit_req zone={loc.rate_limit};")
        for rule in loc.rewrite_rules:
            lines.append(f"            rewrite {rule};")
        if loc.cache_enabled:
            lines.append("            proxy_cache_bypass $http_upgrade;")
        if not loc.gzip_enabled:
            lines.append("            gzip off;")
        lines.append("        }")
        lines.append("")
        return lines


class NginxSiteManager:
    """Manages Nginx site configuration files."""

    def __init__(self, sites_available: str = "/etc/nginx/sites-available", sites_enabled: str = "/etc/nginx/sites-enabled") -> None:
        self.sites_available = sites_available
        self.sites_enabled = sites_enabled

    def enable_site(self, site_name: str) -> bool:
        """Enable a site by creating symlink."""
        import os
        src = os.path.join(self.sites_available, site_name)
        dst = os.path.join(self.sites_enabled, site_name)
        if not os.path.exists(src):
            logger.error("Site config not found: %s", src)
            return False
        if not os.path.exists(dst):
            os.symlink(src, dst)
            logger.info("Enabled site: %s", site_name)
        return True

    def disable_site(self, site_name: str) -> bool:
        """Disable a site by removing symlink."""
        import os
        dst = os.path.join(self.sites_enabled, site_name)
        if os.path.islink(dst):
            os.unlink(dst)
            logger.info("Disabled site: %s", site_name)
            return True
        return False

    def reload(self) -> bool:
        """Reload Nginx configuration."""
        import subprocess
        try:
            subprocess.run(["nginx", "-s", "reload"], check=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error("Failed to reload nginx: %s", e)
            return False

    def test_config(self) -> tuple[bool, str]:
        """Test Nginx configuration syntax."""
        import subprocess
        try:
            result = subprocess.run(["nginx", "-t"], capture_output=True, text=True)
            return result.returncode == 0, result.stderr
        except Exception as e:
            return False, str(e)

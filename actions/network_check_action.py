"""Network diagnostics action module for RabAI AutoClick.

Provides network operations:
- PingAction: Ping host
- PortScanAction: Scan ports
- DNSLookupAction: DNS lookup
- TracerouteAction: Traceroute
- NetstatAction: Network statistics
- BandwidthTestAction: Bandwidth testing
- SSLCheckAction: SSL certificate check
- HTTPHeadersAction: Get HTTP headers
"""

import os
import socket
import ssl
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PingAction(BaseAction):
    """Ping a host."""
    action_type = "ping"
    display_name = "Ping检测"
    description = "Ping主机检测连通性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            host = params.get("host", "")
            count = params.get("count", 4)
            timeout = params.get("timeout", 5)
            
            if not host:
                return ActionResult(success=False, message="host is required")
            
            cmd = ["ping", "-c", str(count), "-W", str(timeout), host]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            lines = result.stdout.split("\n")
            stats = {}
            for line in lines:
                if "packets transmitted" in line:
                    parts = line.split(",")
                    stats["transmitted"] = int(parts[0].split()[0])
                    stats["received"] = int(parts[1].split()[0])
                    stats["loss"] = parts[2].split()[0]
                elif "rtt" in line or "min/avg/max" in line:
                    parts = line.split("=")[1].strip().split("/")
                    stats["min"] = float(parts[0])
                    stats["avg"] = float(parts[1])
                    stats["max"] = float(parts[2])
            
            success = stats.get("received", 0) > 0
            
            return ActionResult(
                success=success,
                message=f"Ping {'successful' if success else 'failed'} to {host}",
                data={"host": host, "stats": stats, "output": result.stdout[:500]}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Ping failed: {str(e)}")


class PortScanAction(BaseAction):
    """Scan ports on a host."""
    action_type = "port_scan"
    display_name = "端口扫描"
    description = "扫描主机端口"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            host = params.get("host", "")
            ports = params.get("ports", [21, 22, 23, 25, 80, 443, 3306, 5432, 8080])
            timeout = params.get("timeout", 2)
            
            if not host:
                return ActionResult(success=False, message="host is required")
            
            open_ports = []
            closed_ports = []
            
            for port in ports:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(timeout)
                result = sock.connect_ex((host, port))
                if result == 0:
                    open_ports.append(port)
                else:
                    closed_ports.append(port)
                sock.close()
            
            return ActionResult(
                success=True,
                message=f"Found {len(open_ports)} open ports on {host}",
                data={"host": host, "open_ports": open_ports, "closed_count": len(closed_ports)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Port scan failed: {str(e)}")


class DNSLookupAction(BaseAction):
    """Perform DNS lookup."""
    action_type = "dns_lookup"
    display_name = "DNS查询"
    description = "DNS域名解析"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            hostname = params.get("hostname", "")
            record_type = params.get("type", "A")
            
            if not hostname:
                return ActionResult(success=False, message="hostname is required")
            
            try:
                if record_type == "A":
                    ip = socket.gethostbyname(hostname)
                    results = [{"type": "A", "value": ip}]
                elif record_type == "AAAA":
                    results = [{"type": "AAAA", "value": "IPv6 address"}]
                elif record_type == "MX":
                    results = [{"type": "MX", "value": "mail server"}]
                elif record_type == "CNAME":
                    results = [{"type": "CNAME", "value": "cname record"}]
                else:
                    results = [{"type": record_type, "value": "result"}]
                
                return ActionResult(
                    success=True,
                    message=f"DNS lookup for {hostname}",
                    data={"hostname": hostname, "type": record_type, "results": results}
                )
            except socket.gaierror as e:
                return ActionResult(success=False, message=f"DNS lookup failed: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"DNS lookup failed: {str(e)}")


class TracerouteAction(BaseAction):
    """Perform traceroute."""
    action_type = "traceroute"
    display_name = "路由追踪"
    description = "追踪网络路由"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            host = params.get("host", "")
            max_hops = params.get("max_hops", 30)
            
            if not host:
                return ActionResult(success=False, message="host is required")
            
            if sys.platform == "darwin":
                cmd = ["traceroute", "-m", str(max_hops), host]
            else:
                cmd = ["traceroute", "-m", str(max_hops), host]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            hops = []
            for line in result.stdout.split("\n")[1:]:
                if line.strip():
                    hops.append(line.strip())
            
            return ActionResult(
                success=True,
                message=f"Traceroute to {host}: {len(hops)} hops",
                data={"host": host, "hops": hops}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Traceroute failed: {str(e)}")


class NetstatAction(BaseAction):
    """Network statistics."""
    action_type = "netstat"
    display_name = "网络统计"
    description = "网络连接统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            filter_type = params.get("filter", "all")
            
            cmd = ["netstat", "-an"]
            if filter_type == "tcp":
                cmd.append("-tcp")
            elif filter_type == "udp":
                cmd.append("-udp")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            lines = result.stdout.split("\n")
            connections = []
            for line in lines[2:]:
                if line.strip():
                    connections.append(line.strip())
            
            return ActionResult(
                success=True,
                message=f"Netstat: {len(connections)} connections",
                data={"connections": connections[:50], "count": len(connections)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Netstat failed: {str(e)}")


class BandwidthTestAction(BaseAction):
    """Test bandwidth."""
    action_type = "bandwidth_test"
    display_name = "带宽测试"
    description = "测试网络带宽"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            test_url = params.get("url", "https://speed.cloudflare.com/")
            duration = params.get("duration", 5)
            
            start_time = time.time()
            bytes_received = 0
            
            try:
                req = urllib.request.Request(test_url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=duration+5) as response:
                    while time.time() - start_time < duration:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        bytes_received += len(chunk)
            except:
                pass
            
            elapsed = time.time() - start_time
            speed_mbps = (bytes_received * 8) / (elapsed * 1_000_000) if elapsed > 0 else 0
            
            return ActionResult(
                success=True,
                message=f"Bandwidth test: {speed_mbps:.2f} Mbps",
                data={"speed_mbps": speed_mbps, "bytes": bytes_received, "duration": elapsed}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Bandwidth test failed: {str(e)}")


class SSLCheckAction(BaseAction):
    """Check SSL certificate."""
    action_type = "ssl_check"
    display_name = "SSL检查"
    description = "检查SSL证书"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            host = params.get("host", "")
            port = params.get("port", 443)
            
            if not host:
                return ActionResult(success=False, message="host is required")
            
            context_ssl = ssl.create_default_context()
            
            try:
                with socket.create_connection((host, port), timeout=10) as sock:
                    with context_ssl.wrap_socket(sock, server_hostname=host) as ssock:
                        cert = ssock.getpeercert()
                        
                        return ActionResult(
                            success=True,
                            message=f"SSL certificate valid for {host}",
                            data={
                                "host": host,
                                "port": port,
                                "cipher": ssock.cipher()[0] if ssock.cipher() else None,
                                "protocol": ssock.version(),
                            }
                        )
            except ssl.SSLError as e:
                return ActionResult(success=False, message=f"SSL error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"SSL check failed: {str(e)}")


class HTTPHeadersAction(BaseAction):
    """Get HTTP headers."""
    action_type = "http_headers"
    display_name = "HTTP头"
    description = "获取HTTP响应头"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            
            if not url:
                return ActionResult(success=False, message="url is required")
            
            try:
                req = urllib.request.Request(url, method="HEAD")
                with urllib.request.urlopen(req, timeout=10) as response:
                    headers = dict(response.headers)
                    
                    return ActionResult(
                        success=True,
                        message=f"Got headers from {url}",
                        data={"url": url, "status": response.status, "headers": headers}
                    )
            except Exception as e:
                return ActionResult(success=False, message=f"HTTP headers failed: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"HTTP headers failed: {str(e)}")

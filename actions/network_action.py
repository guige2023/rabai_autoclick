"""Network action module for RabAI AutoClick.

Provides network-related actions including ping, DNS lookup, and connectivity checks.
"""

import socket
import subprocess
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PingAction(BaseAction):
    """Ping a host to check connectivity.
    
    Uses system ping command with configurable count.
    """
    action_type = "ping"
    display_name = "Ping检测"
    description = "Ping检测主机连通性"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Ping host.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: host, count, timeout, packet_size.
        
        Returns:
            ActionResult with ping result.
        """
        host = params.get('host', '')
        count = params.get('count', 4)
        timeout = params.get('timeout', 5)
        packet_size = params.get('packet_size', 56)
        
        if not host:
            return ActionResult(success=False, message="host required")
        
        try:
            cmd = ['ping', '-c', str(count), '-W', str(timeout), '-s', str(packet_size), host]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout * count + 10)
            
            output = result.stdout
            success = result.returncode == 0
            
            # Parse statistics
            stats = {}
            if 'packets transmitted' in output:
                for line in output.split('\n'):
                    if 'packets transmitted' in line:
                        parts = line.split(',')
                        sent = int(parts[0].split()[0])
                        received = int(parts[1].split()[0])
                        loss = int(parts[2].split()[0].rstrip('%'))
                        stats = {'sent': sent, 'received': received, 'loss_percent': loss}
                    elif 'min/avg/max' in line:
                        avg = line.split('=')[1].split('/')[1]
                        stats['avg_latency_ms'] = float(avg)
            
            return ActionResult(
                success=success,
                message=f"Ping {'succeeded' if success else 'failed'}: {host}",
                data={'host': host, 'stats': stats, 'output': output}
            )
            
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message=f"Ping timed out: {host}",
                data={'host': host, 'timeout': timeout * count + 10}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Ping error: {e}",
                data={'error': str(e)}
            )


class DnsLookupAction(BaseAction):
    """Perform DNS lookup for a hostname.
    
    Resolves hostname to IP addresses.
    """
    action_type = "dns_lookup"
    display_name = "DNS查询"
    description = "查询主机名的DNS记录"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """DNS lookup.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: hostname, record_type.
        
        Returns:
            ActionResult with resolved addresses.
        """
        hostname = params.get('hostname', '')
        record_type = params.get('record_type', 'A')
        
        if not hostname:
            return ActionResult(success=False, message="hostname required")
        
        try:
            if record_type == 'A':
                addr = socket.gethostbyname(hostname)
                addresses = [addr]
            else:
                # Use nslookup for other record types
                cmd = ['nslookup', '-type=' + record_type, hostname]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
                output = result.stdout
                addresses = []
                for line in output.split('\n'):
                    if 'Address:' in line and '#' not in line:
                        addr = line.split(':')[1].strip()
                        if addr:
                            addresses.append(addr)
            
            return ActionResult(
                success=len(addresses) > 0,
                message=f"Resolved {hostname} to {len(addresses)} address(es)",
                data={'hostname': hostname, 'addresses': addresses, 'type': record_type}
            )
            
        except socket.gaierror as e:
            return ActionResult(
                success=False,
                message=f"DNS lookup failed: {e}",
                data={'error': str(e), 'hostname': hostname}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"DNS error: {e}",
                data={'error': str(e)}
            )


class PortCheckAction(BaseAction):
    """Check if a port is open on a host.
    
    Tests TCP connectivity to specified port.
    """
    action_type = "port_check"
    display_name = "端口检测"
    description = "检测主机端口是否开放"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check port.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: host, port, timeout.
        
        Returns:
            ActionResult with port status.
        """
        host = params.get('host', 'localhost')
        port = params.get('port', 80)
        timeout = params.get('timeout', 5)
        
        if not host:
            return ActionResult(success=False, message="host required")
        
        if not isinstance(port, int) or port < 1 or port > 65535:
            return ActionResult(success=False, message="port must be 1-65535")
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            
            is_open = result == 0
            
            return ActionResult(
                success=is_open,
                message=f"Port {port} on {host} is {'open' if is_open else 'closed'}",
                data={'host': host, 'port': port, 'open': is_open}
            )
            
        except socket.timeout:
            return ActionResult(
                success=False,
                message=f"Port check timed out: {host}:{port}",
                data={'host': host, 'port': port, 'timeout': True}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Port check error: {e}",
                data={'error': str(e)}
            )

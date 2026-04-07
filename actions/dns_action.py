"""DNS lookup action module for RabAI AutoClick.

Provides DNS operations:
- DnsLookupAction: DNS lookup
- DnsReverseAction: Reverse DNS lookup
- DnsResolveAction: Resolve hostname to IP
- DnsCheckAction: Check DNS propagation
"""

from __future__ import annotations

import sys
import socket
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DnsLookupAction(BaseAction):
    """DNS record lookup."""
    action_type = "dns_lookup"
    display_name = "DNS查询"
    description = "DNS记录查询"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute DNS lookup."""
        hostname = params.get('hostname', '')
        record_type = params.get('record_type', 'A')  # A, AAAA, MX, TXT, CNAME, NS
        output_var = params.get('output_var', 'dns_result')

        if not hostname:
            return ActionResult(success=False, message="hostname is required")

        try:
            import dns.resolver

            resolved_host = context.resolve_value(hostname) if context else hostname
            resolved_type = context.resolve_value(record_type) if context else record_type

            try:
                answers = dns.resolver.resolve(resolved_host, resolved_type)
                records = []
                for rdata in answers:
                    records.append(str(rdata))

                result = {'hostname': resolved_host, 'type': resolved_type, 'records': records}
                if context:
                    context.set(output_var, result)
                return ActionResult(success=True, message=f"DNS {resolved_type} for {resolved_host}: {records}", data=result)
            except dns.resolver.NXDOMAIN:
                return ActionResult(success=False, message=f"Domain {resolved_host} does not exist")
            except dns.resolver.NoAnswer:
                return ActionResult(success=False, message=f"No {resolved_type} records found for {resolved_host}")
        except ImportError:
            # Fallback to socket
            resolved_host = context.resolve_value(hostname) if context else hostname
            resolved_type = context.resolve_value(record_type) if context else record_type

            if resolved_type == 'A':
                ip = socket.gethostbyname(resolved_host)
                result = {'hostname': resolved_host, 'type': 'A', 'records': [ip]}
                if context:
                    context.set(output_var, result)
                return ActionResult(success=True, message=f"{resolved_host} -> {ip}", data=result)
            return ActionResult(success=False, message="dnspython not installed and fallback not supported for this record type")
        except Exception as e:
            return ActionResult(success=False, message=f"DNS lookup error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['hostname']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'record_type': 'A', 'output_var': 'dns_result'}


class DnsResolveAction(BaseAction):
    """Resolve hostname to IP address."""
    action_type = "dns_resolve"
    display_name = "DNS解析"
    description = "主机名解析为IP"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute DNS resolve."""
        hostname = params.get('hostname', '')
        output_var = params.get('output_var', 'resolved_ip')

        if not hostname:
            return ActionResult(success=False, message="hostname is required")

        try:
            resolved_host = context.resolve_value(hostname) if context else hostname

            result = socket.getaddrinfo(resolved_host, None, socket.AF_INET)
            ips = list(set([r[4][0] for r in result]))

            result_data = {'hostname': resolved_host, 'ips': ips}
            if context:
                context.set(output_var, ips[0] if ips else None)
            return ActionResult(success=True, message=f"{resolved_host} -> {', '.join(ips)}", data=result_data)
        except socket.gaierror as e:
            return ActionResult(success=False, message=f"Could not resolve {hostname}: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"DNS resolve error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['hostname']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'resolved_ip'}


class DnsReverseAction(BaseAction):
    """Reverse DNS lookup (IP to hostname)."""
    action_type = "dns_reverse"
    display_name = "DNS反向查询"
    description = "IP反向查询主机名"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute reverse DNS."""
        ip_address = params.get('ip_address', '')
        output_var = params.get('output_var', 'reverse_dns')

        if not ip_address:
            return ActionResult(success=False, message="ip_address is required")

        try:
            resolved_ip = context.resolve_value(ip_address) if context else ip_address

            hostname, aliaslist, ipaddrlist = socket.gethostbyaddr(resolved_ip)

            result = {'ip': resolved_ip, 'hostname': hostname, 'aliases': aliaslist}
            if context:
                context.set(output_var, hostname)
            return ActionResult(success=True, message=f"{resolved_ip} -> {hostname}", data=result)
        except socket.herror as e:
            return ActionResult(success=False, message=f"No PTR record for {resolved_ip}: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Reverse DNS error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['ip_address']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'reverse_dns'}


class DnsCheckAction(BaseAction):
    """Check DNS health/propagation."""
    action_type = "dns_check"
    display_name = "DNS检查"
    description = "检查DNS健康状态"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute DNS check."""
        hostname = params.get('hostname', '')
        expected_ip = params.get('expected_ip', None)
        output_var = params.get('output_var', 'dns_check_result')

        if not hostname:
            return ActionResult(success=False, message="hostname is required")

        try:
            resolved_host = context.resolve_value(hostname) if context else hostname

            result = socket.getaddrinfo(resolved_host, None, socket.AF_INET)
            ips = list(set([r[4][0] for r in result]))

            if expected_ip:
                resolved_expected = context.resolve_value(expected_ip) if context else expected_ip
                matches = resolved_expected in ips
                result_data = {
                    'hostname': resolved_host,
                    'ips': ips,
                    'expected': resolved_expected,
                    'matches': matches,
                }
                if context:
                    context.set(output_var, result_data)
                return ActionResult(success=matches, message=f"DNS check {'PASS' if matches else 'FAIL'}", data=result_data)

            result_data = {'hostname': resolved_host, 'ips': ips, 'count': len(ips)}
            if context:
                context.set(output_var, result_data)
            return ActionResult(success=True, message=f"{resolved_host} resolves to {len(ips)} IPs", data=result_data)
        except Exception as e:
            return ActionResult(success=False, message=f"DNS check error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['hostname']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'expected_ip': None, 'output_var': 'dns_check_result'}

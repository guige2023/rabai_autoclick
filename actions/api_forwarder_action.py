"""API Forwarder Action.

Forwards API requests to different endpoints based on rules.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class ForwardRule:
    rule_id: str
    name: str
    match_fn: Callable[[Any], bool]
    target_url: str
    transform_request: Optional[Callable[[Any], Any]] = None
    transform_response: Optional[Callable[[Any], Any]] = None
    priority: int = 0
    enabled: bool = True


class APIForwarderAction:
    """Forwards API requests based on routing rules."""

    def __init__(self, default_url: Optional[str] = None) -> None:
        self.default_url = default_url
        self.rules: List[ForwardRule] = []
        self._forward_fn: Optional[Callable[[str, Any], Any]] = None

    def set_forward_fn(self, fn: Callable[[str, Any], Any]) -> None:
        self._forward_fn = fn

    def add_rule(
        self,
        rule_id: str,
        name: str,
        match_fn: Callable[[Any], bool],
        target_url: str,
        transform_request: Optional[Callable[[Any], Any]] = None,
        transform_response: Optional[Callable[[Any], Any]] = None,
        priority: int = 0,
    ) -> ForwardRule:
        rule = ForwardRule(
            rule_id=rule_id,
            name=name,
            match_fn=match_fn,
            target_url=target_url,
            transform_request=transform_request,
            transform_response=transform_response,
            priority=priority,
        )
        self.rules.append(rule)
        self.rules.sort(key=lambda r: -r.priority)
        return rule

    def forward(self, request: Any) -> tuple[Optional[str], Any]:
        for rule in self.rules:
            if not rule.enabled:
                continue
            if rule.match_fn(request):
                target = rule.target_url
                req = rule.transform_request(request) if rule.transform_request else request
                response = None
                if self._forward_fn:
                    response = self._forward_fn(target, req)
                else:
                    response = {"forwarded_to": target, "request": req}
                if rule.transform_response:
                    response = rule.transform_response(response)
                return target, response
        if self.default_url:
            return self.default_url, {"forwarded_to": self.default_url, "request": request}
        return None, {"error": "No matching rule and no default URL"}

    def get_rules(self) -> List[Dict[str, Any]]:
        return [
            {"rule_id": r.rule_id, "name": r.name, "target_url": r.target_url, "priority": r.priority, "enabled": r.enabled}
            for r in self.rules
        ]

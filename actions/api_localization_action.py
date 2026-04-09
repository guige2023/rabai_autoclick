"""API localization action module for RabAI AutoClick.

Provides localization support for API requests:
- ApiLocalizationAction: Localize API requests
- ApiAcceptLanguageAction: Set Accept-Language header
- ApiContentNegotiationAction: Content negotiation
- ApiLocaleRoutingAction: Route based on locale
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ApiLocalizationAction(BaseAction):
    """Localize API requests based on locale."""
    action_type = "api_localization"
    display_name = "API本地化"
    description = "根据区域设置本地化API请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            locale = params.get("locale", "en-US")
            url = params.get("url", "")
            headers = params.get("headers", {})
            include_timezone = params.get("include_timezone", False)

            locales = {
                "en-US": {"Accept-Language": "en-US,en;q=0.9", "Content-Type": "application/json"},
                "zh-CN": {"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", "Content-Type": "application/json"},
                "ja-JP": {"Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8", "Content-Type": "application/json"},
                "ko-KR": {"Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8", "Content-Type": "application/json"},
                "fr-FR": {"Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8", "Content-Type": "application/json"},
                "de-DE": {"Accept-Language": "de-DE,de;q=0.9,en;q=0.8", "Content-Type": "application/json"},
            }

            if locale not in locales:
                locale = "en-US"

            localized_headers = locales[locale].copy()
            localized_headers.update(headers)

            if include_timezone:
                import time
                localized_headers["X-Timezone"] = time.tzname[0]
                localized_headers["X-Locale"] = locale

            return ActionResult(
                success=True,
                message=f"Localized for {locale}",
                data={"locale": locale, "headers": localized_headers, "url": url}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Localization error: {e}")


class ApiAcceptLanguageAction(BaseAction):
    """Set Accept-Language header properly."""
    action_type = "api_accept_language"
    display_name = "API Accept-Language设置"
    description = "正确设置Accept-Language请求头"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            languages = params.get("languages", ["en"])
            quality_values = params.get("quality_values", {})
            headers = params.get("headers", {})

            if isinstance(languages, str):
                languages = [languages]

            accept_language = []
            for lang in languages:
                q = quality_values.get(lang, 0.9)
                if q >= 1.0:
                    accept_language.append(lang)
                elif q > 0:
                    accept_language.append(f"{lang};q={q}")

            accept_language.sort(key=lambda x: float(x.split(";q=")[1]) if ";q=" in x else 1.0, reverse=True)

            headers["Accept-Language"] = ",".join(accept_language)

            return ActionResult(
                success=True,
                message=f"Accept-Language: {headers['Accept-Language']}",
                data={"headers": headers, "accept_language": headers["Accept-Language"]}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Accept-Language error: {e}")


class ApiContentNegotiationAction(BaseAction):
    """Content negotiation for API requests."""
    action_type = "api_content_negotiation"
    display_name = "API内容协商"
    description = "API请求的内容协商"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            preferred_types = params.get("preferred_types", ["application/json"])
            preferred_languages = params.get("preferred_languages", ["en"])
            headers = params.get("headers", {})
            encodings = params.get("preferred_encodings", ["gzip", "deflate"])

            if isinstance(preferred_types, str):
                preferred_types = [preferred_types]
            if isinstance(preferred_languages, str):
                preferred_languages = [preferred_languages]

            accept = ",".join(preferred_types)
            headers["Accept"] = accept

            accept_language = ",".join(preferred_languages)
            headers["Accept-Language"] = accept_language

            accept_encoding = ",".join(encodings)
            headers["Accept-Encoding"] = accept_encoding

            return ActionResult(
                success=True,
                message="Content negotiation headers set",
                data={
                    "headers": headers,
                    "accept": accept,
                    "accept_language": accept_language,
                    "accept_encoding": accept_encoding,
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Content negotiation error: {e}")


class ApiLocaleRoutingAction(BaseAction):
    """Route API requests based on locale."""
    action_type = "api_locale_routing"
    display_name = "API区域路由"
    description = "基于区域设置路由API请求"

    def __init__(self):
        super().__init__()
        self._locale_routes: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "route")
            locale = params.get("locale")
            route_key = params.get("route_key")
            route_config = params.get("route_config")
            default_route = params.get("default_route")

            if operation == "register":
                if not route_key or not route_config:
                    return ActionResult(success=False, message="route_key and route_config required")
                self._locale_routes[route_key] = route_config
                return ActionResult(success=True, message=f"Registered route: {route_key}")

            elif operation == "route":
                if not locale:
                    return ActionResult(success=False, message="locale required")

                for key, config in self._locale_routes.items():
                    supported_locales = config.get("locales", [])
                    if locale in supported_locales or locale.split("-")[0] in [l.split("-")[0] for l in supported_locales]:
                        return ActionResult(
                            success=True,
                            message=f"Routed to '{key}' for locale '{locale}'",
                            data={"route_key": key, "route_config": config, "locale": locale}
                        )

                if default_route and default_route in self._locale_routes:
                    return ActionResult(
                        success=True,
                        message=f"No locale match, using default '{default_route}'",
                        data={"route_key": default_route, "route_config": self._locale_routes[default_route], "locale": locale}
                    )

                return ActionResult(success=False, message=f"No route found for locale: {locale}")

            elif operation == "list":
                return ActionResult(success=True, message=f"{len(self._locale_routes)} routes", data={"routes": list(self._locale_routes.keys())})

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Locale routing error: {e}")

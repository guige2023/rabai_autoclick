"""Config provider action module for RabAI AutoClick.

Provides configuration management:
- ConfigProvider: Load and manage configurations from multiple sources
- EnvConfigLoader: Load environment-based configurations
- RemoteConfigFetcher: Fetch remote configuration
- ConfigVersioning: Version control for configurations
"""

from __future__ import annotations

import json
import sys
import os
import time
import hashlib
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConfigProviderAction(BaseAction):
    """Load and manage configurations from multiple sources."""
    action_type = "config_provider"
    display_name = "配置提供者"
    description = "多源配置加载与管理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "load")
            config_path = params.get("config_path", "/tmp/config")
            source = params.get("source", "file")
            config_name = params.get("config_name", "app")
            environment = params.get("environment", os.environ.get("ENV", "development"))

            os.makedirs(config_path, exist_ok=True)

            if operation == "load":
                if source == "file":
                    config_file = os.path.join(config_path, f"{config_name}.json")
                    if os.path.exists(config_file):
                        with open(config_file) as f:
                            config = json.load(f)
                        return ActionResult(success=True, message=f"Loaded: {config_name}", data={"config": config})
                    return ActionResult(success=False, message=f"Config not found: {config_name}")

                elif source == "env":
                    prefix = params.get("prefix", "APP_")
                    config = {}
                    for key, value in os.environ.items():
                        if key.startswith(prefix):
                            config[key[len(prefix):].lower()] = value
                    return ActionResult(success=True, message=f"Loaded {len(config)} env vars", data={"config": config})

                elif source == "remote":
                    remote_url = params.get("remote_url", "")
                    if not remote_url:
                        return ActionResult(success=False, message="remote_url required")

                    import urllib.request
                    try:
                        with urllib.request.urlopen(remote_url, timeout=10) as resp:
                            config = json.loads(resp.read().decode())
                        cache_file = os.path.join(config_path, f"{config_name}_remote.json")
                        with open(cache_file, "w") as f:
                            json.dump({"config": config, "fetched_at": datetime.now().isoformat()}, f)
                        return ActionResult(success=True, message="Loaded remote config", data={"config": config})
                    except Exception as e:
                        cache_file = os.path.join(config_path, f"{config_name}_remote.json")
                        if os.path.exists(cache_file):
                            with open(cache_file) as f:
                                cached = json.load(f)
                            return ActionResult(success=True, message="Using cached config", data={"config": cached.get("config", {}), "cached": True})
                        return ActionResult(success=False, message=f"Remote fetch failed: {str(e)}")

            elif operation == "save":
                config_data = params.get("config_data", {})
                if not config_data:
                    return ActionResult(success=False, message="config_data required")

                config_file = os.path.join(config_path, f"{config_name}.json")
                with open(config_file, "w") as f:
                    json.dump(config_data, f, indent=2)

                return ActionResult(success=True, message=f"Saved: {config_name}")

            elif operation == "merge":
                base_config = params.get("base_config", {})
                overrides = params.get("overrides", {})

                merged = {**base_config, **overrides}
                return ActionResult(success=True, message=f"Merged configs", data={"config": merged})

            elif operation == "diff":
                config1 = params.get("config1", {})
                config2 = params.get("config2", {})

                all_keys = set(config1.keys()) | set(config2.keys())
                diff = {}
                for key in all_keys:
                    if config1.get(key) != config2.get(key):
                        diff[key] = {"old": config1.get(key), "new": config2.get(key)}

                return ActionResult(success=True, message=f"{len(diff)} differences", data={"diff": diff, "count": len(diff)})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class SecretRotationAction(BaseAction):
    """Rotate secrets periodically."""
    action_type = "secret_rotation"
    display_name = "密钥轮换"
    description = "定期轮换密钥"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "rotate")
            secret_name = params.get("secret_name", "")
            rotation_days = params.get("rotation_days", 90)
            secrets_path = params.get("secrets_path", "/tmp/secrets")

            os.makedirs(secrets_path, exist_ok=True)
            secret_file = os.path.join(secrets_path, f"{secret_name}.json")

            if operation == "rotate":
                new_secret = params.get("new_secret", hashlib.sha256(str(time.time()).encode()).hexdigest())

                rotation_record = {
                    "secret_name": secret_name,
                    "current_secret": new_secret,
                    "rotated_at": datetime.now().isoformat(),
                    "rotation_days": rotation_days,
                    "version": 1,
                }

                if os.path.exists(secret_file):
                    with open(secret_file) as f:
                        old_record = json.load(f)
                    rotation_record["version"] = old_record.get("version", 0) + 1

                with open(secret_file, "w") as f:
                    json.dump(rotation_record, f, indent=2)

                return ActionResult(success=True, message=f"Rotated: {secret_name} v{rotation_record['version']}")

            elif operation == "get":
                if not os.path.exists(secret_file):
                    return ActionResult(success=False, message=f"Secret not found: {secret_name}")
                with open(secret_file) as f:
                    record = json.load(f)
                return ActionResult(success=True, message=f"Secret: {secret_name}", data={"secret": record.get("current_secret"), "version": record.get("version")})

            elif operation == "check_expiry":
                if not os.path.exists(secret_file):
                    return ActionResult(success=False, message=f"Secret not found: {secret_name}")

                with open(secret_file) as f:
                    record = json.load(f)

                rotated_at = datetime.fromisoformat(record.get("rotated_at", ""))
                age_days = (datetime.now() - rotated_at).days
                needs_rotation = age_days >= rotation_days

                return ActionResult(
                    success=True,
                    message=f"Age: {age_days} days, needs rotation: {needs_rotation}",
                    data={"age_days": age_days, "needs_rotation": needs_rotation, "rotation_days": rotation_days}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")

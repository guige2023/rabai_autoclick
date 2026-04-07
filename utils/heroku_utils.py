"""Heroku utilities: app management, dyno operations, config vars, and add-ons."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from typing import Any

__all__ = [
    "HerokuConfig",
    "HerokuApp",
    "HerokuDyno",
    "HerokuAddons",
]


@dataclass
class HerokuConfig:
    """Heroku CLI configuration."""

    api_key: str = ""
    app_name: str = ""


def _heroku(args: list[str], api_key: str = "", check: bool = True) -> subprocess.CompletedProcess:
    """Run a heroku CLI command."""
    env = os.environ.copy()
    if api_key:
        env["HEROKU_API_KEY"] = api_key
    result = subprocess.run(
        ["heroku"] + args,
        capture_output=True,
        text=True,
        env=env,
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"heroku {' '.join(args)} failed: {result.stderr}")
    return result


class HerokuApp:
    """Heroku app management."""

    def __init__(self, config: HerokuConfig) -> None:
        self.config = config

    def info(self) -> dict[str, Any]:
        """Get app info."""
        result = _heroku(["apps:info", "--json", "--app", self.config.app_name], self.config.api_key)
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {}

    def create(self, name: str | None = None, region: str = "us") -> dict[str, Any]:
        """Create a new Heroku app."""
        args = ["apps:create", "--json"]
        if name:
            args.append(name)
        args.extend(["--region", region])
        result = _heroku(args, self.config.api_key)
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {}

    def destroy(self, confirm: str | None = None) -> bool:
        """Destroy the app."""
        args = ["apps:destroy", "--app", self.config.app_name, "--confirm", confirm or self.config.app_name]
        _heroku(args, self.config.api_key)
        return True

    def rename(self, new_name: str) -> dict[str, Any]:
        """Rename the app."""
        result = _heroku(["apps:rename", new_name, "--app", self.config.app_name], self.config.api_key)
        return {"name": new_name}

    def scale(self, process_type: str, count: int) -> bool:
        """Scale a process type to N dynos."""
        result = _heroku(["ps:scale", f"{process_type}={count}", "--app", self.config.app_name], self.config.api_key)
        return result.returncode == 0

    def release_info(self) -> list[dict[str, Any]]:
        """Get recent releases."""
        result = _heroku(["releases", "--json", "--app", self.config.app_name], self.config.api_key)
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return []


class HerokuDyno:
    """Heroku dyno management."""

    def __init__(self, config: HerokuConfig) -> None:
        self.config = config

    def list(self) -> list[dict[str, Any]]:
        """List dynos."""
        result = _heroku(["ps", "--json", "--app", self.config.app_name], self.config.api_key)
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return []

    def restart(self, dyno: str | None = None) -> bool:
        """Restart dyno(s)."""
        args = ["ps:restart"]
        if dyno:
            args.append(dyno)
        args.extend(["--app", self.config.app_name])
        _heroku(args, self.config.api_key)
        return True

    def stop(self, dyno: str) -> bool:
        """Stop a specific dyno."""
        result = _heroku(["ps:stop", dyno, "--app", self.config.app_name], self.config.api_key)
        return result.returncode == 0

    def run(self, command: str, remote: bool = False) -> bool:
        """Run a one-off dyno command."""
        args = ["run", command, "--app", self.config.app_name]
        result = _heroku(args, self.config.api_key, check=False)
        return result.returncode == 0


class HerokuConfigVars:
    """Heroku config vars management."""

    def __init__(self, config: HerokuConfig) -> None:
        self.config = config

    def get(self) -> dict[str, str]:
        """Get all config vars."""
        result = _heroku(["config:json", "--app", self.config.app_name], self.config.api_key)
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {}

    def set(self, vars_: dict[str, str]) -> bool:
        """Set config vars."""
        for key, value in vars_.items():
            _heroku(["config:set", f"{key}={value}", "--app", self.config.app_name], self.config.api_key)
        return True

    def unset(self, *keys: str) -> bool:
        """Unset config vars."""
        for key in keys:
            _heroku(["config:unset", key, "--app", self.config.app_name], self.config.api_key)
        return True


class HerokuAddons:
    """Heroku add-on management."""

    def __init__(self, config: HerokuConfig) -> None:
        self.config = config

    def list(self) -> list[dict[str, Any]]:
        """List add-ons."""
        result = _heroku(["addons", "--json", "--app", self.config.app_name], self.config.api_key)
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return []

    def create(self, plan: str) -> dict[str, Any]:
        """Create an add-on."""
        result = _heroku(["addons:create", plan, "--app", self.config.app_name], self.config.api_key)
        return {"plan": plan, "output": result.stdout}

    def destroy(self, addon_name: str) -> bool:
        """Destroy an add-on."""
        _heroku(["addons:destroy", addon_name, "--app", self.config.app_name, "--confirm", addon_name], self.config.api_key)
        return True

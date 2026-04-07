"""
Terraform utilities for infrastructure-as-code operations.

Provides Terraform config generation, state management, plan/apply runners,
module scaffolding, and workspace management.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Optional

logger = logging.getLogger(__name__)


class TerraformAction(Enum):
    INIT = auto()
    PLAN = auto()
    APPLY = auto()
    DESTROY = auto()
    VALIDATE = auto()
    FMT = auto()
    IMPORT = auto()


@dataclass
class TerraformConfig:
    """Terraform configuration settings."""
    backend: str = "local"
    required_version: str = ">= 1.0"
    required_providers: dict[str, str] = field(default_factory=dict)
    vars: dict[str, str] = field(default_factory=dict)


@dataclass
class TerraformResource:
    """Represents a Terraform resource."""
    resource_type: str
    name: str
    config: dict[str, Any] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)
    lifecycle_policy: Optional[dict[str, Any]] = None

    def to_hcl(self) -> str:
        """Convert resource to HCL format."""
        lines = [f'resource "{self.resource_type}" "{self.name}" {{']
        for key, value in self.config.items():
            lines.append(f"  {key} = {self._format_value(value)}")
        if self.lifecycle_policy:
            lines.append("  lifecycle {")
            for k, v in self.lifecycle_policy.items():
                lines.append(f"    {k} = {self._format_value(v)}")
            lines.append("  }")
        lines.append("}")
        return "\n".join(lines)

    def _format_value(self, value: Any) -> str:
        if isinstance(value, str):
            return f'"{value}"'
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, dict):
            pairs = [f'{k} = {self._format_value(v)}' for k, v in value.items()]
            return "{ " + ", ".join(pairs) + " }"
        elif isinstance(value, list):
            items = [self._format_value(v) for v in value]
            return "[" + ", ".join(items) + "]"
        return str(value)


@dataclass
class TerraformModule:
    """Represents a Terraform module."""
    name: str
    source: str
    version: Optional[str] = None
    inputs: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, str] = field(default_factory=dict)

    def to_hcl(self) -> str:
        """Convert module to HCL format."""
        attrs = [f'source = "{self.source}"']
        if self.version:
            attrs.append(f'version = "{self.version}"')
        for key, value in self.inputs.items():
            attrs.append(f"{key} = {self._format_value(value)}")
        lines = [f'module "{self.name}" {{']
        lines.extend(f"  {a}" for a in attrs)
        lines.append("}")
        if self.outputs:
            lines.append("")
            for key, desc in self.outputs.items():
                lines.append(f'output "{key}" {{')
                lines.append(f'  description = "{desc}"')
                lines.append("  value       = " + f'module.{self.name}.{key}')
                lines.append("}")
        return "\n".join(lines)

    def _format_value(self, value: Any) -> str:
        if isinstance(value, str):
            return f'"{value}"'
        elif isinstance(value, bool):
            return "true" if value else "false"
        return str(value)


@dataclass
class TerraformPlan:
    """Represents a Terraform plan."""
    add: int = 0
    change: int = 0
    destroy: int = 0
    duration_seconds: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


class TerraformRunner:
    """Executes Terraform commands."""

    def __init__(self, working_dir: str = ".", binary: str = "terraform") -> None:
        self.working_dir = working_dir
        self.binary = binary

    def run(self, action: TerraformAction, extra_args: Optional[list[str]] = None) -> tuple[int, str, str]:
        """Run a Terraform command."""
        cmd = [self.binary, action.name.lower()]
        if extra_args:
            cmd.extend(extra_args)
        try:
            result = subprocess.run(
                cmd,
                cwd=self.working_dir,
                capture_output=True,
                text=True,
                timeout=600,
            )
            return result.returncode, result.stdout, result.stderr
        except FileNotFoundError:
            return 1, "", f"Terraform binary not found: {self.binary}"
        except subprocess.TimeoutExpired:
            return 1, "", "Terraform command timed out"

    def init(self, backend_config: Optional[dict[str, str]] = None) -> bool:
        """Run terraform init."""
        args = []
        if backend_config:
            for key, value in backend_config.items():
                args.extend([f"-backend-config={key}={value}"])
        code, _, err = self.run(TerraformAction.INIT, args)
        if code == 0:
            logger.info("Terraform init successful")
        else:
            logger.error("Terraform init failed: %s", err)
        return code == 0

    def validate(self) -> tuple[bool, str]:
        """Run terraform validate."""
        code, out, err = self.run(TerraformAction.VALIDATE)
        return code == 0, out + err

    def plan(self, var_file: Optional[str] = None, out_file: Optional[str] = None) -> Optional[TerraformPlan]:
        """Run terraform plan."""
        args = ["-no-color"]
        if var_file:
            args.extend(["-var-file", var_file])
        if out_file:
            args.extend(["-out", out_file])
        code, out, err = self.run(TerraformAction.PLAN, args)
        if code != 0:
            logger.error("Terraform plan failed: %s", err)
            return None
        return self._parse_plan_output(out)

    def apply(self, auto_approve: bool = True, var_file: Optional[str] = None) -> bool:
        """Run terraform apply."""
        args = ["-no-color"]
        if auto_approve:
            args.append("-auto-approve")
        if var_file:
            args.extend(["-var-file", var_file])
        code, out, err = self.run(TerraformAction.APPLY, args)
        if code == 0:
            logger.info("Terraform apply successful")
        else:
            logger.error("Terraform apply failed: %s", err)
        return code == 0

    def destroy(self, auto_approve: bool = True, var_file: Optional[str] = None) -> bool:
        """Run terraform destroy."""
        args = ["-no-color"]
        if auto_approve:
            args.append("-auto-approve")
        if var_file:
            args.extend(["-var-file", var_file])
        code, _, err = self.run(TerraformAction.DESTROY, args)
        return code == 0

    def _parse_plan_output(self, output: str) -> TerraformPlan:
        """Parse terraform plan output to extract resource changes."""
        plan = TerraformPlan()
        for line in output.split("\n"):
            if "Plan:" in line:
                parts = line.split()
                for i, p in enumerate(parts):
                    if p == "~":
                        plan.change += 1
                    elif p == "+":
                        plan.add += 1
                    elif p == "-":
                        plan.destroy += 1
        return plan


class TerraformConfigGenerator:
    """Generates Terraform configuration files."""

    def __init__(self, config: Optional[TerraformConfig] = None) -> None:
        self.config = config or TerraformConfig()
        self.resources: list[TerraformResource] = []
        self.modules: list[TerraformModule] = []
        self.data_sources: list[TerraformResource] = []
        self.outputs: dict[str, str] = {}
        self.variables: dict[str, dict[str, Any]] = {}

    def add_resource(self, resource: TerraformResource) -> "TerraformConfigGenerator":
        self.resources.append(resource)
        return self

    def add_module(self, module: TerraformModule) -> "TerraformConfigGenerator":
        self.modules.append(module)
        return self

    def add_data_source(self, data: TerraformResource) -> "TerraformConfigGenerator":
        self.data_sources.append(data)
        return self

    def add_output(self, name: str, value: str, description: str = "") -> "TerraformConfigGenerator":
        self.outputs[name] = value
        self.outputs[f"_desc_{name}"] = description
        return self

    def add_variable(
        self,
        name: str,
        type_str: str = "string",
        default: Any = None,
        description: str = "",
    ) -> "TerraformConfigGenerator":
        self.variables[name] = {
            "type": type_str,
            "default": default,
            "description": description,
        }
        return self

    def render(self) -> str:
        """Render the complete Terraform configuration."""
        lines = []
        lines.append("terraform {")
        lines.append(f'  required_version = "{self.config.required_version}"')
        if self.config.required_providers:
            lines.append("  required_providers {")
            for name, source in self.config.required_providers.items():
                lines.append(f'    {name} = {{')
                lines.append(f'      source = "{source}"')
                lines.append("    }")
            lines.append("  }")
        lines.append("}")
        lines.append("")

        for name, var in self.variables.items():
            lines.append(f'variable "{name}" {{')
            if var["description"]:
                lines.append(f'  description = "{var["description"]}"')
            lines.append(f'  type        = {var["type"]}')
            if var["default"] is not None:
                lines.append(f'  default     = {self._format_value(var["default"])}')
            lines.append("}")
            lines.append("")

        for resource in self.resources:
            lines.append(resource.to_hcl())
            lines.append("")

        for module in self.modules:
            lines.append(module.to_hcl())
            lines.append("")

        for name, value in self.outputs.items():
            if name.startswith("_desc_"):
                continue
            desc = self.outputs.get(f"_desc_{name}", "")
            lines.append(f'output "{name}" {{')
            lines.append(f'  description = "{desc}"')
            lines.append(f"  value       = {value}")
            lines.append("}")
            lines.append("")

        return "\n".join(lines)

    def _format_value(self, value: Any) -> str:
        if isinstance(value, str):
            return f'"{value}"'
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, list):
            items = [self._format_value(v) for v in value]
            return "[" + ", ".join(items) + "]"
        return str(value)

    def write_to_dir(self, dir_path: str) -> None:
        """Write Terraform files to a directory."""
        os.makedirs(dir_path, exist_ok=True)
        main_content = self.render()
        with open(os.path.join(dir_path, "main.tf"), "w") as f:
            f.write(main_content)

        if self.variables:
            var_lines = []
            for name, var in self.variables.items():
                var_lines.append(f'variable "{name}" {{')
                if var["description"]:
                    var_lines.append(f'  description = "{var["description"]}"')
                var_lines.append(f'  type        = {var["type"]}')
                if var["default"] is not None:
                    var_lines.append(f'  default     = {self._format_value(var["default"])}')
                var_lines.append("}")
                var_lines.append("")
            with open(os.path.join(dir_path, "variables.tf"), "w") as f:
                f.write("\n".join(var_lines))

        if self.outputs:
            out_lines = []
            for name in self.outputs:
                if name.startswith("_desc_"):
                    continue
                desc = self.outputs.get(f"_desc_{name}", "")
                value = self.outputs[name]
                out_lines.append(f'output "{name}" {{')
                out_lines.append(f'  description = "{desc}"')
                out_lines.append(f"  value       = {value}")
                out_lines.append("}")
                out_lines.append("")
            with open(os.path.join(dir_path, "outputs.tf"), "w") as f:
                f.write("\n".join(out_lines))

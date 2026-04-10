"""
Terraform Workflow Management

A comprehensive Terraform integration system providing:
1. Workspace management: Create/manage Terraform workspaces
2. Plan generation: Generate Terraform execution plans
3. Apply/destroy: Apply or destroy Terraform resources
4. State management: Manage Terraform state
5. Variable management: Manage input variables
6. Output parsing: Parse Terraform outputs
7. Remote backend: Configure remote state backend
8. Import existing: Import existing resources to Terraform
9. Taint/untaint: Taint or untaint resources
10. State locking: Handle state locking for team use

Commit: 'feat(terraform): add Terraform integration with workspace management, plan/apply/destroy, state management, variable management, remote backend, import, taint, state locking'
"""

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple, Set

import fcntl


class WorkspaceAction(Enum):
    """Actions that can be performed on workspaces."""
    CREATE = "create"
    SELECT = "select"
    DELETE = "delete"
    LIST = "list"
    SHOW = "show"


class PlanMode(Enum):
    """Terraform plan modes."""
    PREVIEW = "preview"
    DESTROY = "destroy"
    REFRESH_ONLY = "refresh_only"


class ApplyMode(Enum):
    """Terraform apply modes."""
    NORMAL = "normal"
    DESTROY = "destroy"
    REFRESH_ONLY = "refresh_only"


class BackendType(Enum):
    """Supported Terraform backend types."""
    LOCAL = "local"
    S3 = "s3"
    AZURE = "azure"
    GCS = "gcs"
    CONSUL = "consul"
    ETCD = "etcd"
    HTTP = "http"


class StateLockStatus(Enum):
    """State lock statuses."""
    LOCKED = "locked"
    UNLOCKED = "unlocked"
    ERROR = "error"


@dataclass
class TerraformVariable:
    """Represents a Terraform input variable."""
    name: str
    value: Any
    description: str = ""
    sensitive: bool = False
    validation: Optional[Dict[str, Any]] = None

    def to_tfvar_format(self) -> str:
        """Convert to .tfvars format string."""
        if isinstance(self.value, str):
            return f'{self.name} = "{self.value}"'
        elif isinstance(self.value, bool):
            return f'{self.name} = {"true" if self.value else "false"}'
        elif isinstance(self.value, (int, float)):
            return f'{self.name} = {self.value}'
        elif isinstance(self.value, list):
            items = [f'"{v}"' if isinstance(v, str) else str(v) for v in self.value]
            return f'{self.name} = [{", ".join(items)}]'
        elif isinstance(self.value, dict):
            items = [f'{k} = {"true" if v else "false"}' if isinstance(v, bool) else f'{k} = {v}'
                    for k, v in self.value.items()]
            return f'{self.name} = {{{" ".join(items)}}}'
        return f'{self.name} = "{self.value}"'


@dataclass
class TerraformOutput:
    """Represents a Terraform output value."""
    name: str
    value: Any
    type: str
    sensitive: bool = False
    description: str = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TerraformOutput':
        """Create from terraform output command JSON."""
        return cls(
            name=data.get("name", ""),
            value=data.get("value"),
            type=data.get("type", "string"),
            sensitive=data.get("sensitive", False),
            description=data.get("description", "")
        )


@dataclass
class ResourceChange:
    """Represents a resource change in a plan/apply."""
    address: str
    action: str
    resource_type: str
    resource_name: str
    changes: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ResourceChange':
        """Create from terraform plan JSON."""
        address = data.get("address", "")
        match = re.match(r'(.+)_(.+?)_(.+)', address)
        if match:
            resource_type = match.group(1)
            resource_name = match.group(2)
        else:
            resource_type = ""
            resource_name = address

        return cls(
            address=address,
            action=data.get("action", "no-op"),
            resource_type=resource_type,
            resource_name=resource_name,
            changes=data.get("change", {})
        )


@dataclass
class PlanResult:
    """Result of a Terraform plan operation."""
    success: bool
    plan_file: Optional[str] = None
    changes: List[ResourceChange] = field(default_factory=list)
    resource_counts: Dict[str, int] = field(default_factory=dict)
    output: str = ""
    error: str = ""

    def has_changes(self) -> bool:
        """Check if there are any changes to apply."""
        return len(self.changes) > 0

    def summary(self) -> str:
        """Get a human-readable summary."""
        if self.success:
            add = self.resource_counts.get("add", 0)
            change = self.resource_counts.get("change", 0)
            destroy = self.resource_counts.get("destroy", 0)
            return f"Plan: +{add} ~{change} -{destroy}"
        return f"Plan failed: {self.error}"


@dataclass
class ApplyResult:
    """Result of a Terraform apply operation."""
    success: bool
    output: str = ""
    state_file: Optional[str] = None
    applied_resources: List[str] = field(default_factory=list)
    error: str = ""

    def summary(self) -> str:
        """Get a human-readable summary."""
        if self.success:
            return f"Apply successful: {len(self.applied_resources)} resources"
        return f"Apply failed: {self.error}"


@dataclass
class BackendConfig:
    """Configuration for Terraform backend."""
    backend_type: BackendType
    config: Dict[str, Any]

    def to_terraform_config(self) -> str:
        """Generate terraform block configuration."""
        config_lines = [f'terraform {{']
        config_lines.append(f'  backend "{self.backend_type.value}" {{')

        for key, value in self.config.items():
            if isinstance(value, str):
                config_lines.append(f'    {key} = "{value}"')
            elif isinstance(value, bool):
                config_lines.append(f'    {key} = {"true" if value else "false"}')
            else:
                config_lines.append(f'    {key} = {value}')

        config_lines.append('  }')
        config_lines.append('}')
        return '\n'.join(config_lines)


@dataclass
class WorkspaceInfo:
    """Information about a Terraform workspace."""
    name: str
    path: str
    locked: bool = False
    lock_id: Optional[str] = None
    locked_by: Optional[str] = None
    locked_at: Optional[str] = None

    @classmethod
    def from_terraform_output(cls, name: str, path: str, output: str) -> 'WorkspaceInfo':
        """Parse workspace info from terraform workspace show output."""
        info = cls(name=name, path=path)
        if "Locked" in output or "lock" in output.lower():
            info.locked = True
        return info


@dataclass
class StateInfo:
    """Information about Terraform state."""
    state_file: str
    backend: BackendType
    resources: List[str] = field(default_factory=list)
    serial: int = 0
    lineage: str = ""
    locked: bool = False
    lock_id: Optional[str] = None


class TerraformManager:
    """Manager for Terraform operations."""

    def __init__(
        self,
        working_dir: Optional[str] = None,
        terraform_path: str = "terraform",
        variables: Optional[Dict[str, Any]] = None,
        backend: Optional[BackendConfig] = None
    ):
        """
        Initialize Terraform manager.

        Args:
            working_dir: Working directory for Terraform operations
            terraform_path: Path to terraform binary
            variables: Default variables to use
            backend: Backend configuration for remote state
        """
        self.working_dir = working_dir or os.getcwd()
        self.terraform_path = terraform_path
        self.default_variables = variables or {}
        self.backend = backend
        self.current_workspace = "default"
        self._state_lock_file = None

    def _run_terraform(
        self,
        args: List[str],
        input_vars: Optional[Dict[str, Any]] = None,
        capture_output: bool = True,
        timeout: int = 300
    ) -> Tuple[int, str, str]:
        """
        Run terraform command.

        Args:
            args: Command arguments
            input_vars: Variables for input prompts
            capture_output: Whether to capture stdout/stderr
            timeout: Command timeout in seconds

        Returns:
            Tuple of (return_code, stdout, stderr)
        """
        cmd = [self.terraform_path] + args

        env = os.environ.copy()
        env["TF_IN_AUTOMATION"] = "true"
        env["TF_WARN_OUTPUT_ERRORS"] = "true"

        try:
            result = subprocess.run(
                cmd,
                cwd=self.working_dir,
                capture_output=capture_output,
                text=True,
                env=env,
                timeout=timeout,
                input=None if input_vars is None else '\n'.join(str(v) for v in input_vars.values())
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return -1, "", f"Command timed out after {timeout} seconds"
        except FileNotFoundError:
            return -1, "", f"Terraform not found at {self.terraform_path}"
        except Exception as e:
            return -1, "", str(e)

    def _run_terraform_with_input(
        self,
        args: List[str],
        input_prompts: Optional[Dict[str, str]] = None,
        capture_output: bool = True,
        timeout: int = 300
    ) -> Tuple[int, str, str]:
        """
        Run terraform command with interactive input.

        Args:
            args: Command arguments
            input_prompts: Dict of prompt patterns to responses
            capture_output: Whether to capture stdout/stderr
            timeout: Command timeout in seconds

        Returns:
            Tuple of (return_code, stdout, stderr)
        """
        cmd = [self.terraform_path] + args

        env = os.environ.copy()
        env["TF_IN_AUTOMATION"] = "true"

        try:
            process = subprocess.Popen(
                cmd,
                cwd=self.working_dir,
                stdout=subprocess.PIPE if capture_output else None,
                stderr=subprocess.PIPE if capture_output else None,
                stdin=subprocess.PIPE,
                text=True,
                env=env
            )

            output = ""
            error = ""

            if input_prompts:
                for _ in range(10):
                    if process.poll() is not None:
                        break

            stdout, stderr = process.communicate(timeout=timeout)
            return process.returncode, stdout or "", stderr or ""

        except subprocess.TimeoutExpired:
            process.kill()
            return -1, "", f"Command timed out after {timeout} seconds"
        except Exception as e:
            return -1, "", str(e)

    # ==================== Workspace Management ====================

    def create_workspace(self, name: str) -> bool:
        """
        Create a new Terraform workspace.

        Args:
            name: Workspace name

        Returns:
            True if successful, False otherwise
        """
        code, stdout, stderr = self._run_terraform(["workspace", "new", name])
        if code == 0:
            self.current_workspace = name
            return True
        return False

    def select_workspace(self, name: str) -> bool:
        """
        Select a Terraform workspace.

        Args:
            name: Workspace name

        Returns:
            True if successful, False otherwise
        """
        code, stdout, stderr = self._run_terraform(["workspace", "select", name])
        if code == 0:
            self.current_workspace = name
            return True
        return False

    def delete_workspace(self, name: str, force: bool = False) -> bool:
        """
        Delete a Terraform workspace.

        Args:
            name: Workspace name
            force: Force delete even if state exists

        Returns:
            True if successful, False otherwise
        """
        args = ["workspace", "delete", name]
        if force:
            args.append("-force")
        code, stdout, stderr = self._run_terraform(args)
        return code == 0

    def list_workspaces(self) -> List[str]:
        """
        List all Terraform workspaces.

        Returns:
            List of workspace names
        """
        code, stdout, stderr = self._run_terraform(["workspace", "list"])
        if code == 0:
            workspaces = []
            for line in stdout.splitlines():
                line = line.strip()
                if line.startswith("* "):
                    line = line[2:]
                if line:
                    workspaces.append(line)
            return workspaces
        return []

    def show_workspace(self) -> WorkspaceInfo:
        """
        Show current workspace information.

        Returns:
            WorkspaceInfo object
        """
        code, stdout, stderr = self._run_terraform(["workspace", "show"])
        name = stdout.strip() if code == 0 else self.current_workspace

        state_path = os.path.join(self.working_dir, "terraform.tfstate.d", name)

        return WorkspaceInfo(
            name=name,
            path=state_path,
            locked=self._check_state_lock(name)
        )

    def _check_state_lock(self, workspace: str) -> bool:
        """Check if workspace state is locked."""
        code, stdout, stderr = self._run_terraform(["workspace", "show"])
        return "Locked" in stdout or "lock" in stdout.lower()

    # ==================== Initialization ====================

    def init(
        self,
        backend: Optional[BackendConfig] = None,
        reconfigure: bool = False,
        upgrade: bool = False
    ) -> bool:
        """
        Initialize Terraform with backend configuration.

        Args:
            backend: Backend configuration
            reconfigure: Reconfigure backend
            upgrade: Upgrade provider versions

        Returns:
            True if successful, False otherwise
        """
        args = ["init"]

        if reconfigure:
            args.append("-reconfigure")
        if upgrade:
            args.append("-upgrade")

        if backend:
            config_file = self._create_backend_config_file(backend)
            args.extend(["-backend-config", config_file])

        code, stdout, stderr = self._run_terraform(args, timeout=600)

        if code == 0 and backend:
            self.backend = backend

        return code == 0

    def _create_backend_config_file(self, backend: BackendConfig) -> str:
        """Create temporary backend configuration file."""
        config = {
            "address": backend.config.get("address", ""),
            "lock_address": backend.config.get("lock_address", ""),
            "unlock_address": backend.config.get("unlock_address", ""),
            "username": backend.config.get("username", ""),
            "password": backend.config.get("password", ""),
        }

        config = {k: v for k, v in config.items() if v}

        config_file = os.path.join(self.working_dir, ".terraform_backend_config")
        with open(config_file, 'w') as f:
            json.dump(config, f)

        return config_file

    def validate(self) -> Tuple[bool, str]:
        """
        Validate Terraform configuration.

        Returns:
            Tuple of (success, message)
        """
        code, stdout, stderr = self._run_terraform(["validate"])
        output = stdout + stderr
        return code == 0, output

    def fmt(self, check: bool = False) -> Tuple[bool, str]:
        """
        Format Terraform configuration files.

        Args:
            check: Only check, don't modify

        Returns:
            Tuple of (success, message)
        """
        args = ["fmt"]
        if check:
            args.append("-check")

        code, stdout, stderr = self._run_terraform(args)
        output = stdout + stderr
        return code == 0, output

    # ==================== Planning ====================

    def plan(
        self,
        plan_file: Optional[str] = None,
        variables: Optional[Dict[str, Any]] = None,
        destroy: bool = False,
        out_file: Optional[str] = None
    ) -> PlanResult:
        """
        Generate Terraform execution plan.

        Args:
            plan_file: Existing plan file to show
            variables: Override variables
            destroy: Generate destroy plan
            out_file: Save plan to file

        Returns:
            PlanResult object
        """
        args = ["plan"]

        if plan_file:
            args.extend(["-out", plan_file])
        elif out_file:
            args.extend(["-out", out_file])

        if destroy:
            args.append("-destroy")

        var_dict = {**self.default_variables, **(variables or {})}
        for key, value in var_dict.items():
            args.extend(["-var", f"{key}={value}"])

        code, stdout, stderr = self._run_terraform(args, timeout=600)
        output = stdout + stderr

        result = PlanResult(success=(code == 0), output=output)

        if code == 0:
            result.plan_file = out_file or plan_file
            result.changes = self._parse_plan_changes(output)
            result.resource_counts = self._count_resources(result.changes)

        return result

    def _parse_plan_changes(self, output: str) -> List[ResourceChange]:
        """Parse resource changes from plan output."""
        changes = []

        change_pattern = r'  # (.+)\.(.+?)_(.+) will be (.+)'
        for match in re.finditer(change_pattern, output):
            action = match.group(4)
            changes.append(ResourceChange(
                address=f"{match.group(1)}.{match.group(2)}.{match.group(3)}",
                action=action,
                resource_type=match.group(1),
                resource_name=match.group(2)
            ))

        return changes

    def _count_resources(self, changes: List[ResourceChange]) -> Dict[str, int]:
        """Count resources by action type."""
        counts = {"add": 0, "change": 0, "destroy": 0}

        for change in changes:
            if change.action == "created":
                counts["add"] += 1
            elif change.action in ["updated", "modified"]:
                counts["change"] += 1
            elif change.action == "destroyed":
                counts["destroy"] += 1

        return counts

    def show_plan(self, plan_file: str) -> str:
        """
        Show plan contents.

        Args:
            plan_file: Plan file to show

        Returns:
            Plan output as string
        """
        code, stdout, stderr = self._run_terraform(["show", plan_file])
        return stdout + stderr

    # ==================== Applying ====================

    def apply(
        self,
        plan_file: Optional[str] = None,
        variables: Optional[Dict[str, Any]] = None,
        auto_approve: bool = True,
        refresh: bool = True
    ) -> ApplyResult:
        """
        Apply Terraform resources.

        Args:
            plan_file: Plan file to apply
            variables: Override variables
            auto_approve: Skip approval prompt
            refresh: Refresh state before apply

        Returns:
            ApplyResult object
        """
        args = ["apply"]

        if plan_file:
            args.append(plan_file)
        else:
            args.append("-auto-approve" if auto_approve else "-input=false")

        if not refresh:
            args.append("-refresh=false")

        if not plan_file:
            var_dict = {**self.default_variables, **(variables or {})}
            for key, value in var_dict.items():
                args.extend(["-var", f"{key}={value}"])

        code, stdout, stderr = self._run_terraform(args, timeout=1800)
        output = stdout + stderr

        result = ApplyResult(success=(code == 0), output=output)

        if code == 0:
            self._parse_apply_resources(result, output)

        return result

    def _parse_apply_resources(self, result: ApplyResult, output: str):
        """Parse applied resources from apply output."""
        apply_pattern = r'Apply complete! Resources: (\d+) (?:resource|resources)'
        match = re.search(apply_pattern, output)
        if match:
            count = int(match.group(1))
            result.applied_resources = [f"resource_{i}" for i in range(count)]

    def destroy(
        self,
        variables: Optional[Dict[str, Any]] = None,
        auto_approve: bool = True
    ) -> ApplyResult:
        """
        Destroy Terraform resources.

        Args:
            variables: Override variables
            auto_approve: Skip approval prompt

        Returns:
            ApplyResult object
        """
        args = ["destroy", "-force" if auto_approve else "-input=false"]

        var_dict = {**self.default_variables, **(variables or {})}
        for key, value in var_dict.items():
            args.extend(["-var", f"{key}={value}"])

        code, stdout, stderr = self._run_terraform(args, timeout=1800)
        output = stdout + stderr

        return ApplyResult(success=(code == 0), output=output)

    # ==================== State Management ====================

    def state_list(self, resource_address: Optional[str] = None) -> List[str]:
        """
        List resources in state.

        Args:
            resource_address: Filter by resource address

        Returns:
            List of resource addresses
        """
        args = ["state", "list"]
        if resource_address:
            args.append(resource_address)

        code, stdout, stderr = self._run_terraform(args)
        if code == 0:
            return [line.strip() for line in stdout.splitlines() if line.strip()]
        return []

    def state_pull(self) -> str:
        """
        Pull current state from remote backend.

        Returns:
            State content as JSON string
        """
        code, stdout, stderr = self._run_terraform(["state", "pull"])
        return stdout if code == 0 else ""

    def state_push(self, state_content: str) -> bool:
        """
        Push state to remote backend.

        Args:
            state_content: State content as JSON string

        Returns:
            True if successful
        """
        code, stdout, stderr = self._run_terraform_with_input(
            ["state", "push"],
            input_prompts={"data": state_content}
        )
        return code == 0

    def state_mv(
        self,
        source_address: str,
        dest_address: str,
        dry_run: bool = False
    ) -> bool:
        """
        Move resource in state.

        Args:
            source_address: Source resource address
            dest_address: Destination resource address
            dry_run: Perform dry run

        Returns:
            True if successful
        """
        args = ["state", "mv", source_address, dest_address]
        if dry_run:
            args.append("-dry-run")

        code, stdout, stderr = self._run_terraform(args)
        return code == 0

    def state_rm(self, resource_address: str) -> bool:
        """
        Remove resource from state.

        Args:
            resource_address: Resource address to remove

        Returns:
            True if successful
        """
        code, stdout, stderr = self._run_terraform(["state", "rm", resource_address])
        return code == 0

    def state_show(self, resource_address: str) -> str:
        """
        Show resource details from state.

        Args:
            resource_address: Resource address

        Returns:
            Resource details as string
        """
        code, stdout, stderr = self._run_terraform(["state", "show", resource_address])
        return stdout + stderr

    def state_backup(self, backup_file: Optional[str] = None) -> bool:
        """
        Create state backup.

        Args:
            backup_file: Backup file path

        Returns:
            True if successful
        """
        if not backup_file:
            backup_file = f"terraform.tfstate.backup.{int(time.time())}"
        code, stdout, stderr = self._run_terraform(["state", "pull"])
        if code == 0:
            with open(backup_file, 'w') as f:
                f.write(stdout)
            return True
        return False

    # ==================== Variable Management ====================

    def set_variable(self, name: str, value: Any, sensitive: bool = False) -> TerraformVariable:
        """
        Set a variable value.

        Args:
            name: Variable name
            value: Variable value
            sensitive: Whether variable is sensitive

        Returns:
            TerraformVariable object
        """
        var = TerraformVariable(name=name, value=value, sensitive=sensitive)
        self.default_variables[name] = value
        return var

    def set_variables(self, variables: Dict[str, Any]) -> List[TerraformVariable]:
        """
        Set multiple variables.

        Args:
            variables: Dict of variable names to values

        Returns:
            List of TerraformVariable objects
        """
        result = []
        for name, value in variables.items():
            result.append(self.set_variable(name, value))
        return result

    def write_variables_file(
        self,
        variables: Dict[str, Any],
        filename: str = "terraform.tfvars"
    ) -> str:
        """
        Write variables to .tfvars file.

        Args:
            variables: Variables to write
            filename: Output filename

        Returns:
            Path to created file
        """
        lines = []
        for name, value in variables.items():
            var = TerraformVariable(name=name, value=value)
            lines.append(var.to_tfvar_format())

        filepath = os.path.join(self.working_dir, filename)
        with open(filepath, 'w') as f:
            f.write('\n'.join(lines))

        return filepath

    def read_variables_file(self, filename: str = "terraform.tfvars") -> Dict[str, Any]:
        """
        Read variables from .tfvars file.

        Args:
            filename: Input filename

        Returns:
            Dict of variable names to values
        """
        filepath = os.path.join(self.working_dir, filename)
        if not os.path.exists(filepath):
            return {}

        variables = {}
        current_key = None
        current_value_parts = []

        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                match = re.match(r'^(\w+)\s*=\s*(.+)$', line)
                if match:
                    if current_key:
                        variables[current_key] = self._parse_tfvar_value(''.join(current_value_parts))
                    current_key = match.group(1)
                    current_value_parts = [match.group(2)]
                elif current_key:
                    current_value_parts.append(line)

        if current_key:
            variables[current_key] = self._parse_tfvar_value(''.join(current_value_parts))

        return variables

    def _parse_tfvar_value(self, value: str) -> Any:
        """Parse a terraform variable value."""
        value = value.strip().strip(',').strip('"').strip("'")

        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        if value.lower() == "null":
            return None

        try:
            if '.' in value:
                return float(value)
            return int(value)
        except ValueError:
            pass

        if value.startswith('[') and value.endswith(']'):
            items = value[1:-1].split(',')
            return [self._parse_tfvar_value(i.strip()) for i in items if i.strip()]

        if value.startswith('{') and value.endswith('}'):
            return {}

        return value

    # ==================== Output Parsing ====================

    def output(self, name: Optional[str] = None) -> Dict[str, TerraformOutput]:
        """
        Get Terraform outputs.

        Args:
            name: Specific output name, or None for all

        Returns:
            Dict of output names to TerraformOutput objects
        """
        args = ["output", "-json"]
        if name:
            args.append(name)

        code, stdout, stderr = self._run_terraform(args)

        if code != 0:
            return {}

        try:
            data = json.loads(stdout)
            if name:
                return {name: TerraformOutput.from_dict(data)}
            return {k: TerraformOutput.from_dict({"name": k, **v}) for k, v in data.items()}
        except json.JSONDecodeError:
            return {}

    def output_raw(self, name: Optional[str] = None) -> str:
        """
        Get raw Terraform output.

        Args:
            name: Specific output name, or None for all

        Returns:
            Output as string
        """
        args = ["output"]
        if name:
            args.append(name)

        code, stdout, stderr = self._run_terraform(args)
        return stdout

    # ==================== Remote Backend ====================

    def configure_remote_backend(
        self,
        backend_type: BackendType,
        config: Dict[str, Any]
    ) -> BackendConfig:
        """
        Configure remote state backend.

        Args:
            backend_type: Type of backend
            config: Backend configuration

        Returns:
            BackendConfig object
        """
        backend = BackendConfig(backend_type=backend_type, config=config)

        config_content = backend.to_terraform_config()

        backend_file = os.path.join(self.working_dir, "backend.tf")
        with open(backend_file, 'w') as f:
            f.write(config_content)

        self.backend = backend
        return backend

    def configure_s3_backend(
        self,
        bucket: str,
        key: str = "terraform.tfstate",
        region: str = "us-east-1",
        encrypt: bool = True,
        dynamodb_table: Optional[str] = None
    ) -> BackendConfig:
        """
        Configure S3 backend for remote state.

        Args:
            bucket: S3 bucket name
            key: State file key
            region: AWS region
            encrypt: Enable encryption
            dynamodb_table: DynamoDB table for state locking

        Returns:
            BackendConfig object
        """
        config = {
            "bucket": bucket,
            "key": key,
            "region": region,
            "encrypt": str(encrypt).lower(),
        }

        if dynamodb_table:
            config["dynamodb_table"] = dynamodb_table

        return self.configure_remote_backend(BackendType.S3, config)

    def configure_gcs_backend(
        self,
        bucket: str,
        prefix: str = "",
        credentials_file: Optional[str] = None
    ) -> BackendConfig:
        """
        Configure GCS backend for remote state.

        Args:
            bucket: GCS bucket name
            prefix: Prefix for state file
            credentials_file: Path to credentials JSON

        Returns:
            BackendConfig object
        """
        config = {
            "bucket": bucket,
            "prefix": prefix,
        }

        if credentials_file:
            config["credentials"] = credentials_file

        return self.configure_remote_backend(BackendType.GCS, config)

    def configure_azure_backend(
        self,
        resource_group_name: str,
        storage_account_name: str,
        container_name: str = "terraform",
        key: str = "terraform.tfstate"
    ) -> BackendConfig:
        """
        Configure Azure backend for remote state.

        Args:
            resource_group_name: Azure resource group
            storage_account_name: Storage account name
            container_name: Blob container name
            key: State file key

        Returns:
            BackendConfig object
        """
        config = {
            "resource_group_name": resource_group_name,
            "storage_account_name": storage_account_name,
            "container_name": container_name,
            "key": key,
        }

        return self.configure_remote_backend(BackendType.AZURE, config)

    # ==================== Import Resources ====================

    def import_resource(
        self,
        resource_address: str,
        resource_id: str,
        plan_file: Optional[str] = None
    ) -> bool:
        """
        Import existing resource to Terraform state.

        Args:
            resource_address: Terraform resource address
            resource_id: Cloud resource ID
            plan_file: Optional plan file to generate

        Returns:
            True if successful
        """
        args = ["import", "-address", resource_address, resource_id]

        if plan_file:
            args.extend(["-out", plan_file])

        code, stdout, stderr = self._run_terraform(args, timeout=600)
        return code == 0

    def import_resources_batch(
        self,
        resources: List[Tuple[str, str]],
        parallel: int = 10
    ) -> Dict[str, bool]:
        """
        Import multiple resources.

        Args:
            resources: List of (resource_address, resource_id) tuples
            parallel: Number of parallel imports

        Returns:
            Dict mapping resource addresses to success status
        """
        results = {}

        for address, resource_id in resources:
            success = self.import_resource(address, resource_id)
            results[address] = success
            if not success:
                break

        return results

    # ==================== Taint/Untaint ====================

    def taint(self, resource_address: str) -> bool:
        """
        Mark resource for recreation.

        Args:
            resource_address: Resource address to taint

        Returns:
            True if successful
        """
        code, stdout, stderr = self._run_terraform(["taint", resource_address])
        return code == 0

    def untaint(self, resource_address: str) -> bool:
        """
        Remove taint from resource.

        Args:
            resource_address: Resource address to untaint

        Returns:
            True if successful
        """
        code, stdout, stderr = self._run_terraform(["untaint", resource_address])
        return code == 0

    def list_tainted(self) -> List[str]:
        """
        List tainted resources.

        Returns:
            List of tainted resource addresses
        """
        code, stdout, stderr = self._run_terraform(["state", "list"])
        if code == 0:
            return [line.strip() for line in stdout.splitlines() if ".tainted" in line.lower()]
        return []

    # ==================== State Locking ====================

    def acquire_lock(self, lock_id: Optional[str] = None, timeout: int = 0) -> bool:
        """
        Acquire state lock (for local locking mechanism).

        Args:
            lock_id: Lock identifier
            timeout: Timeout in seconds (0 = no timeout)

        Returns:
            True if lock acquired
        """
        if self._state_lock_file:
            return False

        lock_id = lock_id or str(uuid.uuid4())
        lock_file = os.path.join(self.working_dir, ".terraform_lock")

        start_time = time.time()

        while True:
            try:
                self._state_lock_file = open(lock_file, 'w')
                fcntl.flock(self._state_lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

                self._state_lock_file.write(json.dumps({
                    "lock_id": lock_id,
                    "locked_at": datetime.now().isoformat(),
                    "locked_by": os.environ.get("USER", "unknown")
                }))
                self._state_lock_file.flush()

                return True

            except (IOError, OSError):
                if self._state_lock_file:
                    self._state_lock_file.close()
                    self._state_lock_file = None

                if timeout > 0 and (time.time() - start_time) >= timeout:
                    return False

                if timeout == 0:
                    return False

                time.sleep(1)

        return False

    def release_lock(self) -> bool:
        """
        Release state lock.

        Returns:
            True if lock released
        """
        if not self._state_lock_file:
            return True

        try:
            fcntl.flock(self._state_lock_file.fileno(), fcntl.LOCK_UN)
            self._state_lock_file.close()
            self._state_lock_file = None

            lock_file = os.path.join(self.working_dir, ".terraform_lock")
            if os.path.exists(lock_file):
                os.remove(lock_file)

            return True
        except Exception:
            return False

    def get_lock_info(self) -> Optional[Dict[str, Any]]:
        """
        Get current lock information.

        Returns:
            Lock info dict or None if not locked
        """
        if not self._state_lock_file:
            return None

        try:
            lock_file = os.path.join(self.working_dir, ".terraform_lock")
            if os.path.exists(lock_file):
                with open(lock_file, 'r') as f:
                    return json.load(f)
        except Exception:
            pass

        return None

    def force_unlock(self, lock_id: str) -> bool:
        """
        Force unlock state (uses terraform force-unlock command).

        Args:
            lock_id: Lock ID to force unlock

        Returns:
            True if successful
        """
        code, stdout, stderr = self._run_terraform(["force-unlock", lock_id])
        return code == 0

    # ==================== Providers ====================

    def get_providers(self) -> List[Dict[str, str]]:
        """
        Get installed providers.

        Returns:
            List of provider info dicts
        """
        code, stdout, stderr = self._run_terraform(["providers", "config"])

        if code != 0:
            return []

        providers = []
        current_provider = {}

        for line in stdout.splitlines():
            line = line.strip()

            if line.startswith("provider "):
                if current_provider:
                    providers.append(current_provider)
                current_provider = {"name": line.replace("provider ", "")}
            elif ":" in line:
                key, value = line.split(":", 1)
                current_provider[key.strip()] = value.strip()

        if current_provider:
            providers.append(current_provider)

        return providers

    # ==================== Graph ====================

    def graph(self, output_file: Optional[str] = None) -> str:
        """
        Generate Terraform dependency graph.

        Args:
            output_file: File to write graph output

        Returns:
            Graph output in DOT format
        """
        args = ["graph"]

        code, stdout, stderr = self._run_terraform(args)

        if code == 0:
            if output_file:
                with open(output_file, 'w') as f:
                    f.write(stdout)
            return stdout

        return ""

    # ==================== Refresh ====================

    def refresh(self, variables: Optional[Dict[str, Any]] = None) -> bool:
        """
        Refresh Terraform state.

        Args:
            variables: Override variables

        Returns:
            True if successful
        """
        args = ["refresh", "-input=false"]

        var_dict = {**self.default_variables, **(variables or {})}
        for key, value in var_dict.items():
            args.extend(["-var", f"{key}={value}"])

        code, stdout, stderr = self._run_terraform(args, timeout=600)
        return code == 0

    # ==================== Version ====================

    def version(self) -> str:
        """
        Get Terraform version.

        Returns:
            Version string
        """
        code, stdout, stderr = self._run_terraform(["--version"])
        if code == 0:
            return stdout.strip()
        return ""

    def get_requirements(self) -> Dict[str, str]:
        """
        Get Terraform requirements from configuration.

        Returns:
            Dict of provider/version requirements
        """
        code, stdout, stderr = self._run_terraform(["providers", "mirror"])

        requirements = {}

        if code == 0:
            for line in stdout.splitlines():
                if "required" in line.lower():
                    parts = line.split()
                    if len(parts) >= 2:
                        requirements[parts[0]] = parts[1]

        return requirements

    # ==================== Workspace Context Manager ====================

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.release_lock()
        return False


# Standalone functions for simpler operations

def quick_plan(working_dir: str, variables: Optional[Dict[str, Any]] = None) -> PlanResult:
    """
    Quick plan generation.

    Args:
        working_dir: Terraform working directory
        variables: Variables to use

    Returns:
        PlanResult object
    """
    manager = TerraformManager(working_dir=working_dir)
    return manager.plan(variables=variables)


def quick_apply(
    working_dir: str,
    variables: Optional[Dict[str, Any]] = None,
    auto_approve: bool = True
) -> ApplyResult:
    """
    Quick apply.

    Args:
        working_dir: Terraform working directory
        variables: Variables to use
        auto_approve: Skip approval prompt

    Returns:
        ApplyResult object
    """
    manager = TerraformManager(working_dir=working_dir)
    return manager.apply(variables=variables, auto_approve=auto_approve)


def init_with_backend(
    working_dir: str,
    backend_type: BackendType,
    backend_config: Dict[str, Any]
) -> bool:
    """
    Initialize with remote backend.

    Args:
        working_dir: Terraform working directory
        backend_type: Backend type
        backend_config: Backend configuration

    Returns:
        True if successful
    """
    manager = TerraformManager(working_dir=working_dir)
    return manager.init(backend=BackendConfig(backend_type=backend_type, config=backend_config))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Terraform Workflow Manager")
    parser.add_argument("action", choices=["init", "plan", "apply", "destroy", "workspace"])
    parser.add_argument("directory", nargs="?", default=".")
    parser.add_argument("-var", "--variable", action="append", help="Variables")
    parser.add_argument("-backend", "--backend", help="Backend type")

    args = parser.parse_args()

    manager = TerraformManager(working_dir=args.directory)

    if args.action == "init":
        success = manager.init()
        print(f"Init {'succeeded' if success else 'failed'}")

    elif args.action == "plan":
        result = manager.plan()
        print(result.summary())

    elif args.action == "apply":
        result = manager.apply()
        print(result.summary())

    elif args.action == "destroy":
        result = manager.destroy()
        print(result.summary())

    elif args.action == "workspace":
        workspaces = manager.list_workspaces()
        print("Workspaces:")
        for ws in workspaces:
            print(f"  - {ws}")

"""Terraform action module for RabAI AutoClick.

Provides Terraform infrastructure-as-code operations including
plan, apply, destroy, and state management.
"""

import os
import sys
import time
import json
import subprocess
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class TerraformPlan:
    """Represents a Terraform plan result.
    
    Attributes:
        success: Whether the plan succeeded.
        changes: Number of resources to add/change/destroy.
        output: Plan output text.
        exit_code: Process exit code.
    """
    success: bool
    changes: Dict[str, int] = field(default_factory=dict)
    output: str = ""
    exit_code: int = 0


class TerraformClient:
    """Terraform client for infrastructure operations.
    
    Provides methods for executing Terraform commands
    including init, plan, apply, and destroy operations.
    """
    
    def __init__(
        self,
        working_dir: str = ".",
        terraform_binary: str = "terraform"
    ) -> None:
        """Initialize Terraform client.
        
        Args:
            working_dir: Directory containing Terraform files.
            terraform_binary: Path to Terraform binary.
        """
        self.working_dir = working_dir
        self.terraform_binary = terraform_binary
        self._version: Optional[str] = None
    
    def _run_command(
        self,
        args: List[str],
        input_data: Optional[str] = None,
        timeout: int = 600
    ) -> subprocess.CompletedProcess:
        """Run a Terraform command.
        
        Args:
            args: Command arguments.
            input_data: Optional stdin input.
            timeout: Command timeout in seconds.
            
        Returns:
            CompletedProcess result.
        """
        cmd = [self.terraform_binary] + args
        
        try:
            result = subprocess.run(
                cmd,
                cwd=self.working_dir,
                capture_output=True,
                text=True,
                input=input_data,
                timeout=timeout
            )
            return result
        except subprocess.TimeoutExpired as e:
            raise Exception(f"Terraform command timed out after {timeout}s")
        except Exception as e:
            raise Exception(f"Terraform command failed: {str(e)}")
    
    def connect(self) -> bool:
        """Test if Terraform is available.
        
        Returns:
            True if Terraform is installed and accessible.
        """
        try:
            result = self._run_command(["version"], timeout=10)
            if result.returncode == 0:
                self._version = result.stdout.split("\n")[0]
                return True
            return False
        except Exception:
            return False
    
    def init(
        self,
        backend: Optional[str] = None,
        backend_config: Optional[Dict[str, str]] = None,
        plugin_dir: Optional[str] = None,
        reconfigure: bool = False,
        upgrade: bool = False
    ) -> bool:
        """Initialize a Terraform working directory.
        
        Args:
            backend: Optional backend type.
            backend_config: Backend configuration.
            plugin_dir: Optional plugin directory.
            reconfigure: Reconfigure backend.
            upgrade: Upgrade plugins.
            
        Returns:
            True if init succeeded.
        """
        args = ["init"]
        
        if backend:
            args.extend(["-backend", backend])
        
        if backend_config:
            for key, value in backend_config.items():
                args.extend(["-backend-config", f"{key}={value}"])
        
        if plugin_dir:
            args.extend(["-plugin-dir", plugin_dir])
        
        if reconfigure:
            args.append("-reconfigure")
        
        if upgrade:
            args.append("-upgrade")
        
        result = self._run_command(args, timeout=300)
        return result.returncode == 0
    
    def plan(
        self,
        out_file: Optional[str] = None,
        var_file: Optional[str] = None,
        vars: Optional[Dict[str, str]] = None,
        destroy: bool = False,
        refresh_only: bool = False,
        detailed_exitcode: bool = True
    ) -> TerraformPlan:
        """Create a Terraform execution plan.
        
        Args:
            out_file: Optional file to save plan.
            var_file: Optional variable file.
            vars: Optional variable overrides.
            destroy: Create a destroy plan.
            refresh_only: Refresh state only.
            detailed_exitcode: Return detailed exit code.
            
        Returns:
            TerraformPlan with results.
        """
        args = ["plan"]
        
        if out_file:
            args.extend(["-out", out_file])
        
        if var_file:
            args.extend(["-var-file", var_file])
        
        if vars:
            for key, value in vars.items():
                args.extend(["-var", f"{key}={value}"])
        
        if destroy:
            args.append("-destroy")
        
        if refresh_only:
            args.append("-refresh-only")
        
        if detailed_exitcode:
            args.append("-detailed-exitcode")
        
        result = self._run_command(args, timeout=600)
        
        changes = {"add": 0, "change": 0, "destroy": 0}
        
        for line in result.stdout.split("\n"):
            if "+" in line and "resource" in line.lower():
                changes["add"] += 1
            elif "~" in line and "resource" in line.lower():
                changes["change"] += 1
            elif "-" in line and "resource" in line.lower():
                changes["destroy"] += 1
        
        return TerraformPlan(
            success=result.returncode == 0,
            changes=changes,
            output=result.stdout + result.stderr,
            exit_code=result.returncode
        )
    
    def apply(
        self,
        plan_file: Optional[str] = None,
        var_file: Optional[str] = None,
        vars: Optional[Dict[str, str]] = None,
        auto_approve: bool = False,
        refresh: bool = True
    ) -> bool:
        """Apply Terraform changes.
        
        Args:
            plan_file: Optional plan file to apply.
            var_file: Optional variable file.
            vars: Optional variable overrides.
            auto_approve: Skip approval prompt.
            refresh: Refresh state before apply.
            
        Returns:
            True if apply succeeded.
        """
        args = ["apply"]
        
        if plan_file:
            args.append(plan_file)
        
        if var_file:
            args.extend(["-var-file", var_file])
        
        if vars:
            for key, value in vars.items():
                args.extend(["-var", f"{key}={value}"])
        
        if auto_approve:
            args.append("-auto-approve")
        
        if not refresh:
            args.append("-refresh=false")
        
        if not auto_approve:
            result = self._run_command(args, input_data="yes\n", timeout=1800)
        else:
            result = self._run_command(args, timeout=1800)
        
        return result.returncode == 0
    
    def destroy(
        self,
        var_file: Optional[str] = None,
        vars: Optional[Dict[str, str]] = None,
        auto_approve: bool = False
    ) -> bool:
        """Destroy Terraform-managed infrastructure.
        
        Args:
            var_file: Optional variable file.
            vars: Optional variable overrides.
            auto_approve: Skip approval prompt.
            
        Returns:
            True if destroy succeeded.
        """
        args = ["destroy"]
        
        if var_file:
            args.extend(["-var-file", var_file])
        
        if vars:
            for key, value in vars.items():
                args.extend(["-var", f"{key}={value}"])
        
        if auto_approve:
            args.append("-auto-approve")
        else:
            result = self._run_command(args, input_data="yes\n", timeout=1800)
            return result.returncode == 0
        
        result = self._run_command(args, timeout=1800)
        return result.returncode == 0
    
    def output(self, name: Optional[str] = None) -> Dict[str, Any]:
        """Get Terraform output values.
        
        Args:
            name: Optional specific output name.
            
        Returns:
            Output values dictionary.
        """
        args = ["output"]
        
        if name:
            args.append(name)
        else:
            args.append("-json")
        
        result = self._run_command(args, timeout=30)
        
        if result.returncode != 0:
            return {}
        
        if name:
            return {"value": result.stdout.strip()}
        
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {}
    
    def show(self, plan_file: Optional[str] = None) -> Dict[str, Any]:
        """Show Terraform state or plan.
        
        Args:
            plan_file: Optional plan file to show.
            
        Returns:
            State or plan data.
        """
        args = ["show"]
        
        if plan_file:
            args.append(plan_file)
        else:
            args.append("-json")
        
        result = self._run_command(args, timeout=30)
        
        if result.returncode != 0:
            return {}
        
        if plan_file:
            return {"text": result.stdout}
        
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {}
    
    def state_list(self, resource: Optional[str] = None) -> List[str]:
        """List resources in Terraform state.
        
        Args:
            resource: Optional resource address filter.
            
        Returns:
            List of resource addresses.
        """
        args = ["state", "list"]
        
        if resource:
            args.append(resource)
        
        result = self._run_command(args, timeout=30)
        
        if result.returncode != 0:
            return []
        
        return [line.strip() for line in result.stdout.split("\n") if line.strip()]
    
    def state_pull(self) -> str:
        """Pull the current state from the backend.
        
        Returns:
            State file content.
        """
        result = self._run_command(["state", "pull"], timeout=30)
        
        if result.returncode == 0:
            return result.stdout
        
        return ""
    
    def validate(self) -> Dict[str, Any]:
        """Validate Terraform configuration.
        
        Returns:
            Validation result.
        """
        result = self._run_command(["validate"], timeout=60)
        
        return {
            "valid": result.returncode == 0,
            "output": result.stdout + result.stderr
        }
    
    def get_version(self) -> str:
        """Get Terraform version.
        
        Returns:
            Version string.
        """
        if self._version:
            return self._version
        
        result = self._run_command(["version"], timeout=10)
        if result.returncode == 0:
            self._version = result.stdout.split("\n")[0]
            return self._version
        
        return "unknown"


class TerraformAction(BaseAction):
    """Terraform action for infrastructure-as-code operations.
    
    Supports init, plan, apply, destroy, and state management.
    """
    action_type: str = "terraform"
    display_name: str = "Terraform动作"
    description: str = "Terraform基础设施即代码操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[TerraformClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Terraform operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "init":
                return self._init(params, start_time)
            elif operation == "plan":
                return self._plan(params, start_time)
            elif operation == "apply":
                return self._apply(params, start_time)
            elif operation == "destroy":
                return self._destroy(params, start_time)
            elif operation == "output":
                return self._output(params, start_time)
            elif operation == "show":
                return self._show(params, start_time)
            elif operation == "state_list":
                return self._state_list(params, start_time)
            elif operation == "state_pull":
                return self._state_pull(start_time)
            elif operation == "validate":
                return self._validate(start_time)
            elif operation == "version":
                return self._version(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Terraform operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Initialize Terraform client."""
        working_dir = params.get("working_dir", ".")
        terraform_binary = params.get("terraform_binary", "terraform")
        
        self._client = TerraformClient(
            working_dir=working_dir,
            terraform_binary=terraform_binary
        )
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Terraform available: {self._client.get_version()}" if success else "Terraform not found",
            duration=time.time() - start_time
        )
    
    def _init(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Initialize Terraform working directory."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        backend = params.get("backend")
        backend_config = params.get("backend_config")
        plugin_dir = params.get("plugin_dir")
        reconfigure = params.get("reconfigure", False)
        upgrade = params.get("upgrade", False)
        
        try:
            success = self._client.init(
                backend=backend,
                backend_config=backend_config,
                plugin_dir=plugin_dir,
                reconfigure=reconfigure,
                upgrade=upgrade
            )
            
            return ActionResult(
                success=success,
                message="Terraform initialized" if success else "Init failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _plan(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a Terraform plan."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        out_file = params.get("out_file")
        var_file = params.get("var_file")
        vars_dict = params.get("vars")
        destroy = params.get("destroy", False)
        refresh_only = params.get("refresh_only", False)
        
        try:
            plan = self._client.plan(
                out_file=out_file,
                var_file=var_file,
                vars=vars_dict,
                destroy=destroy,
                refresh_only=refresh_only
            )
            
            return ActionResult(
                success=plan.success,
                message=f"Plan {'succeeded' if plan.success else 'failed'} (exit {plan.exit_code})",
                data={
                    "changes": plan.changes,
                    "exit_code": plan.exit_code,
                    "output": plan.output[-5000:]
                },
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _apply(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Apply Terraform changes."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        plan_file = params.get("plan_file")
        var_file = params.get("var_file")
        vars_dict = params.get("vars")
        auto_approve = params.get("auto_approve", False)
        refresh = params.get("refresh", True)
        
        try:
            success = self._client.apply(
                plan_file=plan_file,
                var_file=var_file,
                vars=vars_dict,
                auto_approve=auto_approve,
                refresh=refresh
            )
            
            return ActionResult(
                success=success,
                message="Apply succeeded" if success else "Apply failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _destroy(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Destroy Terraform-managed infrastructure."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        var_file = params.get("var_file")
        vars_dict = params.get("vars")
        auto_approve = params.get("auto_approve", False)
        
        try:
            success = self._client.destroy(
                var_file=var_file,
                vars=vars_dict,
                auto_approve=auto_approve
            )
            
            return ActionResult(
                success=success,
                message="Destroy succeeded" if success else "Destroy failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _output(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get Terraform output values."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name")
        
        try:
            result = self._client.output(name)
            
            return ActionResult(
                success=True,
                message="Output retrieved",
                data={"output": result},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _show(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Show Terraform state or plan."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        plan_file = params.get("plan_file")
        
        try:
            result = self._client.show(plan_file)
            
            return ActionResult(
                success=True,
                message="State/plan shown",
                data={"state": result},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _state_list(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List resources in Terraform state."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        resource = params.get("resource")
        
        try:
            resources = self._client.state_list(resource)
            
            return ActionResult(
                success=True,
                message=f"Found {len(resources)} resources",
                data={"resources": resources, "count": len(resources)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _state_pull(self, start_time: float) -> ActionResult:
        """Pull the current state from the backend."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            state = self._client.state_pull()
            
            return ActionResult(
                success=bool(state),
                message="State pulled" if state else "State pull failed",
                data={"state": state},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _validate(self, start_time: float) -> ActionResult:
        """Validate Terraform configuration."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            result = self._client.validate()
            
            return ActionResult(
                success=result.get("valid", False),
                message=result.get("output", ""),
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _version(self, start_time: float) -> ActionResult:
        """Get Terraform version."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        version = self._client.get_version()
        
        return ActionResult(
            success=True,
            message=version,
            data={"version": version},
            duration=time.time() - start_time
        )

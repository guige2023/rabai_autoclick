"""Ansible action module for RabAI AutoClick.

Provides Ansible automation operations including
playbook execution, inventory management, and module running.
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
class PlaybookResult:
    """Represents an Ansible playbook execution result.
    
    Attributes:
        success: Whether the playbook succeeded.
        return_code: Process return code.
        stdout: Standard output.
        stderr: Standard error.
        stats: Playbook statistics.
    """
    success: bool
    return_code: int
    stdout: str = ""
    stderr: str = ""
    stats: Dict[str, Any] = field(default_factory=dict)


class AnsibleClient:
    """Ansible client wrapper for automation operations.
    
    Provides methods for running playbooks, managing
    inventory, and executing Ansible modules.
    """
    
    def __init__(
        self,
        inventory: Optional[str] = None,
        private_key: Optional[str] = None,
        user: str = "root",
        become: bool = False,
        vault_id: Optional[str] = None
    ) -> None:
        """Initialize Ansible client.
        
        Args:
            inventory: Path to inventory file or host list.
            private_key: Path to private SSH key.
            user: SSH user.
            become: Whether to use privilege escalation.
            vault_id: Vault ID for encrypted files.
        """
        self.inventory = inventory
        self.private_key = private_key
        self.user = user
        self.become = become
        self.vault_id = vault_id
        self._ansible_playbook_path: Optional[str] = None
        self._ansible_config: Optional[str] = None
    
    def _find_ansible(self) -> bool:
        """Find Ansible executables on the system.
        
        Returns:
            True if Ansible is found, False otherwise.
        """
        for cmd in ["ansible-playbook", "/usr/bin/ansible-playbook", "/usr/local/bin/ansible-playbook"]:
            try:
                result = subprocess.run(
                    [cmd, "--version"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0:
                    self._ansible_playbook_path = cmd
                    return True
            except Exception:
                continue
        return False
    
    def connect(self) -> bool:
        """Test if Ansible is available.
        
        Returns:
            True if Ansible is installed and accessible.
        """
        return self._find_ansible()
    
    def run_playbook(
        self,
        playbook: str,
        extra_vars: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        skip_tags: Optional[List[str]] = None,
        limit: Optional[str] = None,
        check: bool = False,
        diff: bool = False,
        verbose: int = 0
    ) -> PlaybookResult:
        """Run an Ansible playbook.
        
        Args:
            playbook: Path to playbook file.
            extra_vars: Extra variables dictionary.
            tags: Tags to execute.
            skip_tags: Tags to skip.
            limit: Limit to specific hosts.
            check: Check mode (dry run).
            diff: Show diff of changes.
            verbose: Verbosity level (0-4).
            
        Returns:
            PlaybookResult with execution outcome.
        """
        if not self._ansible_playbook_path:
            if not self._find_ansible():
                raise RuntimeError("Ansible is not installed or not found in PATH")
        
        cmd = [self._ansible_playbook_path, playbook]
        
        if self.inventory:
            cmd.extend(["-i", self.inventory])
        
        if self.private_key:
            cmd.extend(["--private-key", self.private_key])
        
        if self.user != "root":
            cmd.extend(["-u", self.user])
        
        if self.become:
            cmd.append("--become")
        
        if self.vault_id:
            cmd.extend(["--vault-id", self.vault_id])
        
        if extra_vars:
            for key, value in extra_vars.items():
                cmd.extend(["-e", f"{key}={json.dumps(value) if isinstance(value, (dict, list)) else value}"])
        
        if tags:
            cmd.extend(["--tags", ",".join(tags)])
        
        if skip_tags:
            cmd.extend(["--skip-tags", ",".join(skip_tags)])
        
        if limit:
            cmd.extend(["--limit", limit])
        
        if check:
            cmd.append("--check")
        
        if diff:
            cmd.append("--diff")
        
        if verbose > 0:
            cmd.append("-" + "v" * verbose)
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600
            )
            
            return PlaybookResult(
                success=result.returncode == 0,
                return_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr
            )
        
        except subprocess.TimeoutExpired:
            return PlaybookResult(
                success=False,
                return_code=-1,
                stderr="Playbook execution timed out"
            )
        except Exception as e:
            return PlaybookResult(
                success=False,
                return_code=-1,
                stderr=str(e)
            )
    
    def run_module(
        self,
        host: str,
        module: str,
        module_args: str = "",
        inventory: Optional[str] = None
    ) -> Dict[str, Any]:
        """Run an Ansible module on a host.
        
        Args:
            host: Target host pattern.
            module: Module name.
            module_args: Module arguments.
            inventory: Optional inventory path.
            
        Returns:
            Module execution result.
        """
        cmd = [
            "ansible",
            host,
            "-m", module,
            "-a", module_args
        ]
        
        if inventory or self.inventory:
            cmd.extend(["-i", inventory or self.inventory])
        
        if self.private_key:
            cmd.extend(["--private-key", self.private_key])
        
        if self.user != "root":
            cmd.extend(["-u", self.user])
        
        if self.become:
            cmd.append("--become")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            return {
                "success": result.returncode == 0,
                "return_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr
            }
        
        except Exception as e:
            return {
                "success": False,
                "return_code": -1,
                "stderr": str(e)
            }
    
    def check_connectivity(
        self,
        inventory: Optional[str] = None,
        pattern: str = "all"
    ) -> Dict[str, Any]:
        """Check connectivity to hosts using ping.
        
        Args:
            inventory: Optional inventory path.
            pattern: Host pattern to ping.
            
        Returns:
            Ping results for each host.
        """
        return self.run_module(
            host=pattern,
            module="ping",
            inventory=inventory or self.inventory
        )
    
    def gather_facts(
        self,
        host: str,
        inventory: Optional[str] = None
    ) -> Dict[str, Any]:
        """Gather facts from a host.
        
        Args:
            host: Target host.
            inventory: Optional inventory path.
            
        Returns:
            Gathered facts.
        """
        return self.run_module(
            host=host,
            module="setup",
            inventory=inventory or self.inventory
        )
    
    def copy_file(
        self,
        src: str,
        dest: str,
        host: str,
        owner: Optional[str] = None,
        group: Optional[str] = None,
        mode: Optional[str] = None,
        inventory: Optional[str] = None
    ) -> Dict[str, Any]:
        """Copy a file to a remote host.
        
        Args:
            src: Source file path.
            dest: Destination path on host.
            host: Target host pattern.
            owner: Optional file owner.
            group: Optional file group.
            mode: Optional file mode.
            inventory: Optional inventory path.
            
        Returns:
            Copy operation result.
        """
        module_args = f"src={src} dest={dest}"
        
        if owner:
            module_args += f" owner={owner}"
        if group:
            module_args += f" group={group}"
        if mode:
            module_args += f" mode={mode}"
        
        return self.run_module(
            host=host,
            module="copy",
            module_args=module_args,
            inventory=inventory or self.inventory
        )
    
    def manage_service(
        self,
        service: str,
        host: str,
        state: str,
        inventory: Optional[str] = None
    ) -> Dict[str, Any]:
        """Manage a service on a remote host.
        
        Args:
            service: Service name.
            host: Target host pattern.
            state: Desired state (started, stopped, restarted).
            inventory: Optional inventory path.
            
        Returns:
            Service management result.
        """
        return self.run_module(
            host=host,
            module="service",
            module_args=f"name={service} state={state}",
            inventory=inventory or self.inventory
        )
    
    def execute_command(
        self,
        cmd: str,
        host: str,
        inventory: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute a command on a remote host.
        
        Args:
            cmd: Command to execute.
            host: Target host pattern.
            inventory: Optional inventory path.
            
        Returns:
            Command execution result.
        """
        return self.run_module(
            host=host,
            module="command",
            module_args=cmd,
            inventory=inventory or self.inventory
        )
    
    def list_inventory(self, inventory: Optional[str] = None) -> Dict[str, Any]:
        """List inventory hosts and groups.
        
        Args:
            inventory: Optional inventory path.
            
        Returns:
            Inventory structure.
        """
        inv = inventory or self.inventory
        if not inv:
            return {"error": "No inventory specified"}
        
        cmd = ["ansible-inventory", "-i", inv, "--list"]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                return json.loads(result.stdout)
            
            return {"error": result.stderr}
        
        except Exception as e:
            return {"error": str(e)}


class AnsibleAction(BaseAction):
    """Ansible action for automation operations.
    
    Supports playbook execution, module running, and inventory management.
    """
    action_type: str = "ansible"
    display_name: str = "Ansible动作"
    description: str = "Ansible自动化运维操作，剧本执行和模块运行"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[AnsibleClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Ansible operation.
        
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
            elif operation == "run_playbook":
                return self._run_playbook(params, start_time)
            elif operation == "run_module":
                return self._run_module(params, start_time)
            elif operation == "check_connectivity":
                return self._check_connectivity(params, start_time)
            elif operation == "gather_facts":
                return self._gather_facts(params, start_time)
            elif operation == "copy_file":
                return self._copy_file(params, start_time)
            elif operation == "manage_service":
                return self._manage_service(params, start_time)
            elif operation == "execute_command":
                return self._execute_command(params, start_time)
            elif operation == "list_inventory":
                return self._list_inventory(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Ansible operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Initialize Ansible client."""
        inventory = params.get("inventory")
        private_key = params.get("private_key")
        user = params.get("user", "root")
        become = params.get("become", False)
        vault_id = params.get("vault_id")
        
        self._client = AnsibleClient(
            inventory=inventory,
            private_key=private_key,
            user=user,
            become=become,
            vault_id=vault_id
        )
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message="Ansible is available" if success else "Ansible not found",
            duration=time.time() - start_time
        )
    
    def _run_playbook(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Run an Ansible playbook."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        playbook = params.get("playbook", "")
        if not playbook:
            return ActionResult(success=False, message="playbook is required", duration=time.time() - start_time)
        
        extra_vars = params.get("extra_vars")
        tags = params.get("tags")
        skip_tags = params.get("skip_tags")
        limit = params.get("limit")
        check = params.get("check", False)
        diff = params.get("diff", False)
        verbose = params.get("verbose", 0)
        
        try:
            result = self._client.run_playbook(
                playbook=playbook,
                extra_vars=extra_vars,
                tags=tags,
                skip_tags=skip_tags,
                limit=limit,
                check=check,
                diff=diff,
                verbose=verbose
            )
            
            return ActionResult(
                success=result.success,
                message=f"Playbook {'succeeded' if result.success else 'failed'} (exit {result.return_code})",
                data={
                    "return_code": result.return_code,
                    "stdout": result.stdout[-5000:] if result.stdout else "",
                    "stderr": result.stderr[-2000:] if result.stderr else ""
                },
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _run_module(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Run an Ansible module."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        host = params.get("host", "all")
        module = params.get("module", "")
        module_args = params.get("module_args", "")
        
        if not module:
            return ActionResult(success=False, message="module is required", duration=time.time() - start_time)
        
        try:
            result = self._client.run_module(
                host=host,
                module=module,
                module_args=module_args
            )
            
            return ActionResult(
                success=result.get("success", False),
                message=f"Module {'succeeded' if result.get('success') else 'failed'}",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _check_connectivity(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Check connectivity to hosts."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        pattern = params.get("pattern", "all")
        
        try:
            result = self._client.check_connectivity(pattern=pattern)
            
            return ActionResult(
                success=result.get("success", False),
                message="Connectivity check completed",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _gather_facts(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Gather facts from a host."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        host = params.get("host", "localhost")
        
        try:
            result = self._client.gather_facts(host=host)
            
            return ActionResult(
                success=result.get("success", False),
                message="Facts gathered",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _copy_file(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Copy a file to a remote host."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        src = params.get("src", "")
        dest = params.get("dest", "")
        host = params.get("host", "all")
        
        if not src or not dest:
            return ActionResult(success=False, message="src and dest are required", duration=time.time() - start_time)
        
        try:
            result = self._client.copy_file(
                src=src,
                dest=dest,
                host=host,
                owner=params.get("owner"),
                group=params.get("group"),
                mode=params.get("mode")
            )
            
            return ActionResult(
                success=result.get("success", False),
                message="File copied" if result.get("success") else "Copy failed",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _manage_service(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Manage a service on a remote host."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        service = params.get("service", "")
        host = params.get("host", "all")
        state = params.get("state", "started")
        
        if not service:
            return ActionResult(success=False, message="service is required", duration=time.time() - start_time)
        
        try:
            result = self._client.manage_service(service=service, host=host, state=state)
            
            return ActionResult(
                success=result.get("success", False),
                message=f"Service {state}: {service}",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _execute_command(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a command on a remote host."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        cmd = params.get("cmd", "")
        host = params.get("host", "all")
        
        if not cmd:
            return ActionResult(success=False, message="cmd is required", duration=time.time() - start_time)
        
        try:
            result = self._client.execute_command(cmd=cmd, host=host)
            
            return ActionResult(
                success=result.get("success", False),
                message="Command executed",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_inventory(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List inventory hosts and groups."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        inventory = params.get("inventory")
        
        try:
            result = self._client.list_inventory(inventory)
            
            if "error" in result:
                return ActionResult(success=False, message=result["error"], duration=time.time() - start_time)
            
            return ActionResult(
                success=True,
                message="Inventory listed",
                data=result,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)

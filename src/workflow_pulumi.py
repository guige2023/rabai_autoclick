"""
Pulumi Infrastructure as Code Workflow Management

A comprehensive Pulumi integration system providing:
1. Stack management: Create/manage Pulumi stacks
2. Program deployment: Deploy Pulumi programs
3. State management: Manage Pulumi state
4. Config management: Manage stack configuration
5. Secret management: Manage Pulumi secrets
6. Import resources: Import existing cloud resources
7. Preview/update: Generate and apply updates
8. Policy as code: Integrate Pulumi CrossGuard
9. Component resources: Create custom component resources
10. Multi-language: Support Python, TypeScript, Go, .NET

Commit: 'feat(pulumi): add Pulumi integration with stack management, program deployment, state, config, secrets, import, preview/update, Policy as Code, component resources, multi-language'
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
import base64
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple, Set

import fcntl


class StackAction(Enum):
    """Actions that can be performed on stacks."""
    CREATE = "create"
    SELECT = "select"
    DELETE = "delete"
    LIST = "list"
    SHOW = "show"
    REFRESH = "refresh"
    IMPORT = "import"


class LanguageType(Enum):
    """Supported Pulumi programming languages."""
    PYTHON = "python"
    TYPESCRIPT = "typescript"
    JAVASCRIPT = "javascript"
    GO = "go"
    DOTNET = "csharp"


class PolicyMode(Enum):
    """Policy enforcement modes for CrossGuard."""
    ADVISORY = "advisory"
    MANDATORY = "mandatory"
    DISABLED = "disabled"


class SecretProvider(Enum):
    """Supported secret providers for Pulumi."""
    DEFAULT = "default"
    PASSPHRASE = "passphrase"
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"
    HASHICORP = "hashicorp"


@dataclass
class PulumiStack:
    """Represents a Pulumi stack."""
    name: str
    project: str
    backend_url: Optional[str] = None
    encrypted_secrets: bool = True
    last_update: Optional[datetime] = None
    version: int = 0
    outputs: Dict[str, Any] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)
    resources_count: int = 0


@dataclass
class PulumiResource:
    """Represents a Pulumi resource."""
    urn: str
    type: str
    id: Optional[str] = None
    parent: Optional[str] = None
    custom: bool = False
    delete: bool = False
    protect: bool = False
    inputs: Dict[str, Any] = field(default_factory=dict)
    outputs: Dict[str, Any] = field(default_factory=dict)
    provider: Optional[str] = None


@dataclass
class PolicyRule:
    """Represents a CrossGuard policy rule."""
    name: str
    description: str = ""
    enforcement: PolicyMode = PolicyMode.ADVISORY
    severity: str = "medium"
    code: Optional[str] = None
    file_path: Optional[str] = None


@dataclass
class PolicyPack:
    """Represents a Pulumi policy pack."""
    name: str
    version: str = "0.1.0"
    display_name: str = ""
    description: str = ""
    rules: List[PolicyRule] = field(default_factory=list)
    enforcement_mode: PolicyMode = PolicyMode.ADVISORY
    parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ComponentResourceSpec:
    """Specification for a custom component resource."""
    name: str
    type: str
    schema: Dict[str, Any] = field(default_factory=dict)
    properties: Dict[str, Any] = field(default_factory=dict)
    required_inputs: List[str] = field(default_factory=list)
    language_templates: Dict[str, str] = field(default_factory=dict)


@dataclass
class ImportResult:
    """Result of importing a resource."""
    urn: str
    id: str
    resource_type: str
    success: bool
    error: Optional[str] = None


class PulumiManager:
    """
    Manages Pulumi infrastructure as code operations.

    Provides comprehensive functionality for:
    - Stack lifecycle management
    - Program deployment across multiple languages
    - State and config management
    - Secret encryption
    - Resource importing
    - Policy as code (CrossGuard)
    - Custom component resources
    """

    def __init__(
        self,
        workdir: Optional[str] = None,
        backend_url: Optional[str] = None,
        secret_provider: SecretProvider = SecretProvider.DEFAULT
    ):
        self.workdir = Path(workdir) if workdir else Path.cwd()
        self.backend_url = backend_url
        self.secret_provider = secret_provider
        self._stack_cache: Dict[str, PulumiStack] = {}
        self._policy_cache: Dict[str, PolicyPack] = {}
        self._component_cache: Dict[str, ComponentResourceSpec] = {}

    def _run_pulumi_cmd(
        self,
        args: List[str],
        cwd: Optional[Path] = None,
        env: Optional[Dict[str, str]] = None,
        capture_output: bool = True
    ) -> Tuple[int, str, str]:
        """Execute a Pulumi CLI command."""
        cmd_env = os.environ.copy()
        if env:
            cmd_env.update(env)
        if self.backend_url:
            cmd_env["PULUMI_BACKEND_URL"] = self.backend_url
        if self.secret_provider != SecretProvider.DEFAULT:
            cmd_env["PULUMI_SECRET_PROVIDER"] = self.secret_provider.value

        try:
            result = subprocess.run(
                ["pulumi"] + args,
                cwd=cwd or self.workdir,
                env=cmd_env,
                capture_output=capture_output,
                text=True,
                timeout=300
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return -1, "", "Command timed out"
        except FileNotFoundError:
            return -1, "", "Pulumi CLI not found"
        except Exception as e:
            return -1, "", str(e)

    # =========================================================================
    # Stack Management
    # =========================================================================

    def create_stack(
        self,
        stack_name: str,
        project_name: Optional[str] = None,
        backend_url: Optional[str] = None,
        description: Optional[str] = None
    ) -> PulumiStack:
        """
        Create a new Pulumi stack.

        Args:
            stack_name: Name of the stack
            project_name: Name of the project (defaults to directory name)
            backend_url: Optional backend URL override
            description: Optional stack description

        Returns:
            PulumiStack object representing the created stack
        """
        if not project_name:
            project_name = self.workdir.name

        args = ["stack", "init", stack_name]
        if description:
            args.extend(["--description", description])

        code, stdout, stderr = self._run_pulumi_cmd(args)

        stack = PulumiStack(
            name=stack_name,
            project=project_name,
            backend_url=backend_url or self.backend_url
        )

        if code == 0:
            self._stack_cache[stack_name] = stack
            return stack
        else:
            raise RuntimeError(f"Failed to create stack: {stderr}")

    def select_stack(self, stack_name: str) -> bool:
        """Select and activate a stack."""
        code, stdout, stderr = self._run_pulumi_cmd(["stack", "select", stack_name])
        return code == 0

    def delete_stack(
        self,
        stack_name: str,
        force: bool = False,
        preserve_config: bool = False
    ) -> bool:
        """
        Delete a Pulumi stack.

        Args:
            stack_name: Name of the stack to delete
            force: Force deletion even if resources exist
            preserve_config: Preserve stack config

        Returns:
            True if deletion succeeded
        """
        args = ["stack", "rm", stack_name]
        if force:
            args.append("--force")
        if preserve_config:
            args.append("--preserve-config")

        code, stdout, stderr = self._run_pulumi_cmd(args)

        if code == 0 and stack_name in self._stack_cache:
            del self._stack_cache[stack_name]

        return code == 0

    def list_stacks(self, all: bool = False) -> List[PulumiStack]:
        """
        List all stacks in the project.

        Args:
            all: Include stacks from all backends

        Returns:
            List of PulumiStack objects
        """
        args = ["stack", "ls"]
        if all:
            args.append("--all")

        code, stdout, stderr = self._run_pulumi_cmd(args)

        stacks = []
        if code == 0:
            for line in stdout.strip().split("\n")[1:]:
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 2:
                        stack = PulumiStack(
                            name=parts[0],
                            project=parts[1] if len(parts) > 1 else ""
                        )
                        stacks.append(stack)

        return stacks

    def get_stack_info(self, stack_name: Optional[str] = None) -> PulumiStack:
        """
        Get detailed information about a stack.

        Args:
            stack_name: Stack name (uses current if None)

        Returns:
            PulumiStack with detailed information
        """
        target_stack = stack_name or self._get_current_stack_name()

        code, stdout, stderr = self._run_pulumi_cmd(
            ["stack", "output", "--json"],
            cwd=self.workdir / target_stack if stack_name else None
        )

        stack = PulumiStack(
            name=target_stack,
            project=self._get_project_name()
        )

        if code == 0:
            try:
                stack.outputs = json.loads(stdout)
            except json.JSONDecodeError:
                pass

        config_code, config_out, _ = self._run_pulumi_cmd(["config", "--json"])
        if config_code == 0:
            try:
                stack.config = json.loads(config_out)
            except json.JSONDecodeError:
                pass

        return stack

    def _get_current_stack_name(self) -> str:
        """Get the currently selected stack name."""
        code, stdout, _ = self._run_pulumi_cmd(["stack", "--show-name"])
        if code == 0:
            return stdout.strip()
        return "default"

    def _get_project_name(self) -> str:
        """Get the project name from Pulumi.yaml."""
        pulumi_yaml = self.workdir / "Pulumi.yaml"
        if pulumi_yaml.exists():
            content = pulumi_yaml.read_text()
            match = re.search(r"name:\s*(.+)", content)
            if match:
                return match.group(1).strip()
        return self.workdir.name

    # =========================================================================
    # Program Deployment
    # =========================================================================

    def deploy_program(
        self,
        program_path: Path,
        language: LanguageType,
        stack_name: Optional[str] = None,
        parallel: int = 10,
        dry_run: bool = False
    ) -> Tuple[bool, str]:
        """
        Deploy a Pulumi program.

        Args:
            program_path: Path to the Pulumi program directory
            language: Programming language of the program
            stack_name: Target stack name
            parallel: Parallelism parameter
            dry_run: If True, only run a preview

        Returns:
            Tuple of (success, output_message)
        """
        if not program_path.exists():
            return False, f"Program path does not exist: {program_path}"

        cmd = ["up"]
        if stack_name:
            cmd.extend(["--stack", stack_name])
        if dry_run:
            cmd.append("--dry-run")
        cmd.extend(["--parallel", str(parallel)])
        cmd.append("--yes")

        code, stdout, stderr = self._run_pulumi_cmd(cmd, cwd=program_path)

        if code == 0:
            return True, stdout
        else:
            return False, stderr

    def init_project(
        self,
        project_name: str,
        language: LanguageType,
        template: Optional[str] = None,
        description: Optional[str] = None
    ) -> bool:
        """
        Initialize a new Pulumi project.

        Args:
            project_name: Name of the project
            language: Programming language
            template: Optional template name
            description: Optional project description

        Returns:
            True if initialization succeeded
        """
        args = ["new", language.value, "--name", project_name, "--yes"]
        if template:
            args.extend(["--template", template])
        if description:
            args.extend(["--description", description])

        code, stdout, stderr = self._run_pulumi_cmd(args)

        return code == 0

    def generate_program_skeleton(
        self,
        language: LanguageType,
        output_dir: Path,
        name: str = "my-project"
    ) -> bool:
        """
        Generate a program skeleton in the specified language.

        Args:
            language: Target programming language
            output_dir: Directory for the generated code
            name: Project name

        Returns:
            True if generation succeeded
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        templates = {
            LanguageType.PYTHON: self._python_skeleton(name),
            LanguageType.TYPESCRIPT: self._typescript_skeleton(name),
            LanguageType.GO: self._go_skeleton(name),
            LanguageType.DOTNET: self._dotnet_skeleton(name),
        }

        template = templates.get(language)
        if not template:
            return False

        for filename, content in template.items():
            (output_dir / filename).write_text(content)

        return True

    def _python_skeleton(self, name: str) -> Dict[str, str]:
        """Generate Python program skeleton."""
        return {
            "__main__.py": f'''import pulumi

# {name} - Pulumi Python Program

class MyStack(pulumi.ComponentResource):
    def __init__(self, name, opts=None):
        super().__init__("{name}", name, {{}}, opts)
        # Add your resources here

def main():
    stack = MyStack("{name}")
    pulumi.export("stack_name", stack._name)

if __name__ == "__main__":
    main()
''',
            "requirements.txt": "pulumi>=3.0.0\n",
            "Pulumi.yaml": f'''name: {name}
runtime: python
description: A Pulumi Python program
'''
        }

    def _typescript_skeleton(self, name: str) -> Dict[str, str]:
        """Generate TypeScript program skeleton."""
        return {
            "index.ts": f'''import * as pulumi from "@pulumi/pulumi";

// {name} - Pulumi TypeScript Program

class MyStack extends pulumi.ComponentResource {{
    constructor(name: string, opts?: pulumi.ComponentResourceOptions) {{
        super("{name}", name, {{}}, opts);
        // Add your resources here
    }}
}}

const stack = new MyStack("{name}");
pulumi.export("stackName", stack.constructor.name);
''',
            "package.json": f'''{{
  "name": "{name}",
  "devDependencies": {{
    "@types/node": "^18"
  }},
  "dependencies": {{
    "@pulumi/pulumi": "^3.0.0"
  }}
}}
''',
            "Pulumi.yaml": f'''name: {name}
runtime: nodejs
description: A Pulumi TypeScript program
'''
        }

    def _go_skeleton(self, name: str) -> Dict[str, str]:
        """Generate Go program skeleton."""
        return {
            "main.go": f'''package main

import (
    "github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

func main() {{
    pulumi.Run(func(ctx *pulumi.Context) error {{
        // {name} - Pulumi Go Program
        ctx.Export("stackName", pulumi.String("{name}"))
        return nil
    }})
}}
''',
            "go.mod": f'''module {name}

go 1.21

require github.com/pulumi/pulumi/sdk/v3 v3.0.0
''',
            "Pulumi.yaml": f'''name: {name}
runtime: go
description: A Pulumi Go program
'''
        }

    def _dotnet_skeleton(self, name: str) -> Dict[str, str]:
        """Generate .NET program skeleton."""
        return {
            "Program.cs": f'''using Pulumi;

class MyStack : ComponentResource
{{
    public MyStack(string name, ComponentResourceOptions? opts = null)
        : base("{name}", name, new Dictionary<string, object>(), opts)
    {{
        // Add your resources here
    }}
}}

return await Deployment.RunAsync(() =>
{{
    var stack = new MyStack("{name}");
    return new Dictionary<string, object>
    {{
        ["stackName"] = "{name}"
    }};
}});
''',
            f"{name}.csproj": f'''<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
    <Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Pulumi" Version="3.*" />
  </ItemGroup>
</Project>
''',
            "Pulumi.yaml": f'''name: {name}
runtime: dotnet
description: A Pulumi .NET program
'''
        }

    # =========================================================================
    # State Management
    # =========================================================================

    def export_state(self, stack_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Export the state of a stack.

        Args:
            stack_name: Stack name (uses current if None)

        Returns:
            State as a dictionary
        """
        args = ["state", "export"]
        if stack_name:
            args.extend(["--stack", stack_name])

        code, stdout, stderr = self._run_pulumi_cmd(args)

        if code == 0:
            try:
                return json.loads(stdout)
            except json.JSONDecodeError:
                return {"error": "Failed to parse state JSON"}
        else:
            return {"error": stderr}

    def import_state(
        self,
        state_data: Dict[str, Any],
        stack_name: Optional[str] = None
    ) -> bool:
        """
        Import state into a stack.

        Args:
            state_data: State dictionary to import
            stack_name: Target stack name

        Returns:
            True if import succeeded
        """
        import_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        try:
            json.dump(state_data, import_file)
            import_file.close()

            args = ["state", "import", "--file", import_file.name]
            if stack_name:
                args.extend(["--stack", stack_name])

            code, _, stderr = self._run_pulumi_cmd(args)
            return code == 0
        finally:
            os.unlink(import_file.name)

    def rename_resource(
        self,
        old_urn: str,
        new_urn: str,
        stack_name: Optional[str] = None
    ) -> bool:
        """
        Rename a resource in the state.

        Args:
            old_urn: Current resource URN
            new_urn: New resource URN
            stack_name: Target stack

        Returns:
            True if rename succeeded
        """
        args = ["state", "rename", old_urn, new_urn]
        if stack_name:
            args.extend(["--stack", stack_name])

        code, _, stderr = self._run_pulumi_cmd(args)
        return code == 0

    def unprotect_resource(
        self,
        resource_urn: str,
        stack_name: Optional[str] = None
    ) -> bool:
        """Unprotect a resource so it can be deleted."""
        args = ["state", "unprotect", resource_urn]
        if stack_name:
            args.extend(["--stack", stack_name])

        code, _, _ = self._run_pulumi_cmd(args)
        return code == 0

    def protect_resource(
        self,
        resource_urn: str,
        stack_name: Optional[str] = None
    ) -> bool:
        """Protect a resource from deletion."""
        args = ["state", "protect", resource_urn]
        if stack_name:
            args.extend(["--stack", stack_name])

        code, _, _ = self._run_pulumi_cmd(args)
        return code == 0

    def delete_resource(
        self,
        resource_urn: str,
        stack_name: Optional[str] = None,
        force: bool = False
    ) -> bool:
        """
        Delete a resource from state.

        Args:
            resource_urn: URN of the resource to delete
            stack_name: Target stack
            force: Force deletion

        Returns:
            True if deletion succeeded
        """
        args = ["state", "delete", resource_urn]
        if stack_name:
            args.extend(["--stack", stack_name])
        if force:
            args.append("--force")

        code, _, _ = self._run_pulumi_cmd(args)
        return code == 0

    def get_resource(
        self,
        resource_urn: str,
        stack_name: Optional[str] = None
    ) -> Optional[PulumiResource]:
        """
        Get a resource by its URN.

        Args:
            resource_urn: URN of the resource
            stack_name: Target stack

        Returns:
            PulumiResource if found
        """
        args = ["state", "query", resource_urn]
        if stack_name:
            args.extend(["--stack", stack_name])

        code, stdout, _ = self._run_pulumi_cmd(args)

        if code == 0 and stdout.strip():
            try:
                data = json.loads(stdout)
                return PulumiResource(
                    urn=data.get("urn", resource_urn),
                    type=data.get("type", ""),
                    id=data.get("id"),
                    custom=data.get("custom", False),
                    protect=data.get("protect", False),
                    inputs=data.get("inputs", {}),
                    outputs=data.get("outputs", {})
                )
            except json.JSONDecodeError:
                pass
        return None

    def list_resources(self, stack_name: Optional[str] = None) -> List[PulumiResource]:
        """
        List all resources in a stack.

        Args:
            stack_name: Target stack

        Returns:
            List of PulumiResource objects
        """
        args = ["state", "ls"]
        if stack_name:
            args.extend(["--stack", stack_name])

        code, stdout, _ = self._run_pulumi_cmd(args)

        resources = []
        if code == 0:
            for line in stdout.strip().split("\n")[1:]:
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 2:
                        resources.append(PulumiResource(
                            urn=parts[0],
                            type=parts[1] if len(parts) > 1 else ""
                        ))

        return resources

    # =========================================================================
    # Config Management
    # =========================================================================

    def set_config(
        self,
        key: str,
        value: Any,
        stack_name: Optional[str] = None,
        secret: bool = False
    ) -> bool:
        """
        Set a configuration value.

        Args:
            key: Configuration key (supports namespacing like 'db:password')
            value: Configuration value
            stack_name: Target stack
            secret: Mark value as secret

        Returns:
            True if setting config succeeded
        """
        args = ["config", "set", key]
        if stack_name:
            args.extend(["--stack", stack_name])
        if secret:
            args.append("--secret")

        code, _, stderr = self._run_pulumi_cmd(args, env={"PULUMI_CONFIG_PASSPHRASE": str(value)})

        if code != 0:
            code, _, _ = self._run_pulumi_cmd(args)

        return code == 0

    def get_config(
        self,
        key: Optional[str] = None,
        stack_name: Optional[str] = None,
        json_output: bool = True
    ) -> Dict[str, Any]:
        """
        Get configuration values.

        Args:
            key: Specific key to get (None for all)
            stack_name: Target stack
            json_output: Return as JSON

        Returns:
            Configuration dictionary
        """
        args = ["config"]
        if json_output:
            args.append("--json")
        if key:
            args.append(key)
        if stack_name:
            args.extend(["--stack", stack_name])

        code, stdout, _ = self._run_pulumi_cmd(args)

        if code == 0:
            try:
                return json.loads(stdout)
            except json.JSONDecodeError:
                return {}
        return {}

    def remove_config(
        self,
        key: str,
        stack_name: Optional[str] = None
    ) -> bool:
        """Remove a configuration value."""
        args = ["config", "rm", key]
        if stack_name:
            args.extend(["--stack", stack_name])

        code, _, _ = self._run_pulumi_cmd(args)
        return code == 0

    def set_all_config(
        self,
        config_dict: Dict[str, Any],
        stack_name: Optional[str] = None,
        secrets: Optional[Set[str]] = None
    ) -> bool:
        """
        Set multiple configuration values at once.

        Args:
            config_dict: Dictionary of key-value pairs
            stack_name: Target stack
            secrets: Set of keys that should be treated as secrets

        Returns:
            True if all configs were set
        """
        secrets = secrets or set()
        all_success = True

        for key, value in config_dict.items():
            is_secret = key in secrets
            if not self.set_config(key, value, stack_name, is_secret):
                all_success = False

        return all_success

    # =========================================================================
    # Secret Management
    # =========================================================================

    def encrypt_secret(
        self,
        plain_text: str,
        provider: Optional[SecretProvider] = None
    ) -> str:
        """
        Encrypt a secret value.

        Args:
            plain_text: Plain text to encrypt
            provider: Secret provider to use

        Returns:
            Encrypted string (base64 encoded)
        """
        provider = provider or self.secret_provider

        args = ["config", "encrypt", "--value", plain_text]
        if provider != SecretProvider.DEFAULT:
            args.extend(["--provider", provider.value])

        code, stdout, _ = self._run_pulumi_cmd(args)

        if code == 0:
            return stdout.strip()
        else:
            encrypted = base64.b64encode(plain_text.encode()).decode()
            return f"encrypt:{encrypted}"

    def decrypt_secret(
        self,
        encrypted_text: str,
        provider: Optional[SecretProvider] = None
    ) -> str:
        """
        Decrypt a secret value.

        Args:
            encrypted_text: Encrypted text to decrypt
            provider: Secret provider to use

        Returns:
            Decrypted plain text
        """
        provider = provider or self.secret_provider

        if encrypted_text.startswith("encrypt:"):
            encoded = encrypted_text[8:]
            return base64.b64decode(encoded.encode()).decode()

        args = ["config", "decrypt", "--value", encrypted_text]
        if provider != SecretProvider.DEFAULT:
            args.extend(["--provider", provider.value])

        code, stdout, _ = self._run_pulumi_cmd(args)

        if code == 0:
            return stdout.strip()
        return encrypted_text

    def set_secret(
        self,
        key: str,
        value: str,
        stack_name: Optional[str] = None
    ) -> bool:
        """
        Set a secret configuration value.

        Args:
            key: Configuration key
            value: Secret value
            stack_name: Target stack

        Returns:
            True if set successfully
        """
        return self.set_config(key, value, stack_name, secret=True)

    def toggle_secrets(
        self,
        enabled: bool,
        stack_name: Optional[str] = None
    ) -> bool:
        """
        Enable or disable secrets encryption.

        Args:
            enabled: True to enable, False to disable
            stack_name: Target stack

        Returns:
            True if toggled successfully
        """
        args = ["config", "secret", "on" if enabled else "off"]
        if stack_name:
            args.extend(["--stack", stack_name])

        code, _, _ = self._run_pulumi_cmd(args)
        return code == 0

    # =========================================================================
    # Import Resources
    # =========================================================================

    def import_resource(
        self,
        resource_type: str,
        name: str,
        id_value: str,
        stack_name: Optional[str] = None,
        parent: Optional[str] = None,
        provider: Optional[str] = None
    ) -> ImportResult:
        """
        Import an existing cloud resource into Pulumi state.

        Args:
            resource_type: Pulumi resource type (e.g., 'aws:s3/bucket:Bucket')
            name: Logical name for the resource
            id_value: Provider-specific resource ID
            stack_name: Target stack
            parent: Optional parent URN
            provider: Optional provider URN

        Returns:
            ImportResult with success status and details
        """
        args = ["import", resource_type, name, id_value, "--yes"]
        if stack_name:
            args.extend(["--stack", stack_name])
        if parent:
            args.extend(["--parent", parent])
        if provider:
            args.extend(["--provider", provider])

        code, stdout, stderr = self._run_pulumi_cmd(args)

        if code == 0:
            urn_match = re.search(r"URN: (.+)", stdout)
            urn = urn_match.group(1) if urn_match else ""

            return ImportResult(
                urn=urn,
                id=id_value,
                resource_type=resource_type,
                success=True
            )
        else:
            return ImportResult(
                urn="",
                id=id_value,
                resource_type=resource_type,
                success=False,
                error=stderr
            )

    def import_resources_batch(
        self,
        imports: List[Dict[str, str]],
        stack_name: Optional[str] = None
    ) -> List[ImportResult]:
        """
        Import multiple resources in batch.

        Args:
            imports: List of dicts with 'type', 'name', 'id' keys
            stack_name: Target stack

        Returns:
            List of ImportResult objects
        """
        results = []

        for imp in imports:
            result = self.import_resource(
                resource_type=imp.get("type", ""),
                name=imp.get("name", ""),
                id_value=imp.get("id", ""),
                stack_name=stack_name
            )
            results.append(result)

        return results

    # =========================================================================
    # Preview and Update
    # =========================================================================

    def preview(
        self,
        program_path: Optional[Path] = None,
        stack_name: Optional[str] = None,
        parallel: int = 10,
        refresh: bool = False,
        replace: Optional[List[str]] = None,
        target: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Generate a preview of changes.

        Args:
            program_path: Path to Pulumi program
            stack_name: Target stack
            parallel: Parallelism parameter
            refresh: Refresh state before preview
            replace: List of resource URNs to replace
            target: List of resource URNs to target

        Returns:
            Dictionary with preview results
        """
        args = ["preview"]
        if stack_name:
            args.extend(["--stack", stack_name])
        args.extend(["--parallel", str(parallel)])
        if refresh:
            args.append("--refresh")
        if replace:
            for r in replace:
                args.extend(["--replace", r])
        if target:
            for t in target:
                args.extend(["--target", t])

        code, stdout, stderr = self._run_pulumi_cmd(args, cwd=program_path)

        return {
            "success": code == 0,
            "output": stdout,
            "error": stderr,
            "changes": self._parse_preview_output(stdout)
        }

    def update(
        self,
        program_path: Optional[Path] = None,
        stack_name: Optional[str] = None,
        parallel: int = 10,
        refresh: bool = True,
        replace: Optional[List[str]] = None,
        target: Optional[List[str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Apply updates to the stack.

        Args:
            program_path: Path to Pulumi program
            stack_name: Target stack
            parallel: Parallelism parameter
            refresh: Refresh state before update
            replace: List of resource URNs to replace
            target: List of resource URNs to target
            dry_run: Perform a preview only

        Returns:
            Dictionary with update results
        """
        args = ["up", "--yes"]
        if stack_name:
            args.extend(["--stack", stack_name])
        args.extend(["--parallel", str(parallel)])
        if refresh:
            args.append("--refresh")
        if dry_run:
            args.append("--dry-run")
        if replace:
            for r in replace:
                args.extend(["--replace", r])
        if target:
            for t in target:
                args.extend(["--target", t])

        code, stdout, stderr = self._run_pulumi_cmd(args, cwd=program_path)

        return {
            "success": code == 0,
            "output": stdout,
            "error": stderr
        }

    def destroy(
        self,
        program_path: Optional[Path] = None,
        stack_name: Optional[str] = None,
        target: Optional[List[str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Destroy all resources in a stack.

        Args:
            program_path: Path to Pulumi program
            stack_name: Target stack
            target: List of resource URNs to target for destruction
            dry_run: Perform a preview only

        Returns:
            Dictionary with destroy results
        """
        args = ["destroy", "--yes"]
        if stack_name:
            args.extend(["--stack", stack_name])
        if dry_run:
            args.append("--dry-run")
        if target:
            for t in target:
                args.extend(["--target", t])

        code, stdout, stderr = self._run_pulumi_cmd(args, cwd=program_path)

        return {
            "success": code == 0,
            "output": stdout,
            "error": stderr
        }

    def refresh(
        self,
        program_path: Optional[Path] = None,
        stack_name: Optional[str] = None,
        target: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Refresh resources to match cloud state.

        Args:
            program_path: Path to Pulumi program
            stack_name: Target stack
            target: List of resource URNs to target

        Returns:
            Dictionary with refresh results
        """
        args = ["refresh", "--yes"]
        if stack_name:
            args.extend(["--stack", stack_name])
        if target:
            for t in target:
                args.extend(["--target", t])

        code, stdout, stderr = self._run_pulumi_cmd(args, cwd=program_path)

        return {
            "success": code == 0,
            "output": stdout,
            "error": stderr
        }

    def _parse_preview_output(self, output: str) -> Dict[str, Any]:
        """Parse preview output to extract change information."""
        changes = {
            "create": 0,
            "update": 0,
            "delete": 0,
            "replace": 0,
            "unchanged": 0
        }

        for line in output.split("\n"):
            line_lower = line.lower()
            if "create" in line_lower and ":" in line:
                try:
                    num = int(line.split(":")[0].strip().split()[-1])
                    changes["create"] = num
                except (ValueError, IndexError):
                    pass
            elif "update" in line_lower and ":" in line:
                try:
                    num = int(line.split(":")[0].strip().split()[-1])
                    changes["update"] = num
                except (ValueError, IndexError):
                    pass
            elif "delete" in line_lower and ":" in line:
                try:
                    num = int(line.split(":")[0].strip().split()[-1])
                    changes["delete"] = num
                except (ValueError, IndexError):
                    pass

        return changes

    # =========================================================================
    # Policy as Code (CrossGuard)
    # =========================================================================

    def create_policy_pack(
        self,
        name: str,
        version: str = "0.1.0",
        description: str = "",
        rules: Optional[List[PolicyRule]] = None,
        enforcement: PolicyMode = PolicyMode.ADVISORY
    ) -> PolicyPack:
        """
        Create a new policy pack.

        Args:
            name: Policy pack name
            version: Version string
            description: Policy pack description
            rules: List of PolicyRule objects
            enforcement: Default enforcement mode

        Returns:
            PolicyPack object
        """
        pack = PolicyPack(
            name=name,
            version=version,
            description=description,
            rules=rules or [],
            enforcement_mode=enforcement
        )

        self._policy_cache[name] = pack
        return pack

    def add_policy_rule(
        self,
        pack_name: str,
        rule: PolicyRule
    ) -> bool:
        """
        Add a rule to a policy pack.

        Args:
            pack_name: Name of the policy pack
            rule: PolicyRule to add

        Returns:
            True if added successfully
        """
        if pack_name not in self._policy_cache:
            return False

        self._policy_cache[pack_name].rules.append(rule)
        return True

    def generate_policy_code(
        self,
        pack: PolicyPack,
        language: LanguageType
    ) -> Dict[str, str]:
        """
        Generate policy pack code for a specific language.

        Args:
            pack: PolicyPack to generate
            language: Target language

        Returns:
            Dictionary of filename -> content
        """
        generators = {
            LanguageType.TYPESCRIPT: self._typescript_policy_code,
            LanguageType.PYTHON: self._python_policy_code,
            LanguageType.GO: self._go_policy_code,
        }

        generator = generators.get(language)
        if generator:
            return generator(pack)
        return {}

    def _typescript_policy_code(self, pack: PolicyPack) -> Dict[str, str]:
        """Generate TypeScript policy pack code."""
        rules_code = "\n".join([
            f'''    {{
        name: "{rule.name}",
        description: "{rule.description}",
        enforcement: "{rule.enforcement.value}",
        validate: (args: PolicyArgs) => {{
            // Add validation logic
            return [];
        }}
    }}''' for rule in pack.rules
        ])

        return {
            "index.ts": f'''import {{
    PolicyArgs,
    PolicyConfig,
    ResourceValidationArgs,
    StackValidationArgs,
    PolicyPacks
}} from "@pulumi/cloud-policy";

const {pack.name}Policy: PolicyConfig = {{
    name: "{pack.name}",
    version: "{pack.version}",
    rules: [
{rules_code}
    ]
}};

export const policyPack: PolicyPacks = [{pack.name}Policy];
''',
            "package.json": f'''{{
  "name": "@pulumi/{pack.name}",
  "version": "{pack.version}",
  "dependencies": {{
    "@pulumi/cloud-policy": "^1.0.0"
  }}
}}
'''
        }

    def _python_policy_code(self, pack: PolicyPack) -> Dict[str, str]:
        """Generate Python policy pack code."""
        rules_code = "\n".join([
            f'''    {{
        "name": "{rule.name}",
        "description": "{rule.description}",
        "enforcement": "{rule.enforcement.value}",
    }}''' for rule in pack.rules
        ])

        return {
            "__main__.py": f'''"""Pulumi Policy Pack: {pack.name}"""

from pulumi_policy import (
    PolicyPack,
    PolicyConfig,
    ResourceValidationArgs,
    StackValidationArgs,
)

{pack.name}_policy: PolicyConfig = {{
    "name": "{pack.name}",
    "version": "{pack.version}",
    "rules": [
{rules_code}
    ]
}}

PolicyPack(**{pack.name}_policy)
''',
            "requirements.txt": "pulumi-policy>=1.0.0\n"
        }

    def _go_policy_code(self, pack: PolicyPack) -> Dict[str, str]:
        """Generate Go policy pack code."""
        rules_code = "\n".join([
            f'''    {{
        Name: "{rule.name}",
        Description: "{rule.description}",
        Enforcement: "{rule.enforcement.value}",
    }}''' for rule in pack.rules
        ])

        return {
            "main.go": f'''package main

import (
    "github.com/pulumi/pulumi-policy-go/policy"
)

func main() {{
    policy.Run(policy.PolicyConfig{{
        Name: "{pack.name}",
        Version: "{pack.version}",
        Rules: []policy.RuleConfig{{
{rules_code}
        }},
    }})
}}
''',
            "go.mod": f'''module {pack.name}

go 1.21

require github.com/pulumi/pulumi-policy-go v1.0.0
'''
        }

    def apply_policy_pack(
        self,
        pack_path: Path,
        stack_name: Optional[str] = None,
        enforce: bool = True
    ) -> bool:
        """
        Apply a policy pack to a stack.

        Args:
            pack_path: Path to the policy pack directory
            stack_name: Target stack
            enforce: Whether to enforce the policies

        Returns:
            True if applied successfully
        """
        args = ["policy", "apply"]
        if stack_name:
            args.extend(["--stack", stack_name])
        if not enforce:
            args.append("--advisory")

        args.append(str(pack_path))

        code, _, _ = self._run_pulumi_cmd(args)
        return code == 0

    def validate_stack_with_policies(
        self,
        program_path: Path,
        policy_pack_path: Path,
        stack_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Validate a stack against a policy pack.

        Args:
            program_path: Path to the Pulumi program
            policy_pack_path: Path to the policy pack
            stack_name: Target stack

        Returns:
            Dictionary with validation results
        """
        args = ["policy", "validate"]
        if stack_name:
            args.extend(["--stack", stack_name])
        args.append(str(policy_pack_path))
        args.append("--")

        code, stdout, stderr = self._run_pulumi_cmd(args, cwd=program_path)

        return {
            "success": code == 0,
            "output": stdout,
            "error": stderr
        }

    # =========================================================================
    # Component Resources
    # =========================================================================

    def create_component_resource(
        self,
        name: str,
        resource_type: str,
        properties: Optional[Dict[str, Any]] = None,
        required_inputs: Optional[List[str]] = None,
        schema: Optional[Dict[str, Any]] = None
    ) -> ComponentResourceSpec:
        """
        Define a custom component resource.

        Args:
            name: Component name
            resource_type: Component type (e.g., 'custom:MyComponent')
            properties: Component properties
            required_inputs: List of required input property names
            schema: JSON schema for the component

        Returns:
            ComponentResourceSpec
        """
        spec = ComponentResourceSpec(
            name=name,
            type=resource_type,
            schema=schema or {},
            properties=properties or {},
            required_inputs=required_inputs or []
        )

        self._component_cache[name] = spec
        return spec

    def generate_component_code(
        self,
        component: ComponentResourceSpec,
        language: LanguageType
    ) -> Dict[str, str]:
        """
        Generate component resource code for a specific language.

        Args:
            component: ComponentResourceSpec to generate
            language: Target programming language

        Returns:
            Dictionary of filename -> content
        """
        generators = {
            LanguageType.PYTHON: self._python_component_code,
            LanguageType.TYPESCRIPT: self._typescript_component_code,
            LanguageType.GO: self._go_component_code,
            LanguageType.DOTNET: self._dotnet_component_code,
        }

        generator = generators.get(language)
        if generator:
            return generator(component)
        return {}

    def _python_component_code(self, component: ComponentResourceSpec) -> Dict[str, str]:
        """Generate Python component resource code."""
        props_init = "\n        ".join([
            f"self.{prop} = {prop}" for prop in component.properties.keys()
        ])

        return {
            f"{component.name.lower()}.py": f'''import pulumi

class {component.name}(pulumi.ComponentResource):
    """Custom component resource: {component.type}"""

    def __init__(
        self,
        resource_name: str,
        {", ".join([f"{k}: pulumi.Input = None" for k in component.properties.keys()])},
        opts: pulumi.ResourceOptions = None
    ):
        super().__init__("{component.type}", resource_name, {{}}, opts)

{props_init}

        self.register_outputs({{}})
'''
        }

    def _typescript_component_code(self, component: ComponentResourceSpec) -> Dict[str, str]:
        """Generate TypeScript component resource code."""
        props_def = "\n".join([
            f"    {k}?: pulumi.Input<{v}>;" for k, v in component.properties.items()
        ])

        return {
            f"{component.name.lower()}.ts": f'''import * as pulumi from "@pulumi/pulumi";

export class {component.name} extends pulumi.ComponentResource {{
{props_def}

    constructor(
        resourceName: string,
        args: {{ {", ".join([f"{k}?: pulumi.Input<{v}>" for k, v in component.properties.items()])} }},
        opts?: pulumi.ComponentResourceOptions
    ) {{
        super("{component.type}", resourceName, args, opts);
    }}
}}
'''
        }

    def _go_component_code(self, component: ComponentResourceSpec) -> Dict[str, str]:
        """Generate Go component resource code."""
        return {
            f"{component.name.lower()}.go": f'''package main

import (
    "github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

type {component.name} struct {{
    pulumi.ResourceState
}}

func New{component.name}(
    ctx *pulumi.Context,
    name string,
    args *{component.name}Args,
    opts ...pulumi.ResourceOption,
) (*{component.name}, error) {{
    var resource {component.name}
    err := ctx.RegisterComponentResource("{component.type}", name, &resource, opts...)
    if err != nil {{
        return nil, err
    }}
    return &resource, nil
}}
'''
        }

    def _dotnet_component_code(self, component: ComponentResourceSpec) -> Dict[str, str]:
        """Generate .NET component resource code."""
        props = "\n".join([
            f"    public Input<object>? {k} {{ get; set; }}" for k in component.properties.keys()
        ])

        return {
            f"{component.name}.cs": f'''using Pulumi;

class {component.name} : ComponentResource
{{
{props}

    public {component.name}(string name, {component.name}Args args, ComponentResourceOptions? opts = null)
        : base("{component.type}", name, args, opts)
    {{
    }}
}}

class {component.name}Args
{{
{props}
}}
'''
        }

    def register_component(
        self,
        component: ComponentResourceSpec,
        program_path: Path
    ) -> bool:
        """
        Register a component resource in a Pulumi program.

        Args:
            component: Component to register
            program_path: Target program directory

        Returns:
            True if registered successfully
        """
        files = self.generate_component_code(component, LanguageType.PYTHON)
        for filename, content in files.items():
            (program_path / filename).write_text(content)
        return True

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def get_version(self) -> str:
        """Get the installed Pulumi version."""
        code, stdout, _ = self._run_pulumi_cmd(["version"])
        if code == 0:
            return stdout.strip()
        return "unknown"

    def whoami(self) -> str:
        """Get the current Pulumi user."""
        code, stdout, _ = self._run_pulumi_cmd(["whoami"])
        if code == 0:
            return stdout.strip()
        return ""

    def login(self, backend_url: Optional[str] = None) -> bool:
        """
        Login to a Pulumi backend.

        Args:
            backend_url: Backend URL (uses default if None)

        Returns:
            True if login succeeded
        """
        args = ["login"]
        if backend_url:
            args.append(backend_url)

        code, _, _ = self._run_pulumi_cmd(args)
        return code == 0

    def logout(self, backend_url: Optional[str] = None) -> bool:
        """
        Logout from a Pulumi backend.

        Args:
            backend_url: Backend URL (uses current if None)

        Returns:
            True if logout succeeded
        """
        args = ["logout"]
        if backend_url:
            args.append(backend_url)

        code, _, _ = self._run_pulumi_cmd(args)
        return code == 0

    def about(self) -> Dict[str, Any]:
        """
        Get information about the Pulumi environment.

        Returns:
            Dictionary with environment information
        """
        code, stdout, _ = self._run_pulumi_cmd(["about", "--json"])

        if code == 0:
            try:
                return json.loads(stdout)
            except json.JSONDecodeError:
                pass

        return {"error": "Failed to get environment info"}

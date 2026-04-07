"""
Terraform infrastructure as code actions.
"""
from __future__ import annotations

import json
import subprocess
import os
from pathlib import Path
from typing import Dict, Any, Optional, List


def run_terraform(
    args: List[str],
    cwd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    timeout: int = 300
) -> Dict[str, Any]:
    """
    Execute a terraform command.

    Args:
        args: Terraform arguments.
        cwd: Working directory.
        env: Environment variables.
        timeout: Command timeout in seconds.

    Returns:
        Dictionary with command result.
    """
    cmd = ['terraform'] + args

    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)

    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            env=merged_env,
            capture_output=True,
            text=True,
            timeout=timeout
        )

        return {
            'success': result.returncode == 0,
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
        }
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'returncode': -1,
            'stdout': '',
            'stderr': 'Command timed out',
        }
    except FileNotFoundError:
        return {
            'success': False,
            'returncode': -1,
            'stdout': '',
            'stderr': 'terraform not found. Is it installed?',
        }
    except Exception as e:
        return {
            'success': False,
            'returncode': -1,
            'stdout': '',
            'stderr': str(e),
        }


def terraform_init(
    cwd: str,
    backend: Optional[str] = None,
    var_file: Optional[str] = None
) -> Dict[str, Any]:
    """
    Initialize a Terraform configuration.

    Args:
        cwd: Working directory with Terraform files.
        backend: Optional backend configuration.
        var_file: Optional variable file path.

    Returns:
        Initialization result.
    """
    args = ['init']

    if backend:
        args.extend(['-backend=true', '-backend-config', backend])

    if var_file:
        args.extend(['-var-file', var_file])

    result = run_terraform(args, cwd=cwd)

    if result['success']:
        return {'success': True, 'message': 'Terraform initialized'}
    return {'success': False, 'error': result['stderr']}


def terraform_plan(
    cwd: str,
    out_file: Optional[str] = None,
    var_file: Optional[str] = None,
    vars: Optional[Dict[str, str]] = None,
    destroy: bool = False
) -> Dict[str, Any]:
    """
    Create a Terraform execution plan.

    Args:
        cwd: Working directory.
        out_file: Optional file to save plan.
        var_file: Optional variable file.
        vars: Optional variable overrides.
        destroy: Create a destroy plan.

    Returns:
        Plan result with changes summary.
    """
    args = ['plan']

    if out_file:
        args.extend(['-out', out_file])

    if var_file:
        args.extend(['-var-file', var_file])

    if vars:
        for key, value in vars.items():
            args.extend(['-var', f'{key}={value}'])

    if destroy:
        args.append('-destroy')

    args.extend(['-no-color'])

    result = run_terraform(args, cwd=cwd)

    return {
        'success': result['success'],
        'output': result['stdout'],
        'error': result['stderr'] if not result['success'] else None,
        'has_changes': 'No changes.' not in result['stdout'],
    }


def terraform_apply(
    cwd: str,
    plan_file: Optional[str] = None,
    var_file: Optional[str] = None,
    vars: Optional[Dict[str, str]] = None,
    auto_approve: bool = True
) -> Dict[str, Any]:
    """
    Apply Terraform changes.

    Args:
        cwd: Working directory.
        plan_file: Plan file to apply.
        var_file: Optional variable file.
        vars: Optional variable overrides.
        auto_approve: Skip approval prompt.

    Returns:
        Apply result.
    """
    args = ['apply']

    if plan_file:
        args.append(plan_file)

    if var_file:
        args.extend(['-var-file', var_file])

    if vars:
        for key, value in vars.items():
            args.extend(['-var', f'{key}={value}'])

    if auto_approve:
        args.append('-auto-approve')

    args.extend(['-no-color'])

    result = run_terraform(args, cwd=cwd, timeout=600)

    return {
        'success': result['success'],
        'output': result['stdout'],
        'error': result['stderr'] if not result['success'] else None,
    }


def terraform_destroy(
    cwd: str,
    var_file: Optional[str] = None,
    vars: Optional[Dict[str, str]] = None,
    auto_approve: bool = True
) -> Dict[str, Any]:
    """
    Destroy Terraform-managed infrastructure.

    Args:
        cwd: Working directory.
        var_file: Optional variable file.
        vars: Optional variable overrides.
        auto_approve: Skip approval prompt.

    Returns:
        Destroy result.
    """
    args = ['destroy']

    if var_file:
        args.extend(['-var-file', var_file])

    if vars:
        for key, value in vars.items():
            args.extend(['-var', f'{key}={value}'])

    if auto_approve:
        args.append('-auto-approve')

    args.extend(['-no-color'])

    result = run_terraform(args, cwd=cwd, timeout=600)

    return {
        'success': result['success'],
        'output': result['stdout'],
        'error': result['stderr'] if not result['success'] else None,
    }


def terraform_validate(cwd: str) -> Dict[str, Any]:
    """
    Validate Terraform configuration.

    Args:
        cwd: Working directory.

    Returns:
        Validation result.
    """
    result = run_terraform(['validate'], cwd=cwd)

    return {
        'success': result['success'],
        'output': result['stdout'],
        'error': result['stderr'] if not result['success'] else None,
    }


def terraform_fmt(
    cwd: str,
    check: bool = False,
    recursive: bool = True
) -> Dict[str, Any]:
    """
    Format Terraform configuration files.

    Args:
        cwd: Working directory.
        check: Check if formatting is needed without modifying.
        recursive: Recursively process subdirectories.

    Returns:
        Format result.
    """
    args = ['fmt']

    if check:
        args.append('-check')

    if recursive:
        args.append('-recursive')

    result = run_terraform(args, cwd=cwd)

    return {
        'success': result['success'] or result['returncode'] == 0,
        'output': result['stdout'],
        'changed': bool(result['stdout'].strip()),
    }


def terraform_show(plan_file: str) -> Dict[str, Any]:
    """
    Show a Terraform plan file.

    Args:
        plan_file: Path to plan file.

    Returns:
        Plan details.
    """
    result = run_terraform(['show', '-json', plan_file])

    if result['success']:
        try:
            return {'success': True, 'plan': json.loads(result['stdout'])}
        except json.JSONDecodeError:
            return {'success': True, 'plan_raw': result['stdout']}

    return {'success': False, 'error': result['stderr']}


def terraform_output(cwd: str, name: Optional[str] = None) -> Dict[str, Any]:
    """
    Get Terraform output values.

    Args:
        cwd: Working directory.
        name: Specific output name (None for all).

    Returns:
        Output values.
    """
    args = ['output', '-json']

    if name:
        args[1] = name

    result = run_terraform(args, cwd=cwd)

    if result['success']:
        try:
            return {'success': True, 'outputs': json.loads(result['stdout'])}
        except json.JSONDecodeError:
            return {'success': False, 'error': 'Failed to parse output'}

    return {'success': False, 'error': result['stderr']}


def terraform_state_list(cwd: str) -> List[str]:
    """
    List resources in Terraform state.

    Args:
        cwd: Working directory.

    Returns:
        List of resource addresses.
    """
    result = run_terraform(['state', 'list'], cwd=cwd)

    if result['success']:
        return [line.strip() for line in result['stdout'].splitlines() if line.strip()]

    return []


def terraform_state_show(cwd: str, resource: str) -> Dict[str, Any]:
    """
    Show details of a resource in state.

    Args:
        cwd: Working directory.
        resource: Resource address.

    Returns:
        Resource state.
    """
    result = run_terraform(['state', 'show', resource], cwd=cwd)

    if result['success']:
        return {'success': True, 'state': result['stdout']}

    return {'success': False, 'error': result['stderr']}


def terraform_refresh(cwd: str, var_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Refresh Terraform state from real infrastructure.

    Args:
        cwd: Working directory.
        var_file: Optional variable file.

    Returns:
        Refresh result.
    """
    args = ['refresh']

    if var_file:
        args.extend(['-var-file', var_file])

    result = run_terraform(args, cwd=cwd)

    return {
        'success': result['success'],
        'output': result['stdout'],
        'error': result['stderr'] if not result['success'] else None,
    }


def terraform_import(
    cwd: str,
    resource: str,
    id: str,
    var_file: Optional[str] = None
) -> Dict[str, Any]:
    """
    Import existing infrastructure into Terraform state.

    Args:
        cwd: Working directory.
        resource: Target resource address.
        id: Resource ID in the provider.
        var_file: Optional variable file.

    Returns:
        Import result.
    """
    args = ['import', resource, id]

    if var_file:
        args.extend(['-var-file', var_file])

    result = run_terraform(args, cwd=cwd)

    return {
        'success': result['success'],
        'output': result['stdout'],
        'error': result['stderr'] if not result['success'] else None,
    }


def get_terraform_version() -> Dict[str, Any]:
    """
    Get Terraform version information.

    Returns:
        Version information.
    """
    result = run_terraform(['version'])

    return {
        'success': result['success'],
        'version': result['stdout'].strip() if result['success'] else None,
        'error': result['stderr'] if not result['success'] else None,
    }


def terraform_workspace_list(cwd: str) -> Dict[str, Any]:
    """
    List Terraform workspaces.

    Args:
        cwd: Working directory.

    Returns:
        Workspace list.
    """
    result = run_terraform(['workspace', 'list'], cwd=cwd)

    if result['success']:
        workspaces = [
            line.strip().lstrip('* ').strip()
            for line in result['stdout'].splitlines()
        ]
        return {'success': True, 'workspaces': workspaces}

    return {'success': False, 'error': result['stderr']}


def terraform_workspace_select(cwd: str, workspace: str) -> Dict[str, Any]:
    """
    Select a Terraform workspace.

    Args:
        cwd: Working directory.
        workspace: Workspace name.

    Returns:
        Selection result.
    """
    result = run_terraform(['workspace', 'select', workspace], cwd=cwd)

    return {
        'success': result['success'],
        'output': result['stdout'],
        'error': result['stderr'] if not result['success'] else None,
    }


def terraform_graph(cwd: str) -> str:
    """
    Generate Terraform dependency graph.

    Args:
        cwd: Working directory.

    Returns:
        Graph in DOT format.
    """
    result = run_terraform(['graph'], cwd=cwd)

    if result['success']:
        return result['stdout']

    return ''

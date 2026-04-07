"""
Ansible Automation Utilities.

Helpers for running Ansible playbooks and modules programmatically,
managing inventories, and automating ad-hoc commands.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import subprocess
from pathlib import Path
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

ANSIBLE_PATH = os.getenv("ANSIBLE_PATH", "ansible")
ANSIBLE_PLAYBOOK = os.getenv("ANSIBLE_PLAYBOOK", "ansible-playbook")
ANSIBLE_CONFIG = os.getenv("ANSIBLE_CONFIG", "")
INVENTORY = os.getenv("ANSIBLE_INVENTORY", "")


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #

def _run(
    args: list[str],
    cwd: Optional[str] = None,
    check: bool = False,
) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    if ANSIBLE_CONFIG:
        env["ANSIBLE_CONFIG"] = ANSIBLE_CONFIG
    return subprocess.run(
        args,
        capture_output=True,
        text=True,
        cwd=cwd,
        env=env,
        check=check,
    )


# --------------------------------------------------------------------------- #
# Playbooks
# --------------------------------------------------------------------------- #

def run_playbook(
    playbook: str,
    inventory: Optional[str] = None,
    tags: Optional[list[str]] = None,
    skip_tags: Optional[list[str]] = None,
    limit: Optional[str] = None,
    extra_vars: Optional[dict[str, Any]] = None,
    become: bool = False,
    check: bool = False,
    diff: bool = False,
    list_tags: bool = False,
    list_tasks: bool = False,
    verbose: int = 0,
) -> subprocess.CompletedProcess:
    """
    Run an Ansible playbook.

    Args:
        playbook: Path to the playbook YAML file.
        inventory: Path to the inventory file or inventory host pattern.
        tags: Tags to include.
        skip_tags: Tags to skip.
        limit: Limit to specific hosts.
        extra_vars: Extra variables as a dict.
        become: Run as privilege escalation.
        check: Dry-run mode.
        diff: Show file changes.
        list_tags: List available tags and exit.
        list_tasks: List all tasks and exit.
        verbose: Verbosity level (-v, -vv, -vvv, ...).

    Returns:
        CompletedProcess with stdout/stderr.
    """
    args = [ANSIBLE_PLAYBOOK, playbook]
    if inventory or INVENTORY:
        args += ["-i", inventory or INVENTORY]
    if tags:
        args += ["--tags", ",".join(tags)]
    if skip_tags:
        args += ["--skip-tags", ",".join(skip_tags)]
    if limit:
        args += ["--limit", limit]
    if extra_vars:
        for k, v in extra_vars.items():
            args += ["-e", f"{k}={json.dumps(v)}"]
    if become:
        args.append("--become")
    if check:
        args.append("--check")
    if diff:
        args.append("--diff")
    if list_tags:
        args.append("--list-tags")
    if list_tasks:
        args.append("--list-tasks")
    if verbose > 0:
        args.append("-" + "v" * verbose)
    return _run(args)


def list_playbook_tasks(playbook: str) -> list[str]:
    """Return a list of task names in a playbook."""
    result = run_playbook(playbook, list_tasks=True)
    tasks: list[str] = []
    in_tasks = False
    for line in result.stdout.splitlines():
        if "playbook:" in line:
            in_tasks = False
        if in_tasks and line.strip():
            tasks.append(line.strip())
        if "tasks:" in line:
            in_tasks = True
    return tasks


# --------------------------------------------------------------------------- #
# Ad-Hoc Commands
# --------------------------------------------------------------------------- #

def ad_hoc(
    module: str,
    hosts: str = "all",
    args: Optional[str] = None,
    inventory: Optional[str] = None,
    become: bool = False,
    check: bool = False,
    verbose: int = 0,
) -> subprocess.CompletedProcess:
    """
    Run an Ansible ad-hoc command.

    Args:
        module: Ansible module name (e.g. 'shell', 'ping', 'apt').
        hosts: Host pattern (default: 'all').
        args: Module arguments.
        inventory: Inventory path or host pattern.
        become: Privilege escalation.
        check: Dry-run mode.
        verbose: Verbosity level.

    Returns:
        CompletedProcess with command output.
    """
    args_list = [ANSIBLE_PATH, hosts, "-m", module]
    if args:
        args_list += ["-a", args]
    if inventory or INVENTORY:
        args_list += ["-i", inventory or INVENTORY]
    if become:
        args_list.append("--become")
    if check:
        args_list.append("--check")
    if verbose > 0:
        args_list.append("-" + "v" * verbose)
    return _run(args_list)


def ping(
    hosts: str = "all",
    inventory: Optional[str] = None,
) -> subprocess.CompletedProcess:
    """Ping all hosts via Ansible."""
    return ad_hoc("ping", hosts=hosts, inventory=inventory)


def shell(
    command: str,
    hosts: str = "all",
    inventory: Optional[str] = None,
    become: bool = False,
) -> subprocess.CompletedProcess:
    """Execute a shell command on remote hosts."""
    return ad_hoc(
        "shell",
        hosts=hosts,
        args=command,
        inventory=inventory,
        become=become,
    )


# --------------------------------------------------------------------------- #
# Inventory Helpers
# --------------------------------------------------------------------------- #

def parse_inventory(inventory: Optional[str] = None) -> dict[str, Any]:
    """
    Return structured inventory data using ansible-inventory.

    Args:
        inventory: Path to inventory file or host pattern.

    Returns:
        Inventory dict with groups and hosts.
    """
    inv = inventory or INVENTORY
    if not inv:
        raise ValueError("No inventory specified")
    result = _run(
        [ANSIBLE_PATH, "-i", inv, "--list"]
    )
    return json.loads(result.stdout)


def inventory_graph(inventory: Optional[str] = None) -> str:
    """Return inventory as a dependency graph."""
    inv = inventory or INVENTORY
    if not inv:
        raise ValueError("No inventory specified")
    result = _run([ANSIBLE_PATH, "-i", inv, "--graph"])
    return result.stdout


# --------------------------------------------------------------------------- #
# Galaxy
# --------------------------------------------------------------------------- #

def galaxy_install(
    role: str,
    roles_path: Optional[str] = None,
    version: Optional[str] = None,
    force: bool = False,
) -> subprocess.CompletedProcess:
    """
    Install a role from Ansible Galaxy.

    Args:
        role: Role name or github user/repo path.
        roles_path: Directory to install into.
        version: Specific version to install.
        force: Force reinstall.

    Returns:
        CompletedProcess with galaxy output.
    """
    args = ["ansible-galaxy", "role", "install", role]
    if roles_path:
        args += ["-p", roles_path]
    if version:
        args += ["--version", version]
    if force:
        args.append("--force")
    return _run(args)


def galaxy_init(
    role_name: str,
    init_path: str = ".",
) -> subprocess.CompletedProcess:
    """
    Initialize a new Galaxy role scaffold.

    Returns:
        CompletedProcess.
    """
    return _run(
        ["ansible-galaxy", "role", "init", role_name, "--init-path", init_path]
    )

"""Workflow utilities for RabAI AutoClick.

Provides utilities for workflow serialization, validation,
and manipulation.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .exceptions import WorkflowValidationError


# Current workflow schema version
SCHEMA_VERSION = "2.0"


def create_workflow(
    name: str = "Untitled Workflow",
    description: str = "",
    variables: Optional[Dict[str, Any]] = None,
    steps: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """Create a new workflow dictionary.

    Args:
        name: Workflow name.
        description: Workflow description.
        variables: Initial variables dictionary.
        steps: Initial steps list.

    Returns:
        Workflow dictionary with metadata.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "name": name,
        "description": description,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "variables": variables or {},
        "steps": steps or [],
    }


def add_step(
    workflow: Dict[str, Any],
    step_type: str,
    params: Dict[str, Any],
    step_id: Optional[int] = None,
    next_step_id: Optional[int] = None,
    pre_delay: float = 0.0,
    post_delay: float = 0.0
) -> Dict[str, Any]:
    """Add a step to a workflow.

    Args:
        workflow: Workflow dictionary.
        step_type: Action type (e.g., 'click', 'delay').
        params: Step parameters.
        step_id: Optional step ID (auto-generated if not provided).
        next_step_id: Optional ID of next step to execute.
        pre_delay: Delay before step execution (seconds).
        post_delay: Delay after step execution (seconds).

    Returns:
        The workflow dictionary.
    """
    if "steps" not in workflow:
        workflow["steps"] = []

    if step_id is None:
        step_id = len(workflow["steps"]) + 1

    step = {
        "id": step_id,
        "type": step_type,
        **params,
    }

    if next_step_id is not None:
        step["next"] = next_step_id
    if pre_delay > 0:
        step["pre_delay"] = pre_delay
    if post_delay > 0:
        step["post_delay"] = post_delay

    workflow["steps"].append(step)
    workflow["updated_at"] = datetime.now().isoformat()

    return workflow


def validate_workflow(workflow: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate a workflow dictionary.

    Args:
        workflow: Workflow dictionary to validate.

    Returns:
        Tuple of (is_valid, error_message).
    """
    # Check schema version
    schema_version = workflow.get("schema_version", "1.0")

    # Check required fields
    if "steps" not in workflow:
        return False, "Workflow must have a 'steps' field"

    steps = workflow["steps"]
    if not isinstance(steps, list):
        return False, "Workflow 'steps' must be a list"

    # Check each step
    step_ids = set()
    for i, step in enumerate(steps):
        if not isinstance(step, dict):
            return False, f"Step {i} must be a dictionary"

        # Check required fields
        if "type" not in step:
            return False, f"Step {i} is missing 'type' field"

        if "id" not in step:
            return False, f"Step {i} is missing 'id' field"

        step_id = step["id"]
        if step_id in step_ids:
            return False, f"Duplicate step ID: {step_id}"
        step_ids.add(step_id)

    return True, ""


def get_step_by_id(
    workflow: Dict[str, Any],
    step_id: int
) -> Optional[Dict[str, Any]]:
    """Get a step by its ID.

    Args:
        workflow: Workflow dictionary.
        step_id: Step ID to find.

    Returns:
        Step dictionary or None if not found.
    """
    for step in workflow.get("steps", []):
        if step.get("id") == step_id:
            return step
    return None


def get_step_index(
    workflow: Dict[str, Any],
    step_id: int
) -> int:
    """Get the index of a step by its ID.

    Args:
        workflow: Workflow dictionary.
        step_id: Step ID to find.

    Returns:
        Step index or -1 if not found.
    """
    for i, step in enumerate(workflow.get("steps", [])):
        if step.get("id") == step_id:
            return i
    return -1


def remove_step(
    workflow: Dict[str, Any],
    step_id: int
) -> bool:
    """Remove a step from the workflow.

    Args:
        workflow: Workflow dictionary.
        step_id: Step ID to remove.

    Returns:
        True if step was removed, False if not found.
    """
    index = get_step_index(workflow, step_id)
    if index >= 0:
        workflow["steps"].pop(index)
        workflow["updated_at"] = datetime.now().isoformat()
        return True
    return False


def insert_step(
    workflow: Dict[str, Any],
    step: Dict[str, Any],
    position: int
) -> Dict[str, Any]:
    """Insert a step at a specific position.

    Args:
        workflow: Workflow dictionary.
        step: Step dictionary to insert.
        position: Position to insert at (0-based).

    Returns:
        The workflow dictionary.
    """
    if "steps" not in workflow:
        workflow["steps"] = []

    workflow["steps"].insert(position, step)
    workflow["updated_at"] = datetime.now().isoformat()

    # Reindex step IDs
    for i, s in enumerate(workflow["steps"]):
        s["id"] = i + 1

    return workflow


def clone_step(
    workflow: Dict[str, Any],
    step_id: int,
    new_step_id: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """Clone a step and add it to the workflow.

    Args:
        workflow: Workflow dictionary.
        step_id: Step ID to clone.
        new_step_id: Optional ID for the new step.

    Returns:
        The cloned step or None if original not found.
    """
    step = get_step_by_id(workflow, step_id)
    if step is None:
        return None

    cloned = step.copy()
    if new_step_id is None:
        new_step_id = len(workflow["steps"]) + 1
    cloned["id"] = new_step_id

    workflow["steps"].append(cloned)
    workflow["updated_at"] = datetime.now().isoformat()

    return cloned


def export_workflow(
    workflow: Dict[str, Any],
    filepath: str,
    pretty: bool = True
) -> bool:
    """Export workflow to a JSON file.

    Args:
        workflow: Workflow dictionary.
        filepath: Path to save the file.
        pretty: Whether to use pretty printing.

    Returns:
        True if exported successfully.
    """
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            if pretty:
                json.dump(workflow, f, ensure_ascii=False, indent=2)
            else:
                json.dump(workflow, f, ensure_ascii=False)
        return True
    except Exception:
        return False


def import_workflow(filepath: str) -> Optional[Dict[str, Any]]:
    """Import workflow from a JSON file.

    Args:
        filepath: Path to the workflow file.

    Returns:
        Workflow dictionary or None if import failed.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def get_workflow_summary(workflow: Dict[str, Any]) -> Dict[str, Any]:
    """Get a summary of the workflow.

    Args:
        workflow: Workflow dictionary.

    Returns:
        Summary dictionary with counts and info.
    """
    steps = workflow.get("steps", [])

    # Count step types
    type_counts: Dict[str, int] = {}
    for step in steps:
        step_type = step.get("type", "unknown")
        type_counts[step_type] = type_counts.get(step_type, 0) + 1

    return {
        "name": workflow.get("name", "Untitled"),
        "schema_version": workflow.get("schema_version", "unknown"),
        "total_steps": len(steps),
        "step_types": type_counts,
        "variables": list(workflow.get("variables", {}).keys()),
        "created_at": workflow.get("created_at"),
        "updated_at": workflow.get("updated_at"),
    }


def merge_workflows(
    workflows: List[Dict[str, Any]],
    start_ids_from: int = 1
) -> Dict[str, Any]:
    """Merge multiple workflows into one.

    Args:
        workflows: List of workflow dictionaries.
        start_ids_from: Starting step ID for merged workflow.

    Returns:
        Merged workflow dictionary.
    """
    if not workflows:
        return create_workflow()

    merged = create_workflow(
        name="Merged Workflow",
        description=f"Merged from {len(workflows)} workflows"
    )

    current_id = start_ids_from
    for workflow in workflows:
        for step in workflow.get("steps", []):
            new_step = step.copy()
            new_step["id"] = current_id
            current_id += 1
            merged["steps"].append(new_step)

    merged["updated_at"] = datetime.now().isoformat()
    return merged
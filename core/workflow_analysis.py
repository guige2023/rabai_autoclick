"""Workflow analysis tools for RabAI AutoClick.

Provides tools for analyzing, debugging, and optimizing workflows.
"""

from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass

from .workflow_utils import get_step_by_id, SCHEMA_VERSION


@dataclass
class StepAnalysis:
    """Analysis results for a single step."""
    step_id: int
    step_type: str
    params: Dict[str, Any]
    issues: List[str]
    suggestions: List[str]


@dataclass
class WorkflowAnalysis:
    """Complete workflow analysis results."""
    workflow_name: str
    total_steps: int
    step_types: Dict[str, int]
    estimated_duration: float
    step_analyses: List[StepAnalysis]
    issues: List[str]
    suggestions: List[str]
    complexity_score: float


def analyze_step(step: Dict[str, Any]) -> StepAnalysis:
    """Analyze a single step for issues and suggestions.

    Args:
        step: Step dictionary.

    Returns:
        StepAnalysis with issues and suggestions.
    """
    step_id = step.get('id', -1)
    step_type = step.get('type', 'unknown')
    issues: List[str] = []
    suggestions: List[str] = []

    # Check for missing pre/post delays (could cause instability)
    if step.get('pre_delay', 0) == 0 and step_type in ['click', 'key_press', 'type_text']:
        suggestions.append(f"Step {step_id}: Consider adding pre_delay for stability")

    if step.get('post_delay', 0) == 0 and step_type in ['click', 'key_press']:
        suggestions.append(f"Step {step_id}: Consider adding post_delay after actions")

    # Check for very short delays
    if step_type == 'wait' or step_type == 'delay':
        delay = step.get('seconds', step.get('duration', 0))
        if isinstance(delay, (int, float)) and delay < 0.1:
            issues.append(f"Step {step_id}: Very short delay ({delay}s) may cause instability")

    # Check for missing coordinates validation
    if step_type in ['click', 'mouse_click', 'mouse_move']:
        x = step.get('x')
        y = step.get('y')
        if x is None or y is None:
            suggestions.append(f"Step {step_id}: Consider adding coordinate validation")

    # Check for hardcoded values instead of variables
    if step_type == 'click':
        x, y = step.get('x'), step.get('y')
        if x is not None and y is not None:
            if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                if x < 0 or y < 0 or x > 5000 or y > 5000:
                    issues.append(f"Step {step_id}: Coordinates seem unusual ({x}, {y})")

    return StepAnalysis(
        step_id=step_id,
        step_type=step_type,
        params={k: v for k, v in step.items() if k not in ('id', 'type')},
        issues=issues,
        suggestions=suggestions
    )


def analyze_workflow(workflow: Dict[str, Any]) -> WorkflowAnalysis:
    """Analyze a complete workflow for issues.

    Args:
        workflow: Workflow dictionary.

    Returns:
        WorkflowAnalysis with comprehensive results.
    """
    steps = workflow.get('steps', [])
    issues: List[str] = []
    suggestions: List[str] = []

    # Analyze each step
    step_analyses = [analyze_step(step) for step in steps]

    # Collect issues and suggestions
    for analysis in step_analyses:
        issues.extend(analysis.issues)
        suggestions.extend(analysis.suggestions)

    # Count step types
    step_types: Dict[str, int] = {}
    for step in steps:
        step_type = step.get('type', 'unknown')
        step_types[step_type] = step_types.get(step_type, 0) + 1

    # Estimate duration
    estimated_duration = estimate_workflow_duration(workflow)

    # Calculate complexity score
    complexity_score = calculate_complexity_score(workflow)

    # Check for unreachable steps
    unreachable = find_unreachable_steps(workflow)
    if unreachable:
        issues.append(f"Unreachable steps found: {unreachable}")

    # Check for circular references
    circular = find_circular_references(workflow)
    if circular:
        issues.append(f"Circular references found: {circular}")

    return WorkflowAnalysis(
        workflow_name=workflow.get('name', 'Untitled'),
        total_steps=len(steps),
        step_types=step_types,
        estimated_duration=estimated_duration,
        step_analyses=step_analyses,
        issues=issues,
        suggestions=suggestions,
        complexity_score=complexity_score
    )


def estimate_workflow_duration(workflow: Dict[str, Any]) -> float:
    """Estimate total workflow duration.

    Args:
        workflow: Workflow dictionary.

    Returns:
        Estimated duration in seconds.
    """
    total = 0.0

    for step in workflow.get('steps', []):
        step_type = step.get('type', '')

        # Add explicit delays
        if 'pre_delay' in step:
            total += step['pre_delay']
        if 'post_delay' in step:
            total += step['post_delay']

        # Add step-specific duration estimates
        if step_type in ('wait', 'delay'):
            total += step.get('seconds', step.get('duration', 1.0))
        elif step_type == 'loop':
            count = step.get('count', 1)
            total += estimate_loop_duration(step, count)
        elif step_type == 'while_loop':
            max_iterations = step.get('max_iterations', 100)
            total += estimate_loop_duration(step, max_iterations)

    return total


def estimate_loop_duration(step: Dict[str, Any], iterations: int) -> float:
    """Estimate duration of a loop.

    Args:
        step: Loop step dictionary.
        iterations: Number of iterations.

    Returns:
        Estimated duration in seconds.
    """
    # This is a simplified estimation
    # In a real implementation, we'd need to find the loop body
    return iterations * 1.0  # Assume 1 second per iteration


def calculate_complexity_score(workflow: Dict[str, Any]) -> float:
    """Calculate workflow complexity score.

    Args:
        workflow: Workflow dictionary.

    Returns:
        Complexity score (0-100).
    """
    score = 0.0
    steps = workflow.get('steps', [])

    # Base score from step count
    score += len(steps) * 2

    # Increase for control flow
    for step in steps:
        step_type = step.get('type', '')
        if step_type in ('loop', 'while_loop', 'condition'):
            score += 10
        if step_type == 'condition':
            score += 5

    # Increase for nested structures
    # (simplified - would need actual nesting detection)
    if len(steps) > 20:
        score += 20
    if len(steps) > 50:
        score += 30

    # Cap at 100
    return min(100.0, score)


def find_unreachable_steps(workflow: Dict[str, Any]) -> List[int]:
    """Find steps that cannot be reached from the start.

    Args:
        workflow: Workflow dictionary.

    Returns:
        List of unreachable step IDs.
    """
    steps = workflow.get('steps', [])
    if not steps:
        return []

    # Build reachability map
    reachable: Set[int] = {steps[0].get('id', 1)}  # Start with first step
    changed = True

    while changed:
        changed = False
        for step in steps:
            step_id = step.get('id')
            if step_id not in reachable:
                # Check if this step is referenced
                for s in steps:
                    if s.get('next') == step_id or s.get('true_next') == step_id or s.get('false_next') == step_id:
                        reachable.add(step_id)
                        changed = True
                        break

    # Find unreachable
    all_ids = {step.get('id') for step in steps}
    unreachable = list(all_ids - reachable)
    return sorted(unreachable)


def find_circular_references(workflow: Dict[str, Any]) -> List[Tuple[int, int]]:
    """Find circular step references.

    Args:
        workflow: Workflow dictionary.

    Returns:
        List of (from_id, to_id) tuples that form cycles.
    """
    steps = workflow.get('steps', [])
    if not steps:
        return []

    # Build adjacency map
    adj: Dict[int, List[int]] = {}
    for step in steps:
        step_id = step.get('id')
        if step_id is not None:
            adj[step_id] = []

            # Direct next
            if 'next' in step:
                adj[step_id].append(step['next'])

            # Conditional nexts
            if 'true_next' in step and step['true_next']:
                adj[step_id].append(step['true_next'])
            if 'false_next' in step and step['false_next']:
                adj[step_id].append(step['false_next'])

    # DFS to find cycles
    cycles: List[Tuple[int, int]] = []
    visited: Set[int] = set()
    rec_stack: Set[int] = set()

    def dfs(node: int, parent: Optional[int] = None) -> None:
        visited.add(node)
        rec_stack.add(node)

        for neighbor in adj.get(node, []):
            if neighbor not in visited:
                dfs(neighbor, node)
            elif neighbor in rec_stack and parent is not None:
                cycles.append((parent, neighbor))

        rec_stack.remove(node)

    for step in steps:
        step_id = step.get('id')
        if step_id and step_id not in visited:
            dfs(step_id)

    return cycles


def optimize_workflow(workflow: Dict[str, Any]) -> Dict[str, Any]:
    """Optimize a workflow by removing redundancies.

    Args:
        workflow: Workflow dictionary.

    Returns:
        Optimized workflow dictionary.
    """
    optimized = workflow.copy()
    steps = optimized.get('steps', [])

    # Remove consecutive delays
    i = 0
    while i < len(steps) - 1:
        current = steps[i]
        next_step = steps[i + 1]

        if current.get('type') == 'delay' and next_step.get('type') == 'delay':
            # Merge delays
            current_seconds = current.get('seconds', current.get('duration', 0))
            next_seconds = next_step.get('seconds', next_step.get('duration', 0))
            current['seconds'] = current_seconds + next_seconds
            steps.pop(i + 1)
        else:
            i += 1

    # Remove empty pre/post delays
    for step in steps:
        if step.get('pre_delay', 0) == 0:
            step.pop('pre_delay', None)
        if step.get('post_delay', 0) == 0:
            step.pop('post_delay', None)

    optimized['updated_at'] = __import__('datetime').datetime.now().isoformat()
    return optimized
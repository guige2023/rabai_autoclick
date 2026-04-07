"""Workflow templates for RabAI AutoClick.

Provides pre-built workflow templates for common automation tasks.
"""

from typing import Any, Dict, List, Optional

from core.workflow_utils import create_workflow, add_step, SCHEMA_VERSION


# Template definitions
TEMPLATES: Dict[str, Dict[str, Any]] = {}


def register_template(
    name: str,
    description: str,
    category: str,
    variables: Dict[str, Any],
    steps: List[Dict[str, Any]]
) -> None:
    """Register a workflow template.

    Args:
        name: Template name.
        description: Template description.
        category: Template category.
        variables: Initial variables.
        steps: Template steps.
    """
    TEMPLATES[name] = {
        'name': name,
        'description': description,
        'category': category,
        'schema_version': SCHEMA_VERSION,
        'variables': variables,
        'steps': steps,
    }


def get_template(name: str) -> Optional[Dict[str, Any]]:
    """Get a template by name.

    Args:
        name: Template name.

    Returns:
        Template dictionary or None if not found.
    """
    return TEMPLATES.get(name)


def get_templates_by_category(category: str) -> List[Dict[str, Any]]:
    """Get all templates in a category.

    Args:
        category: Category name.

    Returns:
        List of template dictionaries.
    """
    return [
        t for t in TEMPLATES.values()
        if t.get('category') == category
    ]


def get_all_templates() -> List[Dict[str, Any]]:
    """Get all available templates.

    Returns:
        List of template dictionaries.
    """
    return list(TEMPLATES.values())


def get_template_categories() -> List[str]:
    """Get all template categories.

    Returns:
        List of category names.
    """
    categories = set(t.get('category') for t in TEMPLATES.values())
    return sorted(categories)


# Register built-in templates

# Basic click workflow
register_template(
    name="basic_click",
    description="Basic workflow that clicks at specified coordinates",
    category="basic",
    variables={'x': 100, 'y': 100, 'button': 'left'},
    steps=[
        {'id': 1, 'type': 'wait', 'seconds': 0.5, 'pre_delay': 0.5},
        {'id': 2, 'type': 'click', 'x': '{{x}}', 'y': '{{y}}', 'button': '{{button}}', 'post_delay': 0.2},
    ]
)

# Type text workflow
register_template(
    name="type_text",
    description="Type specified text at current cursor position",
    category="basic",
    variables={'text': 'Hello World', 'interval': 0.05},
    steps=[
        {'id': 1, 'type': 'wait', 'seconds': 0.5, 'pre_delay': 0.5},
        {'id': 2, 'type': 'type_text', 'text': '{{text}}', 'interval': '{{interval}}', 'post_delay': 0.2},
    ]
)

# Key press workflow
register_template(
    name="key_press",
    description="Press a key or key combination",
    category="basic",
    variables={'key': 'enter'},
    steps=[
        {'id': 1, 'type': 'wait', 'seconds': 0.5, 'pre_delay': 0.5},
        {'id': 2, 'type': 'key_press', 'key': '{{key}}', 'post_delay': 0.2},
    ]
)

# Image click workflow
register_template(
    name="image_click",
    description="Find an image and click at its location",
    category="image",
    variables={'template': 'image.png', 'confidence': 0.8, 'offset_x': 0, 'offset_y': 0},
    steps=[
        {'id': 1, 'type': 'wait', 'seconds': 0.5, 'pre_delay': 0.5},
        {'id': 2, 'type': 'click_image', 'template': '{{template}}', 'confidence': '{{confidence}}', 'offset_x': '{{offset_x}}', 'offset_y': '{{offset_y}}', 'post_delay': 0.2},
    ]
)

# Wait for image workflow
register_template(
    name="wait_for_image",
    description="Wait for an image to appear on screen",
    category="wait",
    variables={'template': 'image.png', 'timeout': 30, 'confidence': 0.8},
    steps=[
        {'id': 1, 'type': 'wait_for_image', 'template': '{{template}}', 'timeout': '{{timeout}}', 'confidence': '{{confidence}}'},
    ]
)

# Screenshot workflow
register_template(
    name="take_screenshot",
    description="Take a screenshot and save to file",
    category="utility",
    variables={'path': 'screenshot.png', 'region': None},
    steps=[
        {'id': 1, 'type': 'screenshot', 'path': '{{path}}', 'region': '{{region}}'},
    ]
)

# Loop workflow
register_template(
    name="repeat_action",
    description="Repeat an action multiple times",
    category="flow",
    variables={'count': 5, 'action_type': 'click', 'action_params': {'x': 100, 'y': 100}},
    steps=[
        {'id': 1, 'type': 'loop', 'count': '{{count}}', 'loop_var': '_loop_count'},
        {'id': 2, 'type': '{{action_type}}', ** '{{action_params}}'},
    ]
)

# Conditional workflow
register_template(
    name="conditional_action",
    description="Perform action based on condition",
    category="flow",
    variables={'condition': 'x > 100', 'true_action': 'click', 'false_action': 'wait'},
    steps=[
        {'id': 1, 'type': 'condition', 'condition': '{{condition}}', 'true_next': 2, 'false_next': 3},
        {'id': 2, 'type': '{{true_action}}'},
        {'id': 3, 'type': '{{false_action}}'},
    ]
)

# Clipboard workflow
register_template(
    name="copy_paste",
    description="Copy text to clipboard and paste",
    category="basic",
    variables={'text': 'Copied text'},
    steps=[
        {'id': 1, 'type': 'clipboard_copy', 'text': '{{text}}'},
        {'id': 2, 'type': 'wait', 'seconds': 0.1},
        {'id': 3, 'type': 'clipboard_paste'},
    ]
)

# OCR workflow
register_template(
    name="ocr_click",
    description="Find text on screen and click it",
    category="ocr",
    variables={'text': 'Click here', 'confidence': 0.6},
    steps=[
        {'id': 1, 'type': 'wait', 'seconds': 0.5, 'pre_delay': 0.5},
        {'id': 2, 'type': 'ocr', 'click_text': '{{text}}', 'exact_match': False, 'post_delay': 0.2},
    ]
)

# Application launch workflow
register_template(
    name="launch_and_work",
    description="Launch an app, wait, then perform actions",
    category="application",
    variables={'app_name': 'Safari'},
    steps=[
        {'id': 1, 'type': 'launch_app', 'app_name': '{{app_name}}'},
        {'id': 2, 'type': 'wait', 'seconds': 2},
    ]
)

# Notification workflow
register_template(
    name="notify_completion",
    description="Send notification when workflow completes",
    category="notification",
    variables={'title': 'RabAI', 'message': 'Workflow completed'},
    steps=[
        {'id': 1, 'type': 'notify', 'title': '{{title}}', 'message': '{{message}}'},
    ]
)

# System sound workflow
register_template(
    name="sound_alert",
    description="Play a sound when workflow completes",
    category="notification",
    variables={'sound': 'glass'},
    steps=[
        {'id': 1, 'type': 'system_sound', 'sound_name': '{{sound}}'},
    ]
)


def create_from_template(name: str, **overrides: Any) -> Optional[Dict[str, Any]]:
    """Create a workflow from a template with optional overrides.

    Args:
        name: Template name.
        **overrides: Variable values to override.

    Returns:
        New workflow dictionary or None if template not found.
    """
    template = get_template(name)
    if template is None:
        return None

    workflow = create_workflow(
        name=template.get('name', 'Untitled'),
        description=template.get('description', ''),
        variables={**template.get('variables', {}), **overrides},
        steps=template.get('steps', [])
    )

    return workflow
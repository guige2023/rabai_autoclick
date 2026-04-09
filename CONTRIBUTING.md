# Contributing to RabAI AutoClick

Thank you for your interest in contributing to RabAI AutoClick!

## Development Workflow

### 1. Fork and Clone

```bash
# Fork the repository on GitHub
# Then clone your fork
git clone https://github.com/YOUR_USERNAME/rabai_autoclick.git
cd rabai_autoclick

# Add upstream remote
git remote add upstream https://github.com/guige2023/rabai_autoclick.git
```

### 2. Create a Branch

```bash
# Create a feature branch
git checkout -b feature/your-feature-name

# Or a bugfix branch
git checkout -b fix/your-bugfix-name
```

### 3. Development Setup

```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .\.venv\\Scripts\\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Install development dependencies
pip install pytest pytest-cov black ruff pytest-asyncio

# Verify installation
python main.py
```

### 4. Make Changes

```bash
# Make your changes
# ...

# Run tests to ensure nothing is broken
pytest tests/ -v

# Format code
black .

# Lint code
ruff check .
```

### 5. Commit Changes

We follow [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Formatting, missing semicolons, etc.
- `refactor`: Code refactoring
- `test`: Adding tests
- `chore`: Maintenance

Examples:
```bash
git commit -m "feat(actions): add new OCR action type"
git commit -m "fix(core): resolve engine deadlock issue"
git commit -m "docs: update README with new features"
```

### 6. Push and Create PR

```bash
# Push your branch
git push origin feature/your-feature-name

# Create Pull Request on GitHub
# Fill in the PR template
```

---

## Code Style Guidelines

### Python Code

- Follow **PEP 8** style guide
- Use **type hints** for all function signatures
- Add **docstrings** to all public methods and classes
- Maximum line length: **100 characters** (enforced by black)

### Type Hints Example

```python
from typing import Any, Dict, List, Optional

def execute_action(action_id: str, params: Dict[str, Any]) -> Optional[ActionResult]:
    """
    Execute an action with the given parameters.

    Args:
        action_id: Unique identifier for the action type
        params: Dictionary of action parameters

    Returns:
        ActionResult on success, None if action not found

    Raises:
        ActionError: If action execution fails
    """
    pass
```

### File Organization

- 4 spaces for indentation (no tabs)
- One class per file for major classes
- Related functions can be grouped in modules
- Order: imports, constants, classes, functions

### Import Order

```python
# Standard library
import os
import sys
from typing import Any, Dict, List

# Third-party
import pytest
from PyQt5.QtWidgets import QWidget

# Local application
from core.base_action import BaseAction
from utils.helpers import format_coord
```

---

## Testing Requirements

### Test Coverage

- All new features must include tests
- Bug fixes must include a regression test
- Minimum coverage: **80%**

### Test Structure

```
tests/
├── test_core/
│   ├── test_engine.py
│   ├── test_context.py
│   └── test_action_loader.py
├── test_actions/
│   ├── test_click.py
│   ├── test_keyboard.py
│   └── test_ocr.py
├── test_gui/
│   └── test_main_window.py
└── conftest.py
```

### Writing Tests

```python
import pytest
from core.engine import Engine

class TestEngine:
    """Tests for the execution engine."""

    def test_execute_simple_action(self, engine, sample_action):
        """Test executing a simple click action."""
        result = engine.execute(sample_action)
        assert result.success is True

    def test_execute_with_params(self, engine):
        """Test executing action with parameters."""
        action = {
            'type': 'click',
            'params': {'x': 100, 'y': 200}
        }
        result = engine.execute(action)
        assert result.success is True

    @pytest.fixture
    def engine(self):
        """Provide engine instance for tests."""
        return Engine()

    @pytest.fixture
    def sample_action(self):
        """Provide sample action for tests."""
        return {'type': 'click', 'params': {}}
```

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html --cov-report=term

# Run specific test file
pytest tests/test_core/test_engine.py -v

# Run tests matching pattern
pytest tests/ -k "test_execute" -v

# Run in parallel (requires pytest-xdist)
pytest tests/ -n auto
```

---

## Pull Request Process

### Before Submitting

1. **Run all tests** - Ensure `pytest tests/ -v` passes
2. **Run linters** - Ensure `ruff check .` passes
3. **Format code** - Ensure `black .` makes no changes
4. **Update documentation** - Update relevant docs if needed
5. **Add tests** - Ensure new code has test coverage

### PR Template

```markdown
## Description
Brief description of the changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation update
- [ ] Code refactoring

## Testing
Describe testing done to validate changes

## Checklist
- [ ] My code follows the style guidelines
- [ ] I have performed a self-review
- [ ] I have commented my code where necessary
- [ ] I have updated the documentation
- [ ] My changes generate no new warnings
- [ ] I have added tests that prove my fix/feature works
- [ ] All new and existing tests pass
```

### Review Process

1. Maintainers will review within 48 hours
2. Address any feedback
3. Once approved, maintainers will merge

---

## Adding New Actions

### Step 1: Create Action File

Create `actions/my_action.py`:

```python
from typing import Any, Dict, List
from core.base_action import BaseAction, ActionResult

class MyAction(BaseAction):
    action_type = "my_action"
    display_name = "我的动作"
    description = "动作描述"
    category = "custom"  # custom, mouse, keyboard, ocr, etc.

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute the action."""
        # Your implementation
        return ActionResult(success=True, message="Success")

    def get_required_params(self) -> List[str]:
        return ['param1', 'param2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'optional_param': 'default_value'}

    def validate_params(self, params: Dict[str, Any]) -> bool:
        """Validate action parameters."""
        return True
```

### Step 2: Register Action

Update `actions/__init__.py` to include your action.

### Step 3: Add Tests

Create `tests/test_actions/test_my_action.py`.

### Step 4: Update Documentation

- Add to README.md action reference
- Add to ARCHITECTURE.md if architecture changed

---

## Issue Guidelines

### Bug Reports

Include:
- RabAI AutoClick version
- Operating system
- Python version
- Steps to reproduce
- Expected vs actual behavior
- Error messages/logs

### Feature Requests

Include:
- Problem/need addressed
- Proposed solution
- Alternative solutions considered
- Any mockups/examples

---

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

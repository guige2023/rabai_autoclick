# Development Guide

Guide for developers contributing to RabAI AutoClick.

## 🚀 Development Setup

### Prerequisites

- Python 3.8+
- Git
- pip

### Setup Steps

1. **Clone the repository:**
```bash
git clone https://github.com/guige2023/rabai_autoclick.git
cd rabai_autoclick
```

2. **Create virtual environment:**
```bash
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Install development dependencies:**
```bash
pip install pytest pytest-cov black ruff pyright
```

5. **Verify setup:**
```bash
python main.py
```

## 🧪 Testing

### Run All Tests

```bash
pytest tests/ -v
```

### Run Tests with Coverage

```bash
pytest tests/ -v --cov=. --cov-report=html --cov-report=term-missing
```

### Run Specific Test File

```bash
pytest tests/test_core.py -v
```

### Run Tests Matching Pattern

```bash
pytest tests/ -k "test_click" -v
```

## 📐 Code Formatting

### Format All Code

```bash
make format
# or
black .
ruff check --fix .
```

### Check Formatting

```bash
black --check .
ruff check .
```

## 🔍 Type Checking

```bash
pyright
# or
make typecheck
```

## 🏗️ Project Structure

```
rabai_autoclick/
├── actions/          # Action implementations
│   ├── __init__.py
│   ├── click.py
│   ├── keyboard.py
│   ├── mouse.py
│   ├── ocr.py
│   ├── image_match.py
│   ├── script.py
│   └── system.py
├── cli/              # CLI interface
├── core/             # Core engine
│   ├── engine.py      # FlowEngine
│   ├── context.py    # ContextManager
│   ├── action_loader.py
│   └── base_action.py
├── gui/              # PyQt5 GUI
├── src/              # Advanced features
├── tests/            # Test suite
├── ui/               # UI components
├── utils/            # Utilities
├── workflows/        # Example workflows
└── docs/             # Documentation
```

## 🎯 Adding New Actions

1. Create file in `actions/`:
```python
# actions/my_action.py
from typing import Any, Dict, List
from core.base_action import BaseAction, ActionResult

class MyAction(BaseAction):
    action_type = "my_action"
    display_name = "My Action"
    description = "Description here"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        # Implementation
        return ActionResult(success=True, message="Done")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
```

2. Export in `actions/__init__.py`:
```python
from .my_action import MyAction
__all__ = [..., 'MyAction']
```

## 🔧 Debugging

### Enable Debug Logging

Set environment variable:
```bash
export RABAI_LOG_LEVEL=DEBUG
python main.py
```

### View Logs

Check `./logs/` directory:
```bash
tail -f logs/app.log
```

### Using Python Debugger

```python
import pdb; pdb.set_trace()
# or
breakpoint()
```

## 📝 Code Standards

### Style Guide

- Follow PEP 8
- Line length: 100 characters (enforced by black)
- Use type hints for function signatures
- Docstrings for public methods

### Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Variables | snake_case | `my_variable` |
| Functions | snake_case | `def my_function` |
| Classes | PascalCase | `MyClass` |
| Constants | UPPER_SNAKE | `MAX_COUNT` |
| Files | snake_case | `my_file.py` |

### Commit Messages

Format:
```
<type>(<scope>): <description>

[optional body]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `chore`: Maintenance
- `refactor`: Refactoring
- `test`: Tests

Example:
```
feat(actions): add screenshot action

Add new screenshot action with region support
- Full screen capture
- Region-specific capture
- Save to configurable path
```

## 🧩 Working with Git

### Branch Naming

```
feature/my-feature
bugfix/issue-123
docs/update-readme
```

### Pull Request Process

1. Create feature branch
2. Make changes
3. Add tests
4. Run linting/type checking
5. Submit PR
6. Address review feedback

## 🏃 Performance Tips

1. Use `pre_delay` and `post_delay` appropriately
2. Avoid tight loops without delays
3. Use image matching only when needed
4. Minimize OCR usage
5. Clear logs periodically

## 📦 Release Process

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create git tag
4. Build application
5. Create GitHub release

## 🆘 Getting Help

- Open an [Issue](https://github.com/guige2023/rabai_autoclick/issues)
- Check [Discussions](https://github.com/guige2023/rabai_autoclick/discussions)
- Read existing code for patterns

# Contributing to RabAI AutoClick

Thank you for your interest in contributing to RabAI AutoClick!

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/rabai_autoclick.git
   cd rabai_autoclick
   ```

3. Create a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # macOS/Linux
   ```

4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   pip install black ruff pytest pytest-cov
   ```

## Development Workflow

### Code Style

- Follow PEP 8 guidelines
- Use type hints for all function signatures
- Add docstrings to all public methods
- Maximum line length: 100 characters

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html

# Run specific test file
pytest tests/test_core.py -v
```

### Code Formatting

```bash
# Format code with black
black .

# Check code with ruff
ruff check .
```

## Project Structure

```
rabai_autoclick/
├── actions/          # Action implementations
│   ├── click.py     # Mouse click actions
│   ├── keyboard.py  # Keyboard actions
│   ├── mouse.py     # Mouse movement actions
│   ├── ocr.py       # OCR actions
│   └── ...
├── cli/              # CLI interface
├── core/             # Core engine
│   ├── engine.py    # Flow engine
│   ├── context.py   # Context manager
│   └── ...
├── src/              # Advanced features
│   ├── predictive_engine.py
│   ├── self_healing_system.py
│   └── ...
├── ui/               # UI components
└── utils/            # Utility modules
```

## Adding New Actions

1. Create a new file in `actions/` (e.g., `my_action.py`)
2. Inherit from `BaseAction`:
   ```python
   from core.base_action import BaseAction, ActionResult
   from typing import Any, Dict, List

   class MyAction(BaseAction):
       action_type = "my_action"
       display_name = "我的动作"
       description = "描述..."
       
       def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
           # Your implementation
           return ActionResult(success=True, message="Done")
       
       def get_required_params(self) -> List[str]:
           return ['param1']
       
       def get_optional_params(self) -> Dict[str, Any]:
           return {'param2': 'default'}
   ```

3. Export in `actions/__init__.py`

## Commit Messages

Use clear, descriptive commit messages:
- `feat: Add new action for XYZ`
- `fix: Resolve issue with ABC`
- `refactor: Improve XYZ module`
- `docs: Update README for ABC`

## Pull Request Process

1. Create a new branch for your feature:
   ```bash
   git checkout -b feature/my-feature
   ```

2. Make your changes and commit

3. Push to your fork:
   ```bash
   git push origin feature/my-feature
   ```

4. Open a Pull Request on GitHub

## Reporting Issues

When reporting issues, please include:
- Your operating system and version
- Python version
- Steps to reproduce the issue
- Expected vs actual behavior
- Any relevant error messages or logs

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

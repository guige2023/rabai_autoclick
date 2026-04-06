# Tests Directory

This directory contains the test suite for RabAI AutoClick.

## Test Files

| File | Description |
|------|-------------|
| `test_core.py` | pytest tests for core modules |
| `test_modules.py` | pytest tests for utils and actions |
| `test_v22.py` | pytest tests for v22 features |
| `test_full.py` | comprehensive test script |
| `debug_loader.py` | debug script for action loading |
| `debug_engine.py` | debug script for engine |

## Running Tests

### Using pytest (Recommended)

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_core.py -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html

# Run specific test class
pytest tests/test_core.py::TestContextManager -v
```

### Using test_full.py

```bash
# Run as script
python tests/test_full.py

# Or with pytest
pytest tests/test_full.py -v
```

## Test Structure

### test_core.py

Core module tests using pytest:

```python
class TestContextManager:
    def test_basic_operations(self):
        ctx = ContextManager()
        ctx.set('key', 'value')
        assert ctx.get('key') == 'value'
```

### test_modules.py

Comprehensive tests for utils, actions, and UI:

```python
@test("Module Name - Test Description")
def test_function():
    # Test implementation
    assert result
```

### test_v22.py

v22 feature tests:

```python
class TestWorkflowShare:
    def test_create_share_link(self):
        # Test workflow sharing
```

## Debug Scripts

### debug_loader.py

Check if actions are loading correctly:

```bash
python tests/debug_loader.py
```

Output:
```
Actions directory: ./actions
Files found:

--- Loading click.py ---
  Found action: ClickAction (type: click)
--- Loading keyboard.py ---
  Found action: TypeAction (type: type_text)
```

### debug_engine.py

Test workflow execution:

```bash
python tests/debug_engine.py
```

Output:
```
Actions loaded: ['click', 'delay', ...]
Before run: {}
Run result: True
After run: {'test_var': 'hello'}
test_var: hello
```

## Writing Tests

### pytest Style

```python
import pytest
from core.context import ContextManager

class TestMyFeature:
    def test_something(self):
        ctx = ContextManager()
        # test code
        assert expected == actual
    
    def test_error_case(self):
        with pytest.raises(ValueError):
            some_function(invalid_input)
```

### Decorator Style (test_modules.py)

```python
from typing import List

test_results: Dict[str, List[Any]] = {
    'passed': [],
    'failed': []
}

def test(name: str = "") -> Callable:
    def decorator(func: Callable) -> Callable:
        def wrapper():
            try:
                func()
                test_results['passed'].append(name)
                print(f"[PASS] {name}")
            except AssertionError as e:
                test_results['failed'].append({'name': name, 'error': str(e)})
                print(f"[FAIL] {name}: {e}")
        return wrapper
    return decorator

@test("My Test")
def test_my_feature():
    assert True
```

## Test Data

Test data files are stored in:
- `tests/data/` - JSON test fixtures
- `tests/fixtures/` - Image and other test assets

## CI/CD

Tests run automatically on:
- Every pull request
- Every push to main branch
- Daily at midnight

## Coverage Goals

- Core modules: 90%+ coverage
- Actions: 80%+ coverage
- Utils: 70%+ coverage
- UI: 50%+ coverage (harder to test)

## Mocking

Use `unittest.mock` for external dependencies:

```python
from unittest.mock import Mock, patch

@patch('pyautogui.click')
def test_click_action(mock_click):
    action = ClickAction()
    action.execute(ctx, {'x': 100, 'y': 200})
    mock_click.assert_called_once_with(100, 200)
```

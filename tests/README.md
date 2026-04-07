# Tests Directory

This directory contains the test suite for RabAI AutoClick.

## 📁 Test Files

| File | Description |
|------|-------------|
| `test_core.py` | pytest tests for core modules |
| `test_modules.py` | pytest tests for utils and actions |
| `test_v22.py` | pytest tests for v22 features |
| `test_full.py` | comprehensive test script |
| `debug_loader.py` | debug script for action loading |
| `debug_engine.py` | debug script for engine |

## 🚀 Quick Start

### Run All Tests

```bash
pytest tests/ -v
```

### Run with Coverage

```bash
pytest tests/ -v --cov=. --cov-report=html --cov-report=term-missing
```

## 🧪 Running Tests

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

# Run tests matching pattern
pytest tests/ -k "test_click" -v

# Run tests in specific directory
pytest tests/core/ -v
```

### Using test_full.py

```bash
# Run as script
python tests/test_full.py

# Or with pytest
pytest tests/test_full.py -v
```

### Using Make

```bash
make test           # Run tests
make test-cov      # Run tests with coverage
```

## 📐 Test Structure

### test_core.py

Core module tests using pytest:

```python
class TestContextManager:
    def test_basic_operations(self):
        ctx = ContextManager()
        ctx.set('key', 'value')
        assert ctx.get('key') == 'value'

    def test_variable_resolution(self):
        ctx = ContextManager()
        ctx.set('a', 10)
        ctx.set('b', 20)
        result = ctx.resolve_value('{{a + b}}')
        assert result == 30
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

## 🔧 Debug Scripts

### debug_loader.py

Check if actions are loading correctly:

```bash
python tests/debug_loader.py
```

**Expected Output:**
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

**Expected Output:**
```
Actions loaded: ['click', 'delay', ...]
Before run: {}
Run result: True
After run: {'test_var': 'hello'}
test_var: hello
```

## ✍️ Writing Tests

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

    @pytest.mark.parametrize("input,expected", [
        (1, 2),
        (2, 4),
        (3, 6),
    ])
    def test_multiplication(self, input, expected):
        result = input * 2
        assert result == expected
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

### Async Tests

```python
import pytest
import asyncio

@pytest.mark.asyncio
async def test_async_action():
    result = await async_function()
    assert result is not None
```

## 📊 Test Data

Test data files are stored in:
- `tests/data/` - JSON test fixtures
- `tests/fixtures/` - Image and other test assets

## 🔄 CI/CD

Tests run automatically on:
- Every pull request
- Every push to main branch
- Daily at midnight

### GitHub Actions Workflow

```yaml
name: Test
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: pytest tests/ -v
```

## 📈 Coverage Goals

| Module | Target |
|--------|--------|
| Core modules | 90%+ |
| Actions | 80%+ |
| Utils | 70%+ |
| UI | 50%+ (harder to test) |

## 🎭 Mocking

Use `unittest.mock` for external dependencies:

```python
from unittest.mock import Mock, patch

@patch('pyautogui.click')
def test_click_action(mock_click):
    action = ClickAction()
    action.execute(ctx, {'x': 100, 'y': 200})
    mock_click.assert_called_once_with(100, 200)

@patch('core.engine.FlowEngine.run')
def test_workflow_run(mock_run):
    engine = FlowEngine()
    engine.load_workflow({...})
    engine.run()
    mock_run.assert_called_once()
```

## 🐛 Fixtures

```python
import pytest

@pytest.fixture
def context():
    """Provide a clean ContextManager for each test."""
    from core.context import ContextManager
    return ContextManager()

@pytest.fixture
def workflow():
    """Provide a sample workflow dict."""
    return {
        "name": "Test Workflow",
        "steps": [
            {"id": 1, "type": "delay", "seconds": 0.1}
        ]
    }
```

## ⚠️ Test Best Practices

1. **Isolation**: Each test should be independent
2. **Clear names**: Test names should describe what they verify
3. **Single assertion**: Prefer one assertion per test when practical
4. **Arrange-Act-Assert**: Structure test code clearly
5. **Test edge cases**: Empty values, boundary conditions, errors
6. **Mock external dependencies**: Don't rely on external systems

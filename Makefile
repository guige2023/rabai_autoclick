# RabAI AutoClick Makefile
# Common development tasks

.PHONY: help install dev test test-cov format lint clean build run gui docs

# Python executable
PYTHON := python3
PIP := pip3
VENV := .venv

# Default target
help:
	@echo "RabAI AutoClick Development Tasks"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Development:"
	@echo "  install    Install dependencies"
	@echo "  dev        Install development dependencies"
	@echo "  test       Run tests"
	@echo "  test-cov   Run tests with coverage"
	@echo "  format     Format code with black"
	@echo "  lint       Run ruff linter"
	@echo "  typecheck  Run pyright type checker"
	@echo ""
	@echo "Building:"
	@echo "  build      Build application bundle"
	@echo "  clean      Remove build artifacts"
	@echo ""
	@echo "Running:"
	@echo "  run        Run the GUI application"
	@echo "  gui        Run the GUI (alias for run)"
	@echo "  cli        Run the CLI"
	@echo ""
	@echo "Documentation:"
	@echo "  docs       Generate documentation"
	@echo ""

# Install dependencies
install:
	$(PIP) install -r requirements.txt

# Install development dependencies
dev:
	$(PIP) install -r requirements.txt
	$(PIP) install black ruff pytest pytest-cov pytest-asyncio pyright

# Run tests
test:
	pytest tests/ -v

# Run tests with coverage
test-cov:
	pytest tests/ -v --cov=. --cov-report=html --cov-report=term-missing

# Format code with black
format:
	black .
	ruff check --fix .

# Run ruff linter
lint:
	ruff check .

# Run type checker
typecheck:
	pyright .

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .mypy_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

# Build application bundle (macOS)
build:
	./build_dmg.sh

# Run the GUI application
run gui:
	$(PYTHON) main.py

# Run the CLI
cli:
	$(PYTHON) -m cli.main --help

# Generate documentation (placeholder for future sphinx integration)
docs:
	@echo "Documentation generation not yet configured"
	@echo "See docs/README.md for current documentation"

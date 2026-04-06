# RabAI AutoClick Makefile
# Common development tasks

.PHONY: help install test format lint clean build run gui

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
	@echo "Targets:"
	@echo "  install    Install dependencies"
	@echo "  dev        Install development dependencies"
	@echo "  test       Run tests"
	@echo "  format     Format code with black"
	@echo "  lint       Run ruff linter"
	@echo "  clean      Remove build artifacts"
	@echo "  build      Build application bundle"
	@echo "  run        Run the GUI application"
	@echo "  gui        Run the GUI (alias for run)"

# Install dependencies
install:
	$(PIP) install -r requirements.txt

# Install development dependencies
dev:
	$(PIP) install -r requirements.txt
	$(PIP) install black ruff pytest pytest-cov pytest-asyncio

# Run tests
test:
	pytest tests/ -v

# Format code with black
format:
	black .

# Run ruff linter
lint:
	ruff check .

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .mypy_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Build application bundle (macOS)
build:
	./build_dmg.sh

# Run the GUI application
run gui:
	$(PYTHON) main.py

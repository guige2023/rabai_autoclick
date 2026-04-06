#!/bin/bash
# Build and test script for RabAI AutoClick

set -e

echo "========================================="
echo "RabAI AutoClick Build and Test"
echo "========================================="

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check Python version
echo -e "${YELLOW}Checking Python version...${NC}"
python3 --version

# Install dependencies
echo -e "${YELLOW}Installing dependencies...${NC}"
pip install -q -r requirements.txt

# Run linting
echo -e "${YELLOW}Running ruff linting...${NC}"
if ruff check .; then
    echo -e "${GREEN}Ruff: OK${NC}"
else
    echo -e "${RED}Ruff: FAILED${NC}"
    exit 1
fi

# Run formatting check
echo -e "${YELLOW}Checking code formatting...${NC}"
if black --check .; then
    echo -e "${GREEN}Black: OK${NC}"
else
    echo -e "${RED}Black: Needs formatting${NC}"
    exit 1
fi

# Run tests
echo -e "${YELLOW}Running tests...${NC}"
pytest tests/ -v --tb=short

echo -e "${GREEN}========================================="
echo "Build and test completed successfully!"
echo "=========================================${NC}"

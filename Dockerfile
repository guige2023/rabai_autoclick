# RabAI AutoClick Dockerfile with xvfb for headless GUI testing
# Build: docker build -t rabai-autoclick .
# Run:   docker run --rm -e DISPLAY=:99 rabai-autoclick

FROM python:3.11-slim

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DISPLAY=:99

# Install system dependencies and xvfb for headless GUI testing
RUN apt-get update && apt-get install -y \
    xvfb \
    libxkbcommon-x11-0 \
    libxcb-icccm4-0 \
    libxcb-image0 \
    libxcb-keysyms1 \
    libxcb-randr0 \
    libxcb-render-util0 \
    libxcb-xinerama0 \
    libxcb-xfixes0 \
    libxcb-shape0 \
    libegl1 \
    libgl1 \
    libglib2.0-0 \
    libdbus-1-3 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libxtst6 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatspi2.0-0 \
    fonts-noto \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy dependency files
COPY requirements.txt pyproject.toml ./

# Install Python dependencies
RUN pip install --no-cache-dir -e ".[dev]"

# Copy application code
COPY . .

# Expose port for any development server
EXPOSE 8765

# Default command - run tests with xvfb
CMD ["xvfb-run", "-a", "pytest", "tests/", "-v", "--tb=short"]

# Alternative commands:
# Run GUI tests: xvfb-run -a python -m pytest tests/gui/ -v
# Run CLI:       xvfb-run -a python -m cli.main
# Interactive:   bash

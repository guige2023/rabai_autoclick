# Scripts Directory

This directory contains utility scripts for RabAI AutoClick.

## Files

| File | Description |
|------|-------------|
| `create_app.py` | macOS .app bundle creator |

## Usage

### create_app.py

Create a macOS application bundle from the RabAI AutoClick source:

```bash
python scripts/create_app.py
```

This creates `dist/RabAI-AutoClick.app` that can be:
- Double-clicked to launch
- Added to Applications folder
- Signed with Developer ID for distribution

## Requirements

- macOS
- Python 3.8+
- All dependencies from requirements.txt installed

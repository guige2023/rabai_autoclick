# Support

## Getting Help

### Documentation

- [README.md](README.md) - Project overview and quick start
- [INSTALL.md](INSTALL.md) - Installation guide
- [docs/使用教程.md](docs/使用教程.md) - Chinese usage tutorial

### Issues

Before opening an issue, please:

1. Search existing issues first
2. Check the [Troubleshooting](docs/使用教程.md#常见问题) section
3. Run the test suite: `pytest tests/ -v`

### Opening an Issue

When opening an issue, include:

- Your operating system and version
- Python version: `python --version`
- RabAI AutoClick version: Check `main.py` or about dialog
- Steps to reproduce
- Expected vs actual behavior
- Error messages or logs

## Community

### Discussion

- [GitHub Discussions](https://github.com/guige2023/rabai_autoclick/discussions) - Q&A and general discussion

### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to contribute.

## Professional Support

For enterprise support or consulting, contact the maintainers through GitHub.

## Quick Fixes

### Python not found

```bash
# macOS
brew install python3

# Ubuntu/Debian
sudo apt install python3 python3-venv

# Windows
# Download from python.org
```

### Permission denied (macOS)

Go to **System Preferences → Security & Privacy → Privacy → Accessibility** and add Terminal/Python.

### OCR not working

```bash
pip install paddleocr paddlepaddle
```

### Import errors

```bash
pip install -r requirements.txt --force-reinstall
```

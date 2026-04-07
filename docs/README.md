# Documentation Directory

This directory contains documentation files for RabAI AutoClick.

## 📚 Documentation Structure

```
docs/
├── README.md          # This file
└── 使用教程.md        # Chinese usage tutorial
```

## 📖 Available Documentation

### Project-Level Documentation

| File | Description |
|------|-------------|
| [README.md](../README.md) | Main project documentation |
| [INSTALL.md](../INSTALL.md) | Installation guide |
| [ARCHITECTURE.md](../ARCHITECTURE.md) | System architecture |
| [CONTRIBUTING.md](../CONTRIBUTING.md) | Contribution guidelines |
| [CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md) | Community code of conduct |
| [SECURITY.md](../SECURITY.md) | Security policy |
| [CHANGELOG.md](../CHANGELOG.md) | Version history |
| [LICENSE](../LICENSE) | MIT License |

### Per-Directory Documentation

Each source directory has its own README:

| Directory | Documentation |
|-----------|---------------|
| [actions/](../actions/README.md) | Action development guide |
| [cli/](../cli/README.md) | CLI commands reference |
| [core/](../core/README.md) | Core engine architecture |
| [src/](../src/README.md) | v22 advanced features |
| [tests/](../tests/README.md) | Testing guide |
| [ui/](../ui/README.md) | UI components reference |
| [utils/](../utils/README.md) | Utility modules reference |
| [workflows/](../workflows/README.md) | Workflow examples |

## 🌍 Language Support

- **Primary**: English documentation
- **中文**: Chinese documentation available in `使用教程.md`

## 📝 Documentation Standards

### Writing Style

1. Use clear, concise language
2. Include code examples for complex features
3. Add screenshots for UI-related documentation
4. Provide Chinese translations for key concepts

### File Naming

- English files: `README.md`, `ARCHITECTURE.md`
- Chinese files: `使用教程.md`, `配置指南.md`

### Markdown Formatting

- Use ATX-style headers (`#`, `##`, `###`)
- Include tables for parameter documentation
- Use code blocks with language specification
- Add badges for version/feature availability

## 🔧 Building Documentation

Future plans include Sphinx-based documentation generation:

```bash
# Install documentation dependencies
pip install sphinx sphinx-rtd-theme

# Generate HTML documentation
make docs
```

## 📞 Contributing to Documentation

See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines on contributing documentation improvements.

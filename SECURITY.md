# Security Policy

## Supported Versions

We release patches for security vulnerabilities for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 2.x.x   | ✅ Currently supported |
| 1.x.x   | ❌ End of life |

## Reporting a Vulnerability

If you discover a security vulnerability within RabAI AutoClick, please follow these steps:

### For Security Researchers

1. **Do NOT** create a public GitHub issue for security vulnerabilities
2. Send a detailed report to the maintainers via:
   - GitHub's [Private vulnerability reporting](https://github.com/guige2023/rabai_autoclick/security/advisories/new)
   - Or email: security@rabai.app

3. Include the following information:
   - Type of vulnerability
   - Full paths of source file(s) related to the vulnerability
   - Location of the affected source code
   - Step-by-step instructions to reproduce the issue
   - Proof-of-concept or exploit code (if possible)
   - Impact assessment

4. Allow time for the maintainers to assess and fix the vulnerability:
   - Initial response within 48 hours
   - Resolution timeline: 2-4 weeks depending on severity

### What We Promise

- **Credit**: We'll credit the reporter in the security advisory (unless you prefer anonymity)
- **Communication**: We'll keep you informed throughout the fix process
- **Recognition**: Security contributors will be featured in our release notes

## Security Best Practices

### For Users

1. **Run with minimal permissions**: Only grant accessibility permissions when necessary
2. **Review workflows**: Before running automation workflows, review the actions they perform
3. **Keep updated**: Always use the latest version for security patches
4. **Protect your data**: Be cautious when sharing workflow files

### For Developers

1. **Input validation**: Always validate user inputs in custom actions
2. **Safe execution**: Use the provided safe_exec sandbox for code execution
3. **No system modifications**: Avoid actions that modify system settings or files outside the project scope

## Known Security Considerations

### Accessibility Permissions

RabAI AutoClick requires macOS accessibility permissions to:
- Simulate mouse clicks and keyboard input
- Capture screen content for OCR and image matching

⚠️ **Warning**: These permissions give the application significant system access. Only use RabAI AutoClick from trusted sources.

### Script Execution

The script execution feature (`ScriptAction`) runs Python code in a sandboxed environment:

**Allowed**:
- Basic Python operations
- Context variable access
- Built-in functions (int, float, str, len, etc.)

**Blocked**:
- Import statements
- File system access
- Network requests
- System command execution

## Dependencies Security

We regularly update dependencies to patch known vulnerabilities:

- PyQt5
- pyautogui
- opencv-python
- pynput
- rapidocr-onnxruntime

We use GitHub's dependency review and Dependabot to monitor and update dependencies.

## License

This security policy is adapted from best practices for open source projects.

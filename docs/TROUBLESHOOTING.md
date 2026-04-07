# Troubleshooting Guide

Common issues and their solutions.

## Installation Issues

### "Module not found" errors

**Symptoms:**
```
ModuleNotFoundError: No module named 'pyautogui'
```

**Solution:**
```bash
pip install -r requirements.txt
```

### macOS permission denied

**Symptoms:**
```
PermissionError: [Errno 13] Permission denied
```

**Solution:**
1. System Preferences → Security & Privacy → Privacy
2. Select "Accessibility"
3. Add Terminal or Python to allowed apps
4. Restart application

### Qt platform plugin not found

**Symptoms:**
```
qt.qpa.plugin: Could not find the Qt platform plugin "cocoa"
```

**Solution:**
```bash
pip install PyQt5
# Or reinstall
pip uninstall PyQt5 PyQt5-sip PyQt5-Qt5
pip install PyQt5
```

## Execution Issues

### Workflow stops unexpectedly

**Symptoms:**
- Workflow stops at a certain step
- No error message displayed

**Solution:**
1. Enable debug logging in settings
2. Check logs in `./logs/`
3. Add delays between steps
4. Verify coordinates are correct

### Click coordinates off by offset

**Symptoms:**
- Clicks happen at wrong position
- Offset is consistent

**Cause:** Multi-monitor setup or display scaling

**Solution:**
1. Close other monitors
2. Adjust display scaling to 100%
3. Use image recognition instead of coordinates

### OCR returns empty results

**Symptoms:**
- OCR action completes but no text found
- `recognized_text` is empty

**Solution:**
1. Reduce recognition region
2. Increase image contrast
3. Use exact_match: false for fuzzy matching
4. Verify screenshot quality

### Image matching fails

**Symptoms:**
- `click_image` action doesn't find template
- Template found but wrong location

**Solution:**
1. Use PNG format for templates
2. Increase confidence threshold (0.7-0.9)
3. Ensure template matches screen exactly
4. Avoid templates with text that changes

## Platform-Specific Issues

### Windows: Admin rights required

**Symptoms:**
- Global shortcuts not working
- Some clicks not registering

**Solution:**
- Run as Administrator
- Disable UAC temporarily

### macOS: Accessibility permissions

**Symptoms:**
- Global shortcuts not working
- Keyboard input not captured

**Solution:**
1. System Preferences → Security & Privacy → Privacy → Accessibility
2. Add Terminal/Python to allowed list
3. Check "Enable access for assistive devices"
4. Restart application

### Linux: Display not found

**Symptoms:**
```
python main.py
# Error: Could not open display
```

**Solution:**
```bash
# Install X11 libraries
sudo apt install libxkbcommon-x11-0 libxcb-icccm4 libxcb-image0 libxcb-keysyms1 libxcb-randr0 libxcb-render-util0 libxcb-xinerama0 libxcb-cursor0

# Or use virtual display
export DISPLAY=:0
```

## Performance Issues

### High CPU usage

**Symptoms:**
- Computer fans spin up
- Application becomes sluggish

**Solution:**
1. Reduce polling frequency
2. Close other applications
3. Disable unnecessary features
4. Increase delays between steps

### Memory leak

**Symptoms:**
- Memory usage grows over time
- Eventually crashes

**Solution:**
1. Enable memory optimization in settings
2. Restart application periodically
3. Clear history/logs
4. Check for infinite loops

## Keyboard/Mouse Issues

### Global shortcuts not responding

**Symptoms:**
- Ctrl+F6, Ctrl+F7 etc. don't work
- Works in other apps

**Solution (macOS):**
```bash
# Grant Accessibility permissions
System Preferences → Security & Privacy → Privacy → Accessibility
```

**Solution (Windows):**
```bash
# Run as administrator
# Or check UAC settings
```

### Type text sending wrong characters

**Symptoms:**
- Wrong characters appear
- Special characters not working

**Solution:**
1. Check keyboard layout matches input
2. Use `key_press` for special keys
3. Avoid special characters in `type_text`

## Workflow Issues

### Variable not resolving

**Symptoms:**
- `{{variable}}` shows literally
- Variables not substituted

**Solution:**
1. Check variable is defined in `variables` section
2. Verify expression syntax: `{{var + 1}}`
3. Check for typos in variable names

### Loop not executing

**Symptoms:**
- Loop runs only once
- `loop_start`/`loop_end` not working

**Solution:**
```json
{
  "id": 1,
  "type": "loop",
  "loop_id": "main",
  "count": 5,
  "loop_start": 2
}
```
- Ensure `loop_start` points to valid step ID
- Use `goto` action to return to loop start

### Condition always true/false

**Symptoms:**
- `condition` always takes same branch
- Unexpected workflow path

**Solution:**
```json
{
  "condition": "{{variable > 0}}",
  "true_next": 5,
  "false_next": 10
}
```
- Use proper comparison operators: `>`, `<`, `==`, `!=`
- String comparisons: `{{text.includes('success')}}`

## Recovery and Prevention

### Backing up workflows

```bash
# Copy workflows directory
cp -r workflows workflows_backup_$(date +%Y%m%d)

# Or use git
git add workflows/
git commit -m "Backup workflows"
```

### Preventing issues

1. **Test incrementally**: Run one step at a time
2. **Add delays**: Insert delays between actions
3. **Use logging**: Enable debug mode
4. **Save frequently**: Save workflow after each change

### Getting help

1. Check [GitHub Issues](https://github.com/guige2023/rabai_autoclick/issues)
2. Include logs from `./logs/`
3. Provide reproduction steps
4. Specify OS and version

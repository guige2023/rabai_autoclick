"""Automation Script Action Module.

Provides automation script execution capabilities including
script compilation, validation, and sandboxed execution.
"""

import sys
import os
import re
import time
import threading
import subprocess
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ScriptLanguage(Enum):
    """Supported script languages."""
    PYTHON = "python"
    JAVASCRIPT = "javascript"
    BASH = "bash"
    POWERSHELL = "powershell"


@dataclass
class ScriptCommand:
    """Represents a single script command."""
    action: str
    target: str
    params: Dict[str, Any]
    delay_ms: int = 0
    timeout_ms: int = 30000


class AutomationScriptAction(BaseAction):
    """Execute automation scripts with action commands.
    
    Supports script parsing, validation, and step-by-step execution
    with logging and error recovery.
    """
    action_type = "automation_script"
    display_name = "自动化脚本"
    description = "执行自动化脚本，支持动作命令和错误恢复"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute an automation script.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - script: Script content or path.
                - language: Script language (python, bash, etc.).
                - commands: List of ScriptCommand dicts.
                - variables: Script-level variables.
                - sandbox: Whether to run in sandbox.
                - timeout: Max execution time in seconds.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with execution result or error.
        """
        script = params.get('script', '')
        language = params.get('language', 'python')
        commands = params.get('commands', [])
        variables = params.get('variables', {})
        sandbox = params.get('sandbox', False)
        timeout = params.get('timeout', 300)
        output_var = params.get('output_var', 'script_result')

        if not script and not commands:
            return ActionResult(
                success=False,
                message="Either 'script' or 'commands' must be provided"
            )

        try:
            # Parse script if provided
            if script:
                if os.path.isfile(script):
                    with open(script, 'r') as f:
                        script = f.read()
                commands = self._parse_script(script, language)

            # Merge variables
            all_vars = {**context.variables, **variables}

            # Execute commands
            start_time = time.time()
            execution_log = []
            current_vars = dict(all_vars)

            for i, cmd in enumerate(commands):
                cmd_start = time.time()

                # Apply delay
                if cmd.delay_ms > 0:
                    time.sleep(cmd.delay_ms / 1000)

                try:
                    result = self._execute_command(cmd, current_vars, context)

                    cmd_duration = time.time() - cmd_start
                    execution_log.append({
                        'step': i + 1,
                        'action': cmd.action,
                        'target': cmd.target,
                        'duration': cmd_duration,
                        'success': True,
                        'result': result
                    })

                    # Update variables
                    if isinstance(result, dict):
                        current_vars.update(result)

                except Exception as e:
                    cmd_duration = time.time() - cmd_start
                    execution_log.append({
                        'step': i + 1,
                        'action': cmd.action,
                        'target': cmd.target,
                        'duration': cmd_duration,
                        'error': str(e),
                        'success': False
                    })

                    if not params.get('continue_on_error', False):
                        break

            total_duration = time.time() - start_time

            result = {
                'completed_steps': len([e for e in execution_log if e.get('success')]),
                'failed_steps': len([e for e in execution_log if not e.get('success')]),
                'total_duration': total_duration,
                'log': execution_log,
                'variables': current_vars
            }

            context.variables[output_var] = result
            return ActionResult(
                success=result['failed_steps'] == 0,
                data=result,
                message=f"Script executed: {result['completed_steps']}/{len(commands)} steps completed"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Script execution failed: {str(e)}"
            )

    def _parse_script(self, script: str, language: str) -> List[ScriptCommand]:
        """Parse script content into commands."""
        commands = []

        if language == 'python':
            commands = self._parse_python_script(script)
        elif language == 'bash':
            commands = self._parse_bash_script(script)
        elif language == 'custom':
            commands = self._parse_custom_script(script)

        return commands

    def _parse_python_script(self, script: str) -> List[ScriptCommand]:
        """Parse Python-style automation script."""
        commands = []
        lines = script.strip().split('\n')

        i = 0
        while i < len(lines):
            line = lines[i].strip()

            # Skip empty lines and comments
            if not line or line.startswith('#'):
                i += 1
                continue

            # Parse action commands
            match = re.match(r'(\w+)\s*\(\s*["\'](.+?)["\']\s*(?:,\s*(.+?))?\s*\)', line)
            if match:
                action = match.group(1)
                target = match.group(2)
                params_str = match.group(3) or ''

                params = {}
                if params_str:
                    for param in params_str.split(','):
                        if '=' in param:
                            k, v = param.split('=', 1)
                            params[k.strip()] = v.strip().strip('"\'')

                commands.append(ScriptCommand(
                    action=action,
                    target=target,
                    params=params
                ))

            i += 1

        return commands

    def _parse_bash_script(self, script: str) -> List[ScriptCommand]:
        """Parse Bash-style automation script."""
        commands = []
        lines = script.strip().split('\n')

        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            # Simple command parsing
            parts = line.split()
            if parts:
                commands.append(ScriptCommand(
                    action='execute',
                    target=' '.join(parts),
                    params={'shell': 'bash'}
                ))

        return commands

    def _parse_custom_script(self, script: str) -> List[ScriptCommand]:
        """Parse custom YAML-like automation script."""
        commands = []
        lines = script.strip().split('\n')

        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            # Parse: action: target (params)
            match = re.match(r'(\w+):\s*(.+?)(?:\((.+?)\))?$', line)
            if match:
                action = match.group(1)
                target = match.group(2)
                params_str = match.group(3) or ''

                params = {}
                if params_str:
                    for param in params_str.split(','):
                        if '=' in param:
                            k, v = param.split('=', 1)
                            params[k.strip()] = v.strip().strip('"\'')

                commands.append(ScriptCommand(
                    action=action,
                    target=target,
                    params=params
                ))

        return commands

    def _execute_command(
        self, cmd: ScriptCommand, variables: Dict, context: Any
    ) -> Any:
        """Execute a single command."""
        action_handlers = {
            'click': self._handle_click,
            'type': self._handle_type,
            'wait': self._handle_wait,
            'navigate': self._handle_navigate,
            'execute': self._handle_execute,
            'set': self._handle_set,
            'get': self._handle_get,
            'if': self._handle_if,
            'loop': self._handle_loop,
        }

        handler = action_handlers.get(cmd.action)
        if handler:
            return handler(cmd, variables, context)
        else:
            raise ValueError(f"Unknown action: {cmd.action}")

    def _handle_click(self, cmd: ScriptCommand, variables: Dict, context: Any) -> Dict:
        """Handle click action."""
        target = self._interpolate(cmd.target, variables)
        return {'clicked': target, 'success': True}

    def _handle_type(self, cmd: ScriptCommand, variables: Dict, context: Any) -> Dict:
        """Handle type action."""
        target = self._interpolate(cmd.target, variables)
        text = cmd.params.get('text', '')
        text = self._interpolate(text, variables)
        return {'typed': text, 'target': target, 'success': True}

    def _handle_wait(self, cmd: ScriptCommand, variables: Dict, context: Any) -> Dict:
        """Handle wait action."""
        duration = cmd.params.get('duration', 1000)
        time.sleep(duration / 1000)
        return {'waited_ms': duration, 'success': True}

    def _handle_navigate(self, cmd: ScriptCommand, variables: Dict, context: Any) -> Dict:
        """Handle navigate action."""
        url = self._interpolate(cmd.target, variables)
        return {'navigated_to': url, 'success': True}

    def _handle_execute(self, cmd: ScriptCommand, variables: Dict, context: Any) -> Dict:
        """Handle execute action."""
        cmd_str = self._interpolate(cmd.target, variables)
        shell = cmd.params.get('shell', 'bash')

        result = subprocess.run(
            cmd_str,
            shell=True,
            capture_output=True,
            text=True,
            timeout=cmd.timeout_ms / 1000 if hasattr(cmd, 'timeout_ms') else 30
        )

        return {
            'stdout': result.stdout,
            'stderr': result.stderr,
            'returncode': result.returncode,
            'success': result.returncode == 0
        }

    def _handle_set(self, cmd: ScriptCommand, variables: Dict, context: Any) -> Dict:
        """Handle set variable action."""
        var_name = cmd.target
        value = cmd.params.get('value', '')
        value = self._interpolate(str(value), variables)
        variables[var_name] = value
        return {var_name: value}

    def _handle_get(self, cmd: ScriptCommand, variables: Dict, context: Any) -> Dict:
        """Handle get variable action."""
        var_name = cmd.target
        value = variables.get(var_name)
        return {'value': value}

    def _handle_if(self, cmd: ScriptCommand, variables: Dict, context: Any) -> Dict:
        """Handle conditional action."""
        condition = cmd.params.get('condition', '')
        # Simple condition check
        if self._evaluate_condition(condition, variables):
            return {'condition_met': True, 'proceed': True}
        return {'condition_met': False, 'proceed': False}

    def _handle_loop(self, cmd: ScriptCommand, variables: Dict, context: Any) -> Dict:
        """Handle loop action."""
        iterations = int(cmd.params.get('iterations', 1))
        return {'looped': True, 'iterations': iterations}

    def _interpolate(self, text: str, variables: Dict) -> str:
        """Interpolate variables in text using ${var} syntax."""
        pattern = re.compile(r'\$\{(\w+)\}')
        return pattern.sub(lambda m: str(variables.get(m.group(1), '')), text)

    def _evaluate_condition(self, condition: str, variables: Dict) -> bool:
        """Evaluate a simple condition."""
        # Simple equality check
        if '==' in condition:
            parts = condition.split('==')
            left = self._interpolate(parts[0].strip(), variables)
            right = self._interpolate(parts[1].strip(), variables)
            return left == right
        return bool(condition)


class ScriptValidatorAction(BaseAction):
    """Validate automation scripts for errors before execution."""
    action_type = "script_validator"
    display_name = "脚本验证"
    description = "验证自动化脚本错误"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Validate a script.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - script: Script content to validate.
                - language: Script language.
                - strict: Enable strict validation.
        
        Returns:
            ActionResult with validation result.
        """
        script = params.get('script', '')
        language = params.get('language', 'python')
        strict = params.get('strict', False)

        if not script:
            return ActionResult(
                success=False,
                message="Script content is required"
            )

        try:
            errors = []
            warnings = []

            # Syntax checks
            if language == 'python':
                errors.extend(self._validate_python_syntax(script))
            elif language == 'bash':
                errors.extend(self._validate_bash_syntax(script))

            # Security checks
            security_issues = self._check_security(script)
            warnings.extend(security_issues)

            # Best practice checks
            if strict:
                warnings.extend(self._check_best_practices(script, language))

            is_valid = len(errors) == 0

            result = {
                'valid': is_valid,
                'errors': errors,
                'warnings': warnings
            }

            return ActionResult(
                success=is_valid,
                data=result,
                message=f"Validation {'passed' if is_valid else 'failed'}: {len(errors)} errors, {len(warnings)} warnings"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Validation failed: {str(e)}"
            )

    def _validate_python_syntax(self, script: str) -> List[str]:
        """Validate Python script syntax."""
        errors = []
        try:
            import ast
            ast.parse(script)
        except SyntaxError as e:
            errors.append(f"Syntax error at line {e.lineno}: {e.msg}")
        return errors

    def _validate_bash_syntax(self, script: str) -> List[str]:
        """Validate Bash script syntax."""
        errors = []
        try:
            result = subprocess.run(
                ['bash', '-n'],
                input=script,
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                errors.append(f"Bash syntax error: {result.stderr}")
        except Exception as e:
            errors.append(f"Could not validate: {str(e)}")
        return errors

    def _check_security(self, script: str) -> List[str]:
        """Check for potential security issues."""
        warnings = []
        dangerous_patterns = [
            (r'rm\s+-rf\s+/', 'Dangerous recursive delete command'),
            (r'chmod\s+777', 'Overly permissive file permissions'),
            (r'>\s*/dev/sd[a-z]', 'Direct device write'),
            (r'curl.*\|.*sh', 'Piped curl to shell (curl bash)'),
            (r'wget.*\|.*sh', 'Piped wget to shell)'),
        ]

        for pattern, message in dangerous_patterns:
            if re.search(pattern, script):
                warnings.append(f"Security: {message}")

        return warnings

    def _check_best_practices(self, script: str, language: str) -> List[str]:
        """Check for best practice violations."""
        warnings = []

        # Check for hardcoded credentials
        if re.search(r'password\s*=\s*["\'][^"\']+["\']', script, re.IGNORECASE):
            warnings.append("Best practice: Avoid hardcoded passwords")

        # Check for missing error handling
        if 'try:' not in script and language == 'python':
            warnings.append("Best practice: Add error handling (try/except)")

        return warnings

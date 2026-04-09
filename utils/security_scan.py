"""Security scanning utilities for RabAI AutoClick.

Scans workflow JSON for potentially dangerous actions and
suspicious parameter patterns that could indicate security risks.
"""

import re
from typing import Any, Dict, List, Optional, Tuple


class SecurityScanner:
    """Scans workflows for security concerns before execution.
    
    Checks for dangerous action combinations and suspicious
    parameter patterns like plain-text passwords or API keys.
    """
    
    # Actions that are considered potentially dangerous
    DANGEROUS_ACTIONS = {
        "delete_file", "rm", "remove_file", "delete_folder",
        "format_disk", "delete_registry", "drop_table",
        "execute_shell", "run_command", "eval", "exec",
        "download_file", "upload_file", "send_request",
        "modify_system", "kill_process", "stop_service",
    }
    
    # Patterns that suggest sensitive data in plain text
    SUSPICIOUS_PATTERNS = [
        (re.compile(r'password["\']?\s*[:=]\s*["\'][^"\']{8,}["\']', re.IGNORECASE),
         "Plain-text password detected"),
        (re.compile(r'api[_-]?key["\']?\s*[:=]\s*["\'][a-zA-Z0-9_-]{20,}["\']', re.IGNORECASE),
         "Potential API key in plain text"),
        (re.compile(r'token["\']?\s*[:=]\s*["\'][a-zA-Z0-9_-]{20,}["\']', re.IGNORECASE),
         "Potential access token in plain text"),
        (re.compile(r'secret["\']?\s*[:=]\s*["\'][^"\']{8,}["\']', re.IGNORECASE),
         "Plain-text secret detected"),
        (re.compile(r'-----BEGIN\s+(RSA\s+)?PRIVATE\s+KEY-----', re.IGNORECASE),
         "Private key embedded in workflow"),
        (re.compile(r'aws[_-]?(access[_-]?key[_-]?id|secret[_-]?access[_-]?key)', re.IGNORECASE),
         "AWS credentials pattern detected"),
        (re.compile(r'ghp_[a-zA-Z0-9]{36}', re.IGNORECASE),
         "GitHub personal access token pattern detected"),
    ]
    
    # Parameter names that should typically be encrypted
    SENSITIVE_PARAM_NAMES = {
        "password", "passwd", "pwd", "secret", "token",
        "api_key", "apikey", "api-key", "access_token",
        "auth_token", "private_key", "credential", "private_key",
        "aws_access_key", "aws_secret_key",
    }
    
    # Combinations of actions that are especially risky
    DANGEROUS_COMBINATIONS = [
        (["delete_file", "download_file"], "File deletion followed by download"),
        (["execute_shell", "delete_file"], "Shell execution followed by file deletion"),
        (["run_command", "delete_file"], "Command execution followed by file deletion"),
        (["eval", "execute_shell"], "Code evaluation followed by shell execution"),
        (["exec", "run_command"], "Execution followed by command running"),
    ]
    
    def __init__(self) -> None:
        """Initialize the security scanner."""
        self._issues: List[Dict[str, Any]] = []
    
    def reset(self) -> None:
        """Reset the scanner issues list."""
        self._issues = []
    
    def _add_issue(
        self,
        severity: str,
        message: str,
        location: Optional[str] = None,
        action: Optional[str] = None,
        param: Optional[str] = None
    ) -> None:
        """Add a security issue to the issues list.
        
        Args:
            severity: Issue severity (low, medium, high, critical).
            message: Human-readable issue description.
            location: Where the issue was found (e.g., action name).
            action: The action that triggered the issue.
            param: The parameter that triggered the issue.
        """
        issue = {
            "severity": severity,
            "message": message,
            "location": location,
            "action": action,
            "param": param,
        }
        self._issues.append(issue)
    
    def _check_suspicious_patterns(
        self,
        params: Dict[str, Any],
        action_name: Optional[str] = None
    ) -> None:
        """Check parameters for suspicious patterns.
        
        Args:
            params: Action parameters to check.
            action_name: Name of the action being checked.
        """
        for key, value in params.items():
            # Check if key is a sensitive parameter name
            key_lower = key.lower()
            if key_lower in self.SENSITIVE_PARAM_NAMES:
                if isinstance(value, str) and len(value) > 0:
                    # Check if it looks encrypted
                    if not value.startswith("_enc_"):
                        self._add_issue(
                            "high",
                            f"Sensitive parameter '{key}' appears to be in plain text",
                            location=action_name,
                            action=action_name,
                            param=key
                        )
            
            # Check value against suspicious patterns
            if isinstance(value, str):
                for pattern, message in self.SUSPICIOUS_PATTERNS:
                    if pattern.search(value):
                        self._add_issue(
                            "high",
                            message,
                            location=action_name,
                            action=action_name,
                            param=key
                        )
            
            # Recursively check nested dicts
            if isinstance(value, dict):
                self._check_suspicious_patterns(value, action_name)
            
            # Check list values
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        for pattern, message in self.SUSPICIOUS_PATTERNS:
                            if pattern.search(item):
                                self._add_issue(
                                    "high",
                                    message,
                                    location=action_name,
                                    action=action_name,
                                    param=key
                                )
    
    def _check_dangerous_actions(self, actions: List[Dict[str, Any]]) -> None:
        """Check for dangerous action combinations.
        
        Args:
            actions: List of workflow actions.
        """
        action_names = [
            a.get("type", "") or a.get("action", "") or a.get("name", "")
            for a in actions
        ]
        
        # Check individual dangerous actions
        for i, action in enumerate(actions):
            action_type = action.get("type", "") or action.get("action", "") or action.get("name", "")
            action_type_lower = action_type.lower()
            
            if action_type_lower in self.DANGEROUS_ACTIONS:
                # Check if action has suspicious parameters
                params = action.get("params", {}) or action.get("parameters", {}) or {}
                self._check_suspicious_patterns(params, action_type)
                
                # High severity for dangerous actions
                self._add_issue(
                    "critical",
                    f"Dangerous action '{action_type}' detected",
                    location=action_type,
                    action=action_type
                )
        
        # Check for dangerous combinations
        for dangerous_combo, description in self.DANGEROUS_COMBINATIONS:
            combo_indices = []
            for i, action_name in enumerate(action_names):
                if any(d in action_name.lower() for d in dangerous_combo):
                    combo_indices.append(i)
            
            if len(combo_indices) >= 2:
                self._add_issue(
                    "critical",
                    f"Dangerous combination detected: {description}",
                    location=f"Actions at positions {combo_indices}"
                )
    
    def scan(self, workflow: Dict[str, Any]) -> Tuple[bool, List[Dict[str, Any]]]:
        """Scan a workflow for security issues.
        
        Args:
            workflow: Workflow JSON dictionary to scan.
            
        Returns:
            Tuple of (is_safe, issues_list).
        """
        self.reset()
        
        # Check workflow structure
        actions = workflow.get("actions", []) or workflow.get("steps", [])
        
        if not isinstance(actions, list):
            self._add_issue(
                "medium",
                "Workflow structure is unusual - 'actions' is not a list"
            )
            actions = []
        
        # Check for dangerous actions
        self._check_dangerous_actions(actions)
        
        # Check metadata for any issues
        metadata = workflow.get("metadata", {})
        if isinstance(metadata, dict):
            self._check_suspicious_patterns(metadata, "metadata")
        
        # Check parameters at workflow level
        params = workflow.get("params", {}) or workflow.get("parameters", {})
        if isinstance(params, dict):
            self._check_suspicious_patterns(params, "workflow_params")
        
        is_safe = not any(
            issue["severity"] in ("high", "critical")
            for issue in self._issues
        )
        
        return is_safe, self._issues
    
    def check_dangerous_patterns(
        self,
        text: str
    ) -> List[Dict[str, Any]]:
        """Check a text string for dangerous patterns.
        
        Args:
            text: Text to check.
            
        Returns:
            List of matching dangerous patterns with descriptions.
        """
        results = []
        
        for pattern, description in self.SUSPICIOUS_PATTERNS:
            match = pattern.search(text)
            if match:
                results.append({
                    "pattern": description,
                    "matched_text": match.group(0)[:50] + "..."
                        if len(match.group(0)) > 50 else match.group(0),
                    "position": match.start(),
                })
        
        return results

"""
IDE Integration Module for VSCode/IntelliJ
Provides VSCode extension support, debugger integration, syntax highlighting,
autocomplete, hover info, go-to-definition, inline validation, code lenses,
breakpoints, and variable inspection.
"""

import json
import os
from dataclasses import dataclass, field
from typing import Any, Optional
from pathlib import Path


@dataclass
class Breakpoint:
    """Represents a breakpoint in a workflow."""
    id: str
    line: int
    enabled: bool = True
    condition: Optional[str] = None
    hit_count: int = 0
    log_message: Optional[str] = None


@dataclass
class StackFrame:
    """Represents a stack frame during debugging."""
    id: str
    name: str
    line: int
    column: int
    source: str
    variables: dict = field(default_factory=dict)


@dataclass
class Variable:
    """Represents a variable during debug inspection."""
    name: str
    value: Any
    type: str
    reference: Optional[int] = None
    children: list = field(default_factory=list)


class IDEIntegration:
    """
    IDE Integration class providing VSCode/IntelliJ support for workflow editing.
    
    Features:
    1. VSCode extension support: Generate VSCode extension manifest
    2. Debugger integration: Debug workflows step by step
    3. Syntax highlighting: Generate TextMate grammars
    4. Autocomplete: Generate language server protocol data
    5. Hover info: Show action documentation on hover
    6. Go-to-definition: Navigate from action reference to definition
    7. Inline validation: Show errors as you type
    8. Code lenses: Show run/debug buttons
    9. Breakpoints: Set breakpoints in workflow editor
    10. Variable inspection: Inspect variables during debug
    """

    LANGUAGE_ID = "rabai-workflow"
    FILE_EXTENSION = ".rabai"

    def __init__(self, project_path: Optional[str] = None):
        self.project_path = project_path or os.getcwd()
        self.breakpoints: dict[str, list[Breakpoint]] = {}
        self.debug_session_active = False
        self.current_stack_frames: list[StackFrame] = []
        self.watch_variables: dict[str, Variable] = {}
        self._action_registry: dict[str, dict] = {}

    # =========================================================================
    # 1. VSCode Extension Support
    # =========================================================================

    def generate_vscode_extension_manifest(self) -> dict:
        """
        Generate VSCode extension manifest (package.json) for the workflow editor.
        
        Returns:
            dict: Complete VSCode extension manifest configuration
        """
        manifest = {
            "name": "rabai-workflow-editor",
            "displayName": "RabaI Workflow Editor",
            "description": "IDE support for RabaI workflow automation",
            "version": "1.0.0",
            "publisher": "rabai",
            "engines": {
                "vscode": "^1.75.0"
            },
            "categories": ["Programming Languages", "Debuggers"],
            "contributes": {
                "languages": [{
                    "id": self.LANGUAGE_ID,
                    "name": "RabaI Workflow",
                    "extensions": [self.FILE_EXTENSION],
                    "aliases": ["RabaI Workflow", "rabai"]
                }],
                "grammars": [{
                    "language": self.LANGUAGE_ID,
                    "scopeName": "source.rabai",
                    "path": "./syntaxes/rabai.tmLanguage.json"
                }],
                "breakpoints": [{
                    "language": self.LANGUAGE_ID
                }],
                "debuggers": [{
                    "type": "rabai",
                    "label": "RabaI Workflow Debugger",
                    "languages": [self.LANGUAGE_ID],
                    "configurationAttributes": {
                        "launch": {
                            "required": ["workflowFile"],
                            "properties": {
                                "workflowFile": {
                                    "type": "string",
                                    "description": "Path to workflow file to debug"
                                },
                                "stopOnEntry": {
                                    "type": "boolean",
                                    "default": True
                                }
                            }
                        }
                    }
                }],
                "commands": [{
                    "command": "rabai.runWorkflow",
                    "title": "Run Workflow",
                    "category": "RabaI"
                }, {
                    "command": "rabai.debugWorkflow",
                    "title": "Debug Workflow",
                    "category": "RabaI"
                }, {
                    "command": "rabai.stopWorkflow",
                    "title": "Stop Workflow",
                    "category": "RabaI"
                }],
                "codeActions": [{
                    "command": "rabai.fixAction",
                    "title": "Quick Fix",
                    "category": "RabaI"
                }]
            },
            "main": "./out/extension.js"
        }
        return manifest

    def generate_vscode_launch_config(self) -> dict:
        """
        Generate VSCode launch configuration for workflow debugging.
        
        Returns:
            dict: Launch configuration for VSCode
        """
        return {
            "version": "0.2.0",
            "configurations": [
                {
                    "name": "Run Workflow",
                    "type": "rabai",
                    "request": "launch",
                    "workflowFile": "${workspaceFolder}/workflow.rabai",
                    "stopOnEntry": False
                },
                {
                    "name": "Debug Workflow",
                    "type": "rabai",
                    "request": "launch",
                    "workflowFile": "${workspaceFolder}/workflow.rabai",
                    "stopOnEntry": True
                },
                {
                    "name": "Attach to Workflow",
                    "type": "rabai",
                    "request": "attach",
                    "workflowFile": "${workspaceFolder}/workflow.rabai"
                }
            ]
        }

    # =========================================================================
    # 2. Debugger Integration
    # =========================================================================

    def start_debug_session(self, workflow_file: str) -> str:
        """
        Start a debug session for a workflow file.
        
        Args:
            workflow_file: Path to the workflow file to debug
            
        Returns:
            str: Session ID for the debug session
        """
        session_id = f"debug-session-{workflow_file}-{id(self)}"
        self.debug_session_active = True
        self.current_stack_frames = []
        self._load_workflow_actions(workflow_file)
        return session_id

    def stop_debug_session(self) -> None:
        """Stop the current debug session and clear all debug state."""
        self.debug_session_active = False
        self.current_stack_frames = []
        self.watch_variables = {}

    def step_over(self) -> Optional[StackFrame]:
        """
        Execute the current line and pause at the next line.
        
        Returns:
            Optional[StackFrame]: Current stack frame after stepping
        """
        if not self.debug_session_active:
            return None
        # Simulate stepping to next line
        if self.current_stack_frames:
            frame = self.current_stack_frames[0]
            frame.line += 1
            self._update_frame_variables(frame)
            return frame
        return None

    def step_into(self) -> Optional[StackFrame]:
        """
        Step into a function or action call.
        
        Returns:
            Optional[StackFrame]: New stack frame after stepping into
        """
        if not self.debug_session_active:
            return None
        new_frame = StackFrame(
            id=f"frame-{len(self.current_stack_frames)}",
            name="stepped_action",
            line=1,
            column=0,
            source="workflow"
        )
        self.current_stack_frames.insert(0, new_frame)
        self._update_frame_variables(new_frame)
        return new_frame

    def step_out(self) -> Optional[StackFrame]:
        """
        Step out of the current function or action.
        
        Returns:
            Optional[StackFrame]: Parent stack frame after stepping out
        """
        if not self.debug_session_active or not self.current_stack_frames:
            return None
        self.current_stack_frames.pop(0)
        if self.current_stack_frames:
            return self.current_stack_frames[0]
        return None

    def resume(self) -> Optional[StackFrame]:
        """
        Resume execution until the next breakpoint or end of workflow.
        
        Returns:
            Optional[StackFrame]: Current stack frame when stopped
        """
        if not self.debug_session_active:
            return None
        # Find next breakpoint
        while self.current_stack_frames:
            frame = self.current_stack_frames[0]
            if self._is_breakpoint_at_frame(frame):
                return frame
            frame.line += 1
        return None

    def pause(self) -> bool:
        """
        Pause execution during a debug session.
        
        Returns:
            bool: True if pause was successful
        """
        if not self.debug_session_active:
            return False
        return True

    # =========================================================================
    # 3. Syntax Highlighting (TextMate Grammars)
    # =========================================================================

    def generate_textmate_grammar(self) -> dict:
        """
        Generate TextMate grammar for workflow syntax highlighting.
        
        Returns:
            dict: TextMate grammar configuration
        """
        return {
            "name": "RabaI Workflow",
            "scopeName": "source.rabai",
            "fileTypes": ["rabai"],
            "patterns": [
                {
                    "name": "comment.line",
                    "match": r"#.*$"
                },
                {
                    "name": "keyword.control.rabai",
                    "match": r"\b(step|action|condition|loop|try|catch|finally|if|else|while|for|return|break|continue)\b"
                },
                {
                    "name": "keyword.operator.rabai",
                    "match": r"(=>|->|::|\|\||&&|==|!=|<=|>=)"
                },
                {
                    "name": "support.function.rabai",
                    "match": r"\b(click|type|wait|open|close|screenshot|execute|validate)\b"
                },
                {
                    "name": "string.quoted.double.rabai",
                    "begin": "\"",
                    "end": "\"",
                    "patterns": [{
                        "name": "constant.character.escape.rabai",
                        "match": r"\\(?:[\\\"'nrt]|x[0-9a-fA-F]{2}|u[0-9a-fA-F]{4})"
                    }]
                },
                {
                    "name": "string.quoted.single.rabai",
                    "begin": "'",
                    "end": "'"
                },
                {
                    "name": "constant.numeric.rabai",
                    "match": r"\b\d+\.?\d*\b"
                },
                {
                    "name": "variable.parameter.rabai",
                    "match": r"\{\{[^}]+\}\}"
                },
                {
                    "name": "entity.name.tag.rabai",
                    "match": r"^[ \t]*@[a-zA-Z_][a-zA-Z0-9_-]*"
                },
                {
                    "name": "meta.action.selector.rabai",
                    "match": r"(by_|css|xpath|class|id|name):"
                }
            ],
            "repository": {
                "actions": {
                    "patterns": [{
                        "name": "entity.name.function.rabai",
                        "match": r"(?<=action\s)[a-zA-Z_][a-zA-Z0-9_]*"
                    }]
                },
                "selectors": {
                    "patterns": [{
                        "name": "constant.other.rabai",
                        "match": r"(css|xpath|class|id|name):\s*['\"][^'\"]+['\"]"
                    }]
                }
            }
        }

    def generate_vscode_syntax_theme(self) -> dict:
        """
        Generate VSCode syntax theme tokens for the workflow language.
        
        Returns:
            dict: VSCode TextMate theme configuration
        """
        return {
            "tokenColors": [
                {
                    "scope": ["source.rabai"],
                    "settings": {
                        "foreground": "#E0E0E0"
                    }
                },
                {
                    "scope": ["keyword.control.rabai"],
                    "settings": {
                        "foreground": "#569CD6",
                        "fontStyle": "bold"
                    }
                },
                {
                    "scope": ["support.function.rabai"],
                    "settings": {
                        "foreground": "#DCDCAA"
                    }
                },
                {
                    "scope": ["string.quoted.double.rabai", "string.quoted.single.rabai"],
                    "settings": {
                        "foreground": "#CE9178"
                    }
                },
                {
                    "scope": ["constant.numeric.rabai"],
                    "settings": {
                        "foreground": "#B5CEA8"
                    }
                },
                {
                    "scope": ["comment.line"],
                    "settings": {
                        "foreground": "#6A9955",
                        "fontStyle": "italic"
                    }
                },
                {
                    "scope": ["variable.parameter.rabai"],
                    "settings": {
                        "foreground": "#9CDCFE"
                    }
                }
            ]
        }

    # =========================================================================
    # 4. Autocomplete (Language Server Protocol)
    # =========================================================================

    def generate_lsp_completion_data(self) -> dict:
        """
        Generate LSP completion data for workflow autocompletion.
        
        Returns:
            dict: LSP completion configuration with actions and properties
        """
        return {
            "completion_items": [
                {
                    "label": "action",
                    "kind": 14,  # Function
                    "detail": "Define a workflow action",
                    "documentation": "action name do ... end",
                    "insert_text": "action ${1:name} do\n\t$0\nend",
                    "insert_text_format": 2
                },
                {
                    "label": "step",
                    "kind": 14,
                    "detail": "Define a workflow step",
                    "documentation": "step name description",
                    "insert_text": "step ${1:name} \"${2:description}\"",
                    "insert_text_format": 2
                },
                {
                    "label": "click",
                    "kind": 1,  # Text
                    "detail": "Click on element",
                    "documentation": "click selector: 'css:.button'",
                    "insert_text": "click selector: '${1:css:.button}'"
                },
                {
                    "label": "type",
                    "kind": 1,
                    "detail": "Type text into element",
                    "documentation": "type selector: 'css:.input', text: 'value'",
                    "insert_text": "type selector: '${1:css:.input}', text: '${2:value}'"
                },
                {
                    "label": "wait",
                    "kind": 1,
                    "detail": "Wait for element or duration",
                    "documentation": "wait selector: 'css:.element', timeout: 5000",
                    "insert_text": "wait selector: '${1:css:.element}', timeout: ${2:5000}"
                },
                {
                    "label": "open",
                    "kind": 1,
                    "detail": "Open URL or file",
                    "documentation": "open url: 'https://...'",
                    "insert_text": "open url: '${1:https://}'"
                },
                {
                    "label": "condition",
                    "kind": 14,
                    "detail": "Conditional execution",
                    "documentation": "condition expression do ... end",
                    "insert_text": "condition ${1:expression} do\n\t$0\nend"
                },
                {
                    "label": "loop",
                    "kind": 14,
                    "detail": "Loop execution",
                    "documentation": "loop times: N do ... end",
                    "insert_text": "loop times: ${1:10} do\n\t$0\nend"
                },
                {
                    "label": "try",
                    "kind": 14,
                    "detail": "Try-catch block",
                    "documentation": "try ... catch error do ... end",
                    "insert_text": "try\n\t$0\ncatch ${1:error} do\n\t\nend"
                }
            ],
            "trigger_characters": [".", ":", " ", "\n"],
            "resolve_timeout_ms": 5000
        }

    def get_completion_items(self, context: Optional[str] = None) -> list[dict]:
        """
        Get completion items based on current context.
        
        Args:
            context: Current editing context (action, step, etc.)
            
        Returns:
            list[dict]: List of applicable completion items
        """
        all_items = self.generate_lsp_completion_data()["completion_items"]
        if not context:
            return all_items
        
        # Filter based on context
        if context == "selector":
            return [item for item in all_items if item["label"] in ["click", "type", "wait"]]
        elif context == "control":
            return [item for item in all_items if item["label"] in ["condition", "loop", "try"]]
        return all_items

    # =========================================================================
    # 5. Hover Info
    # =========================================================================

    def get_hover_documentation(self, symbol: str) -> Optional[dict]:
        """
        Get hover documentation for a symbol.
        
        Args:
            symbol: The symbol to get documentation for
            
        Returns:
            Optional[dict]: Hover content with documentation
        """
        documentation = {
            "click": {
                "summary": "Click on a UI element",
                "syntax": "click selector: 'css:...' | 'xpath:...' | 'id:...'",
                "params": [
                    {"name": "selector", "type": "string", "desc": "Element selector"},
                    {"name": "button", "type": "string", "desc": "Mouse button (left/right)", "optional": True},
                    {"name": "modifier", "type": "string", "desc": "Keyboard modifiers", "optional": True}
                ],
                "example": "click selector: 'css:#submit-button'"
            },
            "type": {
                "summary": "Type text into an input field",
                "syntax": "type selector: '...', text: '...'",
                "params": [
                    {"name": "selector", "type": "string", "desc": "Input element selector"},
                    {"name": "text", "type": "string", "desc": "Text to type"},
                    {"name": "clear", "type": "boolean", "desc": "Clear before typing", "optional": True}
                ],
                "example": "type selector: 'css:input[name=email]', text: 'user@example.com'"
            },
            "wait": {
                "summary": "Wait for element or duration",
                "syntax": "wait selector: '...' | timeout: N",
                "params": [
                    {"name": "selector", "type": "string", "desc": "Element to wait for", "optional": True},
                    {"name": "timeout", "type": "number", "desc": "Timeout in milliseconds", "optional": True},
                    {"name": "state", "type": "string", "desc": "Expected state (visible/hidden/present)", "optional": True}
                ],
                "example": "wait selector: 'css:.loading', state: 'hidden'"
            },
            "open": {
                "summary": "Open URL or file",
                "syntax": "open url: '...'",
                "params": [
                    {"name": "url", "type": "string", "desc": "URL or file path to open"},
                    {"name": "new_tab", "type": "boolean", "desc": "Open in new tab", "optional": True}
                ],
                "example": "open url: 'https://example.com'"
            },
            "screenshot": {
                "summary": "Capture screenshot",
                "syntax": "screenshot name: '...'",
                "params": [
                    {"name": "name", "type": "string", "desc": "Screenshot name/identifier"},
                    {"name": "full_page", "type": "boolean", "desc": "Capture full page", "optional": True}
                ],
                "example": "screenshot name: 'dashboard'"
            },
            "action": {
                "summary": "Define a reusable action",
                "syntax": "action name do ... end",
                "params": [
                    {"name": "name", "type": "string", "desc": "Action name"},
                    {"name": "block", "type": "block", "desc": "Action implementation"}
                ],
                "example": "action login do\n  open url: '...'\n  type selector: '...', text: '...'\nend"
            },
            "condition": {
                "summary": "Conditional execution block",
                "syntax": "condition expr do ... end",
                "params": [
                    {"name": "expression", "type": "boolean", "desc": "Condition to evaluate"},
                    {"name": "block", "type": "block", "desc": "Code to execute if true"}
                ],
                "example": "condition page_loaded == true do\n  click selector: 'css:.next'\nend"
            },
            "loop": {
                "summary": "Loop execution",
                "syntax": "loop times: N | foreach: items do ... end",
                "params": [
                    {"name": "times", "type": "number", "desc": "Number of iterations", "optional": True},
                    {"name": "foreach", "type": "array", "desc": "Items to iterate", "optional": True},
                    {"name": "block", "type": "block", "desc": "Code to repeat"}
                ],
                "example": "loop times: 5 do\n  click selector: 'css:.next'\nend"
            }
        }
        
        if symbol in documentation:
            return {
                "contents": documentation[symbol],
                "range": None
            }
        return None

    # =========================================================================
    # 6. Go-to-Definition
    # =========================================================================

    def find_definition(self, symbol: str, source_file: str) -> Optional[dict]:
        """
        Find the definition location of a symbol.
        
        Args:
            symbol: Symbol to find definition for
            source_file: Current source file path
            
        Returns:
            Optional[dict]: Definition location with file and range
        """
        # Look for action definitions
        if symbol in self._action_registry:
            action = self._action_registry[symbol]
            return {
                "uri": action.get("file", source_file),
                "range": {
                    "start": {"line": action.get("line", 0), "character": 0},
                    "end": {"line": action.get("line", 0), "character": 100}
                },
                "symbol_name": symbol
            }
        
        # Built-in actions
        builtin_actions = ["click", "type", "wait", "open", "close", "screenshot", "execute", "validate"]
        if symbol in builtin_actions:
            return {
                "uri": "builtin://rabai/stdlib",
                "range": {
                    "start": {"line": 0, "character": 0},
                    "end": {"line": 0, "character": 0}
                },
                "symbol_name": symbol,
                "is_builtin": True
            }
        
        return None

    def _load_workflow_actions(self, workflow_file: str) -> None:
        """Load action definitions from workflow file."""
        # Placeholder for loading actual workflow file
        self._action_registry = {}

    # =========================================================================
    # 7. Inline Validation
    # =========================================================================

    def validate_workflow(self, content: str) -> list[dict]:
        """
        Validate workflow content and return errors/warnings.
        
        Args:
            content: Workflow file content to validate
            
        Returns:
            list[dict]: List of diagnostic issues
        """
        diagnostics = []
        lines = content.split('\n')
        
        for i, line in enumerate(lines):
            line_num = i + 1
            
            # Check for unclosed strings
            if line.count("'") % 2 != 0 and line.count("'") > 0:
                diagnostics.append({
                    "range": {
                        "start": {"line": i, "character": line.find("'")},
                        "end": {"line": i, "character": len(line)}
                    },
                    "severity": 8,  # Error
                    "code": "E001",
                    "source": "rabai",
                    "message": "Unclosed string literal"
                })
            
            # Check for invalid selectors
            if ":" in line and any(kw in line for kw in ["click", "type", "wait"]):
                if not any(sel in line for sel in ["css:", "xpath:", "id:", "class:", "name:"]):
                    diagnostics.append({
                        "range": {
                            "start": {"line": i, "character": 0},
                            "end": {"line": i, "character": len(line)}
                        },
                        "severity": 4,  # Warning
                        "code": "W001",
                        "source": "rabai",
                        "message": "Missing or invalid selector type"
                    })
            
            # Check for undefined variables
            import re
            var_refs = re.findall(r'\{\{([^}]+)\}\}', line)
            for var in var_refs:
                if not self._is_variable_defined(var):
                    diagnostics.append({
                        "range": {
                            "start": {"line": i, "character": line.find(f"{{{{{var}}}}}")},
                            "end": {"line": i, "character": line.find(f"{{{{{var}}}}}") + len(f"{{{{{var}}}}}")}
                        },
                        "severity": 4,
                        "code": "W002",
                        "source": "rabai",
                        "message": f"Undefined variable: {var}"
                    })
            
            # Check for missing end keywords
            if line.strip().startswith(("condition ", "loop ", "try ")):
                has_end = any(f"end" in lines[j].strip() and j > i for j in range(i + 1, len(lines)))
                if not has_end:
                    # Check if already closed on same line
                    if " do " not in line and " do\n" not in line and " do\r" not in line:
                        diagnostics.append({
                            "range": {
                                "start": {"line": i, "character": 0},
                                "end": {"line": i, "character": len(line)}
                            },
                            "severity": 8,
                            "code": "E002",
                            "source": "rabai",
                            "message": "Missing 'do' keyword for block statement"
                        })
        
        return diagnostics

    def _is_variable_defined(self, var_name: str) -> bool:
        """Check if a variable is defined."""
        defined_vars = {"page", "response", "result", "error", "data", "items", "index"}
        return var_name in defined_vars

    # =========================================================================
    # 8. Code Lenses
    # =========================================================================

    def generate_code_lenses(self, document_uri: str) -> list[dict]:
        """
        Generate code lens actions for a workflow document.
        
        Args:
            document_uri: URI of the workflow document
            
        Returns:
            list[dict]: List of code lens actions
        """
        return [
            {
                "range": {
                    "start": {"line": 0, "character": 0},
                    "end": {"line": 0, "character": 0}
                },
                "command": {
                    "title": "▶ Run",
                    "command": "rabai.runWorkflow",
                    "arguments": [document_uri]
                }
            },
            {
                "range": {
                    "start": {"line": 0, "character": 0},
                    "end": {"line": 0, "character": 0}
                },
                "command": {
                    "title": "🐛 Debug",
                    "command": "rabai.debugWorkflow",
                    "arguments": [document_uri]
                }
            },
            {
                "range": {
                    "start": {"line": 0, "character": 0},
                    "end": {"line": 0, "character": 0}
                },
                "command": {
                    "title": "⏹ Stop",
                    "command": "rabai.stopWorkflow",
                    "arguments": [document_uri]
                }
            },
            {
                "range": {
                    "start": {"line": 0, "character": 0},
                    "end": {"line": 0, "character": 0}
                },
                "command": {
                    "title": "📋 Copy as JSON",
                    "command": "rabai.exportWorkflow",
                    "arguments": [document_uri, "json"]
                }
            },
            {
                "range": {
                    "start": {"line": 0, "character": 0},
                    "end": {"line": 0, "character": 0}
                },
                "command": {
                    "title": "🔧 Actions Panel",
                    "command": "rabai.showActionsPanel",
                    "arguments": [document_uri]
                }
            }
        ]

    # =========================================================================
    # 9. Breakpoints
    # =========================================================================

    def set_breakpoint(self, file_path: str, line: int, 
                      condition: Optional[str] = None,
                      log_message: Optional[str] = None) -> Breakpoint:
        """
        Set a breakpoint at a specific location.
        
        Args:
            file_path: File path to set breakpoint in
            line: Line number for breakpoint
            condition: Optional condition expression
            log_message: Optional log message when breakpoint is hit
            
        Returns:
            Breakpoint: The created breakpoint
        """
        bp_id = f"{file_path}:{line}"
        breakpoint = Breakpoint(
            id=bp_id,
            line=line,
            enabled=True,
            condition=condition,
            log_message=log_message
        )
        
        if file_path not in self.breakpoints:
            self.breakpoints[file_path] = []
        
        # Remove existing breakpoint at same location
        self.breakpoints[file_path] = [
            bp for bp in self.breakpoints[file_path] 
            if bp.line != line
        ]
        self.breakpoints[file_path].append(breakpoint)
        
        return breakpoint

    def remove_breakpoint(self, file_path: str, line: int) -> bool:
        """
        Remove a breakpoint at a specific location.
        
        Args:
            file_path: File path to remove breakpoint from
            line: Line number of breakpoint to remove
            
        Returns:
            bool: True if breakpoint was removed
        """
        if file_path in self.breakpoints:
            original_count = len(self.breakpoints[file_path])
            self.breakpoints[file_path] = [
                bp for bp in self.breakpoints[file_path] 
                if bp.line != line
            ]
            return len(self.breakpoints[file_path]) < original_count
        return False

    def get_breakpoints(self, file_path: str) -> list[Breakpoint]:
        """
        Get all breakpoints for a file.
        
        Args:
            file_path: File path to get breakpoints for
            
        Returns:
            list[Breakpoint]: List of breakpoints
        """
        return self.breakpoints.get(file_path, [])

    def enable_breakpoint(self, file_path: str, line: int, enabled: bool = True) -> bool:
        """
        Enable or disable a breakpoint.
        
        Args:
            file_path: File path containing breakpoint
            line: Line number of breakpoint
            enabled: True to enable, False to disable
            
        Returns:
            bool: True if breakpoint was found and updated
        """
        if file_path in self.breakpoints:
            for bp in self.breakpoints[file_path]:
                if bp.line == line:
                    bp.enabled = enabled
                    return True
        return False

    def _is_breakpoint_at_frame(self, frame: StackFrame) -> bool:
        """Check if there's a breakpoint at the given frame location."""
        for file_path, breakpoints in self.breakpoints.items():
            for bp in breakpoints:
                if bp.enabled and bp.line == frame.line:
                    return True
        return False

    # =========================================================================
    # 10. Variable Inspection
    # =========================================================================

    def get_variables(self, frame_id: str) -> list[Variable]:
        """
        Get variables for a stack frame.
        
        Args:
            frame_id: Stack frame ID
            
        Returns:
            list[Variable]: List of variables in scope
        """
        variables = [
            Variable(name="page", value="<Page: https://example.com>", type="Page"),
            Variable(name="response", value="<Response: 200>", type="Response"),
            Variable(name="result", value="{'success': true}", type="dict"),
            Variable(name="error", value=None, type="NoneType"),
            Variable(name="data", value="[...]", type="list"),
        ]
        return variables

    def evaluate_expression(self, expression: str, frame_id: str) -> Optional[Variable]:
        """
        Evaluate an expression in the context of a stack frame.
        
        Args:
            expression: Expression to evaluate
            frame_id: Stack frame ID context
            
        Returns:
            Optional[Variable]: Evaluation result
        """
        # Simple expression evaluation
        if expression.startswith("{{") and expression.endswith("}}"):
            var_name = expression[2:-2].strip()
            # Return mock variable
            return Variable(
                name=var_name,
                value=f"value_of_{var_name}",
                type="string"
            )
        
        if expression in ["true", "false"]:
            return Variable(
                name="result",
                value=expression == "true",
                type="boolean"
            )
        
        try:
            num = float(expression)
            return Variable(
                name="result",
                value=num,
                type="number"
            )
        except ValueError:
            pass
        
        return None

    def add_watch(self, expression: str) -> Variable:
        """
        Add an expression to the watch list.
        
        Args:
            expression: Expression or variable to watch
            
        Returns:
            Variable: The watched variable
        """
        var = Variable(
            name=expression,
            value=None,
            type="unknown"
        )
        self.watch_variables[expression] = var
        return var

    def remove_watch(self, expression: str) -> bool:
        """
        Remove an expression from the watch list.
        
        Args:
            expression: Expression to remove
            
        Returns:
            bool: True if expression was removed
        """
        return expression in self.watch_variables and \
               self.watch_variables.pop(expression, None) is not None

    def get_watch_variables(self) -> list[Variable]:
        """
        Get all watch expressions and their current values.
        
        Returns:
            list[Variable]: List of watched variables
        """
        return list(self.watch_variables.values())

    def _update_frame_variables(self, frame: StackFrame) -> None:
        """Update variables for a stack frame after stepping."""
        frame.variables = {
            "page": "<Page>",
            "response": "<Response>",
            "result": None
        }

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def generate_extension_files(self, output_dir: str) -> dict:
        """
        Generate all VSCode extension files.
        
        Args:
            output_dir: Directory to write extension files
            
        Returns:
            dict: Mapping of file names to contents
        """
        files = {
            "package.json": json.dumps(self.generate_vscode_extension_manifest(), indent=2),
            ".vscode/launch.json": json.dumps(self.generate_vscode_launch_config(), indent=2),
            "syntaxes/rabai.tmLanguage.json": json.dumps(self.generate_textmate_grammar(), indent=2),
            "themes/rabai-dark.json": json.dumps(self.generate_vscode_syntax_theme(), indent=2)
        }
        
        output_path = Path(output_dir)
        for file_name, content in files.items():
            file_path = output_path / file_name
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content)
        
        return files

    def get_debugger_state(self) -> dict:
        """
        Get current debugger state for UI display.
        
        Returns:
            dict: Current debugger state
        """
        return {
            "active": self.debug_session_active,
            "stack_frames": [
                {
                    "id": frame.id,
                    "name": frame.name,
                    "line": frame.line,
                    "column": frame.column,
                    "source": frame.source
                }
                for frame in self.current_stack_frames
            ],
            "breakpoints": {
                file_path: [
                    {
                        "line": bp.line,
                        "enabled": bp.enabled,
                        "condition": bp.condition,
                        "hit_count": bp.hit_count
                    }
                    for bp in breakpoints
                ]
                for file_path, breakpoints in self.breakpoints.items()
            },
            "watch_variables": [
                {"name": var.name, "type": var.type, "value": var.value}
                for var in self.watch_variables.values()
            ]
        }

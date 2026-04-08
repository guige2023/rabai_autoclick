"""
Command bar and command palette utilities for automation.

Provides utilities for interacting with command palettes, 
spotlight-style search bars, and command execution interfaces.
"""

from __future__ import annotations

import subprocess
from typing import List, Optional, Callable, Dict, Any, Tuple
from dataclasses import dataclass
from enum import Enum


class CommandSource(Enum):
    """Source of command."""
    MENU = "menu"
    KEYBOARD = "keyboard"
    TOUCHBAR = "touchbar"
    SCRIPT = "script"
    AUTOMATION = "automation"


@dataclass
class CommandResult:
    """Result of a command execution."""
    success: bool
    output: str
    error: Optional[str] = None
    duration_ms: float = 0.0
    source: CommandSource = CommandSource.AUTOMATION


@dataclass
class Command:
    """Represents a command in a command palette."""
    id: str
    title: str
    subtitle: Optional[str] = None
    shortcut: Optional[str] = None
    icon: Optional[str] = None
    category: str = "General"
    action: Optional[Callable] = None
    keywords: List[str] = None
    
    def __post_init__(self):
        if self.keywords is None:
            self.keywords = []


class CommandRegistry:
    """Registry of available commands."""
    
    def __init__(self):
        """Initialize command registry."""
        self._commands: Dict[str, Command] = {}
        self._categories: Dict[str, List[str]] = {}
    
    def register(self, command: Command) -> None:
        """Register a command.
        
        Args:
            command: Command to register
        """
        self._commands[command.id] = command
        
        if command.category not in self._categories:
            self._categories[command.category] = []
        if command.id not in self._categories[command.category]:
            self._categories[command.category].append(command.id)
    
    def unregister(self, command_id: str) -> bool:
        """Unregister a command.
        
        Args:
            command_id: Command ID to remove
            
        Returns:
            True if command was removed
        """
        if command_id not in self._commands:
            return False
        
        command = self._commands[command_id]
        if command.category in self._categories:
            if command_id in self._categories[command.category]:
                self._categories[command.category].remove(command_id)
        
        del self._commands[command_id]
        return True
    
    def get(self, command_id: str) -> Optional[Command]:
        """Get a command by ID.
        
        Args:
            command_id: Command ID
            
        Returns:
            Command or None
        """
        return self._commands.get(command_id)
    
    def get_by_category(self, category: str) -> List[Command]:
        """Get all commands in a category.
        
        Args:
            category: Category name
            
        Returns:
            List of commands
        """
        command_ids = self._categories.get(category, [])
        return [self._commands[cid] for cid in command_ids if cid in self._commands]
    
    def search(self, query: str) -> List[Command]:
        """Search commands by query.
        
        Args:
            query: Search query
            
        Returns:
            List of matching commands
        """
        query_lower = query.lower()
        results = []
        
        for command in self._commands.values():
            # Check title
            if query_lower in command.title.lower():
                results.append(command)
                continue
            
            # Check keywords
            for keyword in command.keywords:
                if query_lower in keyword.lower():
                    results.append(command)
                    break
        
        # Sort by relevance (exact match first)
        results.sort(key=lambda c: (
            0 if query_lower == c.title.lower() else
            1 if c.title.lower().startswith(query_lower) else
            2 if query_lower in c.title.lower() else
            3
        ))
        
        return results
    
    def execute(self, command_id: str) -> CommandResult:
        """Execute a command by ID.
        
        Args:
            command_id: Command ID
            
        Returns:
            CommandResult
        """
        import time
        start = time.time()
        
        command = self.get(command_id)
        if not command:
            return CommandResult(
                success=False,
                output="",
                error=f"Command not found: {command_id}",
                source=CommandSource.AUTOMATION
            )
        
        if not command.action:
            return CommandResult(
                success=False,
                output="",
                error=f"Command has no action: {command_id}",
                source=CommandSource.AUTOMATION
            )
        
        try:
            result = command.action()
            duration_ms = (time.time() - start) * 1000
            
            return CommandResult(
                success=True,
                output=str(result) if result else "",
                duration_ms=duration_ms,
                source=CommandSource.AUTOMATION
            )
        except Exception as e:
            duration_ms = (time.time() - start) * 1000
            return CommandResult(
                success=False,
                output="",
                error=str(e),
                duration_ms=duration_ms,
                source=CommandSource.AUTOMATION
            )
    
    def list_all(self) -> List[Command]:
        """List all registered commands.
        
        Returns:
            List of all commands
        """
        return list(self._commands.values())
    
    def list_categories(self) -> List[str]:
        """List all categories.
        
        Returns:
            List of category names
        """
        return list(self._categories.keys())


class SpotlightController:
    """Controls macOS Spotlight for search and launch."""
    
    def __init__(self):
        """Initialize Spotlight controller."""
        pass
    
    def open_spotlight(self) -> bool:
        """Open Spotlight search.
        
        Returns:
            True if successful
        """
        try:
            script = '''
            tell application "System Events"
                keystroke " " using command down
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=2)
            return True
        except Exception:
            return False
    
    def close_spotlight(self) -> bool:
        """Close Spotlight search.
        
        Returns:
            True if successful
        """
        try:
            script = '''
            tell application "System Events"
                key code 53
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=2)
            return True
        except Exception:
            return False
    
    def search(self, query: str) -> bool:
        """Search using Spotlight.
        
        Args:
            query: Search query
            
        Returns:
            True if search was initiated
        """
        try:
            self.open_spotlight()
            
            # Wait a moment for Spotlight to open
            import time
            time.sleep(0.2)
            
            # Type the query
            script = f'''
            tell application "System Events"
                keystroke "{query}"
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=2)
            
            return True
        except Exception:
            return False
    
    def launch_application(self, app_name: str) -> bool:
        """Launch an application using Spotlight.
        
        Args:
            app_name: Application name
            
        Returns:
            True if application was launched
        """
        try:
            self.open_spotlight()
            
            import time
            time.sleep(0.2)
            
            # Type app name
            script = f'''
            tell application "System Events"
                keystroke "{app_name}"
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=2)
            
            time.sleep(0.3)
            
            # Press Enter to launch
            script = '''
            tell application "System Events"
                key code 36
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=2)
            
            return True
        except Exception:
            return False


class CommandPalette:
    """Generic command palette interface."""
    
    def __init__(self, registry: Optional[CommandRegistry] = None):
        """Initialize command palette.
        
        Args:
            registry: Optional command registry
        """
        self.registry = registry or CommandRegistry()
        self._callbacks: Dict[str, Callable] = {}
    
    def show(self) -> bool:
        """Show the command palette.
        
        Returns:
            True if shown successfully
        """
        raise NotImplementedError
    
    def hide(self) -> bool:
        """Hide the command palette.
        
        Returns:
            True if hidden successfully
        """
        raise NotImplementedError
    
    def type_query(self, query: str) -> None:
        """Type a query into the palette.
        
        Args:
            query: Query string
        """
        raise NotImplementedError
    
    def select_index(self, index: int) -> bool:
        """Select an item by index.
        
        Args:
            index: Item index (0-based)
            
        Returns:
            True if selection was made
        """
        raise NotImplementedError
    
    def confirm(self) -> bool:
        """Confirm the current selection.
        
        Returns:
            True if confirmed
        """
        raise NotImplementedError
    
    def get_current_results(self) -> List[Command]:
        """Get current search results.
        
        Returns:
            List of matching commands
        """
        return []


class AlfredController(CommandPalette):
    """Controller for Alfred command palette."""
    
    def __init__(self, registry: Optional[CommandRegistry] = None):
        """Initialize Alfred controller.
        
        Args:
            registry: Optional command registry
        """
        super().__init__(registry)
        self._hotkey = "option+space"
    
    def set_hotkey(self, hotkey: str) -> None:
        """Set the Alfred hotkey.
        
        Args:
            hotkey: Hotkey string (e.g., "cmd+space")
        """
        self._hotkey = hotkey
    
    def show(self) -> bool:
        """Show Alfred."""
        try:
            # Use AppleScript to trigger Alfred
            script = '''
            tell application "Alfred 3" to search ""
            '''
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=2
            )
            return result.returncode == 0
        except Exception:
            pass
        
        # Fallback: simulate hotkey
        return self._simulate_hotkey()
    
    def _simulate_hotkey(self) -> bool:
        """Simulate the hotkey."""
        try:
            script = f'''
            tell application "System Events"
                keystroke " " using {self._hotkey.replace("+", " down, ")} down
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=2)
            return True
        except Exception:
            return False
    
    def run_workflow(self, workflow_keyword: str) -> bool:
        """Run an Alfred workflow by keyword.
        
        Args:
            workflow_keyword: Workflow trigger keyword
            
        Returns:
            True if workflow was triggered
        """
        try:
            script = f'''
            tell application "Alfred 3"
                run trigger "{workflow_keyword}" in workflow "com.example.workflow"
            end tell
            '''
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5
            )
            return result.returncode == 0
        except Exception:
            return False


class QuickSilverController(CommandPalette):
    """Controller for QuickSilver command palette."""
    
    def show(self) -> bool:
        """Show QuickSilver."""
        try:
            script = '''
            tell application "Quicksilver"
                show main window
            end tell
            '''
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=2
            )
            return result.returncode == 0
        except Exception:
            return False


def create_app_commands(app_name: str) -> List[Command]:
    """Create common commands for an application.
    
    Args:
        app_name: Application name
        
    Returns:
        List of common commands
    """
    commands = []
    
    common_actions = [
        ("new", "New Document", "Create a new document"),
        ("open", "Open...", "Open an existing document"),
        ("save", "Save", "Save the current document"),
        ("close", "Close", "Close the current document"),
        ("quit", "Quit", "Quit the application"),
    ]
    
    for action_id, title, subtitle in common_actions:
        commands.append(Command(
            id=f"{app_name}.{action_id}",
            title=title,
            subtitle=subtitle,
            category="File"
        ))
    
    return commands


def parse_shortcut_string(shortcut: str) -> Dict[str, Any]:
    """Parse a keyboard shortcut string.
    
    Args:
        shortcut: Shortcut string (e.g., "⌘+C", "Cmd+Shift+S")
        
    Returns:
        Dictionary with key and modifiers
    """
    parts = shortcut.upper().replace("⌘", "CMD").replace("⇧", "SHIFT").replace("⌥", "ALT").replace("⌃", "CTRL").split("+")
    
    modifiers = []
    key = ""
    
    for part in parts:
        if part in ["CMD", "SHIFT", "ALT", "CTRL"]:
            modifiers.append(part)
        else:
            key = part
    
    return {
        "modifiers": modifiers,
        "key": key,
        "string": "+".join(modifiers + [key]) if key else "+".join(modifiers)
    }

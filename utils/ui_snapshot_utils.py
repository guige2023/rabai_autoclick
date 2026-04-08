"""
UI snapshot utilities for state capture and comparison.

Provides full UI state capture including element trees,
screenshots, and state diffing for automation testing.
"""

from __future__ import annotations

import json
import time
import hashlib
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import os


@dataclass
class ElementSnapshot:
    """Single UI element snapshot."""
    role: str
    title: str
    value: str
    position: Tuple[int, int]
    size: Tuple[int, int]
    enabled: bool
    focused: bool
    children_count: int
    subrole: Optional[str] = None
    identifier: Optional[str] = None


@dataclass
class UISnapshot:
    """Complete UI state snapshot."""
    id: str
    timestamp: float
    app_bundle_id: Optional[str]
    app_name: str
    focused_window_title: str
    elements: List[ElementSnapshot]
    screenshot_path: Optional[str] = None
    hash: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SnapshotDiff:
    """Difference between two snapshots."""
    added: List[ElementSnapshot]
    removed: List[ElementSnapshot]
    changed: List[Tuple[ElementSnapshot, ElementSnapshot]]
    timestamp: float
    duration: float


def capture_snapshot(app_bundle_id: Optional[str] = None,
                    include_screenshot: bool = True) -> UISnapshot:
    """
    Capture complete UI snapshot.
    
    Args:
        app_bundle_id: Optional app to capture.
        include_screenshot: Whether to include screenshot.
        
    Returns:
        UISnapshot with current UI state.
    """
    import subprocess
    
    elements = []
    app_name = "Unknown"
    
    script = f'''
    tell application "System Events"
        {"set targetApp to first process whose bundle identifier is \"" + app_bundle_id + "\"" if app_bundle_id else "set targetApp to first process whose frontmost is true"}
        set appName to name of targetApp
        set frontWin to first window of targetApp
        set winTitle to title of frontWin
        set elemList to every UI element of frontWin
        set resultData to {{}}
        repeat with elem in elemList
            set elemRole to role of elem
            set elemTitle to title of elem
            set elemValue to value of elem
            set elemPos to position of elem
            set elemSize to size of elem
            set elemEnabled to enabled of elem
            set elemFocused to focused of elem
            set elemChildren to count of UI elements of elem
            set end of resultData to {{elemRole, elemTitle, elemValue, elemPos, elemSize, elemEnabled, elemFocused, elemChildren}}
        end repeat
        return {{appName, winTitle, resultData}}
    end tell
    '''
    
    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.stdout.strip():
            lines = result.stdout.strip().split('\n')
            if len(lines) >= 2:
                app_name = lines[0].strip()
                window_title = lines[1].strip()
    except Exception:
        pass
    
    screenshot_path = None
    if include_screenshot:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"/tmp/snapshot_{timestamp}.png"
            subprocess.run(
                ["screencapture", "-x", screenshot_path],
                check=True,
                capture_output=True
            )
        except Exception:
            screenshot_path = None
    
    snapshot = UISnapshot(
        id=f"snap_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        timestamp=time.time(),
        app_bundle_id=app_bundle_id,
        app_name=app_name,
        focused_window_title="",
        elements=elements,
        screenshot_path=screenshot_path
    )
    
    snapshot.hash = compute_snapshot_hash(snapshot)
    return snapshot


def compute_snapshot_hash(snapshot: UISnapshot) -> str:
    """
    Compute hash of snapshot state.
    
    Args:
        snapshot: Snapshot to hash.
        
    Returns:
        MD5 hash string.
    """
    state = {
        'app': snapshot.app_name,
        'window': snapshot.focused_window_title,
        'elements': [
            {
                'role': e.role,
                'title': e.title,
                'position': e.position,
                'size': e.size,
            }
            for e in snapshot.elements
        ]
    }
    json_str = json.dumps(state, sort_keys=True)
    return hashlib.md5(json_str.encode()).hexdigest()


def compare_snapshots(before: UISnapshot, after: UISnapshot) -> SnapshotDiff:
    """
    Compare two snapshots and return differences.
    
    Args:
        before: Earlier snapshot.
        after: Later snapshot.
        
    Returns:
        SnapshotDiff with all differences.
    """
    start = time.time()
    
    before_map = {
        (e.role, e.title, e.position): e
        for e in before.elements
    }
    after_map = {
        (e.role, e.title, e.position): e
        for e in after.elements
    }
    
    added = []
    removed = []
    changed = []
    
    for key, elem in after_map.items():
        if key not in before_map:
            added.append(elem)
    
    for key, elem in before_map.items():
        if key not in after_map:
            removed.append(elem)
    
    for key, after_elem in after_map.items():
        if key in before_map:
            before_elem = before_map[key]
            if (before_elem.value != after_elem.value or
                before_elem.enabled != after_elem.enabled or
                before_elem.focused != after_elem.focused):
                changed.append((before_elem, after_elem))
    
    return SnapshotDiff(
        added=added,
        removed=removed,
        changed=changed,
        timestamp=time.time(),
        duration=time.time() - start
    )


def save_snapshot(snapshot: UISnapshot, path: str) -> bool:
    """
    Save snapshot to file.
    
    Args:
        snapshot: Snapshot to save.
        path: Output file path.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        data = {
            'id': snapshot.id,
            'timestamp': snapshot.timestamp,
            'app_bundle_id': snapshot.app_bundle_id,
            'app_name': snapshot.app_name,
            'focused_window_title': snapshot.focused_window_title,
            'screenshot_path': snapshot.screenshot_path,
            'hash': snapshot.hash,
            'metadata': snapshot.metadata,
            'elements': [
                {
                    'role': e.role,
                    'title': e.title,
                    'value': e.value,
                    'position': list(e.position),
                    'size': list(e.size),
                    'enabled': e.enabled,
                    'focused': e.focused,
                    'children_count': e.children_count,
                    'subrole': e.subrole,
                    'identifier': e.identifier,
                }
                for e in snapshot.elements
            ]
        }
        
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        return True
    except Exception:
        return False


def load_snapshot(path: str) -> Optional[UISnapshot]:
    """
    Load snapshot from file.
    
    Args:
        path: Snapshot file path.
        
    Returns:
        UISnapshot if successful, None otherwise.
    """
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        
        elements = [
            ElementSnapshot(
                role=e['role'],
                title=e['title'],
                value=e['value'],
                position=tuple(e['position']),
                size=tuple(e['size']),
                enabled=e['enabled'],
                focused=e['focused'],
                children_count=e['children_count'],
                subrole=e.get('subrole'),
                identifier=e.get('identifier'),
            )
            for e in data.get('elements', [])
        ]
        
        return UISnapshot(
            id=data['id'],
            timestamp=data['timestamp'],
            app_bundle_id=data.get('app_bundle_id'),
            app_name=data['app_name'],
            focused_window_title=data.get('focused_window_title', ''),
            elements=elements,
            screenshot_path=data.get('screenshot_path'),
            hash=data.get('hash'),
            metadata=data.get('metadata', {})
        )
    except Exception:
        return None


def wait_for_snapshot_change(initial: UISnapshot,
                              timeout: float = 10.0,
                              poll_interval: float = 0.5) -> Optional[SnapshotDiff]:
    """
    Wait for UI snapshot to change.
    
    Args:
        initial: Initial snapshot to compare against.
        timeout: Maximum wait time in seconds.
        poll_interval: Time between checks.
        
    Returns:
        SnapshotDiff if changed, None on timeout.
    """
    start = time.time()
    
    while time.time() - start < timeout:
        current = capture_snapshot(include_screenshot=False)
        diff = compare_snapshots(initial, current)
        
        if diff.added or diff.removed or diff.changed:
            return diff
        
        time.sleep(poll_interval)
    
    return None

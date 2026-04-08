"""Browser action module for RabAI AutoClick.

Provides browser automation actions for web interaction.
"""

import subprocess
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class BrowserOpenAction(BaseAction):
    """Open URL in browser.
    
    Opens specified URL in default or specific browser.
    """
    action_type = "browser_open"
    display_name = "打开浏览器"
    description = "在浏览器中打开URL"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Open URL in browser.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: url, browser, new_tab, background.
        
        Returns:
            ActionResult with open status.
        """
        url = params.get('url', '')
        browser = params.get('browser', '')
        new_tab = params.get('new_tab', False)
        background = params.get('background', False)
        
        if not url:
            return ActionResult(success=False, message="url required")
        
        if not url.startswith('http'):
            url = 'https://' + url
        
        try:
            cmd = ['open']
            
            if browser:
                browser_map = {
                    'chrome': 'com.google.Chrome',
                    'firefox': 'org.mozilla.firefox',
                    'safari': 'com.apple.Safari',
                    'edge': 'com.microsoft.edgemac'
                }
                bundle_id = browser_map.get(browser.lower())
                if bundle_id:
                    cmd.extend(['-b', bundle_id])
            
            if new_tab:
                cmd.append('-t')
            
            if background:
                cmd.append('-g')
            
            cmd.append(url)
            
            result = subprocess.run(cmd, capture_output=True, timeout=10)
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"Opened: {url}",
                    data={'url': url, 'browser': browser}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Open failed: {result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Browser open timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Browser open error: {e}",
                data={'error': str(e)}
            )


class BrowserJavaScriptAction(BaseAction):
    """Execute JavaScript in browser.
    
    Runs JavaScript code in active browser tab.
    """
    action_type = "browser_javascript"
    display_name = "执行JavaScript"
    description = "在浏览器中执行JavaScript代码"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JavaScript.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: script, browser.
        
        Returns:
            ActionResult with execution result.
        """
        script = params.get('script', '')
        browser = params.get('browser', 'Safari')
        
        if not script:
            return ActionResult(success=False, message="script required")
        
        try:
            # Safari supports JavaScript execution via osascript
            if browser.lower() == 'safari':
                applescript = f'tell application "Safari" to do JavaScript "{script}" in front document'
                result = subprocess.run(
                    ['osascript', '-e', applescript],
                    capture_output=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    return ActionResult(
                        success=True,
                        message="JavaScript executed",
                        data={'result': result.stdout.decode().strip()}
                    )
                else:
                    return ActionResult(
                        success=False,
                        message=f"JavaScript failed: {result.stderr.decode()}"
                    )
            else:
                return ActionResult(
                    success=False,
                    message=f"JavaScript execution not supported for {browser}"
                )
                
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JavaScript error: {e}",
                data={'error': str(e)}
            )


class BrowserTabAction(BaseAction):
    """Manage browser tabs.
    
    Create, close, switch tabs.
    """
    action_type = "browser_tab"
    display_name = "浏览器标签页"
    description = "管理浏览器标签页"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage browser tabs.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: operation (new/close/switch), browser, url, tab_index.
        
        Returns:
            ActionResult with operation status.
        """
        operation = params.get('operation', 'new')
        browser = params.get('browser', 'Safari')
        url = params.get('url', '')
        tab_index = params.get('tab_index', None)
        
        try:
            if browser.lower() == 'safari':
                if operation == 'new':
                    script = 'tell application "Safari" to activate'
                    subprocess.run(['osascript', '-e', script], timeout=5)
                    
                    if url:
                        script = f'tell application "Safari" to open location "{url}"'
                        subprocess.run(['osascript', '-e', script], timeout=5)
                    
                    return ActionResult(
                        success=True,
                        message="New tab created",
                        data={'operation': operation, 'url': url}
                    )
                
                elif operation == 'close':
                    script = 'tell application "Safari" to close front window'
                    subprocess.run(['osascript', '-e', script], timeout=5)
                    return ActionResult(
                        success=True,
                        message="Tab closed",
                        data={'operation': operation}
                    )
                
                elif operation == 'switch' and tab_index is not None:
                    script = f'''
                    tell application "Safari"
                        activate
                        set index of front window to {tab_index}
                    end tell
                    '''
                    subprocess.run(['osascript', '-e', script], timeout=5)
                    return ActionResult(
                        success=True,
                        message=f"Switched to tab {tab_index}",
                        data={'operation': operation, 'tab_index': tab_index}
                    )
            
            return ActionResult(
                success=False,
                message=f"Operation not supported for {browser}"
            )
                
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Tab action error: {e}",
                data={'error': str(e)}
            )

"""Flow engine module for RabAI AutoClick.

Provides the FlowEngine class for executing automation workflows,
including support for pausing, resuming, and step callbacks.
"""

import json
import time
import threading
import logging
from typing import Any, Callable, Dict, List, Optional, Union

try:
    import jsonschema
    HAS_JSONSCHEMA = True
except ImportError:
    HAS_JSONSCHEMA = False

from .context import ContextManager
from .action_loader import ActionLoader
from .base_action import ActionResult


logger = logging.getLogger(__name__)


# Workflow JSON Schema for validation
WORKFLOW_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "version": {"type": "string"},
        "name": {"type": "string"},
        "description": {"type": "string"},
        "variables": {
            "type": "object",
            "additionalProperties": True
        },
        "steps": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "type"],
                "properties": {
                    "id": {"type": "string"},
                    "type": {"type": "string"},
                    "pre_delay": {"type": "number"},
                    "post_delay": {"type": "number"},
                    "output_var": {"type": "string"},
                    "next": {"type": "string"}
                }
            }
        }
    },
    "required": ["steps"]
}


class FlowEngine:
    """Executes automation workflows step by step.
    
    Manages workflow loading, execution state, callbacks, and
    supports pause/resume functionality.
    """
    
    def __init__(self, actions_dir: Optional[str] = None) -> None:
        """Initialize the flow engine.
        
        Args:
            actions_dir: Optional custom actions directory path.
        """
        self.context: ContextManager = ContextManager()
        self.action_loader: ActionLoader = ActionLoader(actions_dir)
        self.action_loader.load_all()
        
        self._workflow: Dict[str, Any] = {}
        self._current_step_index: int = 0
        self._is_running: bool = False
        self._is_paused: bool = False
        self._stop_requested: bool = False
        self._loop_counters: Dict[str, int] = {}

        # Thread safety lock for state flags
        self._state_lock: threading.RLock = threading.RLock()

        # Callbacks
        self._on_step_start: Optional[Callable[[Dict], None]] = None
        self._on_step_end: Optional[Callable[[Dict, ActionResult], None]] = None
        self._on_workflow_end: Optional[Callable[[bool], None]] = None
        self._on_error: Optional[Callable[[Dict, str], None]] = None
    
    def _validate_workflow(self, workflow: Dict[str, Any]) -> bool:
        """Validate a workflow dictionary against the schema.

        Args:
            workflow: Workflow dictionary to validate.

        Returns:
            True if valid, False otherwise.
        """
        if not HAS_JSONSCHEMA:
            logger.warning("jsonschema not installed, skipping workflow validation")
            return True
        try:
            jsonschema.validate(instance=workflow, schema=WORKFLOW_SCHEMA)
            return True
        except jsonschema.ValidationError as e:
            logger.error(f"工作流Schema验证失败: {e.message}")
            return False
        except jsonschema.SchemaError as e:
            logger.error(f"工作流Schema格式错误: {e.message}")
            return False

    def load_workflow(self, workflow_path: str) -> bool:
        """Load a workflow from a JSON file.

        Args:
            workflow_path: Path to the workflow JSON file.

        Returns:
            True if loaded successfully, False otherwise.
        """
        try:
            with open(workflow_path, 'r', encoding='utf-8') as f:
                workflow = json.load(f)

            if not self._validate_workflow(workflow):
                return False

            self._workflow = workflow
            if 'variables' in self._workflow:
                self.context.set_all(self._workflow['variables'])

            return True
        except Exception as e:
            logger.error(f"加载工作流失败: {e}")
            return False

    def load_workflow_from_dict(
        self,
        workflow: Dict[str, Any]
    ) -> bool:
        """Load a workflow from a dictionary.

        Args:
            workflow: Workflow dictionary.

        Returns:
            True if loaded successfully, False otherwise.
        """
        try:
            if not self._validate_workflow(workflow):
                return False

            self._workflow = workflow
            if 'variables' in self._workflow:
                self.context.set_all(self._workflow['variables'])
            return True
        except Exception as e:
            logger.error(f"加载工作流失败: {e}")
            return False
    
    def save_workflow(self, workflow_path: str) -> bool:
        """Save the current workflow to a JSON file.
        
        Args:
            workflow_path: Path to save the workflow JSON file.
            
        Returns:
            True if saved successfully, False otherwise.
        """
        try:
            self._workflow['variables'] = self.context.get_all()
            with open(workflow_path, 'w', encoding='utf-8') as f:
                json.dump(self._workflow, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            logger.error(f"保存工作流失败: {e}")
            return False
    
    def run(self) -> bool:
        """Execute the loaded workflow synchronously.

        Returns:
            True if workflow completed normally, False if stopped.
        """
        with self._state_lock:
            if self._is_running:
                return False
            self._is_running = True
            self._stop_requested = False
            self._is_paused = False
            self._current_step_index = 0
        
        steps: List[Dict[str, Any]] = self._workflow.get('steps', [])
        
        if not steps:
            with self._state_lock:
                self._is_running = False
            return False

        # Build step index map for quick lookup
        step_map: Dict[str, int] = {step['id']: i for i, step in enumerate(steps)}
        current_step: Optional[Dict[str, Any]] = steps[0]

        while current_step is not None:
            # Check stop request
            with self._state_lock:
                if self._stop_requested:
                    break
                paused = self._is_paused

            # Wait while paused (brief lock acquisitions to allow stop/pause changes)
            while paused:
                time.sleep(0.1)
                with self._state_lock:
                    if self._stop_requested:
                        paused = False
                    else:
                        paused = self._is_paused

            with self._state_lock:
                if self._stop_requested:
                    break

            result = self._execute_step(current_step)

            if not result.success:
                if self._on_error:
                    self._on_error(current_step, result.message)
                break

            # Determine next step
            if result.next_step_id is not None:
                if result.next_step_id in step_map:
                    current_step = steps[step_map[result.next_step_id]]
                else:
                    break
            else:
                current_id = current_step['id']
                next_index = step_map.get(current_id, -1) + 1
                if next_index < len(steps):
                    current_step = steps[next_index]
                else:
                    current_step = None

        with self._state_lock:
            self._is_running = False
            stopped = self._stop_requested

        if self._on_workflow_end:
            self._on_workflow_end(not stopped)

        return not stopped
    
    def run_async(
        self, 
        callback: Optional[Callable[[bool], None]] = None
    ) -> threading.Thread:
        """Execute the workflow in a background thread.
        
        Args:
            callback: Optional callback function(result: bool) called on completion.
            
        Returns:
            The background thread.
        """
        def _run() -> None:
            result: bool = self.run()
            if callback:
                callback(result)
        
        thread: threading.Thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        return thread
    
    def _execute_step(self, step: Dict[str, Any]) -> ActionResult:
        """Execute a single workflow step.
        
        Args:
            step: Step dictionary with type and parameters.
            
        Returns:
            ActionResult from the executed action.
        """
        step_type: str = step.get('type', '')
        start_time: float = time.time()
        
        if self._on_step_start:
            self._on_step_start(step)
        
        # Pre-delay
        pre_delay: float = step.get('pre_delay', 0)
        if pre_delay > 0:
            time.sleep(pre_delay)
        
        # Get action class
        action_class = self.action_loader.get_action(step_type)
        
        if not action_class:
            return ActionResult(
                success=False,
                message=f"未找到动作类型: {step_type}"
            )
        
        try:
            action = action_class()
            params: Dict[str, Any] = step.copy()
            # Remove metadata keys
            for key in ('type', 'id', 'next', 'pre_delay', 'post_delay'):
                params.pop(key, None)
            
            # Resolve variable references in params
            params = self.context.resolve_value(params)
            
            result: ActionResult = action.execute(self.context, params)
            
            # Check if stop was requested during action execution
            with self._state_lock:
                if self._stop_requested:
                    return ActionResult(
                        success=False,
                        message='Workflow stopped'
                    )
            
            result.duration = time.time() - start_time
            
            # Store output to context variable if specified
            if result.success and 'output_var' in step and result.data is not None:
                self.context.set(step['output_var'], result.data)
            
            # Post-delay
            post_delay: float = step.get('post_delay', 0)
            if post_delay > 0:
                time.sleep(post_delay)
            
            if self._on_step_end:
                self._on_step_end(step, result)
            
            return result
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"执行步骤失败: {str(e)}",
                duration=time.time() - start_time
            )
    
    def stop(self) -> None:
        """Stop the currently running workflow."""
        with self._state_lock:
            self._stop_requested = True
            self._is_paused = False

    def pause(self) -> None:
        """Pause the currently running workflow."""
        with self._state_lock:
            self._is_paused = True

    def resume(self) -> None:
        """Resume a paused workflow."""
        with self._state_lock:
            self._is_paused = False

    def is_running(self) -> bool:
        """Check if workflow is currently running.

        Returns:
            True if running, False otherwise.
        """
        with self._state_lock:
            return self._is_running

    def is_paused(self) -> bool:
        """Check if workflow is currently paused.

        Returns:
            True if paused, False otherwise.
        """
        with self._state_lock:
            return self._is_paused
    
    def get_current_step_index(self) -> int:
        """Get the index of the current step.
        
        Returns:
            Current step index (0-based).
        """
        return self._current_step_index
    
    def set_callbacks(
        self,
        on_step_start: Optional[Callable[[Dict], None]] = None,
        on_step_end: Optional[Callable[[Dict, ActionResult], None]] = None,
        on_workflow_end: Optional[Callable[[bool], None]] = None,
        on_error: Optional[Callable[[Dict, str], None]] = None
    ) -> None:
        """Set callback functions for workflow events.
        
        Args:
            on_step_start: Called when a step starts (step: Dict).
            on_step_end: Called when a step ends (step: Dict, result: ActionResult).
            on_workflow_end: Called when workflow ends (completed: bool).
            on_error: Called on step error (step: Dict, message: str).
        """
        self._on_step_start = on_step_start
        self._on_step_end = on_step_end
        self._on_workflow_end = on_workflow_end
        self._on_error = on_error
    
    def get_action_info(self) -> Dict[str, Dict[str, Any]]:
        """Get information about all registered actions.
        
        Returns:
            Dictionary mapping action types to their metadata.
        """
        return self.action_loader.get_action_info()

import json
import time
import threading
import logging
from typing import Dict, Any, Optional, Callable
from pathlib import Path
from .context import ContextManager
from .action_loader import ActionLoader
from .base_action import ActionResult

logger = logging.getLogger(__name__)


class FlowEngine:
    def __init__(self, actions_dir: str = None):
        self.context = ContextManager()
        self.action_loader = ActionLoader(actions_dir)
        self.action_loader.load_all()
        
        self._workflow: Dict[str, Any] = {}
        self._current_step_index: int = 0
        self._is_running: bool = False
        self._is_paused: bool = False
        self._stop_requested: bool = False
        self._loop_counters: Dict[str, int] = {}
        
        self._on_step_start: Optional[Callable] = None
        self._on_step_end: Optional[Callable] = None
        self._on_workflow_end: Optional[Callable] = None
        self._on_error: Optional[Callable] = None
    
    def load_workflow(self, workflow_path: str) -> bool:
        try:
            with open(workflow_path, 'r', encoding='utf-8') as f:
                self._workflow = json.load(f)
            
            if 'variables' in self._workflow:
                self.context.set_all(self._workflow['variables'])
            
            return True
        except Exception as e:
            logger.error(f"加载工作流失败: {e}")
            return False
    
    def load_workflow_from_dict(self, workflow: Dict[str, Any]) -> bool:
        try:
            self._workflow = workflow
            if 'variables' in self._workflow:
                self.context.set_all(self._workflow['variables'])
            return True
        except Exception as e:
            logger.error(f"加载工作流失败: {e}")
            return False
    
    def save_workflow(self, workflow_path: str) -> bool:
        try:
            self._workflow['variables'] = self.context.get_all()
            with open(workflow_path, 'w', encoding='utf-8') as f:
                json.dump(self._workflow, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            logger.error(f"保存工作流失败: {e}")
            return False
    
    def run(self) -> bool:
        if self._is_running:
            return False
        
        self._is_running = True
        self._stop_requested = False
        self._is_paused = False
        self._current_step_index = 0
        
        steps = self._workflow.get('steps', [])
        
        if not steps:
            self._is_running = False
            return False
        
        step_map = {step['id']: i for i, step in enumerate(steps)}
        current_step = steps[0]
        
        while current_step and not self._stop_requested:
            while self._is_paused and not self._stop_requested:
                time.sleep(0.1)
            
            if self._stop_requested:
                break
            
            result = self._execute_step(current_step)
            
            if not result.success:
                if self._on_error:
                    self._on_error(current_step, result.message)
                break
            
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
        
        self._is_running = False
        if self._on_workflow_end:
            self._on_workflow_end(not self._stop_requested)
        
        return not self._stop_requested
    
    def run_async(self, callback: Optional[Callable] = None) -> threading.Thread:
        def _run():
            result = self.run()
            if callback:
                callback(result)
        
        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        return thread
    
    def _execute_step(self, step: Dict[str, Any]) -> ActionResult:
        import time
        step_type = step.get('type')
        start_time = time.time()
        
        if self._on_step_start:
            self._on_step_start(step)
        
        pre_delay = step.get('pre_delay', 0)
        if pre_delay > 0:
            time.sleep(pre_delay)
        
        action_class = self.action_loader.get_action(step_type)
        
        if not action_class:
            return ActionResult(
                success=False,
                message=f"未找到动作类型: {step_type}"
            )
        
        try:
            action = action_class()
            params = step.copy()
            params.pop('type', None)
            params.pop('id', None)
            params.pop('next', None)
            params.pop('pre_delay', None)
            params.pop('post_delay', None)
            
            params = self.context.resolve_value(params)
            
            result = action.execute(self.context, params)
            
            result.duration = time.time() - start_time
            
            if result.success and 'output_var' in step and result.data is not None:
                self.context.set(step['output_var'], result.data)
            
            post_delay = step.get('post_delay', 0)
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
        self._stop_requested = True
        self._is_paused = False
    
    def pause(self) -> None:
        self._is_paused = True
    
    def resume(self) -> None:
        self._is_paused = False
    
    def is_running(self) -> bool:
        return self._is_running
    
    def is_paused(self) -> bool:
        return self._is_paused
    
    def get_current_step_index(self) -> int:
        return self._current_step_index
    
    def set_callbacks(self, 
                      on_step_start: Callable = None,
                      on_step_end: Callable = None,
                      on_workflow_end: Callable = None,
                      on_error: Callable = None) -> None:
        self._on_step_start = on_step_start
        self._on_step_end = on_step_end
        self._on_workflow_end = on_workflow_end
        self._on_error = on_error
    
    def get_action_info(self) -> Dict[str, dict]:
        return self.action_loader.get_action_info()

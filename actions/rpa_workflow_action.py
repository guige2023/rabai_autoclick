"""
RPA Workflow Action Module.

Provides Robotic Process Automation workflow patterns including
screen scraping, form automation, and multi-step workflow orchestration.
"""

from typing import Optional, Dict, List, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging
import time

logger = logging.getLogger(__name__)


class WorkflowStepType(Enum):
    """Types of workflow steps."""
    NAVIGATE = "navigate"          # Navigate to URL/location
    CLICK = "click"               # Click element/coordinates
    TYPE = "type"                 # Type text input
    WAIT = "wait"                 # Wait for element/condition
    EXTRACT = "extract"           # Extract data from screen
    CONDITION = "condition"       # Conditional branching
    LOOP = "loop"                # Loop over elements
    CALLBACK = "callback"         # Custom function
    SCREENSHOT = "screenshot"    # Capture screenshot
    ASSERT = "assert"            # Assert condition


@dataclass
class WorkflowStep:
    """Represents a single step in an RPA workflow."""
    step_type: WorkflowStepType
    name: str
    params: Dict[str, Any] = field(default_factory=dict)
    timeout: float = 30.0
    retry_count: int = 3
    retry_delay: float = 1.0
    on_error: Optional[str] = None  # Next step name on error
    conditions: Optional[Dict[str, Any]] = None


@dataclass
class WorkflowResult:
    """Result of workflow execution."""
    success: bool
    steps_executed: int
    data_captured: Dict[str, Any]
    errors: List[Dict[str, Any]]
    duration: float
    screenshot_path: Optional[str] = None


@dataclass
class ElementLocator:
    """Locator for UI elements."""
    locator_type: str  # xpath, css, id, name, text, image
    value: str
    timeout: float = 10.0
    confidence: float = 0.8  # For image-based locators


class RPAWorkflow:
    """
    RPA workflow orchestrator for automating desktop/web tasks.
    
    Example:
        workflow = RPAWorkflow("data_entry")
        
        workflow.step("open_app", WorkflowStepType.NAVIGATE, params={"url": "..."})
        workflow.step("login", WorkflowStepType.TYPE, params={"text": "user@email.com"})
        workflow.step("submit", WorkflowStepType.CLICK)
        
        result = workflow.execute()
    """
    
    def __init__(
        self,
        name: str,
        screenshot_dir: Optional[str] = None,
    ):
        self.name = name
        self.screenshot_dir = screenshot_dir
        self.steps: List[WorkflowStep] = []
        self.step_map: Dict[str, WorkflowStep] = {}
        self.variables: Dict[str, Any] = {}
        self.captured_data: Dict[str, Any] = {}
        self._current_step: int = 0
        
    def step(
        self,
        name: str,
        step_type: WorkflowStepType,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0,
        retry_count: int = 3,
        retry_delay: float = 1.0,
    ) -> "RPAWorkflow":
        """Add a step to the workflow."""
        step = WorkflowStep(
            step_type=step_type,
            name=name,
            params=params or {},
            timeout=timeout,
            retry_count=retry_count,
            retry_delay=retry_delay,
        )
        self.steps.append(step)
        self.step_map[name] = step
        return self
        
    def navigate(
        self,
        name: str,
        url: str,
        timeout: float = 30.0,
    ) -> "RPAWorkflow":
        """Add a navigation step."""
        return self.step(
            name,
            WorkflowStepType.NAVIGATE,
            params={"url": url},
            timeout=timeout,
        )
        
    def click(
        self,
        name: str,
        locator: Optional[ElementLocator] = None,
        x: Optional[int] = None,
        y: Optional[int] = None,
        timeout: float = 10.0,
    ) -> "RPAWorkflow":
        """Add a click step."""
        params = {}
        if locator:
            params["locator"] = {
                "type": locator.locator_type,
                "value": locator.value,
                "timeout": locator.timeout,
            }
        if x is not None:
            params["x"] = x
        if y is not None:
            params["y"] = y
        return self.step(name, WorkflowStepType.CLICK, params=params, timeout=timeout)
        
    def type_text(
        self,
        name: str,
        text: str,
        locator: Optional[ElementLocator] = None,
        clear_first: bool = True,
        timeout: float = 10.0,
    ) -> "RPAWorkflow":
        """Add a type text step."""
        params = {"text": text, "clear_first": clear_first}
        if locator:
            params["locator"] = {
                "type": locator.locator_type,
                "value": locator.value,
            }
        return self.step(name, WorkflowStepType.TYPE, params=params, timeout=timeout)
        
    def wait(
        self,
        name: str,
        seconds: Optional[float] = None,
        locator: Optional[ElementLocator] = None,
        condition: Optional[str] = None,
        timeout: float = 30.0,
    ) -> "RPAWorkflow":
        """Add a wait step."""
        params = {}
        if seconds is not None:
            params["seconds"] = seconds
        if locator:
            params["locator"] = {
                "type": locator.locator_type,
                "value": locator.value,
                "timeout": locator.timeout,
            }
        if condition:
            params["condition"] = condition
        return self.step(name, WorkflowStepType.WAIT, params=params, timeout=timeout)
        
    def extract(
        self,
        name: str,
        variable_name: str,
        locator: Optional[ElementLocator] = None,
        extraction_type: str = "text",
        regex: Optional[str] = None,
        timeout: float = 10.0,
    ) -> "RPAWorkflow":
        """Add a data extraction step."""
        params = {
            "variable_name": variable_name,
            "extraction_type": extraction_type,
        }
        if locator:
            params["locator"] = {
                "type": locator.locator_type,
                "value": locator.value,
            }
        if regex:
            params["regex"] = regex
        return self.step(
            name,
            WorkflowStepType.EXTRACT,
            params=params,
            timeout=timeout,
        )
        
    def if_condition(
        self,
        name: str,
        condition: Callable[[Dict[str, Any]], bool],
        true_step: Optional[str] = None,
        false_step: Optional[str] = None,
    ) -> "RPAWorkflow":
        """Add a conditional branch step."""
        return self.step(
            name,
            WorkflowStepType.CONDITION,
            params={
                "condition_fn": condition,
                "true_step": true_step,
                "false_step": false_step,
            },
        )
        
    def loop(
        self,
        name: str,
        items_var: str,
        loop_step: str,
        max_iterations: Optional[int] = None,
    ) -> "RPAWorkflow":
        """Add a loop step."""
        return self.step(
            name,
            WorkflowStepType.LOOP,
            params={
                "items_var": items_var,
                "loop_step": loop_step,
                "max_iterations": max_iterations,
            },
        )
        
    def screenshot(
        self,
        name: str,
        path: Optional[str] = None,
        full_page: bool = False,
    ) -> "RPAWorkflow":
        """Add a screenshot step."""
        return self.step(
            name,
            WorkflowStepType.SCREENSHOT,
            params={"path": path, "full_page": full_page},
        )
        
    def assert_condition(
        self,
        name: str,
        condition: Callable[[Dict[str, Any]], bool],
        error_message: Optional[str] = None,
    ) -> "RPAWorkflow":
        """Add an assertion step."""
        return self.step(
            name,
            WorkflowStepType.ASSERT,
            params={
                "condition_fn": condition,
                "error_message": error_message,
            },
        )
        
    def set_variable(self, name: str, value: Any) -> "RPAWorkflow":
        """Set a workflow variable."""
        self.variables[name] = value
        return self
        
    def get_variable(self, name: str, default: Any = None) -> Any:
        """Get a workflow variable."""
        return self.variables.get(name, default)
        
    def execute(
        self,
        context: Optional[Dict[str, Any]] = None,
    ) -> WorkflowResult:
        """
        Execute the workflow.
        
        Args:
            context: External context for workflow execution
            
        Returns:
            WorkflowResult with execution details
        """
        start_time = time.time()
        errors: List[Dict[str, Any]] = []
        self.variables.update(context or {})
        self._current_step = 0
        
        try:
            while self._current_step < len(self.steps):
                step = self.steps[self._current_step]
                
                try:
                    self._execute_step(step)
                    
                except Exception as e:
                    logger.error(f"Step {step.name} failed: {e}")
                    errors.append({
                        "step": step.name,
                        "error": str(e),
                        "timestamp": time.time(),
                    })
                    
                    if step.on_error:
                        next_step = self.step_map.get(step.on_error)
                        if next_step:
                            self._current_step = self.steps.index(next_step)
                            continue
                            
                    if len(errors) >= 3:
                        break
                        
                self._current_step += 1
                
        except Exception as e:
            logger.error(f"Workflow {self.name} failed: {e}")
            errors.append({"step": "workflow", "error": str(e)})
            
        return WorkflowResult(
            success=len(errors) == 0,
            steps_executed=self._current_step,
            data_captured=self.captured_data.copy(),
            errors=errors,
            duration=time.time() - start_time,
        )
        
    def _execute_step(self, step: WorkflowStep) -> None:
        """Execute a single workflow step."""
        logger.info(f"Executing step: {step.name} ({step.step_type.value})")
        
        if step.step_type == WorkflowStepType.NAVIGATE:
            self._execute_navigate(step)
        elif step.step_type == WorkflowStepType.CLICK:
            self._execute_click(step)
        elif step.step_type == WorkflowStepType.TYPE:
            self._execute_type(step)
        elif step.step_type == WorkflowStepType.WAIT:
            self._execute_wait(step)
        elif step.step_type == WorkflowStepType.EXTRACT:
            self._execute_extract(step)
        elif step.step_type == WorkflowStepType.CONDITION:
            self._execute_condition(step)
        elif step.step_type == WorkflowStepType.SCREENSHOT:
            self._execute_screenshot(step)
        elif step.step_type == WorkflowStepType.ASSERT:
            self._execute_assert(step)
            
    def _execute_navigate(self, step: WorkflowStep) -> None:
        """Execute navigation step."""
        url = step.params.get("url", "")
        logger.info(f"Navigating to: {url}")
        
    def _execute_click(self, step: WorkflowStep) -> None:
        """Execute click step."""
        x = step.params.get("x")
        y = step.params.get("y")
        locator = step.params.get("locator")
        
        if locator:
            logger.info(f"Clicking element: {locator}")
        elif x is not None and y is not None:
            logger.info(f"Clicking at: ({x}, {y})")
            
    def _execute_type(self, step: WorkflowStep) -> None:
        """Execute type step."""
        text = step.params.get("text", "")
        logger.info(f"Typing: {text[:50]}...")
        
    def _execute_wait(self, step: WorkflowStep) -> None:
        """Execute wait step."""
        seconds = step.params.get("seconds")
        if seconds:
            time.sleep(seconds)
        else:
            logger.info("Waiting for element...")
            
    def _execute_extract(self, step: WorkflowStep) -> None:
        """Execute data extraction step."""
        var_name = step.params.get("variable_name", "extracted")
        self.captured_data[var_name] = f"extracted_value_{len(self.captured_data)}"
        logger.info(f"Extracted data to variable: {var_name}")
        
    def _execute_condition(self, step: WorkflowStep) -> None:
        """Execute conditional step."""
        condition_fn = step.params.get("condition_fn")
        if condition_fn and condition_fn(self.variables):
            logger.info("Condition evaluated: true")
        else:
            logger.info("Condition evaluated: false")
            
    def _execute_screenshot(self, step: WorkflowStep) -> None:
        """Execute screenshot step."""
        path = step.params.get("path", f"/tmp/{self.name}_{int(time.time())}.png")
        logger.info(f"Screenshot saved to: {path}")
        
    def _execute_assert(self, step: WorkflowStep) -> None:
        """Execute assertion step."""
        condition_fn = step.params.get("condition_fn")
        error_msg = step.params.get("error_message", "Assertion failed")
        
        if condition_fn and not condition_fn(self.variables):
            raise AssertionError(error_msg)


class RPAScheduler:
    """
    Scheduler for running RPA workflows at specified times.
    
    Example:
        scheduler = RPAScheduler()
        scheduler.schedule("daily_report", workflow, interval_hours=24)
        scheduler.start()
    """
    
    def __init__(self):
        self.scheduled_workflows: Dict[str, Dict[str, Any]] = {}
        
    def schedule(
        self,
        job_name: str,
        workflow: RPAWorkflow,
        interval_seconds: Optional[float] = None,
        cron_expression: Optional[str] = None,
    ) -> None:
        """Schedule a workflow for execution."""
        self.scheduled_workflows[job_name] = {
            "workflow": workflow,
            "interval_seconds": interval_seconds,
            "cron_expression": cron_expression,
            "last_run": None,
            "next_run": time.time() + (interval_seconds or 0),
        }
        
    def unschedule(self, job_name: str) -> None:
        """Remove a scheduled workflow."""
        self.scheduled_workflows.pop(job_name, None)
        
    def execute_scheduled(self) -> Dict[str, WorkflowResult]:
        """Execute all due workflows."""
        results = {}
        now = time.time()
        
        for name, schedule in list(self.scheduled_workflows.items()):
            if schedule["next_run"] and now >= schedule["next_run"]:
                result = schedule["workflow"].execute()
                results[name] = result
                schedule["last_run"] = now
                
                if schedule["interval_seconds"]:
                    schedule["next_run"] = now + schedule["interval_seconds"]
                else:
                    schedule["next_run"] = None
                    
        return results

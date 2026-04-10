"""
工作流调度和定时系统 v22
支持 cron 表达式、间隔执行、一次性调度、优先级队列、冲突检测、条件执行
"""
import json
import heapq
import time
import threading
import os
import subprocess
import re
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from datetime import datetime, timedelta
from croniter import croniter
import uuid


class ScheduleType(Enum):
    """调度类型"""
    CRON = "cron"                    # Cron 表达式调度
    INTERVAL = "interval"            # 间隔调度
    ONE_TIME = "one_time"            # 一次性调度
    CALENDAR = "calendar"            # 日历调度


class ScheduleState(Enum):
    """调度状态"""
    PENDING = "pending"             # 等待执行
    RUNNING = "running"             # 正在执行
    COMPLETED = "completed"         # 已完成
    FAILED = "failed"               # 执行失败
    SKIPPED = "skipped"             # 已跳过（条件不满足）
    CANCELLED = "cancelled"         # 已取消


class ConditionType(Enum):
    """条件类型"""
    FILE_EXISTS = "file_exists"     # 文件存在
    FILE_NOT_EXISTS = "file_not_exists"
    VARIABLE_EQUALS = "variable_equals"
    VARIABLE_NOT_EQUALS = "variable_not_equals"
    TIME_BETWEEN = "time_between"
    DAY_OF_WEEK = "day_of_week"
    CUSTOM = "custom"


@dataclass
class ScheduleCondition:
    """调度条件"""
    type: ConditionType
    params: Dict[str, Any] = field(default_factory=dict)
    
    def check(self, context: Dict[str, Any]) -> bool:
        """检查条件是否满足"""
        if self.type == ConditionType.FILE_EXISTS:
            path = self.params.get("path", "")
            return os.path.exists(path)
        elif self.type == ConditionType.FILE_NOT_EXISTS:
            path = self.params.get("path", "")
            return not os.path.exists(path)
        elif self.type == ConditionType.VARIABLE_EQUALS:
            var_name = self.params.get("name", "")
            expected = self.params.get("value", "")
            return context.get(var_name) == expected
        elif self.type == ConditionType.VARIABLE_NOT_EQUALS:
            var_name = self.params.get("name", "")
            unexpected = self.params.get("value", "")
            return context.get(var_name) != unexpected
        elif self.type == ConditionType.TIME_BETWEEN:
            now = datetime.now().time()
            start = datetime.strptime(self.params.get("start", "00:00"), "%H:%M").time()
            end = datetime.strptime(self.params.get("end", "23:59"), "%H:%M").time()
            return start <= now <= end
        elif self.type == ConditionType.DAY_OF_WEEK:
            days = self.params.get("days", [])
            today = datetime.now().weekday()
            return today in days
        elif self.type == ConditionType.CUSTOM:
            func = self.params.get("func")
            if callable(func):
                return func(context)
        return True


@dataclass
class ScheduledWorkflow:
    """调度的 Workflow"""
    schedule_id: str
    workflow_id: str
    workflow_name: str
    workflow_data: Dict[str, Any]
    schedule_type: ScheduleType
    schedule_config: Dict[str, Any]  # cron 表达式、间隔、具体时间等
    priority: int = 5                 # 优先级 1-10，1 最高
    conditions: List[ScheduleCondition] = field(default_factory=list)
    enabled: bool = True
    max_retries: int = 3
    timeout: Optional[int] = None     # 超时秒数
    
    # 执行状态
    state: ScheduleState = ScheduleState.PENDING
    scheduled_time: Optional[float] = None    # 计划执行时间
    actual_start_time: Optional[float] = None
    actual_end_time: Optional[float] = None
    retry_count: int = 0
    last_error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    next_run_time: Optional[float] = None     # 下次执行时间（用于间隔调度）


@dataclass
class ExecutionRecord:
    """执行记录"""
    record_id: str
    schedule_id: str
    workflow_id: str
    scheduled_time: float
    actual_start_time: float
    actual_end_time: Optional[float]
    state: ScheduleState
    retry_count: int
    error: Optional[str]
    conditions_checked: Dict[str, bool]
    conditions_skipped: bool


class WorkflowScheduler:
    """工作流调度器"""
    
    def __init__(self, data_dir: str = "./data", notification_callback: Optional[Callable] = None):
        self.data_dir = data_dir
        self.notification_callback = notification_callback
        
        # 调度任务存储
        self.schedules: Dict[str, ScheduledWorkflow] = {}
        
        # 执行队列（最小堆，按执行时间排序）
        self._execution_queue: List[Tuple[float, str]] = []  # (next_run_time, schedule_id)
        
        # 运行中的任务（冲突检测）
        self._running_tasks: Dict[str, threading.Thread] = {}
        self._running_workflows: Dict[str, str] = {}  # workflow_id -> schedule_id
        
        # 执行历史
        self.execution_history: List[ExecutionRecord] = []
        
        # 条件上下文
        self._condition_context: Dict[str, Any] = {}
        
        # 调度器状态
        self._running = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
        # 创建数据目录
        os.makedirs(data_dir, exist_ok=True)
        
        # 加载数据
        self._load_data()
    
    def _load_data(self) -> None:
        """加载持久化数据"""
        # 加载调度配置
        try:
            with open(f"{self.data_dir}/schedules.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for schedule_id, schedule_data in data.items():
                    if "schedule_type" in schedule_data:
                        schedule_data["schedule_type"] = ScheduleType(schedule_data["schedule_type"])
                    if "state" in schedule_data:
                        schedule_data["state"] = ScheduleState(schedule_data["state"])
                    if "conditions" in schedule_data:
                        conditions = []
                        for cond in schedule_data["conditions"]:
                            if "type" in cond:
                                cond["type"] = ConditionType(cond["type"])
                            conditions.append(ScheduleCondition(**cond))
                        schedule_data["conditions"] = conditions
                    self.schedules[schedule_id] = ScheduledWorkflow(**schedule_data)
        except FileNotFoundError:
            pass
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"加载调度数据失败: {e}")
        
        # 加载执行历史
        try:
            with open(f"{self.data_dir}/execution_history.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for record_data in data:
                    if "state" in record_data:
                        record_data["state"] = ScheduleState(record_data["state"])
                    self.execution_history.append(ExecutionRecord(**record_data))
        except FileNotFoundError:
            pass
        except (json.JSONDecodeError, KeyError, TypeError):
            pass
        
        # 重建执行队列
        self._rebuild_queue()
    
    def _save_data(self) -> None:
        """保存数据到持久化"""
        def convert_for_json(obj):
            if hasattr(obj, 'value'):
                return obj.value
            elif hasattr(obj, '__dict__') and not isinstance(obj, (dict, list, tuple, set, frozenset)):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return str(obj)
            elif isinstance(obj, dict):
                return {k: convert_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, (list, tuple, set)):
                return [convert_for_json(x) for x in obj]
            return obj
        
        # 保存调度配置
        schedules_data = {}
        for schedule_id, schedule in self.schedules.items():
            d = asdict(schedule)
            schedules_data[schedule_id] = convert_for_json(d)
        
        with open(f"{self.data_dir}/schedules.json", "w", encoding="utf-8") as f:
            json.dump(schedules_data, f, ensure_ascii=False, indent=2)
        
        # 保存执行历史（限制最近 1000 条）
        history_data = [convert_for_json(r) for r in self.execution_history[-1000:]]
        with open(f"{self.data_dir}/execution_history.json", "w", encoding="utf-8") as f:
            json.dump(history_data, f, ensure_ascii=False, indent=2)
    
    def _rebuild_queue(self) -> None:
        """重建执行队列"""
        self._execution_queue = []
        now = time.time()
        
        for schedule_id, schedule in self.schedules.items():
            if not schedule.enabled:
                continue
            
            next_time = self._calculate_next_run_time(schedule)
            if next_time is not None:
                schedule.next_run_time = next_time
                heapq.heappush(self._execution_queue, (next_time, schedule_id))
    
    def add_schedule(self, 
                    workflow_id: str,
                    workflow_name: str,
                    workflow_data: Dict[str, Any],
                    schedule_type: ScheduleType,
                    schedule_config: Dict[str, Any],
                    priority: int = 5,
                    conditions: List[ScheduleCondition] = None,
                    enabled: bool = True,
                    max_retries: int = 3,
                    timeout: Optional[int] = None) -> str:
        """添加调度任务"""
        schedule_id = str(uuid.uuid4())[:8]
        
        schedule = ScheduledWorkflow(
            schedule_id=schedule_id,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            workflow_data=workflow_data,
            schedule_type=schedule_type,
            schedule_config=schedule_config,
            priority=priority,
            conditions=conditions or [],
            enabled=enabled,
            max_retries=max_retries,
            timeout=timeout
        )
        
        # 计算下次执行时间
        schedule.next_run_time = self._calculate_next_run_time(schedule)
        
        with self._lock:
            self.schedules[schedule_id] = schedule
            if schedule.next_run_time is not None:
                heapq.heappush(self._execution_queue, (schedule.next_run_time, schedule_id))
        
        self._save_data()
        return schedule_id
    
    def _calculate_next_run_time(self, schedule: ScheduledWorkflow) -> Optional[float]:
        """计算下次执行时间"""
        now = time.time()
        
        if schedule.schedule_type == ScheduleType.CRON:
            cron_expr = schedule.schedule_config.get("expression", "")
            if not cron_expr:
                return None
            try:
                cron = croniter(cron_expr, datetime.now())
                next_time = cron.get_next_timestamp()
                return next_time
            except Exception:
                return None
        
        elif schedule.schedule_type == ScheduleType.INTERVAL:
            interval_seconds = schedule.schedule_config.get("seconds", 0)
            interval_minutes = schedule.schedule_config.get("minutes", 0)
            interval_hours = schedule.schedule_config.get("hours", 0)
            total_seconds = interval_seconds + interval_minutes * 60 + interval_hours * 3600
            
            if total_seconds <= 0:
                return None
            
            # 如果没有上次执行时间，则立即执行
            if schedule.next_run_time is None:
                return now
            return schedule.next_run_time + total_seconds
        
        elif schedule.schedule_type == ScheduleType.ONE_TIME:
            run_time = schedule.schedule_config.get("run_time")
            if isinstance(run_time, str):
                # 解析时间字符串
                try:
                    dt = datetime.strptime(run_time, "%Y-%m-%d %H:%M:%S")
                    return dt.timestamp()
                except ValueError:
                    try:
                        dt = datetime.strptime(run_time, "%Y-%m-%d %H:%M")
                        return dt.timestamp()
                    except ValueError:
                        return None
            elif isinstance(run_time, (int, float)):
                return run_time
            return None
        
        elif schedule.schedule_type == ScheduleType.CALENDAR:
            # 日历调度：指定具体日期和时间
            dates = schedule.schedule_config.get("dates", [])
            if not dates:
                return None
            
            # 找到下一个最近的日期
            now_dt = datetime.now()
            next_date = None
            
            for date_str in dates:
                try:
                    if " " in date_str:
                        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    else:
                        dt = datetime.strptime(date_str, "%Y-%m-%d")
                    
                    if dt.timestamp() > now.timestamp():
                        if next_date is None or dt.timestamp() < next_date:
                            next_date = dt.timestamp()
                except ValueError:
                    continue
            
            return next_date
        
        return None
    
    def remove_schedule(self, schedule_id: str) -> bool:
        """移除调度任务"""
        with self._lock:
            if schedule_id in self.schedules:
                del self.schedules[schedule_id]
                # 从队列中移除
                self._execution_queue = [(t, sid) for t, sid in self._execution_queue if sid != schedule_id]
                heapq.heapify(self._execution_queue)
                self._save_data()
                return True
        return False
    
    def enable_schedule(self, schedule_id: str, enabled: bool = True) -> bool:
        """启用/禁用调度任务"""
        with self._lock:
            if schedule_id in self.schedules:
                self.schedules[schedule_id].enabled = enabled
                if enabled:
                    # 重新计算下次执行时间
                    next_time = self._calculate_next_run_time(self.schedules[schedule_id])
                    if next_time:
                        self.schedules[schedule_id].next_run_time = next_time
                        heapq.heappush(self._execution_queue, (next_time, schedule_id))
                else:
                    # 从队列中移除
                    self._execution_queue = [(t, sid) for t, sid in self._execution_queue if sid != schedule_id]
                    heapq.heapify(self._execution_queue)
                self._save_data()
                return True
        return False
    
    def update_condition_context(self, key: str, value: Any) -> None:
        """更新条件上下文"""
        self._condition_context[key] = value
    
    def check_conditions(self, schedule: ScheduledWorkflow) -> Tuple[bool, Dict[str, bool]]:
        """检查所有条件是否满足"""
        if not schedule.conditions:
            return True, {}
        
        results = {}
        all_passed = True
        
        for condition in schedule.conditions:
            result = condition.check(self._condition_context)
            results[str(condition.type.value)] = result
            if not result:
                all_passed = False
        
        return all_passed, results
    
    def _execute_workflow(self, schedule: ScheduledWorkflow) -> Tuple[bool, Optional[str]]:
        """执行工作流"""
        workflow_id = schedule.workflow_id
        
        # 冲突检测：检查同一 workflow 是否已在运行
        if workflow_id in self._running_workflows:
            return False, f"Workflow {workflow_id} is already running (schedule: {self._running_workflows[workflow_id]})"
        
        # 标记为运行中
        self._running_workflows[workflow_id] = schedule.schedule_id
        
        try:
            # 实际执行工作流（这里调用外部执行器）
            # 在实际应用中，这里会调用工作流引擎
            result = self._run_workflow_steps(schedule.workflow_data, schedule.timeout)
            return result, None
        finally:
            # 移除运行标记
            self._running_workflows.pop(workflow_id, None)
    
    def _run_workflow_steps(self, workflow_data: Dict[str, Any], timeout: Optional[int]) -> Tuple[bool, Optional[str]]:
        """执行工作流步骤"""
        # 简化实现：实际应用中应调用完整的工作流引擎
        steps = workflow_data.get("steps", [])
        
        for i, step in enumerate(steps):
            action = step.get("action", "")
            target = step.get("target", "")
            params = step.get("params", {})
            
            # 这里应该是实际的动作执行逻辑
            # 简化：只记录要执行的动作
            print(f"执行步骤 {i+1}: {action} -> {target}")
            
            # 模拟执行
            time.sleep(0.1)
        
        return True, None
    
    def _execute_scheduled_task(self, schedule_id: str) -> None:
        """执行调度的任务（在线程中运行）"""
        schedule = self.schedules.get(schedule_id)
        if not schedule:
            return
        
        actual_start = time.time()
        schedule.state = ScheduleState.RUNNING
        schedule.actual_start_time = actual_start
        
        # 记录执行
        record = ExecutionRecord(
            record_id=str(uuid.uuid4())[:8],
            schedule_id=schedule_id,
            workflow_id=schedule.workflow_id,
            scheduled_time=schedule.scheduled_time or schedule.next_run_time or actual_start,
            actual_start_time=actual_start,
            actual_end_time=None,
            state=ScheduleState.RUNNING,
            retry_count=schedule.retry_count,
            error=None,
            conditions_checked={},
            conditions_skipped=False
        )
        
        # 检查条件
        conditions_passed, conditions_results = self.check_conditions(schedule)
        record.conditions_checked = conditions_results
        
        if not conditions_passed:
            # 条件不满足，跳过执行
            schedule.state = ScheduleState.SKIPPED
            record.state = ScheduleState.SKIPPED
            record.actual_end_time = time.time()
            schedule.actual_end_time = record.actual_end_time
            self.execution_history.append(record)
            self._save_data()
            
            # 计算下次执行时间（用于循环调度）
            if schedule.schedule_type in [ScheduleType.CRON, ScheduleType.INTERVAL, ScheduleType.CALENDAR]:
                schedule.next_run_time = self._calculate_next_run_time(schedule)
                if schedule.next_run_time:
                    heapq.heappush(self._execution_queue, (schedule.next_run_time, schedule_id))
            
            return
        
        # 执行工作流
        success, error = self._execute_workflow(schedule)
        
        actual_end = time.time()
        record.actual_end_time = actual_end
        schedule.actual_end_time = actual_end
        
        if success:
            schedule.state = ScheduleState.COMPLETED
            record.state = ScheduleState.COMPLETED
        else:
            schedule.state = ScheduleState.FAILED
            record.state = ScheduleState.FAILED
            record.error = error
            schedule.last_error = error
            
            # 重试逻辑
            if schedule.retry_count < schedule.max_retries:
                schedule.retry_count += 1
                schedule.state = ScheduleState.PENDING
                # 立即重试
                heapq.heappush(self._execution_queue, (time.time() + 5, schedule_id))
            else:
                schedule.state = ScheduleState.FAILED
        
        self.execution_history.append(record)
        self._save_data()
        
        # 发送通知
        if self.notification_callback:
            try:
                self.notification_callback(schedule, record)
            except Exception as e:
                print(f"通知发送失败: {e}")
        
        # 对于循环调度，计算下次执行时间
        if schedule.schedule_type in [ScheduleType.CRON, ScheduleType.INTERVAL, ScheduleType.CALENDAR]:
            if schedule.state == ScheduleState.COMPLETED:
                schedule.next_run_time = self._calculate_next_run_time(schedule)
                if schedule.next_run_time:
                    heapq.heappush(self._execution_queue, (schedule.next_run_time, schedule_id))
        
        # 对于一次性调度，标记为已完成
        elif schedule.schedule_type == ScheduleType.ONE_TIME:
            schedule.enabled = False
    
    def _scheduler_loop(self) -> None:
        """调度器主循环"""
        while self._running:
            now = time.time()
            
            # 处理队列中的任务
            while self._execution_queue:
                next_time, schedule_id = self._execution_queue[0]
                
                if next_time <= now:
                    # 时间到了，执行任务
                    heapq.heappop(self._execution_queue)
                    
                    schedule = self.schedules.get(schedule_id)
                    if schedule and schedule.enabled:
                        # 在新线程中执行
                        thread = threading.Thread(
                            target=self._execute_scheduled_task,
                            args=(schedule_id,),
                            daemon=True
                        )
                        self._running_tasks[schedule_id] = thread
                        thread.start()
                else:
                    # 还没到时间，等待
                    wait_seconds = min(next_time - now, 60)  # 最多等待 60 秒
                    time.sleep(wait_seconds)
                    break
            
            # 清理已结束的线程
            finished = [sid for sid, t in self._running_tasks.items() if not t.is_alive()]
            for sid in finished:
                del self._running_tasks[sid]
            
            # 如果队列为空，稍作等待
            if not self._execution_queue:
                time.sleep(1)
    
    def start(self) -> None:
        """启动调度器"""
        if self._running:
            return
        
        self._running = True
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()
        print("调度器已启动")
    
    def stop(self) -> None:
        """停止调度器"""
        self._running = False
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
        print("调度器已停止")
    
    def get_next_execution(self, schedule_id: str) -> Optional[float]:
        """获取下次执行时间"""
        schedule = self.schedules.get(schedule_id)
        if schedule:
            return schedule.next_run_time
        return None
    
    def get_execution_history(self, 
                             schedule_id: Optional[str] = None,
                             workflow_id: Optional[str] = None,
                             limit: int = 100) -> List[ExecutionRecord]:
        """获取执行历史"""
        records = self.execution_history
        
        if schedule_id:
            records = [r for r in records if r.schedule_id == schedule_id]
        if workflow_id:
            records = [r for r in records if r.workflow_id == workflow_id]
        
        return records[-limit:]
    
    def get_calendar_view(self, 
                         start_date: datetime,
                         end_date: datetime) -> Dict[str, List[Dict]]:
        """获取日历视图"""
        result: Dict[str, List[Dict]] = {}
        
        for schedule_id, schedule in self.schedules.items():
            if not schedule.enabled:
                continue
            
            # 生成指定时间范围内的所有执行时间
            if schedule.schedule_type == ScheduleType.CRON:
                cron_expr = schedule.schedule_config.get("expression", "")
                if not cron_expr:
                    continue
                try:
                    cron = croniter(cron_expr, start_date)
                    while True:
                        next_time = cron.get_next_timestamp()
                        if next_time > end_date.timestamp():
                            break
                        
                        date_key = datetime.fromtimestamp(next_time).strftime("%Y-%m-%d")
                        if date_key not in result:
                            result[date_key] = []
                        result[date_key].append({
                            "schedule_id": schedule_id,
                            "workflow_name": schedule.workflow_name,
                            "workflow_id": schedule.workflow_id,
                            "time": datetime.fromtimestamp(next_time).strftime("%H:%M:%S"),
                            "timestamp": next_time,
                            "schedule_type": schedule.schedule_type.value
                        })
                except Exception:
                    continue
            
            elif schedule.schedule_type == ScheduleType.CALENDAR:
                dates = schedule.schedule_config.get("dates", [])
                for date_str in dates:
                    try:
                        if " " in date_str:
                            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                        else:
                            dt = datetime.strptime(date_str, "%Y-%m-%d")
                        
                        ts = dt.timestamp()
                        if start_date.timestamp() <= ts <= end_date.timestamp():
                            date_key = dt.strftime("%Y-%m-%d")
                            if date_key not in result:
                                result[date_key] = []
                            result[date_key].append({
                                "schedule_id": schedule_id,
                                "workflow_name": schedule.workflow_name,
                                "workflow_id": schedule.workflow_id,
                                "time": dt.strftime("%H:%M:%S") if " " in date_str else "00:00:00",
                                "timestamp": ts,
                                "schedule_type": schedule.schedule_type.value
                            })
                    except ValueError:
                        continue
            
            elif schedule.schedule_type == ScheduleType.ONE_TIME:
                run_time = schedule.schedule_config.get("run_time")
                if isinstance(run_time, (int, float)):
                    ts = run_time
                elif isinstance(run_time, str):
                    try:
                        dt = datetime.strptime(run_time, "%Y-%m-%d %H:%M:%S")
                        ts = dt.timestamp()
                    except ValueError:
                        continue
                else:
                    continue
                
                if start_date.timestamp() <= ts <= end_date.timestamp():
                    dt = datetime.fromtimestamp(ts)
                    date_key = dt.strftime("%Y-%m-%d")
                    if date_key not in result:
                        result[date_key] = []
                    result[date_key].append({
                        "schedule_id": schedule_id,
                        "workflow_name": schedule.workflow_name,
                        "workflow_id": schedule.workflow_id,
                        "time": dt.strftime("%H:%M:%S"),
                        "timestamp": ts,
                        "schedule_type": schedule.schedule_type.value
                    })
        
        # 排序
        for date_key in result:
            result[date_key].sort(key=lambda x: x["timestamp"])
        
        return result
    
    def handle_missed_executions(self, lookback_minutes: int = 60) -> List[str]:
        """处理错过的执行（系统关闭期间应该执行的任务）"""
        now = time.time()
        lookback = now - (lookback_minutes * 60)
        missed = []
        
        for schedule_id, schedule in self.schedules.items():
            if not schedule.enabled:
                continue
            
            if schedule.schedule_type not in [ScheduleType.CRON, ScheduleType.INTERVAL, ScheduleType.CALENDAR]:
                continue
            
            # 检查上次应该执行的时间
            if schedule.next_run_time and schedule.next_run_time < now:
                # 有错过的执行
                if schedule.next_run_time >= lookback:
                    # 在回溯时间范围内，立即执行
                    schedule.scheduled_time = schedule.next_run_time
                    heapq.heappush(self._execution_queue, (time.time(), schedule_id))
                    missed.append(schedule_id)
                    print(f"发现错过的执行: {schedule.workflow_name} (原计划: {datetime.fromtimestamp(schedule.next_run_time)})")
        
        return missed
    
    def get_schedule_status(self) -> Dict[str, Any]:
        """获取调度器状态"""
        total = len(self.schedules)
        enabled = sum(1 for s in self.schedules.values() if s.enabled)
        running = len(self._running_tasks)
        pending = len(self._execution_queue)
        
        return {
            "total_schedules": total,
            "enabled_schedules": enabled,
            "running_tasks": running,
            "pending_tasks": pending,
            "is_running": self._running,
            "queue_next_run": self._execution_queue[0][0] if self._execution_queue else None
        }
    
    def list_schedules(self, 
                       workflow_id: Optional[str] = None,
                       schedule_type: Optional[ScheduleType] = None,
                       enabled_only: bool = False) -> List[ScheduledWorkflow]:
        """列出调度任务"""
        result = list(self.schedules.values())
        
        if workflow_id:
            result = [s for s in result if s.workflow_id == workflow_id]
        if schedule_type:
            result = [s for s in result if s.schedule_type == schedule_type]
        if enabled_only:
            result = [s for s in result if s.enabled]
        
        return sorted(result, key=lambda s: (s.priority, s.next_run_time or float('inf')))


def create_scheduler(data_dir: str = "./data",
                    notification_callback: Optional[Callable] = None) -> WorkflowScheduler:
    """创建调度器实例"""
    return WorkflowScheduler(data_dir, notification_callback)


# 辅助函数：发送桌面通知
def send_desktop_notification(title: str, message: str) -> None:
    """发送桌面通知"""
    try:
        if os.platform == "darwin":
            script = f'display notification "{message}" with title "{title}"'
            subprocess.run(["osascript", "-e", script], capture_output=True)
        elif os.platform == "linux":
            subprocess.run(["notify-send", title, message], capture_output=True)
        elif os.platform == "win32":
            subprocess.run(["powershell", "-Command", 
                           f'New-Object -ComObject WScript.Shell -ErrorAction Stop | '
                           f'Select-Object -ExpandProperty Popup -ArgumentList "{message}", 0, "{title}"'],
                          capture_output=True)
    except Exception as e:
        print(f"通知发送失败: {e}")


def notification_handler(schedule: ScheduledWorkflow, record: ExecutionRecord) -> None:
    """默认通知处理器"""
    if record.state == ScheduleState.COMPLETED:
        send_desktop_notification(
            f"工作流完成: {schedule.workflow_name}",
            f"计划执行时间: {datetime.fromtimestamp(record.scheduled_time).strftime('%H:%M:%S')}\n"
            f"实际执行时间: {datetime.fromtimestamp(record.actual_start_time).strftime('%H:%M:%S')}"
        )
    elif record.state == ScheduleState.FAILED:
        send_desktop_notification(
            f"工作流失败: {schedule.workflow_name}",
            f"错误: {record.error or '未知错误'}"
        )
    elif record.state == ScheduleState.SKIPPED:
        send_desktop_notification(
            f"工作流跳过: {schedule.workflow_name}",
            f"条件不满足，跳过执行"
        )


# 测试
if __name__ == "__main__":
    # 创建调度器
    scheduler = create_scheduler("./data", notification_handler)
    
    # 添加 Cron 调度
    cron_schedule_id = scheduler.add_schedule(
        workflow_id="wf_001",
        workflow_name="晨间报告",
        workflow_data={
            "workflow_id": "wf_001",
            "name": "晨间报告",
            "steps": [
                {"action": "open_app", "target": "Browser"},
                {"action": "click", "target": "Reports"}
            ]
        },
        schedule_type=ScheduleType.CRON,
        schedule_config={"expression": "0 9 * * *"},  # 每天 9 点
        priority=3
    )
    print(f"添加 Cron 调度: {cron_schedule_id}")
    
    # 添加间隔调度
    interval_schedule_id = scheduler.add_schedule(
        workflow_id="wf_002",
        workflow_name="健康检查",
        workflow_data={
            "workflow_id": "wf_002",
            "name": "健康检查",
            "steps": [
                {"action": "check", "target": "system_health"}
            ]
        },
        schedule_type=ScheduleType.INTERVAL,
        schedule_config={"minutes": 30},  # 每 30 分钟
        priority=5
    )
    print(f"添加间隔调度: {interval_schedule_id}")
    
    # 添加一次性调度
    future_time = datetime.now() + timedelta(minutes=5)
    one_time_schedule_id = scheduler.add_schedule(
        workflow_id="wf_003",
        workflow_name="临时任务",
        workflow_data={
            "workflow_id": "wf_003",
            "name": "临时任务",
            "steps": [
                {"action": "run", "target": "temp_task"}
            ]
        },
        schedule_type=ScheduleType.ONE_TIME,
        schedule_config={"run_time": future_time.strftime("%Y-%m-%d %H:%M:%S")},
        priority=1
    )
    print(f"添加一次性调度: {one_time_schedule_id}")
    
    # 添加带条件的调度
    conditional_schedule_id = scheduler.add_schedule(
        workflow_id="wf_004",
        workflow_name="条件任务",
        workflow_data={
            "workflow_id": "wf_004",
            "name": "条件任务",
            "steps": [
                {"action": "process", "target": "data_file"}
            ]
        },
        schedule_type=ScheduleType.CRON,
        schedule_config={"expression": "0 */2 * * *"},
        priority=4,
        conditions=[
            ScheduleCondition(ConditionType.FILE_EXISTS, {"path": "/tmp/input.txt"}),
            ScheduleCondition(ConditionType.VARIABLE_EQUALS, {"name": "mode", "value": "production"})
        ]
    )
    print(f"添加条件调度: {conditional_schedule_id}")
    
    # 启动调度器
    scheduler.start()
    
    # 获取日历视图
    start = datetime.now()
    end = start + timedelta(days=7)
    calendar = scheduler.get_calendar_view(start, end)
    print(f"\n未来 7 天日历:")
    for date, events in sorted(calendar.items()):
        print(f"  {date}:")
        for event in events:
            print(f"    - {event['time']} {event['workflow_name']} ({event['schedule_type']})")
    
    # 获取状态
    status = scheduler.get_schedule_status()
    print(f"\n调度器状态: {status}")
    
    # 列出所有调度
    schedules = scheduler.list_schedules()
    print(f"\n所有调度 ({len(schedules)}):")
    for s in schedules:
        next_run = datetime.fromtimestamp(s.next_run_time).strftime("%Y-%m-%d %H:%M:%S") if s.next_run_time else "N/A"
        print(f"  [{s.priority}] {s.workflow_name} - {s.schedule_type.value} - 下次: {next_run}")
    
    # 更新条件上下文
    scheduler.update_condition_context("mode", "production")
    
    # 停止调度器
    time.sleep(2)
    scheduler.stop()

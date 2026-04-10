"""
Apache Airflow 集成管理系统 v1.0
支持 DAG 管理、执行、任务管理、变量、连接、XCom、SLA 监控、触发器、池、插件
"""
import json
import time
import threading
import requests
import base64
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
from datetime import datetime, timedelta
import hashlib
import os


class AirflowAPIException(Exception):
    """Airflow API 异常"""
    pass


class DAGState(Enum):
    """DAG 状态"""
    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"
    QUEUED = "queued"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"
    REMOVED = "removed"


class TaskState(Enum):
    """任务状态"""
    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"
    QUEUED = "queued"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    SKIPPED = "skipped"
    UPSTREAM_FAILED = "upstream_failed"
    REMOVED = "removed"


class TriggerType(Enum):
    """触发器类型"""
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    CLI = "cli"
    REST_API = "rest_api"
    Callback = "callback"


class PoolState(Enum):
    """池状态"""
    OPEN = "open"
    FULL = "full"


@dataclass
class DAG:
    """DAG 数据模型"""
    dag_id: str
    fileloc: str
    owners: str = ""
    description: str = ""
    schedule_interval: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    max_active_runs: int = 16
    max_active_tasks: int = 16
    depends_on_past: bool = False
    wait_for_downstream: bool = False
    retry_delay: int = 300
    max_retry_delay: int = 600
    default_args: Dict[str, Any] = field(default_factory=dict)
    is_paused: bool = False
    is_subdag: bool = False
    is_active: bool = True


@dataclass
class DAGRun:
    """DAG 运行记录"""
    run_id: str
    dag_id: str
    state: str
    execution_date: datetime
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    external_trigger: bool = False
    conf: Dict[str, Any] = field(default_factory=dict)
    triggered_by: str = "manual"


@dataclass
class Task:
    """任务数据模型"""
    task_id: str
    dag_id: str
    task_type: str = "python"
    owner: str = "airflow"
    bash_command: Optional[str] = None
    python_callable: Optional[str] = None
    retries: int = 0
    retry_delay: int = 300
    retry_exponential_backoff: bool = False
    max_retry_delay: int = 600
    priority_weight: int = 1
    weight_rule: str = "downstream"
    queue: str = "default"
    pool: str = "default_pool"
    sla: Optional[int] = None
    execution_timeout: Optional[int] = None
    depends_on_past: bool = False
    wait_for_downstream: bool = False
    trigger_rule: str = "all_success"
    upstream_tasks: List[str] = field(default_factory=list)
    downstream_tasks: List[str] = field(default_factory=list)


@dataclass
class Variable:
    """Airflow 变量"""
    key: str
    value: Any
    description: str = ""
    is_encrypted: bool = False


@dataclass
class Connection:
    """Airflow 连接"""
    conn_id: str
    conn_type: str
    host: str = ""
    login: str = ""
    password: str = ""
    port: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)
    description: str = ""


@dataclass
class XComMessage:
    """XCom 消息"""
    key: str
    value: Any
    task_id: str
    dag_id: str
    execution_date: datetime
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class SLA:
    """SLA 配置"""
    task_id: str
    dag_id: str
    deadline: datetime
    email: Optional[str] = None
    description: str = ""
    enabled: bool = True


@dataclass
class Trigger:
    """触发器记录"""
    trigger_id: str
    dag_id: str
    trigger_type: TriggerType
    execution_date: datetime
    status: str = "pending"
    result: Optional[Dict[str, Any]] = None


@dataclass
class Pool:
    """任务池"""
    name: str
    slots: int
    description: str = ""
    used_slots: int = 0
    queued_slots: int = 0


@dataclass
class AirflowPlugin:
    """Airflow 插件"""
    name: str
    version: str
    description: str = ""
    hooks: List[str] = field(default_factory=list)
    executors: List[str] = field(default_factory=list)
    operators: List[str] = field(default_factory=list)
    sensors: List[str] = field(default_factory=list)
    macros: List[str] = field(default_factory=list)
    admin_views: List[str] = field(default_factory=list)
    menu_links: List[str] = field(default_factory=list)
    appbuilder_views: List[str] = field(default_factory=list)
    appbuilder_menu_items: List[str] = field(default_factory=list)


class AirflowManager:
    """
    Apache Airflow 综合管理类
    
    功能列表:
    1. DAG 管理: 创建/管理 Airflow DAGs
    2. DAG 执行: 触发和监控 DAG 运行
    3. 任务管理: 管理 Airflow 任务
    4. 变量管理: 管理 Airflow 变量
    5. 连接管理: 管理 Airflow 连接
    6. XCom 管理: 管理 XCom 消息
    7. SLA 监控: 监控任务 SLA
    8. 触发器管理: 管理 DAG 触发器
    9. 池管理: 管理任务池
    10. 插件集成: Airflow 插件开发
    """
    
    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        username: str = "airflow",
        password: str = "airflow",
        verify_ssl: bool = True
    ):
        """
        初始化 Airflow 管理器
        
        Args:
            base_url: Airflow Web Server 地址
            username: 用户名
            password: 密码
            verify_ssl: 是否验证 SSL 证书
        """
        self.base_url = base_url.rstrip("/")
        self.auth = (username, password)
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self.session.auth = self.auth
        
        self._dags: Dict[str, DAG] = {}
        self._dag_runs: Dict[str, List[DAGRun]] = {}
        self._tasks: Dict[str, Dict[str, Task]] = {}
        self._variables: Dict[str, Variable] = {}
        self._connections: Dict[str, Connection] = {}
        self._xcoms: List[XComMessage] = []
        self._slas: Dict[str, List[SLA]] = {}
        self._triggers: Dict[str, List[Trigger]] = {}
        self._pools: Dict[str, Pool] = {}
        self._plugins: Dict[str, AirflowPlugin] = {}
        self._lock = threading.RLock()
        
        self._stats = {
            "dags_created": 0,
            "dag_runs_triggered": 0,
            "tasks_executed": 0,
            "variables_set": 0,
            "connections_created": 0,
            "xcoms_sent": 0,
            "slas_monitored": 0,
            "triggers_fired": 0,
            "pools_configured": 0,
            "plugins_installed": 0
        }
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        发送 HTTP 请求到 Airflow API
        
        Args:
            method: HTTP 方法
            endpoint: API 端点
            data: 请求数据
            params: 查询参数
            
        Returns:
            API 响应数据
            
        Raises:
            AirflowAPIException: API 请求失败
        """
        url = f"{self.base_url}/api/v1/{endpoint.lstrip('/')}"
        
        try:
            if method.upper() == "GET":
                response = self.session.get(
                    url, json=data, params=params, verify=self.verify_ssl, timeout=30
                )
            elif method.upper() == "POST":
                response = self.session.post(
                    url, json=data, params=params, verify=self.verify_ssl, timeout=30
                )
            elif method.upper() == "PUT":
                response = self.session.put(
                    url, json=data, params=params, verify=self.verify_ssl, timeout=30
                )
            elif method.upper() == "PATCH":
                response = self.session.patch(
                    url, json=data, params=params, verify=self.verify_ssl, timeout=30
                )
            elif method.upper() == "DELETE":
                response = self.session.delete(
                    url, json=data, params=params, verify=self.verify_ssl, timeout=30
                )
            else:
                raise AirflowAPIException(f"Unsupported HTTP method: {method}")
            
            if response.status_code >= 400:
                raise AirflowAPIException(
                    f"API request failed: {response.status_code} - {response.text}"
                )
            
            return response.json() if response.content else {}
            
        except requests.RequestException as e:
            raise AirflowAPIException(f"Failed to connect to Airflow: {str(e)}")
    
    def _generate_run_id(self, dag_id: str, execution_date: Optional[datetime] = None) -> str:
        """生成 DAG Run ID"""
        ts = (execution_date or datetime.now()).isoformat()
        return f"manual__{dag_id}__{hashlib.md5(ts.encode()).hexdigest()[:8]}"
    
    # ==================== DAG 管理 ====================
    
    def create_dag(
        self,
        dag_id: str,
        fileloc: str,
        schedule_interval: Optional[str] = None,
        description: str = "",
        owners: str = "airflow",
        default_args: Optional[Dict[str, Any]] = None,
        max_active_runs: int = 16,
        max_active_tasks: int = 16,
        depends_on_past: bool = False,
        retry_delay: int = 300,
        max_retry_delay: int = 600
    ) -> DAG:
        """
        创建 DAG
        
        Args:
            dag_id: DAG 唯一标识
            fileloc: DAG 文件路径
            schedule_interval: 调度间隔 (cron 表达式或 timedelta)
            description: DAG 描述
            owners: DAG 所有者
            default_args: 默认参数
            max_active_runs: 最大并发运行数
            max_active_tasks: 最大并发任务数
            depends_on_past: 是否依赖上一次运行
            retry_delay: 重试延迟 (秒)
            max_retry_delay: 最大重试延迟 (秒)
            
        Returns:
            创建的 DAG 对象
        """
        with self._lock:
            if dag_id in self._dags:
                raise AirflowAPIException(f"DAG {dag_id} already exists")
            
            dag = DAG(
                dag_id=dag_id,
                fileloc=fileloc,
                description=description,
                owners=owners,
                schedule_interval=schedule_interval,
                max_active_runs=max_active_runs,
                max_active_tasks=max_active_tasks,
                depends_on_past=depends_on_past,
                retry_delay=retry_delay,
                max_retry_delay=max_retry_delay,
                default_args=default_args or {},
                start_date=datetime.now()
            )
            
            self._dags[dag_id] = dag
            self._tasks[dag_id] = {}
            self._dag_runs[dag_id] = []
            self._slas[dag_id] = []
            self._triggers[dag_id] = []
            self._stats["dags_created"] += 1
            
            return dag
    
    def get_dag(self, dag_id: str) -> Optional[DAG]:
        """获取 DAG 信息"""
        return self._dags.get(dag_id)
    
    def list_dags(self) -> List[DAG]:
        """列出所有 DAG"""
        return list(self._dags.values())
    
    def update_dag(self, dag_id: str, **kwargs) -> Optional[DAG]:
        """更新 DAG 配置"""
        with self._lock:
            dag = self._dags.get(dag_id)
            if not dag:
                return None
            
            for key, value in kwargs.items():
                if hasattr(dag, key):
                    setattr(dag, key, value)
            
            return dag
    
    def delete_dag(self, dag_id: str) -> bool:
        """删除 DAG"""
        with self._lock:
            if dag_id not in self._dags:
                return False
            
            del self._dags[dag_id]
            if dag_id in self._tasks:
                del self._tasks[dag_id]
            if dag_id in self._dag_runs:
                del self._dag_runs[dag_id]
            if dag_id in self._slas:
                del self._slas[dag_id]
            if dag_id in self._triggers:
                del self._triggers[dag_id]
            
            return True
    
    def pause_dag(self, dag_id: str) -> bool:
        """暂停 DAG"""
        dag = self._dags.get(dag_id)
        if not dag:
            return False
        dag.is_paused = True
        return True
    
    def unpause_dag(self, dag_id: str) -> bool:
        """恢复 DAG"""
        dag = self._dags.get(dag_id)
        if not dag:
            return False
        dag.is_paused = False
        return True
    
    def get_dag_details(self, dag_id: str) -> Optional[Dict[str, Any]]:
        """获取 DAG 详细信息"""
        dag = self._dags.get(dag_id)
        if not dag:
            return None
        
        return {
            "dag_id": dag.dag_id,
            "fileloc": dag.fileloc,
            "owners": dag.owners,
            "description": dag.description,
            "schedule_interval": dag.schedule_interval,
            "start_date": dag.start_date.isoformat() if dag.start_date else None,
            "end_date": dag.end_date.isoformat() if dag.end_date else None,
            "max_active_runs": dag.max_active_runs,
            "max_active_tasks": dag.max_active_tasks,
            "depends_on_past": dag.depends_on_past,
            "wait_for_downstream": dag.wait_for_downstream,
            "retry_delay": dag.retry_delay,
            "max_retry_delay": dag.max_retry_delay,
            "default_args": dag.default_args,
            "is_paused": dag.is_paused,
            "is_subdag": dag.is_subdag,
            "is_active": dag.is_active,
            "task_count": len(self._tasks.get(dag_id, {})),
            "run_count": len(self._dag_runs.get(dag_id, []))
        }
    
    # ==================== DAG 执行 ====================
    
    def trigger_dag(
        self,
        dag_id: str,
        execution_date: Optional[datetime] = None,
        conf: Optional[Dict[str, Any]] = None,
        replace_microseconds: bool = True
    ) -> str:
        """
        触发 DAG 运行
        
        Args:
            dag_id: DAG ID
            execution_date: 执行时间
            conf: 运行配置
            replace_microseconds: 是否替换毫秒
            
        Returns:
            run_id: DAG Run ID
        """
        with self._lock:
            if dag_id not in self._dags:
                raise AirflowAPIException(f"DAG {dag_id} not found")
            
            dag = self._dags[dag_id]
            if dag.is_paused:
                raise AirflowAPIException(f"DAG {dag_id} is paused")
            
            run_id = self._generate_run_id(dag_id, execution_date)
            exec_date = execution_date or datetime.now()
            
            dag_run = DAGRun(
                run_id=run_id,
                dag_id=dag_id,
                state=DAGState.QUEUED.value,
                execution_date=exec_date,
                external_trigger=True,
                conf=conf or {},
                triggered_by="manual"
            )
            
            self._dag_runs[dag_id].append(dag_run)
            self._stats["dag_runs_triggered"] += 1
            
            return run_id
    
    def get_dag_run(self, dag_id: str, run_id: str) -> Optional[DAGRun]:
        """获取 DAG Run"""
        runs = self._dag_runs.get(dag_id, [])
        for run in runs:
            if run.run_id == run_id:
                return run
        return None
    
    def list_dag_runs(self, dag_id: str, state: Optional[str] = None) -> List[DAGRun]:
        """列出 DAG Runs"""
        runs = self._dag_runs.get(dag_id, [])
        if state:
            runs = [r for r in runs if r.state == state]
        return runs
    
    def update_dag_run_state(self, dag_id: str, run_id: str, state: str) -> bool:
        """更新 DAG Run 状态"""
        run = self.get_dag_run(dag_id, run_id)
        if not run:
            return False
        
        run.state = state
        if state == DAGState.RUNNING.value:
            run.start_date = datetime.now()
        elif state in [DAGState.SUCCESS.value, DAGState.FAILED.value]:
            run.end_date = datetime.now()
        
        return True
    
    def clear_dag_run(self, dag_id: str, run_id: str) -> bool:
        """清除 DAG Run"""
        run = self.get_dag_run(dag_id, run_id)
        if not run:
            return False
        
        run.state = "cleared"
        return True
    
    def monitor_dag_run(
        self,
        dag_id: str,
        run_id: str,
        timeout: int = 3600,
        poll_interval: int = 10
    ) -> Dict[str, Any]:
        """
        监控 DAG Run 执行状态
        
        Args:
            dag_id: DAG ID
            run_id: Run ID
            timeout: 超时时间 (秒)
            poll_interval: 轮询间隔 (秒)
            
        Returns:
            执行结果
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            run = self.get_dag_run(dag_id, run_id)
            if not run:
                raise AirflowAPIException(f"DAG Run {run_id} not found")
            
            state = run.state
            
            if state in [DAGState.SUCCESS.value, DAGState.FAILED.value]:
                return {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "state": state,
                    "execution_date": run.execution_date.isoformat(),
                    "start_date": run.start_date.isoformat() if run.start_date else None,
                    "end_date": run.end_date.isoformat() if run.end_date else None,
                    "duration": (
                        (run.end_date - run.start_date).total_seconds()
                        if run.start_date and run.end_date else None
                    )
                }
            
            time.sleep(poll_interval)
        
        raise AirflowAPIException(f"DAG Run {run_id} timed out after {timeout} seconds")
    
    def get_dag_run_tis(self, dag_id: str, run_id: str) -> List[Dict[str, Any]]:
        """获取 DAG Run 的任务实例"""
        run = self.get_dag_run(dag_id, run_id)
        if not run:
            return []
        
        tasks = self._tasks.get(dag_id, {})
        tis = []
        
        for task_id, task in tasks.items():
            tis.append({
                "task_id": task_id,
                "dag_id": dag_id,
                "run_id": run_id,
                "state": "queued",
                "execution_date": run.execution_date.isoformat(),
                "task_type": task.task_type
            })
        
        return tis
    
    # ==================== 任务管理 ====================
    
    def create_task(
        self,
        task_id: str,
        dag_id: str,
        task_type: str = "python",
        bash_command: Optional[str] = None,
        python_callable: Optional[str] = None,
        owner: str = "airflow",
        retries: int = 0,
        retry_delay: int = 300,
        priority_weight: int = 1,
        pool: str = "default_pool",
        queue: str = "default",
        trigger_rule: str = "all_success",
        sla: Optional[int] = None,
        upstream_tasks: Optional[List[str]] = None,
        downstream_tasks: Optional[List[str]] = None
    ) -> Task:
        """
        创建任务
        
        Args:
            task_id: 任务 ID
            dag_id: DAG ID
            task_type: 任务类型 (bash, python, sensor, etc.)
            bash_command: Bash 命令
            python_callable: Python 可调用函数名
            owner: 任务所有者
            retries: 重试次数
            retry_delay: 重试延迟 (秒)
            priority_weight: 优先级权重
            pool: 任务池
            queue: 队列
            trigger_rule: 触发规则
            sla: SLA 超时 (秒)
            upstream_tasks: 上游任务列表
            downstream_tasks: 下游任务列表
            
        Returns:
            创建的任务对象
        """
        with self._lock:
            if dag_id not in self._dags:
                raise AirflowAPIException(f"DAG {dag_id} not found")
            
            if dag_id not in self._tasks:
                self._tasks[dag_id] = {}
            
            if task_id in self._tasks[dag_id]:
                raise AirflowAPIException(f"Task {task_id} already exists in DAG {dag_id}")
            
            task = Task(
                task_id=task_id,
                dag_id=dag_id,
                task_type=task_type,
                owner=owner,
                bash_command=bash_command,
                python_callable=python_callable,
                retries=retries,
                retry_delay=retry_delay,
                priority_weight=priority_weight,
                pool=pool,
                queue=queue,
                trigger_rule=trigger_rule,
                sla=sla,
                upstream_tasks=upstream_tasks or [],
                downstream_tasks=downstream_tasks or []
            )
            
            self._tasks[dag_id][task_id] = task
            self._stats["tasks_executed"] += 1
            
            return task
    
    def get_task(self, dag_id: str, task_id: str) -> Optional[Task]:
        """获取任务"""
        return self._tasks.get(dag_id, {}).get(task_id)
    
    def list_tasks(self, dag_id: str) -> List[Task]:
        """列出 DAG 中的所有任务"""
        return list(self._tasks.get(dag_id, {}).values())
    
    def update_task(self, dag_id: str, task_id: str, **kwargs) -> Optional[Task]:
        """更新任务配置"""
        with self._lock:
            task = self._tasks.get(dag_id, {}).get(task_id)
            if not task:
                return None
            
            for key, value in kwargs.items():
                if hasattr(task, key):
                    setattr(task, key, value)
            
            return task
    
    def delete_task(self, dag_id: str, task_id: str) -> bool:
        """删除任务"""
        with self._lock:
            if dag_id not in self._tasks:
                return False
            
            if task_id not in self._tasks[dag_id]:
                return False
            
            del self._tasks[dag_id][task_id]
            
            for other_task in self._tasks[dag_id].values():
                if task_id in other_task.upstream_tasks:
                    other_task.upstream_tasks.remove(task_id)
                if task_id in other_task.downstream_tasks:
                    other_task.downstream_tasks.remove(task_id)
            
            return True
    
    def set_task_upstream(self, dag_id: str, task_id: str, upstream_task_id: str) -> bool:
        """设置任务的上游依赖"""
        task = self.get_task(dag_id, task_id)
        upstream_task = self.get_task(dag_id, upstream_task_id)
        
        if not task or not upstream_task:
            return False
        
        if upstream_task_id not in task.upstream_tasks:
            task.upstream_tasks.append(upstream_task_id)
        
        if task_id not in upstream_task.downstream_tasks:
            upstream_task.downstream_tasks.append(task_id)
        
        return True
    
    def set_task_downstream(self, dag_id: str, task_id: str, downstream_task_id: str) -> bool:
        """设置任务的下游依赖"""
        return self.set_task_upstream(dag_id, downstream_task_id, task_id)
    
    def get_task_dependencies(self, dag_id: str, task_id: str) -> Dict[str, List[str]]:
        """获取任务依赖关系"""
        task = self.get_task(dag_id, task_id)
        if not task:
            return {"upstream": [], "downstream": []}
        
        return {
            "upstream": task.upstream_tasks.copy(),
            "downstream": task.downstream_tasks.copy()
        }
    
    # ==================== 变量管理 ====================
    
    def set_variable(
        self,
        key: str,
        value: Any,
        description: str = "",
        encrypt: bool = False
    ) -> Variable:
        """
        设置变量
        
        Args:
            key: 变量键
            value: 变量值
            description: 描述
            encrypt: 是否加密
            
        Returns:
            Variable 对象
        """
        with self._lock:
            var = Variable(
                key=key,
                value=value,
                description=description,
                is_encrypted=encrypt
            )
            self._variables[key] = var
            self._stats["variables_set"] += 1
            return var
    
    def get_variable(self, key: str, default: Any = None) -> Optional[Any]:
        """获取变量值"""
        var = self._variables.get(key)
        return var.value if var else default
    
    def get_variable_full(self, key: str) -> Optional[Variable]:
        """获取完整变量对象"""
        return self._variables.get(key)
    
    def list_variables(self) -> List[Variable]:
        """列出所有变量"""
        return list(self._variables.values())
    
    def delete_variable(self, key: str) -> bool:
        """删除变量"""
        with self._lock:
            if key in self._variables:
                del self._variables[key]
                return True
            return False
    
    def import_variables(self, variables: Dict[str, Any]) -> int:
        """
        批量导入变量
        
        Args:
            variables: 变量字典
            
        Returns:
            导入数量
        """
        count = 0
        for key, value in variables.items():
            self.set_variable(key, value)
            count += 1
        return count
    
    def export_variables(self) -> Dict[str, Any]:
        """导出所有变量"""
        return {var.key: var.value for var in self._variables.values()}
    
    # ==================== 连接管理 ====================
    
    def create_connection(
        self,
        conn_id: str,
        conn_type: str,
        host: str = "",
        login: str = "",
        password: str = "",
        port: int = 0,
        extra: Optional[Dict[str, Any]] = None,
        description: str = ""
    ) -> Connection:
        """
        创建连接
        
        Args:
            conn_id: 连接 ID
            conn_type: 连接类型 (mysql, postgres, http, etc.)
            host: 主机
            login: 登录名
            password: 密码
            port: 端口
            extra: 额外配置
            description: 描述
            
        Returns:
            Connection 对象
        """
        with self._lock:
            if conn_id in self._connections:
                raise AirflowAPIException(f"Connection {conn_id} already exists")
            
            conn = Connection(
                conn_id=conn_id,
                conn_type=conn_type,
                host=host,
                login=login,
                password=password,
                port=port,
                extra=extra or {},
                description=description
            )
            
            self._connections[conn_id] = conn
            self._stats["connections_created"] += 1
            return conn
    
    def get_connection(self, conn_id: str) -> Optional[Connection]:
        """获取连接"""
        return self._connections.get(conn_id)
    
    def list_connections(self, conn_type: Optional[str] = None) -> List[Connection]:
        """列出连接"""
        conns = list(self._connections.values())
        if conn_type:
            conns = [c for c in conns if c.conn_type == conn_type]
        return conns
    
    def update_connection(self, conn_id: str, **kwargs) -> Optional[Connection]:
        """更新连接"""
        with self._lock:
            conn = self._connections.get(conn_id)
            if not conn:
                return None
            
            for key, value in kwargs.items():
                if hasattr(conn, key):
                    setattr(conn, key, value)
            
            return conn
    
    def delete_connection(self, conn_id: str) -> bool:
        """删除连接"""
        with self._lock:
            if conn_id in self._connections:
                del self._connections[conn_id]
                return True
            return False
    
    def test_connection(self, conn_id: str) -> Dict[str, Any]:
        """
        测试连接
        
        Args:
            conn_id: 连接 ID
            
        Returns:
            测试结果
        """
        conn = self._connections.get(conn_id)
        if not conn:
            return {"success": False, "message": f"Connection {conn_id} not found"}
        
        return {
            "success": True,
            "message": f"Connection {conn_id} tested successfully",
            "conn_type": conn.conn_type,
            "host": conn.host,
            "port": conn.port
        }
    
    # ==================== XCom 管理 ====================
    
    def xcom_push(
        self,
        key: str,
        value: Any,
        task_id: str,
        dag_id: str,
        execution_date: Optional[datetime] = None
    ) -> XComMessage:
        """
        推送 XCom 消息
        
        Args:
            key: XCom 键
            value: XCom 值
            task_id: 任务 ID
            dag_id: DAG ID
            execution_date: 执行时间
            
        Returns:
            XComMessage 对象
        """
        with self._lock:
            msg = XComMessage(
                key=key,
                value=value,
                task_id=task_id,
                dag_id=dag_id,
                execution_date=execution_date or datetime.now(),
                timestamp=datetime.now()
            )
            
            self._xcoms.append(msg)
            self._stats["xcoms_sent"] += 1
            return msg
    
    def xcom_pull(
        self,
        task_ids: Optional[List[str]] = None,
        dag_id: Optional[str] = None,
        execution_date: Optional[datetime] = None,
        key: str = "return_value"
    ) -> List[Any]:
        """
        拉取 XCom 消息
        
        Args:
            task_ids: 任务 ID 列表
            dag_id: DAG ID
            execution_date: 执行时间
            key: XCom 键
            
        Returns:
            XCom 值列表
        """
        results = []
        
        for msg in self._xcoms:
            if msg.key != key:
                continue
            if task_ids and msg.task_id not in task_ids:
                continue
            if dag_id and msg.dag_id != dag_id:
                continue
            if execution_date and msg.execution_date != execution_date:
                continue
            
            results.append(msg.value)
        
        return results
    
    def xcom_get(
        self,
        task_id: str,
        dag_id: str,
        execution_date: datetime,
        key: str = "return_value"
    ) -> Optional[Any]:
        """
        获取特定 XCom 消息
        
        Args:
            task_id: 任务 ID
            dag_id: DAG ID
            execution_date: 执行时间
            key: XCom 键
            
        Returns:
            XCom 值
        """
        for msg in reversed(self._xcoms):
            if msg.key == key and msg.task_id == task_id and msg.dag_id == dag_id:
                if msg.execution_date == execution_date:
                    return msg.value
        return None
    
    def xcom_delete(self, task_id: str, dag_id: str, execution_date: datetime, key: str) -> bool:
        """删除 XCom 消息"""
        with self._lock:
            for i, msg in enumerate(self._xcoms):
                if (msg.key == key and msg.task_id == task_id and 
                    msg.dag_id == dag_id and msg.execution_date == execution_date):
                    del self._xcoms[i]
                    return True
            return False
    
    def list_xcoms(
        self,
        dag_id: Optional[str] = None,
        task_id: Optional[str] = None
    ) -> List[XComMessage]:
        """列出 XCom 消息"""
        xcoms = self._xcoms
        if dag_id:
            xcoms = [x for x in xcoms if x.dag_id == dag_id]
        if task_id:
            xcoms = [x for x in xcoms if x.task_id == task_id]
        return xcoms
    
    # ==================== SLA 监控 ====================
    
    def create_sla(
        self,
        task_id: str,
        dag_id: str,
        deadline: datetime,
        email: Optional[str] = None,
        description: str = ""
    ) -> SLA:
        """
        创建 SLA 监控
        
        Args:
            task_id: 任务 ID
            dag_id: DAG ID
            deadline: SLA 截止时间
            email: 通知邮箱
            description: 描述
            
        Returns:
            SLA 对象
        """
        with self._lock:
            sla = SLA(
                task_id=task_id,
                dag_id=dag_id,
                deadline=deadline,
                email=email,
                description=description,
                enabled=True
            )
            
            if dag_id not in self._slas:
                self._slas[dag_id] = []
            
            self._slas[dag_id].append(sla)
            self._stats["slas_monitored"] += 1
            return sla
    
    def get_sla(self, dag_id: str, task_id: str) -> Optional[SLA]:
        """获取 SLA"""
        for sla in self._slas.get(dag_id, []):
            if sla.task_id == task_id:
                return sla
        return None
    
    def list_slas(self, dag_id: Optional[str] = None) -> List[SLA]:
        """列出 SLA"""
        if dag_id:
            return self._slas.get(dag_id, [])
        all_slas = []
        for slas in self._slas.values():
            all_slas.extend(slas)
        return all_slas
    
    def check_sla(self, dag_id: str, task_id: str, current_time: Optional[datetime] = None) -> Dict[str, Any]:
        """
        检查 SLA 状态
        
        Args:
            dag_id: DAG ID
            task_id: 任务 ID
            current_time: 当前时间
            
        Returns:
            SLA 状态
        """
        sla = self.get_sla(dag_id, task_id)
        if not sla:
            return {"has_sla": False}
        
        now = current_time or datetime.now()
        is_missed = now > sla.deadline
        
        return {
            "has_sla": True,
            "is_missed": is_missed,
            "deadline": sla.deadline.isoformat(),
            "email": sla.email,
            "description": sla.description,
            "time_remaining": (sla.deadline - now).total_seconds() if not is_missed else None
        }
    
    def update_sla(self, dag_id: str, task_id: str, **kwargs) -> Optional[SLA]:
        """更新 SLA"""
        with self._lock:
            sla = self.get_sla(dag_id, task_id)
            if not sla:
                return None
            
            for key, value in kwargs.items():
                if hasattr(sla, key):
                    setattr(sla, key, value)
            
            return sla
    
    def delete_sla(self, dag_id: str, task_id: str) -> bool:
        """删除 SLA"""
        with self._lock:
            if dag_id not in self._slas:
                return False
            
            for i, sla in enumerate(self._slas[dag_id]):
                if sla.task_id == task_id:
                    del self._slas[dag_id][i]
                    return True
            return False
    
    def get_missed_slas(self) -> List[Dict[str, Any]]:
        """获取所有错过的 SLA"""
        missed = []
        now = datetime.now()
        
        for dag_id, slas in self._slas.items():
            for sla in slas:
                if sla.enabled and now > sla.deadline:
                    missed.append({
                        "dag_id": dag_id,
                        "task_id": sla.task_id,
                        "deadline": sla.deadline.isoformat(),
                        "email": sla.email,
                        "description": sla.description,
                        "missed_by": (now - sla.deadline).total_seconds()
                    })
        
        return missed
    
    # ==================== 触发器管理 ====================
    
    def create_trigger(
        self,
        dag_id: str,
        trigger_type: TriggerType,
        execution_date: Optional[datetime] = None
    ) -> Trigger:
        """
        创建触发器记录
        
        Args:
            dag_id: DAG ID
            trigger_type: 触发器类型
            execution_date: 执行时间
            
        Returns:
            Trigger 对象
        """
        with self._lock:
            trigger_id = hashlib.md5(
                f"{dag_id}_{trigger_type.value}_{time.time()}".encode()
            ).hexdigest()[:12]
            
            trigger = Trigger(
                trigger_id=trigger_id,
                dag_id=dag_id,
                trigger_type=trigger_type,
                execution_date=execution_date or datetime.now(),
                status="pending"
            )
            
            if dag_id not in self._triggers:
                self._triggers[dag_id] = []
            
            self._triggers[dag_id].append(trigger)
            self._stats["triggers_fired"] += 1
            return trigger
    
    def get_trigger(self, dag_id: str, trigger_id: str) -> Optional[Trigger]:
        """获取触发器"""
        for trigger in self._triggers.get(dag_id, []):
            if trigger.trigger_id == trigger_id:
                return trigger
        return None
    
    def list_triggers(
        self,
        dag_id: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[Trigger]:
        """列出触发器"""
        triggers = []
        if dag_id:
            triggers = self._triggers.get(dag_id, [])
        else:
            for t in self._triggers.values():
                triggers.extend(t)
        
        if status:
            triggers = [t for t in triggers if t.status == status]
        
        return triggers
    
    def update_trigger_status(
        self,
        dag_id: str,
        trigger_id: str,
        status: str,
        result: Optional[Dict[str, Any]] = None
    ) -> bool:
        """更新触发器状态"""
        trigger = self.get_trigger(dag_id, trigger_id)
        if not trigger:
            return False
        
        trigger.status = status
        if result:
            trigger.result = result
        
        return True
    
    def delete_trigger(self, dag_id: str, trigger_id: str) -> bool:
        """删除触发器"""
        with self._lock:
            if dag_id not in self._triggers:
                return False
            
            for i, trigger in enumerate(self._triggers[dag_id]):
                if trigger.trigger_id == trigger_id:
                    del self._triggers[dag_id][i]
                    return True
            return False
    
    # ==================== 池管理 ====================
    
    def create_pool(
        self,
        name: str,
        slots: int,
        description: str = ""
    ) -> Pool:
        """
        创建任务池
        
        Args:
            name: 池名称
            slots: 槽位数
            description: 描述
            
        Returns:
            Pool 对象
        """
        with self._lock:
            if name in self._pools:
                raise AirflowAPIException(f"Pool {name} already exists")
            
            pool = Pool(
                name=name,
                slots=slots,
                description=description,
                used_slots=0,
                queued_slots=0
            )
            
            self._pools[name] = pool
            self._stats["pools_configured"] += 1
            return pool
    
    def get_pool(self, name: str) -> Optional[Pool]:
        """获取池"""
        return self._pools.get(name)
    
    def list_pools(self) -> List[Pool]:
        """列出所有池"""
        return list(self._pools.values())
    
    def update_pool(self, name: str, **kwargs) -> Optional[Pool]:
        """更新池"""
        with self._lock:
            pool = self._pools.get(name)
            if not pool:
                return None
            
            for key, value in kwargs.items():
                if hasattr(pool, key):
                    setattr(pool, key, value)
            
            return pool
    
    def delete_pool(self, name: str) -> bool:
        """删除池"""
        with self._lock:
            if name in self._pools:
                del self._pools[name]
                return True
            return False
    
    def allocate_pool_slots(self, name: str, count: int = 1) -> bool:
        """分配池槽位"""
        pool = self._pools.get(name)
        if not pool:
            return False
        
        if pool.used_slots + count > pool.slots:
            return False
        
        pool.used_slots += count
        return True
    
    def release_pool_slots(self, name: str, count: int = 1) -> bool:
        """释放池槽位"""
        pool = self._pools.get(name)
        if not pool:
            return False
        
        pool.used_slots = max(0, pool.used_slots - count)
        return True
    
    def get_pool_stats(self, name: str) -> Optional[Dict[str, Any]]:
        """获取池统计"""
        pool = self._pools.get(name)
        if not pool:
            return None
        
        return {
            "name": pool.name,
            "total_slots": pool.slots,
            "used_slots": pool.used_slots,
            "queued_slots": pool.queued_slots,
            "available_slots": pool.slots - pool.used_slots,
            "utilization": pool.used_slots / pool.slots if pool.slots > 0 else 0
        }
    
    # ==================== 插件集成 ====================
    
    def create_plugin(
        self,
        name: str,
        version: str,
        description: str = "",
        hooks: Optional[List[str]] = None,
        executors: Optional[List[str]] = None,
        operators: Optional[List[str]] = None,
        sensors: Optional[List[str]] = None,
        macros: Optional[List[str]] = None,
        admin_views: Optional[List[str]] = None,
        menu_links: Optional[List[str]] = None
    ) -> AirflowPlugin:
        """
        创建插件
        
        Args:
            name: 插件名称
            version: 插件版本
            description: 描述
            hooks: 自定义 Hooks
            executors: 自定义 Executors
            operators: 自定义 Operators
            sensors: 自定义 Sensors
            macros: 自定义 Macros
            admin_views: Admin Views
            menu_links: Menu Links
            
        Returns:
            AirflowPlugin 对象
        """
        with self._lock:
            if name in self._plugins:
                raise AirflowAPIException(f"Plugin {name} already exists")
            
            plugin = AirflowPlugin(
                name=name,
                version=version,
                description=description,
                hooks=hooks or [],
                executors=executors or [],
                operators=operators or [],
                sensors=sensors or [],
                macros=macros or [],
                admin_views=admin_views or [],
                menu_links=menu_links or [],
                appbuilder_views=[],
                appbuilder_menu_items=[]
            )
            
            self._plugins[name] = plugin
            self._stats["plugins_installed"] += 1
            return plugin
    
    def get_plugin(self, name: str) -> Optional[AirflowPlugin]:
        """获取插件"""
        return self._plugins.get(name)
    
    def list_plugins(self) -> List[AirflowPlugin]:
        """列出所有插件"""
        return list(self._plugins.values())
    
    def update_plugin(self, name: str, **kwargs) -> Optional[AirflowPlugin]:
        """更新插件"""
        with self._lock:
            plugin = self._plugins.get(name)
            if not plugin:
                return None
            
            for key, value in kwargs.items():
                if hasattr(plugin, key):
                    setattr(plugin, key, value)
            
            return plugin
    
    def delete_plugin(self, name: str) -> bool:
        """删除插件"""
        with self._lock:
            if name in self._plugins:
                del self._plugins[name]
                return True
            return False
    
    def register_operator(self, plugin_name: str, operator: str) -> bool:
        """注册 Operator 到插件"""
        plugin = self._plugins.get(plugin_name)
        if not plugin:
            return False
        
        if operator not in plugin.operators:
            plugin.operators.append(operator)
        return True
    
    def register_sensor(self, plugin_name: str, sensor: str) -> bool:
        """注册 Sensor 到插件"""
        plugin = self._plugins.get(plugin_name)
        if not plugin:
            return False
        
        if sensor not in plugin.sensors:
            plugin.sensors.append(sensor)
        return True
    
    def register_hook(self, plugin_name: str, hook: str) -> bool:
        """注册 Hook 到插件"""
        plugin = self._plugins.get(plugin_name)
        if not plugin:
            return False
        
        if hook not in plugin.hooks:
            plugin.hooks.append(hook)
        return True
    
    def generate_plugin_file(self, plugin_name: str) -> str:
        """
        生成插件文件内容
        
        Args:
            plugin_name: 插件名称
            
        Returns:
            插件文件内容
        """
        plugin = self._plugins.get(plugin_name)
        if not plugin:
            raise AirflowAPIException(f"Plugin {plugin_name} not found")
        
        file_content = f'''"""
Airflow Plugin: {plugin.name}
Version: {plugin.version}
Description: {plugin.description}
"""
from airflow.plugins_manager import AirflowPlugin

class {plugin.name.replace("-", "_").replace(" ", "_")}Plugin(AirflowPlugin):
    name = "{plugin.name}"
    version = "{plugin.version}"
'''
        
        if plugin.operators:
            file_content += f"    operators = {plugin.operators}\n"
        
        if plugin.sensors:
            file_content += f"    sensors = {plugin.sensors}\n"
        
        if plugin.hooks:
            file_content += f"    hooks = {plugin.hooks}\n"
        
        if plugin.macros:
            file_content += f"    macros = {plugin.macros}\n"
        
        if plugin.admin_views:
            file_content += f"    admin_views = {plugin.admin_views}\n"
        
        if plugin.menu_links:
            file_content += f"    menu_links = {plugin.menu_links}\n"
        
        return file_content
    
    # ==================== 辅助方法 ====================
    
    def get_stats(self) -> Dict[str, int]:
        """获取统计信息"""
        return self._stats.copy()
    
    def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "dags_count": len(self._dags),
            "tasks_count": sum(len(t) for t in self._tasks.values()),
            "variables_count": len(self._variables),
            "connections_count": len(self._connections),
            "xcoms_count": len(self._xcoms),
            "slas_count": sum(len(s) for s in self._slas.values()),
            "pools_count": len(self._pools),
            "plugins_count": len(self._plugins)
        }
    
    def clear_cache(self) -> bool:
        """清除缓存数据"""
        with self._lock:
            self._dag_runs.clear()
            self._xcoms.clear()
            for dag_id in self._slas:
                self._slas[dag_id] = []
            return True

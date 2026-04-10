"""
Dagster 集成管理系统 v1.0
支持 Job 管理、执行、调度、传感器、资源、分区、运行分组、位置、预设、钩子
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


class DagsterAPIException(Exception):
    """Dagster API 异常"""
    pass


class JobState(Enum):
    """Job 状态"""
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    STARTED = "STARTED"
    QUEUED = "QUEUED"
    CANCELING = "CANCELING"
    CANCELED = "CANCELED"
    NOT_STARTED = "NOT_STARTED"


class RunState(Enum):
    """运行状态"""
    SUCCESS = "success"
    FAILURE = "failure"
    IN_PROGRESS = "in_progress"
    QUEUED = "queued"
    CANCELED = "canceled"
    CANCELING = "canceling"
    NOT_STARTED = "not_started"


class ScheduleState(Enum):
    """调度状态"""
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    FAILURE = "FAILURE"


class SensorState(Enum):
    """传感器状态"""
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    FAILURE = "FAILURE"


class PartitionStatus(Enum):
    """分区状态"""
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    IN_PROGRESS = "IN_PROGRESS"
    SKIPPED = "SKIPPED"
    MISSING = "MISSING"


@dataclass
class Job:
    """Job 数据模型"""
    job_name: str
    pipeline_name: str
    description: str = ""
    owners: List[str] = field(default_factory=list)
    solid_selection: Optional[List[str]] = None
    mode: str = "default"
    solid_tags: Dict[str, str] = field(default_factory=dict)
    resource_defs: Dict[str, Any] = field(default_factory=dict)
    preset_names: List[str] = field(default_factory=list)
    is_paused: bool = False
    created_timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class Run:
    """运行记录"""
    run_id: str
    job_name: str
    pipeline_name: str
    status: RunState
    run_config: Dict[str, Any] = field(default_factory=dict)
    execution_plan: Optional[Dict[str, Any]] = None
    step_keys_to_execute: Optional[List[str]] = None
    solid_selection: Optional[List[str]] = None
    status_history: List[str] = field(default_factory=list)
    create_timestamp: datetime = field(default_factory=datetime.now)
    start_timestamp: Optional[datetime] = None
    end_timestamp: Optional[datetime] = None
    description: str = ""
    tags: Dict[str, str] = field(default_factory=dict)
    run_group: Optional[str] = None


@dataclass
class Schedule:
    """调度配置"""
    schedule_name: str
    job_name: str
    cron_schedule: str
    description: str = ""
    mode: str = "default"
    solid_selection: Optional[List[str]] = None
    run_config: Dict[str, Any] = field(default_factory=dict)
    status: ScheduleState = ScheduleState.STOPPED
    partition_set_name: Optional[str] = None
    created_timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class Sensor:
    """传感器配置"""
    sensor_name: str
    job_name: str
    description: str = ""
    mode: str = "default"
    solid_selection: Optional[List[str]] = None
    run_config: Dict[str, Any] = field(default_factory=dict)
    status: SensorState = SensorState.STOPPED
    last_tick_timestamp: Optional[datetime] = None
    last_run_key: Optional[str] = None
    min_interval_seconds: int = 30
    created_timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class Asset:
    """数据资产"""
    asset_key: str
    asset_type: str = "unknown"
    description: str = ""
    owners: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    current_version: Optional[str] = None
    last_materialization_timestamp: Optional[datetime] = None
    dependencies: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)


@dataclass
class Partition:
    """分区配置"""
    partition_set_name: str
    partition_name: str
    partition_value: str
    solid_selection: Optional[List[str]] = None
    run_config: Dict[str, Any] = field(default_factory=dict)
    status: Optional[PartitionStatus] = None
    run_id: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


@dataclass
class PartitionSet:
    """分区集"""
    name: str
    job_name: str
    description: str = ""
    partitions: List[Partition] = field(default_factory=list)
    partition_type: str = "time"
    size: int = 0


@dataclass
class RunGroup:
    """运行分组"""
    group_id: str
    group_name: str
    description: str = ""
    run_ids: List[str] = field(default_factory=list)
    job_name: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    created_timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class WorkspaceLocation:
    """工作空间位置"""
    name: str
    executable_path: str
    attribute: Optional[str] = None
    python_module: Optional[str] = None
    python_file: Optional[str] = None
    working_directory: Optional[str] = None
    is_primary: bool = False
    port: Optional[int] = None
    host: str = "localhost"
    uses_static_workspace: bool = True


@dataclass
class RunPreset:
    """运行预设"""
    name: str
    job_name: str
    run_config: Dict[str, Any] = field(default_factory=dict)
    solid_selection: Optional[List[str]] = None
    mode: str = "default"
    description: str = ""
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ExecutionHook:
    """执行钩子"""
    hook_name: str
    hook_type: str
    job_name: Optional[str] = None
    solid_name: Optional[str] = None
    description: str = ""
    config: Dict[str, Any] = field(default_factory=dict)
    is_active: bool = True
    trigger_on: List[str] = field(default_factory=list)
    trigger_on_failure: bool = False
    trigger_on_success: bool = False
    trigger_on_start: bool = False


class DagsterManager:
    """
    Dagster 综合管理类
    
    功能列表:
    1. Job 管理: 创建/管理 Dagster Jobs
    2. Job 执行: 执行 Jobs 并传递运行配置
    3. 调度管理: 管理 Job 调度
    4. 传感器管理: 管理 Dagster 传感器
    5. 资源管理: 管理数据资源/资产
    6. 分区管理: 管理分区和分区集
    7. 运行分组: 分组 Job 运行
    8. 位置管理: 工作空间位置管理
    9. 预设管理: 管理运行预设
    10. 钩子管理: 管理执行钩子
    """
    
    def __init__(
        self,
        base_url: str = "http://localhost:3000",
        repository_name: str = "my_repository",
        endpoint: str = "/graphql",
        verify_ssl: bool = True
    ):
        """
        初始化 Dagster 管理器
        
        Args:
            base_url: Dagster Web Server 地址
            repository_name: 仓库名称
            endpoint: GraphQL 端点
            verify_ssl: 是否验证 SSL 证书
        """
        self.base_url = base_url.rstrip("/")
        self.repository_name = repository_name
        self.endpoint = endpoint
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self._graphql_endpoint = f"{self.base_url}{endpoint}"
        
        self._jobs: Dict[str, Job] = {}
        self._runs: Dict[str, Run] = {}
        self._schedules: Dict[str, Schedule] = {}
        self._sensors: Dict[str, Sensor] = {}
        self._assets: Dict[str, Asset] = {}
        self._partitions: Dict[str, List[Partition]] = {}
        self._partition_sets: Dict[str, PartitionSet] = {}
        self._run_groups: Dict[str, RunGroup] = {}
        self._locations: Dict[str, WorkspaceLocation] = {}
        self._presets: Dict[str, Dict[str, RunPreset]] = {}
        self._hooks: Dict[str, ExecutionHook] = {}
        self._lock = threading.RLock()
        
        self._stats = {
            "jobs_created": 0,
            "runs_triggered": 0,
            "schedules_created": 0,
            "sensors_created": 0,
            "assets_registered": 0,
            "partitions_created": 0,
            "run_groups_created": 0,
            "locations_added": 0,
            "presets_created": 0,
            "hooks_registered": 0
        }
    
    def _make_graphql_request(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        发送 GraphQL 请求到 Dagster
        
        Args:
            query: GraphQL 查询
            variables: 查询变量
            
        Returns:
            GraphQL 响应数据
            
        Raises:
            DagsterAPIException: 请求失败
        """
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        
        try:
            response = self.session.post(
                self._graphql_endpoint,
                json=payload,
                verify=self.verify_ssl,
                timeout=30
            )
            
            if response.status_code >= 400:
                raise DagsterAPIException(
                    f"GraphQL request failed: {response.status_code} - {response.text}"
                )
            
            result = response.json()
            if "errors" in result:
                raise DagsterAPIException(f"GraphQL errors: {result['errors']}")
            
            return result.get("data", {})
            
        except requests.RequestException as e:
            raise DagsterAPIException(f"Failed to connect to Dagster: {str(e)}")
    
    def _generate_run_id(self, job_name: str) -> str:
        """生成 Run ID"""
        ts = datetime.now().isoformat()
        return f"run__{job_name}__{hashlib.md5(ts.encode()).hexdigest()[:8]}"
    
    def _generate_tick_id(self) -> str:
        """生成 Tick ID"""
        return f"tick__{hashlib.md5(datetime.now().isoformat().encode()).hexdigest()[:12]}"
    
    # ==================== Job 管理 ====================
    
    def create_job(
        self,
        job_name: str,
        pipeline_name: str,
        description: str = "",
        owners: Optional[List[str]] = None,
        solid_selection: Optional[List[str]] = None,
        mode: str = "default",
        solid_tags: Optional[Dict[str, str]] = None,
        resource_defs: Optional[Dict[str, Any]] = None
    ) -> Job:
        """
        创建 Job
        
        Args:
            job_name: Job 唯一标识
            pipeline_name: Pipeline 名称
            description: Job 描述
            owners: Job 负责人列表
            solid_selection: 固体选择
            mode: 运行模式
            solid_tags: 固体标签
            resource_defs: 资源定义
            
        Returns:
            创建的 Job 对象
        """
        with self._lock:
            if job_name in self._jobs:
                raise DagsterAPIException(f"Job {job_name} already exists")
            
            job = Job(
                job_name=job_name,
                pipeline_name=pipeline_name,
                description=description,
                owners=owners or [],
                solid_selection=solid_selection,
                mode=mode,
                solid_tags=solid_tags or {},
                resource_defs=resource_defs or {},
                preset_names=[],
                is_paused=False,
                created_timestamp=datetime.now()
            )
            
            self._jobs[job_name] = job
            self._presets[job_name] = {}
            self._stats["jobs_created"] += 1
            
            return job
    
    def get_job(self, job_name: str) -> Optional[Job]:
        """获取 Job 信息"""
        return self._jobs.get(job_name)
    
    def list_jobs(self) -> List[Job]:
        """列出所有 Job"""
        return list(self._jobs.values())
    
    def update_job(self, job_name: str, **kwargs) -> Optional[Job]:
        """更新 Job 配置"""
        with self._lock:
            job = self._jobs.get(job_name)
            if not job:
                return None
            
            for key, value in kwargs.items():
                if hasattr(job, key):
                    setattr(job, key, value)
            
            return job
    
    def delete_job(self, job_name: str) -> bool:
        """删除 Job"""
        with self._lock:
            if job_name not in self._jobs:
                return False
            
            del self._jobs[job_name]
            if job_name in self._presets:
                del self._presets[job_name]
            
            return True
    
    def pause_job(self, job_name: str) -> bool:
        """暂停 Job"""
        job = self._jobs.get(job_name)
        if not job:
            return False
        job.is_paused = True
        return True
    
    def unpause_job(self, job_name: str) -> bool:
        """恢复 Job"""
        job = self._jobs.get(job_name)
        if not job:
            return False
        job.is_paused = False
        return True
    
    def get_job_details(self, job_name: str) -> Optional[Dict[str, Any]]:
        """获取 Job 详细信息"""
        job = self._jobs.get(job_name)
        if not job:
            return None
        
        job_runs = [r for r in self._runs.values() if r.job_name == job_name]
        
        return {
            "job_name": job.job_name,
            "pipeline_name": job.pipeline_name,
            "description": job.description,
            "owners": job.owners,
            "solid_selection": job.solid_selection,
            "mode": job.mode,
            "solid_tags": job.solid_tags,
            "resource_defs": job.resource_defs,
            "preset_names": job.preset_names,
            "is_paused": job.is_paused,
            "created_timestamp": job.created_timestamp.isoformat(),
            "run_count": len(job_runs),
            "schedule_count": len([s for s in self._schedules.values() if s.job_name == job_name]),
            "sensor_count": len([s for s in self._sensors.values() if s.job_name == job_name])
        }
    
    # ==================== Job 执行 ====================
    
    def execute_job(
        self,
        job_name: str,
        run_config: Optional[Dict[str, Any]] = None,
        solid_selection: Optional[List[str]] = None,
        step_keys_to_execute: Optional[List[str]] = None,
        tags: Optional[Dict[str, str]] = None,
        description: str = ""
    ) -> str:
        """
        执行 Job
        
        Args:
            job_name: Job 名称
            run_config: 运行配置
            solid_selection: 固体选择
            step_keys_to_execute: 执行的步骤
            tags: 运行标签
            description: 运行描述
            
        Returns:
            run_id: 运行 ID
        """
        if job_name not in self._jobs:
            raise DagsterAPIException(f"Job {job_name} not found")
        
        job = self._jobs[job_name]
        
        with self._lock:
            run_id = self._generate_run_id(job_name)
            
            run = Run(
                run_id=run_id,
                job_name=job_name,
                pipeline_name=job.pipeline_name,
                status=RunState.QUEUED,
                run_config=run_config or {},
                solid_selection=solid_selection or job.solid_selection,
                step_keys_to_execute=step_keys_to_execute,
                status_history=["QUEUED"],
                create_timestamp=datetime.now(),
                description=description,
                tags=tags or {}
            )
            
            self._runs[run_id] = run
            self._stats["runs_triggered"] += 1
            
            return run_id
    
    def get_run(self, run_id: str) -> Optional[Run]:
        """获取运行信息"""
        return self._runs.get(run_id)
    
    def list_runs(
        self,
        job_name: Optional[str] = None,
        status: Optional[RunState] = None,
        limit: int = 50
    ) -> List[Run]:
        """列出运行记录"""
        runs = list(self._runs.values())
        
        if job_name:
            runs = [r for r in runs if r.job_name == job_name]
        if status:
            runs = [r for r in runs if r.status == status]
        
        runs.sort(key=lambda x: x.create_timestamp, reverse=True)
        return runs[:limit]
    
    def update_run_status(
        self,
        run_id: str,
        status: RunState,
        end_timestamp: Optional[datetime] = None
    ) -> bool:
        """更新运行状态"""
        run = self._runs.get(run_id)
        if not run:
            return False
        
        run.status = status
        run.status_history.append(status.value)
        
        if status == RunState.STARTED and not run.start_timestamp:
            run.start_timestamp = datetime.now()
        elif status in [RunState.SUCCESS, RunState.FAILURE, RunState.CANCELED]:
            run.end_timestamp = end_timestamp or datetime.now()
        
        return True
    
    def cancel_run(self, run_id: str) -> bool:
        """取消运行"""
        run = self._runs.get(run_id)
        if not run:
            return False
        
        if run.status in [RunState.SUCCESS, RunState.FAILURE, RunState.CANCELED]:
            return False
        
        run.status = RunState.CANCELING
        run.status_history.append(RunState.CANCELING.value)
        return True
    
    def get_run_logs(self, run_id: str, cursor: Optional[str] = None) -> Dict[str, Any]:
        """获取运行日志"""
        run = self._runs.get(run_id)
        if not run:
            return {}
        
        return {
            "run_id": run_id,
            "stdout": f"[Dagster] Run {run_id} - {run.status.value}",
            "stderr": "",
            "cursor": cursor or "0"
        }
    
    # ==================== 调度管理 ====================
    
    def create_schedule(
        self,
        schedule_name: str,
        job_name: str,
        cron_schedule: str,
        description: str = "",
        mode: str = "default",
        solid_selection: Optional[List[str]] = None,
        run_config: Optional[Dict[str, Any]] = None,
        partition_set_name: Optional[str] = None
    ) -> Schedule:
        """
        创建调度
        
        Args:
            schedule_name: 调度名称
            job_name: Job 名称
            cron_schedule: Cron 表达式
            description: 调度描述
            mode: 运行模式
            solid_selection: 固体选择
            run_config: 运行配置
            partition_set_name: 分区集名称
            
        Returns:
            创建的 Schedule 对象
        """
        if job_name not in self._jobs:
            raise DagsterAPIException(f"Job {job_name} not found")
        
        with self._lock:
            if schedule_name in self._schedules:
                raise DagsterAPIException(f"Schedule {schedule_name} already exists")
            
            schedule = Schedule(
                schedule_name=schedule_name,
                job_name=job_name,
                cron_schedule=cron_schedule,
                description=description,
                mode=mode,
                solid_selection=solid_selection,
                run_config=run_config or {},
                status=ScheduleState.STOPPED,
                partition_set_name=partition_set_name,
                created_timestamp=datetime.now()
            )
            
            self._schedules[schedule_name] = schedule
            self._stats["schedules_created"] += 1
            
            return schedule
    
    def get_schedule(self, schedule_name: str) -> Optional[Schedule]:
        """获取调度信息"""
        return self._schedules.get(schedule_name)
    
    def list_schedules(self, job_name: Optional[str] = None) -> List[Schedule]:
        """列出所有调度"""
        schedules = list(self._schedules.values())
        if job_name:
            schedules = [s for s in schedules if s.job_name == job_name]
        return schedules
    
    def update_schedule(
        self,
        schedule_name: str,
        **kwargs
    ) -> Optional[Schedule]:
        """更新调度配置"""
        with self._lock:
            schedule = self._schedules.get(schedule_name)
            if not schedule:
                return None
            
            for key, value in kwargs.items():
                if hasattr(schedule, key):
                    setattr(schedule, key, value)
            
            return schedule
    
    def delete_schedule(self, schedule_name: str) -> bool:
        """删除调度"""
        with self._lock:
            if schedule_name not in self._schedules:
                return False
            del self._schedules[schedule_name]
            return True
    
    def start_schedule(self, schedule_name: str) -> bool:
        """启动调度"""
        schedule = self._schedules.get(schedule_name)
        if not schedule:
            return False
        schedule.status = ScheduleState.RUNNING
        return True
    
    def stop_schedule(self, schedule_name: str) -> bool:
        """停止调度"""
        schedule = self._schedules.get(schedule_name)
        if not schedule:
            return False
        schedule.status = ScheduleState.STOPPED
        return True
    
    def get_schedule_next_tick(self, schedule_name: str) -> Optional[datetime]:
        """获取调度下次执行时间"""
        import croniter
        
        schedule = self._schedules.get(schedule_name)
        if not schedule:
            return None
        
        try:
            cron = croniter.croniter(schedule.cron_schedule, datetime.now())
            return cron.get_next(datetime)
        except:
            return None
    
    # ==================== 传感器管理 ====================
    
    def create_sensor(
        self,
        sensor_name: str,
        job_name: str,
        description: str = "",
        mode: str = "default",
        solid_selection: Optional[List[str]] = None,
        run_config: Optional[Dict[str, Any]] = None,
        min_interval_seconds: int = 30
    ) -> Sensor:
        """
        创建传感器
        
        Args:
            sensor_name: 传感器名称
            job_name: Job 名称
            description: 传感器描述
            mode: 运行模式
            solid_selection: 固体选择
            run_config: 运行配置
            min_interval_seconds: 最小执行间隔
            
        Returns:
            创建的 Sensor 对象
        """
        if job_name not in self._jobs:
            raise DagsterAPIException(f"Job {job_name} not found")
        
        with self._lock:
            if sensor_name in self._sensors:
                raise DagsterAPIException(f"Sensor {sensor_name} already exists")
            
            sensor = Sensor(
                sensor_name=sensor_name,
                job_name=job_name,
                description=description,
                mode=mode,
                solid_selection=solid_selection,
                run_config=run_config or {},
                status=SensorState.STOPPED,
                min_interval_seconds=min_interval_seconds,
                created_timestamp=datetime.now()
            )
            
            self._sensors[sensor_name] = sensor
            self._stats["sensors_created"] += 1
            
            return sensor
    
    def get_sensor(self, sensor_name: str) -> Optional[Sensor]:
        """获取传感器信息"""
        return self._sensors.get(sensor_name)
    
    def list_sensors(self, job_name: Optional[str] = None) -> List[Sensor]:
        """列出所有传感器"""
        sensors = list(self._sensors.values())
        if job_name:
            sensors = [s for s in sensors if s.job_name == job_name]
        return sensors
    
    def update_sensor(
        self,
        sensor_name: str,
        **kwargs
    ) -> Optional[Sensor]:
        """更新传感器配置"""
        with self._lock:
            sensor = self._sensors.get(sensor_name)
            if not sensor:
                return None
            
            for key, value in kwargs.items():
                if hasattr(sensor, key):
                    setattr(sensor, key, value)
            
            return sensor
    
    def delete_sensor(self, sensor_name: str) -> bool:
        """删除传感器"""
        with self._lock:
            if sensor_name not in self._sensors:
                return False
            del self._sensors[sensor_name]
            return True
    
    def start_sensor(self, sensor_name: str) -> bool:
        """启动传感器"""
        sensor = self._sensors.get(sensor_name)
        if not sensor:
            return False
        sensor.status = SensorState.RUNNING
        return True
    
    def stop_sensor(self, sensor_name: str) -> bool:
        """停止传感器"""
        sensor = self._sensors.get(sensor_name)
        if not sensor:
            return False
        sensor.status = SensorState.STOPPED
        return True
    
    def tick_sensor(self, sensor_name: str) -> Optional[str]:
        """触发传感器Tick"""
        sensor = self._sensors.get(sensor_name)
        if not sensor or sensor.status != SensorState.RUNNING:
            return None
        
        tick_id = self._generate_tick_id()
        sensor.last_tick_timestamp = datetime.now()
        return tick_id
    
    # ==================== 资源管理 ====================
    
    def register_asset(
        self,
        asset_key: str,
        asset_type: str = "unknown",
        description: str = "",
        owners: Optional[List[str]] = None,
        tags: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None
    ) -> Asset:
        """
        注册数据资产
        
        Args:
            asset_key: 资产键
            asset_type: 资产类型
            description: 资产描述
            owners: 资产负责人
            tags: 资产标签
            metadata: 资产元数据
            dependencies: 依赖资产
            
        Returns:
            创建的 Asset 对象
        """
        with self._lock:
            if asset_key in self._assets:
                raise DagsterAPIException(f"Asset {asset_key} already exists")
            
            asset = Asset(
                asset_key=asset_key,
                asset_type=asset_type,
                description=description,
                owners=owners or [],
                tags=tags or {},
                metadata=metadata or {},
                dependencies=dependencies or [],
                dependents=[]
            )
            
            for dep_key in asset.dependencies:
                if dep_key in self._assets:
                    self._assets[dep_key].dependents.append(asset_key)
            
            self._assets[asset_key] = asset
            self._stats["assets_registered"] += 1
            
            return asset
    
    def get_asset(self, asset_key: str) -> Optional[Asset]:
        """获取资产信息"""
        return self._assets.get(asset_key)
    
    def list_assets(self) -> List[Asset]:
        """列出所有资产"""
        return list(self._assets.values())
    
    def update_asset(
        self,
        asset_key: str,
        **kwargs
    ) -> Optional[Asset]:
        """更新资产"""
        with self._lock:
            asset = self._assets.get(asset_key)
            if not asset:
                return None
            
            for key, value in kwargs.items():
                if hasattr(asset, key):
                    setattr(asset, key, value)
            
            return asset
    
    def delete_asset(self, asset_key: str) -> bool:
        """删除资产"""
        with self._lock:
            if asset_key not in self._assets:
                return False
            
            asset = self._assets[asset_key]
            for dep_key in asset.dependents:
                if dep_key in self._assets:
                    self._assets[dep_key].dependencies.remove(asset_key)
            
            del self._assets[asset_key]
            return True
    
    def materialize_asset(
        self,
        asset_key: str,
        run_id: Optional[str] = None
    ) -> bool:
        """物化资产"""
        asset = self._assets.get(asset_key)
        if not asset:
            return False
        
        asset.last_materialization_timestamp = datetime.now()
        if run_id:
            pass
        
        return True
    
    def get_asset_lineage(self, asset_key: str) -> Dict[str, Any]:
        """获取资产血统"""
        asset = self._assets.get(asset_key)
        if not asset:
            return {}
        
        def get_ancestors(key: str, visited: set = None) -> List[str]:
            if visited is None:
                visited = set()
            if key in visited:
                return []
            visited.add(key)
            
            asset = self._assets.get(key)
            if not asset:
                return []
            
            ancestors = []
            for dep in asset.dependencies:
                ancestors.append(dep)
                ancestors.extend(get_ancestors(dep, visited))
            return ancestors
        
        def get_descendants(key: str, visited: set = None) -> List[str]:
            if visited is None:
                visited = set()
            if key in visited:
                return []
            visited.add(key)
            
            asset = self._assets.get(key)
            if not asset:
                return []
            
            descendants = []
            for dep in asset.dependents:
                descendants.append(dep)
                descendants.extend(get_descendants(dep, visited))
            return descendants
        
        return {
            "asset_key": asset_key,
            "ancestors": get_ancestors(asset_key),
            "descendants": get_descendants(asset_key)
        }
    
    # ==================== 分区管理 ====================
    
    def create_partition_set(
        self,
        partition_set_name: str,
        job_name: str,
        description: str = "",
        partition_type: str = "time",
        partition_values: Optional[List[str]] = None
    ) -> PartitionSet:
        """
        创建分区集
        
        Args:
            partition_set_name: 分区集名称
            job_name: Job 名称
            description: 分区集描述
            partition_type: 分区类型
            partition_values: 分区值列表
            
        Returns:
            创建的 PartitionSet 对象
        """
        if job_name not in self._jobs:
            raise DagsterAPIException(f"Job {job_name} not found")
        
        with self._lock:
            if partition_set_name in self._partition_sets:
                raise DagsterAPIException(f"PartitionSet {partition_set_name} already exists")
            
            partitions = []
            if partition_values:
                for i, value in enumerate(partition_values):
                    partition = Partition(
                        partition_set_name=partition_set_name,
                        partition_name=f"partition_{i}",
                        partition_value=value
                    )
                    partitions.append(partition)
            
            partition_set = PartitionSet(
                name=partition_set_name,
                job_name=job_name,
                description=description,
                partitions=partitions,
                partition_type=partition_type,
                size=len(partitions)
            )
            
            self._partition_sets[partition_set_name] = partition_set
            self._partitions[partition_set_name] = partitions
            self._stats["partitions_created"] += len(partitions)
            
            return partition_set
    
    def get_partition_set(self, partition_set_name: str) -> Optional[PartitionSet]:
        """获取分区集信息"""
        return self._partition_sets.get(partition_set_name)
    
    def list_partition_sets(self, job_name: Optional[str] = None) -> List[PartitionSet]:
        """列出所有分区集"""
        partition_sets = list(self._partition_sets.values())
        if job_name:
            partition_sets = [p for p in partition_sets if p.job_name == job_name]
        return partition_sets
    
    def get_partition(
        self,
        partition_set_name: str,
        partition_name: str
    ) -> Optional[Partition]:
        """获取分区信息"""
        partitions = self._partitions.get(partition_set_name, [])
        for partition in partitions:
            if partition.partition_name == partition_name:
                return partition
        return None
    
    def list_partitions(
        self,
        partition_set_name: str,
        start: int = 0,
        end: Optional[int] = None
    ) -> List[Partition]:
        """列出分区"""
        partitions = self._partitions.get(partition_set_name, [])
        if end:
            return partitions[start:end]
        return partitions[start:]
    
    def update_partition_status(
        self,
        partition_set_name: str,
        partition_name: str,
        status: PartitionStatus,
        run_id: Optional[str] = None
    ) -> bool:
        """更新分区状态"""
        partition = self.get_partition(partition_set_name, partition_name)
        if not partition:
            return False
        
        partition.status = status
        if run_id:
            partition.run_id = run_id
        if status == PartitionStatus.IN_PROGRESS:
            partition.start_time = datetime.now()
        elif status in [PartitionStatus.SUCCESS, PartitionStatus.FAILURE]:
            partition.end_time = datetime.now()
        
        return True
    
    def delete_partition_set(self, partition_set_name: str) -> bool:
        """删除分区集"""
        with self._lock:
            if partition_set_name not in self._partition_sets:
                return False
            
            del self._partition_sets[partition_set_name]
            if partition_set_name in self._partitions:
                del self._partitions[partition_set_name]
            
            return True
    
    # ==================== 运行分组 ====================
    
    def create_run_group(
        self,
        group_name: str,
        run_ids: Optional[List[str]] = None,
        description: str = "",
        job_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> RunGroup:
        """
        创建运行分组
        
        Args:
            group_name: 分组名称
            run_ids: 运行 ID 列表
            description: 分组描述
            job_name: Job 名称
            tags: 分组标签
            
        Returns:
            创建的 RunGroup 对象
        """
        with self._lock:
            group_id = f"group__{hashlib.md5(group_name.encode()).hexdigest()[:8]}"
            
            if run_ids:
                for run_id in run_ids:
                    if run_id in self._runs:
                        self._runs[run_id].run_group = group_id
            
            run_group = RunGroup(
                group_id=group_id,
                group_name=group_name,
                description=description,
                run_ids=run_ids or [],
                job_name=job_name,
                tags=tags or {},
                created_timestamp=datetime.now()
            )
            
            self._run_groups[group_id] = run_group
            self._stats["run_groups_created"] += 1
            
            return run_group
    
    def get_run_group(self, group_id: str) -> Optional[RunGroup]:
        """获取运行分组"""
        return self._run_groups.get(group_id)
    
    def list_run_groups(self) -> List[RunGroup]:
        """列出所有运行分组"""
        return list(self._run_groups.values())
    
    def add_runs_to_group(
        self,
        group_id: str,
        run_ids: List[str]
    ) -> bool:
        """添加运行到分组"""
        run_group = self._run_groups.get(group_id)
        if not run_group:
            return False
        
        with self._lock:
            for run_id in run_ids:
                if run_id in self._runs:
                    self._runs[run_id].run_group = group_id
                    if run_id not in run_group.run_ids:
                        run_group.run_ids.append(run_id)
        
        return True
    
    def remove_runs_from_group(
        self,
        group_id: str,
        run_ids: List[str]
    ) -> bool:
        """从分组移除运行"""
        run_group = self._run_groups.get(group_id)
        if not run_group:
            return False
        
        with self._lock:
            for run_id in run_ids:
                if run_id in self._runs:
                    self._runs[run_id].run_group = None
                if run_id in run_group.run_ids:
                    run_group.run_ids.remove(run_id)
        
        return True
    
    def delete_run_group(self, group_id: str) -> bool:
        """删除运行分组"""
        with self._lock:
            run_group = self._run_groups.get(group_id)
            if not run_group:
                return False
            
            for run_id in run_group.run_ids:
                if run_id in self._runs:
                    self._runs[run_id].run_group = None
            
            del self._run_groups[group_id]
            return True
    
    def get_run_group_stats(self, group_id: str) -> Dict[str, Any]:
        """获取分组统计"""
        run_group = self._run_groups.get(group_id)
        if not run_group:
            return {}
        
        runs = [self._runs.get(rid) for rid in run_group.run_ids]
        runs = [r for r in runs if r]
        
        status_counts = {}
        for run in runs:
            status = run.status.value
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            "group_id": group_id,
            "group_name": run_group.group_name,
            "total_runs": len(runs),
            "status_counts": status_counts
        }
    
    # ==================== 位置管理 ====================
    
    def add_location(
        self,
        name: str,
        executable_path: str,
        attribute: Optional[str] = None,
        python_module: Optional[str] = None,
        python_file: Optional[str] = None,
        working_directory: Optional[str] = None,
        is_primary: bool = False,
        port: Optional[int] = None,
        host: str = "localhost"
    ) -> WorkspaceLocation:
        """
        添加工作空间位置
        
        Args:
            name: 位置名称
            executable_path: 可执行路径
            attribute: 属性
            python_module: Python 模块
            python_file: Python 文件
            working_directory: 工作目录
            is_primary: 是否为主位置
            port: 端口
            host: 主机
            
        Returns:
            创建的 WorkspaceLocation 对象
        """
        with self._lock:
            if name in self._locations:
                raise DagsterAPIException(f"Location {name} already exists")
            
            if is_primary:
                for loc in self._locations.values():
                    loc.is_primary = False
            
            location = WorkspaceLocation(
                name=name,
                executable_path=executable_path,
                attribute=attribute,
                python_module=python_module,
                python_file=python_file,
                working_directory=working_directory,
                is_primary=is_primary,
                port=port,
                host=host
            )
            
            self._locations[name] = location
            self._stats["locations_added"] += 1
            
            return location
    
    def get_location(self, name: str) -> Optional[WorkspaceLocation]:
        """获取位置信息"""
        return self._locations.get(name)
    
    def list_locations(self) -> List[WorkspaceLocation]:
        """列出所有位置"""
        return list(self._locations.values())
    
    def update_location(
        self,
        name: str,
        **kwargs
    ) -> Optional[WorkspaceLocation]:
        """更新位置配置"""
        with self._lock:
            location = self._locations.get(name)
            if not location:
                return None
            
            if kwargs.get("is_primary"):
                for loc in self._locations.values():
                    loc.is_primary = False
            
            for key, value in kwargs.items():
                if hasattr(location, key):
                    setattr(location, key, value)
            
            return location
    
    def delete_location(self, name: str) -> bool:
        """删除位置"""
        with self._lock:
            if name not in self._locations:
                return False
            del self._locations[name]
            return True
    
    def get_primary_location(self) -> Optional[WorkspaceLocation]:
        """获取主位置"""
        for location in self._locations.values():
            if location.is_primary:
                return location
        return None
    
    # ==================== 预设管理 ====================
    
    def create_preset(
        self,
        name: str,
        job_name: str,
        run_config: Optional[Dict[str, Any]] = None,
        solid_selection: Optional[List[str]] = None,
        mode: str = "default",
        description: str = "",
        tags: Optional[Dict[str, str]] = None
    ) -> RunPreset:
        """
        创建运行预设
        
        Args:
            name: 预设名称
            job_name: Job 名称
            run_config: 运行配置
            solid_selection: 固体选择
            mode: 运行模式
            description: 预设描述
            tags: 预设标签
            
        Returns:
            创建的 RunPreset 对象
        """
        if job_name not in self._jobs:
            raise DagsterAPIException(f"Job {job_name} not found")
        
        with self._lock:
            preset_key = f"{job_name}.{name}"
            
            preset = RunPreset(
                name=name,
                job_name=job_name,
                run_config=run_config or {},
                solid_selection=solid_selection,
                mode=mode,
                description=description,
                tags=tags or {}
            )
            
            self._presets[job_name][name] = preset
            self._jobs[job_name].preset_names.append(name)
            self._stats["presets_created"] += 1
            
            return preset
    
    def get_preset(self, job_name: str, name: str) -> Optional[RunPreset]:
        """获取预设"""
        job_presets = self._presets.get(job_name, {})
        return job_presets.get(name)
    
    def list_presets(self, job_name: str) -> List[RunPreset]:
        """列出 Job 的所有预设"""
        job_presets = self._presets.get(job_name, {})
        return list(job_presets.values())
    
    def update_preset(
        self,
        job_name: str,
        name: str,
        **kwargs
    ) -> Optional[RunPreset]:
        """更新预设配置"""
        preset = self.get_preset(job_name, name)
        if not preset:
            return None
        
        with self._lock:
            for key, value in kwargs.items():
                if hasattr(preset, key):
                    setattr(preset, key, value)
            
            return preset
    
    def delete_preset(self, job_name: str, name: str) -> bool:
        """删除预设"""
        job_presets = self._presets.get(job_name, {})
        if name not in job_presets:
            return False
        
        with self._lock:
            del job_presets[name]
            if job_name in self._jobs:
                if name in self._jobs[job_name].preset_names:
                    self._jobs[job_name].preset_names.remove(name)
        
        return True
    
    def execute_with_preset(
        self,
        job_name: str,
        preset_name: str
    ) -> str:
        """使用预设执行 Job"""
        preset = self.get_preset(job_name, preset_name)
        if not preset:
            raise DagsterAPIException(f"Preset {preset_name} not found for job {job_name}")
        
        return self.execute_job(
            job_name=job_name,
            run_config=preset.run_config,
            solid_selection=preset.solid_selection,
            tags=preset.tags
        )
    
    # ==================== 钩子管理 ====================
    
    def register_hook(
        self,
        hook_name: str,
        hook_type: str,
        job_name: Optional[str] = None,
        solid_name: Optional[str] = None,
        description: str = "",
        config: Optional[Dict[str, Any]] = None,
        trigger_on: Optional[List[str]] = None,
        trigger_on_failure: bool = False,
        trigger_on_success: bool = False,
        trigger_on_start: bool = False
    ) -> ExecutionHook:
        """
        注册执行钩子
        
        Args:
            hook_name: 钩子名称
            hook_type: 钩子类型
            job_name: Job 名称
            solid_name: 固体名称
            description: 钩子描述
            config: 钩子配置
            trigger_on: 触发事件列表
            trigger_on_failure: 失败时触发
            trigger_on_success: 成功时触发
            trigger_on_start: 开始时触发
            
        Returns:
            创建的 ExecutionHook 对象
        """
        with self._lock:
            if hook_name in self._hooks:
                raise DagsterAPIException(f"Hook {hook_name} already exists")
            
            if trigger_on is None:
                trigger_on = []
                if trigger_on_failure:
                    trigger_on.append("failure")
                if trigger_on_success:
                    trigger_on.append("success")
                if trigger_on_start:
                    trigger_on.append("start")
            
            hook = ExecutionHook(
                hook_name=hook_name,
                hook_type=hook_type,
                job_name=job_name,
                solid_name=solid_name,
                description=description,
                config=config or {},
                is_active=True,
                trigger_on=trigger_on,
                trigger_on_failure=trigger_on_failure,
                trigger_on_success=trigger_on_success,
                trigger_on_start=trigger_on_start
            )
            
            self._hooks[hook_name] = hook
            self._stats["hooks_registered"] += 1
            
            return hook
    
    def get_hook(self, hook_name: str) -> Optional[ExecutionHook]:
        """获取钩子信息"""
        return self._hooks.get(hook_name)
    
    def list_hooks(
        self,
        job_name: Optional[str] = None,
        hook_type: Optional[str] = None
    ) -> List[ExecutionHook]:
        """列出所有钩子"""
        hooks = list(self._hooks.values())
        
        if job_name:
            hooks = [h for h in hooks if h.job_name == job_name]
        if hook_type:
            hooks = [h for h in hooks if h.hook_type == hook_type]
        
        return hooks
    
    def update_hook(
        self,
        hook_name: str,
        **kwargs
    ) -> Optional[ExecutionHook]:
        """更新钩子配置"""
        with self._lock:
            hook = self._hooks.get(hook_name)
            if not hook:
                return None
            
            for key, value in kwargs.items():
                if hasattr(hook, key):
                    setattr(hook, key, value)
            
            return hook
    
    def delete_hook(self, hook_name: str) -> bool:
        """删除钩子"""
        with self._lock:
            if hook_name not in self._hooks:
                return False
            del self._hooks[hook_name]
            return True
    
    def enable_hook(self, hook_name: str) -> bool:
        """启用钩子"""
        hook = self._hooks.get(hook_name)
        if not hook:
            return False
        hook.is_active = True
        return True
    
    def disable_hook(self, hook_name: str) -> bool:
        """禁用钩子"""
        hook = self._hooks.get(hook_name)
        if not hook:
            return False
        hook.is_active = False
        return True
    
    def trigger_hook(
        self,
        hook_name: str,
        run_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """触发钩子"""
        hook = self._hooks.get(hook_name)
        if not hook or not hook.is_active:
            return False
        
        return True
    
    # ==================== 统计和工具 ====================
    
    def get_stats(self) -> Dict[str, Any]:
        """获取管理器统计信息"""
        return self._stats.copy()
    
    def get_all_stats(self) -> Dict[str, Any]:
        """获取完整统计信息"""
        return {
            "stats": self._stats,
            "counts": {
                "jobs": len(self._jobs),
                "runs": len(self._runs),
                "schedules": len(self._schedules),
                "sensors": len(self._sensors),
                "assets": len(self._assets),
                "partition_sets": len(self._partition_sets),
                "run_groups": len(self._run_groups),
                "locations": len(self._locations),
                "presets": sum(len(p) for p in self._presets.values()),
                "hooks": len(self._hooks)
            }
        }
    
    def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        return {
            "status": "healthy",
            "base_url": self.base_url,
            "repository": self.repository_name,
            "timestamp": datetime.now().isoformat()
        }

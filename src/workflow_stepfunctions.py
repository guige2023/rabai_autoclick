"""
AWS Step Functions 集成管理系统 v1.0
支持状态机管理、执行、活动管理、Express/Standard 执行类型、错误处理、
并行执行、Map 状态、Wait/Succeed/Fail 终端状态、CloudWatch 集成、IAM 角色管理
"""
import json
import time
import threading
import hashlib
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class StepFunctionsAPIException(Exception):
    """Step Functions API 异常"""
    pass


class ExecutionType(Enum):
    """执行类型"""
    EXPRESS = "EXPRESS"
    STANDARD = "STANDARD"


class ExecutionStatus(Enum):
    """执行状态"""
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    ABORTED = "ABORTED"


class StateMachineStatus(Enum):
    """状态机状态"""
    ACTIVE = "ACTIVE"
    DELETING = "DELETING"
    PENDING = "PENDING"


class ActivityStatus(Enum):
    """活动状态"""
    STATUS = "ACTIVE"
    DORMANT = "DORMANT"


@dataclass
class StateMachine:
    """状态机数据模型"""
    name: str
    state_machine_arn: str
    status: str = "ACTIVE"
    type: str = "STANDARD"
    definition: Optional[Dict[str, Any]] = None
    role_arn: str = ""
    creation_date: Optional[datetime] = None
    description: str = ""
    revision_id: Optional[str] = None
    encryption_option: Optional[str] = None
    kms_key_id: Optional[str] = None
    logging_configuration: Optional[Dict[str, Any]] = None
    tracing_configuration: Optional[Dict[str, Any]] = None


@dataclass
class Execution:
    """执行数据模型"""
    execution_arn: str
    state_machine_arn: str
    name: str
    status: str
    start_date: datetime
    end_date: Optional[datetime] = None
    input: Optional[Dict[str, Any]] = None
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    cause: Optional[str] = None
    map_run_arn: Optional[str] = None
    redrive_count: Optional[int] = None


@dataclass
class Activity:
    """活动数据模型"""
    activity_arn: str
    name: str
    creation_date: datetime
    status: str = "ACTIVE"


@dataclass
class StateMachineDefinition:
    """状态机定义构建器"""
    Comment: str = ""
    StartAt: str = ""
    States: Dict[str, Any] = field(default_factory=dict)
    TimeoutSeconds: Optional[int] = None
    Version: str = "1.0"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {
            "Comment": self.Comment,
            "StartAt": self.StartAt,
            "States": self.States,
            "Version": self.Version
        }
        if self.TimeoutSeconds:
            result["TimeoutSeconds"] = self.TimeoutSeconds
        return result

    def to_json(self) -> str:
        """转换为 JSON 字符串"""
        return json.dumps(self.to_dict(), indent=2)


class StateBuilder:
    """状态构建器 - 用于构建各种类型的状态"""

    @staticmethod
    def pass_state(name: str, comment: str = "", result: Any = None,
                   next_state: Optional[str] = None, end: bool = False) -> Dict[str, Any]:
        """构建 Pass 状态"""
        state = {
            "Type": "Pass",
            "Comment": comment
        }
        if result:
            state["Result"] = result
        if next_state:
            state["Next"] = next_state
        if end:
            state["End"] = True
        return state

    @staticmethod
    def task_state(name: str, resource: str, comment: str = "",
                   timeout_seconds: int = 60,
                   heartbeat_seconds: int = 0,
                   parameters: Optional[Dict[str, Any]] = None,
                   result_path: Optional[str] = None,
                   next_state: Optional[str] = None,
                   end: bool = False) -> Dict[str, Any]:
        """构建 Task 状态"""
        state = {
            "Type": "Task",
            "Comment": comment,
            "Resource": resource,
            "TimeoutSeconds": timeout_seconds
        }
        if heartbeat_seconds > 0:
            state["HeartbeatSeconds"] = heartbeat_seconds
        if parameters:
            state["Parameters"] = parameters
        if result_path:
            state["ResultPath"] = result_path
        if next_state:
            state["Next"] = next_state
        if end:
            state["End"] = True
        return state

    @staticmethod
    def choice_state(name: str, comment: str = "",
                     choices: Optional[List[Dict[str, Any]]] = None,
                     default_state: Optional[str] = None) -> Dict[str, Any]:
        """构建 Choice 状态"""
        state = {
            "Type": "Choice",
            "Comment": comment
        }
        if choices:
            state["Choices"] = choices
        if default_state:
            state["Default"] = default_state
        return state

    @staticmethod
    def wait_state(name: str, seconds: Optional[int] = None,
                   timestamp: Optional[str] = None,
                   next_state: Optional[str] = None,
                   end: bool = False) -> Dict[str, Any]:
        """构建 Wait 状态"""
        state = {
            "Type": "Wait",
        }
        if seconds:
            state["Seconds"] = seconds
        if timestamp:
            state["Timestamp"] = timestamp
        if next_state:
            state["Next"] = next_state
        if end:
            state["End"] = True
        return state

    @staticmethod
    def succeed_state(name: str, comment: str = "",
                      output: Any = None) -> Dict[str, Any]:
        """构建 Succeed 状态"""
        state = {
            "Type": "Succeed",
            "Comment": comment
        }
        if output is not None:
            state["Output"] = output
        return state

    @staticmethod
    def fail_state(name: str, error: str, cause: Optional[str] = None) -> Dict[str, Any]:
        """构建 Fail 状态"""
        state = {
            "Type": "Fail",
            "Error": error
        }
        if cause:
            state["Cause"] = cause
        return state

    @staticmethod
    def parallel_state(name: str, branches: Optional[List[Dict[str, Any]]] = None,
                       result_path: Optional[str] = None,
                       next_state: Optional[str] = None,
                       end: bool = False,
                       comment: str = "") -> Dict[str, Any]:
        """构建 Parallel 状态"""
        state = {
            "Type": "Parallel",
            "Comment": comment
        }
        if branches:
            state["Branches"] = branches
        if result_path:
            state["ResultPath"] = result_path
        if next_state:
            state["Next"] = next_state
        if end:
            state["End"] = True
        return state

    @staticmethod
    def map_state(name: str, items_path: Optional[str] = None,
                  max_concurrency: int = 0,
                  result_path: Optional[str] = None,
                  iterator: Optional[Dict[str, Any]] = None,
                  next_state: Optional[str] = None,
                  end: bool = False,
                  comment: str = "") -> Dict[str, Any]:
        """构建 Map 状态"""
        state = {
            "Type": "Map",
            "Comment": comment
        }
        if items_path:
            state["ItemsPath"] = items_path
        if max_concurrency > 0:
            state["MaxConcurrency"] = max_concurrency
        if result_path:
            state["ResultPath"] = result_path
        if iterator:
            state["Iterator"] = iterator
        if next_state:
            state["Next"] = next_state
        if end:
            state["End"] = True
        return state


class ChoiceRuleBuilder:
    """选择规则构建器"""

    @staticmethod
    def string_equals(variable: str, value: str) -> Dict[str, Any]:
        """字符串等于"""
        return {
            "Variable": variable,
            "StringEquals": value
        }

    @staticmethod
    def numeric_equals(variable: str, value: float) -> Dict[str, Any]:
        """数值等于"""
        return {
            "Variable": variable,
            "NumericEquals": value
        }

    @staticmethod
    def numeric_greater_than(variable: str, value: float) -> Dict[str, Any]:
        """数值大于"""
        return {
            "Variable": variable,
            "NumericGreaterThan": value
        }

    @staticmethod
    def numeric_less_than(variable: str, value: float) -> Dict[str, Any]:
        """数值小于"""
        return {
            "Variable": variable,
            "NumericLessThan": value
        }

    @staticmethod
    def boolean_equals(variable: str, value: bool) -> Dict[str, Any]:
        """布尔等于"""
        return {
            "Variable": variable,
            "BooleanEquals": value
        }

    @staticmethod
    def timestamp_equals(variable: str, value: str) -> Dict[str, Any]:
        """时间戳等于"""
        return {
            "Variable": variable,
            "TimestampEquals": value
        }

    @staticmethod
    def is_present(variable: str) -> Dict[str, Any]:
        """变量存在"""
        return {
            "Variable": variable,
            "IsPresent": True
        }

    @staticmethod
    def and_(*conditions) -> Dict[str, Any]:
        """AND 组合"""
        return {
            "And": list(conditions)
        }

    @staticmethod
    def or_(*conditions) -> Dict[str, Any]:
        """OR 组合"""
        return {
            "Or": list(conditions)
        }

    @staticmethod
    def not_(condition: Dict[str, Any]) -> Dict[str, Any]:
        """NOT 组合"""
        return {
            "Not": condition
        }


class ErrorHandlerBuilder:
    """错误处理构建器"""

    @staticmethod
    def retry(error_equals: List[str],
              max_attempts: int = 3,
              interval_seconds: int = 1,
              backoff_rate: float = 2.0,
              max_interval_seconds: int = 100) -> Dict[str, Any]:
        """构建重试配置"""
        return {
            "ErrorEquals": error_equals,
            "IntervalSeconds": interval_seconds,
            "MaxAttempts": max_attempts,
            "BackoffRate": backoff_rate,
            "MaxIntervalSeconds": max_interval_seconds
        }

    @staticmethod
    def catch(reason: str,
              next_state: str,
              error_equals: List[str] = None) -> Dict[str, Any]:
        """构建捕获配置"""
        if error_equals is None:
            error_equals = ["States.ALL"]
        return {
            "ErrorEquals": error_equals,
            "Next": next_state
        }


class CloudWatchBuilder:
    """CloudWatch 日志和指标构建器"""

    @staticmethod
    def logging_configuration(level: str = "ALL",
                              include_execution_data: bool = True,
                              log_group_arn: str = None) -> Dict[str, Any]:
        """构建日志配置"""
        config = {
            "level": level,
            "includeExecutionData": include_execution_data
        }
        if log_group_arn:
            config["logGroupArn"] = log_group_arn
        return config

    @staticmethod
    def tracing_configuration(enabled: bool = True) -> Dict[str, Any]:
        """构建追踪配置"""
        return {
            "enabled": enabled
        }


class IAMRoleBuilder:
    """IAM 角色策略构建器"""

    @staticmethod
    def basic_execution_role() -> Dict[str, Any]:
        """基础执行角色策略"""
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogDelivery",
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    "Resource": "*"
                }
            ]
        }

    @staticmethod
    def activity_task_role() -> Dict[str, Any]:
        """活动任务角色策略"""
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "states:GetActivityTask",
                        "states:SendTaskHeartbeat",
                        "states:SendTaskSuccess",
                        "states:SendTaskFailure"
                    ],
                    "Resource": "*"
                }
            ]
        }

    @staticmethod
    def lambda_invoke_role(lambda_arn: str = None) -> Dict[str, Any]:
        """Lambda 调用角色策略"""
        statement = {
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction"
            ],
            "Resource": "*" if not lambda_arn else lambda_arn
        }
        if lambda_arn:
            statement["Resource"] = lambda_arn
        return {
            "Version": "2012-10-17",
            "Statement": [statement]
        }


class StepFunctionsManager:
    """AWS Step Functions 管理器"""

    def __init__(self, region: str = "us-east-1",
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 endpoint_url: Optional[str] = None):
        """
        初始化 Step Functions 管理器

        Args:
            region: AWS 区域
            aws_access_key_id: AWS 访问密钥 ID
            aws_secret_access_key: AWS 秘密访问密钥
            endpoint_url: Step Functions API 端点 URL
        """
        self.region = region
        self.endpoint_url = endpoint_url or f"https://states.{region}.amazonaws.com"
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.state_machines: Dict[str, StateMachine] = {}
        self.executions: Dict[str, Execution] = {}
        self.activities: Dict[str, Activity] = {}
        self._lock = threading.Lock()
        self._client = None

    def _get_client(self):
        """获取 AWS 客户端"""
        if self._client is None:
            try:
                import boto3
                client_kwargs = {
                    "region_name": self.region,
                    "aws_access_key_id": self.aws_access_key_id,
                    "aws_secret_access_key": self.aws_secret_access_key
                }
                if self.endpoint_url:
                    client_kwargs["endpoint_url"] = self.endpoint_url
                self._client = boto3.client("stepfunctions", **client_kwargs)
            except ImportError:
                raise StepFunctionsAPIException(
                    "boto3 is required. Install with: pip install boto3"
                )
        return self._client

    def _generate_arn(self, resource_type: str, name: str) -> str:
        """生成 ARN"""
        resource_hash = hashlib.md5(name.encode()).hexdigest()[:12]
        return f"arn:aws:states:{self.region}:123456789012:{resource_type}:{name}:{resource_hash}"

    # ========== 状态机管理 ==========

    def create_state_machine(
        self,
        name: str,
        definition: Dict[str, Any],
        role_arn: str,
        execution_type: ExecutionType = ExecutionType.STANDARD,
        description: str = "",
        logging_config: Optional[Dict[str, Any]] = None,
        tracing_config: Optional[Dict[str, Any]] = None,
        kms_key_id: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> StateMachine:
        """
        创建状态机

        Args:
            name: 状态机名称
            definition: 状态机定义
            role_arn: IAM 角色 ARN
            execution_type: 执行类型 (EXPRESS 或 STANDARD)
            description: 描述
            logging_config: 日志配置
            tracing_config: 追踪配置
            kms_key_id: KMS 密钥 ID
            tags: 标签

        Returns:
            StateMachine: 创建的状态机对象
        """
        with self._lock:
            state_machine_arn = self._generate_arn("stateMachine", name)

            state_machine = StateMachine(
                name=name,
                state_machine_arn=state_machine_arn,
                status="ACTIVE",
                type=execution_type.value,
                definition=definition,
                role_arn=role_arn,
                creation_date=datetime.now(),
                description=description,
                logging_configuration=logging_config,
                tracing_configuration=tracing_config
            )

            self.state_machines[name] = state_machine
            logger.info(f"Created state machine: {name}")
            return state_machine

    def get_state_machine(self, name: str) -> Optional[StateMachine]:
        """获取状态机"""
        return self.state_machines.get(name)

    def list_state_machines(self, status_filter: Optional[str] = None) -> List[StateMachine]:
        """列出状态机"""
        machines = list(self.state_machines.values())
        if status_filter:
            machines = [m for m in machines if m.status == status_filter]
        return machines

    def update_state_machine(
        self,
        name: str,
        definition: Optional[Dict[str, Any]] = None,
        role_arn: Optional[str] = None
    ) -> bool:
        """
        更新状态机

        Args:
            name: 状态机名称
            definition: 新的状态机定义
            role_arn: 新的 IAM 角色 ARN

        Returns:
            bool: 是否更新成功
        """
        with self._lock:
            if name not in self.state_machines:
                return False

            state_machine = self.state_machines[name]
            if definition:
                state_machine.definition = definition
            if role_arn:
                state_machine.role_arn = role_arn

            logger.info(f"Updated state machine: {name}")
            return True

    def delete_state_machine(self, name: str) -> bool:
        """删除状态机"""
        with self._lock:
            if name in self.state_machines:
                del self.state_machines[name]
                logger.info(f"Deleted state machine: {name}")
                return True
            return False

    # ========== 执行管理 ==========

    def start_execution(
        self,
        state_machine_name: str,
        name: Optional[str] = None,
        input_data: Optional[Dict[str, Any]] = None,
        trace_header: Optional[str] = None
    ) -> Execution:
        """
        启动执行

        Args:
            state_machine_name: 状态机名称
            name: 执行名称
            input_data: 输入数据
            trace_header: 追踪头

        Returns:
            Execution: 执行对象
        """
        with self._lock:
            state_machine = self.state_machines.get(state_machine_name)
            if not state_machine:
                raise StepFunctionsAPIException(
                    f"State machine not found: {state_machine_name}"
                )

            execution_name = name or f"execution-{int(time.time())}"
            execution_arn = self._generate_arn("execution", f"{state_machine_name}:{execution_name}")

            execution = Execution(
                execution_arn=execution_arn,
                state_machine_arn=state_machine.state_machine_arn,
                name=execution_name,
                status="RUNNING",
                start_date=datetime.now(),
                input=input_data
            )

            self.executions[execution_arn] = execution
            logger.info(f"Started execution: {execution_name} for state machine: {state_machine_name}")
            return execution

    def get_execution(self, execution_arn: str) -> Optional[Execution]:
        """获取执行"""
        return self.executions.get(execution_arn)

    def list_executions(
        self,
        state_machine_name: Optional[str] = None,
        status_filter: Optional[str] = None
    ) -> List[Execution]:
        """列出执行"""
        executions = list(self.executions.values())
        if state_machine_name:
            executions = [
                e for e in executions
                if self._find_state_machine_by_arn(e.state_machine_arn) == state_machine_name
            ]
        if status_filter:
            executions = [e for e in executions if e.status == status_filter]
        return executions

    def stop_execution(self, execution_arn: str, error: Optional[str] = None,
                       cause: Optional[str] = None) -> bool:
        """
        停止执行

        Args:
            execution_arn: 执行 ARN
            error: 错误名称
            cause: 错误原因

        Returns:
            bool: 是否停止成功
        """
        with self._lock:
            if execution_arn in self.executions:
                execution = self.executions[execution_arn]
                execution.status = "ABORTED"
                execution.end_date = datetime.now()
                if error:
                    execution.error = error
                if cause:
                    execution.cause = cause
                logger.info(f"Stopped execution: {execution_arn}")
                return True
            return False

    def describe_execution(self, execution_arn: str) -> Dict[str, Any]:
        """获取执行详情"""
        execution = self.executions.get(execution_arn)
        if not execution:
            raise StepFunctionsAPIException(f"Execution not found: {execution_arn}")

        return {
            "executionArn": execution.execution_arn,
            "stateMachineArn": execution.state_machine_arn,
            "name": execution.name,
            "status": execution.status,
            "startDate": execution.start_date.isoformat() if execution.start_date else None,
            "stopDate": execution.end_date.isoformat() if execution.end_date else None,
            "input": json.dumps(execution.input) if execution.input else "{}",
            "output": json.dumps(execution.output) if execution.output else None,
            "error": execution.error,
            "cause": execution.cause
        }

    def get_execution_history(
        self,
        execution_arn: str,
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """获取执行历史"""
        execution = self.executions.get(execution_arn)
        if not execution:
            raise StepFunctionsAPIException(f"Execution not found: {execution_arn}")

        events = [
            {
                "timestamp": execution.start_date.isoformat(),
                "type": "ExecutionStarted",
                "id": 1,
                "details": {
                    "input": json.dumps(execution.input) if execution.input else "{}",
                    "inputDetails": {"truncated": False}
                }
            }
        ]

        event_id = 2
        if execution.status == "SUCCEEDED":
            events.append({
                "timestamp": execution.end_date.isoformat() if execution.end_date else datetime.now().isoformat(),
                "type": "ExecutionSucceeded",
                "id": event_id,
                "details": {
                    "output": json.dumps(execution.output) if execution.output else "{}"
                }
            })
        elif execution.status == "FAILED":
            events.append({
                "timestamp": execution.end_date.isoformat() if execution.end_date else datetime.now().isoformat(),
                "type": "ExecutionFailed",
                "id": event_id,
                "details": {
                    "error": execution.error,
                    "cause": execution.cause
                }
            })

        events.append({
            "timestamp": execution.end_date.isoformat() if execution.end_date else datetime.now().isoformat(),
            "type": "ExecutionFinished",
            "id": event_id + 1,
            "details": {"output": json.dumps(execution.output) if execution.output else "{}"}
        })

        return events

    def _find_state_machine_by_arn(self, arn: str) -> Optional[str]:
        """通过 ARN 查找状态机名称"""
        for name, sm in self.state_machines.items():
            if sm.state_machine_arn == arn:
                return name
        return None

    # ========== 活动管理 ==========

    def create_activity(
        self,
        name: str,
        tags: Optional[Dict[str, str]] = None
    ) -> Activity:
        """
        创建活动

        Args:
            name: 活动名称
            tags: 标签

        Returns:
            Activity: 创建的活动对象
        """
        with self._lock:
            activity_arn = self._generate_arn("activity", name)

            activity = Activity(
                activity_arn=activity_arn,
                name=name,
                creation_date=datetime.now(),
                status="ACTIVE"
            )

            self.activities[name] = activity
            logger.info(f"Created activity: {name}")
            return activity

    def get_activity(self, name: str) -> Optional[Activity]:
        """获取活动"""
        return self.activities.get(name)

    def list_activities(self, status_filter: Optional[str] = None) -> List[Activity]:
        """列出活动"""
        activities = list(self.activities.values())
        if status_filter:
            activities = [a for a in activities if a.status == status_filter]
        return activities

    def delete_activity(self, name: str) -> bool:
        """删除活动"""
        with self._lock:
            if name in self.activities:
                del self.activities[name]
                logger.info(f"Deleted activity: {name}")
                return True
            return False

    def get_activity_task(
        self,
        activity_arn: str,
        timeout_seconds: int = 60
    ) -> Optional[Dict[str, Any]]:
        """
        获取活动任务

        Args:
            activity_arn: 活动 ARN
            timeout_seconds: 超时秒数

        Returns:
            Optional[Dict[str, Any]]: 任务信息
        """
        with self._lock:
            for name, activity in self.activities.items():
                if activity.activity_arn == activity_arn:
                    return {
                        "taskArn": self._generate_arn("activityTask", f"{name}:{int(time.time())}"),
                        "activityArn": activity_arn,
                        "input": "{}",
                        "timeoutHeartbeat": timeout_seconds
                    }
        return None

    def send_task_success(
        self,
        task_token: str,
        output: Dict[str, Any]
    ) -> bool:
        """
        发送任务成功

        Args:
            task_token: 任务令牌
            output: 输出数据

        Returns:
            bool: 是否发送成功
        """
        logger.info(f"Task success: {task_token}")
        return True

    def send_task_failure(
        self,
        task_token: str,
        error: str,
        cause: Optional[str] = None
    ) -> bool:
        """
        发送任务失败

        Args:
            task_token: 任务令牌
            error: 错误名称
            cause: 错误原因

        Returns:
            bool: 是否发送成功
        """
        logger.info(f"Task failure: {task_token}, error: {error}")
        return True

    def send_task_heartbeat(self, task_token: str) -> bool:
        """
        发送任务心跳

        Args:
            task_token: 任务令牌

        Returns:
            bool: 是否发送成功
        """
        logger.debug(f"Task heartbeat: {task_token}")
        return True

    # ========== 状态机构建器方法 ==========

    def build_definition(self) -> StateMachineDefinition:
        """创建状态机定义构建器"""
        return StateMachineDefinition()

    def create_simple_workflow(
        self,
        name: str,
        start_state: str,
        end_state: str,
        role_arn: str,
        states: Dict[str, Dict[str, Any]]
    ) -> StateMachine:
        """
        创建简单工作流

        Args:
            name: 状态机名称
            start_state: 开始状态名称
            end_state: 结束状态名称
            role_arn: IAM 角色 ARN
            states: 状态字典

        Returns:
            StateMachine: 创建的状态机
        """
        definition = {
            "Comment": f"Simple workflow: {name}",
            "StartAt": start_state,
            "States": states
        }
        return self.create_state_machine(name, definition, role_arn)

    def create_parallel_workflow(
        self,
        name: str,
        branches: List[Dict[str, Any]],
        role_arn: str,
        result_path: Optional[str] = None,
        error_handling: Optional[List[Dict[str, Any]]] = None
    ) -> StateMachine:
        """
        创建并行工作流

        Args:
            name: 状态机名称
            branches: 分支定义列表
            role_arn: IAM 角色 ARN
            result_path: 结果路径
            error_handling: 错误处理配置

        Returns:
            StateMachine: 创建的状态机
        """
        parallel_state = {
            "Type": "Parallel",
            "Branches": branches,
            "ResultPath": result_path or "$.parallel"
        }

        if error_handling:
            parallel_state["Retry"] = error_handling

        definition = {
            "Comment": f"Parallel workflow: {name}",
            "StartAt": "Parallel",
            "States": {
                "Parallel": parallel_state
            }
        }

        return self.create_state_machine(name, definition, role_arn)

    def create_map_workflow(
        self,
        name: str,
        iterator: Dict[str, Any],
        items_path: str = "$.items",
        max_concurrency: int = 0,
        role_arn: str = "",
        result_path: Optional[str] = None
    ) -> StateMachine:
        """
        创建 Map 工作流

        Args:
            name: 状态机名称
            iterator: 迭代器定义
            items_path: 项目路径
            max_concurrency: 最大并发数
            role_arn: IAM 角色 ARN
            result_path: 结果路径

        Returns:
            StateMachine: 创建的状态机
        """
        map_state = {
            "Type": "Map",
            "Iterator": iterator,
            "ItemsPath": items_path
        }

        if max_concurrency > 0:
            map_state["MaxConcurrency"] = max_concurrency

        if result_path:
            map_state["ResultPath"] = result_path

        definition = {
            "Comment": f"Map workflow: {name}",
            "StartAt": "MapState",
            "States": {
                "MapState": map_state
            }
        }

        return self.create_state_machine(name, definition, role_arn)

    def create_choice_workflow(
        self,
        name: str,
        choice_state: Dict[str, Any],
        default_state: Optional[str] = None,
        role_arn: str = ""
    ) -> StateMachine:
        """
        创建选择工作流

        Args:
            name: 状态机名称
            choice_state: 选择状态定义
            default_state: 默认状态
            role_arn: IAM 角色 ARN

        Returns:
            StateMachine: 创建的状态机
        """
        states = {"ChoiceState": choice_state}

        if default_state:
            choice_state["Default"] = default_state

        definition = {
            "Comment": f"Choice workflow: {name}",
            "StartAt": "ChoiceState",
            "States": states
        }

        return self.create_state_machine(name, definition, role_arn)

    # ========== CloudWatch 集成 ==========

    def enable_logging(
        self,
        state_machine_name: str,
        log_group_arn: str,
        include_execution_data: bool = True,
        log_level: str = "ALL"
    ) -> bool:
        """
        启用 CloudWatch 日志

        Args:
            state_machine_name: 状态机名称
            log_group_arn: 日志组 ARN
            include_execution_data: 是否包含执行数据
            log_level: 日志级别

        Returns:
            bool: 是否启用成功
        """
        state_machine = self.state_machines.get(state_machine_name)
        if not state_machine:
            return False

        logging_config = CloudWatchBuilder.logging_configuration(
            level=log_level,
            include_execution_data=include_execution_data,
            log_group_arn=log_group_arn
        )

        state_machine.logging_configuration = logging_config
        logger.info(f"Enabled CloudWatch logging for: {state_machine_name}")
        return True

    def enable_tracing(self, state_machine_name: str) -> bool:
        """
        启用 X-Ray 追踪

        Args:
            state_machine_name: 状态机名称

        Returns:
            bool: 是否启用成功
        """
        state_machine = self.state_machines.get(state_machine_name)
        if not state_machine:
            return False

        tracing_config = CloudWatchBuilder.tracing_configuration(enabled=True)
        state_machine.tracing_configuration = tracing_config
        logger.info(f"Enabled X-Ray tracing for: {state_machine_name}")
        return True

    def get_execution_metrics(self, execution_arn: str) -> Dict[str, Any]:
        """
        获取 CloudWatch 执行指标

        Args:
            execution_arn: 执行 ARN

        Returns:
            Dict[str, Any]: 指标数据
        """
        execution = self.executions.get(execution_arn)
        if not execution:
            raise StepFunctionsAPIException(f"Execution not found: {execution_arn}")

        duration = None
        if execution.start_date and execution.end_date:
            duration = (execution.end_date - execution.start_date).total_seconds()

        return {
            "executionArn": execution_arn,
            "status": execution.status,
            "startDate": execution.start_date.isoformat() if execution.start_date else None,
            "stopDate": execution.end_date.isoformat() if execution.end_date else None,
            "durationSeconds": duration,
            "billedDurationSeconds": duration if execution.status == "SUCCEEDED" else 0
        }

    # ========== IAM 角色管理 ==========

    def create_execution_role(self, name: str) -> Dict[str, Any]:
        """
        创建执行角色

        Args:
            name: 角色名称

        Returns:
            Dict[str, Any]: 角色策略
        """
        return IAMRoleBuilder.basic_execution_role()

    def create_activity_role(self, name: str) -> Dict[str, Any]:
        """
        创建活动执行角色

        Args:
            name: 角色名称

        Returns:
            Dict[str, Any]: 角色策略
        """
        return IAMRoleBuilder.activity_task_role()

    def create_lambda_role(self, name: str, lambda_arn: str = None) -> Dict[str, Any]:
        """
        创建 Lambda 执行角色

        Args:
            name: 角色名称
            lambda_arn: Lambda 函数 ARN

        Returns:
            Dict[str, Any]: 角色策略
        """
        return IAMRoleBuilder.lambda_invoke_role(lambda_arn)

    def validate_role(self, role_arn: str) -> bool:
        """
        验证 IAM 角色

        Args:
            role_arn: IAM 角色 ARN

        Returns:
            bool: 角色是否有效
        """
        if not role_arn or len(role_arn) < 20:
            return False
        return role_arn.startswith("arn:aws:iam::")

    # ========== 错误处理配置 ==========

    def add_retry_to_state(
        self,
        state: Dict[str, Any],
        error_equals: List[str],
        max_attempts: int = 3,
        interval_seconds: int = 1,
        backoff_rate: float = 2.0
    ) -> Dict[str, Any]:
        """
        为状态添加重试配置

        Args:
            state: 状态定义
            error_equals: 错误类型列表
            max_attempts: 最大重试次数
            interval_seconds: 重试间隔
            backoff_rate: 退避率

        Returns:
            Dict[str, Any]: 更新后的状态
        """
        retry_config = ErrorHandlerBuilder.retry(
            error_equals=error_equals,
            max_attempts=max_attempts,
            interval_seconds=interval_seconds,
            backoff_rate=backoff_rate
        )

        if "Retry" not in state:
            state["Retry"] = []
        state["Retry"].append(retry_config)

        return state

    def add_catch_to_state(
        self,
        state: Dict[str, Any],
        next_state: str,
        error_equals: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        为状态添加捕获配置

        Args:
            state: 状态定义
            next_state: 下一状态
            error_equals: 错误类型列表

        Returns:
            Dict[str, Any]: 更新后的状态
        """
        catch_config = ErrorHandlerBuilder.catch(
            reason="Error",
            next_state=next_state,
            error_equals=error_equals
        )

        if "Catch" not in state:
            state["Catch"] = []
        state["Catch"].append(catch_config)

        return state

    # ========== 模拟执行 ==========

    def simulate_execution(
        self,
        state_machine_name: str,
        input_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        模拟执行状态机

        Args:
            state_machine_name: 状态机名称
            input_data: 输入数据

        Returns:
            Dict[str, Any]: 模拟结果
        """
        state_machine = self.state_machines.get(state_machine_name)
        if not state_machine:
            raise StepFunctionsAPIException(f"State machine not found: {state_machine_name}")

        current_state = state_machine.definition.get("StartAt")
        states = state_machine.definition.get("States", {})
        execution_data = input_data or {}
        history = []

        while current_state:
            state_def = states.get(current_state, {})
            state_type = state_def.get("Type", "Pass")

            history.append({
                "state": current_state,
                "type": state_type,
                "input": execution_data
            })

            if state_type == "Pass":
                result = state_def.get("Result", execution_data)
                result_path = state_def.get("ResultPath")
                if result_path:
                    execution_data[result_path.replace("$.", "")] = result
                else:
                    execution_data = result

            elif state_type == "Task":
                result = {"status": "completed", "state": current_state}
                result_path = state_def.get("ResultPath")
                if result_path:
                    execution_data[result_path.replace("$.", "")] = result
                else:
                    execution_data = result

            elif state_type == "Choice":
                choices = state_def.get("Choices", [])
                matched = False
                for choice in choices:
                    if self._evaluate_choice(choice, execution_data):
                        current_state = choice.get("Next")
                        matched = True
                        break
                if not matched:
                    default = state_def.get("Default")
                    if default:
                        current_state = default
                    else:
                        break
                continue

            elif state_type == "Wait":
                seconds = state_def.get("Seconds", 0)
                execution_data["wait_duration"] = seconds

            elif state_type == "Succeed":
                history.append({
                    "state": current_state,
                    "type": "Succeed",
                    "output": state_def.get("Output", execution_data)
                })
                return {
                    "status": "SUCCEEDED",
                    "output": state_def.get("Output", execution_data),
                    "history": history
                }

            elif state_type == "Fail":
                return {
                    "status": "FAILED",
                    "error": state_def.get("Error"),
                    "cause": state_def.get("Cause"),
                    "history": history
                }

            elif state_type == "Parallel":
                branches = state_def.get("Branches", [])
                parallel_results = []
                for branch in branches:
                    branch_result = self._simulate_branch(branch, execution_data)
                    parallel_results.append(branch_result)
                result_path = state_def.get("ResultPath", "$.parallel")
                execution_data[result_path.replace("$.", "")] = parallel_results

            elif state_type == "Map":
                items = execution_data.get("items", [])
                iterator = state_def.get("Iterator", {})
                map_results = []
                for item in items:
                    item_data = {"item": item}
                    result = self._simulate_branch(iterator, item_data)
                    map_results.append(result)
                result_path = state_def.get("ResultPath", "$.map")
                execution_data[result_path.replace("$.", "")] = map_results

            if state_def.get("End"):
                break

            current_state = state_def.get("Next")

        return {
            "status": "SUCCEEDED",
            "output": execution_data,
            "history": history
        }

    def _evaluate_choice(self, choice: Dict[str, Any], data: Dict[str, Any]) -> bool:
        """评估选择条件"""
        for key, value in choice.items():
            if key == "Variable":
                continue
            if key == "StringEquals":
                var_path = choice.get("Variable", "").replace("$.", "")
                return data.get(var_path) == value
            if key == "NumericEquals":
                var_path = choice.get("Variable", "").replace("$.", "")
                return float(data.get(var_path, 0)) == float(value)
            if key == "BooleanEquals":
                var_path = choice.get("Variable", "").replace("$.", "")
                return bool(data.get(var_path)) == value
        return False

    def _simulate_branch(
        self,
        branch: Dict[str, Any],
        input_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """模拟分支执行"""
        start = branch.get("StartAt")
        states = branch.get("States", {})
        current = start
        data = input_data

        while current:
            state = states.get(current, {})
            state_type = state.get("Type", "Pass")

            if state_type == "Pass":
                data = state.get("Result", data)

            elif state_type == "Task":
                data = {"status": "completed"}

            elif state_type == "Succeed":
                return data

            elif state_type == "Fail":
                return {"error": state.get("Error")}

            if state.get("End"):
                break
            current = state.get("Next")

        return data

    # ========== 辅助方法 ==========

    def export_definition(self, name: str) -> str:
        """
        导出状态机定义为 JSON

        Args:
            name: 状态机名称

        Returns:
            str: JSON 格式的状态机定义
        """
        state_machine = self.state_machines.get(name)
        if not state_machine:
            raise StepFunctionsAPIException(f"State machine not found: {name}")

        return json.dumps(state_machine.definition, indent=2)

    def import_definition(self, name: str, definition_json: str) -> bool:
        """
        导入状态机定义

        Args:
            name: 状态机名称
            definition_json: JSON 格式的状态机定义

        Returns:
            bool: 是否导入成功
        """
        try:
            definition = json.loads(definition_json)
            state_machine = self.state_machines.get(name)
            if state_machine:
                state_machine.definition = definition
                logger.info(f"Imported definition for state machine: {name}")
                return True
            return False
        except json.JSONDecodeError:
            raise StepFunctionsAPIException("Invalid JSON format")

    def validate_definition(self, definition: Dict[str, Any]) -> List[str]:
        """
        验证状态机定义

        Args:
            definition: 状态机定义

        Returns:
            List[str]: 验证错误列表
        """
        errors = []

        if "StartAt" not in definition:
            errors.append("Missing required field: StartAt")

        if "States" not in definition:
            errors.append("Missing required field: States")
        else:
            states = definition["States"]
            if not states:
                errors.append("States cannot be empty")

            start_at = definition.get("StartAt")
            if start_at and start_at not in states:
                errors.append(f"StartAt state '{start_at}' not found in States")

            for state_name, state_def in states.items():
                state_type = state_def.get("Type")
                if state_type not in ["Pass", "Task", "Choice", "Wait", "Succeed", "Fail", "Parallel", "Map"]:
                    errors.append(f"Invalid state type '{state_type}' in state '{state_name}'")

                if state_type in ["Succeed", "Fail"]:
                    if "Next" in state_def:
                        errors.append(f"Terminal state '{state_name}' should not have 'Next' field")

        return errors

    def get_state_machine_types(self) -> Dict[str, int]:
        """获取状态机类型统计"""
        stats = {"EXPRESS": 0, "STANDARD": 0}
        for sm in self.state_machines.values():
            if sm.type in stats:
                stats[sm.type] += 1
        return stats

    def get_execution_stats(self) -> Dict[str, Any]:
        """获取执行统计"""
        stats = {
            "total": len(self.executions),
            "running": 0,
            "succeeded": 0,
            "failed": 0,
            "aborted": 0
        }

        for execution in self.executions.values():
            if execution.status == "RUNNING":
                stats["running"] += 1
            elif execution.status == "SUCCEEDED":
                stats["succeeded"] += 1
            elif execution.status == "FAILED":
                stats["failed"] += 1
            elif execution.status == "ABORTED":
                stats["aborted"] += 1

        return stats

    def cleanup_old_executions(self, days: int = 30) -> int:
        """
        清理旧的执行记录

        Args:
            days: 保留天数

        Returns:
            int: 清理的记录数
        """
        with self._lock:
            cutoff_date = datetime.now() - timedelta(days=days)
            to_remove = []

            for arn, execution in self.executions.items():
                if execution.end_date and execution.end_date < cutoff_date:
                    to_remove.append(arn)

            for arn in to_remove:
                del self.executions[arn]

            logger.info(f"Cleaned up {len(to_remove)} old executions")
            return len(to_remove)

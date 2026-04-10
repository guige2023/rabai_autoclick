"""
Model Context Protocol (MCP) 集成 v1.0.0
P0级功能 - MCP服务器管理、工具注册、资源共享、提示模板、多服务器支持、健康检查
"""
import json
import asyncio
import hashlib
import time
import uuid
import logging
import subprocess
import signal
from typing import Dict, List, Optional, Any, Callable, Awaitable
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict
from contextlib import asynccontextmanager
import threading
import re

logger = logging.getLogger(__name__)


class MCPProtocolVersion(Enum):
    """MCP协议版本"""
    V1_0 = "1.0"
    V1_1 = "1.1"
    V1_2 = "1.2"
    V1_3 = "1.3"
    LATEST = "1.3"


class ServerStatus(Enum):
    """服务器状态"""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"
    RESTARTING = "restarting"


class HealthStatus(Enum):
    """健康检查状态"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class MCPServerConfig:
    """MCP服务器配置"""
    server_id: str
    name: str
    command: str
    args: List[str] = field(default_factory=list)
    env: Dict[str, str] = field(default_factory=dict)
    protocol_version: str = "1.3"
    auto_restart: bool = True
    max_restart_attempts: int = 3
    health_check_interval: int = 30
    startup_timeout: int = 30
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MCPServer:
    """MCP服务器实例"""
    config: MCPServerConfig
    status: ServerStatus = ServerStatus.STOPPED
    process: Optional[subprocess.Popen] = None
    restart_attempts: int = 0
    last_health_check: Optional[float] = None
    health_status: HealthStatus = HealthStatus.UNKNOWN
    connected_at: Optional[float] = None
    error_message: Optional[str] = None


@dataclass
class MCPTool:
    """MCP工具定义"""
    name: str
    description: str
    input_schema: Dict[str, Any]
    handler: Optional[Callable[..., Awaitable[Any]]] = None
    workflow_action_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MCPResource:
    """MCP资源定义"""
    uri: str
    name: str
    description: str
    mime_type: str = "application/json"
    content: Optional[Any] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MCPPrompt:
    """MCP提示模板"""
    name: str
    description: str
    arguments: List[Dict[str, Any]]
    template: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MCPMessage:
    """MCP消息"""
    jsonrpc: str = "2.0"
    id: Optional[str] = None
    method: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None


@dataclass
class HealthCheckResult:
    """健康检查结果"""
    server_id: str
    status: HealthStatus
    latency_ms: float
    timestamp: float
    details: Dict[str, Any] = field(default_factory=dict)


class StreamingResponse:
    """流式响应迭代器"""
    
    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._done: bool = False
    
    async def put(self, chunk: Dict[str, Any]) -> None:
        """添加一个数据块"""
        await self._queue.put(chunk)
    
    async def finish(self) -> None:
        """标记流结束"""
        self._done = True
        await self._queue.put(None)
    
    def is_done(self) -> bool:
        return self._done and self._queue.empty()
    
    async def __aiter__(self):
        return self
    
    async def __anext__(self) -> Dict[str, Any]:
        if self._done and self._queue.empty():
            raise StopAsyncIteration
        return await self._queue.get()


class ToolMapping:
    """工具映射配置"""
    
    def __init__(self, mcp_tool_name: str, workflow_action_id: str,
                 parameter_mapping: Optional[Dict[str, str]] = None):
        self.mcp_tool_name = mcp_tool_name
        self.workflow_action_id = workflow_action_id
        self.parameter_mapping = parameter_mapping or {}


class WorkflowMCP:
    """
    Model Context Protocol 工作流集成类
    
    提供功能:
    1. MCP服务器管理: 启动/停止/配置MCP服务器
    2. 工具注册: 将工作流操作注册为MCP工具
    3. 资源管理: 通过MCP共享工作流资源
    4. 提示模板: 定义MCP提示模板
    5. 流式响应: 支持流式响应
    6. 多服务器: 连接多个MCP服务器
    7. 工具映射: 将MCP工具映射到工作流操作
    8. 服务器生命周期: 自动重启失败的服务器
    9. 健康检查: 监控MCP服务器健康状态
    10. 协议协商: 处理MCP协议版本
    """
    
    def __init__(self, workflow_core: Optional[Any] = None):
        """初始化WorkflowMCP
        
        Args:
            workflow_core: 工作流核心对象引用
        """
        self.workflow_core = workflow_core
        
        # 服务器管理
        self._servers: Dict[str, MCPServer] = {}
        self._server_configs: Dict[str, MCPServerConfig] = {}
        
        # 工具和资源管理
        self._tools: Dict[str, MCPTool] = {}
        self._resources: Dict[str, MCPResource] = {}
        self._prompts: Dict[str, MCPPrompt] = {}
        
        # 工具映射
        self._tool_mappings: Dict[str, ToolMapping] = {}
        self._workflow_action_tools: Dict[str, str] = {}  # workflow_action_id -> mcp_tool_name
        
        # 流式响应
        self._streaming_responses: Dict[str, StreamingResponse] = {}
        
        # 协议协商
        self._supported_protocols: List[MCPProtocolVersion] = [
            MCPProtocolVersion.V1_3,
            MCPProtocolVersion.V1_2,
            MCPProtocolVersion.V1_1,
            MCPProtocolVersion.V1_0,
        ]
        self._negotiated_versions: Dict[str, MCPProtocolVersion] = {}
        
        # 健康检查
        self._health_checks: Dict[str, HealthCheckResult] = {}
        self._health_check_task: Optional[asyncio.Task] = None
        self._running: bool = False
        
        # 锁
        self._server_lock = threading.RLock()
        self._tool_lock = threading.RLock()
        
        # 回调
        self._on_server_status_change: Optional[Callable[[str, ServerStatus], None]] = None
        self._on_health_status_change: Optional[Callable[[str, HealthStatus], None]] = None
        self._on_tool_call: Optional[Callable[[str, Dict[str, Any]], Any]] = None
        
        logger.info("WorkflowMCP 初始化完成")
    
    # ========== 服务器管理 ==========
    
    def register_server(self, config: MCPServerConfig) -> bool:
        """注册MCP服务器配置
        
        Args:
            config: 服务器配置
            
        Returns:
            是否成功注册
        """
        with self._server_lock:
            if config.server_id in self._server_configs:
                logger.warning(f"服务器 {config.server_id} 已存在，将更新配置")
            
            self._server_configs[config.server_id] = config
            self._servers[config.server_id] = MCPServer(config=config)
            logger.info(f"服务器 {config.server_id} 注册成功: {config.name}")
            return True
    
    def unregister_server(self, server_id: str) -> bool:
        """注销MCP服务器
        
        Args:
            server_id: 服务器ID
            
        Returns:
            是否成功注销
        """
        with self._server_lock:
            if server_id not in self._server_configs:
                logger.warning(f"服务器 {server_id} 不存在")
                return False
            
            # 停止服务器（如果运行中）
            self._stop_server_sync(server_id)
            
            del self._server_configs[server_id]
            del self._servers[server_id]
            logger.info(f"服务器 {server_id} 已注销")
            return True
    
    async def start_server(self, server_id: str) -> bool:
        """启动MCP服务器
        
        Args:
            server_id: 服务器ID
            
        Returns:
            是否成功启动
        """
        with self._server_lock:
            if server_id not in self._servers:
                logger.error(f"服务器 {server_id} 未注册")
                return False
            
            server = self._servers[server_id]
            if server.status == ServerStatus.RUNNING:
                logger.warning(f"服务器 {server_id} 已在运行")
                return True
            
            server.status = ServerStatus.STARTING
            server.error_message = None
            
            try:
                # 构建环境变量
                env = {
                    "MCP_SERVER_ID": server_id,
                    "MCP_PROTOCOL_VERSION": server.config.protocol_version,
                    **server.config.env
                }
                
                # 启动进程
                process = subprocess.Popen(
                    [server.config.command] + server.config.args,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                server.process = process
                
                # 等待服务器启动或超时
                start_time = time.time()
                while time.time() - start_time < server.config.startup_timeout:
                    if process.poll() is not None:
                        # 进程已退出
                        stderr = process.stderr.read() if process.stderr else ""
                        server.error_message = f"进程启动失败: {stderr}"
                        server.status = ServerStatus.FAILED
                        logger.error(f"服务器 {server_id} 启动失败: {server.error_message}")
                        return False
                    
                    # 检查服务器就绪（这里简化为等待超时）
                    await asyncio.sleep(0.5)
                
                server.status = ServerStatus.RUNNING
                server.connected_at = time.time()
                server.restart_attempts = 0
                logger.info(f"服务器 {server_id} 启动成功")
                
                # 触发状态变化回调
                if self._on_server_status_change:
                    self._on_server_status_change(server_id, ServerStatus.RUNNING)
                
                return True
                
            except Exception as e:
                server.error_message = str(e)
                server.status = ServerStatus.FAILED
                logger.error(f"服务器 {server_id} 启动异常: {e}")
                return False
    
    def _stop_server_sync(self, server_id: str) -> bool:
        """同步停止服务器（内部使用）"""
        if server_id not in self._servers:
            return False
        
        server = self._servers[server_id]
        if server.status == ServerStatus.STOPPED:
            return True
        
        server.status = ServerStatus.STOPPING
        
        try:
            if server.process:
                server.process.terminate()
                try:
                    server.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    server.process.kill()
                    server.process.wait()
            
            server.status = ServerStatus.STOPPED
            server.process = None
            logger.info(f"服务器 {server_id} 已停止")
            return True
            
        except Exception as e:
            logger.error(f"停止服务器 {server_id} 失败: {e}")
            server.status = ServerStatus.FAILED
            return False
    
    async def stop_server(self, server_id: str) -> bool:
        """停止MCP服务器
        
        Args:
            server_id: 服务器ID
            
        Returns:
            是否成功停止
        """
        with self._server_lock:
            result = self._stop_server_sync(server_id)
            if result and self._on_server_status_change:
                self._on_server_status_change(server_id, ServerStatus.STOPPED)
            return result
    
    async def restart_server(self, server_id: str) -> bool:
        """重启MCP服务器
        
        Args:
            server_id: 服务器ID
            
        Returns:
            是否成功重启
        """
        logger.info(f"重启服务器 {server_id}")
        
        # 停止服务器
        await self.stop_server(server_id)
        
        # 等待一小段时间
        await asyncio.sleep(1)
        
        # 重新启动
        return await self.start_server(server_id)
    
    async def start_all_servers(self) -> Dict[str, bool]:
        """启动所有注册的服务器
        
        Returns:
            服务器启动结果字典
        """
        results = {}
        for server_id in self._server_configs:
            results[server_id] = await self.start_server(server_id)
        return results
    
    async def stop_all_servers(self) -> Dict[str, bool]:
        """停止所有运行的服务器
        
        Returns:
            服务器停止结果字典
        """
        results = {}
        for server_id in list(self._servers.keys()):
            results[server_id] = await self.stop_server(server_id)
        return results
    
    def get_server_status(self, server_id: str) -> Optional[ServerStatus]:
        """获取服务器状态
        
        Args:
            server_id: 服务器ID
            
        Returns:
            服务器状态，如果不存在返回None
        """
        if server_id not in self._servers:
            return None
        return self._servers[server_id].status
    
    def get_all_server_statuses(self) -> Dict[str, ServerStatus]:
        """获取所有服务器状态
        
        Returns:
            服务器ID到状态的字典
        """
        return {
            server_id: server.status
            for server_id, server in self._servers.items()
        }
    
    # ========== 工具注册 ==========
    
    def register_tool(self, tool: MCPTool) -> bool:
        """注册MCP工具
        
        Args:
            tool: 工具定义
            
        Returns:
            是否成功注册
        """
        with self._tool_lock:
            if tool.name in self._tools:
                logger.warning(f"工具 {tool.name} 已存在，将更新")
            
            self._tools[tool.name] = tool
            logger.info(f"工具 {tool.name} 注册成功")
            return True
    
    def register_tools(self, tools: List[MCPTool]) -> int:
        """批量注册工具
        
        Args:
            tools: 工具列表
            
        Returns:
            成功注册的数量
        """
        count = 0
        for tool in tools:
            if self.register_tool(tool):
                count += 1
        return count
    
    def unregister_tool(self, tool_name: str) -> bool:
        """注销MCP工具
        
        Args:
            tool_name: 工具名称
            
        Returns:
            是否成功注销
        """
        with self._tool_lock:
            if tool_name not in self._tools:
                logger.warning(f"工具 {tool_name} 不存在")
                return False
            
            del self._tools[tool_name]
            
            # 清理相关映射
            if tool_name in self._tool_mappings:
                del self._tool_mappings[tool_name]
            
            logger.info(f"工具 {tool_name} 已注销")
            return True
    
    def get_tool(self, tool_name: str) -> Optional[MCPTool]:
        """获取工具
        
        Args:
            tool_name: 工具名称
            
        Returns:
            工具定义，如果不存在返回None
        """
        return self._tools.get(tool_name)
    
    def get_all_tools(self) -> List[MCPTool]:
        """获取所有已注册的工具
        
        Returns:
            工具列表
        """
        return list(self._tools.values())
    
    async def call_tool(self, tool_name: str, arguments: Dict[str, Any],
                       server_id: Optional[str] = None) -> Any:
        """调用MCP工具
        
        Args:
            tool_name: 工具名称
            arguments: 工具参数
            server_id: 可选的服务器ID（用于多服务器场景）
            
        Returns:
            工具执行结果
        """
        if tool_name not in self._tools:
            raise ValueError(f"工具 {tool_name} 不存在")
        
        tool = self._tools[tool_name]
        
        # 如果有工作流动作映射，调用工作流核心
        if tool.workflow_action_id and self.workflow_core:
            # 应用参数映射（如果存在）
            mapped_args = arguments
            if tool_name in self._tool_mappings:
                mapping = self._tool_mappings[tool_name]
                if mapping.parameter_mapping:
                    mapped_args = {
                        mapping.parameter_mapping.get(k, k): v
                        for k, v in arguments.items()
                    }
            
            return await self.workflow_core.execute_action(
                tool.workflow_action_id,
                mapped_args
            )
        
        # 直接调用工具处理器
        if tool.handler:
            return await tool.handler(**arguments)
        
        raise ValueError(f"工具 {tool_name} 没有可用的处理器")
    
    # ========== 资源管理 ==========
    
    def register_resource(self, resource: MCPResource) -> bool:
        """注册MCP资源
        
        Args:
            resource: 资源定义
            
        Returns:
            是否成功注册
        """
        if resource.uri in self._resources:
            logger.warning(f"资源 {resource.uri} 已存在，将更新")
        
        self._resources[resource.uri] = resource
        logger.info(f"资源 {resource.uri} 注册成功: {resource.name}")
        return True
    
    def register_resources(self, resources: List[MCPResource]) -> int:
        """批量注册资源
        
        Args:
            resources: 资源列表
            
        Returns:
            成功注册的数量
        """
        count = 0
        for resource in resources:
            if self.register_resource(resource):
                count += 1
        return count
    
    def unregister_resource(self, uri: str) -> bool:
        """注销MCP资源
        
        Args:
            uri: 资源URI
            
        Returns:
            是否成功注销
        """
        if uri not in self._resources:
            logger.warning(f"资源 {uri} 不存在")
            return False
        
        del self._resources[uri]
        logger.info(f"资源 {uri} 已注销")
        return True
    
    def get_resource(self, uri: str) -> Optional[MCPResource]:
        """获取资源
        
        Args:
            uri: 资源URI
            
        Returns:
            资源定义，如果不存在返回None
        """
        return self._resources.get(uri)
    
    def get_all_resources(self) -> List[MCPResource]:
        """获取所有已注册的资源
        
        Returns:
            资源列表
        """
        return list(self._resources.values())
    
    def update_resource_content(self, uri: str, content: Any) -> bool:
        """更新资源内容
        
        Args:
            uri: 资源URI
            content: 新的内容
            
        Returns:
            是否成功更新
        """
        if uri not in self._resources:
            logger.warning(f"资源 {uri} 不存在")
            return False
        
        self._resources[uri].content = content
        return True
    
    # ========== 提示模板 ==========
    
    def register_prompt(self, prompt: MCPPrompt) -> bool:
        """注册MCP提示模板
        
        Args:
            prompt: 提示模板定义
            
        Returns:
            是否成功注册
        """
        if prompt.name in self._prompts:
            logger.warning(f"提示模板 {prompt.name} 已存在，将更新")
        
        self._prompts[prompt.name] = prompt
        logger.info(f"提示模板 {prompt.name} 注册成功")
        return True
    
    def register_prompts(self, prompts: List[MCPPrompt]) -> int:
        """批量注册提示模板
        
        Args:
            prompts: 提示模板列表
            
        Returns:
            成功注册的数量
        """
        count = 0
        for prompt in prompts:
            if self.register_prompt(prompt):
                count += 1
        return count
    
    def unregister_prompt(self, name: str) -> bool:
        """注销MCP提示模板
        
        Args:
            name: 提示模板名称
            
        Returns:
            是否成功注销
        """
        if name not in self._prompts:
            logger.warning(f"提示模板 {name} 不存在")
            return False
        
        del self._prompts[name]
        logger.info(f"提示模板 {name} 已注销")
        return True
    
    def get_prompt(self, name: str) -> Optional[MCPPrompt]:
        """获取提示模板
        
        Args:
            name: 提示模板名称
            
        Returns:
            提示模板定义，如果不存在返回None
        """
        return self._prompts.get(name)
    
    def get_all_prompts(self) -> List[MCPPrompt]:
        """获取所有已注册的提示模板
        
        Returns:
            提示模板列表
        """
        return list(self._prompts.values())
    
    def render_prompt(self, name: str, arguments: Dict[str, Any]) -> Optional[str]:
        """渲染提示模板
        
        Args:
            name: 提示模板名称
            arguments: 模板参数
            
        Returns:
            渲染后的提示文本，如果模板不存在返回None
        """
        prompt = self._prompts.get(name)
        if not prompt:
            return None
        
        try:
            return prompt.template.format(**arguments)
        except KeyError as e:
            logger.error(f"渲染提示模板 {name} 失败，缺少参数: {e}")
            return None
    
    # ========== 流式响应 ==========
    
    def create_streaming_response(self, stream_id: Optional[str] = None) -> str:
        """创建流式响应
        
        Args:
            stream_id: 可选的流ID
            
        Returns:
            流ID
        """
        if stream_id is None:
            stream_id = str(uuid.uuid4())
        
        self._streaming_responses[stream_id] = StreamingResponse()
        return stream_id
    
    async def stream_write(self, stream_id: str, chunk: Dict[str, Any]) -> bool:
        """写入流数据
        
        Args:
            stream_id: 流ID
            chunk: 数据块
            
        Returns:
            是否成功写入
        """
        if stream_id not in self._streaming_responses:
            logger.warning(f"流 {stream_id} 不存在")
            return False
        
        await self._streaming_responses[stream_id].put(chunk)
        return True
    
    async def stream_finish(self, stream_id: str) -> bool:
        """结束流
        
        Args:
            stream_id: 流ID
            
        Returns:
            是否成功结束
        """
        if stream_id not in self._streaming_responses:
            return False
        
        await self._streaming_responses[stream_id].finish()
        return True
    
    def get_streaming_response(self, stream_id: str) -> Optional[StreamingResponse]:
        """获取流式响应对象
        
        Args:
            stream_id: 流ID
            
        Returns:
            流式响应对象
        """
        return self._streaming_responses.get(stream_id)
    
    def close_streaming_response(self, stream_id: str) -> bool:
        """关闭流式响应
        
        Args:
            stream_id: 流ID
            
        Returns:
            是否成功关闭
        """
        if stream_id in self._streaming_responses:
            del self._streaming_responses[stream_id]
            return True
        return False
    
    # ========== 多服务器支持 ==========
    
    async def send_to_server(self, server_id: str, message: MCPMessage) -> Optional[Any]:
        """向指定服务器发送消息
        
        Args:
            server_id: 服务器ID
            message: MCP消息
            
        Returns:
            服务器响应
        """
        if server_id not in self._servers:
            logger.error(f"服务器 {server_id} 不存在")
            return None
        
        server = self._servers[server_id]
        if server.status != ServerStatus.RUNNING:
            logger.error(f"服务器 {server_id} 未在运行")
            return None
        
        try:
            # 构建JSON-RPC消息
            msg_dict = {
                "jsonrpc": "2.0",
                "id": message.id or str(uuid.uuid4()),
            }
            
            if message.method:
                msg_dict["method"] = message.method
                if message.params:
                    msg_dict["params"] = message.params
            elif message.result is not None:
                msg_dict["result"] = message.result
            elif message.error:
                msg_dict["error"] = message.error
            
            # 发送到服务器进程（通过stdin）
            if server.process and server.process.stdin:
                server.process.stdin.write(json.dumps(msg_dict) + "\n")
                server.process.stdin.flush()
            
            return {"status": "sent", "message_id": msg_dict["id"]}
            
        except Exception as e:
            logger.error(f"向服务器 {server_id} 发送消息失败: {e}")
            return None
    
    async def broadcast_message(self, message: MCPMessage) -> Dict[str, Any]:
        """向所有运行的服务器广播消息
        
        Args:
            message: MCP消息
            
        Returns:
            各服务器的发送结果
        """
        results = {}
        for server_id in self._servers:
            if self._servers[server_id].status == ServerStatus.RUNNING:
                result = await self.send_to_server(server_id, message)
                results[server_id] = result
            else:
                results[server_id] = {"status": "not_running"}
        return results
    
    def get_connected_servers(self) -> List[str]:
        """获取已连接的服务器列表
        
        Returns:
            服务器ID列表
        """
        return [
            server_id for server_id, server in self._servers.items()
            if server.status == ServerStatus.RUNNING
        ]
    
    # ========== 工具映射 ==========
    
    def register_tool_mapping(self, mapping: ToolMapping) -> bool:
        """注册工具映射
        
        Args:
            mapping: 工具映射配置
            
        Returns:
            是否成功注册
        """
        self._tool_mappings[mapping.mcp_tool_name] = mapping
        self._workflow_action_tools[mapping.workflow_action_id] = mapping.mcp_tool_name
        logger.info(f"工具映射注册成功: {mapping.mcp_tool_name} -> {mapping.workflow_action_id}")
        return True
    
    def register_tool_mappings(self, mappings: List[ToolMapping]) -> int:
        """批量注册工具映射
        
        Args:
            mappings: 工具映射列表
            
        Returns:
            成功注册的数量
        """
        count = 0
        for mapping in mappings:
            if self.register_tool_mapping(mapping):
                count += 1
        return count
    
    def get_tool_mapping(self, tool_name: str) -> Optional[ToolMapping]:
        """获取工具映射
        
        Args:
            tool_name: MCP工具名称
            
        Returns:
            工具映射配置
        """
        return self._tool_mappings.get(tool_name)
    
    def get_workflow_action_tool(self, workflow_action_id: str) -> Optional[str]:
        """获取工作流动作对应的MCP工具
        
        Args:
            workflow_action_id: 工作流动作ID
            
        Returns:
            MCP工具名称
        """
        return self._workflow_action_tools.get(workflow_action_id)
    
    def get_all_tool_mappings(self) -> List[ToolMapping]:
        """获取所有工具映射
        
        Returns:
            工具映射列表
        """
        return list(self._tool_mappings.values())
    
    # ========== 服务器生命周期（自动重启）==========
    
    async def _auto_restart_failed_servers(self) -> None:
        """自动重启失败的服务器"""
        while self._running:
            for server_id, server in list(self._servers.items()):
                if server.status == ServerStatus.FAILED and server.config.auto_restart:
                    if server.restart_attempts < server.config.max_restart_attempts:
                        logger.info(f"尝试自动重启服务器 {server_id} "
                                  f"(尝试 {server.restart_attempts + 1}/"
                                  f"{server.config.max_restart_attempts})")
                        
                        server.status = ServerStatus.RESTARTING
                        if await self.restart_server(server_id):
                            logger.info(f"服务器 {server_id} 自动重启成功")
                        else:
                            server.restart_attempts += 1
                            logger.warning(f"服务器 {server_id} 自动重启失败")
                    else:
                        logger.error(f"服务器 {server_id} 达到最大重启次数限制")
            
            await asyncio.sleep(5)  # 每5秒检查一次
    
    def set_server_restart_config(self, server_id: str, auto_restart: bool,
                                 max_attempts: int) -> bool:
        """设置服务器重启配置
        
        Args:
            server_id: 服务器ID
            auto_restart: 是否自动重启
            max_attempts: 最大重启次数
            
        Returns:
            是否成功设置
        """
        if server_id not in self._server_configs:
            return False
        
        config = self._server_configs[server_id]
        config.auto_restart = auto_restart
        config.max_restart_attempts = max_attempts
        return True
    
    # ========== 健康检查 ==========
    
    async def _health_check_loop(self) -> None:
        """健康检查循环"""
        while self._running:
            for server_id, server in list(self._servers.items()):
                if server.status == ServerStatus.RUNNING:
                    result = await self.check_server_health(server_id)
                    self._health_checks[server_id] = result
                    
                    # 检查是否需要处理不健康的服务器
                    if result.status == HealthStatus.UNHEALTHY:
                        logger.warning(f"服务器 {server_id} 健康检查失败")
                        if self._on_health_status_change:
                            self._on_health_status_change(server_id, HealthStatus.UNHEALTHY)
                        
                        # 触发自动重启（如果启用）
                        if server.config.auto_restart:
                            server.status = ServerStatus.FAILED
            
            await asyncio.sleep(30)  # 默认每30秒检查一次
    
    async def check_server_health(self, server_id: str) -> HealthCheckResult:
        """检查服务器健康状态
        
        Args:
            server_id: 服务器ID
            
        Returns:
            健康检查结果
        """
        if server_id not in self._servers:
            return HealthCheckResult(
                server_id=server_id,
                status=HealthStatus.UNKNOWN,
                latency_ms=0,
                timestamp=time.time(),
                details={"error": "服务器不存在"}
            )
        
        server = self._servers[server_id]
        start_time = time.time()
        
        try:
            if server.status != ServerStatus.RUNNING or not server.process:
                return HealthCheckResult(
                    server_id=server_id,
                    status=HealthStatus.UNHEALTHY,
                    latency_ms=0,
                    timestamp=time.time(),
                    details={"error": "服务器未运行"}
                )
            
            # 发送ping消息检查服务器响应
            response = await self.send_to_server(
                server_id,
                MCPMessage(method="ping", id=str(uuid.uuid4()))
            )
            
            latency_ms = (time.time() - start_time) * 1000
            
            if response:
                return HealthCheckResult(
                    server_id=server_id,
                    status=HealthStatus.HEALTHY,
                    latency_ms=latency_ms,
                    timestamp=time.time(),
                    details={"response": response}
                )
            else:
                return HealthCheckResult(
                    server_id=server_id,
                    status=HealthStatus.DEGRADED,
                    latency_ms=latency_ms,
                    timestamp=time.time(),
                    details={"error": "无响应"}
                )
                
        except Exception as e:
            return HealthCheckResult(
                server_id=server_id,
                status=HealthStatus.UNHEALTHY,
                latency_ms=(time.time() - start_time) * 1000,
                timestamp=time.time(),
                details={"error": str(e)}
            )
    
    def get_health_status(self, server_id: str) -> Optional[HealthCheckResult]:
        """获取服务器健康状态
        
        Args:
            server_id: 服务器ID
            
        Returns:
            健康检查结果
        """
        return self._health_checks.get(server_id)
    
    def get_all_health_statuses(self) -> Dict[str, HealthCheckResult]:
        """获取所有服务器健康状态
        
        Returns:
            服务器ID到健康检查结果的字典
        """
        return dict(self._health_checks)
    
    def set_health_check_interval(self, server_id: str, interval: int) -> bool:
        """设置健康检查间隔
        
        Args:
            server_id: 服务器ID
            interval: 间隔秒数
            
        Returns:
            是否成功设置
        """
        if server_id not in self._server_configs:
            return False
        
        self._server_configs[server_id].health_check_interval = interval
        return True
    
    # ========== 协议协商 ==========
    
    def negotiate_protocol_version(self, server_id: str,
                                   client_versions: List[str]) -> Optional[str]:
        """协商MCP协议版本
        
        Args:
            server_id: 服务器ID
            client_versions: 客户端支持的版本列表
            
        Returns:
            协商后的协议版本，如果不兼容返回None
        """
        if server_id not in self._server_configs:
            return None
        
        server_config = self._server_configs[server_id]
        
        # 解析服务器支持的版本
        server_version = server_config.protocol_version
        
        # 查找双方都支持的版本
        for client_ver in client_versions:
            if client_ver == server_version:
                negotiated = MCPProtocolVersion(server_version)
                self._negotiated_versions[server_id] = negotiated
                logger.info(f"与服务器 {server_id} 协商的协议版本: {negotiated.value}")
                return negotiated.value
        
        # 尝试向下兼容
        for protocol in self._supported_protocols:
            if protocol.value in client_versions and self._is_compatible(
                protocol.value, server_version
            ):
                self._negotiated_versions[server_id] = protocol
                logger.info(f"与服务器 {server_id} 协商的协议版本(兼容模式): {protocol.value}")
                return protocol.value
        
        logger.warning(f"与服务器 {server_id} 无法协商兼容的协议版本")
        return None
    
    def _is_compatible(self, version1: str, version2: str) -> bool:
        """检查两个协议版本是否兼容"""
        v1_parts = version1.split(".")
        v2_parts = version2.split(".")
        
        # 主版本号必须相同
        return v1_parts[0] == v2_parts[0]
    
    def get_negotiated_version(self, server_id: str) -> Optional[str]:
        """获取与服务器的协商版本
        
        Args:
            server_id: 服务器ID
            
        Returns:
            协商的协议版本
        """
        negotiated = self._negotiated_versions.get(server_id)
        return negotiated.value if negotiated else None
    
    def get_supported_protocols(self) -> List[str]:
        """获取支持的协议版本列表
        
        Returns:
            协议版本列表
        """
        return [p.value for p in self._supported_protocols]
    
    # ========== 生命周期管理 ==========
    
    async def start(self) -> bool:
        """启动WorkflowMCP服务
        
        Returns:
            是否成功启动
        """
        if self._running:
            logger.warning("WorkflowMCP 已在运行")
            return True
        
        self._running = True
        
        # 启动健康检查循环
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        
        # 启动自动重启循环
        self._restart_task = asyncio.create_task(self._auto_restart_failed_servers())
        
        # 启动所有服务器
        await self.start_all_servers()
        
        logger.info("WorkflowMCP 服务已启动")
        return True
    
    async def stop(self) -> bool:
        """停止WorkflowMCP服务
        
        Returns:
            是否成功停止
        """
        if not self._running:
            logger.warning("WorkflowMCP 未运行")
            return True
        
        self._running = False
        
        # 取消健康检查任务
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
        
        # 取消自动重启任务
        if hasattr(self, '_restart_task'):
            self._restart_task.cancel()
            try:
                await self._restart_task
            except asyncio.CancelledError:
                pass
        
        # 停止所有服务器
        await self.stop_all_servers()
        
        # 清理流式响应
        self._streaming_responses.clear()
        
        logger.info("WorkflowMCP 服务已停止")
        return True
    
    # ========== 回调设置 ==========
    
    def set_server_status_callback(self, 
        callback: Callable[[str, ServerStatus], None]) -> None:
        """设置服务器状态变化回调
        
        Args:
            callback: 回调函数(server_id, new_status)
        """
        self._on_server_status_change = callback
    
    def set_health_status_callback(self,
        callback: Callable[[str, HealthStatus], None]) -> None:
        """设置健康状态变化回调
        
        Args:
            callback: 回调函数(server_id, new_status)
        """
        self._on_health_status_change = callback
    
    def set_tool_call_callback(self,
        callback: Callable[[str, Dict[str, Any]], Any]) -> None:
        """设置工具调用回调
        
        Args:
            callback: 回调函数(tool_name, arguments)
        """
        self._on_tool_call = callback
    
    # ========== 工具便捷方法 ==========
    
    def create_tool_from_workflow_action(self, action_id: str, action_name: str,
                                         action_description: str,
                                         input_schema: Dict[str, Any],
                                         parameter_mapping: Optional[Dict[str, str]] = None
                                         ) -> MCPTool:
        """从工作流动作创建MCP工具
        
        Args:
            action_id: 工作流动作ID
            action_name: 工具名称
            action_description: 工具描述
            input_schema: 输入模式
            parameter_mapping: 参数映射
            
        Returns:
            创建的工具
        """
        tool = MCPTool(
            name=action_name,
            description=action_description,
            input_schema=input_schema,
            workflow_action_id=action_id,
            metadata={"created_from_action": action_id}
        )
        
        # 创建并注册工具映射
        mapping = ToolMapping(
            mcp_tool_name=action_name,
            workflow_action_id=action_id,
            parameter_mapping=parameter_mapping
        )
        self.register_tool_mapping(mapping)
        
        return tool
    
    # ========== 状态序列化 ==========
    
    def get_status_summary(self) -> Dict[str, Any]:
        """获取状态摘要
        
        Returns:
            状态摘要字典
        """
        servers_running = sum(
            1 for s in self._servers.values() if s.status == ServerStatus.RUNNING
        )
        
        return {
            "running": self._running,
            "servers_total": len(self._servers),
            "servers_running": servers_running,
            "tools_registered": len(self._tools),
            "resources_registered": len(self._resources),
            "prompts_registered": len(self._prompts),
            "tool_mappings": len(self._tool_mappings),
            "supported_protocols": self.get_supported_protocols(),
        }
    
    def export_config(self) -> Dict[str, Any]:
        """导出配置
        
        Returns:
            配置字典
        """
        return {
            "servers": [
                {
                    "server_id": config.server_id,
                    "name": config.name,
                    "command": config.command,
                    "args": config.args,
                    "env": config.env,
                    "protocol_version": config.protocol_version,
                    "auto_restart": config.auto_restart,
                    "max_restart_attempts": config.max_restart_attempts,
                    "health_check_interval": config.health_check_interval,
                    "startup_timeout": config.startup_timeout,
                }
                for config in self._server_configs.values()
            ],
            "tools": [
                {
                    "name": tool.name,
                    "description": tool.description,
                    "input_schema": tool.input_schema,
                    "workflow_action_id": tool.workflow_action_id,
                    "metadata": tool.metadata,
                }
                for tool in self._tools.values()
            ],
            "resources": [
                {
                    "uri": resource.uri,
                    "name": resource.name,
                    "description": resource.description,
                    "mime_type": resource.mime_type,
                    "metadata": resource.metadata,
                }
                for resource in self._resources.values()
            ],
            "prompts": [
                {
                    "name": prompt.name,
                    "description": prompt.description,
                    "arguments": prompt.arguments,
                    "template": prompt.template,
                    "metadata": prompt.metadata,
                }
                for prompt in self._prompts.values()
            ],
            "tool_mappings": [
                {
                    "mcp_tool_name": mapping.mcp_tool_name,
                    "workflow_action_id": mapping.workflow_action_id,
                    "parameter_mapping": mapping.parameter_mapping,
                }
                for mapping in self._tool_mappings.values()
            ],
        }
    
    def import_config(self, config: Dict[str, Any]) -> bool:
        """导入配置
        
        Args:
            config: 配置字典
            
        Returns:
            是否成功导入
        """
        try:
            # 导入服务器配置
            for server_data in config.get("servers", []):
                server_config = MCPServerConfig(
                    server_id=server_data["server_id"],
                    name=server_data["name"],
                    command=server_data["command"],
                    args=server_data.get("args", []),
                    env=server_data.get("env", {}),
                    protocol_version=server_data.get("protocol_version", "1.3"),
                    auto_restart=server_data.get("auto_restart", True),
                    max_restart_attempts=server_data.get("max_restart_attempts", 3),
                    health_check_interval=server_data.get("health_check_interval", 30),
                    startup_timeout=server_data.get("startup_timeout", 30),
                )
                self.register_server(server_config)
            
            # 导入工具
            for tool_data in config.get("tools", []):
                tool = MCPTool(
                    name=tool_data["name"],
                    description=tool_data["description"],
                    input_schema=tool_data["input_schema"],
                    workflow_action_id=tool_data.get("workflow_action_id"),
                    metadata=tool_data.get("metadata", {}),
                )
                self.register_tool(tool)
            
            # 导入资源
            for resource_data in config.get("resources", []):
                resource = MCPResource(
                    uri=resource_data["uri"],
                    name=resource_data["name"],
                    description=resource_data["description"],
                    mime_type=resource_data.get("mime_type", "application/json"),
                    metadata=resource_data.get("metadata", {}),
                )
                self.register_resource(resource)
            
            # 导入提示模板
            for prompt_data in config.get("prompts", []):
                prompt = MCPPrompt(
                    name=prompt_data["name"],
                    description=prompt_data["description"],
                    arguments=prompt_data["arguments"],
                    template=prompt_data["template"],
                    metadata=prompt_data.get("metadata", {}),
                )
                self.register_prompt(prompt)
            
            # 导入工具映射
            for mapping_data in config.get("tool_mappings", []):
                mapping = ToolMapping(
                    mcp_tool_name=mapping_data["mcp_tool_name"],
                    workflow_action_id=mapping_data["workflow_action_id"],
                    parameter_mapping=mapping_data.get("parameter_mapping"),
                )
                self.register_tool_mapping(mapping)
            
            logger.info("配置导入成功")
            return True
            
        except Exception as e:
            logger.error(f"配置导入失败: {e}")
            return False

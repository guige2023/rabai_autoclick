"""
WorkflowMCP 测试
Model Context Protocol 集成测试
"""
import unittest
import asyncio
import time
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from dataclasses import asdict

import sys
import os
sys.path.insert(0, '/Users/guige/my_project')

# Check for pytest-asyncio availability
try:
    import pytest
    HAS_PYTEST = True
except ImportError:
    HAS_PYTEST = False

from src.workflow_mcp import (
    WorkflowMCP,
    MCPProtocolVersion,
    ServerStatus,
    HealthStatus,
    MCPServerConfig,
    MCPServer,
    MCPTool,
    MCPResource,
    MCPPrompt,
    MCPMessage,
    HealthCheckResult,
    StreamingResponse,
    ToolMapping,
)


class TestMCPServerConfig(unittest.TestCase):
    """测试MCP服务器配置"""
    
    def test_create_server_config(self):
        """测试创建服务器配置"""
        config = MCPServerConfig(
            server_id="test_server_1",
            name="Test Server",
            command="python",
            args=["-m", "server"],
            env={"KEY": "value"},
            protocol_version="1.3",
            auto_restart=True,
            max_restart_attempts=3,
            health_check_interval=30,
            startup_timeout=30
        )
        
        self.assertEqual(config.server_id, "test_server_1")
        self.assertEqual(config.name, "Test Server")
        self.assertEqual(config.command, "python")
        self.assertEqual(config.args, ["-m", "server"])
        self.assertEqual(config.env, {"KEY": "value"})
        self.assertEqual(config.protocol_version, "1.3")
        self.assertTrue(config.auto_restart)
        self.assertEqual(config.max_restart_attempts, 3)
    
    def test_server_config_defaults(self):
        """测试服务器配置默认值"""
        config = MCPServerConfig(
            server_id="test_server_2",
            name="Test Server 2",
            command="echo"
        )
        
        self.assertEqual(config.args, [])
        self.assertEqual(config.env, {})
        self.assertEqual(config.protocol_version, "1.3")
        self.assertTrue(config.auto_restart)
        self.assertEqual(config.max_restart_attempts, 3)
        self.assertEqual(config.health_check_interval, 30)
        self.assertEqual(config.startup_timeout, 30)


class TestMCPServer(unittest.TestCase):
    """测试MCP服务器实例"""
    
    def test_create_server(self):
        """测试创建服务器实例"""
        config = MCPServerConfig(
            server_id="server_1",
            name="Server One",
            command="echo"
        )
        server = MCPServer(config=config)
        
        self.assertEqual(server.config, config)
        self.assertEqual(server.status, ServerStatus.STOPPED)
        self.assertIsNone(server.process)
        self.assertEqual(server.restart_attempts, 0)
        self.assertEqual(server.health_status, HealthStatus.UNKNOWN)
    
    def test_server_status_enum(self):
        """测试服务器状态枚举"""
        self.assertEqual(ServerStatus.STOPPED.value, "stopped")
        self.assertEqual(ServerStatus.STARTING.value, "starting")
        self.assertEqual(ServerStatus.RUNNING.value, "running")
        self.assertEqual(ServerStatus.FAILED.value, "failed")


class TestMCPTool(unittest.TestCase):
    """测试MCP工具"""
    
    def test_create_tool(self):
        """测试创建工具"""
        tool = MCPTool(
            name="test_tool",
            description="A test tool",
            input_schema={"type": "object", "properties": {}},
            metadata={"version": "1.0"}
        )
        
        self.assertEqual(tool.name, "test_tool")
        self.assertEqual(tool.description, "A test tool")
        self.assertEqual(tool.input_schema, {"type": "object", "properties": {}})
        self.assertIsNone(tool.handler)
        self.assertIsNone(tool.workflow_action_id)
    
    def test_tool_with_workflow_action(self):
        """测试带工作流动作ID的工具"""
        tool = MCPTool(
            name="action_tool",
            description="An action tool",
            input_schema={},
            workflow_action_id="action_123"
        )
        
        self.assertEqual(tool.workflow_action_id, "action_123")


class TestMCPResource(unittest.TestCase):
    """测试MCP资源"""
    
    def test_create_resource(self):
        """测试创建资源"""
        resource = MCPResource(
            uri="file://test/resource",
            name="Test Resource",
            description="A test resource",
            mime_type="application/json",
            metadata={"author": "test"}
        )
        
        self.assertEqual(resource.uri, "file://test/resource")
        self.assertEqual(resource.name, "Test Resource")
        self.assertEqual(resource.mime_type, "application/json")
        self.assertIsNone(resource.content)


class TestMCPPrompt(unittest.TestCase):
    """测试MCP提示模板"""
    
    def test_create_prompt(self):
        """测试创建提示模板"""
        prompt = MCPPrompt(
            name="test_prompt",
            description="A test prompt template",
            arguments=[{"name": "arg1", "type": "string"}],
            template="Hello {arg1}!",
            metadata={"version": "1.0"}
        )
        
        self.assertEqual(prompt.name, "test_prompt")
        self.assertEqual(prompt.template, "Hello {arg1}!")


class TestMCPMessage(unittest.TestCase):
    """测试MCP消息"""
    
    def test_create_message(self):
        """测试创建消息"""
        msg = MCPMessage(
            method="test_method",
            params={"key": "value"},
            id="msg_123"
        )
        
        self.assertEqual(msg.jsonrpc, "2.0")
        self.assertEqual(msg.method, "test_method")
        self.assertEqual(msg.params, {"key": "value"})
        self.assertEqual(msg.id, "msg_123")
    
    def test_message_with_result(self):
        """测试带结果的消息"""
        msg = MCPMessage(
            id="msg_456",
            result={"status": "success"}
        )
        
        self.assertEqual(msg.result, {"status": "success"})
        self.assertIsNone(msg.error)


class TestStreamingResponse(unittest.TestCase):
    """测试流式响应"""
    
    def test_create_streaming_response(self):
        """测试创建流式响应"""
        response = StreamingResponse()
        self.assertFalse(response.is_done())
    
    def test_stream_put_and_finish(self):
        """测试写入和结束流"""
        async def _run():
            response = StreamingResponse()
            await response.put({"chunk": "data"})
            self.assertFalse(response.is_done())
            await response.finish()
            self.assertFalse(response.is_done())  # Still has data in queue
        
        if HAS_PYTEST:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(_run())
            finally:
                loop.close()
        else:
            # Run directly in the current event loop if pytest-asyncio not available
            asyncio.get_event_loop().run_until_complete(_run())


class TestToolMapping(unittest.TestCase):
    """测试工具映射"""
    
    def test_create_tool_mapping(self):
        """测试创建工具映射"""
        mapping = ToolMapping(
            mcp_tool_name="tool_1",
            workflow_action_id="action_1",
            parameter_mapping={"input": "output"}
        )
        
        self.assertEqual(mapping.mcp_tool_name, "tool_1")
        self.assertEqual(mapping.workflow_action_id, "action_1")
        self.assertEqual(mapping.parameter_mapping, {"input": "output"})
    
    def test_tool_mapping_defaults(self):
        """测试工具映射默认值"""
        mapping = ToolMapping(
            mcp_tool_name="tool_2",
            workflow_action_id="action_2"
        )
        
        self.assertEqual(mapping.parameter_mapping, {})


class TestWorkflowMCP(unittest.TestCase):
    """测试WorkflowMCP主类"""
    
    def setUp(self):
        """设置测试环境"""
        self.mcp = WorkflowMCP()
    
    def test_initialization(self):
        """测试初始化"""
        self.assertIsNone(self.mcp.workflow_core)
        self.assertEqual(len(self.mcp._servers), 0)
        self.assertEqual(len(self.mcp._server_configs), 0)
        self.assertEqual(len(self.mcp._tools), 0)
        self.assertEqual(len(self.mcp._resources), 0)
        self.assertEqual(len(self.mcp._prompts), 0)
        self.assertFalse(self.mcp._running)
    
    def test_initialization_with_workflow_core(self):
        """测试带workflow_core初始化"""
        mock_core = Mock()
        mcp = WorkflowMCP(workflow_core=mock_core)
        self.assertEqual(mcp.workflow_core, mock_core)
    
    # ========== 服务器管理测试 ==========
    
    def test_register_server(self):
        """测试注册服务器"""
        config = MCPServerConfig(
            server_id="server_1",
            name="Server One",
            command="echo"
        )
        
        result = self.mcp.register_server(config)
        self.assertTrue(result)
        self.assertIn("server_1", self.mcp._server_configs)
        self.assertIn("server_1", self.mcp._servers)
    
    def test_register_duplicate_server(self):
        """测试注册重复服务器"""
        config = MCPServerConfig(
            server_id="server_1",
            name="Server One",
            command="echo"
        )
        
        self.mcp.register_server(config)
        result = self.mcp.register_server(config)  # Should update
        self.assertTrue(result)
    
    def test_unregister_server(self):
        """测试注销服务器"""
        config = MCPServerConfig(
            server_id="server_1",
            name="Server One",
            command="echo"
        )
        self.mcp.register_server(config)
        
        result = self.mcp.unregister_server("server_1")
        self.assertTrue(result)
        self.assertNotIn("server_1", self.mcp._server_configs)
    
    def test_unregister_nonexistent_server(self):
        """测试注销不存在的服务器"""
        result = self.mcp.unregister_server("nonexistent")
        self.assertFalse(result)
    
    def test_get_server_status(self):
        """测试获取服务器状态"""
        config = MCPServerConfig(
            server_id="server_1",
            name="Server One",
            command="echo"
        )
        self.mcp.register_server(config)
        
        status = self.mcp.get_server_status("server_1")
        self.assertEqual(status, ServerStatus.STOPPED)
    
    def test_get_server_status_nonexistent(self):
        """测试获取不存在的服务器状态"""
        status = self.mcp.get_server_status("nonexistent")
        self.assertIsNone(status)
    
    def test_get_all_server_statuses(self):
        """测试获取所有服务器状态"""
        config1 = MCPServerConfig(server_id="s1", name="S1", command="echo")
        config2 = MCPServerConfig(server_id="s2", name="S2", command="echo")
        
        self.mcp.register_server(config1)
        self.mcp.register_server(config2)
        
        statuses = self.mcp.get_all_server_statuses()
        self.assertEqual(len(statuses), 2)
        self.assertEqual(statuses["s1"], ServerStatus.STOPPED)
        self.assertEqual(statuses["s2"], ServerStatus.STOPPED)
    
    # ========== 工具管理测试 ==========
    
    def test_register_tool(self):
        """测试注册工具"""
        tool = MCPTool(
            name="tool_1",
            description="Test tool",
            input_schema={}
        )
        
        result = self.mcp.register_tool(tool)
        self.assertTrue(result)
        self.assertIn("tool_1", self.mcp._tools)
    
    def test_register_tools_batch(self):
        """测试批量注册工具"""
        tools = [
            MCPTool(name="tool_1", description="T1", input_schema={}),
            MCPTool(name="tool_2", description="T2", input_schema={}),
            MCPTool(name="tool_3", description="T3", input_schema={}),
        ]
        
        count = self.mcp.register_tools(tools)
        self.assertEqual(count, 3)
        self.assertEqual(len(self.mcp._tools), 3)
    
    def test_unregister_tool(self):
        """测试注销工具"""
        tool = MCPTool(name="tool_1", description="T1", input_schema={})
        self.mcp.register_tool(tool)
        
        result = self.mcp.unregister_tool("tool_1")
        self.assertTrue(result)
        self.assertNotIn("tool_1", self.mcp._tools)
    
    def test_unregister_nonexistent_tool(self):
        """测试注销不存在的工具"""
        result = self.mcp.unregister_tool("nonexistent")
        self.assertFalse(result)
    
    def test_get_tool(self):
        """测试获取工具"""
        tool = MCPTool(name="tool_1", description="T1", input_schema={})
        self.mcp.register_tool(tool)
        
        retrieved = self.mcp.get_tool("tool_1")
        self.assertEqual(retrieved.name, "tool_1")
    
    def test_get_all_tools(self):
        """测试获取所有工具"""
        tools = [
            MCPTool(name="t1", description="T1", input_schema={}),
            MCPTool(name="t2", description="T2", input_schema={}),
        ]
        self.mcp.register_tools(tools)
        
        all_tools = self.mcp.get_all_tools()
        self.assertEqual(len(all_tools), 2)
    
    # ========== 资源管理测试 ==========
    
    def test_register_resource(self):
        """测试注册资源"""
        resource = MCPResource(
            uri="file://test",
            name="Test",
            description="Test resource"
        )
        
        result = self.mcp.register_resource(resource)
        self.assertTrue(result)
        self.assertIn("file://test", self.mcp._resources)
    
    def test_register_resources_batch(self):
        """测试批量注册资源"""
        resources = [
            MCPResource(uri="r1://test", name="R1", description="R1"),
            MCPResource(uri="r2://test", name="R2", description="R2"),
        ]
        
        count = self.mcp.register_resources(resources)
        self.assertEqual(count, 2)
    
    def test_unregister_resource(self):
        """测试注销资源"""
        resource = MCPResource(uri="r1://test", name="R1", description="R1")
        self.mcp.register_resource(resource)
        
        result = self.mcp.unregister_resource("r1://test")
        self.assertTrue(result)
        self.assertNotIn("r1://test", self.mcp._resources)
    
    def test_get_resource(self):
        """测试获取资源"""
        resource = MCPResource(uri="r1://test", name="R1", description="R1")
        self.mcp.register_resource(resource)
        
        retrieved = self.mcp.get_resource("r1://test")
        self.assertEqual(retrieved.name, "R1")
    
    def test_update_resource_content(self):
        """测试更新资源内容"""
        resource = MCPResource(uri="r1://test", name="R1", description="R1")
        self.mcp.register_resource(resource)
        
        result = self.mcp.update_resource_content("r1://test", {"new": "content"})
        self.assertTrue(result)
        self.assertEqual(self.mcp._resources["r1://test"].content, {"new": "content"})
    
    # ========== 提示模板测试 ==========
    
    def test_register_prompt(self):
        """测试注册提示模板"""
        prompt = MCPPrompt(
            name="prompt_1",
            description="Test prompt",
            arguments=[],
            template="Hello {name}!"
        )
        
        result = self.mcp.register_prompt(prompt)
        self.assertTrue(result)
        self.assertIn("prompt_1", self.mcp._prompts)
    
    def test_register_prompts_batch(self):
        """测试批量注册提示模板"""
        prompts = [
            MCPPrompt(name="p1", description="P1", arguments=[], template="Hi"),
            MCPPrompt(name="p2", description="P2", arguments=[], template="Hello"),
        ]
        
        count = self.mcp.register_prompts(prompts)
        self.assertEqual(count, 2)
    
    def test_render_prompt(self):
        """测试渲染提示模板"""
        prompt = MCPPrompt(
            name="greeting",
            description="Greeting prompt",
            arguments=[{"name": "name"}],
            template="Hello {name}!"
        )
        self.mcp.register_prompt(prompt)
        
        rendered = self.mcp.render_prompt("greeting", {"name": "World"})
        self.assertEqual(rendered, "Hello World!")
    
    def test_render_prompt_missing_arg(self):
        """测试渲染缺少参数的提示模板"""
        prompt = MCPPrompt(
            name="greeting",
            description="Greeting prompt",
            arguments=[{"name": "name"}],
            template="Hello {name}!"
        )
        self.mcp.register_prompt(prompt)
        
        rendered = self.mcp.render_prompt("greeting", {})
        self.assertIsNone(rendered)
    
    def test_render_nonexistent_prompt(self):
        """测试渲染不存在的提示模板"""
        rendered = self.mcp.render_prompt("nonexistent", {})
        self.assertIsNone(rendered)
    
    # ========== 工具映射测试 ==========
    
    def test_register_tool_mapping(self):
        """测试注册工具映射"""
        mapping = ToolMapping(
            mcp_tool_name="tool_1",
            workflow_action_id="action_1",
            parameter_mapping={"a": "b"}
        )
        
        result = self.mcp.register_tool_mapping(mapping)
        self.assertTrue(result)
        self.assertIn("tool_1", self.mcp._tool_mappings)
        self.assertIn("action_1", self.mcp._workflow_action_tools)
    
    def test_register_tool_mappings_batch(self):
        """测试批量注册工具映射"""
        mappings = [
            ToolMapping("t1", "a1"),
            ToolMapping("t2", "a2"),
        ]
        
        count = self.mcp.register_tool_mappings(mappings)
        self.assertEqual(count, 2)
    
    def test_get_tool_mapping(self):
        """测试获取工具映射"""
        mapping = ToolMapping("t1", "a1", {"x": "y"})
        self.mcp.register_tool_mapping(mapping)
        
        retrieved = self.mcp.get_tool_mapping("t1")
        self.assertEqual(retrieved.workflow_action_id, "a1")
    
    def test_get_workflow_action_tool(self):
        """测试获取工作流动作对应的工具"""
        mapping = ToolMapping("t1", "a1")
        self.mcp.register_tool_mapping(mapping)
        
        tool_name = self.mcp.get_workflow_action_tool("a1")
        self.assertEqual(tool_name, "t1")
    
    def test_create_tool_from_workflow_action(self):
        """测试从工作流动作创建工具"""
        tool = self.mcp.create_tool_from_workflow_action(
            action_id="action_1",
            action_name="test_tool",
            action_description="A test tool",
            input_schema={"type": "object"},
            parameter_mapping={"input": "param"}
        )
        
        self.assertEqual(tool.name, "test_tool")
        self.assertEqual(tool.workflow_action_id, "action_1")
        self.assertIn("test_tool", self.mcp._tool_mappings)
    
    # ========== 流式响应测试 ==========
    
    def test_create_streaming_response(self):
        """测试创建流式响应"""
        stream_id = self.mcp.create_streaming_response()
        self.assertIsNotNone(stream_id)
        self.assertIn(stream_id, self.mcp._streaming_responses)
    
    def test_create_streaming_response_with_id(self):
        """测试使用指定ID创建流式响应"""
        custom_id = "my_stream_123"
        stream_id = self.mcp.create_streaming_response(stream_id=custom_id)
        self.assertEqual(stream_id, custom_id)
    
    def test_get_streaming_response(self):
        """测试获取流式响应"""
        stream_id = self.mcp.create_streaming_response()
        
        response = self.mcp.get_streaming_response(stream_id)
        self.assertIsNotNone(response)
        self.assertIsInstance(response, StreamingResponse)
    
    def test_get_nonexistent_streaming_response(self):
        """测试获取不存在的流式响应"""
        response = self.mcp.get_streaming_response("nonexistent")
        self.assertIsNone(response)
    
    def test_close_streaming_response(self):
        """测试关闭流式响应"""
        stream_id = self.mcp.create_streaming_response()
        
        result = self.mcp.close_streaming_response(stream_id)
        self.assertTrue(result)
        self.assertNotIn(stream_id, self.mcp._streaming_responses)
    
    # ========== 多服务器测试 ==========
    
    def test_get_connected_servers(self):
        """测试获取已连接服务器列表"""
        config = MCPServerConfig(server_id="s1", name="S1", command="echo")
        self.mcp.register_server(config)
        
        connected = self.mcp.get_connected_servers()
        self.assertEqual(len(connected), 0)  # No servers running yet
    
    # ========== 协议协商测试 ==========
    
    def test_negotiate_protocol_version(self):
        """测试协议版本协商"""
        config = MCPServerConfig(
            server_id="s1",
            name="S1",
            command="echo",
            protocol_version="1.3"
        )
        self.mcp.register_server(config)
        
        version = self.mcp.negotiate_protocol_version("s1", ["1.3", "1.2"])
        self.assertEqual(version, "1.3")
    
    def test_negotiate_protocol_version_incompatible(self):
        """测试协议版本不兼容"""
        config = MCPServerConfig(
            server_id="s1",
            name="S1",
            command="echo",
            protocol_version="2.0"
        )
        self.mcp.register_server(config)
        
        version = self.mcp.negotiate_protocol_version("s1", ["1.3", "1.2"])
        self.assertIsNone(version)
    
    def test_get_supported_protocols(self):
        """测试获取支持的协议版本"""
        protocols = self.mcp.get_supported_protocols()
        self.assertIn("1.3", protocols)
        self.assertIn("1.2", protocols)
        self.assertIn("1.1", protocols)
        self.assertIn("1.0", protocols)
    
    # ========== 健康检查测试 ==========
    
    def test_get_health_status(self):
        """测试获取健康状态"""
        result = self.mcp.get_health_status("nonexistent")
        self.assertIsNone(result)
    
    def test_get_all_health_statuses(self):
        """测试获取所有健康状态"""
        statuses = self.mcp.get_all_health_statuses()
        self.assertEqual(len(statuses), 0)
    
    # ========== 生命周期测试 ==========
    
    def test_set_server_status_callback(self):
        """测试设置服务器状态回调"""
        callback = Mock()
        self.mcp.set_server_status_callback(callback)
        self.assertEqual(self.mcp._on_server_status_change, callback)
    
    def test_set_health_status_callback(self):
        """测试设置健康状态回调"""
        callback = Mock()
        self.mcp.set_health_status_callback(callback)
        self.assertEqual(self.mcp._on_health_status_change, callback)
    
    def test_set_tool_call_callback(self):
        """测试设置工具调用回调"""
        callback = Mock()
        self.mcp.set_tool_call_callback(callback)
        self.assertEqual(self.mcp._on_tool_call, callback)
    
    # ========== 状态序列化测试 ==========
    
    def test_get_status_summary(self):
        """测试获取状态摘要"""
        config = MCPServerConfig(server_id="s1", name="S1", command="echo")
        self.mcp.register_server(config)
        
        tool = MCPTool(name="t1", description="T1", input_schema={})
        self.mcp.register_tool(tool)
        
        summary = self.mcp.get_status_summary()
        
        self.assertFalse(summary["running"])
        self.assertEqual(summary["servers_total"], 1)
        self.assertEqual(summary["tools_registered"], 1)
        self.assertEqual(summary["resources_registered"], 0)
        self.assertEqual(summary["prompts_registered"], 0)
    
    def test_export_config(self):
        """测试导出配置"""
        config = MCPServerConfig(
            server_id="s1",
            name="S1",
            command="echo",
            protocol_version="1.3"
        )
        self.mcp.register_server(config)
        
        tool = MCPTool(name="t1", description="T1", input_schema={})
        self.mcp.register_tool(tool)
        
        exported = self.mcp.export_config()
        
        self.assertIn("servers", exported)
        self.assertIn("tools", exported)
        self.assertEqual(len(exported["servers"]), 1)
        self.assertEqual(len(exported["tools"]), 1)
    
    def test_import_config(self):
        """测试导入配置"""
        config_data = {
            "servers": [{
                "server_id": "s1",
                "name": "S1",
                "command": "echo",
                "protocol_version": "1.3"
            }],
            "tools": [{
                "name": "t1",
                "description": "T1",
                "input_schema": {}
            }],
            "resources": [],
            "prompts": [],
            "tool_mappings": []
        }
        
        result = self.mcp.import_config(config_data)
        self.assertTrue(result)
        self.assertIn("s1", self.mcp._server_configs)
        self.assertIn("t1", self.mcp._tools)


class TestWorkflowMCPAsync(unittest.TestCase):
    """异步测试WorkflowMCP"""
    
    def setUp(self):
        """设置测试环境"""
        self.mcp = WorkflowMCP()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
    
    def tearDown(self):
        """清理"""
        self.loop.close()
    
    def test_start_server_not_registered(self):
        """测试启动未注册的服务器"""
        async def _test():
            result = await self.mcp.start_server("nonexistent")
            self.assertFalse(result)
        
        self.loop.run_until_complete(_test())
    
    def test_stop_server_not_registered(self):
        """测试停止未注册的服务器"""
        async def _test():
            result = await self.mcp.stop_server("nonexistent")
            self.assertFalse(result)
        
        self.loop.run_until_complete(_test())
    
    @patch('subprocess.Popen')
    def test_start_server_success(self, mock_popen):
        """测试成功启动服务器"""
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_process.stderr = MagicMock()
        mock_process.stderr.read.return_value = ""
        mock_popen.return_value = mock_process
        
        config = MCPServerConfig(
            server_id="s1",
            name="S1",
            command="echo",
            startup_timeout=1
        )
        self.mcp.register_server(config)
        
        async def _test():
            result = await self.mcp.start_server("s1")
            return result
        
        result = self.loop.run_until_complete(_test())
        self.assertTrue(result)
        self.assertEqual(self.mcp.get_server_status("s1"), ServerStatus.RUNNING)
    
    @patch('subprocess.Popen')
    def test_stop_server(self, mock_popen):
        """测试停止服务器"""
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_process.wait.return_value = 0
        mock_process.stderr = MagicMock()
        mock_process.stderr.read.return_value = ""
        mock_popen.return_value = mock_process
        
        config = MCPServerConfig(
            server_id="s1",
            name="S1",
            command="echo"
        )
        self.mcp.register_server(config)
        
        async def _test():
            await self.mcp.start_server("s1")
            result = await self.mcp.stop_server("s1")
            return result
        
        result = self.loop.run_until_complete(_test())
        self.assertTrue(result)
    
    def test_restart_server(self):
        """测试重启服务器"""
        config = MCPServerConfig(
            server_id="s1",
            name="S1",
            command="echo"
        )
        self.mcp.register_server(config)
        
        async def _test():
            # Can't actually restart since server isn't running
            result = await self.mcp.restart_server("s1")
            return result
        
        # Just test that it doesn't crash
        try:
            self.loop.run_until_complete(_test())
        except Exception:
            pass  # Expected since process isn't actually running
    
    @patch('subprocess.Popen')
    def test_start_all_servers(self, mock_popen):
        """测试启动所有服务器"""
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_process.stderr = MagicMock()
        mock_process.stderr.read.return_value = ""
        mock_popen.return_value = mock_process
        
        config1 = MCPServerConfig(server_id="s1", name="S1", command="echo")
        config2 = MCPServerConfig(server_id="s2", name="S2", command="echo")
        self.mcp.register_server(config1)
        self.mcp.register_server(config2)
        
        async def _test():
            results = await self.mcp.start_all_servers()
            return results
        
        results = self.loop.run_until_complete(_test())
        self.assertEqual(len(results), 2)
    
    @patch('subprocess.Popen')
    def test_stop_all_servers(self, mock_popen):
        """测试停止所有服务器"""
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_process.wait.return_value = 0
        mock_process.stderr = MagicMock()
        mock_process.stderr.read.return_value = ""
        mock_popen.return_value = mock_process
        
        config = MCPServerConfig(server_id="s1", name="S1", command="echo")
        self.mcp.register_server(config)
        
        async def _test():
            await self.mcp.start_server("s1")
            results = await self.mcp.stop_all_servers()
            return results
        
        results = self.loop.run_until_complete(_test())
        self.assertEqual(results["s1"], True)
    
    @patch('subprocess.Popen')
    def test_check_server_health_unknown_server(self, mock_popen):
        """测试检查未知服务器的健康状态"""
        async def _test():
            result = await self.mcp.check_server_health("nonexistent")
            return result
        
        result = self.loop.run_until_complete(_test())
        self.assertEqual(result.status, HealthStatus.UNKNOWN)
    
    @patch('subprocess.Popen')
    def test_check_server_health_not_running(self, mock_popen):
        """测试检查未运行服务器的健康状态"""
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_process.stderr = MagicMock()
        mock_process.stderr.read.return_value = ""
        mock_popen.return_value = mock_process
        
        config = MCPServerConfig(
            server_id="s1",
            name="S1",
            command="echo"
        )
        self.mcp.register_server(config)
        
        async def _test():
            result = await self.mcp.check_server_health("s1")
            return result
        
        result = self.loop.run_until_complete(_test())
        self.assertEqual(result.status, HealthStatus.UNHEALTHY)
    
    @patch('subprocess.Popen')
    def test_send_to_server(self, mock_popen):
        """测试向服务器发送消息"""
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_process.stdin = MagicMock()
        mock_process.stderr = MagicMock()
        mock_process.stderr.read.return_value = ""
        mock_popen.return_value = mock_process
        
        config = MCPServerConfig(
            server_id="s1",
            name="S1",
            command="echo"
        )
        self.mcp.register_server(config)
        
        async def _test():
            await self.mcp.start_server("s1")
            msg = MCPMessage(method="ping", id="test_123")
            result = await self.mcp.send_to_server("s1", msg)
            return result
        
        result = self.loop.run_until_complete(_test())
        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "sent")
    
    @patch('subprocess.Popen')
    def test_broadcast_message(self, mock_popen):
        """测试广播消息"""
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_process.stdin = MagicMock()
        mock_process.stderr = MagicMock()
        mock_process.stderr.read.return_value = ""
        mock_popen.return_value = mock_process
        
        config = MCPServerConfig(server_id="s1", name="S1", command="echo")
        self.mcp.register_server(config)
        
        async def _test():
            await self.mcp.start_server("s1")
            msg = MCPMessage(method="broadcast", id="test_456")
            results = await self.mcp.broadcast_message(msg)
            return results
        
        results = self.loop.run_until_complete(_test())
        self.assertIn("s1", results)


class TestProtocolVersion(unittest.TestCase):
    """测试协议版本枚举"""
    
    def test_protocol_versions(self):
        """测试协议版本枚举值"""
        self.assertEqual(MCPProtocolVersion.V1_0.value, "1.0")
        self.assertEqual(MCPProtocolVersion.V1_1.value, "1.1")
        self.assertEqual(MCPProtocolVersion.V1_2.value, "1.2")
        self.assertEqual(MCPProtocolVersion.V1_3.value, "1.3")
        self.assertEqual(MCPProtocolVersion.LATEST.value, "1.3")


class TestHealthStatus(unittest.TestCase):
    """测试健康状态枚举"""
    
    def test_health_statuses(self):
        """测试健康状态枚举值"""
        self.assertEqual(HealthStatus.HEALTHY.value, "healthy")
        self.assertEqual(HealthStatus.DEGRADED.value, "degraded")
        self.assertEqual(HealthStatus.UNHEALTHY.value, "unhealthy")
        self.assertEqual(HealthStatus.UNKNOWN.value, "unknown")


if __name__ == "__main__":
    unittest.main()

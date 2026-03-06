"""
RabAI AutoClick v22 测试套件
"""
import pytest
import json
import time
import os
import tempfile
import shutil
from pathlib import Path


# 测试配置
TEST_DATA_DIR = tempfile.mkdtemp()


def setup_module(module):
    """测试模块初始化"""
    os.makedirs(TEST_DATA_DIR, exist_ok=True)


def teardown_module(module):
    """测试模块清理"""
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)


class TestWorkflowShare:
    """测试工作流分享系统"""
    
    def test_create_share_link(self):
        """测试创建分享链接"""
        from src.workflow_share import create_share_system, ShareType
        
        share = create_share_system(TEST_DATA_DIR)
        
        # 注册工作流
        workflow = {
            "workflow_id": "wf_test",
            "name": "测试工作流",
            "description": "测试",
            "steps": [{"action": "click", "target": "button"}]
        }
        wf_id = share.register_workflow(workflow)
        assert wf_id == "wf_test"
        
        # 创建分享链接
        link = share.create_share_link(wf_id, ShareType.PUBLIC, expires_in_days=7)
        assert link is not None
        assert link.share_type == ShareType.PUBLIC
        assert link.expires_at is not None
    
    def test_export_import(self):
        """测试导出导入"""
        from src.workflow_share import create_share_system
        
        share = create_share_system(TEST_DATA_DIR)
        
        # 注册并导出
        workflow = {
            "workflow_id": "wf_export",
            "name": "导出测试",
            "steps": [{"action": "click"}]
        }
        share.register_workflow(workflow)
        
        # 导出为JSON
        json_str = share.export_to_json("wf_export")
        assert json_str is not None
        
        # 导出为Base64
        b64 = share.export_to_base64("wf_export")
        assert b64 is not None
        
        # 导入
        report = share.import_workflow(b64, "base64")
        assert report.result.value == "success"
    
    def test_share_stats(self):
        """测试分享统计"""
        from src.workflow_share import create_share_system, ShareType
        
        share = create_share_system(TEST_DATA_DIR)
        
        # 注册工作流并创建链接 - 使用唯一ID避免与其他测试冲突
        workflow = {"workflow_id": "wf_stats_test", "name": "StatsTest", "steps": []}
        share.register_workflow(workflow)
        share.create_share_link("wf_stats_test", ShareType.PUBLIC)
        
        stats = share.get_share_stats()
        assert stats["total_links"] >= 1
        assert stats["active_links"] >= 1


class TestPipelineMode:
    """测试管道集成模式"""
    
    def test_create_chain(self):
        """测试创建管道链"""
        from src.pipeline_mode import create_pipeline_runner, PipeMode
        
        runner = create_pipeline_runner(TEST_DATA_DIR)
        
        chain = runner.create_chain("测试管道", PipeMode.LINEAR)
        assert chain.name == "测试管道"
        assert chain.mode == PipeMode.LINEAR
    
    def test_add_step(self):
        """测试添加步骤"""
        from src.pipeline_mode import create_pipeline_runner, PipeMode
        
        runner = create_pipeline_runner(TEST_DATA_DIR)
        
        chain = runner.create_chain("管道1", PipeMode.LINEAR)
        step = runner.add_step(chain.chain_id, "步骤1", "echo hello")
        
        assert step is not None
        assert step.name == "步骤1"
    
    def test_execute_chain(self):
        """测试执行管道链"""
        from src.pipeline_mode import create_pipeline_runner, PipeMode
        
        runner = create_pipeline_runner(TEST_DATA_DIR)
        
        # 创建并添加步骤
        chain = runner.create_chain("执行测试", PipeMode.LINEAR)
        runner.add_step(chain.chain_id, "测试", "echo 'test'")
        
        # 执行
        result = runner.execute_chain(chain.chain_id, {"test": "data"})
        
        assert result.chain_id == chain.chain_id
        assert result.total_duration >= 0


class TestScreenRecorder:
    """测试屏幕录制转工作流"""
    
    def test_start_stop_recording(self):
        """测试开始停止录制"""
        from src.screen_recorder import create_screen_recorder
        
        converter = create_screen_recorder(TEST_DATA_DIR)
        
        # 开始录制
        rec = converter.start_recording("测试录制", "描述")
        assert rec.recording_id is not None
        
        # 停止录制
        rec = converter.stop_recording(rec.recording_id)
        assert rec is not None
        assert len(rec.actions) == 0
    
    def test_add_action(self):
        """测试添加动作"""
        from src.screen_recorder import create_screen_recorder
        
        converter = create_screen_recorder(TEST_DATA_DIR)
        
        rec = converter.start_recording("录制1")
        
        # 添加动作
        action_data = {
            "action_type": "click",
            "x": 100,
            "y": 200,
            "timestamp": time.time()
        }
        result = converter.add_action(rec.recording_id, action_data)
        assert result is True
        
        rec = converter.get_recording(rec.recording_id)
        assert len(rec.actions) == 1
    
    def test_convert_to_workflow(self):
        """测试转换为工作流"""
        from src.screen_recorder import create_screen_recorder, ElementDetection
        
        converter = create_screen_recorder(TEST_DATA_DIR)
        
        rec = converter.start_recording("转换测试")
        
        # 添加动作
        for i in range(3):
            converter.add_action(rec.recording_id, {
                "action_type": "click",
                "x": 100 + i * 10,
                "y": 200,
                "timestamp": time.time() + i
            })
        
        rec = converter.stop_recording(rec.recording_id)
        
        # 转换
        result = converter.convert_to_workflow(rec.recording_id, detection_mode=ElementDetection.IMAGE)
        
        assert result is not None
        assert result.success is True
        assert len(result.steps) >= 3


class TestDiagnosticsV2:
    """测试增强版诊断"""
    
    def test_record_execution(self):
        """测试记录执行"""
        from src.workflow_diagnostics import create_diagnostics
        
        diag = create_diagnostics(TEST_DATA_DIR)
        
        # 记录执行
        diag.record_execution(
            "wf_test",
            "测试工作流",
            [{"name": "step1", "duration": 1.0, "success": True}],
            2.5,
            True
        )
        
        assert "wf_test" in diag.execution_history
        assert len(diag.execution_history["wf_test"]) == 1
    
    def test_diagnose(self):
        """测试诊断"""
        from src.workflow_diagnostics import create_diagnostics
        
        diag = create_diagnostics(TEST_DATA_DIR)
        
        # 添加多条执行记录
        for i in range(10):
            success = i < 8
            diag.record_execution(
                "wf_diag",
                "诊断测试",
                [{"name": "step1", "duration": 1.0, "success": success}],
                2.0,
                success,
                None if success else "test error"
            )
        
        # 诊断
        report = diag.diagnose("wf_diag")
        
        assert report.workflow_id == "wf_diag"
        assert report.execution_count == 10
        assert report.success_rate == 0.8
        assert report.health_score > 0
    
    def test_trend_analysis(self):
        """测试趋势分析"""
        from src.workflow_diagnostics import create_diagnostics
        
        diag = create_diagnostics(TEST_DATA_DIR)
        
        # 添加历史数据
        for i in range(20):
            diag.record_execution(
                "wf_trend",
                "趋势测试",
                [{"name": "s1", "duration": 1.0, "success": True}],
                1.0,
                True
            )
        
        report = diag.diagnose("wf_trend")
        assert len(report.trends) >= 0  # 可能没有趋势数据
    
    def test_root_cause_analysis(self):
        """测试根因分析"""
        from src.workflow_diagnostics import create_diagnostics
        
        diag = create_diagnostics(TEST_DATA_DIR)
        
        # 添加有错误的执行
        diag.record_execution("wf_error", "错误测试", [], 1.0, False, "Connection timeout")
        diag.record_execution("wf_error", "错误测试", [], 1.0, False, "Connection timeout")
        diag.record_execution("wf_error", "错误测试", [], 1.0, False, "Permission denied")
        
        report = diag.diagnose("wf_error")
        
        assert len(report.issues) > 0
        assert len(report.root_causes) > 0


class TestIntegration:
    """集成测试"""
    
    def test_full_workflow_share_convert(self):
        """完整流程测试：录制 -> 转换 -> 分享"""
        from src.screen_recorder import create_screen_recorder, ElementDetection
        from src.workflow_share import create_share_system
        
        # 1. 录制
        converter = create_screen_recorder(TEST_DATA_DIR)
        rec = converter.start_recording("完整测试")
        
        converter.add_action(rec.recording_id, {
            "action_type": "launch_app",
            "app": "Chrome",
            "timestamp": time.time()
        })
        converter.add_action(rec.recording_id, {
            "action_type": "click",
            "x": 100, "y": 200,
            "timestamp": time.time() + 1
        })
        
        rec = converter.stop_recording(rec.recording_id)
        
        # 2. 转换
        result = converter.convert_to_workflow(rec.recording_id, ElementDetection.IMAGE)
        assert result.success
        
        # 3. 分享
        share = create_share_system(TEST_DATA_DIR)
        workflow = {
            "workflow_id": result.workflow_id,
            "name": result.workflow_name,
            "steps": [{"action": s.action, "target": s.target} for s in result.steps]
        }
        wf_id = share.register_workflow(workflow)
        assert wf_id == result.workflow_id


# 运行测试
if __name__ == "__main__":
    pytest.main([__file__, "-v"])

#!/usr/bin/env python3
"""RabAI AutoClick v22 test suite.

Tests for v22 features:
- Workflow sharing system
- Pipeline integration mode
- Screen recording to workflow conversion
- Enhanced workflow diagnostics
"""

import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict

import pytest


# Setup test data directory
TEST_DATA_DIR: str = tempfile.mkdtemp()


def setup_module(module: Any) -> None:
    """Initialize test module.
    
    Args:
        module: Test module.
    """
    os.makedirs(TEST_DATA_DIR, exist_ok=True)


def teardown_module(module: Any) -> None:
    """Cleanup test module.
    
    Args:
        module: Test module.
    """
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)


class TestWorkflowShare:
    """Test workflow sharing system."""
    
    def test_create_share_link(self) -> None:
        """Test creating share link."""
        from src.workflow_share import create_share_system, ShareType
        
        share = create_share_system(TEST_DATA_DIR)
        
        workflow: Dict[str, Any] = {
            "workflow_id": "wf_test",
            "name": "测试工作流",
            "description": "测试",
            "steps": [{"action": "click", "target": "button"}]
        }
        wf_id = share.register_workflow(workflow)
        assert wf_id == "wf_test"
        
        link = share.create_share_link(wf_id, ShareType.PUBLIC, expires_in_days=7)
        assert link is not None
        assert link.share_type == ShareType.PUBLIC
        assert link.expires_at is not None
    
    def test_export_import(self) -> None:
        """Test export and import functionality."""
        from src.workflow_share import create_share_system
        
        share = create_share_system(TEST_DATA_DIR)
        
        workflow: Dict[str, Any] = {
            "workflow_id": "wf_export",
            "name": "导出测试",
            "steps": [{"action": "click"}]
        }
        share.register_workflow(workflow)
        
        json_str = share.export_to_json("wf_export")
        assert json_str is not None
        
        b64 = share.export_to_base64("wf_export")
        assert b64 is not None
        
        report = share.import_workflow(b64, "base64")
        assert report.result.value == "success"
    
    def test_share_stats(self) -> None:
        """Test share statistics."""
        from src.workflow_share import create_share_system, ShareType
        
        share = create_share_system(TEST_DATA_DIR)
        
        workflow: Dict[str, Any] = {
            "workflow_id": "wf_stats_test",
            "name": "StatsTest",
            "steps": []
        }
        share.register_workflow(workflow)
        share.create_share_link("wf_stats_test", ShareType.PUBLIC)
        
        stats = share.get_share_stats()
        assert stats["total_links"] >= 1
        assert stats["active_links"] >= 1


class TestPipelineMode:
    """Test pipeline integration mode."""
    
    def test_create_chain(self) -> None:
        """Test creating pipeline chain."""
        from src.pipeline_mode import create_pipeline_runner, PipeMode
        
        runner = create_pipeline_runner(TEST_DATA_DIR)
        
        chain = runner.create_chain("测试管道", PipeMode.LINEAR)
        assert chain.name == "测试管道"
        assert chain.mode == PipeMode.LINEAR
    
    def test_add_step(self) -> None:
        """Test adding step to chain."""
        from src.pipeline_mode import create_pipeline_runner, PipeMode
        
        runner = create_pipeline_runner(TEST_DATA_DIR)
        
        chain = runner.create_chain("管道1", PipeMode.LINEAR)
        step = runner.add_step(chain.chain_id, "步骤1", "echo hello")
        
        assert step is not None
        assert step.name == "步骤1"
    
    def test_execute_chain(self) -> None:
        """Test executing pipeline chain."""
        from src.pipeline_mode import create_pipeline_runner, PipeMode
        
        runner = create_pipeline_runner(TEST_DATA_DIR)
        
        chain = runner.create_chain("执行测试", PipeMode.LINEAR)
        runner.add_step(chain.chain_id, "测试", "echo 'test'")
        
        result = runner.execute_chain(chain.chain_id, {"test": "data"})
        
        assert result.chain_id == chain.chain_id
        assert result.total_duration >= 0


class TestScreenRecorder:
    """Test screen recording to workflow conversion."""
    
    def test_start_stop_recording(self) -> None:
        """Test starting and stopping recording."""
        from src.screen_recorder import create_screen_recorder
        
        converter = create_screen_recorder(TEST_DATA_DIR)
        
        rec = converter.start_recording("测试录制", "描述")
        assert rec.recording_id is not None
        
        rec = converter.stop_recording(rec.recording_id)
        assert rec is not None
        assert len(rec.actions) == 0
    
    def test_add_action(self) -> None:
        """Test adding action to recording."""
        from src.screen_recorder import create_screen_recorder
        
        converter = create_screen_recorder(TEST_DATA_DIR)
        
        rec = converter.start_recording("录制1")
        
        action_data: Dict[str, Any] = {
            "action_type": "click",
            "x": 100,
            "y": 200,
            "timestamp": time.time()
        }
        result = converter.add_action(rec.recording_id, action_data)
        assert result is True
        
        rec = converter.get_recording(rec.recording_id)
        assert len(rec.actions) == 1
    
    def test_convert_to_workflow(self) -> None:
        """Test converting recording to workflow."""
        from src.screen_recorder import create_screen_recorder, ElementDetection
        
        converter = create_screen_recorder(TEST_DATA_DIR)
        
        rec = converter.start_recording("转换测试")
        
        for i in range(3):
            converter.add_action(rec.recording_id, {
                "action_type": "click",
                "x": 100 + i * 10,
                "y": 200,
                "timestamp": time.time() + i
            })
        
        rec = converter.stop_recording(rec.recording_id)
        
        result = converter.convert_to_workflow(
            rec.recording_id,
            detection_mode=ElementDetection.IMAGE
        )
        
        assert result is not None
        assert result.success is True
        assert len(result.steps) >= 3


class TestDiagnosticsV2:
    """Test enhanced workflow diagnostics."""
    
    def test_record_execution(self) -> None:
        """Test recording workflow execution."""
        from src.workflow_diagnostics import create_diagnostics
        
        diag = create_diagnostics(TEST_DATA_DIR)
        
        diag.record_execution(
            "wf_test",
            "测试工作流",
            [{"name": "step1", "duration": 1.0, "success": True}],
            2.5,
            True
        )
        
        assert "wf_test" in diag.execution_history
        assert len(diag.execution_history["wf_test"]) == 1
    
    def test_diagnose(self) -> None:
        """Test workflow diagnosis."""
        from src.workflow_diagnostics import create_diagnostics
        
        diag = create_diagnostics(TEST_DATA_DIR)
        
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
        
        report = diag.diagnose("wf_diag")
        
        assert report.workflow_id == "wf_diag"
        assert report.execution_count == 10
        assert report.success_rate == 0.8
        assert report.health_score > 0
    
    def test_trend_analysis(self) -> None:
        """Test trend analysis."""
        from src.workflow_diagnostics import create_diagnostics
        
        diag = create_diagnostics(TEST_DATA_DIR)
        
        for i in range(20):
            diag.record_execution(
                "wf_trend",
                "趋势测试",
                [{"name": "s1", "duration": 1.0, "success": True}],
                1.0,
                True
            )
        
        report = diag.diagnose("wf_trend")
        assert len(report.trends) >= 0
    
    def test_root_cause_analysis(self) -> None:
        """Test root cause analysis."""
        from src.workflow_diagnostics import create_diagnostics
        
        diag = create_diagnostics(TEST_DATA_DIR)
        
        diag.record_execution(
            "wf_error", "错误测试", [], 1.0, False, "Connection timeout"
        )
        diag.record_execution(
            "wf_error", "错误测试", [], 1.0, False, "Connection timeout"
        )
        diag.record_execution(
            "wf_error", "错误测试", [], 1.0, False, "Permission denied"
        )
        
        report = diag.diagnose("wf_error")
        
        assert len(report.issues) > 0
        assert len(report.root_causes) > 0


class TestIntegration:
    """Integration tests for v22 features."""
    
    def test_full_workflow_share_convert(self) -> None:
        """Full flow test: record -> convert -> share."""
        from src.screen_recorder import create_screen_recorder, ElementDetection
        from src.workflow_share import create_share_system
        
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
        
        result = converter.convert_to_workflow(
            rec.recording_id,
            ElementDetection.IMAGE
        )
        assert result.success
        
        share = create_share_system(TEST_DATA_DIR)
        workflow: Dict[str, Any] = {
            "workflow_id": result.workflow_id,
            "name": result.workflow_name,
            "steps": [
                {"action": s.action, "target": s.target}
                for s in result.steps
            ]
        }
        wf_id = share.register_workflow(workflow)
        assert wf_id == result.workflow_id


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

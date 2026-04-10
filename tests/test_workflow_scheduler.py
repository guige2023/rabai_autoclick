"""
工作流调度器测试
"""
import unittest
import time
import tempfile
import shutil
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.workflow_scheduler import (
    WorkflowScheduler,
    ScheduleType,
    ScheduleState,
    ConditionType,
    ScheduleCondition,
    ScheduledWorkflow,
    ExecutionRecord,
    create_scheduler,
    notification_handler,
    send_desktop_notification
)


class TestScheduleCondition(unittest.TestCase):
    """测试调度条件"""
    
    def test_file_exists_condition(self):
        """测试文件存在条件"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name
        
        try:
            cond = ScheduleCondition(ConditionType.FILE_EXISTS, {"path": temp_path})
            self.assertTrue(cond.check({}))
            
            cond = ScheduleCondition(ConditionType.FILE_EXISTS, {"path": "/nonexistent/file.txt"})
            self.assertFalse(cond.check({}))
        finally:
            os.unlink(temp_path)
    
    def test_file_not_exists_condition(self):
        """测试文件不存在条件"""
        cond = ScheduleCondition(ConditionType.FILE_NOT_EXISTS, {"path": "/nonexistent/file.txt"})
        self.assertTrue(cond.check({}))
        
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name
        
        try:
            cond = ScheduleCondition(ConditionType.FILE_NOT_EXISTS, {"path": temp_path})
            self.assertFalse(cond.check({}))
        finally:
            os.unlink(temp_path)
    
    def test_variable_equals_condition(self):
        """测试变量相等条件"""
        cond = ScheduleCondition(ConditionType.VARIABLE_EQUALS, {"name": "mode", "value": "production"})
        
        self.assertTrue(cond.check({"mode": "production"}))
        self.assertFalse(cond.check({"mode": "development"}))
        self.assertFalse(cond.check({}))
    
    def test_variable_not_equals_condition(self):
        """测试变量不相等条件"""
        cond = ScheduleCondition(ConditionType.VARIABLE_NOT_EQUALS, {"name": "mode", "value": "production"})
        
        self.assertFalse(cond.check({"mode": "production"}))
        self.assertTrue(cond.check({"mode": "development"}))
        self.assertTrue(cond.check({}))
    
    def test_time_between_condition(self):
        """测试时间段条件"""
        cond = ScheduleCondition(ConditionType.TIME_BETWEEN, {"start": "00:00", "end": "23:59"})
        self.assertTrue(cond.check({}))
        
        # 测试无效时间（end < start）
        cond = ScheduleCondition(ConditionType.TIME_BETWEEN, {"start": "23:00", "end": "00:00"})
        # 这种情况下可能返回 False（取决于当前时间）
    
    def test_day_of_week_condition(self):
        """测试星期条件"""
        cond = ScheduleCondition(ConditionType.DAY_OF_WEEK, {"days": [0, 1, 2, 3, 4]})  # 工作日
        today = datetime.now().weekday()
        expected = today < 5
        self.assertEqual(cond.check({}), expected)
    
    def test_custom_condition(self):
        """测试自定义条件"""
        def custom_check(context):
            return context.get("value", 0) > 10
        
        cond = ScheduleCondition(ConditionType.CUSTOM, {"func": custom_check})
        self.assertTrue(cond.check({"value": 20}))
        self.assertFalse(cond.check({"value": 5}))


class TestScheduledWorkflow(unittest.TestCase):
    """测试调度工作流"""
    
    def test_create_scheduled_workflow(self):
        """测试创建调度工作流"""
        workflow = ScheduledWorkflow(
            schedule_id="test_001",
            workflow_id="wf_001",
            workflow_name="测试工作流",
            workflow_data={"steps": []},
            schedule_type=ScheduleType.CRON,
            schedule_config={"expression": "0 9 * * *"},
            priority=5
        )
        
        self.assertEqual(workflow.schedule_id, "test_001")
        self.assertEqual(workflow.state, ScheduleState.PENDING)
        self.assertTrue(workflow.enabled)
        self.assertEqual(workflow.retry_count, 0)


class TestExecutionRecord(unittest.TestCase):
    """测试执行记录"""
    
    def test_create_execution_record(self):
        """测试创建执行记录"""
        now = time.time()
        record = ExecutionRecord(
            record_id="rec_001",
            schedule_id="sched_001",
            workflow_id="wf_001",
            scheduled_time=now,
            actual_start_time=now + 1,
            actual_end_time=now + 10,
            state=ScheduleState.COMPLETED,
            retry_count=0,
            error=None,
            conditions_checked={"file_exists": True},
            conditions_skipped=False
        )
        
        self.assertEqual(record.state, ScheduleState.COMPLETED)
        self.assertIsNone(record.error)


class TestWorkflowScheduler(unittest.TestCase):
    """测试工作流调度器"""
    
    def setUp(self):
        """设置测试环境"""
        self.temp_dir = tempfile.mkdtemp()
        self.scheduler = create_scheduler(self.temp_dir)
    
    def tearDown(self):
        """清理测试环境"""
        self.scheduler.stop()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_create_scheduler(self):
        """测试创建调度器"""
        self.assertIsNotNone(self.scheduler)
        self.assertEqual(len(self.scheduler.schedules), 0)
        self.assertEqual(len(self.scheduler._execution_queue), 0)
    
    def test_add_cron_schedule(self):
        """测试添加 Cron 调度"""
        schedule_id = self.scheduler.add_schedule(
            workflow_id="wf_001",
            workflow_name="晨间报告",
            workflow_data={"steps": []},
            schedule_type=ScheduleType.CRON,
            schedule_config={"expression": "0 9 * * *"},
            priority=3
        )
        
        self.assertIsNotNone(schedule_id)
        self.assertIn(schedule_id, self.scheduler.schedules)
        self.assertEqual(len(self.scheduler._execution_queue), 1)
    
    def test_add_interval_schedule(self):
        """测试添加间隔调度"""
        schedule_id = self.scheduler.add_schedule(
            workflow_id="wf_002",
            workflow_name="健康检查",
            workflow_data={"steps": []},
            schedule_type=ScheduleType.INTERVAL,
            schedule_config={"minutes": 30},
            priority=5
        )
        
        self.assertIsNotNone(schedule_id)
        schedule = self.scheduler.schedules[schedule_id]
        self.assertEqual(schedule.schedule_type, ScheduleType.INTERVAL)
        self.assertIsNotNone(schedule.next_run_time)
    
    def test_add_one_time_schedule(self):
        """测试添加一次性调度"""
        future_time = datetime.now() + timedelta(minutes=10)
        schedule_id = self.scheduler.add_schedule(
            workflow_id="wf_003",
            workflow_name="临时任务",
            workflow_data={"steps": []},
            schedule_type=ScheduleType.ONE_TIME,
            schedule_config={"run_time": future_time.strftime("%Y-%m-%d %H:%M:%S")},
            priority=1
        )
        
        self.assertIsNotNone(schedule_id)
        schedule = self.scheduler.schedules[schedule_id]
        self.assertEqual(schedule.schedule_type, ScheduleType.ONE_TIME)
    
    def test_add_calendar_schedule(self):
        """测试添加日历调度"""
        future_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        schedule_id = self.scheduler.add_schedule(
            workflow_id="wf_004",
            workflow_name="日历任务",
            workflow_data={"steps": []},
            schedule_type=ScheduleType.CALENDAR,
            schedule_config={"dates": [future_date]},
            priority=2
        )
        
        self.assertIsNotNone(schedule_id)
    
    def test_remove_schedule(self):
        """测试移除调度"""
        schedule_id = self.scheduler.add_schedule(
            workflow_id="wf_001",
            workflow_name="测试",
            workflow_data={"steps": []},
            schedule_type=ScheduleType.INTERVAL,
            schedule_config={"minutes": 10}
        )
        
        self.assertIn(schedule_id, self.scheduler.schedules)
        
        result = self.scheduler.remove_schedule(schedule_id)
        self.assertTrue(result)
        self.assertNotIn(schedule_id, self.scheduler.schedules)
    
    def test_enable_disable_schedule(self):
        """测试启用/禁用调度"""
        schedule_id = self.scheduler.add_schedule(
            workflow_id="wf_001",
            workflow_name="测试",
            workflow_data={"steps": []},
            schedule_type=ScheduleType.INTERVAL,
            schedule_config={"minutes": 10}
        )
        
        # 禁用
        result = self.scheduler.enable_schedule(schedule_id, False)
        self.assertTrue(result)
        self.assertFalse(self.scheduler.schedules[schedule_id].enabled)
        
        # 启用
        result = self.scheduler.enable_schedule(schedule_id, True)
        self.assertTrue(result)
        self.assertTrue(self.scheduler.schedules[schedule_id].enabled)
    
    def test_update_condition_context(self):
        """测试更新条件上下文"""
        self.scheduler.update_condition_context("mode", "production")
        self.assertEqual(self.scheduler._condition_context["mode"], "production")
        
        self.scheduler.update_condition_context("count", 42)
        self.assertEqual(self.scheduler._condition_context["count"], 42)
    
    def test_check_conditions(self):
        """测试条件检查"""
        schedule = ScheduledWorkflow(
            schedule_id="test",
            workflow_id="wf",
            workflow_name="测试",
            workflow_data={},
            schedule_type=ScheduleType.CRON,
            schedule_config={"expression": "0 9 * * *"},
            conditions=[
                ScheduleCondition(ConditionType.VARIABLE_EQUALS, {"name": "mode", "value": "production"})
            ]
        )
        
        # 不满足条件
        passed, results = self.scheduler.check_conditions(schedule)
        self.assertFalse(passed)
        
        # 满足条件
        self.scheduler.update_condition_context("mode", "production")
        passed, results = self.scheduler.check_conditions(schedule)
        self.assertTrue(passed)
    
    def test_calendar_view(self):
        """测试日历视图"""
        # 添加一个未来 3 天的日历调度
        for i in range(1, 4):
            future_date = (datetime.now() + timedelta(days=i)).strftime("%Y-%m-%d")
            self.scheduler.add_schedule(
                workflow_id=f"wf_{i}",
                workflow_name=f"任务 {i}",
                workflow_data={"steps": []},
                schedule_type=ScheduleType.CALENDAR,
                schedule_config={"dates": [future_date]},
                priority=5
            )
        
        start = datetime.now()
        end = start + timedelta(days=7)
        calendar = self.scheduler.get_calendar_view(start, end)
        
        # 应该有未来 3 天的数据
        self.assertGreater(len(calendar), 0)
    
    def test_execution_history(self):
        """测试执行历史"""
        # 添加调度
        schedule_id = self.scheduler.add_schedule(
            workflow_id="wf_001",
            workflow_name="测试",
            workflow_data={"steps": []},
            schedule_type=ScheduleType.ONE_TIME,
            schedule_config={"run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        )
        
        # 添加一条执行记录
        now = time.time()
        record = ExecutionRecord(
            record_id="rec_001",
            schedule_id=schedule_id,
            workflow_id="wf_001",
            scheduled_time=now,
            actual_start_time=now,
            actual_end_time=now + 10,
            state=ScheduleState.COMPLETED,
            retry_count=0,
            error=None,
            conditions_checked={},
            conditions_skipped=False
        )
        self.scheduler.execution_history.append(record)
        
        # 获取历史
        history = self.scheduler.get_execution_history()
        self.assertEqual(len(history), 1)
        
        # 按 schedule_id 过滤
        history = self.scheduler.get_execution_history(schedule_id=schedule_id)
        self.assertEqual(len(history), 1)
        
        # 按 workflow_id 过滤
        history = self.scheduler.get_execution_history(workflow_id="wf_002")
        self.assertEqual(len(history), 0)
    
    def test_list_schedules(self):
        """测试列出调度"""
        # 添加多个调度
        for i in range(3):
            self.scheduler.add_schedule(
                workflow_id=f"wf_{i}",
                workflow_name=f"任务 {i}",
                workflow_data={"steps": []},
                schedule_type=ScheduleType.INTERVAL,
                schedule_config={"minutes": 10 + i},
                priority=5 - i
            )
        
        # 列出所有
        schedules = self.scheduler.list_schedules()
        self.assertEqual(len(schedules), 3)
        
        # 按 workflow_id 过滤
        schedules = self.scheduler.list_schedules(workflow_id="wf_0")
        self.assertEqual(len(schedules), 1)
        
        # 按类型过滤
        schedules = self.scheduler.list_schedules(schedule_type=ScheduleType.CRON)
        self.assertEqual(len(schedules), 0)
        
        # 只显示启用的
        self.scheduler.enable_schedule(list(self.scheduler.schedules.keys())[0], False)
        schedules = self.scheduler.list_schedules(enabled_only=True)
        self.assertEqual(len(schedules), 2)
    
    def test_get_schedule_status(self):
        """测试获取调度器状态"""
        # 添加一些调度
        for i in range(3):
            self.scheduler.add_schedule(
                workflow_id=f"wf_{i}",
                workflow_name=f"任务 {i}",
                workflow_data={"steps": []},
                schedule_type=ScheduleType.INTERVAL,
                schedule_config={"minutes": 10},
                priority=5
            )
        
        status = self.scheduler.get_schedule_status()
        self.assertEqual(status["total_schedules"], 3)
        self.assertEqual(status["enabled_schedules"], 3)
        self.assertFalse(status["is_running"])  # 还未启动
    
    def test_start_stop(self):
        """测试启动/停止调度器"""
        self.scheduler.start()
        self.assertTrue(self.scheduler._running)
        
        self.scheduler.stop()
        self.assertFalse(self.scheduler._running)
    
    def test_handle_missed_executions(self):
        """测试处理错过的执行"""
        # 添加一个间隔调度，立即设置其下次执行时间为过去
        schedule_id = self.scheduler.add_schedule(
            workflow_id="wf_001",
            workflow_name="测试",
            workflow_data={"steps": []},
            schedule_type=ScheduleType.INTERVAL,
            schedule_config={"minutes": 30},
            priority=5
        )
        
        # 将下次执行时间设置为过去
        self.scheduler.schedules[schedule_id].next_run_time = time.time() - 3600
        
        # 处理错过的执行
        missed = self.scheduler.handle_missed_executions(lookback_minutes=120)
        self.assertIn(schedule_id, missed)
    
    def test_conflict_detection(self):
        """测试冲突检测"""
        schedule_id = self.scheduler.add_schedule(
            workflow_id="wf_001",
            workflow_name="测试",
            workflow_data={"steps": []},
            schedule_type=ScheduleType.INTERVAL,
            schedule_config={"minutes": 1},
            priority=5
        )
        
        # 标记为运行中
        self.scheduler._running_workflows["wf_001"] = schedule_id
        
        # 尝试再次执行应该被阻止
        schedule = self.scheduler.schedules[schedule_id]
        success, error = self.scheduler._execute_workflow(schedule)
        self.assertFalse(success)
        self.assertIn("already running", error)
    
    def test_notification_handler(self):
        """测试通知处理器"""
        schedule = ScheduledWorkflow(
            schedule_id="test",
            workflow_id="wf",
            workflow_name="测试工作流",
            workflow_data={},
            schedule_type=ScheduleType.CRON,
            schedule_config={}
        )
        
        record = ExecutionRecord(
            record_id="rec",
            schedule_id="test",
            workflow_id="wf",
            scheduled_time=time.time(),
            actual_start_time=time.time(),
            actual_end_time=time.time() + 10,
            state=ScheduleState.COMPLETED,
            retry_count=0,
            error=None,
            conditions_checked={},
            conditions_skipped=False
        )
        
        # 测试通知处理（使用 mock）
        with patch('src.workflow_scheduler.send_desktop_notification') as mock_notify:
            notification_handler(schedule, record)
            mock_notify.assert_called_once()
            call_args = mock_notify.call_args[0]
            self.assertIn("完成", call_args[0])
    
    def test_persistence(self):
        """测试数据持久化"""
        # 添加调度
        schedule_id = self.scheduler.add_schedule(
            workflow_id="wf_001",
            workflow_name="持久化测试",
            workflow_data={"steps": []},
            schedule_type=ScheduleType.INTERVAL,
            schedule_config={"minutes": 15},
            priority=5
        )
        
        # 创建新的调度器实例
        scheduler2 = create_scheduler(self.temp_dir)
        
        # 应该加载之前的数据
        self.assertEqual(len(scheduler2.schedules), 1)
        self.assertIn(schedule_id, scheduler2.schedules)
        self.assertEqual(scheduler2.schedules[schedule_id].workflow_name, "持久化测试")


class TestSendDesktopNotification(unittest.TestCase):
    """测试桌面通知"""
    
    @patch('subprocess.run')
    def test_send_notification_mac(self, mock_run):
        """测试 macOS 通知"""
        with patch('platform.system', return_value='Darwin'):
            send_desktop_notification("Test", "Message")
            mock_run.assert_called_once()
    
    @patch('subprocess.run')
    def test_send_notification_linux(self, mock_run):
        """测试 Linux 通知"""
        with patch('platform.system', return_value='Linux'):
            send_desktop_notification("Test", "Message")
            mock_run.assert_called_once()
    
    @patch('subprocess.run')
    def test_send_notification_windows(self, mock_run):
        """测试 Windows 通知"""
        with patch('platform.system', return_value='Windows'):
            send_desktop_notification("Test", "Message")
            mock_run.assert_called_once()


if __name__ == "__main__":
    unittest.main()

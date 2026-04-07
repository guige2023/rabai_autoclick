#!/usr/bin/env python3
"""
RabAI AutoClick v22 GUI
图形用户界面 - 完全调用 CLI 所有功能

This module provides a Tkinter-based graphical interface for the RabAI AutoClick
automation tool, featuring predictive engine, self-healing, scene management,
diagnostics, workflow sharing, pipeline integration, and screen recording.

Author: RabAI Team
Version: 22.0.0
"""

from __future__ import annotations

import sys
import os
import json
import time
import datetime
import threading
import subprocess
from pathlib import Path
from typing import Optional, Callable, Any, Dict, List, Union, TypeVar, TextIO

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.predictive_engine import create_predictive_engine
from src.self_healing_system import create_self_healing_system
from src.workflow_package import create_scene_manager
from src.workflow_diagnostics import create_diagnostics
from src.workflow_share import create_share_system, ShareType
from src.pipeline_mode import create_pipeline_runner, PipeMode
from src.screen_recorder import create_screen_recorder, ElementDetection

DATA_DIR = Path(__file__).parent.parent / "data"


class OutputRedirector:
    """
    Redirects stdout/stderr to a Tkinter ScrolledText widget.
    
    This class captures write operations and displays them in the GUI's
    log text widget, providing real-time output visibility.
    
    Attributes:
        text_widget: The Tkinter ScrolledText widget to write to.
        buffer: Internal buffer for incomplete writes.
    """
    
    def __init__(self, text_widget: Any) -> None:
        """
        Initialize the redirector with a target text widget.
        
        Args:
            text_widget: A Tkinter ScrolledText or Text widget instance.
        """
        self.text_widget = text_widget
        self.buffer: str = ""

    def write(self, string: str) -> None:
        """
        Write a string to the text widget.
        
        Args:
            string: The text content to display.
        """
        self.text_widget.configure(state='normal')
        self.text_widget.insert(tk.END, string)
        self.text_widget.see(tk.END)
        self.text_widget.configure(state='disabled')

    def flush(self) -> None:
        """Flush the internal buffer (no-op for GUI redirector)."""
        pass


class BaseTab(ttk.Frame):
    """
    Base class for all GUI tabs in RabAI AutoClick.
    
    Provides common functionality for async task execution,
    logging, and UI setup patterns.
    
    Attributes:
        app: Reference to the main RabAIGUI application instance.
    """
    
    def __init__(self, parent: Any, app: RabAIGUI) -> None:
        """
        Initialize the base tab.
        
        Args:
            parent: The parent Tkinter widget (typically a ttk.Notebook).
            app: Reference to the main RabAIGUI application.
        """
        super().__init__(parent)
        self.app = app
        self.setup_ui()

    def setup_ui(self) -> None:
        """Set up the tab's UI components. Override in subclasses."""
        pass

    def log(self, message: str) -> None:
        """
        Log a message to the main application's log area.
        
        Args:
            message: The message string to log.
        """
        self.app.log(message)

    def run_async(self, func: Callable[[], Any], callback: Optional[Callable[[Any], None]] = None) -> None:
        """
        Execute a function in a background thread safely.
        
        This method runs the given function in a daemon thread and,
        upon completion, invokes the optional callback in the main
        GUI thread using after_idle.
        
        Args:
            func: A callable to execute in the background.
            callback: Optional callback function to receive the result.
        """
        def wrapper() -> None:
            try:
                result = func()
                if callback:
                    self.after_idle(lambda: callback(result))
            except Exception as e:
                import traceback
                error_msg = f"❌ 错误: {str(e)}\n{traceback.format_exc()}"
                self.after_idle(lambda: self.log(error_msg))
        thread = threading.Thread(target=wrapper, daemon=True)
        thread.start()


class PredictTab(BaseTab):
    """
    Predictive engine tab for action prediction and behavior analysis.
    
    Features:
        - Record user actions for learning
        - Predict next actions based on patterns
        - Analyze user behavior statistics
    """
    
    def setup_ui(self) -> None:
        """Set up the tab's UI components."""
        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        record_frame = ttk.Frame(notebook)
        notebook.add(record_frame, text="记录动作")
        self._setup_record_tab(record_frame)

        predict_frame = ttk.Frame(notebook)
        notebook.add(predict_frame, text="预测动作")
        self._setup_predict_tab(predict_frame)

        analyze_frame = ttk.Frame(notebook)
        notebook.add(analyze_frame, text="行为分析")
        self._setup_analyze_tab(analyze_frame)

    def _setup_record_tab(self, parent: Any) -> None:
        """
        Set up the action recording subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="记录用户动作", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="动作类型:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.action_type_var = tk.StringVar(value="click")
        action_combo = ttk.Combobox(frame, textvariable=self.action_type_var, 
                                     values=["click", "type", "hotkey", "wait", "launch_app"], width=30)
        action_combo.grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="目标:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.target_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.target_var, width=35).grid(row=1, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="上下文 (JSON):").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.context_var = tk.StringVar(value="{}")
        ttk.Entry(frame, textvariable=self.context_var, width=35).grid(row=2, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="执行结果:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.result_var = tk.StringVar(value="success")
        ttk.Combobox(frame, textvariable=self.result_var, values=["success", "failure"], width=30).grid(row=3, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="耗时 (秒):").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.duration_var = tk.StringVar(value="0.0")
        ttk.Entry(frame, textvariable=self.duration_var, width=35).grid(row=4, column=1, sticky=tk.W, pady=5)

        ttk.Button(frame, text="✓ 记录动作", command=self._record_action).grid(row=5, column=0, columnspan=2, pady=15)

    def _setup_predict_tab(self, parent: Any) -> None:
        """
        Set up the prediction subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="预测下一个动作", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="当前活动应用 (可选):").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.app_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.app_var, width=35).grid(row=0, column=1, sticky=tk.W, pady=5)

        btn_frame = ttk.Frame(frame)
        btn_frame.grid(row=1, column=0, columnspan=2, pady=10)
        ttk.Button(btn_frame, text="🔮 预测下一个动作", command=self._predict_next).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="💡 获取工作流建议", command=self._get_suggestion).pack(side=tk.LEFT, padx=5)

        self.predict_result = scrolledtext.ScrolledText(frame, height=10, state='disabled')
        self.predict_result.grid(row=2, column=0, columnspan=2, sticky=tk.EW, pady=10)

    def _setup_analyze_tab(self, parent: Any) -> None:
        """
        Set up the behavior analysis subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="用户行为分析", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Button(frame, text="📊 分析用户行为", command=self._analyze_behavior).pack(pady=10)

        self.analyze_result = scrolledtext.ScrolledText(frame, height=15, state='disabled')
        self.analyze_result.pack(fill=tk.BOTH, expand=True, pady=10)

    def _record_action(self) -> None:
        """
        Record a user action to the predictive engine.
        
        Validates JSON context input and numeric duration before recording.
        """
        def do_record() -> str:
            engine = create_predictive_engine(str(DATA_DIR))
            ctx = {}
            context_str = self.context_var.get()
            if context_str and context_str != "{}":
                try:
                    ctx = json.loads(context_str)
                except json.JSONDecodeError as e:
                    return f"❌ JSON格式错误: {str(e)}"
            
            duration_str = self.duration_var.get()
            try:
                duration = float(duration_str) if duration_str else 0.0
            except ValueError:
                return f"❌ 耗时必须是数字: {duration_str}"
            
            engine.record_action(
                self.action_type_var.get(),
                self.target_var.get(),
                ctx,
                self.result_var.get(),
                duration
            )
            return f"✓ 已记录动作: {self.action_type_var.get()} -> {self.target_var.get()}"

        self.run_async(do_record, lambda r: self.log(r))

    def _predict_next(self) -> None:
        """
        Predict the next user action based on current context.
        
        Retrieves prediction from the predictive engine and displays
        the results including confidence score and alternatives.
        """
        def do_predict():
            engine = create_predictive_engine(str(DATA_DIR))
            ctx = {"active_app": self.app_var.get()} if self.app_var.get() else {}
            return engine.predict_next_action(ctx)

        def show_result(prediction):
            self.predict_result.configure(state='normal')
            self.predict_result.delete(1.0, tk.END)
            if prediction:
                self.predict_result.insert(tk.END, f"🔮 预测动作: {prediction.predicted_action}\n")
                self.predict_result.insert(tk.END, f"   置信度: {prediction.confidence * 100:.1f}%\n")
                self.predict_result.insert(tk.END, f"   推理: {prediction.reasoning}\n")
                if prediction.alternatives:
                    self.predict_result.insert(tk.END, f"   备选: {', '.join(prediction.alternatives)}\n")
            else:
                self.predict_result.insert(tk.END, "暂无预测数据，请先记录更多动作")
            self.predict_result.configure(state='disabled')

        self.run_async(do_predict, show_result)

    def _get_suggestion(self) -> None:
        """
        Get workflow creation suggestions based on recorded behavior.
        """
        def do_suggest():
            engine = create_predictive_engine(str(DATA_DIR))
            return engine.suggest_workflow_creation()

        def show_result(suggestion):
            self.predict_result.configure(state='normal')
            self.predict_result.delete(1.0, tk.END)
            if suggestion:
                self.predict_result.insert(tk.END, f"💡 {suggestion}")
            else:
                self.predict_result.insert(tk.END, "暂无建议")
            self.predict_result.configure(state='disabled')

        self.run_async(do_suggest, show_result)

    def _analyze_behavior(self) -> None:
        """
        Analyze and display user behavior statistics.
        """
        def do_analyze():
            engine = create_predictive_engine(str(DATA_DIR))
            return engine.analyze_user_behavior()

        def show_result(analysis):
            self.analyze_result.configure(state='normal')
            self.analyze_result.delete(1.0, tk.END)
            self.analyze_result.insert(tk.END, "📊 用户行为分析\n\n")
            self.analyze_result.insert(tk.END, f"  总动作数: {analysis.get('total_actions', 0)}\n")
            self.analyze_result.insert(tk.END, f"  成功率: {analysis.get('success_rate', 0)*100:.1f}%\n")
            
            if "top_targets" in analysis:
                self.analyze_result.insert(tk.END, "\n  常用操作 TOP5:\n")
                for i, (target, count) in enumerate(list(analysis["top_targets"].items())[:5], 1):
                    self.analyze_result.insert(tk.END, f"    {i}. {target}: {count}次\n")
            self.analyze_result.configure(state='disabled')

        self.run_async(do_analyze, show_result)


class HealTab(BaseTab):
    """
    Self-healing system tab for error analysis and fix suggestions.
    
    Features:
        - Analyze errors and provide fix suggestions
        - View error statistics and recovery rates
    """
    
    def setup_ui(self) -> None:
        """Set up the tab's UI components."""
        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        fix_frame = ttk.Frame(notebook)
        notebook.add(fix_frame, text="错误修复")
        self._setup_fix_tab(fix_frame)

        stats_frame = ttk.Frame(notebook)
        notebook.add(stats_frame, text="错误统计")
        self._setup_stats_tab(stats_frame)

    def _setup_fix_tab(self, parent: Any) -> None:
        """
        Set up the error fixing subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="错误分析与修复建议", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="工作流名称:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.workflow_name_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.workflow_name_var, width=35).grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="步骤名称:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.step_name_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.step_name_var, width=35).grid(row=1, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="错误信息:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.error_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.error_var, width=35).grid(row=2, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="步骤索引:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.step_index_var = tk.StringVar(value="0")
        ttk.Entry(frame, textvariable=self.step_index_var, width=35).grid(row=3, column=1, sticky=tk.W, pady=5)

        ttk.Button(frame, text="🔧 分析错误并获取修复建议", command=self._analyze_error).grid(row=4, column=0, columnspan=2, pady=15)

        self.fix_result = scrolledtext.ScrolledText(frame, height=12, state='disabled')
        self.fix_result.grid(row=5, column=0, columnspan=2, sticky=tk.EW, pady=10)

    def _setup_stats_tab(self, parent: Any) -> None:
        """
        Set up the error statistics subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="错误统计", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Button(frame, text="📊 获取错误统计", command=self._get_stats).pack(pady=10)

        self.stats_result = scrolledtext.ScrolledText(frame, height=15, state='disabled')
        self.stats_result.pack(fill=tk.BOTH, expand=True, pady=10)

    def _analyze_error(self) -> None:
        """
        Analyze error and get fix suggestions.
        
        Validates step index is a valid integer before analysis.
        """
        def do_analyze():
            system = create_self_healing_system(str(DATA_DIR))
            
            class MockError(Exception):
                pass
            
            step_index_str = self.step_index_var.get()
            try:
                step_index = int(step_index_str) if step_index_str else 0
            except ValueError:
                return None, ["❌ 步骤索引必须是整数"]
            
            err = MockError(self.error_var.get())
            record = system.analyze_error(
                err, 
                self.workflow_name_var.get(), 
                self.step_name_var.get(), 
                step_index, 
                {}
            )
            suggestions = system.get_fix_suggestions(record)
            return record, suggestions

        def show_result(result):
            record, suggestions = result
            self.fix_result.configure(state='normal')
            self.fix_result.delete(1.0, tk.END)
            self.fix_result.insert(tk.END, f"🔧 错误分析: {self.error_var.get()}\n")
            self.fix_result.insert(tk.END, f"   类型: {record.error_type.value}\n\n")
            
            if suggestions:
                self.fix_result.insert(tk.END, f"💡 修复建议 ({len(suggestions)}条):\n")
                for i, s in enumerate(suggestions, 1):
                    self.fix_result.insert(tk.END, f"  {i}. [{s.strategy.value}] {s.description}\n")
                    self.fix_result.insert(tk.END, f"     实现: {s.implementation}\n")
            else:
                self.fix_result.insert(tk.END, "暂无建议")
            self.fix_result.configure(state='disabled')

        self.run_async(do_analyze, show_result)

    def _get_stats(self) -> None:
        """
        Retrieve and display error statistics.
        """
        def do_stats():
            system = create_self_healing_system(str(DATA_DIR))
            return system.get_error_statistics()

        def show_result(stats):
            self.stats_result.configure(state='normal')
            self.stats_result.delete(1.0, tk.END)
            self.stats_result.insert(tk.END, "📊 错误统计\n\n")
            self.stats_result.insert(tk.END, f"  总错误数: {stats.get('total_errors', 0)}\n")
            self.stats_result.insert(tk.END, f"  恢复成功率: {stats.get('recovery_rate', 0)*100:.1f}%\n")
            
            if "error_type_distribution" in stats:
                self.stats_result.insert(tk.END, "\n  错误类型分布:\n")
                for etype, count in stats["error_type_distribution"].items():
                    self.stats_result.insert(tk.END, f"    - {etype}: {count}\n")
            self.stats_result.configure(state='disabled')

        self.run_async(do_stats, show_result)


class SceneTab(BaseTab):
    """
    Scene management tab for organizing workflows into logical groups.
    
    Features:
        - List and filter scenes by tags
        - Create new scenes
        - View scene statistics
        - Activate/deactivate scenes
    """
    
    def setup_ui(self) -> None:
        """Set up the tab's UI components."""
        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        list_frame = ttk.Frame(notebook)
        notebook.add(list_frame, text="场景列表")
        self._setup_list_tab(list_frame)

        create_frame = ttk.Frame(notebook)
        notebook.add(create_frame, text="创建场景")
        self._setup_create_tab(create_frame)

        stats_frame = ttk.Frame(notebook)
        notebook.add(stats_frame, text="场景统计")
        self._setup_stats_tab(stats_frame)

    def _setup_list_tab(self, parent: Any) -> None:
        """
        Set up the scene list subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="场景列表", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        filter_frame = ttk.Frame(frame)
        filter_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(filter_frame, text="按标签筛选:").pack(side=tk.LEFT)
        self.tag_filter_var = tk.StringVar()
        ttk.Entry(filter_frame, textvariable=self.tag_filter_var, width=20).pack(side=tk.LEFT, padx=5)
        ttk.Button(filter_frame, text="🔍 查询", command=self._list_scenes).pack(side=tk.LEFT, padx=5)

        self.scene_tree = ttk.Treeview(frame, columns=("id", "name", "status", "workflows", "usage"), show="headings", height=10)
        self.scene_tree.heading("id", text="ID")
        self.scene_tree.heading("name", text="名称")
        self.scene_tree.heading("status", text="状态")
        self.scene_tree.heading("workflows", text="工作流数")
        self.scene_tree.heading("usage", text="使用次数")
        self.scene_tree.column("id", width=100)
        self.scene_tree.column("name", width=150)
        self.scene_tree.column("status", width=80)
        self.scene_tree.column("workflows", width=80)
        self.scene_tree.column("usage", width=80)
        self.scene_tree.pack(fill=tk.BOTH, expand=True, pady=10)

        ttk.Button(frame, text="▶ 激活选中场景", command=self._activate_scene).pack(pady=5)

    def _setup_create_tab(self, parent: Any) -> None:
        """
        Set up the scene creation subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="创建新场景", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="场景名称:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.scene_name_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.scene_name_var, width=35).grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="描述:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.scene_desc_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.scene_desc_var, width=35).grid(row=1, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="图标:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.scene_icon_var = tk.StringVar(value="📦")
        ttk.Combobox(frame, textvariable=self.scene_icon_var, 
                     values=["📦", "🏠", "💼", "🎮", "📚", "🎵", "📧", "🔧"], width=30).grid(row=2, column=1, sticky=tk.W, pady=5)

        ttk.Button(frame, text="✅ 创建场景", command=self._create_scene).grid(row=3, column=0, columnspan=2, pady=15)

    def _setup_stats_tab(self, parent: Any) -> None:
        """
        Set up the scene statistics subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="场景统计", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Button(frame, text="📊 获取场景统计", command=self._get_stats).pack(pady=10)

        self.scene_stats_result = scrolledtext.ScrolledText(frame, height=10, state='disabled')
        self.scene_stats_result.pack(fill=tk.BOTH, expand=True, pady=10)

    def _list_scenes(self) -> None:
        """
        List scenes, optionally filtered by tag.
        """
        def do_list():
            manager = create_scene_manager(str(DATA_DIR))
            tags = [self.tag_filter_var.get()] if self.tag_filter_var.get() else None
            return manager.list_scenes(tags=tags)

        def show_result(scenes):
            for item in self.scene_tree.get_children():
                self.scene_tree.delete(item)
            
            for s in scenes:
                status = "✅ 激活" if s.status.value == "active" else "⏸️ 暂停"
                self.scene_tree.insert("", tk.END, values=(s.scene_id, s.name, status, len(s.workflows), s.usage_count))

        self.run_async(do_list, show_result)

    def _activate_scene(self) -> None:
        """
        Activate the selected scene in the scene tree.
        """
        selected = self.scene_tree.selection()
        if not selected:
            messagebox.showwarning("提示", "请先选择一个场景")
            return
        
        scene_id = self.scene_tree.item(selected[0])["values"][0]

        def do_activate():
            manager = create_scene_manager(str(DATA_DIR))
            return manager.activate_scene(scene_id)

        def show_result(success):
            if success:
                self.log(f"✅ 已激活场景: {scene_id}")
                self._list_scenes()
            else:
                self.log(f"❌ 场景不存在: {scene_id}")

        self.run_async(do_activate, show_result)

    def _create_scene(self) -> None:
        """
        Create a new scene with the specified name, description, and icon.
        """
        def do_create():
            manager = create_scene_manager(str(DATA_DIR))
            return manager.create_scene(self.scene_name_var.get(), self.scene_desc_var.get(), self.scene_icon_var.get())

        def show_result(scene):
            self.log(f"✅ 已创建场景: {scene.name} (ID: {scene.scene_id})")
            self.scene_name_var.set("")
            self.scene_desc_var.set("")

        self.run_async(do_create, show_result)

    def _get_stats(self) -> None:
        """
        Retrieve and display scene statistics.
        """
        def do_stats():
            manager = create_scene_manager(str(DATA_DIR))
            return manager.get_scene_statistics()

        def show_result(stats):
            self.scene_stats_result.configure(state='normal')
            self.scene_stats_result.delete(1.0, tk.END)
            self.scene_stats_result.insert(tk.END, "📊 场景统计\n\n")
            self.scene_stats_result.insert(tk.END, f"  总场景数: {stats['total_scenes']}\n")
            self.scene_stats_result.insert(tk.END, f"  激活中: {stats['active_scenes']}\n")
            self.scene_stats_result.configure(state='disabled')

        self.run_async(do_stats, show_result)


class DiagTab(BaseTab):
    """
    Diagnostics tab for workflow health monitoring and reporting.
    
    Features:
        - Run diagnostics on workflows
        - View health overview
        - Generate detailed diagnostic reports
    """
    
    def setup_ui(self) -> None:
        """Set up the tab's UI components."""
        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        run_frame = ttk.Frame(notebook)
        notebook.add(run_frame, text="运行诊断")
        self._setup_run_tab(run_frame)

        summary_frame = ttk.Frame(notebook)
        notebook.add(summary_frame, text="健康概览")
        self._setup_summary_tab(summary_frame)

        report_frame = ttk.Frame(notebook)
        notebook.add(report_frame, text="详细报告")
        self._setup_report_tab(report_frame)

    def _setup_run_tab(self, parent: Any) -> None:
        """
        Set up the diagnostic run subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="运行诊断", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="工作流 ID (可选):").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.diag_workflow_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.diag_workflow_var, width=35).grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Button(frame, text="🔍 运行诊断", command=self._run_diag).grid(row=1, column=0, columnspan=2, pady=15)

        self.diag_result = scrolledtext.ScrolledText(frame, height=15, state='disabled')
        self.diag_result.grid(row=2, column=0, columnspan=2, sticky=tk.EW, pady=10)

    def _setup_summary_tab(self, parent: Any) -> None:
        """
        Set up the health summary subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="健康概览", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Button(frame, text="📊 获取健康概览", command=self._get_summary).pack(pady=10)

        self.summary_result = scrolledtext.ScrolledText(frame, height=15, state='disabled')
        self.summary_result.pack(fill=tk.BOTH, expand=True, pady=10)

    def _setup_report_tab(self, parent: Any) -> None:
        """
        Set up the report generation subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="生成详细报告", padding=10)
        frame = ttk.LabelFrame(parent, text="生成详细报告", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="工作流 ID:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.report_workflow_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.report_workflow_var, width=35).grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Button(frame, text="📄 生成报告", command=self._generate_report).grid(row=1, column=0, columnspan=2, pady=15)

        self.report_result = scrolledtext.ScrolledText(frame, height=12, state='disabled')
        self.report_result.grid(row=2, column=0, columnspan=2, sticky=tk.EW, pady=10)

    def _run_diag(self) -> None:
        """
        Run diagnostics on specified workflow or all workflows.
        """
        def do_diag():
            diag = create_diagnostics(str(DATA_DIR))
            workflow_id = self.diag_workflow_var.get()
            
            if workflow_id:
                return diag.diagnose(workflow_id)
            else:
                return diag.get_all_workflows_health()

        def show_result(result):
            self.diag_result.configure(state='normal')
            self.diag_result.delete(1.0, tk.END)
            
            if isinstance(result, list):
                self.diag_result.insert(tk.END, f"📊 工作流健康概览 ({len(result)}个工作流)\n\n")
                for r in result[:10]:
                    emoji = "🟢" if r.health_score >= 75 else "🟡" if r.health_score >= 50 else "🔴"
                    self.diag_result.insert(tk.END, f"  {emoji} {r.workflow_name}: {r.health_score:.1f}分 ({r.success_rate*100:.0f}%成功率)\n")
            else:
                self.diag_result.insert(tk.END, create_diagnostics(str(DATA_DIR)).generate_report_text(result))
            
            self.diag_result.configure(state='disabled')

        self.run_async(do_diag, show_result)

    def _get_summary(self) -> None:
        """
        Retrieve and display health summary across all workflows.
        """
        def do_summary():
            diag = create_diagnostics(str(DATA_DIR))
            return diag.get_health_summary()

        def show_result(summary):
            self.summary_result.configure(state='normal')
            self.summary_result.delete(1.0, tk.END)
            self.summary_result.insert(tk.END, "📊 总体健康状态\n\n")
            self.summary_result.insert(tk.END, f"  工作流总数: {summary.get('total_workflows', 0)}\n")
            
            if 'avg_health_score' in summary:
                self.summary_result.insert(tk.END, f"  平均健康分: {summary['avg_health_score']:.1f}\n")
            if 'avg_success_rate' in summary:
                self.summary_result.insert(tk.END, f"  平均成功率: {summary['avg_success_rate']*100:.1f}%\n")
            
            if "health_distribution" in summary:
                self.summary_result.insert(tk.END, "\n  健康分布:\n")
                for level, count in summary["health_distribution"].items():
                    self.summary_result.insert(tk.END, f"    - {level}: {count}\n")
            
            if summary.get("needs_attention"):
                self.summary_result.insert(tk.END, "\n  ⚠️ 需要关注:\n")
                for name in summary["needs_attention"]:
                    self.summary_result.insert(tk.END, f"    - {name}\n")
            
            self.summary_result.configure(state='disabled')

        self.run_async(do_summary, show_result)

    def _generate_report(self) -> None:
        """
        Generate and save a detailed diagnostic report for a workflow.
        """
        workflow_id = self.report_workflow_var.get()
        if not workflow_id:
            messagebox.showwarning("提示", "请输入工作流 ID")
            return

        def do_report():
            diag = create_diagnostics(str(DATA_DIR))
            return diag.diagnose(workflow_id), workflow_id

        def show_result(result):
            report, wf_id = result
            report_file = DATA_DIR / f"report_{wf_id}_{int(time.time())}.json"
            with open(report_file, "w", encoding="utf-8") as f:
                json.dump({
                    "workflow_id": report.workflow_id,
                    "workflow_name": report.workflow_name,
                    "overall_health": report.overall_health.value,
                    "health_score": report.health_score,
                    "execution_count": report.execution_count,
                    "success_rate": report.success_rate,
                    "avg_duration": report.avg_duration,
                    "trends": [
                        {
                            "period": t.period,
                            "success_rate_change": t.success_rate_change,
                            "trend_direction": t.trend_direction
                        }
                        for t in report.trends
                    ],
                    "issues": [
                        {
                            "type": i.issue_type,
                            "severity": i.severity.value,
                            "title": i.title,
                            "suggestion": i.suggestion,
                            "auto_fixable": i.auto_fixable
                        }
                        for i in report.issues
                    ],
                    "root_causes": report.root_causes,
                    "recommendations": report.recommendations
                }, f, ensure_ascii=False, indent=2)
            
            self.report_result.configure(state='normal')
            self.report_result.delete(1.0, tk.END)
            self.report_result.insert(tk.END, f"✅ 报告已保存: {report_file}\n\n")
            self.report_result.insert(tk.END, create_diagnostics(str(DATA_DIR)).generate_report_text(report))
            self.report_result.configure(state='disabled')
            self.log(f"✅ 报告已保存: {report_file}")

        self.run_async(do_report, show_result)


class ShareTab(BaseTab):
    """
    Workflow sharing tab for registering, importing, and exporting workflows.
    
    Features:
        - Register workflows for sharing
        - Create share links (public/private/team)
        - Import/export workflows in various formats
        - View sharing statistics
    """
    
    def setup_ui(self) -> None:
        """Set up the tab's UI components."""
        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        register_frame = ttk.Frame(notebook)
        notebook.add(register_frame, text="注册工作流")
        self._setup_register_tab(register_frame)

        link_frame = ttk.Frame(notebook)
        notebook.add(link_frame, text="创建链接")
        self._setup_link_tab(link_frame)

        import_frame = ttk.Frame(notebook)
        notebook.add(import_frame, text="导入/导出")
        self._setup_import_export_tab(import_frame)

        list_frame = ttk.Frame(notebook)
        notebook.add(list_frame, text="分享列表")
        self._setup_list_tab(list_frame)

    def _setup_register_tab(self, parent: Any) -> None:
        """
        Set up the workflow registration subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="注册工作流", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="工作流文件:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.register_file_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.register_file_var, width=35).grid(row=0, column=1, sticky=tk.W, pady=5)
        ttk.Button(frame, text="📁 浏览", command=self._browse_workflow_file).grid(row=0, column=2, padx=5)

        ttk.Button(frame, text="✅ 注册工作流", command=self._register_workflow).grid(row=1, column=0, columnspan=3, pady=15)

    def _setup_link_tab(self, parent: Any) -> None:
        """
        Set up the share link creation subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="创建分享链接", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="工作流 ID:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.link_workflow_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.link_workflow_var, width=35).grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="分享类型:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.link_type_var = tk.StringVar(value="public")
        ttk.Combobox(frame, textvariable=self.link_type_var, values=["public", "private", "team"], width=30).grid(row=1, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="过期天数 (可选):").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.link_expires_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.link_expires_var, width=35).grid(row=2, column=1, sticky=tk.W, pady=5)

        ttk.Button(frame, text="🔗 创建分享链接", command=self._create_link).grid(row=3, column=0, columnspan=2, pady=15)

        self.link_result = scrolledtext.ScrolledText(frame, height=6, state='disabled')
        self.link_result.grid(row=4, column=0, columnspan=2, sticky=tk.EW, pady=10)

    def _setup_import_export_tab(self, parent: Any) -> None:
        """
        Set up the import/export subtab container.
        
        Args:
            parent: The parent frame widget.
        """
        notebook = ttk.Notebook(parent)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        import_frame = ttk.Frame(notebook)
        notebook.add(import_frame, text="导入")
        self._setup_import_tab(import_frame)

        export_frame = ttk.Frame(notebook)
        notebook.add(export_frame, text="导出")
        self._setup_export_tab(export_frame)

    def _setup_import_tab(self, parent: Any) -> None:
        """
        Set up the import subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="导入工作流", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="来源:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.import_source_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.import_source_var, width=35).grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="格式:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.import_format_var = tk.StringVar(value="json")
        ttk.Combobox(frame, textvariable=self.import_format_var, values=["json", "base64", "url"], width=30).grid(row=1, column=1, sticky=tk.W, pady=5)

        ttk.Button(frame, text="📥 导入工作流", command=self._import_workflow).grid(row=2, column=0, columnspan=2, pady=15)

        self.import_result = scrolledtext.ScrolledText(frame, height=8, state='disabled')
        self.import_result.grid(row=3, column=0, columnspan=2, sticky=tk.EW, pady=10)

    def _setup_export_tab(self, parent: Any) -> None:
        """
        Set up the export subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="导出工作流", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="工作流 ID:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.export_workflow_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.export_workflow_var, width=35).grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="格式:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.export_format_var = tk.StringVar(value="json")
        ttk.Combobox(frame, textvariable=self.export_format_var, values=["json", "base64"], width=30).grid(row=1, column=1, sticky=tk.W, pady=5)

        ttk.Button(frame, text="📤 导出工作流", command=self._export_workflow).grid(row=2, column=0, columnspan=2, pady=15)

        self.export_result = scrolledtext.ScrolledText(frame, height=8, state='disabled')
        self.export_result.grid(row=3, column=0, columnspan=2, sticky=tk.EW, pady=10)

    def _setup_list_tab(self, parent):
        frame = ttk.LabelFrame(parent, text="分享列表", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill=tk.X, pady=5)
        ttk.Button(btn_frame, text="📋 列出分享", command=self._list_shared).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="📊 分享统计", command=self._share_stats).pack(side=tk.LEFT, padx=5)

        self.share_list_result = scrolledtext.ScrolledText(frame, height=12, state='disabled')
        self.share_list_result.pack(fill=tk.BOTH, expand=True, pady=10)

    def _browse_workflow_file(self) -> None:
        """
        Open a file dialog to browse for workflow JSON files.
        """
        filename = filedialog.askopenfilename(filetypes=[("JSON files", "*.json"), ("All files", "*.*")])
        if filename:
            self.register_file_var.set(filename)

    def _register_workflow(self):
        workflow_file = self.register_file_var.get()
        if not workflow_file:
            messagebox.showwarning("提示", "请选择工作流文件")
            return

        def do_register():
            share_sys = create_share_system(str(DATA_DIR))
            with open(workflow_file, "r", encoding="utf-8") as f:
                workflow_data = json.load(f)
            return share_sys.register_workflow(workflow_data)

        def show_result(wf_id):
            self.log(f"✅ 已注册工作流: {wf_id}")

        self.run_async(do_register, show_result)

    def _create_link(self) -> None:
        """
        Create a share link for the specified workflow.
        """
        workflow_id = self.link_workflow_var.get()
        if not workflow_id:
            messagebox.showwarning("提示", "请输入工作流 ID")
            return

        def do_create():
            share_sys = create_share_system(str(DATA_DIR))
            share_type = ShareType.PUBLIC if self.link_type_var.get() == "public" else ShareType.PRIVATE if self.link_type_var.get() == "private" else ShareType.TEAM
            expires = int(self.link_expires_var.get()) if self.link_expires_var.get() else None
            return share_sys.create_share_link(workflow_id, share_type, expires)

        def show_result(link):
            self.link_result.configure(state='normal')
            self.link_result.delete(1.0, tk.END)
            if link:
                share_sys = create_share_system(str(DATA_DIR))
                url = share_sys.generate_share_url(link.link_id)
                self.link_result.insert(tk.END, f"✅ 分享链接已创建:\n")
                self.link_result.insert(tk.END, f"   {url}\n")
                self.link_result.insert(tk.END, f"   链接ID: {link.link_id}\n")
                if link.expires_at:
                    exp_date = datetime.datetime.fromtimestamp(link.expires_at)
                    self.link_result.insert(tk.END, f"   过期时间: {exp_date.strftime('%Y-%m-%d %H:%M')}\n")
            else:
                self.link_result.insert(tk.END, f"❌ 工作流不存在: {workflow_id}")
            self.link_result.configure(state='disabled')

        self.run_async(do_create, show_result)

    def _import_workflow(self) -> None:
        """
        Import a workflow from URL, JSON, or base64 format.
        """
        source = self.import_source_var.get()
        if not source:
            messagebox.showwarning("提示", "请输入来源")
            return

        def do_import():
            share_sys = create_share_system(str(DATA_DIR))
            fmt = self.import_format_var.get()
            if fmt == "url":
                return share_sys.import_from_url(source)
            else:
                return share_sys.import_workflow(source, fmt)

        def show_result(report):
            self.import_result.configure(state='normal')
            self.import_result.delete(1.0, tk.END)
            self.import_result.insert(tk.END, f"导入结果: {report.result.value}\n")
            self.import_result.insert(tk.END, f"工作流: {report.workflow_name}\n")
            self.import_result.insert(tk.END, f"消息: {report.message}\n")
            if report.warnings:
                self.import_result.insert(tk.END, "\n警告:\n")
                for w in report.warnings:
                    self.import_result.insert(tk.END, f"  - {w}\n")
            self.import_result.configure(state='disabled')

        self.run_async(do_import, show_result)

    def _export_workflow(self) -> None:
        """
        Export a workflow to JSON or base64 format.
        """
        workflow_id = self.export_workflow_var.get()
        if not workflow_id:
            messagebox.showwarning("提示", "请输入工作流 ID")
            return

        def do_export():
            share_sys = create_share_system(str(DATA_DIR))
            if self.export_format_var.get() == "base64":
                return share_sys.export_to_base64(workflow_id)
            else:
                return share_sys.export_to_json(workflow_id)

        def show_result(output):
            self.export_result.configure(state='normal')
            self.export_result.delete(1.0, tk.END)
            if output:
                self.export_result.insert(tk.END, f"✅ 工作流已导出:\n")
                if self.export_format_var.get() == "base64":
                    self.export_result.insert(tk.END, output[:500] + "..." if len(output) > 500 else output)
                else:
                    self.export_result.insert(tk.END, output[:500] + "..." if len(output) > 500 else output)
            else:
                self.export_result.insert(tk.END, f"❌ 工作流不存在: {workflow_id}")
            self.export_result.configure(state='disabled')

        self.run_async(do_export, show_result)

    def _list_shared(self) -> None:
        """
        List all shared workflows and their statistics.
        """
        def do_list():
            share_sys = create_share_system(str(DATA_DIR))
            return share_sys.list_shared_workflows()

        def show_result(links):
            self.share_list_result.configure(state='normal')
            self.share_list_result.delete(1.0, tk.END)
            if not links:
                self.share_list_result.insert(tk.END, "暂无分享链接")
            else:
                self.share_list_result.insert(tk.END, f"📤 分享列表 ({len(links)}个):\n\n")
                for link in links:
                    self.share_list_result.insert(tk.END, f"  {link.link_id}: {link.workflow_name}\n")
                    self.share_list_result.insert(tk.END, f"    类型: {link.share_type.value}, 查看: {link.view_count}, 导入: {link.import_count}\n")
            self.share_list_result.configure(state='disabled')

        self.run_async(do_list, show_result)

    def _share_stats(self) -> None:
        """
        Display sharing statistics.
        """
        def do_stats():
            share_sys = create_share_system(str(DATA_DIR))
            return share_sys.get_share_stats()

        def show_result(stats):
            self.share_list_result.configure(state='normal')
            self.share_list_result.delete(1.0, tk.END)
            self.share_list_result.insert(tk.END, "📊 分享统计\n\n")
            self.share_list_result.insert(tk.END, f"  总链接数: {stats['total_links']}\n")
            self.share_list_result.insert(tk.END, f"  总查看: {stats['total_views']}\n")
            self.share_list_result.insert(tk.END, f"  总导入: {stats['total_imports']}\n")
            self.share_list_result.insert(tk.END, f"  有效链接: {stats['active_links']}\n")
            self.share_list_result.configure(state='disabled')

        self.run_async(do_stats, show_result)


class PipeTab(BaseTab):
    """
    Pipeline tab for chaining multiple workflow steps together.
    
    Features:
        - List existing pipeline chains
        - Create new chains (linear/branch/parallel modes)
        - Add steps to chains
        - Execute pipeline chains
    """
    
    def setup_ui(self) -> None:
        """Set up the tab's UI components."""
        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        list_frame = ttk.Frame(notebook)
        notebook.add(list_frame, text="管道列表")
        self._setup_list_tab(list_frame)

        create_frame = ttk.Frame(notebook)
        notebook.add(create_frame, text="创建管道")
        self._setup_create_tab(create_frame)

        run_frame = ttk.Frame(notebook)
        notebook.add(run_frame, text="运行管道")
        self._setup_run_tab(run_frame)

    def _setup_list_tab(self, parent):
        frame = ttk.LabelFrame(parent, text="管道链列表", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Button(frame, text="🔄 刷新列表", command=self._list_chains).pack(pady=5)

        self.pipe_tree = ttk.Treeview(frame, columns=("id", "name", "mode", "steps", "status"), show="headings", height=10)
        self.pipe_tree.heading("id", text="ID")
        self.pipe_tree.heading("name", text="名称")
        self.pipe_tree.heading("mode", text="模式")
        self.pipe_tree.heading("steps", text="步骤数")
        self.pipe_tree.heading("status", text="状态")
        self.pipe_tree.column("id", width=100)
        self.pipe_tree.column("name", width=150)
        self.pipe_tree.column("mode", width=80)
        self.pipe_tree.column("steps", width=80)
        self.pipe_tree.column("status", width=80)
        self.pipe_tree.pack(fill=tk.BOTH, expand=True, pady=10)

        add_frame = ttk.LabelFrame(frame, text="添加步骤到选中管道", padding=5)
        add_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(add_frame, text="步骤名称:").grid(row=0, column=0, sticky=tk.W)
        self.add_step_name_var = tk.StringVar()
        ttk.Entry(add_frame, textvariable=self.add_step_name_var, width=15).grid(row=0, column=1, padx=5)
        
        ttk.Label(add_frame, text="命令:").grid(row=0, column=2, sticky=tk.W)
        self.add_step_cmd_var = tk.StringVar()
        ttk.Entry(add_frame, textvariable=self.add_step_cmd_var, width=20).grid(row=0, column=3, padx=5)
        
        ttk.Button(add_frame, text="➕ 添加步骤", command=self._add_step).grid(row=0, column=4, padx=5)

    def _setup_create_tab(self, parent):
        frame = ttk.LabelFrame(parent, text="创建管道链", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="管道名称:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.pipe_name_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.pipe_name_var, width=35).grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="管道模式:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.pipe_mode_var = tk.StringVar(value="linear")
        ttk.Combobox(frame, textvariable=self.pipe_mode_var, values=["linear", "branch", "parallel"], width=30).grid(row=1, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="模式说明:").grid(row=2, column=0, sticky=tk.NW, pady=5)
        desc_text = "linear: 线性管道 (A | B | C)\nbranch: 分支管道 (A -> [B, C])\nparallel: 并行管道"
        ttk.Label(frame, text=desc_text, justify=tk.LEFT).grid(row=2, column=1, sticky=tk.W, pady=5)

        ttk.Button(frame, text="✅ 创建管道链", command=self._create_chain).grid(row=3, column=0, columnspan=2, pady=15)

    def _setup_run_tab(self, parent):
        frame = ttk.LabelFrame(parent, text="运行管道链", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="管道链 ID:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.run_chain_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.run_chain_var, width=35).grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="输入数据 (JSON):").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.run_input_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.run_input_var, width=35).grid(row=1, column=1, sticky=tk.W, pady=5)

        ttk.Button(frame, text="▶ 运行管道", command=self._run_chain).grid(row=2, column=0, columnspan=2, pady=15)

        self.run_result = scrolledtext.ScrolledText(frame, height=12, state='disabled')
        self.run_result.grid(row=3, column=0, columnspan=2, sticky=tk.EW, pady=10)

    def _list_chains(self) -> None:
        """
        List all pipeline chains in the tree view.
        """
        def do_list():
            runner = create_pipeline_runner(str(DATA_DIR))
            return runner.list_chains()

        def show_result(chains):
            for item in self.pipe_tree.get_children():
                self.pipe_tree.delete(item)
            
            for chain in chains:
                status = "active" if any(s.enabled for s in chain.steps) else "disabled"
                self.pipe_tree.insert("", tk.END, values=(chain.chain_id, chain.name, chain.mode.value, len(chain.steps), status))

        self.run_async(do_list, show_result)

    def _create_chain(self) -> None:
        """
        Create a new pipeline chain with the specified name and mode.
        """
        def do_create():
            runner = create_pipeline_runner(str(DATA_DIR))
            pipe_mode = PipeMode(self.pipe_mode_var.get())
            return runner.create_chain(self.pipe_name_var.get(), pipe_mode)

        def show_result(chain):
            self.log(f"✅ 已创建管道链: {chain.chain_id}")
            self.pipe_name_var.set("")
            self._list_chains()

        self.run_async(do_create, show_result)

    def _add_step(self) -> None:
        """
        Add a step to the selected pipeline chain.
        """
        selected = self.pipe_tree.selection()
        if not selected:
            messagebox.showwarning("提示", "请先选择一个管道链")
            return
        
        chain_id = self.pipe_tree.item(selected[0])["values"][0]
        step_name = self.add_step_name_var.get()
        step_cmd = self.add_step_cmd_var.get()
        
        if not step_name or not step_cmd:
            messagebox.showwarning("提示", "请输入步骤名称和命令")
            return

        def do_add():
            runner = create_pipeline_runner(str(DATA_DIR))
            return runner.add_step(chain_id, step_name, step_cmd)

        def show_result(step):
            if step:
                self.log(f"✅ 已添加步骤: {step.step_id}")
                self.add_step_name_var.set("")
                self.add_step_cmd_var.set("")
                self._list_chains()
            else:
                self.log(f"❌ 管道链不存在: {chain_id}")

        self.run_async(do_add, show_result)

    def _run_chain(self) -> None:
        """
        Execute the specified pipeline chain with optional input data.
        """
        chain_id = self.run_chain_var.get()
        if not chain_id:
            messagebox.showwarning("提示", "请输入管道链 ID")
            return

        def do_run():
            runner = create_pipeline_runner(str(DATA_DIR))
            input_data = None
            if self.run_input_var.get():
                try:
                    input_data = json.loads(self.run_input_var.get())
                except json.JSONDecodeError:
                    return None
            return runner.execute_chain(chain_id, input_data)

        def show_result(result):
            self.run_result.configure(state='normal')
            self.run_result.delete(1.0, tk.END)
            if result:
                self.run_result.insert(tk.END, f"管道执行结果:\n")
                self.run_result.insert(tk.END, f"  成功: {'✅' if result.success else '❌'}\n")
                self.run_result.insert(tk.END, f"  耗时: {result.total_duration:.2f}秒\n")
                
                if result.final_output:
                    self.run_result.insert(tk.END, f"\n  输出:\n")
                    self.run_result.insert(tk.END, f"  {json.dumps(result.final_output, ensure_ascii=False, indent=4)}\n")
                
                if result.errors:
                    self.run_result.insert(tk.END, f"\n错误:\n")
                    for err in result.errors:
                        self.run_result.insert(tk.END, f"  - {err}\n")
            else:
                self.run_result.insert(tk.END, "❌ 无效的JSON输入或管道链不存在")
            self.run_result.configure(state='disabled')

        self.run_async(do_run, show_result)


class RecTab(BaseTab):
    """
    Screen recording tab for capturing and converting user actions to workflows.
    
    Features:
        - Record screen actions
        - Manually add action records
        - List and analyze recordings
        - Convert recordings to workflows
    """
    
    def setup_ui(self) -> None:
        """Set up the tab's UI components."""
        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        record_frame = ttk.Frame(notebook)
        notebook.add(record_frame, text="录制")
        self._setup_record_tab(record_frame)

        list_frame = ttk.Frame(notebook)
        notebook.add(list_frame, text="录制列表")
        self._setup_list_tab(list_frame)

        convert_frame = ttk.Frame(notebook)
        notebook.add(convert_frame, text="转换工作流")
        self._setup_convert_tab(convert_frame)

    def _setup_record_tab(self, parent: Any) -> None:
        """
        Set up the recording subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="屏幕录制", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="录制名称:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.rec_name_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.rec_name_var, width=35).grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="描述:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.rec_desc_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.rec_desc_var, width=35).grid(row=1, column=1, sticky=tk.W, pady=5)

        btn_frame = ttk.Frame(frame)
        btn_frame.grid(row=2, column=0, columnspan=2, pady=10)
        ttk.Button(btn_frame, text="🎬 开始录制", command=self._start_recording).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="⏹ 停止录制", command=self._stop_recording).pack(side=tk.LEFT, padx=5)

        add_frame = ttk.LabelFrame(frame, text="手动添加动作", padding=5)
        add_frame.grid(row=3, column=0, columnspan=2, sticky=tk.EW, pady=10)

        ttk.Label(add_frame, text="录制ID:").grid(row=0, column=0, sticky=tk.W)
        self.add_action_rec_id_var = tk.StringVar()
        ttk.Entry(add_frame, textvariable=self.add_action_rec_id_var, width=15).grid(row=0, column=1, padx=5)

        ttk.Label(add_frame, text="类型:").grid(row=0, column=2, sticky=tk.W)
        self.add_action_type_var = tk.StringVar(value="click")
        ttk.Combobox(add_frame, textvariable=self.add_action_type_var, 
                     values=["click", "type", "hotkey", "wait", "launch_app"], width=10).grid(row=0, column=3, padx=5)

        ttk.Label(add_frame, text="X:").grid(row=1, column=0, sticky=tk.W)
        self.add_action_x_var = tk.StringVar()
        ttk.Entry(add_frame, textvariable=self.add_action_x_var, width=8).grid(row=1, column=1, sticky=tk.W, padx=5)

        ttk.Label(add_frame, text="Y:").grid(row=1, column=2, sticky=tk.W)
        self.add_action_y_var = tk.StringVar()
        ttk.Entry(add_frame, textvariable=self.add_action_y_var, width=8).grid(row=1, column=3, sticky=tk.W, padx=5)

        ttk.Label(add_frame, text="文本:").grid(row=2, column=0, sticky=tk.W)
        self.add_action_text_var = tk.StringVar()
        ttk.Entry(add_frame, textvariable=self.add_action_text_var, width=15).grid(row=2, column=1, columnspan=2, sticky=tk.W, padx=5)

        ttk.Label(add_frame, text="热键:").grid(row=2, column=3, sticky=tk.W)
        self.add_action_key_var = tk.StringVar()
        ttk.Entry(add_frame, textvariable=self.add_action_key_var, width=8).grid(row=2, column=4, sticky=tk.W, padx=5)

        ttk.Button(add_frame, text="➕ 添加动作", command=self._add_action).grid(row=3, column=0, columnspan=5, pady=5)

    def _setup_list_tab(self, parent: Any) -> None:
        """
        Set up the recording list subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="录制列表", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill=tk.X, pady=5)
        ttk.Button(btn_frame, text="🔄 刷新列表", command=self._list_recordings).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="📊 分析选中录制", command=self._analyze_recording).pack(side=tk.LEFT, padx=5)

        self.rec_tree = ttk.Treeview(frame, columns=("id", "name", "actions", "duration", "created"), show="headings", height=10)
        self.rec_tree.heading("id", text="ID")
        self.rec_tree.heading("name", text="名称")
        self.rec_tree.heading("actions", text="动作数")
        self.rec_tree.heading("duration", text="时长(秒)")
        self.rec_tree.heading("created", text="创建时间")
        self.rec_tree.column("id", width=100)
        self.rec_tree.column("name", width=150)
        self.rec_tree.column("actions", width=80)
        self.rec_tree.column("duration", width=80)
        self.rec_tree.column("created", width=120)
        self.rec_tree.pack(fill=tk.BOTH, expand=True, pady=10)

        self.rec_analysis_result = scrolledtext.ScrolledText(frame, height=8, state='disabled')
        self.rec_analysis_result.pack(fill=tk.BOTH, expand=True, pady=5)

    def _setup_convert_tab(self, parent: Any) -> None:
        """
        Set up the workflow conversion subtab.
        
        Args:
            parent: The parent frame widget.
        """
        frame = ttk.LabelFrame(parent, text="转换为工作流", padding=10)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(frame, text="录制 ID:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.convert_rec_id_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.convert_rec_id_var, width=35).grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="工作流名称:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.convert_name_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.convert_name_var, width=35).grid(row=1, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="检测模式:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.convert_mode_var = tk.StringVar(value="image")
        ttk.Combobox(frame, textvariable=self.convert_mode_var, values=["image", "text", "coordinate"], width=30).grid(row=2, column=1, sticky=tk.W, pady=5)

        ttk.Button(frame, text="🔄 转换为工作流", command=self._convert_workflow).grid(row=3, column=0, columnspan=2, pady=15)

        self.convert_result = scrolledtext.ScrolledText(frame, height=10, state='disabled')
        self.convert_result.grid(row=4, column=0, columnspan=2, sticky=tk.EW, pady=10)

    def _start_recording(self) -> None:
        """
        Start a new screen recording session.
        """
        def do_start():
            converter = create_screen_recorder(str(DATA_DIR))
            return converter.start_recording(self.rec_name_var.get(), self.rec_desc_var.get())

        def show_result(rec):
            self.log(f"✅ 开始录制: {rec.recording_id}")
            self.add_action_rec_id_var.set(rec.recording_id)

        self.run_async(do_start, show_result)

    def _stop_recording(self) -> None:
        """
        Stop the current recording session.
        """
        rec_id = self.add_action_rec_id_var.get()
        if not rec_id:
            messagebox.showwarning("提示", "请输入录制 ID")
            return

        def do_stop():
            converter = create_screen_recorder(str(DATA_DIR))
            return converter.stop_recording(rec_id)

        def show_result(rec):
            if rec:
                self.log(f"✅ 录制停止: {rec.recording_id}, 动作数: {len(rec.actions)}, 时长: {rec.duration:.1f}秒")
                self._list_recordings()
            else:
                self.log(f"❌ 录制不存在: {rec_id}")

        self.run_async(do_stop, show_result)

    def _add_action(self) -> None:
        """
        Manually add an action to the current recording.
        
        Validates X/Y coordinates are integers if provided.
        """
        rec_id = self.add_action_rec_id_var.get()
        if not rec_id:
            messagebox.showwarning("提示", "请输入录制 ID")
            return

        def do_add():
            converter = create_screen_recorder(str(DATA_DIR))
            action_data: Dict[str, Any] = {
                "action_type": self.add_action_type_var.get(),
                "timestamp": time.time()
            }
            
            x_str = self.add_action_x_var.get()
            y_str = self.add_action_y_var.get()
            
            if x_str:
                try:
                    action_data["x"] = int(x_str)
                except ValueError:
                    return False
            if y_str:
                try:
                    action_data["y"] = int(y_str)
                except ValueError:
                    return False
            
            text_val = self.add_action_text_var.get()
            if text_val:
                action_data["text"] = text_val
            
            key_val = self.add_action_key_var.get()
            if key_val:
                action_data["key"] = key_val
            
            return converter.add_action(rec_id, action_data)

        def show_result(success):
            if success:
                self.log(f"✅ 已添加动作")
            else:
                self.log(f"❌ 录制不存在")

        self.run_async(do_add, show_result)

    def _list_recordings(self) -> None:
        """
        List all recordings in the tree view.
        """
        def do_list():
            converter = create_screen_recorder(str(DATA_DIR))
            return converter.list_recordings()

        def show_result(recordings):
            for item in self.rec_tree.get_children():
                self.rec_tree.delete(item)
            
            for rec in recordings:
                created = datetime.datetime.fromtimestamp(rec.created_at).strftime('%Y-%m-%d %H:%M')
                self.rec_tree.insert("", tk.END, values=(rec.recording_id, rec.name, len(rec.actions), f"{rec.duration:.1f}", created))

        self.run_async(do_list, show_result)

    def _analyze_recording(self) -> None:
        """
        Analyze the selected recording and display statistics.
        """
        selected = self.rec_tree.selection()
        if not selected:
            messagebox.showwarning("提示", "请先选择一个录制")
            return
        
        rec_id = self.rec_tree.item(selected[0])["values"][0]

        def do_analyze():
            converter = create_screen_recorder(str(DATA_DIR))
            return converter.analyze_recording(rec_id)

        def show_result(analysis):
            self.rec_analysis_result.configure(state='normal')
            self.rec_analysis_result.delete(1.0, tk.END)
            if analysis:
                self.rec_analysis_result.insert(tk.END, f"📊 录制分析: {analysis.get('name')}\n")
                self.rec_analysis_result.insert(tk.END, f"  动作数: {analysis.get('action_count')}\n")
                self.rec_analysis_result.insert(tk.END, f"  时长: {analysis.get('duration', 0):.1f}秒\n")
                self.rec_analysis_result.insert(tk.END, f"  分辨率: {analysis.get('resolution')}\n")
                
                if analysis.get('action_types'):
                    self.rec_analysis_result.insert(tk.END, f"\n  动作类型分布:\n")
                    for at, count in analysis['action_types'].items():
                        self.rec_analysis_result.insert(tk.END, f"    - {at}: {count}\n")
            else:
                self.rec_analysis_result.insert(tk.END, f"❌ 录制不存在: {rec_id}")
            self.rec_analysis_result.configure(state='disabled')

        self.run_async(do_analyze, show_result)

    def _convert_workflow(self) -> None:
        """
        Convert a recording to a workflow with the specified detection mode.
        """
        rec_id = self.convert_rec_id_var.get()
        if not rec_id:
            messagebox.showwarning("提示", "请输入录制 ID")
            return

        def do_convert():
            converter = create_screen_recorder(str(DATA_DIR))
            detection = ElementDetection.TEXT if self.convert_mode_var.get() == "text" else ElementDetection.COORDINATE if self.convert_mode_var.get() == "coordinate" else ElementDetection.IMAGE
            return converter.convert_to_workflow(rec_id, self.convert_name_var.get(), detection)

        def show_result(result):
            self.convert_result.configure(state='normal')
            self.convert_result.delete(1.0, tk.END)
            if result:
                self.convert_result.insert(tk.END, f"✅ 转换成功: {result.workflow_name}\n")
                self.convert_result.insert(tk.END, f"   工作流ID: {result.workflow_id}\n")
                self.convert_result.insert(tk.END, f"   步骤数: {len(result.steps)}\n")
                
                if result.warnings:
                    self.convert_result.insert(tk.END, f"\n⚠️ 警告 ({len(result.warnings)}个):\n")
                    for w in result.warnings[:3]:
                        self.convert_result.insert(tk.END, f"   - {w}\n")
                
                converter = create_screen_recorder(str(DATA_DIR))
                workflow_file = DATA_DIR / f"workflow_{result.workflow_id}.json"
                with open(workflow_file, "w", encoding="utf-8") as f:
                    f.write(converter.export_workflow_json(result))
                
                self.convert_result.insert(tk.END, f"\n💾 工作流已保存: {workflow_file}\n")
                self.log(f"✅ 工作流已保存: {workflow_file}")
            else:
                self.convert_result.insert(tk.END, f"❌ 录制不存在或为空")
            self.convert_result.configure(state='disabled')

        self.run_async(do_convert, show_result)


class RabAIGUI:
    """
    Main GUI application class for RabAI AutoClick.
    
    This class manages the main application window, sets up the UI layout,
    and coordinates all tabs including predictive engine, self-healing,
    scene management, diagnostics, workflow sharing, pipeline, and recording.
    
    Attributes:
        root: The root Tkinter window.
        tabs: Dictionary of tab instances keyed by name.
        log_text: ScrolledText widget for log output.
        status_label: Label widget for status display.
    """
    
    def __init__(self) -> None:
        """
        Initialize the RabAI GUI application.
        """
        self.root = tk.Tk()
        self.root.title("RabAI AutoClick v22 - 智能自动化工具")
        self.root.geometry("1000x700")
        self.root.minsize(800, 600)

        self._setup_styles()
        self._setup_menu()
        self._setup_main_layout()

    def _setup_styles(self) -> None:
        """
        Configure Tkinter ttk styles for the application.
        """
        style = ttk.Style()
        style.theme_use('clam')
        
        style.configure('TNotebook', background='#f0f0f0')
        style.configure('TNotebook.Tab', padding=[15, 5], font=('Arial', 10))
        style.configure('TFrame', background='#f0f0f0')
        style.configure('TLabelframe', background='#f0f0f0')
        style.configure('TLabelframe.Label', font=('Arial', 10, 'bold'))
        style.configure('TButton', padding=5)
        style.configure('Treeview', rowheight=25)

    def _setup_menu(self) -> None:
        """
        Set up the application menu bar.
        """
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="文件", menu=file_menu)
        file_menu.add_command(label="打开数据目录", command=self._open_data_dir)
        file_menu.add_separator()
        file_menu.add_command(label="退出", command=self.root.quit)

        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="帮助", menu=help_menu)
        help_menu.add_command(label="关于", command=self._show_about)

    def _setup_main_layout(self) -> None:
        """
        Set up the main layout with tabs, log area, and status bar.
        """
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill=tk.BOTH, expand=True)

        self.tabs = {
            "predict": PredictTab(notebook, self),
            "heal": HealTab(notebook, self),
            "scene": SceneTab(notebook, self),
            "diag": DiagTab(notebook, self),
            "share": ShareTab(notebook, self),
            "pipe": PipeTab(notebook, self),
            "rec": RecTab(notebook, self),
        }

        notebook.add(self.tabs["predict"], text="🔮 预测引擎")
        notebook.add(self.tabs["heal"], text="🔧 故障自愈")
        notebook.add(self.tabs["scene"], text="📦 场景管理")
        notebook.add(self.tabs["diag"], text="📊 智能诊断")
        notebook.add(self.tabs["share"], text="📤 工作流分享")
        notebook.add(self.tabs["pipe"], text="🔗 管道集成")
        notebook.add(self.tabs["rec"], text="🎬 屏幕录制")

        log_frame = ttk.LabelFrame(main_frame, text="日志输出", padding=5)
        log_frame.pack(fill=tk.X, pady=5)

        self.log_text = scrolledtext.ScrolledText(log_frame, height=5, state='disabled')
        self.log_text.pack(fill=tk.X)

        status_frame = ttk.Frame(main_frame)
        status_frame.pack(fill=tk.X, pady=2)
        self.status_label = ttk.Label(status_frame, text="就绪")
        self.status_label.pack(side=tk.LEFT)

    def log(self, message: str) -> None:
        """
        Log a message with timestamp to the log text area.
        
        Args:
            message: The message string to log.
        """
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        self.log_text.configure(state='normal')
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state='disabled')

    def _open_data_dir(self) -> None:
        """
        Open the data directory in the system file explorer.
        """
        import subprocess
        subprocess.run(['open', str(DATA_DIR)])

    def _show_about(self) -> None:
        """
        Display the about dialog with application information.
        """
        messagebox.showinfo("关于", 
            "RabAI AutoClick v22\n\n"
            "智能自动化工具\n\n"
            "功能:\n"
            "- 预测性自动化引擎\n"
            "- 故障自愈系统\n"
            "- 场景化工作流包\n"
            "- 增强版智能诊断室\n"
            "- 无代码工作流分享\n"
            "- CLI 管道集成\n"
            "- 屏幕录制转工作流\n\n"
            "版本: 22.0.0"
        )

    def run(self) -> None:
        """
        Start the GUI main event loop.
        """
        self.root.mainloop()


def main() -> None:
    """
    Main entry point for the RabAI AutoClick GUI application.
    
    Initializes the data directory and launches the GUI.
    """
    DATA_DIR.mkdir(exist_ok=True)
    app = RabAIGUI()
    app.run()


if __name__ == "__main__":
    main()

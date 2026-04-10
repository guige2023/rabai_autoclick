"""
场景化工作流包 v22
用户体验优化 - 将常用工作流打包为场景，一键切换
增强功能:
- 依赖图可视化 (ASCII/SVG)
- 并行执行优化
- 关键路径分析
- 工作流热键
- 工作流注解
- 工作流对比
- 工作流推荐
- 执行成本估算
- 工作流测试
- 迁移工具
"""
import json
import time
import re
from datetime import datetime
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import math


class SceneStatus(Enum):
    """场景状态"""
    ACTIVE = "active"         # 启用中
    INACTIVE = "inactive"     # 未启用
    SCHEDULED = "scheduled"  # 预定启用


class TriggerType(Enum):
    """触发类型"""
    MANUAL = "manual"         # 手动触发
    TIME = "time"             # 时间触发
    LOCATION = "location"     # 位置触发
    APP_LAUNCH = "app_launch" # 应用启动触发
    EVENT = "event"           # 事件触发


class CostType(Enum):
    """成本类型"""
    CPU = "cpu"
    IO = "io"
    NETWORK = "network"
    MEMORY = "memory"


@dataclass
class WorkflowRef:
    """工作流引用"""
    workflow_id: str
    workflow_name: str
    enabled: bool = True
    delay: float = 0.0  # 延迟秒数
    order: int = 0      # 执行顺序
    depends_on: List[str] = field(default_factory=list)  # 依赖的工作流ID
    cost: Dict[str, float] = field(default_factory=lambda: {"cpu": 0.1, "io": 0.1, "network": 0.0, "memory": 50})
    hotkey: Optional[str] = None  # 热键绑定
    annotation: Optional[str] = None  # 注解


@dataclass
class StepAnnotation:
    """工作流步骤注解"""
    step_id: str
    note: str
    author: str = "system"
    created_at: float = field(default_factory=time.time)
    tags: List[str] = field(default_factory=list)


@dataclass
class HotkeyBinding:
    """热键绑定"""
    hotkey: str
    scene_id: str
    workflow_id: Optional[str] = None  # 如果为None，则触发整个场景
    description: str = ""
    modifiers: List[str] = field(default_factory=lambda: ["ctrl"])


@dataclass
class StepCost:
    """步骤成本估算"""
    step_id: str
    cpu_cost: float = 0.0      # CPU使用率 0-100
    io_cost: float = 0.0       # IO操作次数
    network_cost: float = 0.0  # 网络请求次数
    memory_cost: float = 0.0   # 内存MB
    estimated_time: float = 0.0  # 预估耗时秒


@dataclass
class Schedule:
    """定时计划"""
    enabled: bool = False
    time: str = ""      # HH:MM 格式
    days: List[str] = field(default_factory=list)  # ["monday", "tuesday", ...]
    timezone: str = "Asia/Shanghai"


@dataclass
class WorkflowScene:
    """工作流场景"""
    scene_id: str
    name: str
    description: str
    icon: str = "📦"
    status: SceneStatus = SceneStatus.INACTIVE
    workflows: List[WorkflowRef] = field(default_factory=list)
    schedule: Schedule = field(default_factory=Schedule)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    usage_count: int = 0
    tags: List[str] = field(default_factory=list)
    settings: Dict[str, Any] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)  # step_id -> annotation
    hotkeys: Dict[str, str] = field(default_factory=dict)  # step_id -> hotkey

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scene_id": self.scene_id,
            "name": self.name,
            "description": self.description,
            "icon": self.icon,
            "status": self.status.value,
            "workflows": [
                {
                    "workflow_id": w.workflow_id,
                    "workflow_name": w.workflow_name,
                    "enabled": w.enabled,
                    "delay": w.delay,
                    "order": w.order,
                    "depends_on": w.depends_on,
                    "cost": w.cost,
                    "hotkey": w.hotkey,
                    "annotation": w.annotation
                }
                for w in self.workflows
            ],
            "schedule": {
                "enabled": self.schedule.enabled,
                "time": self.schedule.time,
                "days": self.schedule.days,
                "timezone": self.schedule.timezone
            },
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "usage_count": self.usage_count,
            "tags": self.tags,
            "settings": self.settings,
            "annotations": self.annotations,
            "hotkeys": self.hotkeys
        }


class WorkflowGraph:
    """工作流依赖图分析"""

    def __init__(self, workflows: List[WorkflowRef]):
        self.workflows = workflows
        self.graph: Dict[str, List[str]] = defaultdict(list)
        self.reverse_graph: Dict[str, List[str]] = defaultdict(list)
        self._build_graph()

    def _build_graph(self) -> None:
        """构建依赖图"""
        for wf in self.workflows:
            self.graph[wf.workflow_id] = wf.depends_on.copy()
            for dep in wf.depends_on:
                self.reverse_graph[dep].append(wf.workflow_id)

    def get_parallel_groups(self) -> List[List[str]]:
        """获取可并行执行的步骤分组"""
        remaining = {wf.workflow_id for wf in self.workflows}
        executed = set()
        groups = []

        while remaining:
            # 找到所有依赖都已完成的步骤
            ready = []
            for wf_id in remaining:
                wf = next((w for w in self.workflows if w.workflow_id == wf_id), None)
                if wf and all(dep in executed for dep in wf.depends_on):
                    ready.append(wf_id)

            if not ready:
                # 检测到循环依赖
                break

            groups.append(ready)
            executed.update(ready)
            remaining.difference_update(ready)

        return groups

    def get_critical_path(self) -> Tuple[List[str], float]:
        """获取关键路径（最长依赖链）"""
        # 计算每个节点的最长路径
        dist = {}
        parent = {}

        def dfs(node: str, visited: Set[str]) -> float:
            if node in dist:
                return dist[node]
            if node in visited:
                return 0  # 循环

            visited.add(node)
            max_dep_time = 0
            max_parent = None

            for dep in self.graph.get(node, []):
                dep_time = dfs(dep, visited.copy())
                if dep_time > max_dep_time:
                    max_dep_time = dep_time
                    max_parent = dep

            wf = next((w for w in self.workflows if w.workflow_id == node), None)
            node_cost = (wf.delay + wf.cost.get("estimated_time", 0)) if wf else 0

            dist[node] = max_dep_time + node_cost
            parent[node] = max_parent

            return dist[node]

        # 计算所有节点的 longest distance
        for wf in self.workflows:
            if wf.workflow_id not in dist:
                dfs(wf.workflow_id, set())

        # 找到最长路径的终点
        end_node = max(dist, key=dist.get)
        total_time = dist[end_node]

        # 回溯构建路径
        path = []
        current = end_node
        while current:
            path.append(current)
            current = parent.get(current)

        path.reverse()
        return path, total_time

    def to_ascii(self) -> str:
        """生成ASCII依赖图"""
        if not self.workflows:
            return "No workflows"

        # 按层级组织
        levels = self.get_parallel_groups()
        lines = []

        for i, group in enumerate(levels):
            level_label = f"Level {i} (parallel):" if len(group) > 1 else f"Level {i}:"
            nodes = []
            for wf_id in group:
                wf = next((w for w in self.workflows if w.workflow_id == wf_id), None)
                if wf:
                    label = f"{wf.workflow_name}"
                    if wf.delay > 0:
                        label += f" (+{wf.delay}s)"
                    nodes.append(label)

            lines.append(level_label)
            lines.append("  " + " | ".join(nodes))

            # 添加连接线
            if i < len(levels) - 1:
                next_level = levels[i + 1]
                lines.append("  |")
                # 检查哪些节点连接到下一层
                connections = []
                for wf_id in group:
                    wf = next((w for w in self.workflows if w.workflow_id == wf_id), None)
                    if wf:
                        deps_in_next = [d for d in wf.depends_on if d in next_level]
                        if deps_in_next:
                            connections.append(f"({wf.workflow_name} -> deps)")
                if connections:
                    lines.append("  |-> " + ", ".join(connections[:3]))

        # 添加关键路径标记
        critical_path, total_time = self.get_critical_path()
        lines.append("")
        lines.append(f"Critical Path ({total_time:.1f}s):")
        path_names = []
        for wf_id in critical_path:
            wf = next((w for w in self.workflows if w.workflow_id == wf_id), None)
            if wf:
                path_names.append(wf.workflow_name)
        lines.append("  -> ".join(path_names))

        return "\n".join(lines)

    def to_svg(self, width: int = 800, height: int = 600) -> str:
        """生成SVG依赖图"""
        if not self.workflows:
            return "<svg></svg>"

        levels = self.get_parallel_groups()
        node_width = 120
        node_height = 40
        padding = 60

        # 计算布局
        max_level_width = max(len(level) for level in levels) if levels else 1
        total_width = max(width, (node_width + padding * 2) * max_level_width)
        total_height = (node_height + padding * 2) * len(levels) + 40

        svg_lines = [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{total_width}" height="{total_height}">',
            '<style>',
            '.node { fill: #e8f4fc; stroke: #2196f3; stroke-width: 2; }',
            '.node-critical { fill: #ffeb3b; stroke: #f44336; stroke-width: 3; }',
            '.label { font-family: Arial; font-size: 12px; text-anchor: middle; }',
            '.edge { stroke: #999; stroke-width: 1.5; fill: none; marker-end: url(#arrow); }',
            '.edge-critical { stroke: #f44336; stroke-width: 2.5; }',
            '</style>',
            '<defs><marker id="arrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z"/></marker></defs>'
        ]

        # 计算节点位置
        critical_path, _ = self.get_critical_path()
        critical_set = set(critical_path)
        node_positions = {}

        for level_idx, level in enumerate(levels):
            level_width = len(level) * (node_width + padding) - padding
            start_x = (total_width - level_width) // 2

            for node_idx, wf_id in enumerate(level):
                x = start_x + node_idx * (node_width + padding)
                y = level_idx * (node_height + padding * 2) + 40
                node_positions[wf_id] = (x, y)

                is_critical = wf_id in critical_set
                node_class = "node-critical" if is_critical else "node"

                wf = next((w for w in self.workflows if w.workflow_id == wf_id), None)
                label = wf.workflow_name[:15] + "..." if len(wf.workflow_name) > 15 else wf.workflow_name if wf else wf_id

                svg_lines.append(
                    f'<rect x="{x}" y="{y}" width="{node_width}" height="{node_height}" rx="5" class="{node_class}"/>'
                )
                svg_lines.append(
                    f'<text x="{x + node_width//2}" y="{y + node_height//2 + 4}" class="label">{label}</text>'
                )

        # 绘制边
        for wf in self.workflows:
            if wf.workflow_id not in node_positions:
                continue
            x1, y1 = node_positions[wf.workflow_id]
            x1 += node_width // 2
            y1 += node_height

            for dep in wf.depends_on:
                if dep in node_positions:
                    x2, y2 = node_positions[dep]
                    x2 += node_width // 2

                    is_critical = wf.workflow_id in critical_set and dep in critical_set
                    edge_class = "edge-critical" if is_critical else "edge"

                    svg_lines.append(
                        f'<path d="M{x1},{y1} Q{x1},{(y1+y2)//2} {x2},{(y1+y2)//2}" class="{edge_class}"/>'
                    )

        svg_lines.append('</svg>')
        return "\n".join(svg_lines)


class WorkflowComparator:
    """工作流对比分析"""

    @staticmethod
    def compare_scenes(scene1: WorkflowScene, scene2: WorkflowScene) -> Dict[str, Any]:
        """对比两个场景"""
        result = {
            "scene1": {"id": scene1.scene_id, "name": scene1.name},
            "scene2": {"id": scene2.scene_id, "name": scene2.name},
            "workflow_count": {
                "scene1": len(scene1.workflows),
                "scene2": len(scene2.workflows)
            },
            "total_delay": {
                "scene1": sum(w.delay for w in scene1.workflows),
                "scene2": sum(w.delay for w in scene2.workflows)
            },
            "total_cost": {
                "scene1": WorkflowComparator._sum_costs(scene1.workflows),
                "scene2": WorkflowComparator._sum_costs(scene2.workflows)
            },
            "unique_workflows": {
                "scene1": [w.workflow_id for w in scene1.workflows if w.workflow_id not in [x.workflow_id for x in scene2.workflows]],
                "scene2": [w.workflow_id for w in scene2.workflows if w.workflow_id not in [x.workflow_id for x in scene1.workflows]]
            },
            "common_workflows": [
                w.workflow_id for w in scene1.workflows
                if w.workflow_id in [x.workflow_id for x in scene2.workflows]
            ],
            "comparison_table": WorkflowComparator._create_comparison_table(scene1, scene2)
        }
        return result

    @staticmethod
    def _sum_costs(workflows: List[WorkflowRef]) -> Dict[str, float]:
        """计算总成本"""
        total = {"cpu": 0.0, "io": 0.0, "network": 0.0, "memory": 0.0}
        for w in workflows:
            for k in total:
                total[k] += w.cost.get(k, 0)
        return total

    @staticmethod
    def _create_comparison_table(scene1: WorkflowScene, scene2: WorkflowScene) -> List[Dict]:
        """创建对比表格"""
        table = []
        max_len = max(len(scene1.workflows), len(scene2.workflows))

        for i in range(max_len):
            row = {}
            wf1 = scene1.workflows[i] if i < len(scene1.workflows) else None
            wf2 = scene2.workflows[i] if i < len(scene2.workflows) else None

            row["index"] = i + 1
            row["scene1"] = {
                "name": wf1.workflow_name if wf1 else "-",
                "delay": wf1.delay if wf1 else 0,
                "cost": wf1.cost if wf1 else {}
            } if wf1 else {"name": "-", "delay": 0, "cost": {}}

            row["scene2"] = {
                "name": wf2.workflow_name if wf2 else "-",
                "delay": wf2.delay if wf2 else 0,
                "cost": wf2.cost if wf2 else {}
            } if wf2 else {"name": "-", "delay": 0, "cost": {}}

            table.append(row)

        return table

    @staticmethod
    def diff_as_text(comparison: Dict) -> str:
        """生成文本格式的差异报告"""
        lines = [
            f"{'='*60}",
            f"Workflow Comparison Report",
            f"{'='*60}",
            f"",
            f"Scene 1: {comparison['scene1']['name']}",
            f"Scene 2: {comparison['scene2']['name']}",
            f"",
            f"{'Workflow Count':<20} | Scene1: {comparison['workflow_count']['scene1']} | Scene2: {comparison['workflow_count']['scene2']}",
            f"{'Total Delay':<20} | Scene1: {comparison['total_delay']['scene1']:.1f}s | Scene2: {comparison['total_delay']['scene2']:.1f}s",
            f"",
            f"{'Cost Summary':-^60}",
        ]

        costs = ["cpu", "io", "network", "memory"]
        for cost_type in costs:
            c1 = comparison['total_cost']['scene1'].get(cost_type, 0)
            c2 = comparison['total_cost']['scene2'].get(cost_type, 0)
            lines.append(f"  {cost_type:<15} | Scene1: {c1:>8.2f} | Scene2: {c2:>8.2f}")

        lines.extend([
            f"",
            f"{'Comparison Table':-^60}",
            f"{'Idx':<4} | {'Scene1':<25} | {'Scene2':<25}",
            f"{'-'*60}"
        ])

        for row in comparison["comparison_table"]:
            s1_name = row["scene1"]["name"][:24]
            s2_name = row["scene2"]["name"][:24]
            lines.append(f"{row['index']:<4} | {s1_name:<25} | {s2_name:<25}")

        lines.extend([
            f"",
            f"{'Unique Workflows':-^60}",
            f"Only in Scene1 ({comparison['scene1']['name']}):",
        ])
        for wf_id in comparison["unique_workflows"]["scene1"]:
            lines.append(f"  - {wf_id}")

        lines.append(f"Only in Scene2 ({comparison['scene2']['name']}):")
        for wf_id in comparison["unique_workflows"]["scene2"]:
            lines.append(f"  - {wf_id}")

        return "\n".join(lines)


class WorkflowAnalyzer:
    """工作流分析器 - 提供优化建议"""

    @staticmethod
    def analyze_scene(scene: WorkflowScene) -> Dict[str, Any]:
        """分析场景并返回优化建议"""
        recommendations = []
        issues = []
        stats = {}

        # 基础统计
        stats["workflow_count"] = len(scene.workflows)
        stats["total_delay"] = sum(w.delay for w in scene.workflows)
        stats["enabled_count"] = len([w for w in scene.workflows if w.enabled])

        # 成本分析
        total_cost = {"cpu": 0, "io": 0, "network": 0, "memory": 0}
        for w in scene.workflows:
            for k in total_cost:
                total_cost[k] += w.cost.get(k, 0)
        stats["cost"] = total_cost

        # 依赖分析
        if scene.workflows:
            graph = WorkflowGraph(scene.workflows)
            parallel_groups = graph.get_parallel_groups()
            critical_path, critical_time = graph.get_critical_path()

            stats["parallel_levels"] = len(parallel_groups)
            stats["parallelizable_count"] = sum(len(g) for g in parallel_groups if len(g) > 1)
            stats["critical_path_length"] = len(critical_path)
            stats["critical_path_time"] = critical_time

        # 检测问题和建议
        # 1. 检查长延迟
        for w in scene.workflows:
            if w.delay > 60:
                issues.append({
                    "type": "long_delay",
                    "workflow_id": w.workflow_id,
                    "workflow_name": w.workflow_name,
                    "message": f"Step '{w.workflow_name}' has a long delay ({w.delay}s)"
                })
                recommendations.append({
                    "type": "reduce_delay",
                    "workflow_id": w.workflow_id,
                    "suggestion": f"Consider reducing delay for '{w.workflow_name}' or making it parallel"
                })

        # 2. 检查串行执行是否可以并行
        if len(parallel_groups) > 1:
            for i, group in enumerate(parallel_groups[:-1]):
                if len(group) == 1:
                    recommendations.append({
                        "type": "parallelize",
                        "workflow_id": group[0],
                        "suggestion": f"'{group[0]}' runs alone in level {i}, consider if it can run in parallel with subsequent steps"
                    })

        # 3. 检查成本过高的步骤
        for w in scene.workflows:
            cpu = w.cost.get("cpu", 0)
            if cpu > 50:
                recommendations.append({
                    "type": "optimize_cost",
                    "workflow_id": w.workflow_id,
                    "suggestion": f"'{w.workflow_name}' has high CPU cost ({cpu}), consider optimization"
                })

        # 4. 检查是否有依赖但没有声明
        workflow_ids = {w.workflow_id for w in scene.workflows}
        for w in scene.workflows:
            for dep in w.depends_on:
                if dep not in workflow_ids:
                    issues.append({
                        "type": "missing_dependency",
                        "workflow_id": w.workflow_id,
                        "message": f"'{w.workflow_name}' depends on '{dep}' which doesn't exist"
                    })

        return {
            "scene_id": scene.scene_id,
            "scene_name": scene.name,
            "stats": stats,
            "issues": issues,
            "recommendations": recommendations
        }

    @staticmethod
    def get_optimization_tips() -> List[Dict[str, str]]:
        """获取通用优化技巧"""
        return [
            {
                "category": "performance",
                "tip": "Group independent steps to run in parallel",
                "benefit": "Reduces total execution time"
            },
            {
                "category": "performance",
                "tip": "Use delays only when necessary",
                "benefit": "Reduces overall workflow duration"
            },
            {
                "category": "cost",
                "tip": "Batch similar operations together",
                "benefit": "Reduces overhead and resource usage"
            },
            {
                "category": "maintainability",
                "tip": "Add annotations to complex steps",
                "benefit": "Makes workflows easier to understand"
            },
            {
                "category": "maintainability",
                "tip": "Assign hotkeys to frequently used workflows",
                "benefit": "Faster access and execution"
            }
        ]


class CostEstimator:
    """执行成本估算器"""

    @staticmethod
    def estimate_scene(scene: WorkflowScene) -> Dict[str, Any]:
        """估算场景执行的资源消耗"""
        total_cpu = 0.0
        total_io = 0.0
        total_network = 0.0
        total_memory = 0.0
        estimated_time = 0.0

        for w in scene.workflows:
            if not w.enabled:
                continue

            # 累加各项成本
            total_cpu += w.cost.get("cpu", 0.1)
            total_io += w.cost.get("io", 0.1)
            total_network += w.cost.get("network", 0)
            total_memory += w.cost.get("memory", 50)

            # 基础执行时间 + 延迟
            base_time = w.cost.get("estimated_time", 1.0)
            estimated_time += base_time + w.delay

        # 考虑并行执行的时间缩减
        if scene.workflows:
            graph = WorkflowGraph(scene.workflows)
            parallel_groups = graph.get_parallel_groups()

            # 理论最短时间（考虑并行）
            parallel_time = 0.0
            for group in parallel_groups:
                group_time = max(
                    (next((w.delay for w in scene.workflows if w.workflow_id == g), 0) +
                     next((w.cost.get("estimated_time", 1.0) for w in scene.workflows if w.workflow_id == g), 0))
                    for g in group
                )
                parallel_time += group_time

            # 取串行和并行的较小值（实际应该是并行优化后的时间）
            optimized_time = min(estimated_time, parallel_time)
        else:
            optimized_time = 0

        return {
            "scene_id": scene.scene_id,
            "scene_name": scene.name,
            "cpu_units": total_cpu,
            "io_operations": total_io,
            "network_requests": total_network,
            "memory_peak_mb": total_memory,
            "estimated_time_seconds": estimated_time,
            "optimized_time_seconds": optimized_time,
            "parallel_speedup": estimated_time / optimized_time if optimized_time > 0 else 1.0,
            "cost_breakdown": {
                "cpu": CostEstimator._categorize_cpu(total_cpu),
                "io": CostEstimator._categorize_io(total_io),
                "network": CostEstimator._categorize_network(total_network),
                "memory": CostEstimator._categorize_memory(total_memory)
            }
        }

    @staticmethod
    def _categorize_cpu(cpu: float) -> Dict[str, Any]:
        """CPU成本分类"""
        category = "low" if cpu < 20 else "medium" if cpu < 50 else "high"
        return {"value": cpu, "category": category, "unit": "%-equivalent"}

    @staticmethod
    def _categorize_io(io: float) -> Dict[str, Any]:
        """IO成本分类"""
        category = "low" if io < 10 else "medium" if io < 30 else "high"
        return {"value": io, "category": category, "unit": "operations"}

    @staticmethod
    def _categorize_network(network: float) -> Dict[str, Any]:
        """网络成本分类"""
        category = "low" if network < 5 else "medium" if network < 20 else "high"
        return {"value": network, "category": category, "unit": "requests"}

    @staticmethod
    def _categorize_memory(memory: float) -> Dict[str, Any]:
        """内存成本分类"""
        category = "low" if memory < 100 else "medium" if memory < 500 else "high"
        return {"value": memory, "category": category, "unit": "MB"}


class WorkflowTester:
    """工作流测试工具"""

    def __init__(self):
        self.test_results: List[Dict] = []

    def smoke_test(self, scene: WorkflowScene) -> Dict[str, Any]:
        """执行烟雾测试"""
        results = {
            "scene_id": scene.scene_id,
            "scene_name": scene.name,
            "tests": [],
            "passed": 0,
            "failed": 0,
            "warnings": []
        }

        # Test 1: 场景结构完整性
        test = {"name": "structure_integrity", "passed": True, "message": ""}
        if not scene.workflows:
            test["passed"] = False
            test["message"] = "Scene has no workflows"
            results["warnings"].append("Empty scene - nothing to execute")
        else:
            test["message"] = f"Scene has {len(scene.workflows)} workflows"
        results["tests"].append(test)
        if test["passed"]: results["passed"] += 1
        else: results["failed"] += 1

        # Test 2: 工作流ID唯一性
        test = {"name": "workflow_id_uniqueness", "passed": True, "message": ""}
        wf_ids = [w.workflow_id for w in scene.workflows]
        if len(wf_ids) != len(set(wf_ids)):
            test["passed"] = False
            test["message"] = "Duplicate workflow IDs found"
            results["warnings"].append("Duplicate workflow_id values detected")
        else:
            test["message"] = "All workflow IDs are unique"
        results["tests"].append(test)
        if test["passed"]: results["passed"] += 1
        else: results["failed"] += 1

        # Test 3: 依赖完整性
        test = {"name": "dependency_integrity", "passed": True, "message": ""}
        wf_id_set = set(wf_ids)
        invalid_deps = []
        for w in scene.workflows:
            for dep in w.depends_on:
                if dep not in wf_id_set:
                    invalid_deps.append(f"{w.workflow_id}->{dep}")
        if invalid_deps:
            test["passed"] = False
            test["message"] = f"Invalid dependencies: {invalid_deps}"
            results["warnings"].append("Some workflows have invalid dependencies")
        else:
            test["message"] = "All dependencies are valid"
        results["tests"].append(test)
        if test["passed"]: results["passed"] += 1
        else: results["failed"] += 1

        # Test 4: 无循环依赖
        test = {"name": "no_circular_dependencies", "passed": True, "message": ""}
        graph = WorkflowGraph(scene.workflows)
        if graph.get_parallel_groups() and not all(len(g) > 0 for g in [graph.get_parallel_groups()]):
            test["passed"] = False
            test["message"] = "Possible circular dependency detected"
            results["warnings"].append("Circular dependency may exist")
        else:
            test["message"] = "No circular dependencies"
        results["tests"].append(test)
        if test["passed"]: results["passed"] += 1
        else: results["failed"] += 1

        # Test 5: 成本估算合理性
        test = {"name": "cost_estimation", "passed": True, "message": ""}
        for w in scene.workflows:
            cpu = w.cost.get("cpu", 0)
            io = w.cost.get("io", 0)
            if cpu < 0 or io < 0:
                test["passed"] = False
                break
        if test["passed"]:
            test["message"] = "Cost values are valid"
        else:
            test["message"] = "Invalid cost values found"
            results["warnings"].append("Negative cost values detected")
        results["tests"].append(test)
        if test["passed"]: results["passed"] += 1
        else: results["failed"] += 1

        # Test 6: 定时配置有效性
        test = {"name": "schedule_config", "passed": True, "message": ""}
        if scene.schedule.enabled:
            if not scene.schedule.time or not re.match(r"^\d{2}:\d{2}$", scene.schedule.time):
                test["passed"] = False
                test["message"] = "Invalid time format"
            if scene.schedule.days:
                valid_days = {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}
                invalid_days = [d for d in scene.schedule.days if d.lower() not in valid_days]
                if invalid_days:
                    test["passed"] = False
                    test["message"] = f"Invalid days: {invalid_days}"
        if test["passed"]:
            test["message"] = "Schedule configuration is valid"
        results["tests"].append(test)
        if test["passed"]: results["passed"] += 1
        else: results["failed"] += 1

        # Test 7: 热键格式验证
        test = {"name": "hotkey_format", "passed": True, "message": ""}
        hotkeys = set()
        for w in scene.workflows:
            if w.hotkey:
                if w.hotkey in hotkeys:
                    test["passed"] = False
                    test["message"] = f"Duplicate hotkey: {w.hotkey}"
                    break
                hotkeys.add(w.hotkey)
        if test["passed"]:
            test["message"] = f"All {len(hotkeys)} hotkeys are unique" if hotkeys else "No hotkeys defined"
        results["tests"].append(test)
        if test["passed"]: results["passed"] += 1
        else: results["failed"] += 1

        results["success"] = results["failed"] == 0
        return results

    def validate_workflow(self, workflow: WorkflowRef) -> Dict[str, Any]:
        """验证单个工作流"""
        issues = []

        if not workflow.workflow_id:
            issues.append("Missing workflow_id")

        if not workflow.workflow_name:
            issues.append("Missing workflow_name")

        if workflow.delay < 0:
            issues.append("Negative delay not allowed")

        for cost_type, value in workflow.cost.items():
            if value < 0:
                issues.append(f"Negative {cost_type} cost")

        return {
            "workflow_id": workflow.workflow_id,
            "valid": len(issues) == 0,
            "issues": issues
        }


class MigrationTool:
    """工作流迁移工具"""

    # AutoIt 脚本模式
    AUTOIT_PATTERN = re.compile(r'(Run|WinWait|WinActivate|Send|ControlClick|MouseClick)\s*\(')
    AUTOIT_ACTIONS = {
        "Run": "execute_app",
        "WinWait": "wait_window",
        "WinActivate": "activate_window",
        "Send": "send_keys",
        "ControlClick": "click_control",
        "MouseClick": "mouse_click"
    }

    # Selenium 模式
    SELENIUM_PATTERN = re.compile(r'(find_element|click|send_keys|get|refresh)\s*\(')
    SELENIUM_ACTIONS = {
        "find_element": "locate_element",
        "click": "element_click",
        "send_keys": "input_text",
        "get": "navigate_url",
        "refresh": "refresh_page"
    }

    # AutoHotkey 模式
    AHK_PATTERN = re.compile(r'(Send|Sleep|Click|Run|WinWait)\s*,?\s*')
    AHK_ACTIONS = {
        "Send": "send_keys",
        "Sleep": "wait_delay",
        "Click": "mouse_click",
        "Run": "execute_app",
        "WinWait": "wait_window"
    }

    @classmethod
    def detect_source_format(cls, code: str) -> Optional[str]:
        """检测源代码格式"""
        if cls.AUTOIT_PATTERN.search(code):
            return "autoit"
        if cls.SELENIUM_PATTERN.search(code):
            return "selenium"
        if cls.AHK_PATTERN.search(code):
            return "autohotkey"
        return None

    @classmethod
    def migrate_from_autoit(cls, code: str) -> Dict[str, Any]:
        """从 AutoIt 脚本迁移"""
        lines = code.split('\n')
        workflows = []

        for i, line in enumerate(lines):
            for action, wf_type in cls.AUTOIT_ACTIONS.items():
                if action in line:
                    # 提取参数
                    match = re.search(rf'{action}\s*\((.*)\)', line)
                    params = match.group(1) if match else ""
                    workflows.append({
                        "type": wf_type,
                        "line": i + 1,
                        "params": params.strip(),
                        "raw": line.strip()
                    })
                    break

        return {
            "source_format": "autoit",
            "workflows": workflows,
            "migrated_config": cls._create_migration_config(workflows)
        }

    @classmethod
    def migrate_from_selenium(cls, code: str) -> Dict[str, Any]:
        """从 Selenium 脚本迁移"""
        lines = code.split('\n')
        workflows = []

        for i, line in enumerate(lines):
            for action, wf_type in cls.SELENIUM_ACTIONS.items():
                if action in line:
                    match = re.search(rf'{action}\s*\((.*)\)', line)
                    params = match.group(1) if match else ""
                    workflows.append({
                        "type": wf_type,
                        "line": i + 1,
                        "params": params.strip(),
                        "raw": line.strip()
                    })
                    break

        return {
            "source_format": "selenium",
            "workflows": workflows,
            "migrated_config": cls._create_migration_config(workflows)
        }

    @classmethod
    def migrate_from_autohotkey(cls, code: str) -> Dict[str, Any]:
        """从 AutoHotkey 脚本迁移"""
        lines = code.split('\n')
        workflows = []

        for i, line in enumerate(lines):
            for action, wf_type in cls.AHK_ACTIONS.items():
                if action in line:
                    # AHK uses comma-separated params
                    parts = line.split(',', 1)
                    params = parts[1].strip() if len(parts) > 1 else ""
                    workflows.append({
                        "type": wf_type,
                        "line": i + 1,
                        "params": params,
                        "raw": line.strip()
                    })
                    break

        return {
            "source_format": "autohotkey",
            "workflows": workflows,
            "migrated_config": cls._create_migration_config(workflows)
        }

    @classmethod
    def _create_migration_config(cls, workflows: List[Dict]) -> Dict[str, Any]:
        """创建迁移后的配置"""
        return {
            "workflows": [
                {
                    "workflow_id": f"migrated_{i}",
                    "workflow_name": f"{w['type'].replace('_', ' ').title()} ({w['line']})",
                    "enabled": True,
                    "delay": 0.0,
                    "order": i,
                    "cost": {"cpu": 5.0, "io": 2.0, "network": 0.0, "memory": 30},
                    "annotation": f"Migrated: {w['raw'][:50]}..."
                }
                for i, w in enumerate(workflows)
            ],
            "migration_info": {
                "total_steps": len(workflows),
                "mapping_version": "1.0"
            }
        }

    @classmethod
    def migrate(cls, code: str) -> Dict[str, Any]:
        """自动检测并迁移"""
        source_format = cls.detect_source_format(code)

        if source_format == "autoit":
            return cls.migrate_from_autoit(code)
        elif source_format == "selenium":
            return cls.migrate_from_selenium(code)
        elif source_format == "autohotkey":
            return cls.migrate_from_autohotkey(code)
        else:
            return {
                "source_format": "unknown",
                "workflows": [],
                "migrated_config": {},
                "error": "Unable to detect source format. Supported: AutoIt, Selenium, AutoHotkey"
            }

    @classmethod
    def generate_migration_report(cls, migration_result: Dict) -> str:
        """生成迁移报告"""
        lines = [
            f"{'='*60}",
            f"Migration Report",
            f"{'='*60}",
            f"",
            f"Source Format: {migration_result['source_format']}",
            f"",
        ]

        if migration_result.get("error"):
            lines.append(f"Error: {migration_result['error']}")
            return "\n".join(lines)

        config = migration_result.get("migrated_config", {})
        workflows = config.get("workflows", [])

        lines.extend([
            f"Migrated {len(workflows)} workflow steps:",
            ""
        ])

        for w in workflows:
            lines.append(f"  [{w['workflow_id']}] {w['workflow_name']}")
            lines.append(f"    Cost: {w['cost']}")
            lines.append(f"    Note: {w['annotation']}")
            lines.append("")

        return "\n".join(lines)


class WorkflowSceneManager:
    """场景化工作流包管理器"""

    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir
        self.scenes: Dict[str, WorkflowScene] = {}
        self.active_scene_id: Optional[str] = None
        self.global_hotkeys: Dict[str, HotkeyBinding] = {}
        self._load_scenes()

    def _load_scenes(self) -> None:
        """加载场景数据"""
        try:
            with open(f"{self.data_dir}/workflow_scenes.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for item in data.get("scenes", []):
                    scene = WorkflowScene(
                        scene_id=item["scene_id"],
                        name=item["name"],
                        description=item["description"],
                        icon=item.get("icon", "📦"),
                        status=SceneStatus(item.get("status", "inactive")),
                        workflows=[
                            WorkflowRef(
                                workflow_id=w["workflow_id"],
                                workflow_name=w["workflow_name"],
                                enabled=w.get("enabled", True),
                                delay=w.get("delay", 0.0),
                                order=w.get("order", 0),
                                depends_on=w.get("depends_on", []),
                                cost=w.get("cost", {"cpu": 0.1, "io": 0.1, "network": 0.0, "memory": 50}),
                                hotkey=w.get("hotkey"),
                                annotation=w.get("annotation")
                            )
                            for w in item.get("workflows", [])
                        ],
                        schedule=Schedule(**item.get("schedule", {})),
                        created_at=item.get("created_at", time.time()),
                        updated_at=item.get("updated_at", time.time()),
                        usage_count=item.get("usage_count", 0),
                        tags=item.get("tags", []),
                        settings=item.get("settings", {}),
                        annotations=item.get("annotations", {}),
                        hotkeys=item.get("hotkeys", {})
                    )
                    self.scenes[scene.scene_id] = scene

                # 加载全局热键
                for hk in data.get("global_hotkeys", []):
                    self.global_hotkeys[hk["hotkey"]] = HotkeyBinding(**hk)

                self.active_scene_id = data.get("active_scene_id")
        except FileNotFoundError:
            self._init_default_scenes()

    def _save_scenes(self) -> None:
        """保存场景数据"""
        data = {
            "scenes": [s.to_dict() for s in self.scenes.values()],
            "active_scene_id": self.active_scene_id,
            "global_hotkeys": [
                {
                    "hotkey": hk.hotkey,
                    "scene_id": hk.scene_id,
                    "workflow_id": hk.workflow_id,
                    "description": hk.description,
                    "modifiers": hk.modifiers
                }
                for hk in self.global_hotkeys.values()
            ]
        }
        with open(f"{self.data_dir}/workflow_scenes.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _init_default_scenes(self) -> None:
        """初始化默认场景"""
        # 晨间场景
        morning = WorkflowScene(
            scene_id="morning_routine",
            name="晨间Routine",
            description="早上起床后的一系列自动化任务",
            icon="🌅",
            workflows=[
                WorkflowRef(workflow_id="wf_001", workflow_name="开灯", order=1),
                WorkflowRef(workflow_id="wf_002", workflow_name="播放音乐", delay=5, order=2),
                WorkflowRef(workflow_id="wf_003", workflow_name="煮咖啡", delay=10, order=3),
                WorkflowRef(workflow_id="wf_004", workflow_name="播报天气", delay=30, order=4),
                WorkflowRef(workflow_id="wf_005", workflow_name="打开新闻", delay=60, order=5)
            ],
            schedule=Schedule(
                enabled=True,
                time="07:00",
                days=["monday", "tuesday", "wednesday", "thursday", "friday"]
            ),
            tags=["生活", "早晨", "自动化"]
        )

        # 工作场景
        work = WorkflowScene(
            scene_id="work_mode",
            name="工作模式",
            description="开启专注工作模式",
            icon="💼",
            workflows=[
                WorkflowRef(workflow_id="wf_010", workflow_name="打开工作应用", order=1),
                WorkflowRef(workflow_id="wf_011", workflow_name="开启勿扰", order=2),
                WorkflowRef(workflow_id="wf_012", workflow_name="整理桌面", order=3)
            ],
            tags=["工作", "专注"]
        )

        # 下班场景
        shutdown = WorkflowScene(
            scene_id="shutdown_routine",
            name="下班Shutdown",
            description="下班前自动清理工作环境",
            icon="🏠",
            workflows=[
                WorkflowRef(workflow_id="wf_020", workflow_name="保存所有文件", order=1),
                WorkflowRef(workflow_id="wf_021", workflow_name="关闭不必要的应用", order=2),
                WorkflowRef(workflow_id="wf_022", workflow_name="清理桌面", order=3),
                WorkflowRef(workflow_id="wf_023", workflow_name="锁屏", delay=5, order=4)
            ],
            schedule=Schedule(
                enabled=True,
                time="18:30",
                days=["monday", "tuesday", "wednesday", "thursday", "friday"]
            ),
            tags=["工作", "下班", "清理"]
        )

        # 出差场景
        travel = WorkflowScene(
            scene_id="travel_ready",
            name="出差Ready",
            description="出差前的一系列准备任务",
            icon="✈️",
            workflows=[
                WorkflowRef(workflow_id="wf_030", workflow_name="查看天气", order=1),
                WorkflowRef(workflow_id="wf_031", workflow_name="打包清单", order=2),
                WorkflowRef(workflow_id="wf_032", workflow_name="叫车", delay=60, order=3),
                WorkflowRef(workflow_id="wf_033", workflow_name="发送行程", delay=120, order=4)
            ],
            tags=["出差", "旅行", "准备"]
        )

        # 会议场景
        meeting = WorkflowScene(
            scene_id="meeting_mode",
            name="会议模式",
            description="开会时自动调整环境",
            icon="📅",
            workflows=[
                WorkflowRef(workflow_id="wf_040", workflow_name="开启静音", order=1),
                WorkflowRef(workflow_id="wf_041", workflow_name="关闭通知", order=2),
                WorkflowRef(workflow_id="wf_042", workflow_name="打开会议软件", delay=5, order=3)
            ],
            tags=["会议", "专注"]
        )

        # 休息场景
        rest = WorkflowScene(
            scene_id="rest_time",
            name="休息时间",
            description="放松休息时的自动化",
            icon="🛋️",
            workflows=[
                WorkflowRef(workflow_id="wf_050", workflow_name="调暗灯光", order=1),
                WorkflowRef(workflow_id="wf_051", workflow_name="播放轻音乐", order=2),
                WorkflowRef(workflow_id="wf_052", workflow_name="关闭工作应用", order=3)
            ],
            tags=["休息", "放松"]
        )

        self.scenes = {
            morning.scene_id: morning,
            work.scene_id: work,
            shutdown.scene_id: shutdown,
            travel.scene_id: travel,
            meeting.scene_id: meeting,
            rest.scene_id: rest
        }
        self._save_scenes()

    def create_scene(self, name: str, description: str = "",
                    icon: str = "📦", tags: List[str] = None) -> WorkflowScene:
        """创建新场景"""
        scene_id = f"scene_{int(time.time())}"
        scene = WorkflowScene(
            scene_id=scene_id,
            name=name,
            description=description,
            icon=icon,
            tags=tags or []
        )
        self.scenes[scene_id] = scene
        self._save_scenes()
        return scene

    def update_scene(self, scene_id: str, **kwargs) -> Optional[WorkflowScene]:
        """更新场景"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return None

        for key, value in kwargs.items():
            if hasattr(scene, key):
                setattr(scene, key, value)

        scene.updated_at = time.time()
        self._save_scenes()
        return scene

    def delete_scene(self, scene_id: str) -> bool:
        """删除场景"""
        if scene_id in self.scenes:
            # 如果删除的是当前激活的场景
            if self.active_scene_id == scene_id:
                self.active_scene_id = None
            del self.scenes[scene_id]
            self._save_scenes()
            return True
        return False

    def add_workflow_to_scene(self, scene_id: str, workflow_id: str,
                             workflow_name: str, order: int = 0,
                             delay: float = 0.0,
                             depends_on: List[str] = None,
                             cost: Dict[str, float] = None) -> bool:
        """添加工作流到场景"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False

        wf = WorkflowRef(
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            order=order,
            delay=delay,
            depends_on=depends_on or [],
            cost=cost or {"cpu": 0.1, "io": 0.1, "network": 0.0, "memory": 50}
        )
        scene.workflows.append(wf)
        scene.updated_at = time.time()
        self._save_scenes()
        return True

    def remove_workflow_from_scene(self, scene_id: str, workflow_id: str) -> bool:
        """从场景移除工作流"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False

        scene.workflows = [w for w in scene.workflows if w.workflow_id != workflow_id]
        scene.updated_at = time.time()
        self._save_scenes()
        return True

    def activate_scene(self, scene_id: str) -> bool:
        """激活场景"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False

        # 先停用当前场景
        if self.active_scene_id and self.active_scene_id in self.scenes:
            self.scenes[self.active_scene_id].status = SceneStatus.INACTIVE

        # 激活新场景
        scene.status = SceneStatus.ACTIVE
        scene.usage_count += 1
        scene.updated_at = time.time()
        self.active_scene_id = scene_id
        self._save_scenes()

        return True

    def deactivate_scene(self, scene_id: str) -> bool:
        """停用场景"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False

        scene.status = SceneStatus.INACTIVE
        if self.active_scene_id == scene_id:
            self.active_scene_id = None
        scene.updated_at = time.time()
        self._save_scenes()
        return True

    def get_active_scene(self) -> Optional[WorkflowScene]:
        """获取当前激活的场景"""
        if self.active_scene_id:
            return self.scenes.get(self.active_scene_id)
        return None

    def get_scene(self, scene_id: str) -> Optional[WorkflowScene]:
        """获取场景"""
        return self.scenes.get(scene_id)

    def list_scenes(self, status: SceneStatus = None,
                   tags: List[str] = None) -> List[WorkflowScene]:
        """列出场景"""
        scenes = list(self.scenes.values())

        if status:
            scenes = [s for s in scenes if s.status == status]

        if tags:
            scenes = [s for s in scenes if any(t in s.tags for t in tags)]

        return sorted(scenes, key=lambda s: s.usage_count, reverse=True)

    def execute_scene(self, scene_id: str,
                     execute_callback: callable = None) -> Dict[str, Any]:
        """执行场景"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return {"success": False, "error": "场景不存在"}

        # 按顺序执行工作流（考虑依赖）
        enabled_workflows = [w for w in scene.workflows if w.enabled]
        sorted_workflows = self._sort_by_dependencies(enabled_workflows)

        results = []
        for wf in sorted_workflows:
            result = {
                "workflow_id": wf.workflow_id,
                "workflow_name": wf.workflow_name,
                "status": "pending"
            }

            try:
                # 如果有回调函数则执行
                if execute_callback:
                    execute_callback(wf.workflow_id)
                result["status"] = "success"
            except Exception as e:
                result["status"] = "failed"
                result["error"] = str(e)

            results.append(result)

        scene.usage_count += 1
        self._save_scenes()

        return {
            "success": True,
            "scene_id": scene_id,
            "scene_name": scene.name,
            "workflows_executed": len(results),
            "results": results
        }

    def _sort_by_dependencies(self, workflows: List[WorkflowRef]) -> List[WorkflowRef]:
        """根据依赖关系排序"""
        if not workflows:
            return []

        graph = WorkflowGraph(workflows)
        parallel_groups = graph.get_parallel_groups()

        sorted_wfs = []
        for group in parallel_groups:
            for wf_id in group:
                wf = next((w for w in workflows if w.workflow_id == wf_id), None)
                if wf:
                    sorted_wfs.append(wf)

        return sorted_wfs

    def get_scene_execution_order(self, scene_id: str) -> List[str]:
        """获取场景执行顺序"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return []

        sorted_workflows = self._sort_by_dependencies(scene.workflows)
        return [wf.workflow_name for wf in sorted_workflows]

    def enable_schedule(self, scene_id: str, schedule_time: str,
                       days: List[str]) -> bool:
        """启用定时"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False

        scene.schedule.enabled = True
        scene.schedule.time = schedule_time
        scene.schedule.days = days
        import time as time_module
        scene.updated_at = time_module.time()
        self._save_scenes()
        return True

    def disable_schedule(self, scene_id: str) -> bool:
        """禁用定时"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False

        scene.schedule.enabled = False
        scene.updated_at = time.time()
        self._save_scenes()
        return True

    def check_scheduled_scenes(self) -> List[WorkflowScene]:
        """检查需要执行的定时场景"""
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        current_day = now.strftime("%A").lower()

        scheduled = []
        for scene in self.scenes.values():
            if scene.schedule.enabled:
                if scene.schedule.time == current_time:
                    if current_day in scene.schedule.days or not scene.schedule.days:
                        scheduled.append(scene)

        return scheduled

    def export_scene(self, scene_id: str) -> str:
        """导出场景配置"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return ""
        return json.dumps(scene.to_dict(), ensure_ascii=False, indent=2)

    def import_scene(self, scene_json: str) -> bool:
        """导入场景配置"""
        try:
            data = json.loads(scene_json)
            # 重新生成 ID 避免冲突
            data["scene_id"] = f"scene_{int(time.time())}"
            data["created_at"] = time.time()
            data["updated_at"] = time.time()
            data["usage_count"] = 0

            scene = WorkflowScene(
                scene_id=data["scene_id"],
                name=data["name"],
                description=data.get("description", ""),
                icon=data.get("icon", "📦"),
                status=SceneStatus(data.get("status", "inactive")),
                workflows=[
                    WorkflowRef(**w) for w in data.get("workflows", [])
                ],
                schedule=Schedule(**data.get("schedule", {})),
                tags=data.get("tags", []),
                settings=data.get("settings", {}),
                annotations=data.get("annotations", {}),
                hotkeys=data.get("hotkeys", {})
            )

            self.scenes[scene.scene_id] = scene
            self._save_scenes()
            return True
        except Exception:
            return False

    def get_scene_statistics(self) -> Dict[str, Any]:
        """获取场景统计"""
        total_scenes = len(self.scenes)
        active_scenes = len([s for s in self.scenes.values()
                           if s.status == SceneStatus.ACTIVE])
        scheduled_scenes = len([s for s in self.scenes.values()
                               if s.schedule.enabled])

        # 使用最多的场景
        top_scenes = sorted(self.scenes.values(),
                          key=lambda s: s.usage_count,
                          reverse=True)[:5]

        return {
            "total_scenes": total_scenes,
            "active_scenes": active_scenes,
            "scheduled_scenes": scheduled_scenes,
            "top_scenes": [
                {"name": s.name, "usage_count": s.usage_count}
                for s in top_scenes
            ]
        }

    # ========== 新增功能方法 ==========

    def get_dependency_graph(self, scene_id: str, format: str = "ascii") -> str:
        """获取依赖图可视化"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return "Scene not found"

        graph = WorkflowGraph(scene.workflows)
        if format == "svg":
            return graph.to_svg()
        return graph.to_ascii()

    def get_parallel_execution_plan(self, scene_id: str) -> Dict[str, Any]:
        """获取并行执行计划"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return {"error": "Scene not found"}

        graph = WorkflowGraph(scene.workflows)
        parallel_groups = graph.get_parallel_groups()

        group_details = []
        for i, group in enumerate(parallel_groups):
            group_wfs = []
            for wf_id in group:
                wf = next((w for w in scene.workflows if w.workflow_id == wf_id), None)
                if wf:
                    group_wfs.append({
                        "workflow_id": wf.workflow_id,
                        "workflow_name": wf.workflow_name,
                        "delay": wf.delay,
                        "estimated_time": wf.cost.get("estimated_time", 1.0)
                    })
            group_details.append({
                "level": i,
                "can_run_parallel": len(group) > 1,
                "workflows": group_wfs
            })

        return {
            "scene_id": scene_id,
            "scene_name": scene.name,
            "parallel_groups": group_details,
            "total_levels": len(parallel_groups),
            "estimated_sequential_time": sum(w.delay + w.cost.get("estimated_time", 1.0) for w in scene.workflows),
            "estimated_parallel_time": sum(
                max(w.delay + w.cost.get("estimated_time", 1.0) for w in scene.workflows if w.workflow_id in g)
                for g in parallel_groups
            )
        }

    def get_critical_path(self, scene_id: str) -> Dict[str, Any]:
        """获取关键路径分析"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return {"error": "Scene not found"}

        graph = WorkflowGraph(scene.workflows)
        path, total_time = graph.get_critical_path()

        path_details = []
        for wf_id in path:
            wf = next((w for w in scene.workflows if w.workflow_id == wf_id), None)
            if wf:
                path_details.append({
                    "workflow_id": wf.workflow_id,
                    "workflow_name": wf.workflow_name,
                    "delay": wf.delay,
                    "estimated_time": wf.cost.get("estimated_time", 1.0)
                })

        return {
            "scene_id": scene_id,
            "scene_name": scene.name,
            "critical_path": path_details,
            "critical_path_length": len(path),
            "total_time_seconds": total_time,
            "bottleneck_step": path_details[-1] if path_details else None
        }

    def set_workflow_hotkey(self, scene_id: str, workflow_id: str, hotkey: str) -> bool:
        """设置工作流热键"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False

        for wf in scene.workflows:
            if wf.workflow_id == workflow_id:
                wf.hotkey = hotkey
                scene.hotkeys[workflow_id] = hotkey
                scene.updated_at = time.time()
                self._save_scenes()
                return True
        return False

    def set_scene_hotkey(self, scene_id: str, hotkey: str, workflow_id: str = None) -> bool:
        """设置场景热键（可指定触发特定工作流）"""
        if scene_id not in self.scenes:
            return False

        self.global_hotkeys[hotkey] = HotkeyBinding(
            hotkey=hotkey,
            scene_id=scene_id,
            workflow_id=workflow_id,
            description=f"Trigger {self.scenes[scene_id].name}"
        )
        self._save_scenes()
        return True

    def add_annotation(self, scene_id: str, workflow_id: str, note: str) -> bool:
        """添加工作流注解"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False

        for wf in scene.workflows:
            if wf.workflow_id == workflow_id:
                wf.annotation = note
                scene.annotations[workflow_id] = note
                scene.updated_at = time.time()
                self._save_scenes()
                return True
        return False

    def get_annotation(self, scene_id: str, workflow_id: str) -> Optional[str]:
        """获取工作流注解"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return None

        for wf in scene.workflows:
            if wf.workflow_id == workflow_id:
                return wf.annotation
        return None

    def compare_scenes(self, scene_id1: str, scene_id2: str) -> Dict[str, Any]:
        """对比两个场景"""
        scene1 = self.scenes.get(scene_id1)
        scene2 = self.scenes.get(scene_id2)

        if not scene1 or not scene2:
            return {"error": "One or both scenes not found"}

        return WorkflowComparator.compare_scenes(scene1, scene2)

    def get_recommendations(self, scene_id: str) -> Dict[str, Any]:
        """获取场景优化建议"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return {"error": "Scene not found"}

        return WorkflowAnalyzer.analyze_scene(scene)

    def estimate_cost(self, scene_id: str) -> Dict[str, Any]:
        """估算执行成本"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return {"error": "Scene not found"}

        return CostEstimator.estimate_scene(scene)

    def run_smoke_test(self, scene_id: str) -> Dict[str, Any]:
        """运行烟雾测试"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return {"error": "Scene not found"}

        tester = WorkflowTester()
        return tester.smoke_test(scene)

    def migrate_from_code(self, code: str, source_format: str = None) -> Dict[str, Any]:
        """从代码迁移"""
        if source_format:
            if source_format == "autoit":
                return MigrationTool.migrate_from_autoit(code)
            elif source_format == "selenium":
                return MigrationTool.migrate_from_selenium(code)
            elif source_format == "autohotkey":
                return MigrationTool.migrate_from_autohotkey(code)

        return MigrationTool.migrate(code)

    def apply_migrated_workflows(self, scene_id: str, migration_result: Dict) -> bool:
        """应用迁移的工作流到场景"""
        scene = self.scenes.get(scene_id)
        if not scene:
            return False

        config = migration_result.get("migrated_config", {})
        workflows = config.get("workflows", [])

        for w in workflows:
            wf = WorkflowRef(
                workflow_id=w["workflow_id"],
                workflow_name=w["workflow_name"],
                enabled=w.get("enabled", True),
                delay=w.get("delay", 0.0),
                order=w.get("order", 0),
                cost=w.get("cost", {"cpu": 0.1, "io": 0.1, "network": 0.0, "memory": 50}),
                annotation=w.get("annotation")
            )
            scene.workflows.append(wf)

        scene.updated_at = time.time()
        self._save_scenes()
        return True


def create_scene_manager(data_dir: str = "./data") -> WorkflowSceneManager:
    """创建场景管理器实例"""
    return WorkflowSceneManager(data_dir)


# 测试
if __name__ == "__main__":
    manager = create_scene_manager("./data")

    # 列出所有场景
    scenes = manager.list_scenes()
    print("=== 场景列表 ===")
    for s in scenes:
        print(f"{s.icon} {s.name} - {s.description}")
        print(f"   状态: {s.status.value}, 使用次数: {s.usage_count}")
        print(f"   工作流数: {len(s.workflows)}")
        if s.schedule.enabled:
            print(f"   定时: {s.schedule.time} {s.schedule.days}")
        print()

    # 统计
    stats = manager.get_scene_statistics()
    print("=== 场景统计 ===")
    print(f"总场景数: {stats['total_scenes']}")
    print(f"激活中: {stats['active_scenes']}")
    print(f"定时任务: {stats['scheduled_scenes']}")

    # 测试新功能
    print("\n=== 依赖图示例 (morning_routine) ===")
    print(manager.get_dependency_graph("morning_routine"))

    print("\n=== 关键路径分析 ===")
    critical = manager.get_critical_path("morning_routine")
    print(f"关键路径: {[w['workflow_name'] for w in critical['critical_path']]}")
    print(f"总时间: {critical['total_time_seconds']:.1f}s")

    print("\n=== 并行执行计划 ===")
    parallel = manager.get_parallel_execution_plan("morning_routine")
    for group in parallel['parallel_groups']:
        print(f"Level {group['level']}: {[w['workflow_name'] for w in group['workflows']]}")

    print("\n=== 成本估算 ===")
    cost = manager.estimate_cost("morning_routine")
    print(f"CPU: {cost['cpu_units']:.1f}, IO: {cost['io_operations']:.1f}, Memory: {cost['memory_peak_mb']:.1f}MB")
    print(f"预估时间: {cost['estimated_time_seconds']:.1f}s (优化后: {cost['optimized_time_seconds']:.1f}s)")

    print("\n=== 优化建议 ===")
    recs = manager.get_recommendations("morning_routine")
    for rec in recs.get('recommendations', [])[:3]:
        print(f"- {rec['suggestion']}")

    print("\n=== 烟雾测试 ===")
    test = manager.run_smoke_test("morning_routine")
    print(f"通过: {test['passed']}, 失败: {test['failed']}, 警告: {len(test['warnings'])}")

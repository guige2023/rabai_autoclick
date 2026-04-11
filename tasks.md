# RabAI AutoClick 开发任务

> 创建时间: 2026-04-11
> 最后更新: 2026-04-11 10:35

## 项目状态

- **分支**: main
- **与 origin/main 同步**: ✅ (已推送)
- **最新 commit**: `01918e4` - "fix: resolve test collection errors and add tasks.md"

---

## 测试状态

### 测试收集错误

| 文件 | 问题 | 状态 |
|------|------|------|
| `tests/test_workflow_monitoring.py` | 修复完成 (MetricCollector→WorkflowMonitoring, 缺失类=None) | ✅ 已修复 |
| `tests/test_workflow_webhooks.py` | 修复完成 (添加 httpx try/except) | ✅ 已修复 |
| `tests/test_workflow_prometheus.py` | KeyError: editable install MAPPING 机制问题 | 🔴 待解决 |
| `tests/test_workflow_monitoring.py` (运行) | 测试使用不存在的类 (AlertManager, HealthChecker 等) | 🔴 65 failed |
| `tests/test_actions.py` (运行) | 多种 Action 测试失败 | 🔴 44 failed |

### 测试验证

```
python3 -m pytest tests/test_core.py → 34 passed ✅
python3 -m pytest tests/test_workflow_monitoring.py → 3 passed, 65 failed
python3 -m pytest tests/test_workflow_prometheus.py → 收集错误 (KeyError)
```

---

## 待处理任务

### 高优先级

- [ ] **FIX-003**: 修复 `test_workflow_prometheus.py` 的 KeyError
  - 问题: pytest 收集时出现 `KeyError: 'rabai_autoclick.tests.test_workflow_prometheus'`
  - 根因: pytest assertion rewriting 与 editable install MAPPING 机制冲突
  - 方案: 调查 _EditableFinder.find_spec 中 pytest 如何触发错误

- [ ] **FIX-004**: 修复 `test_workflow_monitoring.py` 运行错误
  - 问题: 65 个测试使用不存在的类 (AlertManager, HealthChecker, SystemMonitor 等)
  - 方案: 在测试中将这些类设为 None 或删除相关测试类

- [ ] **FIX-005**: 修复 `test_actions.py` 测试失败
  - 问题: 44 个测试失败 (Action 类实现问题)
  - 需要: 分析具体失败原因

### 中优先级

- [ ] **ANALYSIS-001**: 分析 `test_workflow_monitoring.py` 中测试引用的类为何不存在
  - AlertManager, HealthChecker, SystemMonitor, WorkflowMonitor, MonitoringDashboard, MonitoringEngine
  - 这些类在测试中被引用但不在 `workflow_monitoring.py` 中

- [ ] **TEST-001**: 恢复完整测试套件运行
  - 目标: 所有测试收集无错误，基本测试通过

---

## 已完成任务

### 2026-04-11 10:35

- ✅ 项目结构分析
- ✅ Git 状态检查
- ✅ 测试运行分析 (发现 3 个收集错误)
- ✅ 创建 tasks.md 文档
- ✅ 修复 test_workflow_monitoring.py 收集错误
- ✅ 修复 test_workflow_webhooks.py 收集错误
- ✅ 创建 tests/__init__.py
- ✅ 提交修改: `01918e4`
- ✅ 推送到 origin/main

---

## Git 日志 (最近 5 commits)

```
01918e4 fix: resolve test collection errors and add tasks.md
c3b6c63 tests: add comprehensive tests for workflow_aws_sagemakerml...
7d34846 feat(aws-rekognition): add Amazon Rekognition with face detection...
2b0531a feat(aws-polly): add Amazon Polly with text-to-speech...
1309c35 feat(aws-comprehend): add Amazon Comprehend...
```

---

## 下一步行动

1. 调查 `test_workflow_prometheus.py` KeyError (pytest assertion rewriting)
2. 分析 `test_workflow_monitoring.py` 测试引用的缺失类
3. 修复 `test_actions.py` 的 44 个测试失败
4. 运行完整测试套件验证

---

## 技术笔记

### editable install MAPPING 机制

`_EditableFinder.find_spec` 中，如果 `fullname.startswith('tests.')`，则：
- `parent = 'tests'`, `parent not in MAPPING` → 返回 None
- 这导致 pytest 的 assertion rewriting hook 收到 None，无法正常 import

### prometheus_client 安装

```bash
pip3 install prometheus_client
```

测试文件可以直接 import `src.workflow_prometheus`，但 pytest 收集时报 KeyError。

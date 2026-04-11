# RabAI AutoClick 开发任务

> 创建时间: 2026-04-11
> 最后更新: 2026-04-11 12:30

## 项目状态

- **分支**: main
- **与 origin/main 同步**: ✅
- **最新 commit**: `0cfa44c`

---

## 测试状态总览

**核心文件**: 340 passed, 12 failed, 3 skipped

### ✅ 已完全解决 (207 + 64 + 38 = 309 passed)

| 文件 | 结果 | 关键修复 |
|------|------|----------|
| `test_core.py` | ✅ 34 passed | - |
| `test_workflow_prometheus.py` | ✅ 61 passed | `--import-mode=importlib` + MockPrometheusClient |
| `test_workflow_monitoring.py` | ✅ 68 passed | 缺失类 stub |
| `test_actions.py` | ✅ 44 passed, 3 skipped | `time.sleep` mock |
| `test_error_handler.py` | ✅ 64 passed | `Lock→RLock` + `time.sleep` mock + `recent_timestamps` + Template |
| `test_src.py` | ✅ 38 passed | PipelineValidationError.message + TestPipelineMode setUp |

### 🔴 仍有问题

| 文件 | 问题 | 失败数 |
|------|------|--------|
| `test_utils.py` (部分) | `TestAuditLogger` 日志持久化导致数据污染 | 5 failed |
| `test_utils.py` (部分) | `TestWorkflowSigner.verify()` 递归过滤 bug | 1 failed |
| `test_utils.py` (部分) | `cryptography` 库未安装 | 6 failed |

> 注: `test_utils.py` 的失败涉及实现 bug 或环境依赖，非测试代码问题。

---

## Git 提交历史 (今天)

```
0cfa44c fix: resolve error_handler deadlock, pipeline exceptions, and test patches
2edb9bd fix: resolve test collection, deadlock, and import errors
01918e4 feat: add comprehensive tests for workflow_aws_sagemaker_ml...
```

---

## 关键 Bug 修复笔记

### 1. `ErrorPatternDetector` 死锁 (FIX-006)
- **问题**: `Lock()` 不可重入，`record_occurrence` 持有锁时调用 `register_pattern` 再获取同一把锁
- **修复**: `Lock()` → `RLock()`
- **影响文件**: `src/error_handler.py`

### 2. `WorkflowErrorHandler.record_error` 7 秒延迟
- **问题**: `_retry_network_connection` 真实尝试 TCP 连接 + `time.sleep` 重试，最长 7 秒
- **修复**: `TestWorkflowErrorHandler.setUpClass` 中 patch `src.error_handler.time.sleep`
- **影响文件**: `tests/test_error_handler.py`

### 3. `ErrorDashboardGenerator` 模板格式化 bug
- **问题**: `_template.format()` 中 CSS 的 `{` `}` 被 Python 解释为占位符
- **修复**: 改用 `string.Template` + `safe_substitute()`
- **影响文件**: `src/error_handler.py`

### 4. `ErrorPattern.recent_timestamps` 缺失
- **问题**: `record_occurrence()` 访问 `pattern.recent_timestamps` 但 dataclass 未定义
- **修复**: 添加 `recent_timestamps: List[float] = field(default_factory=list)`
- **影响文件**: `src/error_handler.py`

### 5. pytest `--import-mode=importlib`
- **问题**: pytest 默认 `importmode=prepend` 导致 namespace package 冲突
- **修复**: `pyproject.toml` addopts 添加 `--import-mode=importlib`

### 6. `PipelineValidationError` / `PipelineExecuteError` 缺少 `message`
- **问题**: `Exception.__init__(message)` 后 `self.message` 未显式存储
- **修复**: 添加 `self.message = message`
- **影响文件**: `src/pipeline_mode.py`

---

## 待处理任务

- [ ] 修复 `test_utils.py` `TestAuditLogger` 数据污染 (在 `setUp` 中清空或隔离日志文件)
- [ ] 修复 `test_utils.py` `TestWorkflowSigner.verify()` 递归过滤 bug
- [ ] 考虑安装 `cryptography` 库解决 6 个 `WorkflowCrypto` 测试

---

## 快速命令

```bash
# 运行核心测试
cd ~/my_project/rabai_autoclick
python3 -m pytest tests/test_core.py tests/test_workflow_prometheus.py tests/test_workflow_monitoring.py tests/test_actions.py tests/test_error_handler.py tests/test_src.py -q --tb=no

# 运行全部测试 (部分文件有问题)
python3 -m pytest tests/ -q --tb=no
```

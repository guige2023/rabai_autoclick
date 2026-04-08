"""Process monitoring action module for RabAI AutoClick.

Provides process management operations:
- ProcessListAction: List running processes
- ProcessStartAction: Start a new process
- ProcessStopAction: Stop a process
- ProcessRestartAction: Restart a process
- ProcessStatsAction: Get process statistics
- ProcessMonitorAction: Monitor process health
- ProcessLogAction: Capture process logs
- ProcessPriorityAction: Set process priority
"""

import os
import psutil
import signal
import subprocess
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ProcessListAction(BaseAction):
    """List running processes."""
    action_type = "process_list"
    display_name = "进程列表"
    description = "列出运行中的进程"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            filter_name = params.get("filter", "")
            limit = params.get("limit", 50)
            
            processes = []
            for proc in psutil.process_iter(["pid", "name", "cpu_percent", "memory_percent"]):
                try:
                    info = proc.info
                    if filter_name and filter_name.lower() not in info["name"].lower():
                        continue
                    processes.append(info)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            
            processes.sort(key=lambda x: x.get("cpu_percent", 0), reverse=True)
            processes = processes[:limit]
            
            return ActionResult(
                success=True,
                message=f"Found {len(processes)} processes",
                data={"processes": processes, "count": len(processes)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Process list failed: {str(e)}")


class ProcessStartAction(BaseAction):
    """Start a new process."""
    action_type = "process_start"
    display_name = "启动进程"
    description = "启动新进程"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            command = params.get("command", "")
            args = params.get("args", [])
            cwd = params.get("cwd", None)
            env = params.get("env", {})
            detached = params.get("detached", False)
            
            if not command:
                return ActionResult(success=False, message="command is required")
            
            full_command = [command] + args
            
            if detached:
                if sys.platform == "darwin":
                    subprocess.Popen(full_command, cwd=cwd, env={**os.environ, **env}, 
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                else:
                    subprocess.Popen(full_command, cwd=cwd, env={**os.environ, **env},
                                   start_new_session=True)
                pid = 0
            else:
                proc = subprocess.Popen(full_command, cwd=cwd, env={**os.environ, **env})
                pid = proc.pid
            
            return ActionResult(
                success=True,
                message=f"Started process: {command}",
                data={"command": command, "pid": pid, "detached": detached}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Process start failed: {str(e)}")


class ProcessStopAction(BaseAction):
    """Stop a process."""
    action_type = "process_stop"
    display_name = "停止进程"
    description = "停止运行中的进程"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pid = params.get("pid")
            process_name = params.get("name", "")
            force = params.get("force", False)
            
            if not pid and not process_name:
                return ActionResult(success=False, message="pid or name required")
            
            if process_name:
                for proc in psutil.process_iter(["pid", "name"]):
                    try:
                        if process_name.lower() in proc.info["name"].lower():
                            pid = proc.info["pid"]
                            break
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
            
            if pid:
                try:
                    proc = psutil.Process(pid)
                    if force:
                        proc.kill()
                    else:
                        proc.terminate()
                    proc.wait(timeout=5)
                except psutil.TimeoutExpired:
                    return ActionResult(success=False, message=f"Process {pid} did not stop gracefully")
                except psutil.NoSuchProcess:
                    pass
            
            return ActionResult(
                success=True,
                message=f"Stopped process {pid or process_name}",
                data={"pid": pid, "name": process_name, "force": force}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Process stop failed: {str(e)}")


class ProcessRestartAction(BaseAction):
    """Restart a process."""
    action_type = "process_restart"
    display_name = "重启进程"
    description = "重启进程"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pid = params.get("pid")
            process_name = params.get("name", "")
            command = params.get("command", "")
            
            if not command and not process_name:
                return ActionResult(success=False, message="command or name required for restart")
            
            stop_result = self._stop_process(pid, process_name)
            
            time.sleep(1)
            
            start_result = self._start_process(command)
            
            return ActionResult(
                success=True,
                message=f"Restarted process",
                data={"stopped": stop_result, "started": start_result}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Process restart failed: {str(e)}")

    def _stop_process(self, pid, name):
        try:
            if name:
                for proc in psutil.process_iter(["pid", "name"]):
                    if name.lower() in proc.info["name"].lower():
                        pid = proc.info["pid"]
                        break
            if pid:
                proc = psutil.Process(pid)
                proc.terminate()
                return {"pid": pid, "status": "terminated"}
        except:
            pass
        return {"status": "not_found"}

    def _start_process(self, command):
        try:
            proc = subprocess.Popen(command, shell=True)
            return {"pid": proc.pid, "command": command}
        except Exception as e:
            return {"error": str(e)}


class ProcessStatsAction(BaseAction):
    """Get process statistics."""
    action_type = "process_stats"
    display_name = "进程统计"
    description = "获取进程统计信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pid = params.get("pid")
            
            if not pid:
                return ActionResult(success=False, message="pid is required")
            
            try:
                proc = psutil.Process(pid)
                
                stats = {
                    "pid": pid,
                    "name": proc.name(),
                    "status": proc.status(),
                    "cpu_percent": proc.cpu_percent(interval=0.1),
                    "memory_percent": proc.memory_percent(),
                    "memory_info": proc.memory_info()._asdict(),
                    "num_threads": proc.num_threads(),
                    "create_time": datetime.fromtimestamp(proc.create_time()).isoformat(),
                }
                
                try:
                    stats["connections"] = len(proc.connections())
                except psutil.AccessDenied:
                    stats["connections"] = None
                
                return ActionResult(
                    success=True,
                    message=f"Stats for PID {pid}",
                    data=stats
                )
            except psutil.NoSuchProcess:
                return ActionResult(success=False, message=f"Process {pid} not found")
        except Exception as e:
            return ActionResult(success=False, message=f"Process stats failed: {str(e)}")


class ProcessMonitorAction(BaseAction):
    """Monitor process health."""
    action_type = "process_monitor"
    display_name = "进程监控"
    description = "监控进程健康状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pid = params.get("pid")
            threshold_cpu = params.get("threshold_cpu", 80)
            threshold_mem = params.get("threshold_mem", 90)
            duration = params.get("duration", 60)
            
            if not pid:
                return ActionResult(success=False, message="pid is required")
            
            start_time = time.time()
            violations = []
            
            while time.time() - start_time < duration:
                try:
                    proc = psutil.Process(pid)
                    cpu = proc.cpu_percent(interval=1)
                    mem = proc.memory_percent()
                    
                    if cpu > threshold_cpu:
                        violations.append({"time": time.time(), "type": "cpu", "value": cpu})
                    if mem > threshold_mem:
                        violations.append({"time": time.time(), "type": "memory", "value": mem})
                except psutil.NoSuchProcess:
                    return ActionResult(success=False, message=f"Process {pid} died")
                
                time.sleep(1)
            
            return ActionResult(
                success=True,
                message=f"Monitored for {duration}s, {len(violations)} violations",
                data={"violations": violations, "duration": duration}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Process monitor failed: {str(e)}")


class ProcessLogAction(BaseAction):
    """Capture process logs."""
    action_type = "process_log"
    display_name = "进程日志"
    description = "捕获进程日志"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pid = params.get("pid")
            lines = params.get("lines", 100)
            stdout = params.get("stdout", True)
            stderr = params.get("stderr", True)
            
            if not pid:
                return ActionResult(success=False, message="pid is required")
            
            try:
                proc = psutil.Process(pid)
                log_output = []
                
                try:
                    with open(proc.stdout.fileno() if stdout else None) as f:
                        log_output.extend(f.readlines()[-lines:])
                except:
                    pass
                
                return ActionResult(
                    success=True,
                    message=f"Captured logs from PID {pid}",
                    data={"pid": pid, "lines": len(log_output), "logs": log_output[-10:]}
                )
            except psutil.NoSuchProcess:
                return ActionResult(success=False, message=f"Process {pid} not found")
        except Exception as e:
            return ActionResult(success=False, message=f"Process log failed: {str(e)}")


class ProcessPriorityAction(BaseAction):
    """Set process priority."""
    action_type = "process_priority"
    display_name = "进程优先级"
    description = "设置进程优先级"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pid = params.get("pid")
            priority = params.get("priority", "normal")
            
            if not pid:
                return ActionResult(success=False, message="pid is required")
            
            try:
                proc = psutil.Process(pid)
                
                if sys.platform == "darwin":
                    if priority == "high":
                        os.nice(-10)
                    elif priority == "low":
                        os.nice(10)
                    else:
                        os.nice(0)
                else:
                    if priority == "high":
                        proc.nice(psutil.HIGH_PRIORITY_CLASS)
                    elif priority == "low":
                        proc.nice(psutil.IDLE_PRIORITY_CLASS)
                    else:
                        proc.nice(psutil.NORMAL_PRIORITY_CLASS)
                
                return ActionResult(
                    success=True,
                    message=f"Set priority to {priority} for PID {pid}",
                    data={"pid": pid, "priority": priority}
                )
            except psutil.NoSuchProcess:
                return ActionResult(success=False, message=f"Process {pid} not found")
        except Exception as e:
            return ActionResult(success=False, message=f"Process priority failed: {str(e)}")

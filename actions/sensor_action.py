"""Sensor action module for RabAI AutoClick.

Provides sensor operations:
- SensorBatteryAction: Get battery status
- SensorCpuTempAction: Get CPU temperature
- SensorFanSpeedAction: Get fan speed
- SensorBrightnessAction: Get screen brightness
- SensorPowerAction: Get power supply status
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SensorBatteryAction(BaseAction):
    """Get battery status."""
    action_type = "sensor_battery"
    display_name = "电池状态"
    description = "获取电池状态"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute battery status.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with battery status.
        """
        output_var = params.get('output_var', 'battery_status')

        try:
            import psutil

            battery = psutil.sensors_battery()
            if battery is None:
                return ActionResult(
                    success=False,
                    message="获取电池状态失败: 未找到电池"
                )

            result = {
                'percent': battery.percent,
                'seconds_left': battery.secsleft,
                'power_plugged': battery.power_plugged,
                'is_charging': battery.percent > 100
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"电池状态: {result['percent']}% {'充电中' if result['power_plugged'] else '使用电池'}",
                data={
                    'battery': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取电池状态失败: 未安装psutil库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取电池状态失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'battery_status'}


class SensorCpuTempAction(BaseAction):
    """Get CPU temperature."""
    action_type = "sensor_cpu_temp"
    display_name = "CPU温度"
    description = "获取CPU温度"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CPU temperature.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with CPU temperature.
        """
        output_var = params.get('output_var', 'cpu_temp')

        try:
            import psutil

            temps = psutil.sensors_temperatures()
            if not temps:
                return ActionResult(
                    success=False,
                    message="获取CPU温度失败: 未找到温度传感器"
                )

            result = {}
            for name, entries in temps.items():
                result[name] = [{
                    'label': entry.label,
                    'current': entry.current,
                    'high': entry.high,
                    'critical': entry.critical
                } for entry in entries]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"CPU温度",
                data={
                    'temperatures': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取CPU温度失败: 未安装psutil库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取CPU温度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cpu_temp'}


class SensorFanSpeedAction(BaseAction):
    """Get fan speed."""
    action_type = "sensor_fan_speed"
    display_name = "风扇速度"
    description = "获取风扇速度"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute fan speed.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with fan speed.
        """
        output_var = params.get('output_var', 'fan_speed')

        try:
            import psutil

            fans = psutil.sensors_fans()
            if not fans:
                return ActionResult(
                    success=False,
                    message="获取风扇速度失败: 未找到风扇传感器"
                )

            result = {}
            for name, entries in fans.items():
                result[name] = [{
                    'label': entry.label,
                    'current': entry.current,
                    'high': entry.high
                } for entry in entries]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"风扇速度",
                data={
                    'fans': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取风扇速度失败: 未安装psutil库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取风扇速度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'fan_speed'}


class SensorBrightnessAction(BaseAction):
    """Get screen brightness."""
    action_type = "sensor_brightness"
    display_name = "屏幕亮度"
    description = "获取屏幕亮度"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute brightness.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with brightness.
        """
        output_var = params.get('output_var', 'brightness')

        try:
            result = None
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"屏幕亮度: 不支持",
                data={
                    'brightness': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取屏幕亮度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'brightness'}


class SensorPowerAction(BaseAction):
    """Get power supply status."""
    action_type = "sensor_power"
    display_name = "电源状态"
    description = "获取电源供应状态"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute power supply.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with power supply status.
        """
        output_var = params.get('output_var', 'power_status')

        try:
            import psutil

            power = psutil.sensors_power()
            if not power:
                return ActionResult(
                    success=False,
                    message="获取电源状态失败: 未找到电源传感器"
                )

            result = {}
            for name, entries in power.items():
                result[name] = [{
                    'label': entry.label,
                    'current': entry.current,
                    'power': entry.power,
                    'energy': entry.energy
                } for entry in entries]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"电源状态",
                data={
                    'power': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取电源状态失败: 未安装psutil库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取电源状态失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'power_status'}
"""Convert5 action module for RabAI AutoClick.

Provides additional conversion operations:
- ConvertTemperatureAction: Temperature conversion
- ConvertWeightAction: Weight conversion
- ConvertDistanceAction: Distance conversion
- ConvertSpeedAction: Speed conversion
- ConvertVolumeAction: Volume conversion
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConvertTemperatureAction(BaseAction):
    """Temperature conversion."""
    action_type = "convert5_temperature"
    display_name = "温度转换"
    description = "温度单位转换"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute temperature conversion.

        Args:
            context: Execution context.
            params: Dict with value, from_unit, to_unit, output_var.

        Returns:
            ActionResult with converted temperature.
        """
        value = params.get('value', 0)
        from_unit = params.get('from_unit', 'C')
        to_unit = params.get('to_unit', 'F')
        output_var = params.get('output_var', 'converted_temperature')

        try:
            resolved_value = float(context.resolve_value(value)) if value else 0
            resolved_from = context.resolve_value(from_unit)
            resolved_to = context.resolve_value(to_unit)

            # Convert to Celsius first
            if resolved_from == 'C':
                celsius = resolved_value
            elif resolved_from == 'F':
                celsius = (resolved_value - 32) * 5 / 9
            elif resolved_from == 'K':
                celsius = resolved_value - 273.15
            else:
                celsius = resolved_value

            # Convert from Celsius to target
            if resolved_to == 'C':
                result = celsius
            elif resolved_to == 'F':
                result = celsius * 9 / 5 + 32
            elif resolved_to == 'K':
                result = celsius + 273.15
            else:
                result = celsius

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"温度转换: {resolved_value}{resolved_from} = {result:.2f}{resolved_to}",
                data={
                    'value': resolved_value,
                    'from_unit': resolved_from,
                    'to_unit': resolved_to,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"温度转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'from_unit', 'to_unit']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'converted_temperature'}


class ConvertWeightAction(BaseAction):
    """Weight conversion."""
    action_type = "convert5_weight"
    display_name = "重量转换"
    description = "重量单位转换"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute weight conversion.

        Args:
            context: Execution context.
            params: Dict with value, from_unit, to_unit, output_var.

        Returns:
            ActionResult with converted weight.
        """
        value = params.get('value', 0)
        from_unit = params.get('from_unit', 'kg')
        to_unit = params.get('to_unit', 'lb')
        output_var = params.get('output_var', 'converted_weight')

        try:
            resolved_value = float(context.resolve_value(value)) if value else 0
            resolved_from = context.resolve_value(from_unit)
            resolved_to = context.resolve_value(to_unit)

            # Convert to kg first
            to_kg = {
                'kg': 1,
                'g': 0.001,
                'mg': 0.000001,
                'lb': 0.453592,
                'oz': 0.0283495,
                't': 1000
            }

            kg = resolved_value * to_kg.get(resolved_from, 1)
            result = kg / to_kg.get(resolved_to, 1)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"重量转换: {resolved_value}{resolved_from} = {result:.2f}{resolved_to}",
                data={
                    'value': resolved_value,
                    'from_unit': resolved_from,
                    'to_unit': resolved_to,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"重量转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'from_unit', 'to_unit']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'converted_weight'}


class ConvertDistanceAction(BaseAction):
    """Distance conversion."""
    action_type = "convert5_distance"
    display_name = "距离转换"
    description = "距离单位转换"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute distance conversion.

        Args:
            context: Execution context.
            params: Dict with value, from_unit, to_unit, output_var.

        Returns:
            ActionResult with converted distance.
        """
        value = params.get('value', 0)
        from_unit = params.get('from_unit', 'm')
        to_unit = params.get('to_unit', 'ft')
        output_var = params.get('output_var', 'converted_distance')

        try:
            resolved_value = float(context.resolve_value(value)) if value else 0
            resolved_from = context.resolve_value(from_unit)
            resolved_to = context.resolve_value(to_unit)

            # Convert to meters first
            to_meters = {
                'm': 1,
                'km': 1000,
                'cm': 0.01,
                'mm': 0.001,
                'mi': 1609.344,
                'ft': 0.3048,
                'in': 0.0254,
                'yd': 0.9144
            }

            meters = resolved_value * to_meters.get(resolved_from, 1)
            result = meters / to_meters.get(resolved_to, 1)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"距离转换: {resolved_value}{resolved_from} = {result:.2f}{resolved_to}",
                data={
                    'value': resolved_value,
                    'from_unit': resolved_from,
                    'to_unit': resolved_to,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"距离转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'from_unit', 'to_unit']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'converted_distance'}


class ConvertSpeedAction(BaseAction):
    """Speed conversion."""
    action_type = "convert5_speed"
    display_name = "速度转换"
    description = "速度单位转换"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute speed conversion.

        Args:
            context: Execution context.
            params: Dict with value, from_unit, to_unit, output_var.

        Returns:
            ActionResult with converted speed.
        """
        value = params.get('value', 0)
        from_unit = params.get('from_unit', 'kmh')
        to_unit = params.get('to_unit', 'mph')
        output_var = params.get('output_var', 'converted_speed')

        try:
            resolved_value = float(context.resolve_value(value)) if value else 0
            resolved_from = context.resolve_value(from_unit)
            resolved_to = context.resolve_value(to_unit)

            # Convert to m/s first
            to_ms = {
                'ms': 1,
                'kmh': 0.277778,
                'mph': 0.44704,
                'knot': 0.514444,
                'fts': 0.3048
            }

            ms = resolved_value * to_ms.get(resolved_from, 1)
            result = ms / to_ms.get(resolved_to, 1)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"速度转换: {resolved_value}{resolved_from} = {result:.2f}{resolved_to}",
                data={
                    'value': resolved_value,
                    'from_unit': resolved_from,
                    'to_unit': resolved_to,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"速度转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'from_unit', 'to_unit']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'converted_speed'}


class ConvertVolumeAction(BaseAction):
    """Volume conversion."""
    action_type = "convert5_volume"
    display_name = "体积转换"
    description = "体积单位转换"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute volume conversion.

        Args:
            context: Execution context.
            params: Dict with value, from_unit, to_unit, output_var.

        Returns:
            ActionResult with converted volume.
        """
        value = params.get('value', 0)
        from_unit = params.get('from_unit', 'L')
        to_unit = params.get('to_unit', 'gal')
        output_var = params.get('output_var', 'converted_volume')

        try:
            resolved_value = float(context.resolve_value(value)) if value else 0
            resolved_from = context.resolve_value(from_unit)
            resolved_to = context.resolve_value(to_unit)

            # Convert to liters first
            to_liters = {
                'L': 1,
                'ml': 0.001,
                'gal': 3.78541,
                'qt': 0.946353,
                'pt': 0.473176,
                'cup': 0.236588,
                'floz': 0.0295735
            }

            liters = resolved_value * to_liters.get(resolved_from, 1)
            result = liters / to_liters.get(resolved_to, 1)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"体积转换: {resolved_value}{resolved_from} = {result:.2f}{resolved_to}",
                data={
                    'value': resolved_value,
                    'from_unit': resolved_from,
                    'to_unit': resolved_to,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"体积转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'from_unit', 'to_unit']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'converted_volume'}
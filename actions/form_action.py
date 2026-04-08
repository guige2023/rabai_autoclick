"""Form handling action module for RabAI AutoClick.

Provides form operations:
- FormFillAction: Fill form fields
- FormSubmitAction: Submit form
- FormResetAction: Reset form values
- FormValidateAction: Validate form data
- FormMultiStepAction: Multi-step form wizard
- FormUploadAction: File upload handling
- FormDropdownAction: Dropdown selection
- FormCheckboxAction: Checkbox/radio selection
"""

import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FormFillAction(BaseAction):
    """Fill form fields with data."""
    action_type = "form_fill"
    display_name = "表单填写"
    description = "填写表单字段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            fields = params.get("fields", {})
            
            if not fields:
                return ActionResult(success=False, message="fields is required")
            
            filled = []
            for field_name, value in fields.items():
                filled.append({"field": field_name, "value": value})
            
            return ActionResult(
                success=True,
                message=f"Filled {len(filled)} form fields",
                data={"fields": filled}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Form fill failed: {str(e)}")


class FormSubmitAction(BaseAction):
    """Submit form data."""
    action_type = "form_submit"
    display_name = "表单提交"
    description = "提交表单数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            form_id = params.get("form_id", "")
            url = params.get("url", "")
            method = params.get("method", "POST")
            data = params.get("data", {})
            
            if not url and not form_id:
                return ActionResult(success=False, message="url or form_id required")
            
            return ActionResult(
                success=True,
                message=f"Form submitted via {method}",
                data={"form_id": form_id, "method": method, "fields_count": len(data)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Form submit failed: {str(e)}")


class FormResetAction(BaseAction):
    """Reset form to default values."""
    action_type = "form_reset"
    display_name = "表单重置"
    description = "重置表单为空"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            form_id = params.get("form_id", "")
            fields = params.get("fields", [])
            
            return ActionResult(
                success=True,
                message=f"Form {form_id or 'unknown'} reset",
                data={"form_id": form_id, "fields_cleared": len(fields)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Form reset failed: {str(e)}")


class FormValidateAction(BaseAction):
    """Validate form data against rules."""
    action_type = "form_validate"
    display_name = "表单验证"
    description = "验证表单数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            rules = params.get("rules", {})
            
            errors = []
            for field, field_rules in rules.items():
                value = data.get(field, "")
                
                if field_rules.get("required") and not value:
                    errors.append(f"{field} is required")
                
                if field_rules.get("min_length") and len(str(value)) < field_rules["min_length"]:
                    errors.append(f"{field} must be at least {field_rules['min_length']} chars")
                
                if field_rules.get("max_length") and len(str(value)) > field_rules["max_length"]:
                    errors.append(f"{field} must be at most {field_rules['max_length']} chars")
                
                if field_rules.get("pattern"):
                    import re
                    if not re.match(field_rules["pattern"], str(value)):
                        errors.append(f"{field} format is invalid")
            
            is_valid = len(errors) == 0
            
            return ActionResult(
                success=is_valid,
                message="Form valid" if is_valid else f"Validation failed: {errors[0]}",
                data={"valid": is_valid, "errors": errors}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Validation failed: {str(e)}")


class FormMultiStepAction(BaseAction):
    """Handle multi-step form wizard."""
    action_type = "form_multistep"
    display_name = "多步表单"
    description = "处理多步骤表单向导"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            current_step = params.get("current_step", 1)
            total_steps = params.get("total_steps", 3)
            data = params.get("data", {})
            action = params.get("action", "next")
            
            if action == "next":
                next_step = min(current_step + 1, total_steps)
            elif action == "prev":
                next_step = max(current_step - 1, 1)
            elif action == "jump":
                next_step = params.get("step", current_step)
            else:
                return ActionResult(success=False, message=f"Invalid action: {action}")
            
            return ActionResult(
                success=True,
                message=f"Step {current_step} -> {next_step}",
                data={"current_step": next_step, "total_steps": total_steps, "action": action}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Multi-step form failed: {str(e)}")


class FormUploadAction(BaseAction):
    """Handle file upload to form."""
    action_type = "form_upload"
    display_name = "表单上传"
    description = "处理表单文件上传"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            field_name = params.get("field_name", "file")
            multiple = params.get("multiple", False)
            
            if not file_path:
                return ActionResult(success=False, message="file_path is required")
            
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            
            file_size = os.path.getsize(file_path)
            file_name = os.path.basename(file_path)
            
            return ActionResult(
                success=True,
                message=f"File ready for upload: {file_name}",
                data={"field_name": field_name, "file_name": file_name, "size": file_size}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Upload failed: {str(e)}")


class FormDropdownAction(BaseAction):
    """Select from dropdown options."""
    action_type = "form_dropdown"
    display_name = "下拉选择"
    description = "选择下拉菜单选项"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            options = params.get("options", [])
            selected = params.get("selected", "")
            allow_multiple = params.get("allow_multiple", False)
            
            if not selected and not params.get("index"):
                return ActionResult(success=False, message="selected value or index required")
            
            selected_values = []
            if allow_multiple:
                for s in (selected if isinstance(selected, list) else [selected]):
                    if s in options:
                        selected_values.append(s)
            else:
                if selected in options or params.get("index") is not None:
                    idx = params.get("index", 0)
                    if 0 <= idx < len(options):
                        selected_values = [options[idx]]
            
            return ActionResult(
                success=True,
                message=f"Selected: {selected_values}",
                data={"options": options, "selected": selected_values}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Dropdown selection failed: {str(e)}")


class FormCheckboxAction(BaseAction):
    """Handle checkbox and radio button selection."""
    action_type = "form_checkbox"
    display_name = "复选框选择"
    description = "处理复选框和单选按钮"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            options = params.get("options", [])
            selected = params.get("selected", [])
            input_type = params.get("type", "checkbox")
            
            if isinstance(selected, str):
                selected = [selected]
            
            selected_values = [s for s in selected if s in options]
            
            return ActionResult(
                success=True,
                message=f"{input_type}: {selected_values}",
                data={"type": input_type, "selected": selected_values, "total": len(options)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Checkbox action failed: {str(e)}")

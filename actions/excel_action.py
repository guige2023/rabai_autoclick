"""Excel file action module for RabAI AutoClick.

Provides Excel operations:
- ExcelReadAction: Read Excel file into DataFrame
- ExcelWriteAction: Write data to Excel file
- ExcelAppendAction: Append to existing Excel
- ExcelSheetListAction: List sheet names
- ExcelReadSheetAction: Read specific sheet
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ExcelReadAction(BaseAction):
    """Read Excel file."""
    action_type = "excel_read"
    display_name = "Excel读取"
    description = "读取Excel文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Excel read."""
        file_path = params.get('file_path', '')
        sheet_name = params.get('sheet_name', 0)
        header = params.get('header', 0)  # row number for header, None for no header
        output_var = params.get('output_var', 'excel_data')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import pandas as pd

            resolved_path = context.resolve_value(file_path) if context else file_path
            resolved_sheet = context.resolve_value(sheet_name) if context else sheet_name

            df = pd.read_excel(resolved_path, sheet_name=resolved_sheet, header=header)

            result = {
                'shape': df.shape,
                'columns': list(df.columns),
                'sheet_name': resolved_sheet,
            }
            if context:
                context.set(output_var, df)
                context.set(f"{output_var}_info", result)
            return ActionResult(success=True, message=f"Read Excel: {df.shape[0]} rows", data=result)
        except ImportError:
            return ActionResult(success=False, message="pandas or openpyxl not installed")
        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"Excel read error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'sheet_name': 0, 'header': 0, 'output_var': 'excel_data'}


class ExcelWriteAction(BaseAction):
    """Write data to Excel file."""
    action_type = "excel_write"
    display_name = "Excel写入"
    description = "写入Excel文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Excel write."""
        file_path = params.get('file_path', '')
        data = params.get('data', None)
        dataframe_var = params.get('dataframe_var', None)
        sheet_name = params.get('sheet_name', 'Sheet1')
        index = params.get('index', False)
        output_var = params.get('output_var', 'excel_write_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")
        if not data and not dataframe_var:
            return ActionResult(success=False, message="data or dataframe_var is required")

        try:
            import pandas as pd

            resolved_path = context.resolve_value(file_path) if context else file_path
            _os.makedirs(_os.path.dirname(resolved_path) or '.', exist_ok=True)

            if dataframe_var:
                df = context.resolve_value(dataframe_var) if context else None
                if df is None:
                    df = context.resolve_value(dataframe_var)
            else:
                resolved_data = context.resolve_value(data) if context else data
                df = pd.DataFrame(resolved_data)

            if isinstance(df, pd.DataFrame):
                df.to_excel(resolved_path, sheet_name=sheet_name, index=index)
            else:
                return ActionResult(success=False, message="data must be convertible to DataFrame")

            result = {'output_path': resolved_path, 'rows': len(df), 'sheet': sheet_name}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Written {len(df)} rows to {resolved_path}", data=result)
        except ImportError:
            return ActionResult(success=False, message="pandas or openpyxl not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Excel write error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'data': None, 'dataframe_var': None, 'sheet_name': 'Sheet1', 'index': False, 'output_var': 'excel_write_result'}


class ExcelMultiSheetAction(BaseAction):
    """Write multiple sheets to Excel."""
    action_type = "excel_multi_sheet"
    display_name = "Excel多Sheet写入"
    description = "写入多个Sheet到Excel"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute multi-sheet Excel write."""
        file_path = params.get('file_path', '')
        sheets = params.get('sheets', [])  # [{name, data}, ...]
        output_var = params.get('output_var', 'excel_multi_result')

        if not file_path or not sheets:
            return ActionResult(success=False, message="file_path and sheets are required")

        try:
            import pandas as pd
            from openpyxl import Workbook

            resolved_path = context.resolve_value(file_path) if context else file_path
            resolved_sheets = context.resolve_value(sheets) if context else sheets

            _os.makedirs(_os.path.dirname(resolved_path) or '.', exist_ok=True)

            with pd.ExcelWriter(resolved_path, engine='openpyxl') as writer:
                for sheet in resolved_sheets:
                    name = sheet.get('name', 'Sheet')
                    data = sheet.get('data')
                    if isinstance(data, pd.DataFrame):
                        data.to_excel(writer, sheet_name=name, index=False)
                    elif isinstance(data, list):
                        df = pd.DataFrame(data)
                        df.to_excel(writer, sheet_name=name, index=False)

            result = {'output_path': resolved_path, 'sheets': len(resolved_sheets)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Written {len(resolved_sheets)} sheets", data=result)
        except ImportError:
            return ActionResult(success=False, message="pandas or openpyxl not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Excel multi-sheet error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path', 'sheets']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'excel_multi_result'}


class ExcelSheetListAction(BaseAction):
    """List sheet names in Excel file."""
    action_type = "excel_sheet_list"
    display_name = "ExcelSheet列表"
    description = "列出Excel中的Sheet"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute sheet list."""
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'sheet_names')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            import pandas as pd

            resolved_path = context.resolve_value(file_path) if context else file_path

            xl = pd.ExcelFile(resolved_path)
            sheet_names = xl.sheet_names

            result = {'sheets': sheet_names, 'count': len(sheet_names)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Found {len(sheet_names)} sheets", data=result)
        except ImportError:
            return ActionResult(success=False, message="pandas not installed")
        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"Excel sheet list error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sheet_names'}


class ExcelStyleAction(BaseAction):
    """Style Excel cells."""
    action_type = "excel_style"
    display_name = "Excel格式化"
    description = "格式化Excel单元格"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Excel styling."""
        file_path = params.get('file_path', '')
        cell_range = params.get('cell_range', 'A1')
        bold = params.get('bold', False)
        font_color = params.get('font_color', None)
        bg_color = params.get('bg_color', None)
        font_size = params.get('font_size', None)
        number_format = params.get('number_format', None)
        sheet_name = params.get('sheet_name', 'Sheet1')
        output_var = params.get('output_var', 'style_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            from openpyxl import load_workbook
            from openpyxl.styles import Font, PatternFill, Alignment

            resolved_path = context.resolve_value(file_path) if context else file_path

            wb = load_workbook(resolved_path)
            ws = wb[sheet_name]

            cell = ws[cell_range]

            if bold or font_color or font_size:
                current = cell.font or Font()
                kwargs = {'bold': bold if bold else current.bold}
                if font_color:
                    kwargs['color'] = font_color
                if font_size:
                    kwargs['size'] = font_size
                cell.font = Font(**kwargs)

            if bg_color:
                cell.fill = PatternFill(start_color=bg_color, end_color=bg_color, fill_type='solid')

            if number_format:
                cell.number_format = number_format

            wb.save(resolved_path)

            result = {'styled': cell_range, 'path': resolved_path}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Cell {cell_range} styled", data=result)
        except ImportError:
            return ActionResult(success=False, message="openpyxl not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Excel style error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'cell_range': 'A1', 'bold': False, 'font_color': None, 'bg_color': None,
            'font_size': None, 'number_format': None, 'sheet_name': 'Sheet1', 'output_var': 'style_result'
        }

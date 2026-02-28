import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QTextEdit, QPushButton,
    QLabel, QComboBox, QMessageBox, QSplitter, QWidget, QGroupBox,
    QFormLayout, QLineEdit, QSpinBox
)
from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtGui import QFont, QSyntaxHighlighter, QTextCharFormat, QColor, QKeySequence
import traceback

from core.context import ContextManager


class PythonHighlighter(QSyntaxHighlighter):
    def __init__(self, parent=None):
        super().__init__(parent)
        
        self.formats = {}
        
        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor('#0000FF'))
        keyword_format.setFontWeight(QFont.Bold)
        keywords = [
            'False', 'None', 'True', 'and', 'as', 'assert', 'async', 'await',
            'break', 'class', 'continue', 'def', 'del', 'elif', 'else', 'except',
            'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is',
            'lambda', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return', 'try',
            'while', 'with', 'yield'
        ]
        self.formats['keyword'] = (keyword_format, keywords)
        
        string_format = QTextCharFormat()
        string_format.setForeground(QColor('#008000'))
        self.formats['string'] = string_format
        
        comment_format = QTextCharFormat()
        comment_format.setForeground(QColor('#808080'))
        self.formats['comment'] = comment_format
        
        number_format = QTextCharFormat()
        number_format.setForeground(QColor('#FF0000'))
        self.formats['number'] = number_format
        
        builtin_format = QTextCharFormat()
        builtin_format.setForeground(QColor('#800080'))
        builtins = ['print', 'len', 'int', 'str', 'float', 'list', 'dict', 'range', 'sum', 'min', 'max']
        self.formats['builtin'] = (builtin_format, builtins)
    
    def highlightBlock(self, text):
        for name, value in self.formats.items():
            if name == 'keyword':
                fmt, words = value
                for word in words:
                    for pos in self._find_word(text, word):
                        self.setFormat(pos, len(word), fmt)
            elif name == 'builtin':
                fmt, words = value
                for word in words:
                    for pos in self._find_word(text, word):
                        self.setFormat(pos, len(word), fmt)
            elif name == 'string':
                self._highlight_strings(text, value)
            elif name == 'comment':
                if '#' in text:
                    pos = text.index('#')
                    self.setFormat(pos, len(text) - pos, value)
            elif name == 'number':
                self._highlight_numbers(text, value)
    
    def _find_word(self, text, word):
        import re
        pattern = r'\b' + word + r'\b'
        return [m.start() for m in re.finditer(pattern, text)]
    
    def _highlight_strings(self, text, fmt):
        import re
        for match in re.finditer(r'"[^"]*"|\'[^\']*\'', text):
            self.setFormat(match.start(), match.end() - match.start(), fmt)
    
    def _highlight_numbers(self, text, fmt):
        import re
        for match in re.finditer(r'\b\d+\.?\d*\b', text):
            self.setFormat(match.start(), match.end() - match.start(), fmt)


class ScriptEditorDialog(QDialog):
    def __init__(self, context: ContextManager = None, initial_code: str = '', parent=None):
        super().__init__(parent)
        self.context = context or ContextManager()
        self.setWindowTitle("脚本编辑器")
        self.setMinimumSize(900, 700)
        self._init_ui()
        self._load_templates()
        
        if initial_code:
            self.code_edit.setPlainText(initial_code)
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        
        toolbar = QHBoxLayout()
        
        self.template_combo = QComboBox()
        self.template_combo.addItem("-- 选择模板 --")
        self.template_combo.currentIndexChanged.connect(self._on_template_selected)
        toolbar.addWidget(QLabel("模板:"))
        toolbar.addWidget(self.template_combo)
        
        toolbar.addStretch()
        
        self.help_btn = QPushButton("📖 帮助")
        self.help_btn.clicked.connect(self._show_help)
        toolbar.addWidget(self.help_btn)
        
        layout.addLayout(toolbar)
        
        splitter = QSplitter(Qt.Vertical)
        
        code_group = QGroupBox("脚本代码")
        code_layout = QVBoxLayout(code_group)
        
        self.code_edit = QTextEdit()
        self.code_edit.setFont(QFont('Consolas', 11))
        self.code_edit.setPlaceholderText("# 在此输入 Python 代码\n# 可使用 context 变量访问上下文\n# 示例: return_value = context.get('var1') + 10")
        self.highlighter = PythonHighlighter(self.code_edit.document())
        code_layout.addWidget(self.code_edit)
        
        splitter.addWidget(code_group)
        
        test_group = QGroupBox("测试区域")
        test_layout = QVBoxLayout(test_group)
        
        vars_layout = QHBoxLayout()
        vars_layout.addWidget(QLabel("测试变量:"))
        
        self.test_var_name = QLineEdit()
        self.test_var_name.setPlaceholderText("变量名")
        self.test_var_value = QLineEdit()
        self.test_var_value.setPlaceholderText("变量值")
        self.add_var_btn = QPushButton("添加")
        self.add_var_btn.clicked.connect(self._add_test_var)
        
        vars_layout.addWidget(self.test_var_name)
        vars_layout.addWidget(self.test_var_value)
        vars_layout.addWidget(self.add_var_btn)
        test_layout.addLayout(vars_layout)
        
        self.test_vars_display = QTextEdit()
        self.test_vars_display.setMaximumHeight(80)
        self.test_vars_display.setReadOnly(True)
        self.test_vars_display.setPlaceholderText("测试变量将显示在这里...")
        test_layout.addWidget(self.test_vars_display)
        
        result_layout = QHBoxLayout()
        
        self.run_btn = QPushButton("▶ 运行测试")
        self.run_btn.setStyleSheet("background-color: #4CAF50; color: white; padding: 8px 16px;")
        self.run_btn.clicked.connect(self._run_test)
        
        self.clear_btn = QPushButton("清空")
        self.clear_btn.clicked.connect(self._clear_test)
        
        result_layout.addWidget(self.run_btn)
        result_layout.addWidget(self.clear_btn)
        result_layout.addStretch()
        test_layout.addLayout(result_layout)
        
        self.result_edit = QTextEdit()
        self.result_edit.setReadOnly(True)
        self.result_edit.setMaximumHeight(150)
        self.result_edit.setPlaceholderText("运行结果将显示在这里...")
        test_layout.addWidget(self.result_edit)
        
        splitter.addWidget(test_group)
        
        splitter.setSizes([400, 300])
        layout.addWidget(splitter)
        
        output_layout = QHBoxLayout()
        output_layout.addWidget(QLabel("输出变量名:"))
        self.output_var_edit = QLineEdit()
        self.output_var_edit.setPlaceholderText("可选，保存 return_value 到此变量")
        output_layout.addWidget(self.output_var_edit)
        layout.addLayout(output_layout)
        
        button_box = QHBoxLayout()
        
        self.insert_btn = QPushButton("插入到步骤")
        self.insert_btn.clicked.connect(self.accept)
        
        self.cancel_btn = QPushButton("取消")
        self.cancel_btn.clicked.connect(self.reject)
        
        button_box.addStretch()
        button_box.addWidget(self.insert_btn)
        button_box.addWidget(self.cancel_btn)
        layout.addLayout(button_box)
        
        self._test_vars = {}
    
    def _load_templates(self):
        self.templates = {
            "基本运算": '''# 基本运算示例
a = context.get('a', 0)
b = context.get('b', 0)
return_value = a + b
print(f"计算结果: {a} + {b} = {return_value}")''',
            
            "字符串处理": '''# 字符串处理示例
text = context.get('text', '')
return_value = text.strip().upper()
print(f"处理结果: {return_value}")''',
            
            "条件判断": '''# 条件判断示例
value = context.get('value', 0)
if value > 100:
    return_value = 'large'
elif value > 50:
    return_value = 'medium'
else:
    return_value = 'small'
print(f"判断结果: {return_value}")''',
            
            "循环处理": '''# 循环处理示例
items = context.get('items', [])
total = 0
for item in items:
    total += item
return_value = total
print(f"总和: {total}")''',
            
            "列表操作": '''# 列表操作示例
data = context.get('data', '')
items = data.split(',')
return_value = [item.strip() for item in items]
print(f"列表: {return_value}")''',
            
            "字典操作": '''# 字典操作示例
info = context.get('info', {})
info['processed'] = True
info['timestamp'] = len(str(info))
return_value = info
print(f"处理后的字典: {return_value}")''',
            
            "读取变量": '''# 读取多个变量
name = context.get('name', '未知')
count = context.get('count', 0)
return_value = f"{name}: {count}"
print(return_value)''',
            
            "设置多个变量": '''# 设置多个变量
context.set('result', 'success')
context.set('timestamp', 12345)
return_value = True
print("变量已设置")''',
        }
        
        for name in self.templates:
            self.template_combo.addItem(name)
    
    def _on_template_selected(self, index):
        if index > 0:
            template_name = self.template_combo.currentText()
            if template_name in self.templates:
                self.code_edit.setPlainText(self.templates[template_name])
    
    def _add_test_var(self):
        name = self.test_var_name.text().strip()
        value = self.test_var_value.text().strip()
        
        if name:
            try:
                import json
                value = json.loads(value) if value.startswith(('[', '{', '"')) else value
                if value.isdigit():
                    value = int(value)
                elif value.replace('.', '').isdigit():
                    value = float(value)
            except:
                pass
            
            self._test_vars[name] = value
            self._update_test_vars_display()
            
            self.test_var_name.clear()
            self.test_var_value.clear()
    
    def _update_test_vars_display(self):
        import json
        display = json.dumps(self._test_vars, ensure_ascii=False, indent=2)
        self.test_vars_display.setPlainText(display)
    
    def _run_test(self):
        code = self.code_edit.toPlainText().strip()
        
        if not code:
            self.result_edit.setPlainText("❌ 错误: 代码为空")
            return
        
        test_context = ContextManager()
        test_context.set_all(self._test_vars.copy())
        
        try:
            result = test_context.safe_exec(code)
            
            output = []
            output.append("✅ 执行成功\n")
            output.append(f"返回值: {result}")
            output.append(f"\n当前变量:\n{test_context.to_json()}")
            
            self.result_edit.setPlainText('\n'.join(output))
            self.result_edit.setStyleSheet("color: green;")
            
        except Exception as e:
            error_msg = f"❌ 执行失败:\n{str(e)}\n\n详细错误:\n{traceback.format_exc()}"
            self.result_edit.setPlainText(error_msg)
            self.result_edit.setStyleSheet("color: red;")
    
    def _clear_test(self):
        self._test_vars.clear()
        self.test_vars_display.clear()
        self.result_edit.clear()
    
    def _show_help(self):
        help_text = '''# 脚本编辑器帮助

## 基本说明
- 脚本使用 Python 语法
- 可通过 `context` 变量访问上下文数据
- 使用 `return_value` 变量返回结果

## 可用函数
- `context.get(name, default)` - 获取变量
- `context.set(name, value)` - 设置变量
- `int()`, `str()`, `float()` - 类型转换
- `len()`, `sum()`, `min()`, `max()` - 内置函数

## 示例代码
```python
# 获取变量并计算
a = context.get('a', 0)
b = context.get('b', 0)
return_value = a + b
```

## 注意事项
- 不支持导入外部模块
- 不支持文件操作
- 不支持网络请求
'''
        QMessageBox.information(self, "脚本帮助", help_text)
    
    def get_code(self) -> str:
        return self.code_edit.toPlainText()
    
    def get_output_var(self) -> str:
        return self.output_var_edit.text().strip()


class ScriptHelpDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("脚本功能说明")
        self.setMinimumSize(600, 500)
        self._init_ui()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        
        help_text = QTextEdit()
        help_text.setReadOnly(True)
        help_text.setMarkdown('''
# 脚本执行功能说明

## 功能介绍
脚本执行功能允许你运行 Python 代码片段，实现更复杂的逻辑处理。

## 使用方法

### 1. 基本语法
```python
# 获取变量
value = context.get('变量名', 默认值)

# 设置变量
context.set('变量名', 值)

# 返回结果
return_value = 计算结果
```

### 2. 可用函数
| 函数 | 说明 |
|------|------|
| `context.get(name, default)` | 获取变量值 |
| `context.set(name, value)` | 设置变量值 |
| `int(x)`, `float(x)`, `str(x)` | 类型转换 |
| `len(x)` | 获取长度 |
| `sum(list)` | 求和 |
| `min(x, y)`, `max(x, y)` | 最小/最大值 |

### 3. 示例脚本

**字符串处理:**
```python
text = context.get('ocr_result', '')
code = text.split(':')[1].strip()
return_value = code
```

**数值计算:**
```python
a = context.get('count', 0)
return_value = a * 2 + 10
```

**条件判断:**
```python
value = context.get('result', '')
if '成功' in value:
    return_value = True
else:
    return_value = False
```

## 安全限制
- 不支持 `import` 导入模块
- 不支持文件读写操作
- 不支持网络请求
- 不支持执行系统命令

## 输出变量
如果设置了"输出变量名"，`return_value` 的值将保存到该变量中，供后续步骤使用。
''')
        layout.addWidget(help_text)
        
        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn)

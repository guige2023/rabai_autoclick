"""
Visual Workflow Workbench v22
Canvas-based workflow editor with drag-and-drop, connections, zoom/pan, mini-map, undo/redo
"""
import json
import time
import uuid
import copy
import math
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from typing import Dict, List, Optional, Any, Callable, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import threading


# Constants
GRID_SIZE = 20
NODE_WIDTH = 180
NODE_HEIGHT = 80
MINIMAP_SCALE = 0.15
CANVAS_PADDING = 100


class NodeType(Enum):
    """Node types for workflow"""
    ACTION = "action"
    CONDITION = "condition"
    LOOP = "loop"
    TRY_CATCH = "try_catch"
    START = "start"
    END = "end"
    COMMENT = "comment"
    VARIABLE = "variable"
    WAIT = "wait"
    NOTIFY = "notify"


class ConnectionType(Enum):
    """Connection line types"""
    NORMAL = "normal"
    TRUE = "true"
    FALSE = "false"
    ERROR = "error"


@dataclass
class WorkflowNode:
    """Represents a workflow node"""
    node_id: str
    node_type: NodeType
    name: str
    x: float
    y: float
    width: float = NODE_WIDTH
    height: float = NODE_HEIGHT
    params: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    description: str = ""
    color: str = "#4a90d9"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type.value,
            "name": self.name,
            "x": self.x,
            "y": self.y,
            "width": self.width,
            "height": self.height,
            "params": self.params,
            "enabled": self.enabled,
            "description": self.description,
            "color": self.color
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowNode':
        return cls(
            node_id=data["node_id"],
            node_type=NodeType(data["node_type"]),
            name=data["name"],
            x=data["x"],
            y=data["y"],
            width=data.get("width", NODE_WIDTH),
            height=data.get("height", NODE_HEIGHT),
            params=data.get("params", {}),
            enabled=data.get("enabled", True),
            description=data.get("description", ""),
            color=data.get("color", "#4a90d9")
        )


@dataclass
class WorkflowConnection:
    """Represents a connection between nodes"""
    conn_id: str
    source_id: str
    target_id: str
    connection_type: ConnectionType = ConnectionType.NORMAL
    label: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "conn_id": self.conn_id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "connection_type": self.connection_type.value,
            "label": self.label
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowConnection':
        return cls(
            conn_id=data["conn_id"],
            source_id=data["source_id"],
            target_id=data["target_id"],
            connection_type=ConnectionType(data.get("connection_type", "normal")),
            label=data.get("label", "")
        )


class UndoManager:
    """Manages undo/redo operations"""
    def __init__(self, max_history: int = 100):
        self.max_history = max_history
        self.undo_stack: List[Dict[str, Any]] = []
        self.redo_stack: List[Dict[str, Any]] = []
    
    def save_state(self, state: Dict[str, Any]):
        """Save current state for undo"""
        self.undo_stack.append(copy.deepcopy(state))
        if len(self.undo_stack) > self.max_history:
            self.undo_stack.pop(0)
        self.redo_stack.clear()
    
    def undo(self) -> Optional[Dict[str, Any]]:
        """Undo last operation"""
        if self.undo_stack:
            state = self.undo_stack.pop()
            return copy.deepcopy(state)
        return None
    
    def redo(self) -> Optional[Dict[str, Any]]:
        """Redo last undone operation"""
        if self.redo_stack:
            state = self.redo_stack.pop()
            return copy.deepcopy(state)
        return None


class ValidationResult:
    """Workflow validation result"""
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.is_valid: bool = True
    
    def add_error(self, message: str):
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str):
        self.warnings.append(message)


class WorkflowWorkbench:
    """
    Visual workflow workbench/IDE with canvas editor, node palette,
    connections, zoom/pan, mini-map, undo/redo, and more.
    """
    
    def __init__(self, parent: tk.Tk or tk.Widget, app=None):
        self.parent = parent
        self.app = app
        
        # Workflow data
        self.nodes: Dict[str, WorkflowNode] = {}
        self.connections: Dict[str, WorkflowConnection] = {}
        
        # Undo/redo
        self.undo_manager = UndoManager()
        
        # Selection and interaction state
        self.selected_node_id: Optional[str] = None
        self.selected_nodes: Set[str] = set()
        self.is_dragging = False
        self.is_panning = False
        self.is_connecting = False
        self.connect_start_node: Optional[str] = None
        self.mouse_start_x = 0
        self.mouse_start_y = 0
        self.drag_offset_x = 0
        self.drag_offset_y = 0
        
        # Canvas state
        self.zoom = 1.0
        self.pan_x = 0
        self.pan_y = 0
        self.canvas_width = 800
        self.canvas_height = 600
        
        # Grid settings
        self.grid_size = GRID_SIZE
        self.snap_to_grid = True
        self.show_grid = True
        
        # Clipboard
        self.clipboard: Dict[str, Any] = {"nodes": {}, "connections": {}}
        
        # Node palette definitions
        self.node_palette: Dict[NodeType, Dict[str, Any]] = self._create_node_palette()
        
        # Validation callbacks
        self.validation_callbacks: List[Callable] = []
        
        # Build UI
        self._create_ui()
        self._bind_events()
        
        # Initial state
        self._save_state()
    
    def _create_node_palette(self) -> Dict[NodeType, Dict[str, Any]]:
        """Create the node palette with available actions"""
        return {
            NodeType.START: {
                "name": "开始",
                "color": "#27ae60",
                "icon": "▶",
                "description": "工作流起点"
            },
            NodeType.END: {
                "name": "结束",
                "color": "#e74c3c",
                "icon": "■",
                "description": "工作流终点"
            },
            NodeType.ACTION: {
                "name": "动作",
                "color": "#4a90d9",
                "icon": "⚡",
                "description": "执行动作"
            },
            NodeType.CONDITION: {
                "name": "条件",
                "color": "#f39c12",
                "icon": "◆",
                "description": "条件判断"
            },
            NodeType.LOOP: {
                "name": "循环",
                "color": "#9b59b6",
                "icon": "⟳",
                "description": "循环执行"
            },
            NodeType.TRY_CATCH: {
                "name": "尝试/捕获",
                "color": "#e67e22",
                "icon": "⚠",
                "description": "错误处理"
            },
            NodeType.WAIT: {
                "name": "等待",
                "color": "#1abc9c",
                "icon": "⏱",
                "description": "等待时间"
            },
            NodeType.NOTIFY: {
                "name": "通知",
                "color": "#3498db",
                "icon": "✉",
                "description": "发送通知"
            },
            NodeType.VARIABLE: {
                "name": "变量",
                "color": "#95a5a6",
                "icon": "≡",
                "description": "变量操作"
            },
            NodeType.COMMENT: {
                "name": "注释",
                "color": "#bdc3c7",
                "icon": "💬",
                "description": "添加注释"
            }
        }
    
    def _create_ui(self):
        """Create the workbench UI"""
        # Main container
        self.main_frame = ttk.Frame(self.parent)
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Toolbar
        self._create_toolbar()
        
        # Paned window for resizable panels
        paned = ttk.PanedWindow(self.main_frame, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True)
        
        # Left panel - Node palette
        self._create_palette_panel(paned)
        
        # Center - Canvas area
        self._create_canvas_area(paned)
        
        # Right panel - Properties
        self._create_properties_panel(paned)
        
        # Bottom - Status bar and validation
        self._create_status_bar()
        
        # Mini-map
        self._create_minimap()
    
    def _create_toolbar(self):
        """Create toolbar with actions"""
        toolbar = ttk.Frame(self.main_frame)
        toolbar.pack(side=tk.TOP, fill=tk.X, padx=5, pady=2)
        
        # File operations
        ttk.Button(toolbar, text="新建", width=6, command=self.new_workflow).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="打开", width=6, command=self.open_workflow).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="保存", width=6, command=self.save_workflow).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="导出", width=6, command=self.export_canvas).pack(side=tk.LEFT, padx=2)
        
        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=5)
        
        # Edit operations
        ttk.Button(toolbar, text="撤销", width=6, command=self.undo).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="重做", width=6, command=self.redo).pack(side=tk.LEFT, padx=2)
        
        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=5)
        
        # Layout operations
        ttk.Button(toolbar, text="自动布局", width=8, command=self.auto_layout).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="验证", width=6, command=self.validate_workflow).pack(side=tk.LEFT, padx=2)
        
        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=5)
        
        # Zoom controls
        ttk.Label(toolbar, text="缩放:").pack(side=tk.LEFT, padx=2)
        self.zoom_label = ttk.Label(toolbar, text="100%")
        self.zoom_label.pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="+", width=3, command=self.zoom_in).pack(side=tk.LEFT, padx=1)
        ttk.Button(toolbar, text="-", width=3, command=self.zoom_out).pack(side=tk.LEFT, padx=1)
        ttk.Button(toolbar, text="重置", width=6, command=self.reset_zoom).pack(side=tk.LEFT, padx=2)
        
        # Grid toggle
        self.grid_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(toolbar, text="网格", variable=self.grid_var, command=self.toggle_grid).pack(side=tk.LEFT, padx=5)
        
        # Snap toggle
        self.snap_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(toolbar, text="吸附", variable=self.snap_var, command=self.toggle_snap).pack(side=tk.LEFT, padx=5)
    
    def _create_palette_panel(self, paned):
        """Create node palette panel"""
        palette_frame = ttk.Frame(paned, width=200)
        paned.add(palette_frame, weight=0)
        
        ttk.Label(palette_frame, text="节点库", font=("", 10, "bold")).pack(pady=5)
        
        # Scrollable canvas for palette items
        palette_canvas = tk.Canvas(palette_frame, bg="#f0f0f0", highlightthickness=0)
        palette_scroll = ttk.Scrollbar(palette_frame, orient=tk.VERTICAL, command=palette_canvas.yview)
        palette_canvas.configure(yscrollcommand=palette_scroll.set)
        
        palette_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        palette_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Add palette items
        y_pos = 10
        for node_type, info in self.node_palette.items():
            item = self._create_palette_item(palette_canvas, node_type, info, y_pos)
            y_pos += 70
        
        palette_canvas.configure(scrollregion=(0, 0, 180, y_pos + 20))
        
        # Make palette items draggable
        palette_canvas.tag_bind("palette_item", "<ButtonPress-1>", self._on_palette_drag_start)
        palette_canvas.tag_bind("palette_item", "<B1-Motion>", self._on_palette_drag)
        palette_canvas.tag_bind("palette_item", "<ButtonRelease-1>", self._on_palette_drag_end)
    
    def _create_palette_item(self, canvas, node_type: NodeType, info: Dict, y: int) -> int:
        """Create a palette item and return next y position"""
        x = 10
        item_id = canvas.create_rectangle(
            x, y, x + 160, y + 60,
            fill=info["color"], outline="#888", width=2,
            tags=("palette_item", f"palette_{node_type.value}")
        )
        text_id = canvas.create_text(
            x + 10, y + 15,
            text=f"{info['icon']} {info['name']}",
            anchor=tk.W, fill="white", font=("", 10, "bold")
        )
        desc_id = canvas.create_text(
            x + 10, y + 40,
            text=info['description'],
            anchor=tk.W, fill="white", font=("", 8)
        )
        
        # Store node type in canvas item
        canvas.itemconfigure(item_id, node_type=node_type.value)
        
        return y + 70
    
    def _create_canvas_area(self, paned):
        """Create the main canvas area"""
        canvas_frame = ttk.Frame(paned)
        paned.add(canvas_frame, weight=1)
        
        # Canvas with scrollbars
        self.canvas = tk.Canvas(
            canvas_frame,
            bg="white",
            highlightthickness=1,
            highlightcolor="#4a90d9"
        )
        
        # Scrollbars
        v_scroll = ttk.Scrollbar(canvas_frame, orient=tk.VERTICAL, command=self.canvas.yview)
        h_scroll = ttk.Scrollbar(canvas_frame, orient=tk.HORIZONTAL, command=self.canvas.xview)
        
        self.canvas.configure(yscrollcommand=v_scroll.set, xscrollcommand=h_scroll.set)
        
        v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        h_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        self.canvas.pack(fill=tk.BOTH, expand=True)
        
        # Configure canvas scrolling region
        self.canvas.configure(scrollregion=(-5000, -5000, 10000, 10000))
        
        # Mouse wheel zoom
        self.canvas.bind("<MouseWheel>", self._on_mousewheel)
        self.canvas.bind("<Control-MouseWheel>", self._on_mousewheel)
    
    def _create_properties_panel(self, paned):
        """Create properties panel"""
        props_frame = ttk.Frame(paned, width=250)
        paned.add(props_frame, weight=0)
        
        ttk.Label(props_frame, text="属性", font=("", 10, "bold")).pack(pady=5)
        
        # Properties notebook
        notebook = ttk.Notebook(props_frame)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Node properties tab
        node_frame = ttk.Frame(notebook)
        notebook.add(node_frame, text="节点")
        self._create_node_properties(node_frame)
        
        # Workflow properties tab
        workflow_frame = ttk.Frame(notebook)
        notebook.add(workflow_frame, text="工作流")
        self._create_workflow_properties(workflow_frame)
        
        # Validation tab
        validation_frame = ttk.Frame(notebook)
        notebook.add(validation_frame, text="验证")
        self._create_validation_panel(validation_frame)
    
    def _create_node_properties(self, parent):
        """Create node properties form"""
        self.node_props_labels = {}
        
        # Name
        ttk.Label(parent, text="名称:").grid(row=0, column=0, sticky=tk.W, pady=3)
        self.node_name_var = tk.StringVar()
        self.node_props_labels['name'] = ttk.Entry(parent, textvariable=self.node_name_var, width=20)
        self.node_props_labels['name'].grid(row=0, column=1, pady=3)
        
        # Type
        ttk.Label(parent, text="类型:").grid(row=1, column=0, sticky=tk.W, pady=3)
        self.node_type_var = tk.StringVar()
        self.node_props_labels['type'] = ttk.Label(parent, text="-", foreground="gray")
        self.node_props_labels['type'].grid(row=1, column=1, sticky=tk.W, pady=3)
        
        # Position X
        ttk.Label(parent, text="X:").grid(row=2, column=0, sticky=tk.W, pady=3)
        self.node_x_var = tk.IntVar()
        self.node_props_labels['x'] = ttk.Entry(parent, textvariable=self.node_x_var, width=10)
        self.node_props_labels['x'].grid(row=2, column=1, sticky=tk.W, pady=3)
        
        # Position Y
        ttk.Label(parent, text="Y:").grid(row=3, column=0, sticky=tk.W, pady=3)
        self.node_y_var = tk.IntVar()
        self.node_props_labels['y'] = ttk.Entry(parent, textvariable=self.node_y_var, width=10)
        self.node_props_labels['y'].grid(row=3, column=1, sticky=tk.W, pady=3)
        
        # Description
        ttk.Label(parent, text="描述:").grid(row=4, column=0, sticky=tk.W, pady=3)
        self.node_desc_text = tk.Text(parent, width=20, height=4)
        self.node_props_labels['desc'] = self.node_desc_text
        self.node_desc_text.grid(row=4, column=1, pady=3)
        
        # Buttons
        btn_frame = ttk.Frame(parent)
        btn_frame.grid(row=5, column=0, columnspan=2, pady=10)
        ttk.Button(btn_frame, text="应用", command=self.apply_node_properties).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="删除", command=self.delete_selected_node).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="复制", command=self.copy_selection).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="剪切", command=self.cut_selection).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="粘贴", command=self.paste).pack(side=tk.LEFT, padx=2)
    
    def _create_workflow_properties(self, parent):
        """Create workflow properties form"""
        ttk.Label(parent, text="工作流名称:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.workflow_name_var = tk.StringVar(value="未命名工作流")
        ttk.Entry(parent, textvariable=self.workflow_name_var, width=20).grid(row=0, column=1, pady=5)
        
        ttk.Label(parent, text="描述:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.workflow_desc_text = tk.Text(parent, width=20, height=5)
        self.workflow_desc_text.grid(row=1, column=1, pady=5)
        
        ttk.Label(parent, text="节点数:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.node_count_label = ttk.Label(parent, text="0")
        self.node_count_label.grid(row=2, column=1, sticky=tk.W, pady=5)
        
        ttk.Label(parent, text="连接数:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.conn_count_label = ttk.Label(parent, text="0")
        self.conn_count_label.grid(row=3, column=1, sticky=tk.W, pady=5)
    
    def _create_validation_panel(self, parent):
        """Create validation results panel"""
        self.validation_text = tk.Text(parent, width=25, height=15, state=tk.DISABLED)
        self.validation_text.pack(fill=tk.BOTH, expand=True)
        
        scroll = ttk.Scrollbar(self.validation_text, command=self.validation_text.yview)
        self.validation_text.configure(yscrollcommand=scroll.set)
    
    def _create_status_bar(self):
        """Create status bar"""
        self.status_bar = ttk.Frame(self.main_frame)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        
        self.status_label = ttk.Label(self.status_bar, text="就绪")
        self.status_label.pack(side=tk.LEFT, padx=5)
        
        self.coord_label = ttk.Label(self.status_bar, text="X: 0, Y: 0")
        self.coord_label.pack(side=tk.RIGHT, padx=5)
    
    def _create_minimap(self):
        """Create mini-map window"""
        self.minimap_toplevel = tk.Toplevel(self.parent)
        self.minimap_toplevel.title("小地图")
        self.minimap_toplevel.geometry("200x150")
        self.minimap_toplevel.resizable(False, False)
        
        # Make it stay on top
        self.minimap_toplevel.attributes("-topmost", True)
        
        minimap_frame = ttk.Frame(self.minimap_toplevel)
        minimap_frame.pack(fill=tk.BOTH, expand=True)
        
        self.minimap_canvas = tk.Canvas(minimap_frame, bg="white", width=190, height=140)
        self.minimap_canvas.pack()
        
        # Update minimap when canvas changes
        self.canvas.bind("<Configure>", lambda e: self.update_minimap())
    
    def _bind_events(self):
        """Bind keyboard and mouse events"""
        # Canvas events
        self.canvas.bind("<Button-1>", self._on_canvas_click)
        self.canvas.bind("<Button-2>", self._on_pan_start)
        self.canvas.bind("<Button-3>", self._on_context_menu)
        self.canvas.bind("<B2-Motion>", self._on_pan)
        self.canvas.bind("<ButtonRelease-2>", self._on_pan_end)
        self.canvas.bind("<B1-Motion>", self._on_drag)
        self.canvas.bind("<ButtonRelease-1>", self._on_release)
        self.canvas.bind("<Motion>", self._on_motion)
        
        # Keyboard shortcuts
        self.parent.bind("<Control-z>", lambda e: self.undo())
        self.parent.bind("<Control-y>", lambda e: self.redo())
        self.parent.bind("<Control-c>", lambda e: self.copy_selection())
        self.parent.bind("<Control-v>", lambda e: self.paste())
        self.parent.bind("<Control-x>", lambda e: self.cut_selection())
        self.parent.bind("<Delete>", lambda e: self.delete_selected_node())
        self.parent.bind("<Control-a>", lambda e: self.select_all())
        self.parent.bind("<Control-s>", lambda e: self.save_workflow())
        self.parent.bind("<Control-o>", lambda e: self.open_workflow())
        self.parent.bind("<Control-n>", lambda e: self.new_workflow())
        self.parent.bind("<Control-g>", lambda e: self.toggle_grid())
        self.parent.bind("<space>", lambda e: self.start_connection())
        self.parent.bind("<Escape>", lambda e: self.cancel_operation())
        self.parent.bind("<F5>", lambda e: self.validate_workflow())
        self.parent.bind("<F6>", lambda e: self.auto_layout())
        
        # Zoom with keyboard
        self.parent.bind("<Control-equal>", lambda e: self.zoom_in())
        self.parent.bind("<Control-minus>", lambda e: self.zoom_out())
        self.parent.bind("<Control-0>", lambda e: self.reset_zoom())
    
    # ==================== Canvas Operations ====================
    
    def _on_canvas_click(self, event):
        """Handle canvas click"""
        x = self.canvas.canvasx(event.x)
        y = self.canvas.canvasy(event.y)
        
        # Check if clicked on a node
        node = self._get_node_at_position(x, y)
        if node:
            self.select_node(node.node_id)
        else:
            self.deselect_all()
    
    def _on_drag(self, event):
        """Handle drag operation"""
        x = self.canvas.canvasx(event.x)
        y = self.canvas.canvasy(event.y)
        
        if self.selected_node_id and not self.is_connecting:
            # Dragging a node
            if not self.is_dragging:
                self.is_dragging = True
                node = self.nodes[self.selected_node_id]
                self.drag_offset_x = x - node.x
                self.drag_offset_y = y - node.y
            
            # Move node(s)
            if self.selected_nodes:
                for node_id in self.selected_nodes:
                    node = self.nodes[node_id]
                    new_x = x - self.drag_offset_x
                    new_y = y - self.drag_offset_y
                    if self.snap_to_grid:
                        new_x = self._snap_to_grid(new_x)
                        new_y = self._snap_to_grid(new_y)
                    node.x = new_x
                    node.y = new_y
            else:
                node = self.nodes[self.selected_node_id]
                new_x = x - self.drag_offset_x
                new_y = y - self.drag_offset_y
                if self.snap_to_grid:
                    new_x = self._snap_to_grid(new_x)
                    new_y = self._snap_to_grid(new_y)
                node.x = new_x
                node.y = new_y
            
            self._redraw_canvas()
        elif self.is_panning:
            # Panning
            dx = event.x - self.mouse_start_x
            dy = event.y - self.mouse_start_y
            self.canvas.move(tk.ALL, dx, dy)
            self.mouse_start_x = event.x
            self.mouse_start_y = event.y
    
    def _on_release(self, event):
        """Handle mouse release"""
        if self.is_dragging:
            self.is_dragging = False
            self._save_state()
            self.update_properties_panel()
        
        if self.is_connecting:
            x = self.canvas.canvasx(event.x)
            y = self.canvas.canvasy(event.y)
            target_node = self._get_node_at_position(x, y)
            
            if target_node and target_node.node_id != self.connect_start_node:
                self._create_connection(self.connect_start_node, target_node.node_id)
            
            self.is_connecting = False
            self.connect_start_node = None
            self._redraw_canvas()
        
        if self.is_panning:
            self.is_panning = False
    
    def _on_pan_start(self, event):
        """Start panning"""
        self.is_panning = True
        self.mouse_start_x = event.x
        self.mouse_start_y = event.y
    
    def _on_pan(self, event):
        """Pan the canvas"""
        if self.is_panning:
            dx = event.x - self.mouse_start_x
            dy = event.y - self.mouse_start_y
            self.canvas.move(tk.ALL, dx, dy)
            self.mouse_start_x = event.x
            self.mouse_start_y = event.y
    
    def _on_pan_end(self, event):
        """End panning"""
        self.is_panning = False
    
    def _on_motion(self, event):
        """Handle mouse motion"""
        x = self.canvas.canvasx(event.x)
        y = self.canvas.canvasy(event.y)
        self.coord_label.config(text=f"X: {int(x)}, Y: {int(y)}")
        
        # Update cursor based on context
        node = self._get_node_at_position(x, y)
        if node:
            self.canvas.config(cursor="hand1")
        elif self.is_connecting:
            self.canvas.config(cursor="crosshair")
        else:
            self.canvas.config(cursor="arrow")
    
    def _on_mousewheel(self, event):
        """Handle mouse wheel for zooming"""
        if event.state & 0x4:  # Control key
            if event.delta > 0:
                self.zoom_in()
            else:
                self.zoom_out()
        else:
            # Scroll the canvas
            self.canvas.yview_scroll(int(-event.delta/120), "units")
    
    def _on_context_menu(self, event):
        """Show context menu"""
        x = self.canvas.canvasx(event.x)
        y = self.canvas.canvasy(event.y)
        
        menu = tk.Menu(self.parent, tearoff=0)
        menu.add_command(label="粘贴", command=self.paste)
        menu.add_command(label="新建节点", command=lambda: self.add_node_at(x, y))
        menu.add_separator()
        menu.add_command(label="全选", command=self.select_all)
        menu.add_command(label="取消选择", command=self.deselect_all)
        
        menu.post(event.x_root, event.y_root)
    
    def _on_palette_drag_start(self, event):
        """Start dragging from palette"""
        self.palette_drag_start = True
        widget = event.widget
        x = widget.canvasx(event.x)
        y = widget.canvasy(event.y)
        
        # Find which item was clicked
        items = widget.find_overlapping(x-5, y-5, x+5, y+5)
        for item in items:
            tags = widget.gettags(item)
            if "palette_item" in tags:
                for tag in tags:
                    if tag.startswith("palette_"):
                        self.drag_node_type = NodeType(tag.replace("palette_", ""))
                        break
                break
    
    def _on_palette_drag(self, event):
        """Handle palette item drag"""
        pass  # Visual feedback could be added here
    
    def _on_palette_drag_end(self, event):
        """End dragging from palette"""
        if hasattr(self, 'drag_node_type'):
            x = self.canvas.canvasx(event.x)
            y = self.canvas.canvasy(event.y)
            
            # Check if dropped on canvas
            bbox = self.canvas.bbox(tk.ALL)
            if bbox and self._point_in_rect(x, y, bbox):
                self.add_node_at(x, y, self.drag_node_type)
            
            delattr(self, 'drag_node_type')
    
    # ==================== Node Operations ====================
    
    def _get_node_at_position(self, x: float, y: float) -> Optional[WorkflowNode]:
        """Get node at given canvas position"""
        for node in self.nodes.values():
            if (node.x <= x <= node.x + node.width and
                node.y <= y <= node.y + node.height):
                return node
        return None
    
    def _point_in_rect(self, x: float, y: float, bbox: Tuple) -> bool:
        """Check if point is inside rectangle"""
        return bbox[0] <= x <= bbox[2] and bbox[1] <= y <= bbox[3]
    
    def _snap_to_grid(self, value: float) -> float:
        """Snap value to grid"""
        return round(value / self.grid_size) * self.grid_size
    
    def add_node_at(self, x: float, y: float, node_type: NodeType = NodeType.ACTION):
        """Add a new node at position"""
        if self.snap_to_grid:
            x = self._snap_to_grid(x)
            y = self._snap_to_grid(y)
        
        node_id = str(uuid.uuid4())
        info = self.node_palette.get(node_type, {})
        
        node = WorkflowNode(
            node_id=node_id,
            node_type=node_type,
            name=info.get("name", "新节点"),
            x=x - NODE_WIDTH / 2,
            y=y - NODE_HEIGHT / 2,
            color=info.get("color", "#4a90d9")
        )
        
        self.nodes[node_id] = node
        self._save_state()
        self._redraw_canvas()
        self.select_node(node_id)
        self.update_minimap()
        
        return node
    
    def select_node(self, node_id: str):
        """Select a node"""
        self.selected_node_id = node_id
        self.selected_nodes = {node_id}
        self._redraw_canvas()
        self.update_properties_panel()
        self.status_label.config(text=f"已选择: {self.nodes[node_id].name}")
    
    def deselect_all(self):
        """Deselect all nodes"""
        self.selected_node_id = None
        self.selected_nodes = set()
        self._redraw_canvas()
        self.clear_properties_panel()
        self.status_label.config(text="就绪")
    
    def delete_selected_node(self):
        """Delete selected node(s)"""
        if not self.selected_nodes:
            return
        
        self._save_state()
        
        for node_id in list(self.selected_nodes):
            # Delete connections
            conns_to_delete = [
                cid for cid, conn in self.connections.items()
                if conn.source_id == node_id or conn.target_id == node_id
            ]
            for cid in conns_to_delete:
                del self.connections[cid]
            
            # Delete node
            del self.nodes[node_id]
        
        self.deselect_all()
        self._redraw_canvas()
        self.update_minimap()
        self.update_status_counts()
    
    def update_node_position(self, node_id: str, x: float, y: float):
        """Update node position"""
        if node_id in self.nodes:
            if self.snap_to_grid:
                x = self._snap_to_grid(x)
                y = self._snap_to_grid(y)
            self.nodes[node_id].x = x
            self.nodes[node_id].y = y
            self._redraw_canvas()
    
    # ==================== Connection Operations ====================
    
    def start_connection(self):
        """Start creating a connection"""
        if self.selected_node_id:
            self.is_connecting = True
            self.connect_start_node = self.selected_node_id
            self.status_label.config(text="连接模式: 点击目标节点")
    
    def _create_connection(self, source_id: str, target_id: str, conn_type: ConnectionType = ConnectionType.NORMAL):
        """Create a connection between nodes"""
        # Check for duplicate
        for conn in self.connections.values():
            if conn.source_id == source_id and conn.target_id == target_id:
                return
        
        # Check for self-connection
        if source_id == target_id:
            return
        
        conn_id = str(uuid.uuid4())
        conn = WorkflowConnection(
            conn_id=conn_id,
            source_id=source_id,
            target_id=target_id,
            connection_type=conn_type
        )
        
        self.connections[conn_id] = conn
        self._save_state()
        self.update_status_counts()
    
    def delete_connection(self, conn_id: str):
        """Delete a connection"""
        if conn_id in self.connections:
            del self.connections[conn_id]
            self._save_state()
            self._redraw_canvas()
            self.update_status_counts()
    
    # ==================== Drawing ====================
    
    def _redraw_canvas(self):
        """Redraw the entire canvas"""
        self.canvas.delete(tk.ALL)
        
        # Draw grid
        if self.show_grid:
            self._draw_grid()
        
        # Draw connections
        for conn in self.connections.values():
            self._draw_connection(conn)
        
        # Draw nodes
        for node in self.nodes.values():
            self._draw_node(node)
        
        # Draw connection preview if connecting
        if self.is_connecting and self.selected_node_id:
            self._draw_connection_preview()
        
        self.update_minimap()
    
    def _draw_grid(self):
        """Draw background grid"""
        # Calculate visible area
        bbox = self.canvas.bbox(tk.ALL)
        if not bbox:
            return
        
        x1, y1, x2, y2 = bbox
        
        # Adjust for zoom
        grid_size = self.grid_size * self.zoom
        
        if grid_size < 5:
            return  # Too small to draw
        
        # Draw vertical lines
        start_x = math.floor(x1 / grid_size) * grid_size
        for x in range(int(start_x), int(x2), int(grid_size)):
            self.canvas.create_line(x, y1, x, y2, fill="#e0e0e0", tags="grid")
        
        # Draw horizontal lines
        start_y = math.floor(y1 / grid_size) * grid_size
        for y in range(int(start_y), int(y2), int(grid_size)):
            self.canvas.create_line(x1, y, x2, y, fill="#e0e0e0", tags="grid")
    
    def _draw_node(self, node: WorkflowNode):
        """Draw a single node"""
        x, y = node.x, node.y
        w, h = node.width, node.height
        
        # Determine colors
        if node.node_id in self.selected_nodes:
            outline = "#2ecc71"  # Green outline for selection
            lw = 3
        else:
            outline = "#333"
            lw = 2
        
        # Draw shadow
        self.canvas.create_rectangle(
            x + 3, y + 3, x + w + 3, y + h + 3,
            fill="#00000020", outline="", tags="node_shadow"
        )
        
        # Draw main rectangle
        self.canvas.create_rectangle(
            x, y, x + w, y + h,
            fill=node.color, outline=outline, width=lw,
            tags=("node", f"node_{node.node_id}")
        )
        
        # Draw header
        header_h = 25
        info = self.node_palette.get(node.type, {})
        self.canvas.create_rectangle(
            x, y, x + w, y + header_h,
            fill=self._darken_color(node.color, 0.8),
            outline="", tags=("node_header", f"node_{node.node_id}")
        )
        
        # Draw text
        icon = info.get("icon", "●")
        self.canvas.create_text(
            x + 10, y + header_h / 2,
            text=f"{icon} {node.name}",
            anchor=tk.W, fill="white", font=("", 10, "bold"),
            tags=("node_text", f"node_{node.node_id}")
        )
        
        # Draw description if exists
        if node.description:
            self.canvas.create_text(
                x + 10, y + header_h + 15,
                text=node.description[:30],
                anchor=tk.W, fill="white", font=("", 8),
                tags=("node_desc", f"node_{node.node_id}")
            )
        
        # Draw connection points
        self._draw_connection_points(node)
    
    def _draw_connection_points(self, node: WorkflowNode):
        """Draw connection points on node"""
        x, y = node.x, node.y
        w, h = node.width, node.height
        
        r = 6  # Point radius
        
        # Input point (top center)
        self.canvas.create_oval(
            x + w/2 - r, y - r, x + w/2 + r, y + r,
            fill="#fff", outline="#333", width=2,
            tags=("conn_point", f"input_{node.node_id}")
        )
        
        # Output point (bottom center)
        self.canvas.create_oval(
            x + w/2 - r, y + h - r, x + w/2 + r, y + h + r,
            fill="#fff", outline="#333", width=2,
            tags=("conn_point", f"output_{node.node_id}")
        )
    
    def _draw_connection(self, conn: WorkflowConnection):
        """Draw a connection line"""
        if conn.source_id not in self.nodes or conn.target_id not in self.nodes:
            return
        
        source = self.nodes[conn.source_id]
        target = self.nodes[conn.target_id]
        
        # Calculate connection points
        sx = source.x + source.width / 2
        sy = source.y + source.height
        tx = target.x + target.width / 2
        ty = target.y
        
        # Draw bezier curve
        mid_y = (sy + ty) / 2
        
        # Determine color based on connection type
        color = self._get_connection_color(conn.connection_type)
        
        points = [sx, sy, sx, mid_y, tx, mid_y, tx, ty]
        
        self.canvas.create_line(
            points, smooth=True, splinesteps=20,
            fill=color, width=2,
            tags=("connection", f"conn_{conn.conn_id}")
        )
        
        # Draw arrow at end
        self._draw_arrow(tx, ty, tx, ty - 10, color, conn.conn_id)
        
        # Draw label if exists
        if conn.label:
            mx = (sx + tx) / 2
            my = mid_y
            self.canvas.create_text(
                mx, my, text=conn.label,
                fill=color, font=("", 8),
                tags=("conn_label", f"conn_{conn.conn_id}")
            )
    
    def _draw_arrow(self, x, y, dx, dy, color, conn_id):
        """Draw arrowhead"""
        size = 8
        angle = math.atan2(dy - y, dx - x)
        
        p1x = x - size * math.cos(angle - math.pi/6)
        p1y = y - size * math.sin(angle - math.pi/6)
        p2x = x - size * math.cos(angle + math.pi/6)
        p2y = y - size * math.sin(angle + math.pi/6)
        
        self.canvas.create_line(
            x, y, p1x, p1y, p2x, p2y, x, y,
            fill=color, width=2,
            tags=("arrow", f"conn_{conn_id}")
        )
    
    def _draw_connection_preview(self):
        """Draw connection creation preview"""
        if not self.connect_start_node or self.selected_node_id == self.connect_start_node:
            return
        
        source = self.nodes[self.connect_start_node]
        sx = source.x + source.width / 2
        sy = source.y + source.height
        
        # Get current mouse position
        x = self.canvas.canvasx(self.canvas.winfo_pointerx() - self.canvas.winfo_rootx())
        y = self.canvas.canvasy(self.canvas.winfo_pointery() - self.canvas.winfo_rooty())
        
        self.canvas.create_line(
            sx, sy, sx, (sy + y) / 2, x, (sy + y) / 2, x, y,
            smooth=True, splinesteps=20,
            fill="#888888", width=2, dash=(5, 5),
            tags="connection_preview"
        )
    
    def _get_connection_color(self, conn_type: ConnectionType) -> str:
        """Get color for connection type"""
        colors = {
            ConnectionType.NORMAL: "#4a90d9",
            ConnectionType.TRUE: "#27ae60",
            ConnectionType.FALSE: "#e74c3c",
            ConnectionType.ERROR: "#e67e22"
        }
        return colors.get(conn_type, "#4a90d9")
    
    def _darken_color(self, hex_color: str, factor: float) -> str:
        """Darken a hex color"""
        hex_color = hex_color.lstrip('#')
        r = int(hex_color[0:2], 16)
        g = int(hex_color[2:4], 16)
        b = int(hex_color[4:6], 16)
        
        r = int(r * factor)
        g = int(g * factor)
        b = int(b * factor)
        
        return f"#{r:02x}{g:02x}{b:02x}"
    
    # ==================== Zoom and Pan ====================
    
    def zoom_in(self):
        """Zoom in"""
        self.zoom *= 1.2
        self.zoom = min(self.zoom, 5.0)
        self._apply_zoom()
    
    def zoom_out(self):
        """Zoom out"""
        self.zoom /= 1.2
        self.zoom = max(self.zoom, 0.2)
        self._apply_zoom()
    
    def reset_zoom(self):
        """Reset zoom to default"""
        self.zoom = 1.0
        self._apply_zoom()
    
    def _apply_zoom(self):
        """Apply current zoom to canvas"""
        self.canvas.scale(tk.ALL, 0, 0, self.zoom, self.zoom)
        self.zoom_label.config(text=f"{int(self.zoom * 100)}%")
        self._redraw_canvas()
    
    def pan_to(self, x: float, y: float):
        """Pan to specific position"""
        self.canvas.xview_moveto(x)
        self.canvas.yview_moveto(y)
    
    # ==================== Mini-map ====================
    
    def update_minimap(self):
        """Update mini-map display"""
        self.minimap_canvas.delete(tk.ALL)
        
        if not self.nodes:
            return
        
        # Calculate bounds
        min_x = min(n.x for n in self.nodes.values())
        min_y = min(n.y for n in self.nodes.values())
        max_x = max(n.x + n.width for n in self.nodes.values())
        max_y = max(n.y + n.height for n in self.nodes.values())
        
        # Add padding
        padding = 50
        min_x -= padding
        min_y -= padding
        max_x += padding
        max_y += padding
        
        # Calculate scale to fit in minimap
        map_w = 190
        map_h = 140
        scale_x = map_w / (max_x - min_x)
        scale_y = map_h / (max_y - min_y)
        scale = min(scale_x, scale_y) * 0.9
        
        # Center offset
        offset_x = map_w / 2 - (max_x + min_x) / 2 * scale
        offset_y = map_h / 2 - (max_y + min_y) / 2 * scale
        
        # Draw connections
        for conn in self.connections.values():
            if conn.source_id in self.nodes and conn.target_id in self.nodes:
                s = self.nodes[conn.source_id]
                t = self.nodes[conn.target_id]
                
                sx = (s.x + s.width/2) * scale + offset_x
                sy = (s.y + s.height) * scale + offset_y
                tx = (t.x + t.width/2) * scale + offset_x
                ty = t.y * scale + offset_y
                
                self.minimap_canvas.create_line(sx, sy, tx, ty, fill="#aaa", width=1)
        
        # Draw nodes
        for node in self.nodes.values():
            x = node.x * scale + offset_x
            y = node.y * scale + offset_y
            w = node.width * scale
            h = node.height * scale
            
            fill = node.color if node.node_id not in self.selected_nodes else "#2ecc71"
            self.minimap_canvas.create_rectangle(x, y, x + w, y + h, fill=fill, outline="#333")
        
        # Draw viewport indicator
        bbox = self.canvas.bbox(tk.ALL)
        if bbox:
            vx1 = bbox[0] * scale + offset_x
            vy1 = bbox[1] * scale + offset_y
            vx2 = bbox[2] * scale + offset_x
            vy2 = bbox[3] * scale + offset_y
            
            self.minimap_canvas.create_rectangle(
                vx1, vy1, vx2, vy2,
                outline="#e74c3c", width=2
            )
    
    # ==================== Undo/Redo ====================
    
    def _save_state(self):
        """Save current state for undo"""
        state = {
            "nodes": {nid: n.to_dict() for nid, n in self.nodes.items()},
            "connections": {cid: c.to_dict() for cid, c in self.connections.items()}
        }
        self.undo_manager.save_state(state)
    
    def undo(self):
        """Undo last operation"""
        state = self.undo_manager.undo()
        if state:
            self.undo_manager.redo_stack.append({
                "nodes": {nid: n.to_dict() for nid, n in self.nodes.items()},
                "connections": {cid: c.to_dict() for cid, c in self.connections.items()}
            })
            self._restore_state(state)
            self.status_label.config(text="撤销")
    
    def redo(self):
        """Redo last undone operation"""
        state = self.undo_manager.redo()
        if state:
            self.undo_manager.undo_stack.append({
                "nodes": {nid: n.to_dict() for nid, n in self.nodes.items()},
                "connections": {cid: c.to_dict() for cid, c in self.connections.items()}
            })
            self._restore_state(state)
            self.status_label.config(text="重做")
    
    def _restore_state(self, state: Dict):
        """Restore state from saved"""
        self.nodes = {nid: WorkflowNode.from_dict(d) for nid, d in state["nodes"].items()}
        self.connections = {cid: WorkflowConnection.from_dict(d) for cid, d in state["connections"].items()}
        self.deselect_all()
        self._redraw_canvas()
        self.update_status_counts()
    
    # ==================== Copy/Paste ====================
    
    def copy_selection(self):
        """Copy selected nodes to clipboard"""
        if not self.selected_nodes:
            return
        
        self.clipboard = {
            "nodes": {nid: self.nodes[nid].to_dict() for nid in self.selected_nodes},
            "connections": {}
        }
        
        # Copy connections between selected nodes
        for cid, conn in self.connections.items():
            if conn.source_id in self.selected_nodes and conn.target_id in self.selected_nodes:
                self.clipboard["connections"][cid] = conn.to_dict()
        
        self.status_label.config(text=f"已复制 {len(self.selected_nodes)} 个节点")
    
    def cut_selection(self):
        """Cut selected nodes"""
        self.copy_selection()
        self.delete_selected_node()
    
    def paste(self):
        """Paste nodes from clipboard"""
        if not self.clipboard["nodes"]:
            return
        
        self._save_state()
        
        # Create new IDs and offset positions
        id_mapping = {}
        offset_x = 50
        offset_y = 50
        
        for nid, node_dict in self.clipboard["nodes"].items():
            new_id = str(uuid.uuid4())
            id_mapping[nid] = new_id
            
            node_dict = copy.deepcopy(node_dict)
            node_dict["node_id"] = new_id
            node_dict["x"] += offset_x
            node_dict["y"] += offset_y
            
            if self.snap_to_grid:
                node_dict["x"] = self._snap_to_grid(node_dict["x"])
                node_dict["y"] = self._snap_to_grid(node_dict["y"])
            
            self.nodes[new_id] = WorkflowNode.from_dict(node_dict)
        
        # Create connections
        for cid, conn_dict in self.clipboard["connections"].items():
            conn_dict = copy.deepcopy(conn_dict)
            conn_dict["conn_id"] = str(uuid.uuid4())
            conn_dict["source_id"] = id_mapping[conn_dict["source_id"]]
            conn_dict["target_id"] = id_mapping[conn_dict["target_id"]]
            
            self.connections[conn_dict["conn_id"]] = WorkflowConnection.from_dict(conn_dict)
        
        # Select pasted nodes
        self.selected_nodes = set(id_mapping.values())
        self.selected_node_id = list(self.selected_nodes)[0] if self.selected_nodes else None
        
        self._redraw_canvas()
        self.update_minimap()
        self.update_status_counts()
        self.status_label.config(text=f"已粘贴 {len(self.clipboard['nodes'])} 个节点")
    
    def select_all(self):
        """Select all nodes"""
        self.selected_nodes = set(self.nodes.keys())
        if self.selected_nodes:
            self.selected_node_id = list(self.selected_nodes)[0]
        self._redraw_canvas()
        self.update_properties_panel()
    
    # ==================== Properties Panel ====================
    
    def update_properties_panel(self):
        """Update properties panel with selected node"""
        if self.selected_node_id and self.selected_node_id in self.nodes:
            node = self.nodes[self.selected_node_id]
            
            self.node_name_var.set(node.name)
            self.node_type_var.set(self.node_palette.get(node.node_type, {}).get("name", node.node_type.value))
            self.node_x_var.set(int(node.x))
            self.node_y_var.set(int(node.y))
            
            self.node_desc_text.delete("1.0", tk.END)
            self.node_desc_text.insert("1.0", node.description)
        else:
            self.clear_properties_panel()
    
    def clear_properties_panel(self):
        """Clear properties panel"""
        self.node_name_var.set("")
        self.node_type_var.set("-")
        self.node_x_var.set(0)
        self.node_y_var.set(0)
        self.node_desc_text.delete("1.0", tk.END)
    
    def apply_node_properties(self):
        """Apply properties from panel to selected node"""
        if self.selected_node_id and self.selected_node_id in self.nodes:
            node = self.nodes[self.selected_node_id]
            
            node.name = self.node_name_var.get()
            node.x = self.node_x_var.get()
            node.y = self.node_y_var.get()
            node.description = self.node_desc_text.get("1.0", tk.END).strip()
            
            self._save_state()
            self._redraw_canvas()
            self.update_minimap()
            self.status_label.config(text=f"已更新: {node.name}")
    
    def update_status_counts(self):
        """Update node and connection counts"""
        self.node_count_label.config(text=str(len(self.nodes)))
        self.conn_count_label.config(text=str(len(self.connections)))
    
    # ==================== Validation ====================
    
    def validate_workflow(self):
        """Validate the workflow"""
        result = ValidationResult()
        
        # Check for start node
        start_nodes = [n for n in self.nodes.values() if n.node_type == NodeType.START]
        if not start_nodes:
            result.add_warning("工作流没有开始节点")
        elif len(start_nodes) > 1:
            result.add_warning(f"工作流有 {len(start_nodes)} 个开始节点")
        
        # Check for end node
        end_nodes = [n for n in self.nodes.values() if n.node_type == NodeType.END]
        if not end_nodes:
            result.add_warning("工作流没有结束节点")
        
        # Check for orphan nodes
        connected_ids = set()
        for conn in self.connections.values():
            connected_ids.add(conn.source_id)
            connected_ids.add(conn.target_id)
        
        for node in self.nodes.values():
            if node.node_type not in (NodeType.START, NodeType.END) and node.node_id not in connected_ids:
                result.add_warning(f"节点 '{node.name}' 未连接到工作流")
        
        # Check for circular dependencies
        if self._has_circular_dependency():
            result.add_warning("工作流存在循环依赖")
        
        # Run custom validation callbacks
        for callback in self.validation_callbacks:
            try:
                callback(result)
            except Exception as e:
                result.add_error(f"验证回调错误: {str(e)}")
        
        # Update validation panel
        self._update_validation_display(result)
        
        if result.is_valid and not result.warnings:
            self.status_label.config(text="验证通过: 工作流有效")
            messagebox.showinfo("验证", "工作流验证通过!")
        else:
            msg = ""
            if result.errors:
                msg += "错误:\n" + "\n".join(result.errors) + "\n\n"
            if result.warnings:
                msg += "警告:\n" + "\n".join(result.warnings)
            self.status_label.config(text="验证完成")
            messagebox.showwarning("验证", msg if msg else "验证完成")
        
        return result
    
    def _has_circular_dependency(self) -> bool:
        """Check for circular dependencies using DFS"""
        visited = set()
        rec_stack = set()
        
        def dfs(node_id: str) -> bool:
            visited.add(node_id)
            rec_stack.add(node_id)
            
            for conn in self.connections.values():
                if conn.source_id == node_id:
                    if conn.target_id not in visited:
                        if dfs(conn.target_id):
                            return True
                    elif conn.target_id in rec_stack:
                        return True
            
            rec_stack.remove(node_id)
            return False
        
        for node_id in self.nodes:
            if node_id not in visited:
                if dfs(node_id):
                    return True
        
        return False
    
    def _update_validation_display(self, result: ValidationResult):
        """Update validation panel display"""
        self.validation_text.configure(state=tk.NORMAL)
        self.validation_text.delete("1.0", tk.END)
        
        if result.is_valid and not result.warnings:
            self.validation_text.insert("1.0", "✓ 验证通过\n\n工作流有效")
        else:
            if result.errors:
                self.validation_text.insert(tk.END, "错误:\n")
                for err in result.errors:
                    self.validation_text.insert(tk.END, f"  ✗ {err}\n")
            
            if result.warnings:
                if result.errors:
                    self.validation_text.insert(tk.END, "\n")
                self.validation_text.insert(tk.END, "警告:\n")
                for warn in result.warnings:
                    self.validation_text.insert(tk.END, f"  ⚠ {warn}\n")
        
        self.validation_text.configure(state=tk.DISABLED)
    
    def add_validation_callback(self, callback: Callable):
        """Add custom validation callback"""
        self.validation_callbacks.append(callback)
    
    # ==================== Auto Layout ====================
    
    def auto_layout(self):
        """Automatically arrange nodes"""
        if not self.nodes:
            return
        
        self._save_state()
        
        # Simple hierarchical layout
        # Find start nodes
        start_nodes = [n for n in self.nodes.values() if n.node_type == NodeType.START]
        
        if not start_nodes:
            # Use nodes with no incoming connections
            start_nodes = []
            for node in self.nodes.values():
                has_incoming = any(c.target_id == node.node_id for c in self.connections.values())
                if not has_incoming:
                    start_nodes.append(node)
        
        if not start_nodes:
            start_nodes = list(self.nodes.values())[:1]
        
        # BFS to assign levels
        levels: Dict[str, int] = {}
        queue = [(n.node_id, 0) for n in start_nodes]
        visited = set()
        
        while queue:
            node_id, level = queue.pop(0)
            if node_id in visited:
                continue
            visited.add(node_id)
            levels[node_id] = level
            
            # Find children
            for conn in self.connections.values():
                if conn.source_id == node_id:
                    if conn.target_id not in visited:
                        queue.append((conn.target_id, level + 1))
        
        # Assign remaining unvisited nodes
        for node_id in self.nodes:
            if node_id not in levels:
                levels[node_id] = 0
        
        # Group by level
        level_groups: Dict[int, List[str]] = {}
        for node_id, level in levels.items():
            if level not in level_groups:
                level_groups[level] = []
            level_groups[level].append(node_id)
        
        # Position nodes
        level_height = NODE_HEIGHT + 80
        horiz_spacing = NODE_WIDTH + 100
        
        for level, node_ids in level_groups.items():
            y = level * level_height + 100
            total_width = len(node_ids) * horiz_spacing
            start_x = -total_width / 2 + horiz_spacing / 2
            
            for i, node_id in enumerate(node_ids):
                node = self.nodes[node_id]
                node.x = start_x + i * horiz_spacing
                node.y = y
        
        self._redraw_canvas()
        self.update_minimap()
        self.status_label.config(text="自动布局完成")
    
    # ==================== Grid and Snap ====================
    
    def toggle_grid(self):
        """Toggle grid visibility"""
        self.show_grid = self.grid_var.get()
        self._redraw_canvas()
    
    def toggle_snap(self):
        """Toggle grid snapping"""
        self.snap_to_grid = self.snap_var.get()
    
    # ==================== File Operations ====================
    
    def new_workflow(self):
        """Create new workflow"""
        if self.nodes:
            if not messagebox.askyesno("新建", "确定要创建新工作流吗? 未保存的更改将丢失."):
                return
        
        self.nodes.clear()
        self.connections.clear()
        self.deselect_all()
        self._save_state()
        self._redraw_canvas()
        self.update_minimap()
        self.update_status_counts()
        self.status_label.config(text="新工作流")
    
    def open_workflow(self):
        """Open workflow from file"""
        path = filedialog.askopenfilename(
            title="打开工作流",
            filetypes=[("Workflow Files", "*.wf"), ("JSON Files", "*.json"), ("All Files", "*.*")]
        )
        
        if not path:
            return
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.nodes = {nid: WorkflowNode.from_dict(d) for nid, d in data.get("nodes", {}).items()}
            self.connections = {cid: WorkflowConnection.from_dict(d) for cid, d in data.get("connections", {}).items()}
            
            if "name" in data:
                self.workflow_name_var.set(data["name"])
            if "description" in data:
                self.workflow_desc_text.delete("1.0", tk.END)
                self.workflow_desc_text.insert("1.0", data["description"])
            
            self._save_state()
            self._redraw_canvas()
            self.update_minimap()
            self.update_status_counts()
            self.status_label.config(text=f"已打开: {path}")
            
        except Exception as e:
            messagebox.showerror("错误", f"无法打开文件: {str(e)}")
    
    def save_workflow(self):
        """Save workflow to file"""
        path = filedialog.asksaveasfilename(
            title="保存工作流",
            defaultextension=".wf",
            filetypes=[("Workflow Files", "*.wf"), ("JSON Files", "*.json"), ("All Files", "*.*")]
        )
        
        if not path:
            return
        
        data = {
            "name": self.workflow_name_var.get(),
            "description": self.workflow_desc_text.get("1.0", tk.END).strip(),
            "nodes": {nid: n.to_dict() for nid, n in self.nodes.items()},
            "connections": {cid: c.to_dict() for cid, c in self.connections.items()}
        }
        
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            self.status_label.config(text=f"已保存: {path}")
            
        except Exception as e:
            messagebox.showerror("错误", f"无法保存文件: {str(e)}")
    
    def export_canvas(self):
        """Export canvas as PNG or SVG"""
        # Ask for format
        dialog = tk.Toplevel(self.parent)
        dialog.title("导出")
        dialog.geometry("200x150")
        dialog.resizable(False, False)
        
        ttk.Label(dialog, text="选择导出格式:").pack(pady=10)
        
        def export_png():
            dialog.destroy()
            self._export_as_png()
        
        def export_svg():
            dialog.destroy()
            self._export_as_svg()
        
        ttk.Button(dialog, text="PNG 图片", command=export_png).pack(pady=5, padx=20, fill=tk.X)
        ttk.Button(dialog, text="SVG 矢量图", command=export_svg).pack(pady=5, padx=20, fill=tk.X)
        ttk.Button(dialog, text="取消", command=dialog.destroy).pack(pady=5, padx=20, fill=tk.X)
    
    def _export_as_png(self):
        """Export canvas as PNG"""
        path = filedialog.asksaveasfilename(
            title="导出 PNG",
            defaultextension=".png",
            filetypes=[("PNG Files", "*.png"), ("All Files", "*.*")]
        )
        
        if not path:
            return
        
        try:
            # Get all canvas content
            self.canvas.update_idletasks()
            
            # Create postscript
            ps = self.canvas.postscript(colormode='color')
            
            # Try to use PIL to convert
            try:
                from PIL import Image
                import io
                
                img = Image.open(io.BytesIO(ps.encode('utf-8')))
                img.save(path, 'png')
                
                self.status_label.config(text=f"已导出 PNG: {path}")
                
            except ImportError:
                # Fallback: just save postscript
                messagebox.showinfo("提示", "请安装 Pillow 库以支持 PNG 导出")
                with open(path.replace('.png', '.ps'), 'w') as f:
                    f.write(ps)
        
        except Exception as e:
            messagebox.showerror("错误", f"导出失败: {str(e)}")
    
    def _export_as_svg(self):
        """Export canvas as SVG"""
        path = filedialog.asksaveasfilename(
            title="导出 SVG",
            defaultextension=".svg",
            filetypes=[("SVG Files", "*.svg"), ("All Files", "*.*")]
        )
        
        if not path:
            return
        
        try:
            # Calculate bounds
            if self.nodes:
                min_x = min(n.x for n in self.nodes.values()) - 50
                min_y = min(n.y for n in self.nodes.values()) - 50
                max_x = max(n.x + n.width for n in self.nodes.values()) + 50
                max_y = max(n.y + n.height for n in self.nodes.values()) + 50
            else:
                min_x, min_y, max_x, max_y = 0, 0, 800, 600
            
            width = max_x - min_x
            height = max_y - min_y
            
            svg = ['<?xml version="1.0" encoding="UTF-8"?>']
            svg.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="{min_x} {min_y} {width} {height}">')
            
            # Draw connections
            for conn in self.connections.values():
                if conn.source_id in self.nodes and conn.target_id in self.nodes:
                    s = self.nodes[conn.source_id]
                    t = self.nodes[conn.target_id]
                    
                    sx = s.x + s.width / 2
                    sy = s.y + s.height
                    tx = t.x + t.width / 2
                    ty = t.y
                    
                    color = self._get_connection_color(conn.connection_type)
                    svg.append(f'  <path d="M {sx} {sy} Q {sx} {(sy+ty)/2} {tx} {(sy+ty)/2} Q {tx} {(sy+ty)/2} {tx} {ty}" fill="none" stroke="{color}" stroke-width="2"/>')
            
            # Draw nodes
            for node in self.nodes.values():
                svg.append(f'  <rect x="{node.x}" y="{node.y}" width="{node.width}" height="{node.height}" fill="{node.color}" rx="5"/>')
                
                info = self.node_palette.get(node.node_type, {})
                icon = info.get("icon", "")
                svg.append(f'  <text x="{node.x + 10}" y="{node.y + 18}" fill="white" font-size="12">{icon} {node.name}</text>')
            
            svg.append('</svg>')
            
            with open(path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(svg))
            
            self.status_label.config(text=f"已导出 SVG: {path}")
            
        except Exception as e:
            messagebox.showerror("错误", f"导出失败: {str(e)}")
    
    # ==================== Utilities ====================
    
    def cancel_operation(self):
        """Cancel current operation"""
        if self.is_connecting:
            self.is_connecting = False
            self.connect_start_node = None
            self._redraw_canvas()
            self.status_label.config(text="操作已取消")
    
    def get_workflow_data(self) -> Dict[str, Any]:
        """Get workflow data as dictionary"""
        return {
            "nodes": {nid: n.to_dict() for nid, n in self.nodes.items()},
            "connections": {cid: c.to_dict() for cid, c in self.connections.items()}
        }
    
    def load_workflow_data(self, data: Dict[str, Any]):
        """Load workflow data from dictionary"""
        self.nodes = {nid: WorkflowNode.from_dict(d) for nid, d in data.get("nodes", {}).items()}
        self.connections = {cid: WorkflowConnection.from_dict(d) for cid, d in data.get("connections", {}).items()}
        self._save_state()
        self._redraw_canvas()
        self.update_minimap()
        self.update_status_counts()


# Standalone test
if __name__ == "__main__":
    root = tk.Tk()
    root.title("Workflow Workbench Test")
    root.geometry("1200x800")
    
    workbench = WorkflowWorkbench(root)
    
    # Add some test nodes
    workbench.add_node_at(200, 100, NodeType.START)
    workbench.add_node_at(400, 200, NodeType.ACTION)
    workbench.add_node_at(400, 350, NodeType.CONDITION)
    workbench.add_node_at(200, 450, NodeType.END)
    
    # Create a connection
    if len(workbench.nodes) >= 2:
        node_ids = list(workbench.nodes.keys())
        workbench._create_connection(node_ids[0], node_ids[1])
    
    root.mainloop()

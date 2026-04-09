"""Gesture template storage for saving and loading gesture templates."""
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import json
import hashlib


@dataclass
class GestureTemplate:
    """A stored gesture template."""
    name: str
    template_type: str
    points: List[tuple]
    normalized_points: List[tuple] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)


class GestureTemplateStorage:
    """Stores and manages gesture templates for matching.
    
    Provides CRUD operations for gesture templates with
    support for tagging, searching, and versioning.
    
    Example:
        storage = GestureTemplateStorage()
        storage.save_template(GestureTemplate(name="circle", template_type="shape", points=[(0,0),(1,1)]))
        template = storage.find_by_name("circle")
    """

    def __init__(self, storage_path: Optional[str] = None) -> None:
        self._storage_path = storage_path or "/tmp/gesture_templates.json"
        self._templates: Dict[str, GestureTemplate] = {}
        self._tags_index: Dict[str, set] = {}
        self._load()

    def save_template(self, template: GestureTemplate) -> str:
        """Save a gesture template."""
        tid = self._generate_id(template)
        self._templates[tid] = template
        for tag in template.tags:
            if tag not in self._tags_index:
                self._tags_index[tag] = set()
            self._tags_index[tag].add(tid)
        self._persist()
        return tid

    def get_template(self, template_id: str) -> Optional[GestureTemplate]:
        """Get a template by ID."""
        return self._templates.get(template_id)

    def find_by_name(self, name: str) -> Optional[GestureTemplate]:
        """Find template by exact name."""
        for t in self._templates.values():
            if t.name == name:
                return t
        return None

    def find_by_tag(self, tag: str) -> List[GestureTemplate]:
        """Find all templates with a tag."""
        ids = self._tags_index.get(tag, set())
        return [self._templates[tid] for tid in ids if tid in self._templates]

    def find_by_type(self, template_type: str) -> List[GestureTemplate]:
        """Find all templates of a type."""
        return [t for t in self._templates.values() if t.template_type == template_type]

    def delete_template(self, template_id: str) -> bool:
        """Delete a template."""
        if template_id not in self._templates:
            return False
        for tag in self._templates[template_id].tags:
            self._tags_index.get(tag, set()).discard(template_id)
        del self._templates[template_id]
        self._persist()
        return True

    def list_templates(self) -> List[GestureTemplate]:
        """List all templates."""
        return list(self._templates.values())

    def list_tags(self) -> List[str]:
        """List all tags."""
        return list(self._tags_index.keys())

    def _generate_id(self, template: GestureTemplate) -> str:
        content = f"{template.name}:{template.template_type}:{len(template.points)}"
        return hashlib.md5(content.encode()).hexdigest()[:12]

    def _persist(self) -> None:
        try:
            data = {tid: {
                "name": t.name, "template_type": t.template_type,
                "points": t.points, "normalized_points": t.normalized_points,
                "metadata": t.metadata, "tags": t.tags,
            } for tid, t in self._templates.items()}
            with open(self._storage_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    def _load(self) -> None:
        try:
            with open(self._storage_path) as f:
                data = json.load(f)
            for tid, tdata in data.items():
                template = GestureTemplate(
                    name=tdata["name"], template_type=tdata["template_type"],
                    points=[tuple(p) for p in tdata["points"]],
                    normalized_points=[tuple(p) for p in tdata.get("normalized_points", [])],
                    metadata=tdata.get("metadata", {}), tags=tdata.get("tags", []),
                )
                self._templates[tid] = template
                for tag in template.tags:
                    if tag not in self._tags_index:
                        self._tags_index[tag] = set()
                    self._tags_index[tag].add(tid)
        except Exception:
            self._templates = {}
            self._tags_index = {}

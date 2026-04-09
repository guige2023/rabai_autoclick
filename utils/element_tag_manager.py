"""Element tag manager for tagging and searching UI elements."""
from typing import Dict, List, Set, Optional, Any, Callable
from dataclasses import dataclass, field
import json


@dataclass
class ElementTag:
    """A tag associated with an element."""
    name: str
    element_id: str
    created_at: float
    metadata: Dict[str, Any] = field(default_factory=dict)


class ElementTagManager:
    """Manages tags for UI elements for easy search and grouping.
    
    Provides tagging capabilities to mark elements with semantic
    labels for easier lookup and categorization.
    
    Example:
        manager = ElementTagManager()
        manager.tag_element("btn_submit", "primary", metadata={"priority": "high"})
        manager.tag_element("btn_submit", "form-action")
        elements = manager.find_by_tag("primary")
    """

    def __init__(self) -> None:
        self._element_tags: Dict[str, Set[str]] = {}
        self._tag_elements: Dict[str, Set[str]] = {}
        self._tag_data: Dict[Tuple[str, str], ElementTag] = {}

    def tag_element(
        self,
        element_id: str,
        tag_name: str,
        metadata: Optional[Dict[str, Any]] = None,
        created_at: float = 0,
    ) -> ElementTag:
        """Tag an element with a tag."""
        if element_id not in self._element_tags:
            self._element_tags[element_id] = set()
        if tag_name not in self._tag_elements:
            self._tag_elements[tag_name] = set()
        
        self._element_tags[element_id].add(tag_name)
        self._tag_elements[tag_name].add(element_id)
        
        tag = ElementTag(
            name=tag_name,
            element_id=element_id,
            created_at=created_at,
            metadata=metadata or {},
        )
        self._tag_data[(element_id, tag_name)] = tag
        return tag

    def untag_element(self, element_id: str, tag_name: str) -> bool:
        """Remove a tag from an element."""
        if (element_id, tag_name) in self._tag_data:
            del self._tag_data[(element_id, tag_name)]
        
        if element_id in self._element_tags:
            self._element_tags[element_id].discard(tag_name)
        
        if tag_name in self._tag_elements:
            self._tag_elements[tag_name].discard(element_id)
        
        return True

    def get_tags(self, element_id: str) -> List[str]:
        """Get all tags for an element."""
        return list(self._element_tags.get(element_id, set()))

    def find_by_tag(self, tag_name: str) -> List[str]:
        """Find all element IDs with a tag."""
        return list(self._tag_elements.get(tag_name, set()))

    def find_by_tags(self, tag_names: List[str], match_all: bool = False) -> List[str]:
        """Find elements matching given tags."""
        if match_all:
            result = None
            for tag_name in tag_names:
                elements = set(self._tag_elements.get(tag_name, set()))
                if result is None:
                    result = elements
                else:
                    result &= elements
            return list(result or set())
        else:
            result = set()
            for tag_name in tag_names:
                result |= set(self._tag_elements.get(tag_name, set()))
            return list(result)

    def get_tag_metadata(self, element_id: str, tag_name: str) -> Optional[Dict]:
        """Get metadata for a specific tag on an element."""
        tag = self._tag_data.get((element_id, tag_name))
        return tag.metadata if tag else None

    def remove_all_tags(self, element_id: str) -> None:
        """Remove all tags from an element."""
        tags = self._element_tags.pop(element_id, set())
        for tag_name in tags:
            if tag_name in self._tag_elements:
                self._tag_elements[tag_name].discard(element_id)

    def list_all_tags(self) -> List[str]:
        """List all tag names in use."""
        return list(self._tag_elements.keys())

    def get_stats(self) -> Dict[str, Any]:
        """Get tagging statistics."""
        return {
            "total_elements": len(self._element_tags),
            "total_tags": len(self._tag_elements),
            "most_used_tags": sorted(
                [(tag, len(elements)) for tag, elements in self._tag_elements.items()],
                key=lambda x: -x[1]
            )[:10],
        }

"""Element anchor chain builder for creating chained element lookups."""
from typing import List, Optional, Callable, Any, Tuple
from dataclasses import dataclass


@dataclass
class AnchorLink:
    """A single link in an anchor chain."""
    anchor_type: str
    selector: str
    index: int = 0
    predicate: Optional[Callable[[Any], bool]] = None
    offset: Tuple[int, int] = (0, 0)


class ElementAnchorChain:
    """Builds chains of element anchors for reliable element lookup.
    
    Creates anchor chains where each element is found relative to
    the previous element, improving reliability in dynamic UIs.
    
    Example:
        chain = ElementAnchorChain()
        elem = (chain
            .start_with("title", "class:header-title")
            .then("button", "class:action-btn", index=2)
            .then("icon", "class:btn-icon")
            .resolve(finder=app.find_element))
    """

    def __init__(self) -> None:
        self._links: List[AnchorLink] = []

    def start_with(self, anchor_type: str, selector: str) -> "ElementAnchorChain":
        """Start chain with an initial anchor element."""
        self._links.append(AnchorLink(anchor_type=anchor_type, selector=selector))
        return self

    def then(self, anchor_type: str, selector: str, index: int = 0,
             predicate: Optional[Callable[[Any], bool]] = None) -> "ElementAnchorChain":
        """Add a linked anchor relative to previous element."""
        self._links.append(AnchorLink(
            anchor_type=anchor_type,
            selector=selector,
            index=index,
            predicate=predicate,
        ))
        return self

    def offset(self, dx: int, dy: int) -> "ElementAnchorChain":
        """Add coordinate offset to the last link."""
        if self._links:
            self._links[-1].offset = (dx, dy)
        return self

    def resolve(self, finder: Callable[[str], Any]) -> Optional[Tuple[Any, Tuple[int, int]]]:
        """Resolve the anchor chain to find the target element."""
        if not self._links:
            return None
        current = None
        for i, link in enumerate(self._links):
            if i == 0:
                current = finder(link.selector)
            else:
                if current is None:
                    return None
                candidates = self._find_candidates(current, link)
                if candidates and link.index < len(candidates):
                    current = candidates[link.index]
                else:
                    return None
        if current is None:
            return None
        return (current, self._links[-1].offset if self._links else (0, 0))

    def _find_candidates(self, parent: Any, link: AnchorLink) -> List[Any]:
        """Find candidate elements for a link."""
        return []

    def clear(self) -> None:
        """Clear all links from the chain."""
        self._links.clear()

    def get_links(self) -> List[AnchorLink]:
        """Get all links in the chain."""
        return self._links.copy()

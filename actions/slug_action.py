"""Slug action module for RabAI AutoClick.

Provides slug generation and manipulation:
- SlugGenerator: Generate URL-friendly slugs
- SlugNormalizer: Normalize and validate slugs
- SlugRegistry: Manage slug mappings
"""

from typing import Any, Callable, Dict, List, Optional, Set
import re
import unicodedata
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SlugGenerator:
    """Generate URL-friendly slugs."""

    def __init__(
        self,
        max_length: int = 80,
        separator: str = "-",
        lowercase: bool = True,
        strip_chars: str = "[^\\w\\s-]",
    ):
        self.max_length = max_length
        self.separator = separator
        self.lowercase = lowercase
        self.strip_chars = re.compile(strip_chars)

    def generate(self, text: str) -> str:
        """Generate slug from text."""
        if not text:
            return ""

        slug = unicodedata.normalize("NFKD", text)
        slug = slug.encode("ascii", "ignore").decode("ascii")

        slug = self.strip_chars.sub("", slug)

        slug = re.sub(r"[-\s]+", self.separator, slug)

        slug = slug.strip(self.separator)

        if self.lowercase:
            slug = slug.lower()

        if len(slug) > self.max_length:
            slug = slug[:self.max_length].rsplit(self.separator, 1)[0]
            slug = slug.strip(self.separator)

        return slug

    def generate_unique(self, text: str, existing: Set[str]) -> str:
        """Generate unique slug."""
        base_slug = self.generate(text)
        if base_slug not in existing:
            return base_slug

        counter = 1
        while True:
            candidate = f"{base_slug}{self.separator}{counter}"
            if candidate not in existing:
                return candidate
            counter += 1


class SlugNormalizer:
    """Normalize and validate slugs."""

    def __init__(self, min_length: int = 1, max_length: int = 80):
        self.min_length = min_length
        self.max_length = max_length

    def normalize(self, slug: str) -> str:
        """Normalize a slug."""
        if not slug:
            return ""

        slug = slug.strip()
        slug = re.sub(r"[-\s]+", "-", slug)
        slug = re.sub(r"-+", "-", slug)
        slug = slug.strip("-")
        slug = slug.lower()

        return slug

    def validate(self, slug: str) -> Dict[str, Any]:
        """Validate a slug."""
        errors: List[str] = []

        if not slug:
            return {"valid": False, "errors": ["Slug cannot be empty"]}

        if len(slug) < self.min_length:
            errors.append(f"Slug too short (min {self.min_length})")

        if len(slug) > self.max_length:
            errors.append(f"Slug too long (max {self.max_length})")

        if not re.match(r"^[a-z0-9]+(?:-[a-z0-9]+)*$", slug):
            errors.append("Slug contains invalid characters")

        if slug.startswith("-") or slug.endswith("-"):
            errors.append("Slug cannot start or end with hyphen")

        if "--" in slug:
            errors.append("Slug cannot contain consecutive hyphens")

        return {"valid": len(errors) == 0, "errors": errors}


class SlugRegistry:
    """Registry for slug mappings."""

    def __init__(self):
        self._slugs: Dict[str, str] = {}
        self._reverse: Dict[str, str] = {}
        self._reserved: Set[str] = set()

    def register(self, slug: str, target: str, overwrite: bool = False) -> bool:
        """Register a slug mapping."""
        normalized = SlugNormalizer().normalize(slug)

        if normalized in self._reserved:
            return False

        if normalized in self._slugs and not overwrite:
            return False

        if normalized in self._slugs and overwrite:
            old_target = self._slugs[normalized]
            del self._reverse[old_target]

        self._slugs[normalized] = target
        self._reverse[target] = normalized
        return True

    def reserve(self, slug: str) -> None:
        """Reserve a slug."""
        normalized = SlugNormalizer().normalize(slug)
        self._reserved.add(normalized)

    def resolve(self, slug: str) -> Optional[str]:
        """Resolve slug to target."""
        normalized = SlugNormalizer().normalize(slug)
        return self._slugs.get(normalized)

    def reverse_resolve(self, target: str) -> Optional[str]:
        """Reverse resolve target to slug."""
        return self._reverse.get(target)

    def exists(self, slug: str) -> bool:
        """Check if slug exists."""
        normalized = SlugNormalizer().normalize(slug)
        return normalized in self._slugs

    def list_all(self) -> Dict[str, str]:
        """List all mappings."""
        return self._slugs.copy()

    def remove(self, slug: str) -> bool:
        """Remove a slug mapping."""
        normalized = SlugNormalizer().normalize(slug)
        if normalized in self._slugs:
            target = self._slugs.pop(normalized)
            del self._reverse[target]
            return True
        return False


class SlugAction(BaseAction):
    """Slug generation and management action."""
    action_type = "slug"
    display_name = "Slug生成器"
    description = "URL友好的slug生成"

    def __init__(self):
        super().__init__()
        self._generator = SlugGenerator()
        self._registry = SlugRegistry()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "generate")

            if operation == "generate":
                return self._generate(params)
            elif operation == "normalize":
                return self._normalize(params)
            elif operation == "validate":
                return self._validate(params)
            elif operation == "register":
                return self._register(params)
            elif operation == "reserve":
                return self._reserve(params)
            elif operation == "resolve":
                return self._resolve(params)
            elif operation == "list":
                return self._list(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Slug error: {str(e)}")

    def _generate(self, params: Dict[str, Any]) -> ActionResult:
        """Generate a slug."""
        text = params.get("text", "")
        unique = params.get("unique", False)
        existing = params.get("existing", set())

        if not text:
            return ActionResult(success=False, message="text is required")

        if unique:
            slug = self._generator.generate_unique(text, set(existing))
        else:
            slug = self._generator.generate(text)

        return ActionResult(success=True, message=f"Slug generated: {slug}", data={"slug": slug})

    def _normalize(self, params: Dict[str, Any]) -> ActionResult:
        """Normalize a slug."""
        slug = params.get("slug", "")

        if not slug:
            return ActionResult(success=False, message="slug is required")

        normalizer = SlugNormalizer()
        normalized = normalizer.normalize(slug)

        return ActionResult(success=True, message=f"Normalized: {normalized}", data={"slug": normalized})

    def _validate(self, params: Dict[str, Any]) -> ActionResult:
        """Validate a slug."""
        slug = params.get("slug", "")

        if not slug:
            return ActionResult(success=False, message="slug is required")

        normalizer = SlugNormalizer()
        result = normalizer.validate(slug)

        return ActionResult(
            success=result["valid"],
            message="Valid" if result["valid"] else f"Invalid: {', '.join(result['errors'])}",
            data=result,
        )

    def _register(self, params: Dict[str, Any]) -> ActionResult:
        """Register a slug."""
        slug = params.get("slug")
        target = params.get("target")
        overwrite = params.get("overwrite", False)

        if not slug or not target:
            return ActionResult(success=False, message="slug and target are required")

        success = self._registry.register(slug, target, overwrite)

        return ActionResult(success=success, message="Registered" if success else "Registration failed")

    def _reserve(self, params: Dict[str, Any]) -> ActionResult:
        """Reserve a slug."""
        slug = params.get("slug")

        if not slug:
            return ActionResult(success=False, message="slug is required")

        self._registry.reserve(slug)

        return ActionResult(success=True, message=f"Reserved: {slug}")

    def _resolve(self, params: Dict[str, Any]) -> ActionResult:
        """Resolve a slug."""
        slug = params.get("slug")

        if not slug:
            return ActionResult(success=False, message="slug is required")

        target = self._registry.resolve(slug)

        return ActionResult(success=target is not None, message=f"Resolved: {target}" if target else "Not found", data={"target": target})

    def _list(self, params: Dict[str, Any]) -> ActionResult:
        """List all mappings."""
        mappings = self._registry.list_all()
        return ActionResult(success=True, message=f"{len(mappings)} mappings", data={"mappings": mappings})

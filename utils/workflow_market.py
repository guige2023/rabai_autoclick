"""
Workflow Marketplace for RabAI AutoClick.

Provides workflow sharing, categorization, search, and bundle management.
"""

import os
import json
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from datetime import datetime
import re


CATEGORIES = [
    'automation',
    'productivity',
    'testing',
    'data_processing',
    'web_scraping',
    'image_processing',
    'file_management',
    'system_admin',
    'custom',
]

DEFAULT_TAGS = [
    'windows', 'macos', 'linux',
    'beginner', 'advanced', 'expert',
    'fast', 'reliable', 'experimental',
]


@dataclass
class WorkflowMetadata:
    """Workflow metadata for marketplace listing."""
    id: str
    name: str
    description: str
    version: str
    author: str
    category: str
    tags: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    download_count: int = 0
    rating: float = 0.0
    bundle_path: str = ""
    bundle_hash: str = ""
    size_bytes: int = 0
    min_rabai_version: str = ""
    dependencies: List[str] = field(default_factory=list)
    readme: str = ""
    screenshots: List[str] = field(default_factory=list)
    license: str = "MIT"


@dataclass
class WorkflowBundle:
    """A workflow bundle (.rabai file) representation."""
    metadata: WorkflowMetadata
    workflow_data: Dict[str, Any]
    resources: Dict[str, bytes] = field(default_factory=dict)
    dependencies: List[Dict[str, str]] = field(default_factory=list)


class WorkflowRegistry:
    """Local registry for workflow metadata."""
    
    def __init__(self, registry_path: Optional[Path] = None):
        if registry_path is None:
            self.registry_path = Path.home() / '.rabai' / 'workflow_registry.json'
        else:
            self.registry_path = Path(registry_path)
        self._workflows: Dict[str, WorkflowMetadata] = {}
        self._load_registry()
    
    def _load_registry(self) -> None:
        """Load registry from disk."""
        if not self.registry_path.exists():
            self.registry_path.parent.mkdir(parents=True, exist_ok=True)
            return
        
        try:
            with open(self.registry_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            for wf_data in data.get('workflows', []):
                metadata = WorkflowMetadata(
                    id=wf_data['id'],
                    name=wf_data['name'],
                    description=wf_data['description'],
                    version=wf_data['version'],
                    author=wf_data['author'],
                    category=wf_data['category'],
                    tags=wf_data.get('tags', []),
                    download_count=wf_data.get('download_count', 0),
                    rating=wf_data.get('rating', 0.0),
                    min_rabai_version=wf_data.get('min_rabai_version', ''),
                    dependencies=wf_data.get('dependencies', []),
                    readme=wf_data.get('readme', ''),
                    license=wf_data.get('license', 'MIT'),
                )
                
                if wf_data.get('created_at'):
                    metadata.created_at = datetime.fromisoformat(wf_data['created_at'])
                if wf_data.get('updated_at'):
                    metadata.updated_at = datetime.fromisoformat(wf_data['updated_at'])
                
                self._workflows[metadata.id] = metadata
                
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Failed to load registry: {e}")
    
    def _save_registry(self) -> None:
        """Save registry to disk."""
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        
        workflows_data = []
        for wf in self._workflows.values():
            wf_data = {
                'id': wf.id,
                'name': wf.name,
                'description': wf.description,
                'version': wf.version,
                'author': wf.author,
                'category': wf.category,
                'tags': wf.tags,
                'download_count': wf.download_count,
                'rating': wf.rating,
                'min_rabai_version': wf.min_rabai_version,
                'dependencies': wf.dependencies,
                'readme': wf.readme,
                'license': wf.license,
            }
            if wf.created_at:
                wf_data['created_at'] = wf.created_at.isoformat()
            if wf.updated_at:
                wf_data['updated_at'] = wf.updated_at.isoformat()
            workflows_data.append(wf_data)
        
        with open(self.registry_path, 'w', encoding='utf-8') as f:
            json.dump({'workflows': workflows_data, 'version': '1.0'}, f, indent=2)
    
    def register(self, metadata: WorkflowMetadata) -> None:
        """Register a workflow in the local registry."""
        metadata.updated_at = datetime.now()
        if metadata.id not in self._workflows:
            metadata.created_at = datetime.now()
        self._workflows[metadata.id] = metadata
        self._save_registry()
    
    def unregister(self, workflow_id: str) -> bool:
        """Unregister a workflow from the local registry."""
        if workflow_id in self._workflows:
            del self._workflows[workflow_id]
            self._save_registry()
            return True
        return False
    
    def get(self, workflow_id: str) -> Optional[WorkflowMetadata]:
        """Get workflow metadata by ID."""
        return self._workflows.get(workflow_id)
    
    def list_all(self) -> List[WorkflowMetadata]:
        """List all registered workflows."""
        return list(self._workflows.values())
    
    def list_by_category(self, category: str) -> List[WorkflowMetadata]:
        """List workflows by category."""
        return [wf for wf in self._workflows.values() if wf.category == category]
    
    def list_by_tag(self, tag: str) -> List[WorkflowMetadata]:
        """List workflows by tag."""
        return [wf for wf in self._workflows.values() if tag in wf.tags]
    
    def search(self, query: str) -> List[WorkflowMetadata]:
        """Search workflows by query string."""
        query_lower = query.lower()
        results = []
        
        for wf in self._workflows.values():
            if query_lower in wf.name.lower():
                results.append(wf)
                continue
            if query_lower in wf.description.lower():
                results.append(wf)
                continue
            if query_lower in wf.author.lower():
                results.append(wf)
                continue
            if any(query_lower in tag.lower() for tag in wf.tags):
                results.append(wf)
        
        return results


class WorkflowMarket:
    """
    Workflow marketplace for sharing and discovering workflows.
    
    Supports:
    - Categories: automation, productivity, testing, etc.
    - Tags and search functionality
    - Local workflow registry
    - Export/import workflow bundles (.rabai files)
    
    Usage:
        market = WorkflowMarket()
        market.import_bundle('/path/to/workflow.rabai')
        results = market.search('automation')
        results = market.list_by_category('productivity')
    """
    
    def __init__(self, registry_path: Optional[Path] = None, bundles_dir: Optional[Path] = None):
        self.registry = WorkflowRegistry(registry_path)
        
        if bundles_dir is None:
            self.bundles_dir = Path.home() / '.rabai' / 'workflow_bundles'
        else:
            self.bundles_dir = Path(bundles_dir)
        self.bundles_dir.mkdir(parents=True, exist_ok=True)
        
        self._workflows_dir = Path.home() / '.rabai' / 'workflows'
        self._workflows_dir.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def generate_workflow_id(name: str, author: str) -> str:
        """Generate a unique workflow ID."""
        content = f"{name}:{author}:{datetime.now().isoformat()}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def create_bundle(
        self,
        workflow_data: Dict[str, Any],
        name: str,
        author: str,
        description: str = "",
        category: str = "custom",
        tags: Optional[List[str]] = None,
        min_version: str = "",
        dependencies: Optional[List[str]] = None,
        resources: Optional[Dict[str, bytes]] = None,
    ) -> WorkflowBundle:
        """Create a new workflow bundle."""
        workflow_id = self.generate_workflow_id(name, author)
        
        metadata = WorkflowMetadata(
            id=workflow_id,
            name=name,
            description=description,
            version="1.0.0",
            author=author,
            category=category,
            tags=tags or [],
            created_at=datetime.now(),
            updated_at=datetime.now(),
            min_rabai_version=min_version,
            dependencies=dependencies or [],
        )
        
        bundle = WorkflowBundle(
            metadata=metadata,
            workflow_data=workflow_data,
            resources=resources or {},
            dependencies=[],
        )
        
        return bundle
    
    def export_bundle(
        self,
        bundle: WorkflowBundle,
        output_path: Optional[Path] = None,
    ) -> Path:
        """Export a workflow bundle to a .rabai file."""
        from .workflow_bundle import WorkflowBundleManager
        
        if output_path is None:
            output_path = self.bundles_dir / f"{bundle.metadata.id}.rabai"
        
        manager = WorkflowBundleManager()
        manager.save_bundle(bundle, output_path)
        
        # Update metadata with bundle info
        bundle.metadata.bundle_path = str(output_path)
        if output_path.exists():
            bundle.metadata.size_bytes = output_path.stat().st_size
        
        # Register in local registry
        self.registry.register(bundle.metadata)
        
        return output_path
    
    def import_bundle(self, bundle_path: Path) -> WorkflowBundle:
        """Import a workflow bundle from a .rabai file."""
        from .workflow_bundle import WorkflowBundleManager
        
        manager = WorkflowBundleManager()
        bundle = manager.load_bundle(bundle_path)
        
        # Register in local registry
        self.registry.register(bundle.metadata)
        
        return bundle
    
    def list_categories(self) -> List[str]:
        """List available workflow categories."""
        return CATEGORIES.copy()
    
    def list_tags(self) -> List[str]:
        """List all tags used by workflows plus default tags."""
        tags: Set[str] = set(DEFAULT_TAGS)
        for wf in self.registry.list_all():
            tags.update(wf.tags)
        return sorted(tags)
    
    def search(self, query: str) -> List[WorkflowMetadata]:
        """Search workflows by query."""
        return self.registry.search(query)
    
    def list_by_category(self, category: str) -> List[WorkflowMetadata]:
        """List workflows by category."""
        return self.registry.list_by_category(category)
    
    def list_by_tag(self, tag: str) -> List[WorkflowMetadata]:
        """List workflows by tag."""
        return self.registry.list_by_tag(tag)
    
    def list_all(self) -> List[WorkflowMetadata]:
        """List all available workflows."""
        return self.registry.list_all()
    
    def get_workflow(self, workflow_id: str) -> Optional[WorkflowMetadata]:
        """Get workflow metadata by ID."""
        return self.registry.get(workflow_id)
    
    def rate_workflow(self, workflow_id: str, rating: float) -> bool:
        """Rate a workflow (1-5 stars)."""
        metadata = self.registry.get(workflow_id)
        if not metadata:
            return False
        
        # Update weighted rating
        old_count = metadata.download_count
        old_rating = metadata.rating
        metadata.download_count += 1
        metadata.rating = (old_rating * old_count + rating) / metadata.download_count
        metadata.updated_at = datetime.now()
        
        self.registry.register(metadata)
        return True
    
    def increment_download(self, workflow_id: str) -> bool:
        """Increment download count for a workflow."""
        metadata = self.registry.get(workflow_id)
        if not metadata:
            return False
        
        metadata.download_count += 1
        metadata.updated_at = datetime.now()
        self.registry.register(metadata)
        return True
    
    def validate_bundle(self, bundle_path: Path) -> tuple[bool, Optional[str]]:
        """Validate a workflow bundle."""
        from .workflow_bundle import WorkflowBundleManager
        
        manager = WorkflowBundleManager()
        return manager.validate_bundle(bundle_path)
    
    def get_registry_path(self) -> Path:
        """Get the path to the local registry."""
        return self.registry.registry_path
    
    def get_bundles_directory(self) -> Path:
        """Get the bundles directory."""
        return self.bundles_dir

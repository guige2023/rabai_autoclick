"""
Workflow Bundle Manager for RabAI AutoClick.

Provides bundling, extraction, and validation of workflow bundles in .rabai format.
"""

import os
import json
import zipfile
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import tempfile
import shutil


BUNDLE_VERSION = "1.0"
REQUIRED_MANIFEST_FIELDS = ['name', 'version', 'workflow_data']
ALLOWED_EXTENSIONS = {'.json', '.py', '.txt', '.md', '.png', '.jpg', '.jpeg', '.gif', '.yaml', '.yml'}


@dataclass
class BundleManifest:
    """Manifest data stored in bundle."""
    name: str
    version: str
    author: str
    description: str
    category: str
    tags: List[str]
    bundle_version: str
    created_at: str
    workflow_id: str
    min_rabai_version: str
    dependencies: List[Dict[str, str]]
    checksum: str


class WorkflowBundleManager:
    """
    Manager for workflow bundles (.rabai files).
    
    .rabai files are ZIP archives containing:
    - manifest.json: Bundle metadata
    - workflow.json: The workflow definition
    - resources/: Additional resources (images, data files, etc.)
    - dependencies/: Required dependencies
    
    Usage:
        manager = WorkflowBundleManager()
        manager.save_bundle(bundle, '/path/to/output.rabai')
        bundle = manager.load_bundle('/path/to/bundle.rabai')
        valid, error = manager.validate_bundle('/path/to/bundle.rabai')
    """
    
    def __init__(self):
        self.bundle_version = BUNDLE_VERSION
    
    def _compute_checksum(self, data: bytes) -> str:
        """Compute SHA256 checksum of data."""
        return hashlib.sha256(data).hexdigest()
    
    def _create_manifest(
        self,
        name: str,
        version: str,
        author: str,
        description: str,
        category: str,
        tags: List[str],
        workflow_id: str,
        min_version: str,
        dependencies: List[Dict],
        workflow_data: Dict,
    ) -> BundleManifest:
        """Create a bundle manifest."""
        # Create a checksum of the workflow data
        workflow_json = json.dumps(workflow_data, sort_keys=True)
        checksum = self._compute_checksum(workflow_json.encode())
        
        return BundleManifest(
            name=name,
            version=version,
            author=author,
            description=description,
            category=category,
            tags=tags,
            bundle_version=self.bundle_version,
            created_at=datetime.now().isoformat(),
            workflow_id=workflow_id,
            min_rabai_version=min_version,
            dependencies=dependencies,
            checksum=checksum,
        )
    
    def _manifest_to_dict(self, manifest: BundleManifest) -> Dict:
        """Convert manifest to dictionary."""
        return {
            'name': manifest.name,
            'version': manifest.version,
            'author': manifest.author,
            'description': manifest.description,
            'category': manifest.category,
            'tags': manifest.tags,
            'bundle_version': manifest.bundle_version,
            'created_at': manifest.created_at,
            'workflow_id': manifest.workflow_id,
            'min_rabai_version': manifest.min_rabai_version,
            'dependencies': manifest.dependencies,
            'checksum': manifest.checksum,
        }
    
    def _dict_to_manifest(self, data: Dict) -> BundleManifest:
        """Convert dictionary to manifest."""
        return BundleManifest(
            name=data['name'],
            version=data['version'],
            author=data['author'],
            description=data.get('description', ''),
            category=data.get('category', 'custom'),
            tags=data.get('tags', []),
            bundle_version=data.get('bundle_version', '1.0'),
            created_at=data.get('created_at', ''),
            workflow_id=data.get('workflow_id', ''),
            min_rabai_version=data.get('min_rabai_version', ''),
            dependencies=data.get('dependencies', []),
            checksum=data.get('checksum', ''),
        )
    
    def save_bundle(
        self,
        bundle,
        output_path: Path,
        include_resources: bool = True,
    ) -> Path:
        """
        Save a workflow bundle to a .rabai file.
        
        Args:
            bundle: WorkflowBundle object to save
            output_path: Path where the .rabai file will be written
            include_resources: Whether to include resources in the bundle
            
        Returns:
            Path to the created bundle file
        """
        from .workflow_market import WorkflowBundle, WorkflowMetadata
        
        output_path = Path(output_path)
        
        # Create manifest
        manifest = self._create_manifest(
            name=bundle.metadata.name,
            version=bundle.metadata.version,
            author=bundle.metadata.author,
            description=bundle.metadata.description,
            category=bundle.metadata.category,
            tags=bundle.metadata.tags,
            workflow_id=bundle.metadata.id,
            min_version=bundle.metadata.min_rabai_version,
            dependencies=bundle.dependencies,
            workflow_data=bundle.workflow_data,
        )
        
        # Create zip file
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Add manifest
            manifest_json = json.dumps(self._manifest_to_dict(manifest), indent=2)
            zf.writestr('manifest.json', manifest_json)
            
            # Add workflow data
            workflow_json = json.dumps(bundle.workflow_data, indent=2)
            zf.writestr('workflow.json', workflow_json)
            
            # Add README if exists in metadata
            if bundle.metadata.readme:
                zf.writestr('README.md', bundle.metadata.readme)
            
            # Add resources
            if include_resources and bundle.resources:
                for resource_name, resource_data in bundle.resources.items():
                    # Validate extension
                    ext = Path(resource_name).suffix.lower()
                    if ext in ALLOWED_EXTENSIONS:
                        zf.writestr(f'resources/{resource_name}', resource_data)
            
            # Add dependencies info
            if bundle.dependencies:
                deps_json = json.dumps(bundle.dependencies, indent=2)
                zf.writestr('dependencies.json', deps_json)
        
        return output_path
    
    def load_bundle(self, bundle_path: Path) -> 'WorkflowBundle':
        """
        Load a workflow bundle from a .rabai file.
        
        Args:
            bundle_path: Path to the .rabai file
            
        Returns:
            WorkflowBundle object
            
        Raises:
            FileNotFoundError: If bundle doesn't exist
            ValueError: If bundle is invalid
        """
        from .workflow_market import WorkflowBundle, WorkflowMetadata
        
        bundle_path = Path(bundle_path)
        
        if not bundle_path.exists():
            raise FileNotFoundError(f"Bundle not found: {bundle_path}")
        
        resources = {}
        dependencies = []
        workflow_data = {}
        readme = ""
        
        with zipfile.ZipFile(bundle_path, 'r') as zf:
            # Validate bundle structure
            namelist = zf.namelist()
            
            if 'manifest.json' not in namelist:
                raise ValueError("Invalid bundle: missing manifest.json")
            if 'workflow.json' not in namelist:
                raise ValueError("Invalid bundle: missing workflow.json")
            
            # Load manifest
            manifest_data = json.loads(zf.read('manifest.json'))
            manifest = self._dict_to_manifest(manifest_data)
            
            # Validate checksum
            workflow_json = zf.read('workflow.json')
            computed_checksum = self._compute_checksum(workflow_json)
            if computed_checksum != manifest.checksum:
                raise ValueError("Bundle checksum validation failed")
            
            # Load workflow data
            workflow_data = json.loads(workflow_json)
            
            # Load resources
            for name in namelist:
                if name.startswith('resources/'):
                    resources[name[10:]] = zf.read(name)
            
            # Load dependencies
            if 'dependencies.json' in namelist:
                dependencies = json.loads(zf.read('dependencies.json'))
            
            # Load README if exists
            if 'README.md' in namelist:
                readme = zf.read('README.md').decode('utf-8', errors='ignore')
        
        # Create metadata from manifest
        metadata = WorkflowMetadata(
            id=manifest.workflow_id,
            name=manifest.name,
            description=manifest.description,
            version=manifest.version,
            author=manifest.author,
            category=manifest.category,
            tags=manifest.tags,
            created_at=datetime.fromisoformat(manifest.created_at) if manifest.created_at else None,
            updated_at=datetime.now(),
            bundle_path=str(bundle_path),
            bundle_hash=manifest.checksum,
            size_bytes=bundle_path.stat().st_size,
            min_rabai_version=manifest.min_rabai_version,
            dependencies=[d.get('name', '') for d in manifest.dependencies],
            readme=readme,
        )
        
        return WorkflowBundle(
            metadata=metadata,
            workflow_data=workflow_data,
            resources=resources,
            dependencies=dependencies,
        )
    
    def validate_bundle(self, bundle_path: Path) -> Tuple[bool, Optional[str]]:
        """
        Validate a workflow bundle without fully loading it.
        
        Args:
            bundle_path: Path to the .rabai file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        bundle_path = Path(bundle_path)
        
        if not bundle_path.exists():
            return False, f"Bundle not found: {bundle_path}"
        
        if not bundle_path.suffix.lower() == '.rabai':
            return False, f"Invalid bundle extension: {bundle_path.suffix}"
        
        try:
            with zipfile.ZipFile(bundle_path, 'r') as zf:
                namelist = zf.namelist()
                
                # Check required files
                if 'manifest.json' not in namelist:
                    return False, "Missing required file: manifest.json"
                if 'workflow.json' not in namelist:
                    return False, "Missing required file: workflow.json"
                
                # Validate manifest JSON
                manifest_data = json.loads(zf.read('manifest.json'))
                for field in REQUIRED_MANIFEST_FIELDS:
                    if field == 'workflow_data':
                        continue
                    if field not in manifest_data:
                        return False, f"Missing required field in manifest: {field}"
                
                # Validate workflow data
                workflow_data = json.loads(zf.read('workflow.json'))
                if not isinstance(workflow_data, dict):
                    return False, "workflow.json must contain a JSON object"
                
                # Check bundle version compatibility
                bundle_version = manifest_data.get('bundle_version', '1.0')
                major_version = bundle_version.split('.')[0]
                current_major = self.bundle_version.split('.')[0]
                if major_version != current_major:
                    return False, f"Incompatible bundle version: {bundle_version} (supported: {self.bundle_version})"
                
                # Validate file extensions in bundle
                for name in namelist:
                    if name.startswith('resources/'):
                        ext = Path(name).suffix.lower()
                        if ext not in ALLOWED_EXTENSIONS:
                            return False, f"Unsupported file extension in bundle: {ext}"
                
                # Validate checksum
                workflow_json = zf.read('workflow.json')
                computed_checksum = self._compute_checksum(workflow_json)
                stored_checksum = manifest_data.get('checksum', '')
                if computed_checksum != stored_checksum:
                    return False, "Checksum validation failed"
                
        except zipfile.BadZipFile:
            return False, "Invalid ZIP format"
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON: {e}"
        except Exception as e:
            return False, f"Validation error: {e}"
        
        return True, None
    
    def extract_bundle(
        self,
        bundle_path: Path,
        extract_dir: Path,
        extract_resources: bool = True,
    ) -> Tuple[Path, Optional[str]]:
        """
        Extract a workflow bundle to a directory.
        
        Args:
            bundle_path: Path to the .rabai file
            extract_dir: Directory to extract to
            extract_resources: Whether to extract resources
            
        Returns:
            Tuple of (extracted directory path, error message or None)
        """
        bundle_path = Path(bundle_path)
        extract_dir = Path(extract_dir)
        
        # Validate first
        valid, error = self.validate_bundle(bundle_path)
        if not valid:
            return extract_dir, error
        
        try:
            # Create extraction directory
            bundle_name = bundle_path.stem
            target_dir = extract_dir / bundle_name
            target_dir.mkdir(parents=True, exist_ok=True)
            
            with zipfile.ZipFile(bundle_path, 'r') as zf:
                namelist = zf.namelist()
                
                for name in namelist:
                    # Skip resources if not requested
                    if not extract_resources and name.startswith('resources/'):
                        continue
                    
                    # Extract file
                    target_path = target_dir / name
                    
                    if name.endswith('/'):
                        # Directory
                        target_path.mkdir(parents=True, exist_ok=True)
                    else:
                        target_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(target_path, 'wb') as f:
                            f.write(zf.read(name))
            
            return target_dir, None
            
        except Exception as e:
            return extract_dir, f"Extraction failed: {e}"
    
    def create_bundle_from_directory(
        self,
        source_dir: Path,
        output_path: Path,
        name: str = "",
        author: str = "unknown",
        description: str = "",
        category: str = "custom",
        tags: Optional[List[str]] = None,
    ) -> Tuple[Path, Optional[str]]:
        """
        Create a workflow bundle from a directory.
        
        The directory should contain:
        - workflow.json (required): The workflow definition
        - manifest.json (optional): Additional metadata
        - resources/ (optional): Additional resources
        - README.md (optional): Documentation
        
        Args:
            source_dir: Directory containing workflow files
            output_path: Path for the output .rabai file
            name: Workflow name (defaults to directory name)
            author: Workflow author
            description: Workflow description
            category: Workflow category
            tags: Workflow tags
            
        Returns:
            Tuple of (output path, error message or None)
        """
        from .workflow_market import WorkflowBundle, WorkflowMetadata, WorkflowMarket
        
        source_dir = Path(source_dir)
        
        if not source_dir.exists():
            return output_path, f"Source directory not found: {source_dir}"
        
        workflow_json_path = source_dir / 'workflow.json'
        if not workflow_json_path.exists():
            return output_path, "Missing required file: workflow.json"
        
        try:
            # Load workflow data
            with open(workflow_json_path, 'r', encoding='utf-8') as f:
                workflow_data = json.load(f)
            
            # Load manifest if exists
            manifest_path = source_dir / 'manifest.json'
            manifest_data = {}
            if manifest_path.exists():
                with open(manifest_path, 'r', encoding='utf-8') as f:
                    manifest_data = json.load(f)
            
            # Load resources
            resources = {}
            resources_dir = source_dir / 'resources'
            if resources_dir.exists():
                for resource_file in resources_dir.rglob('*'):
                    if resource_file.is_file():
                        ext = resource_file.suffix.lower()
                        if ext in ALLOWED_EXTENSIONS:
                            rel_path = resource_file.relative_to(resources_dir)
                            with open(resource_file, 'rb') as f:
                                resources[str(rel_path)] = f.read()
            
            # Load README if exists
            readme = ""
            readme_path = source_dir / 'README.md'
            if readme_path.exists():
                with open(readme_path, 'r', encoding='utf-8') as f:
                    readme = f.read()
            
            # Generate workflow ID
            workflow_name = name or manifest_data.get('name', source_dir.name)
            workflow_id = WorkflowMarket.generate_workflow_id(workflow_name, author)
            
            # Create metadata
            metadata = WorkflowMetadata(
                id=workflow_id,
                name=workflow_name,
                description=description or manifest_data.get('description', ''),
                version=manifest_data.get('version', '1.0.0'),
                author=author or manifest_data.get('author', 'unknown'),
                category=category or manifest_data.get('category', 'custom'),
                tags=tags or manifest_data.get('tags', []),
                created_at=datetime.now(),
                updated_at=datetime.now(),
                min_rabai_version=manifest_data.get('min_rabai_version', ''),
                readme=readme,
            )
            
            # Create bundle
            bundle = WorkflowBundle(
                metadata=metadata,
                workflow_data=workflow_data,
                resources=resources,
                dependencies=manifest_data.get('dependencies', []),
            )
            
            # Save bundle
            self.save_bundle(bundle, output_path)
            
            return output_path, None
            
        except json.JSONDecodeError as e:
            return output_path, f"Invalid JSON: {e}"
        except Exception as e:
            return output_path, f"Error creating bundle: {e}"
    
    def list_bundle_contents(self, bundle_path: Path) -> List[str]:
        """
        List contents of a bundle without extracting it.
        
        Args:
            bundle_path: Path to the .rabai file
            
        Returns:
            List of file paths in the bundle
        """
        bundle_path = Path(bundle_path)
        
        if not bundle_path.exists():
            return []
        
        try:
            with zipfile.ZipFile(bundle_path, 'r') as zf:
                return zf.namelist()
        except zipfile.BadZipFile:
            return []
    
    def get_bundle_info(self, bundle_path: Path) -> Optional[Dict]:
        """
        Get basic info about a bundle without fully loading it.
        
        Args:
            bundle_path: Path to the .rabai file
            
        Returns:
            Dictionary with bundle info or None
        """
        bundle_path = Path(bundle_path)
        
        if not bundle_path.exists():
            return None
        
        try:
            with zipfile.ZipFile(bundle_path, 'r') as zf:
                manifest_data = json.loads(zf.read('manifest.json'))
                return {
                    'name': manifest_data.get('name', 'Unknown'),
                    'version': manifest_data.get('version', 'Unknown'),
                    'author': manifest_data.get('author', 'Unknown'),
                    'workflow_id': manifest_data.get('workflow_id', ''),
                    'bundle_version': manifest_data.get('bundle_version', 'Unknown'),
                    'created_at': manifest_data.get('created_at', ''),
                    'file_count': len(zf.namelist()),
                    'file_size': bundle_path.stat().st_size,
                }
        except Exception:
            return None

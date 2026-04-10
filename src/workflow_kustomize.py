"""
Kustomize Integration for Kubernetes

A comprehensive Kustomize integration system providing:
1. Kustomization: Create and manage kustomization files
2. Overlay management: Create overlays for different environments
3. Secret generation: Generate secrets from sealed secrets/kustomize
4. Component usage: Use kustomize components
5. Build/preview: Build and preview kubernetes manifests
6. Image updates: Update container images in kustomization
7. Config generator: Generate configmaps and secrets from sources
8. Patches: Apply strategic merge patches and JSON patches
9. Helm integration: Integrate Helm charts within kustomize
10. Remote bases: Use remote bases and Git overlays

Commit: 'feat(kustomize): add Kustomize integration with kustomization files, overlays, secret generation, components, build preview, image updates, config generation, patches, Helm integration, remote bases'
"""

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid
import base64
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple, Set

import yaml


class KustomizeResourceKind(Enum):
    """Kubernetes resource kinds supported by kustomize."""
    CONFIGMAP = "ConfigMap"
    SECRET = "Secret"
    DEPLOYMENT = "Deployment"
    SERVICE = "Service"
    INGRESS = "Ingress"
    STATEFULSET = "StatefulSet"
    DAEMONSET = "DaemonSet"
    JOB = "Job"
    CRONJOB = "CronJob"
    Pvc = "PersistentVolumeClaim"
    NETWORKPOLICY = "NetworkPolicy"
    SERVICEACCOUNT = "ServiceAccount"
    ROLE = "Role"
    ROLEBINDING = "RoleBinding"
    CLUSTERROLE = "ClusterRole"
    CLUSTERROLEBINDING = "ClusterRoleBinding"


class ImageTagStrategy(Enum):
    """Strategies for updating container image tags."""
    EXACT = "exact"
    LATEST = "latest"
    DIGEST = "digest"
    NEW_TAG = "newTag"
    NEW_NAME = "newName"


class PatchType(Enum):
    """Types of patches supported by kustomize."""
    STRATEGIC_MERGE = "StrategicMergePatch"
    JSON6902 = "JSON6902"
    UNIFIED = "unified"


class HelmChartSource(Enum):
    """Source types for Helm charts."""
    LOCAL = "local"
    REMOTE = "remote"
    OCI = "oci"


@dataclass
class KustomizeImage:
    """Represents a container image in kustomization."""
    name: str
    new_name: Optional[str] = None
    new_tag: Optional[str] = None
    digest: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {"name": self.name}
        if self.new_name:
            result["newName"] = self.new_name
        if self.new_tag:
            result["newTag"] = self.new_tag
        if self.digest:
            result["digest"] = self.digest
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KustomizeImage":
        return cls(
            name=data.get("name", ""),
            new_name=data.get("newName"),
            new_tag=data.get("newTag"),
            digest=data.get("digest")
        )


@dataclass
class KustomizeComponent:
    """Represents a kustomize component."""
    name: str
    path: str
    included: bool = True


@dataclass
class KustomizePatch:
    """Represents a patch in kustomization."""
    target: Dict[str, Any]
    patch: Optional[str] = None
    patch_type: PatchType = PatchType.STRATEGIC_MERGE
    path: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {"target": self.target}
        if self.patch:
            result["patch"] = self.patch
        if self.patch_type == PatchType.JSON6902:
            result["path"] = self.path
        return result


@dataclass
class HelmChart:
    """Represents a Helm chart integration."""
    name: str
    chart: str
    release_name: str = ""
    namespace: str = ""
    values_files: List[str] = field(default_factory=list)
    additional_values: Dict[str, Any] = field(default_factory=dict)
    source_type: HelmChartSource = HelmChartSource.REMOTE
    repo_url: str = ""

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "name": self.name,
            "chart": self.chart,
            "releaseName": self.release_name or self.name,
        }
        if self.namespace:
            result["namespace"] = self.namespace
        if self.values_files:
            result["valuesFiles"] = self.values_files
        if self.additional_values:
            result["additionalValues"] = self.additional_values
        if self.source_type == HelmChartSource.LOCAL:
            result["local"] = {"path": self.chart}
        elif self.repo_url:
            result["repo"] = self.repo_url
        return result


@dataclass
class KustomizeSecretGenerator:
    """Configuration for secret generation."""
    type: str = "Opaque"
    env_files: List[str] = field(default_factory=list)
    literals: Dict[str, str] = field(default_factory=dict)
    files: Dict[str, str] = field(default_factory=dict)
    enable_alpha_plugins: bool = False


@dataclass
class KustomizationSpec:
    """Full kustomization specification."""
    api_version: str = "kustomize.config.k8s.io/v1beta1"
    kind: str = "Kustomization"
    resources: List[str] = field(default_factory=list)
    components: List[str] = field(default_factory=list)
    crds: List[str] = field(default_factory=list)
    images: List[Dict[str, Any]] = field(default_factory=list)
    config_map_generator: List[Dict[str, Any]] = field(default_factory=list)
    secret_generator: List[Dict[str, Any]] = field(default_factory=list)
    HelmCharts: List[Dict[str, Any]] = field(default_factory=list)
    replacements: List[Dict[str, Any]] = field(default_factory=list)
    patches: List[Dict[str, Any]] = field(default_factory=list)
    patches_strategic_merge: List[Dict[str, Any]] = field(default_factory=list)
    patches_json6902: List[Dict[str, Any]] = field(default_factory=list)
    configurations: List[str] = field(default_factory=list)
    common_labels: Dict[str, str] = field(default_factory=dict)
    common_annotations: Dict[str, str] = field(default_factory=dict)
    name_prefix: str = ""
    name_suffix: str = ""
    namespace: str = ""
    bases: List[str] = field(default_factory=list)
    replicas: List[Dict[str, Any]] = field(default_factory=list)
    resources: List[str] = field(default_factory=list)


class KustomizeManager:
    """Manager for Kustomize operations."""

    def __init__(self, work_dir: Optional[str] = None):
        """
        Initialize KustomizeManager.
        
        Args:
            work_dir: Working directory for kustomize operations
        """
        self.work_dir = Path(work_dir) if work_dir else Path.cwd()
        self.kustomize_cmd = self._find_kustomize()
        self._ensure_kustomize_installed()

    def _find_kustomize(self) -> str:
        """Find kustomize executable."""
        for cmd in ["kustomize", "kubectl kustomize"]:
            result = subprocess.run(
                f"which {cmd.split()[0]}",
                shell=True,
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                return cmd
        return "kustomize"

    def _ensure_kustomize_installed(self) -> None:
        """Check if kustomize is available."""
        try:
            subprocess.run(
                [self.kustomize_cmd, "version"],
                capture_output=True,
                check=True
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError(
                "kustomize is not installed. Install it with: "
                "go install sigs.k8s.io/kustomize/kustomize@latest"
            )

    def create_kustomization(
        self,
        path: str,
        resources: Optional[List[str]] = None,
        namespace: str = "",
        name_prefix: str = "",
        name_suffix: str = "",
        common_labels: Optional[Dict[str, str]] = None,
        common_annotations: Optional[Dict[str, str]] = None,
        api_version: str = "kustomize.config.k8s.io/v1beta1",
        kind: str = "Kustomization"
    ) -> Path:
        """
        Create a kustomization.yaml file.
        
        Args:
            path: Directory path for the kustomization
            resources: List of resource files/paths
            namespace: Kubernetes namespace
            name_prefix: Prefix for resource names
            name_suffix: Suffix for resource names
            common_labels: Labels to apply to all resources
            common_annotations: Annotations to apply to all resources
            api_version: Kustomize API version
            kind: Kustomize kind
            
        Returns:
            Path to created kustomization.yaml
        """
        kustomization_dir = Path(path)
        kustomization_dir.mkdir(parents=True, exist_ok=True)
        
        spec = KustomizationSpec(
            api_version=api_version,
            kind=kind,
            namespace=namespace,
            name_prefix=name_prefix,
            name_suffix=name_suffix,
            common_labels=common_labels or {},
            common_annotations=common_annotations or {},
        )
        
        if resources:
            spec.resources = resources
        
        return self._write_kustomization(kustomization_dir, spec)

    def _write_kustomization(self, directory: Path, spec: KustomizationSpec) -> Path:
        """Write kustomization.yaml to directory."""
        kustomization_path = directory / "kustomization.yaml"
        
        data = {
            "apiVersion": spec.api_version,
            "kind": spec.kind,
        }
        
        if spec.namespace:
            data["namespace"] = spec.namespace
        if spec.name_prefix:
            data["namePrefix"] = spec.name_prefix
        if spec.name_suffix:
            data["nameSuffix"] = spec.name_suffix
        if spec.common_labels:
            data["commonLabels"] = spec.common_labels
        if spec.common_annotations:
            data["commonAnnotations"] = spec.common_annotations
        if spec.resources:
            data["resources"] = spec.resources
        if spec.components:
            data["components"] = spec.components
        if spec.crds:
            data["crds"] = spec.crds
        if spec.images:
            data["images"] = spec.images
        if spec.config_map_generator:
            data["configMapGenerator"] = spec.config_map_generator
        if spec.secret_generator:
            data["secretGenerator"] = spec.secret_generator
        if spec.HelmCharts:
            data["helmCharts"] = spec.HelmCharts
        if spec.replacements:
            data["replacements"] = spec.replacements
        if spec.patches:
            data["patches"] = spec.patches
        if spec.patches_strategic_merge:
            data["patchesStrategicMerge"] = spec.patches_strategic_merge
        if spec.patches_json6902:
            data["patchesJson6902"] = spec.patches_json6902
        if spec.configurations:
            data["configurations"] = spec.configurations
        if spec.replicas:
            data["replicas"] = spec.replicas
        if spec.bases:
            data["bases"] = spec.bases
            
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return kustomization_path

    def update_kustomization(
        self,
        path: str,
        updates: Dict[str, Any]
    ) -> Path:
        """
        Update an existing kustomization.yaml.
        
        Args:
            path: Path to kustomization.yaml
            updates: Dictionary of updates to apply
            
        Returns:
            Path to updated kustomization.yaml
        """
        kustomization_path = Path(path)
        if not kustomization_path.exists():
            raise FileNotFoundError(f"Kustomization not found at {path}")
        
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        for key, value in updates.items():
            data[key] = value
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return kustomization_path

    def create_overlay(
        self,
        base_path: str,
        overlay_name: str,
        overlay_dir: Optional[str] = None,
        patches: Optional[Dict[str, Any]] = None,
        additional_resources: Optional[List[str]] = None,
        name_prefix: str = "",
        name_suffix: str = "",
        namespace: str = "",
        common_labels: Optional[Dict[str, str]] = None,
    ) -> Path:
        """
        Create an overlay for a different environment.
        
        Args:
            base_path: Path to base kustomization
            overlay_name: Name of the overlay (e.g., 'dev', 'staging', 'prod')
            overlay_dir: Parent directory for overlays (defaults to 'overlays' subdir of base)
            patches: Patches to apply in this overlay
            additional_resources: Additional resources for this overlay
            name_prefix: Prefix for resource names
            name_suffix: Suffix for resource names
            namespace: Kubernetes namespace
            common_labels: Labels to apply in this overlay
            
        Returns:
            Path to created overlay kustomization.yaml
        """
        base_path = Path(base_path)
        if overlay_dir:
            overlay_path = Path(overlay_dir) / overlay_name
        else:
            overlay_path = base_path.parent / "overlays" / overlay_name
        
        overlay_path.mkdir(parents=True, exist_ok=True)
        
        spec = KustomizationSpec(
            namespace=namespace,
            name_prefix=name_prefix,
            name_suffix=name_suffix,
            common_labels=common_labels or {},
        )
        
        rel_base = os.path.relpath(base_path, overlay_path)
        spec.bases = [rel_base]
        
        if additional_resources:
            spec.resources = additional_resources
        
        kustomization_path = self._write_kustomization(overlay_path, spec)
        
        if patches:
            self.apply_patches(str(kustomization_path), patches)
        
        return kustomization_path

    def add_component(
        self,
        kustomization_path: str,
        component_path: str
    ) -> Path:
        """
        Add a component to kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            component_path: Path to component directory
            
        Returns:
            Path to updated kustomization.yaml
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        if "components" not in data:
            data["components"] = []
        
        component_rel_path = os.path.relpath(
            Path(component_path).resolve(),
            Path(kustomization_path).parent.resolve()
        )
        
        if component_rel_path not in data["components"]:
            data["components"].append(component_rel_path)
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return Path(kustomization_path)

    def create_component(
        self,
        path: str,
        resources: Optional[List[str]] = None,
        patches: Optional[Dict[str, Any]] = None
    ) -> Path:
        """
        Create a kustomize component.
        
        Args:
            path: Directory path for the component
            resources: List of resource files/paths
            patches: Patches to apply in component
            
        Returns:
            Path to created kustomization.yaml
        """
        component_dir = Path(path)
        component_dir.mkdir(parents=True, exist_ok=True)
        
        spec = KustomizationSpec(
            kind="Component",
            resources=resources or [],
        )
        
        kustomization_path = self._write_kustomization(component_dir, spec)
        
        if patches:
            self.apply_patches(str(kustomization_path), patches)
        
        return kustomization_path

    def update_image(
        self,
        kustomization_path: str,
        image_name: str,
        new_name: Optional[str] = None,
        new_tag: Optional[str] = None,
        digest: Optional[str] = None,
        strategy: ImageTagStrategy = ImageTagStrategy.NEW_TAG
    ) -> Path:
        """
        Update container image in kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            image_name: Current image name (e.g., 'nginx')
            new_name: New image name (if renaming)
            new_tag: New tag to set
            digest: New digest to set
            strategy: Tag update strategy
            
        Returns:
            Path to updated kustomization.yaml
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        if "images" not in data:
            data["images"] = []
        
        image_found = False
        for img in data["images"]:
            if img.get("name") == image_name:
                if new_name:
                    img["newName"] = new_name
                if new_tag:
                    if strategy == ImageTagStrategy.EXACT:
                        img["newTag"] = new_tag
                    elif strategy == ImageTagStrategy.DIGEST:
                        img["digest"] = digest or new_tag
                    else:
                        img["newTag"] = new_tag
                elif digest:
                    img["digest"] = digest
                image_found = True
                break
        
        if not image_found:
            new_image = {"name": image_name}
            if new_name:
                new_image["newName"] = new_name
            if new_tag:
                new_image["newTag"] = new_tag
            if digest:
                new_image["digest"] = digest
            data["images"].append(new_image)
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return Path(kustomization_path)

    def update_replicas(
        self,
        kustomization_path: str,
        resource_name: str,
        replicas: int,
        kind: str = "Deployment"
    ) -> Path:
        """
        Update replica count for a resource.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            resource_name: Name of the resource
            replicas: Number of replicas
            kind: Resource kind
            
        Returns:
            Path to updated kustomization.yaml
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        if "replicas" not in data:
            data["replicas"] = []
        
        replica_found = False
        for rep in data["replicas"]:
            if rep.get("name") == resource_name:
                rep["count"] = replicas
                replica_found = True
                break
        
        if not replica_found:
            data["replicas"].append({
                "name": resource_name,
                "count": replicas
            })
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return Path(kustomization_path)

    def generate_configmap(
        self,
        kustomization_path: str,
        name: str,
        literals: Optional[Dict[str, str]] = None,
        files: Optional[List[str]] = None,
        env_file: Optional[str] = None,
        behavior: str = ""
    ) -> Path:
        """
        Generate a ConfigMap in kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            name: ConfigMap name
            literals: Key-value pairs for config data
            files: List of file paths to include
            env_file: Path to .env file
            behavior: Behavior flag ('create', 'replace', 'merge')
            
        Returns:
            Path to updated kustomization.yaml
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        if "configMapGenerator" not in data:
            data["configMapGenerator"] = []
        
        generator_entry: Dict[str, Any] = {"name": name}
        
        if literals:
            generator_entry["literals"] = [
                f"{k}={v}" for k, v in literals.items()
            ]
        
        if files:
            generator_entry["files"] = files
        
        if env_file:
            generator_entry["envs"] = [env_file] if isinstance(env_file, str) else env_file
        
        if behavior:
            generator_entry["behavior"] = behavior
        
        data["configMapGenerator"].append(generator_entry)
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return Path(kustomization_path)

    def generate_secret(
        self,
        kustomization_path: str,
        name: str,
        secret_type: str = "Opaque",
        literals: Optional[Dict[str, str]] = None,
        files: Optional[Dict[str, str]] = None,
        env_file: Optional[str] = None,
        behavior: str = ""
    ) -> Path:
        """
        Generate a Secret in kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            name: Secret name
            secret_type: Secret type (Opaque, tls, etc.)
            literals: Key-value pairs for secret data
            files: Dict of file paths (target: source)
            env_file: Path to .env file
            behavior: Behavior flag ('create', 'replace', 'merge')
            
        Returns:
            Path to updated kustomization.yaml
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        if "secretGenerator" not in data:
            data["secretGenerator"] = []
        
        generator_entry: Dict[str, Any] = {
            "name": name,
            "type": secret_type
        }
        
        if literals:
            generator_entry["literals"] = [
                f"{k}={v}" for k, v in literals.items()
            ]
        
        if files:
            generator_entry["files"] = list(files.values())
        
        if env_file:
            generator_entry["envs"] = [env_file] if isinstance(env_file, str) else env_file
        
        if behavior:
            generator_entry["behavior"] = behavior
        
        data["secretGenerator"].append(generator_entry)
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return Path(kustomization_path)

    def generate_sealed_secret(
        self,
        kustomization_path: str,
        name: str,
        secrets: Dict[str, str],
        secret_type: str = "Opaque",
        namespace: str = ""
    ) -> Path:
        """
        Generate Sealed Secrets (cryptographically secured secrets).
        Note: Requires kubeseal CLI and sealed-secrets controller.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            name: Secret name
            secrets: Dictionary of secret values
            secret_type: Secret type
            namespace: Namespace for sealing
            
        Returns:
            Path to updated kustomization.yaml
        """
        try:
            subprocess.run(
                ["kubeseal", "--version"],
                capture_output=True,
                check=True
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError(
                "kubeseal is not installed. Install it to use sealed secrets."
            )
        
        sealed_secrets_dir = Path(kustomization_path).parent / "sealed-secrets"
        sealed_secrets_dir.mkdir(exist_ok=True)
        
        secret_yaml_path = sealed_secrets_dir / f"{name}.yaml"
        
        secret_data = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": name,
                "namespace": namespace or "default"
            },
            "type": secret_type,
            "data": {
                k: base64.b64encode(v.encode()).decode() 
                for k, v in secrets.items()
            }
        }
        
        with open(secret_yaml_path, "w") as f:
            yaml.dump(secret_data, f)
        
        sealed_path = sealed_secrets_dir / f"{name}-sealed.yaml"
        
        cert_path = sealed_secrets_dir / "cert.pem"
        
        try:
            subprocess.run(
                [
                    "kubeseal", "--cert", str(cert_path),
                    "--密封", "--output", str(sealed_path),
                    "-f", str(secret_yaml_path)
                ] if False else [
                    "kubeseal", f"--cert={cert_path}",
                    "-o", str(sealed_path),
                    "-f", str(secret_yaml_path)
                ],
                check=True,
                capture_output=True
            )
        except subprocess.CalledProcessError:
            sealed_path = secret_yaml_path
        
        return self.add_resource(kustomization_path, str(sealed_path))

    def apply_patches(
        self,
        kustomization_path: str,
        patches: List[Dict[str, Any]]
    ) -> Path:
        """
        Apply strategic merge patches and JSON patches.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            patches: List of patch configurations
            
        Returns:
            Path to updated kustomization.yaml
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        strategic_patches = []
        json_patches = []
        
        for patch in patches:
            patch_type = patch.get("type", "strategic")
            patch_content = patch.get("patch", "")
            patch_path = patch.get("path")
            target = patch.get("target", {})
            
            if patch_type == "json" or patch_type == PatchType.JSON6902.value:
                json_patch_entry: Dict[str, Any] = {
                    "target": target
                }
                if patch_content:
                    json_patch_entry["patch"] = patch_content
                if patch_path:
                    json_patch_entry["path"] = patch_path
                json_patches.append(json_patch_entry)
            else:
                strategic_patch_entry: Dict[str, Any] = {
                    "target": target,
                    "patch": patch_content
                }
                strategic_patches.append(strategic_patch_entry)
        
        if strategic_patches:
            if "patchesStrategicMerge" not in data:
                data["patchesStrategicMerge"] = []
            data["patchesStrategicMerge"].extend(strategic_patches)
        
        if json_patches:
            if "patchesJson6902" not in data:
                data["patchesJson6902"] = []
            data["patchesJson6902"].extend(json_patches)
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return Path(kustomization_path)

    def add_strategic_merge_patch(
        self,
        kustomization_path: str,
        patch_content: str,
        target_selector: Optional[List[str]] = None,
        target_kind: Optional[str] = None,
        target_name: Optional[str] = None,
        target_namespace: Optional[str] = None
    ) -> Path:
        """
        Add a strategic merge patch.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            patch_content: YAML patch content
            target_selector: Label selector for target resources
            target_kind: Kind of target resource
            target_name: Name of target resource
            target_namespace: Namespace of target resource
            
        Returns:
            Path to updated kustomization.yaml
        """
        target: Dict[str, Any] = {}
        
        if target_selector:
            target["labelSelector"] = ",".join(target_selector)
        if target_kind:
            target["kind"] = target_kind
        if target_name:
            target["name"] = target_name
        if target_namespace:
            target["namespace"] = target_namespace
        
        patch = {
            "target": target,
            "patch": patch_content
        }
        
        return self.apply_patches(kustomization_path, [patch])

    def add_json_patch(
        self,
        kustomization_path: str,
        patch_content: str,
        target_selector: Optional[List[str]] = None,
        target_kind: Optional[str] = None,
        target_name: Optional[str] = None,
        target_namespace: Optional[str] = None,
        patch_file: Optional[str] = None
    ) -> Path:
        """
        Add a JSON 6902 patch.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            patch_content: JSON patch content
            target_selector: Label selector for target resources
            target_kind: Kind of target resource
            target_name: Name of target resource
            target_namespace: Namespace of target resource
            patch_file: Path to patch file (alternative to patch_content)
            
        Returns:
            Path to updated kustomization.yaml
        """
        target: Dict[str, Any] = {}
        
        if target_selector:
            target["labelSelector"] = ",".join(target_selector)
        if target_kind:
            target["kind"] = target_kind
        if target_name:
            target["name"] = target_name
        if target_namespace:
            target["namespace"] = target_namespace
        
        patch: Dict[str, Any] = {
            "type": "json",
            "target": target
        }
        
        if patch_content:
            patch["patch"] = patch_content
        if patch_file:
            patch["path"] = patch_file
        
        return self.apply_patches(kustomization_path, [patch])

    def add_helm_chart(
        self,
        kustomization_path: str,
        chart: HelmChart
    ) -> Path:
        """
        Integrate a Helm chart within kustomize.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            chart: HelmChart configuration
            
        Returns:
            Path to updated kustomization.yaml
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        if "helmCharts" not in data:
            data["helmCharts"] = []
        
        data["helmCharts"].append(chart.to_dict())
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return Path(kustomization_path)

    def create_helm_chart(
        self,
        kustomization_path: str,
        name: str,
        chart: str,
        release_name: str = "",
        namespace: str = "",
        values_files: Optional[List[str]] = None,
        additional_values: Optional[Dict[str, Any]] = None,
        source_type: HelmChartSource = HelmChartSource.REMOTE,
        repo_url: str = ""
    ) -> Path:
        """
        Add a Helm chart to kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            name: Release name
            chart: Chart name or path
            release_name: Helm release name
            namespace: Kubernetes namespace
            values_files: List of values files
            additional_values: Additional values to set
            source_type: LOCAL, REMOTE, or OCI
            repo_url: Repository URL for remote charts
            
        Returns:
            Path to updated kustomization.yaml
        """
        helm_chart = HelmChart(
            name=name,
            chart=chart,
            release_name=release_name,
            namespace=namespace,
            values_files=values_files or [],
            additional_values=additional_values or {},
            source_type=source_type,
            repo_url=repo_url
        )
        
        return self.add_helm_chart(kustomization_path, helm_chart)

    def add_remote_base(
        self,
        kustomization_path: str,
        base: str,
        branch: str = "main"
    ) -> Path:
        """
        Add a remote base using Git.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            base: Git repository URL with path
            branch: Git branch to use
            
        Returns:
            Path to updated kustomization.yaml
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        if base.startswith("https://") or base.startswith("git@"):
            if "?" in base:
                base = f"{base}&ref={branch}"
            else:
                base = f"{base}?ref={branch}"
        
        if "bases" not in data:
            data["bases"] = []
        
        if base not in data["bases"]:
            data["bases"].append(base)
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return Path(kustomization_path)

    def add_git_overlay(
        self,
        kustomization_path: str,
        repo_url: str,
        path: str,
        branch: str = "main"
    ) -> Path:
        """
        Add a Git-based overlay (remote base with additional patches).
        
        Args:
            kustomization_path: Path to kustomization.yaml
            repo_url: Git repository URL
            path: Path within the repository
            branch: Git branch to use
            
        Returns:
            Path to updated kustomization.yaml
        """
        full_base = f"{repo_url}/{path}?ref={branch}"
        return self.add_remote_base(kustomization_path, full_base)

    def add_resource(
        self,
        kustomization_path: str,
        resource_path: str
    ) -> Path:
        """
        Add a resource to kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            resource_path: Path to resource file or remote resource
            
        Returns:
            Path to updated kustomization.yaml
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        if "resources" not in data:
            data["resources"] = []
        
        rel_path = os.path.relpath(
            Path(resource_path).resolve(),
            Path(kustomization_path).parent.resolve()
        )
        
        if rel_path not in data["resources"]:
            data["resources"].append(rel_path)
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return Path(kustomization_path)

    def add_label(
        self,
        kustomization_path: str,
        key: str,
        value: str
    ) -> Path:
        """
        Add a common label to all resources.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            key: Label key
            value: Label value
            
        Returns:
            Path to updated kustomization.yaml
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        if "commonLabels" not in data:
            data["commonLabels"] = {}
        
        data["commonLabels"][key] = value
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return Path(kustomization_path)

    def add_annotation(
        self,
        kustomization_path: str,
        key: str,
        value: str
    ) -> Path:
        """
        Add a common annotation to all resources.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            key: Annotation key
            value: Annotation value
            
        Returns:
            Path to updated kustomization.yaml
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        if "commonAnnotations" not in data:
            data["commonAnnotations"] = {}
        
        data["commonAnnotations"][key] = value
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return Path(kustomization_path)

    def build(
        self,
        kustomization_path: str,
        enable_alpha_plugins: bool = False,
        enable_helm: bool = False,
        enable_exec: bool = False,
        reorder: str = "legacy"
    ) -> str:
        """
        Build kubernetes manifests from kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            enable_alpha_plugins: Enable alpha plugins
            enable_helm: Enable Helm chart inflation
            enable_exec: Enable exec plugins
            reorder: Resource ordering ('legacy', 'sort', 'none')
            
        Returns:
            Built manifest as string
        """
        args = [
            self.kustomize_cmd,
            "build",
            str(Path(kustomization_path).parent)
        ]
        
        if enable_alpha_plugins:
            args.append("--enable-alpha-plugins")
        if enable_helm:
            args.append("--enable-helm")
        if enable_exec:
            args.append("--enable-exec")
        if reorder != "legacy":
            args.extend(["--reorder", reorder])
        
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            check=True
        )
        
        return result.stdout

    def build_to_file(
        self,
        kustomization_path: str,
        output_path: str,
        **kwargs
    ) -> Path:
        """
        Build and save kubernetes manifests to a file.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            output_path: Output file path
            **kwargs: Additional build arguments
            
        Returns:
            Path to output file
        """
        manifest = self.build(kustomization_path, **kwargs)
        
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(manifest)
        
        return output

    def preview(
        self,
        kustomization_path: str,
        **kwargs
    ) -> None:
        """
        Preview built manifests (print to stdout).
        
        Args:
            kustomization_path: Path to kustomization.yaml
            **kwargs: Additional build arguments
        """
        manifest = self.build(kustomization_path, **kwargs)
        print(manifest)

    def diff(
        self,
        kustomization_path: str,
        target_env: Optional[str] = None,
        use_diff_tool: bool = False
    ) -> str:
        """
        Show differences between current and target environment.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            target_env: Optional path to target overlay
            use_diff_tool: Use diff tool instead of build comparison
            
        Returns:
            Diff output as string
        """
        current = self.build(kustomization_path)
        
        if target_env:
            target_kustomization = Path(kustomization_path).parent / ".." / target_env / "kustomization.yaml"
            if target_kustomization.exists():
                target = self.build(str(target_kustomization))
            else:
                target = ""
        else:
            target = ""
        
        if use_diff_tool and target:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f1:
                f1.write(current)
                f1.flush()
                temp1 = f1.name
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f2:
                f2.write(target)
                f2.flush()
                temp2 = f2.name
            
            try:
                result = subprocess.run(
                    ["diff", "-u", temp1, temp2],
                    capture_output=True,
                    text=True
                )
                return result.stdout or "No differences found"
            finally:
                os.unlink(temp1)
                os.unlink(temp2)
        else:
            return f"Current:\n{current}\n\nTarget:\n{target}"

    def validate(
        self,
        kustomization_path: str
    ) -> Tuple[bool, str]:
        """
        Validate kustomization file.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            manifest = self.build(kustomization_path)
            
            resources = list(yaml.safe_load_all(manifest))
            
            for resource in resources:
                if not isinstance(resource, dict):
                    continue
                if "apiVersion" not in resource or "kind" not in resource:
                    return False, "Invalid resource: missing apiVersion or kind"
            
            return True, "Valid"
            
        except Exception as e:
            return False, str(e)

    def add_replacement(
        self,
        kustomization_path: str,
        source: Dict[str, Any],
        targets: List[Dict[str, Any]]
    ) -> Path:
        """
        Add replacements to kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            source: Source for replacement
            targets: Target specifications
            
        Returns:
            Path to updated kustomization.yaml
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        if "replacements" not in data:
            data["replacements"] = []
        
        replacement = {
            "source": source,
            "targets": targets
        }
        
        data["replacements"].append(replacement)
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return Path(kustomization_path)

    def set_namespace(
        self,
        kustomization_path: str,
        namespace: str
    ) -> Path:
        """
        Set namespace in kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            namespace: Namespace to set
            
        Returns:
            Path to updated kustomization.yaml
        """
        return self.update_kustomization(
            kustomization_path,
            {"namespace": namespace}
        )

    def set_name_prefix(
        self,
        kustomization_path: str,
        prefix: str
    ) -> Path:
        """
        Set name prefix in kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            prefix: Name prefix
            
        Returns:
            Path to updated kustomization.yaml
        """
        return self.update_kustomization(
            kustomization_path,
            {"namePrefix": prefix}
        )

    def set_name_suffix(
        self,
        kustomization_path: str,
        suffix: str
    ) -> Path:
        """
        Set name suffix in kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            suffix: Name suffix
            
        Returns:
            Path to updated kustomization.yaml
        """
        return self.update_kustomization(
            kustomization_path,
            {"nameSuffix": suffix}
        )

    def list_resources(
        self,
        kustomization_path: str
    ) -> List[Dict[str, Any]]:
        """
        List resources defined in kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            
        Returns:
            List of resource specifications
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        return data.get("resources", [])

    def get_images(
        self,
        kustomization_path: str
    ) -> List[KustomizeImage]:
        """
        Get list of images defined in kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            
        Returns:
            List of KustomizeImage objects
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        images = data.get("images", [])
        return [KustomizeImage.from_dict(img) for img in images]

    def remove_resource(
        self,
        kustomization_path: str,
        resource_path: str
    ) -> Path:
        """
        Remove a resource from kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            resource_path: Path to resource to remove
            
        Returns:
            Path to updated kustomization.yaml
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        if "resources" in data:
            rel_path = os.path.relpath(
                Path(resource_path).resolve(),
                Path(kustomization_path).parent.resolve()
            )
            data["resources"] = [
                r for r in data["resources"] 
                if r != rel_path and r != resource_path
            ]
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return Path(kustomization_path)

    def remove_image(
        self,
        kustomization_path: str,
        image_name: str
    ) -> Path:
        """
        Remove an image from kustomization.
        
        Args:
            kustomization_path: Path to kustomization.yaml
            image_name: Name of image to remove
            
        Returns:
            Path to updated kustomization.yaml
        """
        with open(kustomization_path, "r") as f:
            data = yaml.safe_load(f) or {}
        
        if "images" in data:
            data["images"] = [
                img for img in data["images"]
                if img.get("name") != image_name
            ]
        
        with open(kustomization_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return Path(kustomization_path)

    def clone_overlay(
        self,
        source_overlay_path: str,
        new_overlay_name: str,
        target_dir: Optional[str] = None
    ) -> Path:
        """
        Clone an existing overlay to create a new one.
        
        Args:
            source_overlay_path: Path to source overlay
            new_overlay_name: Name for new overlay
            target_dir: Target directory (defaults to parent of source)
            
        Returns:
            Path to new overlay
        """
        source = Path(source_overlay_path)
        parent = source.parent
        
        if target_dir:
            target = Path(target_dir) / new_overlay_name
        else:
            target = parent.parent / new_overlay_name
        
        shutil.copytree(source, target, dirs_exist_ok=True)
        
        kustomization_file = target / "kustomization.yaml"
        if kustomization_file.exists():
            with open(kustomization_file, "r") as f:
                data = yaml.safe_load(f) or {}
            
            data["namePrefix"] = ""
            data["nameSuffix"] = ""
            
            with open(kustomization_file, "w") as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        return target

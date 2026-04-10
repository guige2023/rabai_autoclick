"""
Crossplane integration for cloud-native infrastructure management.

This module provides the CrossplaneManager class for managing Crossplane providers,
CompositeResourceDefinitions (XRDs), Compositions, Claims, configurations, secrets,
and observing managed resources across AWS, GCP, and Azure provider families.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class ProviderType(Enum):
    """Supported Crossplane provider types."""
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"
    UNIVERSAL = "universal"


class ProviderStatus(Enum):
    """Crossplane provider installation status."""
    PENDING = "pending"
    INSTALLING = "installing"
    INSTALLED = "installed"
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DELETING = "deleting"


class XRDStatus(Enum):
    """CompositeResourceDefinition status."""
    PENDING = "pending"
    ESTABLISHED = "established"
    OFFERED = "offered"


class CompositionStatus(Enum):
    """Composition status."""
    PENDING = "pending"
    ACTIVE = "active"


@dataclass
class Provider:
    """Crossplane provider representation."""
    name: str
    provider_type: ProviderType
    version: str = "latest"
    status: ProviderStatus = ProviderStatus.PENDING
    namespace: str = "crossplane-system"
    config: Optional[Dict[str, Any]] = None
    created_at: Optional[float] = None
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class XRD:
    """CompositeResourceDefinition representation."""
    name: str
    group: str
    version: str
    kind: str
    plural: str
    status: XRDStatus = XRDStatus.PENDING
    short_names: List[str] = field(default_factory=list)
    validation: bool = True
    created_at: Optional[float] = None
    claims: List[str] = field(default_factory=list)


@dataclass
class Composition:
    """Composition for composite resources."""
    name: str
    composite_type: str
    status: CompositionStatus = CompositionStatus.PENDING
    resources: List[Dict[str, Any]] = field(default_factory=list)
    patch_sets: List[Dict[str, Any]] = field(default_factory=list)
    created_at: Optional[float] = None


@dataclass
class Claim:
    """Claim for portable cloud resources."""
    name: str
    composite_kind: str
    namespace: str = "default"
    status: str = "Pending"
    created_at: Optional[float] = None
    connection_details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ManagedResource:
    """Managed resource observed by Crossplane."""
    name: str
    kind: str
    api_version: str
    namespace: Optional[str]
    provider: str
    status: str
    ready: bool = False
    synced: bool = False
    age: float = 0.0
    conditions: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class UsageStats:
    """Crossplane usage statistics."""
    total_providers: int = 0
    healthy_providers: int = 0
    total_xrds: int = 0
    active_compositions: int = 0
    total_claims: int = 0
    active_claims: int = 0
    total_managed_resources: int = 0
    ready_resources: int = 0
    provider_families: Dict[str, int] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)


class CrossplaneManager:
    """
    Manager for Crossplane cloud-native infrastructure operations.

    Provides capabilities for:
    - Provider management (install/manage Crossplane providers)
    - XRD management (CompositeResourceDefinitions)
    - Composition management (for composite resources)
    - Claim management (portable cloud resources)
    - Configuration packaging and distribution
    - Secret integration (sync secrets to cloud providers)
    - Observation (watch and manage managed resources)
    - Connection sharing (share connection details across namespaces)
    - Usage stats reporting
    - Provider family management (AWS, GCP, Azure)
    """

    def __init__(self, kubeconfig: Optional[str] = None, context: Optional[str] = None):
        """
        Initialize CrossplaneManager.

        Args:
            kubeconfig: Path to kubeconfig file for cluster access.
            context: Kubernetes context to use.
        """
        self.kubeconfig = kubeconfig
        self.context = context
        self._providers: Dict[str, Provider] = {}
        self._xrds: Dict[str, XRD] = {}
        self._compositions: Dict[str, Composition] = {}
        self._claims: Dict[str, Claim] = {}
        self._managed_resources: Dict[str, ManagedResource] = {}
        self._secrets_sync: Dict[str, Dict[str, Any]] = {}
        self._connection_details: Dict[str, Dict[str, Any]] = {}
        self._initialized = False
        logger.info("CrossplaneManager initialized")

    # =========================================================================
    # Provider Management
    # =========================================================================

    def install_provider(
        self,
        name: str,
        provider_type: ProviderType,
        version: str = "latest",
        namespace: str = "crossplane-system",
        config: Optional[Dict[str, Any]] = None,
        labels: Optional[Dict[str, str]] = None
    ) -> Provider:
        """
        Install a Crossplane provider.

        Args:
            name: Provider name.
            provider_type: Type of provider (AWS, GCP, AZURE, UNIVERSAL).
            version: Provider version.
            namespace: Namespace to install provider.
            config: Provider configuration.
            labels: Resource labels.

        Returns:
            Installed Provider object.
        """
        provider = Provider(
            name=name,
            provider_type=provider_type,
            version=version,
            namespace=namespace,
            config=config,
            status=ProviderStatus.INSTALLING,
            created_at=time.time(),
            labels=labels or {}
        )
        self._providers[name] = provider
        logger.info(f"Installing provider {name} of type {provider_type.value}")
        # Simulate installation
        provider.status = ProviderStatus.INSTALLED
        return provider

    def get_provider(self, name: str) -> Optional[Provider]:
        """Get provider by name."""
        return self._providers.get(name)

    def list_providers(self) -> List[Provider]:
        """List all providers."""
        return list(self._providers.values())

    def delete_provider(self, name: str) -> bool:
        """
        Delete a Crossplane provider.

        Args:
            name: Provider name to delete.

        Returns:
            True if deleted successfully.
        """
        if name in self._providers:
            self._providers[name].status = ProviderStatus.DELETING
            del self._providers[name]
            logger.info(f"Deleted provider {name}")
            return True
        return False

    def check_provider_health(self, name: str) -> ProviderStatus:
        """
        Check health status of a provider.

        Args:
            name: Provider name.

        Returns:
            Current health status.
        """
        provider = self._providers.get(name)
        if not provider:
            return ProviderStatus.UNHEALTHY

        # Simulate health check - mark as healthy if installed
        if provider.status == ProviderStatus.INSTALLED:
            provider.status = ProviderStatus.HEALTHY
        return provider.status

    # =========================================================================
    # XRD Management
    # =========================================================================

    def create_xrd(
        self,
        name: str,
        group: str,
        version: str,
        kind: str,
        plural: str,
        short_names: Optional[List[str]] = None,
        validation: bool = True,
        claims: Optional[List[str]] = None
    ) -> XRD:
        """
        Create a CompositeResourceDefinition.

        Args:
            name: XRD name.
            group: API group.
            version: API version.
            kind: Resource kind.
            plural: Plural form for resource.
            short_names: Short names for kubectl.
            validation: Enable validation webhook.
            claims: Claim kinds offered by this XRD.

        Returns:
            Created XRD object.
        """
        xrd = XRD(
            name=name,
            group=group,
            version=version,
            kind=kind,
            plural=plural,
            short_names=short_names or [],
            validation=validation,
            claims=claims or [],
            status=XRDStatus.PENDING,
            created_at=time.time()
        )
        self._xrds[name] = xrd
        logger.info(f"Creating XRD {name} for {group}/{version}/{kind}")
        # Simulate XRD establishment
        xrd.status = XRDStatus.ESTABLISHED
        return xrd

    def get_xrd(self, name: str) -> Optional[XRD]:
        """Get XRD by name."""
        return self._xrds.get(name)

    def list_xrds(self) -> List[XRD]:
        """List all XRDs."""
        return list(self._xrds.values())

    def delete_xrd(self, name: str) -> bool:
        """
        Delete a CompositeResourceDefinition.

        Args:
            name: XRD name to delete.

        Returns:
            True if deleted successfully.
        """
        if name in self._xrds:
            del self._xrds[name]
            logger.info(f"Deleted XRD {name}")
            return True
        return False

    def offer_claim(self, xrd_name: str, claim_kind: str) -> bool:
        """
        Offer a claim type from an XRD.

        Args:
            xrd_name: XRD name.
            claim_kind: Claim kind to offer.

        Returns:
            True if offered successfully.
        """
        xrd = self._xrds.get(xrd_name)
        if xrd:
            if claim_kind not in xrd.claims:
                xrd.claims.append(claim_kind)
            xrd.status = XRDStatus.OFFERED
            logger.info(f"Offered claim {claim_kind} from XRD {xrd_name}")
            return True
        return False

    # =========================================================================
    # Composition Management
    # =========================================================================

    def create_composition(
        self,
        name: str,
        composite_type: str,
        resources: Optional[List[Dict[str, Any]]] = None,
        patch_sets: Optional[List[Dict[str, Any]]] = None
    ) -> Composition:
        """
        Create a Composition for composite resources.

        Args:
            name: Composition name.
            composite_type: Composite resource type.
            resources: List of managed resource definitions.
            patch_sets: Patch sets for resource composition.

        Returns:
            Created Composition object.
        """
        composition = Composition(
            name=name,
            composite_type=composite_type,
            resources=resources or [],
            patch_sets=patch_sets or [],
            status=CompositionStatus.PENDING,
            created_at=time.time()
        )
        self._compositions[name] = composition
        logger.info(f"Creating Composition {name} for {composite_type}")
        # Simulate composition activation
        composition.status = CompositionStatus.ACTIVE
        return composition

    def get_composition(self, name: str) -> Optional[Composition]:
        """Get Composition by name."""
        return self._compositions.get(name)

    def list_compositions(self) -> List[Composition]:
        """List all Compositions."""
        return list(self._compositions.values())

    def delete_composition(self, name: str) -> bool:
        """
        Delete a Composition.

        Args:
            name: Composition name to delete.

        Returns:
            True if deleted successfully.
        """
        if name in self._compositions:
            del self._compositions[name]
            logger.info(f"Deleted Composition {name}")
            return True
        return False

    def add_composition_resource(
        self,
        composition_name: str,
        resource: Dict[str, Any]
    ) -> bool:
        """
        Add a resource to a Composition.

        Args:
            composition_name: Target composition name.
            resource: Resource definition to add.

        Returns:
            True if added successfully.
        """
        composition = self._compositions.get(composition_name)
        if composition:
            composition.resources.append(resource)
            logger.info(f"Added resource to Composition {composition_name}")
            return True
        return False

    # =========================================================================
    # Claim Management
    # =========================================================================

    def create_claim(
        self,
        name: str,
        composite_kind: str,
        namespace: str = "default",
        connection_details: Optional[Dict[str, Any]] = None
    ) -> Claim:
        """
        Create a Claim for portable cloud resources.

        Args:
            name: Claim name.
            composite_kind: Composite resource kind to claim.
            namespace: Namespace for the claim.
            connection_details: Initial connection details.

        Returns:
            Created Claim object.
        """
        claim = Claim(
            name=name,
            composite_kind=composite_kind,
            namespace=namespace,
            connection_details=connection_details or {},
            status="Pending",
            created_at=time.time()
        )
        self._claims[name] = claim
        logger.info(f"Creating Claim {name} for {composite_kind}")
        # Simulate claim provisioning
        claim.status = "Ready"
        return claim

    def get_claim(self, name: str) -> Optional[Claim]:
        """Get Claim by name."""
        return self._claims.get(name)

    def list_claims(self) -> List[Claim]:
        """List all Claims."""
        return list(self._claims.values())

    def delete_claim(self, name: str) -> bool:
        """
        Delete a Claim.

        Args:
            name: Claim name to delete.

        Returns:
            True if deleted successfully.
        """
        if name in self._claims:
            del self._claims[name]
            logger.info(f"Deleted Claim {name}")
            return True
        return False

    def update_claim_connection_details(
        self,
        name: str,
        connection_details: Dict[str, Any]
    ) -> bool:
        """
        Update connection details for a Claim.

        Args:
            name: Claim name.
            connection_details: New connection details.

        Returns:
            True if updated successfully.
        """
        claim = self._claims.get(name)
        if claim:
            claim.connection_details.update(connection_details)
            logger.info(f"Updated connection details for Claim {name}")
            return True
        return False

    # =========================================================================
    # Configuration Management
    # =========================================================================

    def package_configuration(
        self,
        name: str,
        xrds: List[str],
        compositions: List[str],
        providers: List[str],
        meta: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Package a Crossplane configuration.

        Args:
            name: Configuration package name.
            xrds: List of XRD names to include.
            compositions: List of Composition names to include.
            providers: List of Provider names to include.
            meta: Metadata for the configuration.

        Returns:
            Configuration package manifest.
        """
        config_package = {
            "apiVersion": "meta.pkg.crossplane.io/v1alpha1",
            "kind": "Configuration",
            "metadata": {
                "name": name,
                "annotations": meta or {}
            },
            "spec": {
                "xrds": xrds,
                "compositions": compositions,
                "providers": providers
            }
        }
        logger.info(f"Packaged configuration {name}")
        return config_package

    def distribute_configuration(
        self,
        name: str,
        package_image: str,
        target_namespace: str = "crossplane-system"
    ) -> bool:
        """
        Distribute a configuration to a cluster.

        Args:
            name: Configuration name.
            package_image: OCI image for the package.
            target_namespace: Namespace to deploy to.

        Returns:
            True if distributed successfully.
        """
        logger.info(f"Distributing configuration {name} from {package_image}")
        return True

    def list_configurations(self) -> List[Dict[str, Any]]:
        """List all known configurations."""
        return []

    # =========================================================================
    # Secret Integration
    # =========================================================================

    def create_secret_sync(
        self,
        name: str,
        secret_name: str,
        secret_namespace: str,
        provider: str,
        keys: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Create a secret sync configuration.

        Args:
            name: Sync configuration name.
            secret_name: Kubernetes secret name.
            secret_namespace: Secret namespace.
            provider: Target provider.
            keys: Specific keys to sync.

        Returns:
            Secret sync configuration.
        """
        sync_config = {
            "name": name,
            "secret_name": secret_name,
            "secret_namespace": secret_namespace,
            "provider": provider,
            "keys": keys or [],
            "status": "pending"
        }
        self._secrets_sync[name] = sync_config
        logger.info(f"Created secret sync {name} for {secret_name}")
        return sync_config

    def sync_secret(self, sync_name: str) -> bool:
        """
        Sync a secret to the cloud provider.

        Args:
            sync_name: Secret sync configuration name.

        Returns:
            True if synced successfully.
        """
        sync_config = self._secrets_sync.get(sync_name)
        if sync_config:
            sync_config["status"] = "synced"
            logger.info(f"Synced secret {sync_config['secret_name']}")
            return True
        return False

    def get_secret_sync(self, name: str) -> Optional[Dict[str, Any]]:
        """Get secret sync configuration by name."""
        return self._secrets_sync.get(name)

    # =========================================================================
    # Observation
    # =========================================================================

    def observe_managed_resource(
        self,
        name: str,
        kind: str,
        api_version: str,
        namespace: Optional[str],
        provider: str
    ) -> ManagedResource:
        """
        Observe a managed resource.

        Args:
            name: Resource name.
            kind: Resource kind.
            api_version: API version.
            namespace: Resource namespace.
            provider: Managing provider.

        Returns:
            Observed ManagedResource.
        """
        resource = ManagedResource(
            name=name,
            kind=kind,
            api_version=api_version,
            namespace=namespace,
            provider=provider,
            status="Pending",
            ready=False,
            synced=False,
            age=0.0
        )
        self._managed_resources[f"{namespace}/{name}" if namespace else name] = resource
        logger.info(f"Observing managed resource {kind}/{name}")
        return resource

    def update_resource_status(
        self,
        name: str,
        status: str,
        ready: bool,
        synced: bool,
        conditions: Optional[List[Dict[str, Any]]] = None
    ) -> bool:
        """
        Update status of a managed resource.

        Args:
            name: Resource name.
            status: New status.
            ready: Ready condition.
            synced: Synced condition.
            conditions: Additional conditions.

        Returns:
            True if updated successfully.
        """
        resource = self._managed_resources.get(name)
        if resource:
            resource.status = status
            resource.ready = ready
            resource.synced = synced
            resource.conditions = conditions or []
            logger.info(f"Updated resource {name} status to {status}")
            return True
        return False

    def list_managed_resources(self) -> List[ManagedResource]:
        """List all observed managed resources."""
        return list(self._managed_resources.values())

    def get_managed_resource(self, name: str) -> Optional[ManagedResource]:
        """Get managed resource by name."""
        return self._managed_resources.get(name)

    # =========================================================================
    # Connection Sharing
    # =========================================================================

    def publish_connection_details(
        self,
        claim_name: str,
        details: Dict[str, Any],
        target_namespaces: List[str]
    ) -> bool:
        """
        Publish connection details across namespaces.

        Args:
            claim_name: Source claim name.
            details: Connection details to publish.
            target_namespaces: List of target namespaces.

        Returns:
            True if published successfully.
        """
        for ns in target_namespaces:
            key = f"{ns}/{claim_name}"
            self._connection_details[key] = {
                "claim_name": claim_name,
                "details": details,
                "namespace": ns
            }
        logger.info(f"Published connection details for {claim_name} to {target_namespaces}")
        return True

    def get_connection_details(
        self,
        claim_name: str,
        namespace: str = "default"
    ) -> Optional[Dict[str, Any]]:
        """
        Get connection details for a claim in a namespace.

        Args:
            claim_name: Claim name.
            namespace: Target namespace.

        Returns:
            Connection details if found.
        """
        key = f"{namespace}/{claim_name}"
        return self._connection_details.get(key)

    def list_shared_connections(self) -> List[Dict[str, Any]]:
        """List all shared connection details."""
        return list(self._connection_details.values())

    # =========================================================================
    # Usage Statistics
    # =========================================================================

    def get_usage_stats(self) -> UsageStats:
        """
        Get Crossplane usage statistics.

        Returns:
            UsageStats object with current metrics.
        """
        stats = UsageStats()

        # Provider stats
        stats.total_providers = len(self._providers)
        stats.healthy_providers = sum(
            1 for p in self._providers.values()
            if p.status == ProviderStatus.HEALTHY
        )

        # XRD stats
        stats.total_xrds = len(self._xrds)

        # Composition stats
        stats.active_compositions = sum(
            1 for c in self._compositions.values()
            if c.status == CompositionStatus.ACTIVE
        )

        # Claim stats
        stats.total_claims = len(self._claims)
        stats.active_claims = sum(
            1 for c in self._claims.values()
            if c.status == "Ready"
        )

        # Managed resource stats
        stats.total_managed_resources = len(self._managed_resources)
        stats.ready_resources = sum(
            1 for r in self._managed_resources.values()
            if r.ready
        )

        # Provider family stats
        for provider in self._providers.values():
            family = provider.provider_type.value
            stats.provider_families[family] = stats.provider_families.get(family, 0) + 1

        logger.info("Generated usage statistics")
        return stats

    def report_usage_stats(self) -> str:
        """
        Generate a usage report.

        Returns:
            Formatted usage report string.
        """
        stats = self.get_usage_stats()
        report = [
            "Crossplane Usage Report",
            "=" * 40,
            f"Providers: {stats.healthy_providers}/{stats.total_providers} healthy",
            f"XRDs: {stats.total_xrds}",
            f"Compositions: {stats.active_compositions} active",
            f"Claims: {stats.active_claims}/{stats.total_claims} ready",
            f"Managed Resources: {stats.ready_resources}/{stats.total_managed_resources} ready",
            "Provider Families:",
        ]
        for family, count in stats.provider_families.items():
            report.append(f"  - {family}: {count}")
        return "\n".join(report)

    # =========================================================================
    # Provider Family Management
    # =========================================================================

    def install_aws_provider_family(
        self,
        name: str = "provider-aws",
        version: str = "latest",
        config: Optional[Dict[str, Any]] = None
    ) -> Provider:
        """
        Install AWS provider family.

        Args:
            name: Provider name.
            version: Provider version.
            config: AWS-specific configuration.

        Returns:
            Installed AWS Provider.
        """
        aws_config = config or {}
        aws_config.setdefault("region", "us-east-1")
        return self.install_provider(
            name=name,
            provider_type=ProviderType.AWS,
            version=version,
            config=aws_config
        )

    def install_gcp_provider_family(
        self,
        name: str = "provider-gcp",
        version: str = "latest",
        config: Optional[Dict[str, Any]] = None
    ) -> Provider:
        """
        Install GCP provider family.

        Args:
            name: Provider name.
            version: Provider version.
            config: GCP-specific configuration.

        Returns:
            Installed GCP Provider.
        """
        gcp_config = config or {}
        gcp_config.setdefault("projectID", "my-project")
        return self.install_provider(
            name=name,
            provider_type=ProviderType.GCP,
            version=version,
            config=gcp_config
        )

    def install_azure_provider_family(
        self,
        name: str = "provider-azure",
        version: str = "latest",
        config: Optional[Dict[str, Any]] = None
    ) -> Provider:
        """
        Install Azure provider family.

        Args:
            name: Provider name.
            version: Provider version.
            config: Azure-specific configuration.

        Returns:
            Installed Azure Provider.
        """
        azure_config = config or {}
        azure_config.setdefault("tenantID", "my-tenant")
        return self.install_provider(
            name=name,
            provider_type=ProviderType.AZURE,
            version=version,
            config=azure_config
        )

    def list_provider_families(self) -> Dict[str, List[Provider]]:
        """
        List all providers grouped by family.

        Returns:
            Dict mapping provider type to list of providers.
        """
        families: Dict[str, List[Provider]] = {
            "aws": [],
            "gcp": [],
            "azure": [],
            "universal": []
        }
        for provider in self._providers.values():
            families[provider.provider_type.value].append(provider)
        return families

    def configure_provider_family(
        self,
        name: str,
        config: Dict[str, Any]
    ) -> bool:
        """
        Configure a provider family with provider-specific settings.

        Args:
            name: Provider name.
            config: Provider configuration.

        Returns:
            True if configured successfully.
        """
        provider = self._providers.get(name)
        if provider:
            if provider.config:
                provider.config.update(config)
            else:
                provider.config = config
            logger.info(f"Configured provider family {name}")
            return True
        return False

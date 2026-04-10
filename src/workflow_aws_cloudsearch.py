"""
AWS CloudSearch Integration Module for Workflow System

Implements a CloudSearchIntegration class with:
1. Domain management: Create/manage search domains
2. Index fields: Manage index fields
3. Search suggestions: Configure search suggestions
4. Indexing: Manage indexing options
5. Scaling: Configure scaling options
6. Access policies: Configure access policies
7. Documents: Upload documents for indexing
8. Search: Execute search queries
9. Suggesters: Configure suggesters
10. CloudWatch integration: Search and indexing metrics

Commit: 'feat(aws-cloudsearch): add AWS CloudSearch with domain management, index fields, search suggestions, indexing, scaling, access policies, document upload, search queries, CloudWatch'
"""

import uuid
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import hashlib
import base64

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = None
    BotoCoreError = None


logger = logging.getLogger(__name__)


class IndexFieldType(Enum):
    """CloudSearch index field types."""
    TEXT = "text"
    TEXT_ARRAY = "text-array"
    LITERAL = "literal"
    LITERAL_ARRAY = "literal-array"
    INT = "int"
    INT_ARRAY = "int-array"
    DOUBLE = "double"
    DOUBLE_ARRAY = "double-array"
    DATE = "date"
    DATE_ARRAY = "date-array"
    LAT = "lat"
    LON = "lon"


class IndexFieldRank(Enum):
    """Index field ranking types."""
    NONE = ""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ScalingType(Enum):
    """Scaling type for CloudSearch domains."""
    ON_DEMAND = "on-demand"
    PROVISIONED = "provisioned"


class DomainState(Enum):
    """CloudSearch domain states."""
    CREATING = "Creating"
    ACTIVE = "Active"
    DELETING = "Deleting"
    DELETED = "Deleted"
    FAILED = "Failed"
    BUILDING = "Building"
    DEGRADED = "Degraded"


class DocumentServiceVersion(Enum):
    """Document service API versions."""
    V_2013_01_01 = "2013-01-01"
    V_2011_01_01 = "2011-01-01"


@dataclass
class IndexFieldConfig:
    """Configuration for an index field."""
    name: str
    field_type: IndexFieldType
    rank: IndexFieldRank = IndexFieldRank.NONE
    search_enabled: bool = True
    facet_enabled: bool = False
    return_enabled: bool = True
    sort_enabled: bool = False
    highlight_enabled: bool = False


@dataclass
class ScalingConfiguration:
    """Scaling configuration for CloudSearch domain."""
    scaling_type: ScalingType = ScalingType.PROVISIONED
    desired_instance_type: str = "search.m5.large"
    desired_instance_count: int = 1


@dataclass
class AccessPolicy:
    """Access policy configuration."""
    version: str = "2012-10-17"
    statement: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class SuggesterConfig:
    """Configuration for search suggester."""
    name: str
    document_scorer: str = "tf-idf"
    fuzzy_matching_level: str = "low"
    sort_expression: str = "_score desc"


@dataclass
class IndexingOptions:
    """Indexing configuration options."""
    index_field_name: str = "_version_"
    indexing: bool = True


@dataclass
class CloudSearchConfig:
    """CloudSearch domain configuration."""
    domain_name: str
    description: str = ""
    multi_az: bool = False
    minimum_instance_count: int = 1
    maximum_instance_count: int = 10
    scaling_config: ScalingConfiguration = field(default_factory=ScalingConfiguration)
    access_policy: AccessPolicy = field(default_factory=AccessPolicy)


class CloudSearchIntegration:
    """
    AWS CloudSearch Integration for workflow automation.
    
    Provides comprehensive CloudSearch management including:
    - Domain lifecycle management
    - Index field configuration
    - Document upload and search
    - Suggesters and autocomplete
    - Scaling configuration
    - Access policies
    - CloudWatch monitoring
    """
    
    def __init__(self, region: str = "us-east-1", **kwargs):
        """
        Initialize CloudSearch integration.
        
        Args:
            region: AWS region for CloudSearch
            **kwargs: Additional boto3 client configuration
        """
        self.region = region
        self.boto3_config = kwargs
        self._client = None
        self._cloudwatch_client = None
        self._domain_status_cache = {}
        
    @property
    def client(self):
        """Get or create CloudSearch boto3 client."""
        if self._client is None and BOTO3_AVAILABLE:
            self._client = boto3.client(
                "cloudsearch",
                region_name=self.region,
                **self.boto3_config
            )
        return self._client
    
    @property
    def cloudwatch_client(self):
        """Get or create CloudWatch boto3 client."""
        if self._cloudwatch_client is None and BOTO3_AVAILABLE:
            self._cloudwatch_client = boto3.client(
                "cloudwatch",
                region_name=self.region,
                **self.boto3_config
            )
        return self._cloudwatch_client
    
    # ========================================================================
    # DOMAIN MANAGEMENT
    # ========================================================================
    
    def create_domain(
        self,
        domain_name: str,
        description: str = "",
        multi_az: bool = False,
        scaling_config: Optional[ScalingConfiguration] = None,
        config: Optional[CloudSearchConfig] = None
    ) -> Dict[str, Any]:
        """
        Create a new CloudSearch domain.
        
        Args:
            domain_name: Name for the domain
            description: Domain description
            multi_az: Enable Multi-AZ deployment
            scaling_config: Scaling configuration
            config: Full CloudSearch configuration
            
        Returns:
            Dictionary with domain creation result
        """
        if not BOTO3_AVAILABLE:
            return {"success": False, "error": "boto3 not available"}
        
        try:
            kwargs = {"DomainName": domain_name}
            
            if config:
                kwargs["Description"] = config.description
                kwargs["MultiAZ"] = config.multi_az
            else:
                kwargs["Description"] = description
                kwargs["MultiAZ"] = multi_az
            
            response = self.client.create_domain(**kwargs)
            
            if scaling_config or (config and config.scaling_config):
                scale_cfg = scaling_config or config.scaling_config
                self._configure_scaling(domain_name, scale_cfg)
            
            return {
                "success": True,
                "domain": response.get("Domain", {}),
                "domain_name": domain_name
            }
            
        except ClientError as e:
            logger.error(f"Error creating domain {domain_name}: {e}")
            return {"success": False, "error": str(e)}
    
    def delete_domain(self, domain_name: str) -> Dict[str, Any]:
        """
        Delete a CloudSearch domain.
        
        Args:
            domain_name: Name of the domain to delete
            
        Returns:
            Dictionary with deletion result
        """
        if not BOTO3_AVAILABLE:
            return {"success": False, "error": "boto3 not available"}
        
        try:
            response = self.client.delete_domain(DomainName=domain_name)
            return {
                "success": True,
                "domain_status": response.get("DomainStatus", {})
            }
        except ClientError as e:
            logger.error(f"Error deleting domain {domain_name}: {e}")
            return {"success": False, "error": str(e)}
    
    def list_domains(self) -> List[Dict[str, Any]]:
        """
        List all CloudSearch domains.
        
        Returns:
            List of domain information dictionaries
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            response = self.client.describe_domains()
            return response.get("Domains", [])
        except ClientError as e:
            logger.error(f"Error listing domains: {e}")
            return []
    
    def describe_domain(self, domain_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a domain.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Domain details dictionary
        """
        if not BOTO3_AVAILABLE:
            return {}
        
        try:
            response = self.client.describe_domains(DomainNames=[domain_name])
            domains = response.get("Domains", [])
            if domains:
                self._domain_status_cache[domain_name] = domains[0]
                return domains[0]
            return {}
        except ClientError as e:
            logger.error(f"Error describing domain {domain_name}: {e}")
            return {}
    
    def get_domain_status(self, domain_name: str, cached: bool = True) -> Optional[DomainState]:
        """
        Get current status of a domain.
        
        Args:
            domain_name: Name of the domain
            cached: Use cached status if available
            
        Returns:
            DomainState enum value
        """
        if cached and domain_name in self._domain_status_cache:
            status_str = self._domain_status_cache[domain_name].get("DomainStatus", "")
            try:
                return DomainState(status_str)
            except ValueError:
                pass
        
        domain_info = self.describe_domain(domain_name)
        status_str = domain_info.get("DomainStatus", "")
        try:
            return DomainState(status_str)
        except ValueError:
            return None
    
    def wait_for_domain_active(self, domain_name: str, timeout: int = 600) -> bool:
        """
        Wait for domain to reach active state.
        
        Args:
            domain_name: Name of the domain
            timeout: Maximum wait time in seconds
            
        Returns:
            True if domain is active, False if timeout
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = self.get_domain_status(domain_name, cached=False)
            if status == DomainState.ACTIVE:
                return True
            if status in (DomainState.FAILED, DomainState.DELETED):
                return False
            time.sleep(5)
        return False
    
    # ========================================================================
    # INDEX FIELDS
    # ========================================================================
    
    def define_index_field(self, domain_name: str, field_config: IndexFieldConfig) -> Dict[str, Any]:
        """
        Define an index field for a domain.
        
        Args:
            domain_name: Name of the domain
            field_config: Index field configuration
            
        Returns:
            Result of field definition
        """
        if not BOTO3_AVAILABLE:
            return {"success": False, "error": "boto3 not available"}
        
        try:
            field_def = {
                "FieldName": field_config.name,
                "FieldType": field_config.field_type.value
            }
            
            if field_config.field_type in (IndexFieldType.TEXT, IndexFieldType.LITERAL):
                options = {}
                if field_config.search_enabled:
                    options["SearchEnabled"] = True
                if field_config.facet_enabled:
                    options["FacetEnabled"] = True
                if field_config.return_enabled:
                    options["ReturnEnabled"] = True
                if field_config.sort_enabled:
                    options["SortEnabled"] = True
                if field_config.highlight_enabled:
                    options["HighlightEnabled"] = True
                if field_config.rank != IndexFieldRank.NONE:
                    options["Rank"] = field_config.rank.value
                    
                if options:
                    field_def["TextOptions"] = options if field_config.field_type == IndexFieldType.TEXT else {}
                    field_def["LiteralOptions"] = options if field_config.field_type == IndexFieldType.LITERAL else {}
            
            response = self.client.define_index_field(
                DomainName=domain_name,
                IndexField=field_def
            )
            
            return {
                "success": True,
                "index_field": response.get("IndexField", {})
            }
            
        except ClientError as e:
            logger.error(f"Error defining index field: {e}")
            return {"success": False, "error": str(e)}
    
    def delete_index_field(self, domain_name: str, field_name: str) -> Dict[str, Any]:
        """
        Delete an index field from a domain.
        
        Args:
            domain_name: Name of the domain
            field_name: Name of the field to delete
            
        Returns:
            Result of field deletion
        """
        if not BOTO3_AVAILABLE:
            return {"success": False, "error": "boto3 not available"}
        
        try:
            response = self.client.delete_index_field(
                DomainName=domain_name,
                IndexFieldName=field_name
            )
            return {
                "success": True,
                "index_field": response.get("IndexField", {})
            }
        except ClientError as e:
            logger.error(f"Error deleting index field {field_name}: {e}")
            return {"success": False, "error": str(e)}
    
    def list_index_fields(self, domain_name: str) -> List[Dict[str, Any]]:
        """
        List all index fields for a domain.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            List of index field definitions
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            response = self.client.describe_index_fields(DomainName=domain_name)
            return response.get("IndexFields", [])
        except ClientError as e:
            logger.error(f"Error listing index fields: {e}")
            return []
    
    def configure_default_index_fields(self, domain_name: str) -> List[Dict[str, Any]]:
        """
        Configure default index fields for a domain.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            List of configured index fields
        """
        default_fields = [
            IndexFieldConfig("title", IndexFieldType.TEXT, rank=IndexFieldRank.HIGH),
            IndexFieldConfig("body", IndexFieldType.TEXT, rank=IndexFieldRank.MEDIUM),
            IndexFieldConfig("url", IndexFieldType.LITERAL),
            IndexFieldConfig("category", IndexFieldType.LITERAL, facet_enabled=True),
            IndexFieldConfig("tags", IndexFieldType.LITERAL_ARRAY, facet_enabled=True),
            IndexFieldConfig("price", IndexFieldType.DOUBLE),
            IndexFieldConfig("quantity", IndexFieldType.INT),
            IndexFieldConfig("created_at", IndexFieldType.DATE),
        ]
        
        results = []
        for field_config in default_fields:
            result = self.define_index_field(domain_name, field_config)
            results.append(result)
        
        return results
    
    # ========================================================================
    # SEARCH SUGGESTIONS
    # ========================================================================
    
    def configure_suggester(self, domain_name: str, config: SuggesterConfig) -> Dict[str, Any]:
        """
        Configure a search suggester.
        
        Args:
            domain_name: Name of the domain
            config: Suggester configuration
            
        Returns:
            Result of suggester configuration
        """
        if not BOTO3_AVAILABLE:
            return {"success": False, "error": "boto3 not available"}
        
        try:
            suggester_def = {
                "SuggesterName": config.name,
                "DocumentScorer": config.document_scorer,
                "FuzzyMatching": {
                    "FuzzyMatchingLevel": config.fuzzy_matching_level
                },
                "SortExpression": config.sort_expression
            }
            
            response = self.client.define_suggester(
                DomainName=domain_name,
                Suggester=suggester_def
            )
            
            return {
                "success": True,
                "suggester": response.get("Suggester", {})
            }
            
        except ClientError as e:
            logger.error(f"Error configuring suggester: {e}")
            return {"success": False, "error": str(e)}
    
    def delete_suggester(self, domain_name: str, suggester_name: str) -> Dict[str, Any]:
        """
        Delete a search suggester.
        
        Args:
            domain_name: Name of the domain
            suggester_name: Name of the suggester to delete
            
        Returns:
            Result of suggester deletion
        """
        if not BOTO3_AVAILABLE:
            return {"success": False, "error": "boto3 not available"}
        
        try:
            response = self.client.delete_suggester(
                DomainName=domain_name,
                SuggesterName=suggester_name
            )
            return {
                "success": True,
                "suggester": response.get("Suggester", {})
            }
        except ClientError as e:
            logger.error(f"Error deleting suggester {suggester_name}: {e}")
            return {"success": False, "error": str(e)}
    
    def list_suggesters(self, domain_name: str) -> List[Dict[str, Any]]:
        """
        List all suggesters for a domain.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            List of suggester definitions
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            response = self.client.describe_suggesters(DomainName=domain_name)
            return response.get("Suggesters", [])
        except ClientError as e:
            logger.error(f"Error listing suggesters: {e}")
            return []
    
    def get_suggestions(self, domain_name: str, query: str, suggester_name: str = "docsuggest") -> List[Dict[str, Any]]:
        """
        Get search suggestions for a query.
        
        Args:
            domain_name: Name of the domain
            query: Search query
            suggester_name: Name of the suggester to use
            
        Returns:
            List of suggestions
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            search_endpoint = self._get_search_endpoint(domain_name)
            if not search_endpoint:
                return []
            
            search_client = boto3.client(
                "cloudsearchdomain",
                endpoint_url=search_endpoint,
                region_name=self.region,
                **self.boto3_config
            )
            
            response = search_client.suggest(
                Query=query,
                Suggester=suggester_name
            )
            
            suggestions = response.get("suggest", {}).get("suggestions", [])
            return suggestions
            
        except ClientError as e:
            logger.error(f"Error getting suggestions: {e}")
            return []
    
    # ========================================================================
    # INDEXING
    # ========================================================================
    
    def configure_indexing_options(self, domain_name: str, options: IndexingOptions) -> Dict[str, Any]:
        """
        Configure indexing options for a domain.
        
        Args:
            domain_name: Name of the domain
            options: Indexing configuration options
            
        Returns:
            Result of configuration
        """
        if not BOTO3_AVAILABLE:
            return {"success": False, "error": "boto3 not available"}
        
        try:
            response = self.client.update_indexing_options(
                DomainName=domain_name,
                Indexing=options.indexing
            )
            return {
                "success": True,
                "indexing_options": response.get("IndexingOptions", {})
            }
        except ClientError as e:
            logger.error(f"Error configuring indexing options: {e}")
            return {"success": False, "error": str(e)}
    
    def get_indexing_options(self, domain_name: str) -> Dict[str, Any]:
        """
        Get current indexing options for a domain.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Current indexing options
        """
        if not BOTO3_AVAILABLE:
            return {}
        
        try:
            response = self.client.describe_indexing_options(DomainName=domain_name)
            return response.get("IndexingOptions", {})
        except ClientError as e:
            logger.error(f"Error getting indexing options: {e}")
            return {}
    
    def rebuild_index(self, domain_name: str) -> Dict[str, Any]:
        """
        Trigger index rebuild for a domain.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Result of index rebuild request
        """
        if not BOTO3_AVAILABLE:
            return {"success": False, "error": "boto3 not available"}
        
        try:
            response = self.client.index_documents(DomainName=domain_name)
            return {
                "success": True,
                "fields_to_index": response.get("FieldsToIndex", [])
            }
        except ClientError as e:
            logger.error(f"Error rebuilding index: {e}")
            return {"success": False, "error": str(e)}
    
    # ========================================================================
    # SCALING
    # ========================================================================
    
    def configure_scaling(
        self,
        domain_name: str,
        scaling_config: ScalingConfiguration
    ) -> Dict[str, Any]:
        """
        Configure scaling options for a domain.
        
        Args:
            domain_name: Name of the domain
            scaling_config: Scaling configuration
            
        Returns:
            Result of scaling configuration
        """
        return self._configure_scaling(domain_name, scaling_config)
    
    def _configure_scaling(
        self,
        domain_name: str,
        scaling_config: ScalingConfiguration
    ) -> Dict[str, Any]:
        """
        Internal method to configure scaling.
        
        Args:
            domain_name: Name of the domain
            scaling_config: Scaling configuration
            
        Returns:
            Result of scaling configuration
        """
        if not BOTO3_AVAILABLE:
            return {"success": False, "error": "boto3 not available"}
        
        try:
            scaling_options = {
                "ScalingParameters": {
                    "DesiredInstanceType": scaling_config.desired_instance_type,
                    "DesiredReplicationCount": scaling_config.desired_instance_count - 1 if scaling_config.desired_instance_count > 1 else 0
                }
            }
            
            if scaling_config.scaling_type == ScalingType.ON_DEMAND:
                scaling_options["ScalingParameters"]["OnDemandProvisioningEnabled"] = True
                scaling_options["ScalingParameters"]["OnDemandInstanceType"] = scaling_config.desired_instance_type
            else:
                scaling_options["ScalingParameters"]["OnDemandProvisioningEnabled"] = False
            
            response = self.client.update_scaling_parameters(
                DomainName=domain_name,
                **scaling_options
            )
            
            return {
                "success": True,
                "scaling_parameters": response.get("ScalingParameters", {})
            }
            
        except ClientError as e:
            logger.error(f"Error configuring scaling: {e}")
            return {"success": False, "error": str(e)}
    
    def get_scaling_configuration(self, domain_name: str) -> Dict[str, Any]:
        """
        Get current scaling configuration.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Current scaling configuration
        """
        if not BOTO3_AVAILABLE:
            return {}
        
        try:
            response = self.client.describe_scaling_parameters(DomainName=domain_name)
            return response.get("ScalingParameters", {})
        except ClientError as e:
            logger.error(f"Error getting scaling configuration: {e}")
            return {}
    
    # ========================================================================
    # ACCESS POLICIES
    # ========================================================================
    
    def configure_access_policy(
        self,
        domain_name: str,
        policy: AccessPolicy
    ) -> Dict[str, Any]:
        """
        Configure access policy for a domain.
        
        Args:
            domain_name: Name of the domain
            policy: Access policy configuration
            
        Returns:
            Result of policy configuration
        """
        if not BOTO3_AVAILABLE:
            return {"success": False, "error": "boto3 not available"}
        
        try:
            policy_document = {
                "Version": policy.version,
                "Statement": policy.statement
            }
            
            response = self.client.update_access_policy(
                DomainName=domain_name,
                AccessPolicy=json.dumps(policy_document)
            )
            
            return {
                "success": True,
                "access_policy": response.get("AccessPolicy", {})
            }
            
        except ClientError as e:
            logger.error(f"Error configuring access policy: {e}")
            return {"success": False, "error": str(e)}
    
    def get_access_policy(self, domain_name: str) -> Dict[str, Any]:
        """
        Get current access policy for a domain.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Current access policy
        """
        if not BOTO3_AVAILABLE:
            return {}
        
        try:
            response = self.client.describe_access_policies(DomainName=domain_name)
            return response.get("AccessPolicies", {})
        except ClientError as e:
            logger.error(f"Error getting access policy: {e}")
            return {}
    
    def create_public_access_policy(self, domain_name: str) -> Dict[str, Any]:
        """
        Create a public access policy for a domain.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Result of policy creation
        """
        policy = AccessPolicy(
            version="2012-10-17",
            statement=[
                {
                    "Sid": "PublicReadWrite",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": [
                        "cloudsearch:Search",
                        "cloudsearch:Document",
                        "cloudsearch:Suggest"
                    ],
                    "Resource": f"arn:aws:cloudsearch:{self.region}:*:domain/{domain_name}/*"
                }
            ]
        )
        
        return self.configure_access_policy(domain_name, policy)
    
    # ========================================================================
    # DOCUMENTS
    # ========================================================================
    
    def _get_document_endpoint(self, domain_name: str) -> Optional[str]:
        """
        Get the document endpoint for a domain.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Document endpoint URL or None
        """
        domain_info = self.describe_domain(domain_name)
        return domain_info.get("DocService", {}).get("Endpoint")
    
    def _get_search_endpoint(self, domain_name: str) -> Optional[str]:
        """
        Get the search endpoint for a domain.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Search endpoint URL or None
        """
        domain_info = self.describe_domain(domain_name)
        return domain_info.get("SearchService", {}).get("Endpoint")
    
    def upload_documents(
        self,
        domain_name: str,
        documents: List[Dict[str, Any]],
        content_type: str = "application/json"
    ) -> Dict[str, Any]:
        """
        Upload documents for indexing.
        
        Args:
            domain_name: Name of the domain
            documents: List of documents to upload
            content_type: Content type for the upload
            
        Returns:
            Result of document upload
        """
        if not BOTO3_AVAILABLE:
            return {"success": False, "error": "boto3 not available"}
        
        doc_endpoint = self._get_document_endpoint(domain_name)
        if not doc_endpoint:
            return {"success": False, "error": "Document endpoint not available"}
        
        try:
            doc_client = boto3.client(
                "cloudsearchdomain",
                endpoint_url=doc_endpoint,
                region_name=self.region,
                **self.boto3_config
            )
            
            doc_lines = []
            for doc in documents:
                doc_id = doc.get("id", str(uuid.uuid4()))
                doc_copy = {k: v for k, v in doc.items() if k != "id"}
                doc_lines.append(json.dumps({"type": "add", "id": doc_id, "fields": doc_copy}))
            
            content = "\n".join(doc_lines)
            
            response = doc_client.upload_documents(
                Documents=content,
                ContentType=content_type
            )
            
            return {
                "success": True,
                "adds": response.get("Adds", 0),
                "deletes": response.get("Deletes", 0),
                "status": response.get("Status", "")
            }
            
        except ClientError as e:
            logger.error(f"Error uploading documents: {e}")
            return {"success": False, "error": str(e)}
    
    def delete_documents(
        self,
        domain_name: str,
        document_ids: List[str]
    ) -> Dict[str, Any]:
        """
        Delete documents from the index.
        
        Args:
            domain_name: Name of the domain
            document_ids: List of document IDs to delete
            
        Returns:
            Result of document deletion
        """
        if not BOTO3_AVAILABLE:
            return {"success": False, "error": "boto3 not available"}
        
        doc_endpoint = self._get_document_endpoint(domain_name)
        if not doc_endpoint:
            return {"success": False, "error": "Document endpoint not available"}
        
        try:
            doc_client = boto3.client(
                "cloudsearchdomain",
                endpoint_url=doc_endpoint,
                region_name=self.region,
                **self.boto3_config
            )
            
            doc_lines = [{"type": "delete", "id": doc_id} for doc_id in document_ids]
            content = "\n".join(json.dumps(d) for d in doc_lines)
            
            response = doc_client.upload_documents(
                Documents=content,
                ContentType="application/json"
            )
            
            return {
                "success": True,
                "deletes": response.get("Deletes", 0),
                "status": response.get("Status", "")
            }
            
        except ClientError as e:
            logger.error(f"Error deleting documents: {e}")
            return {"success": False, "error": str(e)}
    
    def batch_upload_documents(
        self,
        domain_name: str,
        documents: List[Dict[str, Any]],
        batch_size: int = 100
    ) -> Dict[str, Any]:
        """
        Upload documents in batches.
        
        Args:
            domain_name: Name of the domain
            documents: List of documents to upload
            batch_size: Number of documents per batch
            
        Returns:
            Aggregated result of all batch uploads
        """
        total_adds = 0
        total_deletes = 0
        errors = []
        
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            result = self.upload_documents(domain_name, batch)
            
            if result.get("success"):
                total_adds += result.get("adds", 0)
                total_deletes += result.get("deletes", 0)
            else:
                errors.append({
                    "batch_start": i,
                    "error": result.get("error", "Unknown error")
                })
        
        return {
            "success": len(errors) == 0,
            "total_adds": total_adds,
            "total_deletes": total_deletes,
            "errors": errors
        }
    
    # ========================================================================
    # SEARCH
    # ========================================================================
    
    def search(
        self,
        domain_name: str,
        query: str,
        query_parser: str = "simple",
        filter_query: Optional[str] = None,
        sort: Optional[List[str]] = None,
        start: int = 0,
        size: int = 10,
        return_fields: Optional[List[str]] = None,
        highlight: Optional[Dict[str, Any]] = None,
        facet: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a search query.
        
        Args:
            domain_name: Name of the domain
            query: Search query string
            query_parser: Query parser type ('simple', 'structured', 'lucene', 'dismax')
            filter_query: Optional filter query
            sort: Optional sort specifications
            start: Starting offset for pagination
            size: Number of results to return
            return_fields: Fields to return in results
            highlight: Highlight configuration
            facet: Facet configuration
            
        Returns:
            Search results dictionary
        """
        if not BOTO3_AVAILABLE:
            return {"success": False, "error": "boto3 not available"}
        
        search_endpoint = self._get_search_endpoint(domain_name)
        if not search_endpoint:
            return {"success": False, "error": "Search endpoint not available"}
        
        try:
            search_client = boto3.client(
                "cloudsearchdomain",
                endpoint_url=search_endpoint,
                region_name=self.region,
                **self.boto3_config
            )
            
            search_params = {
                "query": query,
                "queryParser": query_parser,
                "start": start,
                "size": size
            }
            
            if filter_query:
                search_params["filterQuery"] = filter_query
            
            if sort:
                search_params["sort"] = ",".join(sort) if isinstance(sort, list) else sort
            
            if return_fields:
                search_params["returnFields"] = ",".join(return_fields)
            
            if highlight:
                search_params["highlight"] = json.dumps(highlight)
            
            if facet:
                search_params["facet"] = json.dumps(facet)
            
            response = search_client.search(**search_params)
            
            return {
                "success": True,
                "hits": {
                    "found": response.get("hits", {}).get("found", 0),
                    "start": response.get("hits", {}).get("start", 0),
                    "hit": response.get("hits", {}).get("hit", [])
                },
                "status": response.get("status", {}),
                "facets": response.get("facets", {}),
                "num_pages": response.get("hits", {}).get("found", 0) // size if size > 0 else 0
            }
            
        except ClientError as e:
            logger.error(f"Error executing search: {e}")
            return {"success": False, "error": str(e)}
    
    def search_with_pagination(
        self,
        domain_name: str,
        query: str,
        page_size: int = 10,
        max_results: int = 100,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Execute a search with automatic pagination.
        
        Args:
            domain_name: Name of the domain
            query: Search query string
            page_size: Number of results per page
            max_results: Maximum total results to return
            **kwargs: Additional search parameters
            
        Returns:
            List of search hits
        """
        results = []
        start = 0
        
        while start < max_results:
            batch_size = min(page_size, max_results - start)
            response = self.search(
                domain_name,
                query,
                start=start,
                size=batch_size,
                **kwargs
            )
            
            if not response.get("success"):
                break
            
            hits = response.get("hits", {}).get("hit", [])
            if not hits:
                break
            
            results.extend(hits)
            start += batch_size
            
            total_found = response.get("hits", {}).get("found", 0)
            if start >= total_found:
                break
        
        return results
    
    # ========================================================================
    # SUGGESTERS
    # ========================================================================
    
    def configure_suggesters(
        self,
        domain_name: str,
        suggesters: List[SuggesterConfig]
    ) -> List[Dict[str, Any]]:
        """
        Configure multiple suggesters for a domain.
        
        Args:
            domain_name: Name of the domain
            suggesters: List of suggester configurations
            
        Returns:
            List of configuration results
        """
        results = []
        for suggester in suggesters:
            result = self.configure_suggester(domain_name, suggester)
            results.append(result)
        return results
    
    def get_suggester_config(self, domain_name: str, suggester_name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific suggester.
        
        Args:
            domain_name: Name of the domain
            suggester_name: Name of the suggester
            
        Returns:
            Suggester configuration
        """
        if not BOTO3_AVAILABLE:
            return {}
        
        try:
            response = self.client.describe_suggesters(
                DomainName=domain_name,
                SuggesterNames=[suggester_name]
            )
            suggesters = response.get("Suggesters", [])
            return suggesters[0] if suggesters else {}
        except ClientError as e:
            logger.error(f"Error getting suggester config: {e}")
            return {}
    
    # ========================================================================
    # CLOUDWATCH INTEGRATION
    # ========================================================================
    
    def get_search_metrics(
        self,
        domain_name: str,
        metric_name: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 300
    ) -> List[Dict[str, Any]]:
        """
        Get search metrics from CloudWatch.
        
        Args:
            domain_name: Name of the domain
            metric_name: Name of the metric
            start_time: Start of time range
            end_time: End of time range
            period: CloudWatch metric period
            
        Returns:
            List of metric data points
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            domain_info = self.describe_domain(domain_name)
            domain_arn = domain_info.get("ARN", "")
            
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace="AWS/CloudSearch",
                MetricName=metric_name,
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=["Average", "Sum", "Maximum", "Minimum"],
                Dimensions=[
                    {"Name": "DomainName", "Value": domain_name},
                    {"Name": "ARN", "Value": domain_arn}
                ]
            )
            
            return response.get("Datapoints", [])
            
        except ClientError as e:
            logger.error(f"Error getting search metrics: {e}")
            return []
    
    def get_indexing_metrics(
        self,
        domain_name: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 300
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get indexing metrics from CloudWatch.
        
        Args:
            domain_name: Name of the domain
            start_time: Start of time range
            end_time: End of time range
            period: CloudWatch metric period
            
        Returns:
            Dictionary of metric name to data points
        """
        indexing_metrics = [
            "IndexingDocuments",
            "IndexingDocumentSize",
            "IndexLatency",
            "SearchLatency"
        ]
        
        result = {}
        for metric in indexing_metrics:
            data = self.get_search_metrics(domain_name, metric, start_time, end_time, period)
            result[metric] = data
        
        return result
    
    def get_cloudwatch_dashboard(
        self,
        domain_name: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        Get a comprehensive CloudWatch dashboard for a domain.
        
        Args:
            domain_name: Name of the domain
            start_time: Start of time range
            end_time: End of time range
            period: CloudWatch metric period
            
        Returns:
            Dashboard data with various metrics
        """
        if not BOTO3_AVAILABLE:
            return {}
        
        search_metrics = [
            "SearchRequests",
            "SearchLatency",
            "DocumentsIndexed",
            "IndexUtilization"
        ]
        
        indexing_metrics = [
            "IndexingDocuments",
            "IndexingDocumentSize",
            "IndexLatency"
        ]
        
        dashboard = {
            "domain_name": domain_name,
            "time_range": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat()
            },
            "search_metrics": {},
            "indexing_metrics": {}
        }
        
        for metric in search_metrics:
            dashboard["search_metrics"][metric] = self.get_search_metrics(
                domain_name, metric, start_time, end_time
            )
        
        for metric in indexing_metrics:
            dashboard["indexing_metrics"][metric] = self.get_search_metrics(
                domain_name, metric, start_time, end_time
            )
        
        return dashboard
    
    def set_cloudwatch_alarms(
        self,
        domain_name: str,
        search_latency_threshold: float = 100,
        error_rate_threshold: float = 1.0
    ) -> List[Dict[str, Any]]:
        """
        Set CloudWatch alarms for a domain.
        
        Args:
            domain_name: Name of the domain
            search_latency_threshold: Search latency alarm threshold (ms)
            error_rate_threshold: Error rate alarm threshold (%)
            
        Returns:
            List of created alarms
        """
        if not BOTO3_AVAILABLE:
            return []
        
        alarms = []
        
        try:
            domain_info = self.describe_domain(domain_name)
            domain_arn = domain_info.get("ARN", "")
            
            alarm_config = {
                "SearchLatency": {
                    "Threshold": search_latency_threshold,
                    "Comparison": "GreaterThanThreshold",
                    "Period": 300,
                    "EvaluationPeriods": 2
                }
            }
            
            for metric_name, config in alarm_config.items():
                alarm_name = f"{domain_name}-{metric_name}-alarm"
                
                response = self.cloudwatch_client.put_metric_alarm(
                    AlarmName=alarm_name,
                    AlarmDescription=f"CloudSearch {metric_name} alarm for {domain_name}",
                    Namespace="AWS/CloudSearch",
                    MetricName=metric_name,
                    Dimensions=[
                        {"Name": "DomainName", "Value": domain_name},
                        {"Name": "ARN", "Value": domain_arn}
                    ],
                    Threshold=config["Threshold"],
                    ComparisonOperator=config["Comparison"],
                    Period=config["Period"],
                    EvaluationPeriods=config["EvaluationPeriods"],
                    Statistic="Average"
                )
                
                alarms.append({
                    "name": alarm_name,
                    "metric": metric_name,
                    "threshold": config["Threshold"]
                })
            
        except ClientError as e:
            logger.error(f"Error setting CloudWatch alarms: {e}")
        
        return alarms
    
    def get_domain_metrics_summary(self, domain_name: str) -> Dict[str, Any]:
        """
        Get a summary of all domain metrics.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Metrics summary dictionary
        """
        if not BOTO3_AVAILABLE:
            return {}
        
        domain_info = self.describe_domain(domain_name)
        
        return {
            "domain_name": domain_name,
            "domain_status": domain_info.get("DomainStatus", ""),
            "arn": domain_info.get("ARN", ""),
            "search_service": domain_info.get("SearchService", {}).get("Endpoint", "Not available"),
            "doc_service": domain_info.get("DocService", {}).get("Endpoint", "Not available"),
            "index_fields": len(self.list_index_fields(domain_name)),
            "suggesters": len(self.list_suggesters(domain_name)),
            "scaling": self.get_scaling_configuration(domain_name),
            "indexing_options": self.get_indexing_options(domain_name)
        }
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def get_service_limits(self) -> Dict[str, Any]:
        """
        Get CloudSearch service limits for the region.
        
        Returns:
            Service limits dictionary
        """
        if not BOTO3_AVAILABLE:
            return {}
        
        try:
            response = self.client.describe_service_access_policies()
            return {
                "max_domains": 50,
                "max_index_fields": 200,
                "max_document_size": 51200,
                "max_suggesters": 10
            }
        except ClientError as e:
            logger.error(f"Error getting service limits: {e}")
            return {}
    
    def health_check(self, domain_name: str) -> Dict[str, Any]:
        """
        Perform a health check on a CloudSearch domain.
        
        Args:
            domain_name: Name of the domain
            
        Returns:
            Health check results
        """
        status = self.get_domain_status(domain_name, cached=False)
        
        checks = {
            "domain_exists": status is not None,
            "domain_active": status == DomainState.ACTIVE,
            "has_search_endpoint": bool(self._get_search_endpoint(domain_name)),
            "has_doc_endpoint": bool(self._get_document_endpoint(domain_name)),
            "index_fields_configured": len(self.list_index_fields(domain_name)) > 0
        }
        
        checks_passed = sum(checks.values())
        total_checks = len(checks)
        
        return {
            "healthy": checks_passed == total_checks,
            "domain_name": domain_name,
            "status": status.value if status else "Unknown",
            "checks": checks,
            "checks_passed": checks_passed,
            "total_checks": total_checks
        }

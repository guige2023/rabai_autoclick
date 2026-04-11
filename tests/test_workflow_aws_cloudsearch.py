"""
Tests for workflow_aws_cloudsearch module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import types

# Create mock boto3 module before importing workflow_aws_cloudsearch
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()
mock_boto3.resource = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions

# Import the module
import src.workflow_aws_cloudsearch as cloudsearch_module

CloudSearchIntegration = cloudsearch_module.CloudSearchIntegration
IndexFieldType = cloudsearch_module.IndexFieldType
IndexFieldRank = cloudsearch_module.IndexFieldRank
ScalingType = cloudsearch_module.ScalingType
DomainState = cloudsearch_module.DomainState
DocumentServiceVersion = cloudsearch_module.DocumentServiceVersion
IndexFieldConfig = cloudsearch_module.IndexFieldConfig
ScalingConfiguration = cloudsearch_module.ScalingConfiguration
AccessPolicy = cloudsearch_module.AccessPolicy
SuggesterConfig = cloudsearch_module.SuggesterConfig
IndexingOptions = cloudsearch_module.IndexingOptions
CloudSearchConfig = cloudsearch_module.CloudSearchConfig


class TestCloudSearchEnums(unittest.TestCase):
    """Test CloudSearch enums"""

    def test_index_field_type_values(self):
        self.assertEqual(IndexFieldType.TEXT.value, "text")
        self.assertEqual(IndexFieldType.TEXT_ARRAY.value, "text-array")
        self.assertEqual(IndexFieldType.LITERAL.value, "literal")
        self.assertEqual(IndexFieldType.INT.value, "int")
        self.assertEqual(IndexFieldType.DOUBLE.value, "double")
        self.assertEqual(IndexFieldType.DATE.value, "date")
        self.assertEqual(IndexFieldType.LAT.value, "lat")
        self.assertEqual(IndexFieldType.LON.value, "lon")

    def test_index_field_rank_values(self):
        self.assertEqual(IndexFieldRank.NONE.value, "")
        self.assertEqual(IndexFieldRank.LOW.value, "low")
        self.assertEqual(IndexFieldRank.MEDIUM.value, "medium")
        self.assertEqual(IndexFieldRank.HIGH.value, "high")

    def test_scaling_type_values(self):
        self.assertEqual(ScalingType.ON_DEMAND.value, "on-demand")
        self.assertEqual(ScalingType.PROVISIONED.value, "provisioned")

    def test_domain_state_values(self):
        self.assertEqual(DomainState.CREATING.value, "Creating")
        self.assertEqual(DomainState.ACTIVE.value, "Active")
        self.assertEqual(DomainState.DELETING.value, "Deleting")
        self.assertEqual(DomainState.DELETED.value, "Deleted")
        self.assertEqual(DomainState.FAILED.value, "Failed")
        self.assertEqual(DomainState.BUILDING.value, "Building")
        self.assertEqual(DomainState.DEGRADED.value, "Degraded")

    def test_document_service_version_values(self):
        self.assertEqual(DocumentServiceVersion.V_2013_01_01.value, "2013-01-01")
        self.assertEqual(DocumentServiceVersion.V_2011_01_01.value, "2011-01-01")


class TestCloudSearchDataclasses(unittest.TestCase):
    """Test CloudSearch dataclasses"""

    def test_index_field_config_defaults(self):
        config = IndexFieldConfig(name="test_field", field_type=IndexFieldType.TEXT)
        self.assertEqual(config.name, "test_field")
        self.assertEqual(config.field_type, IndexFieldType.TEXT)
        self.assertEqual(config.rank, IndexFieldRank.NONE)
        self.assertTrue(config.search_enabled)
        self.assertFalse(config.facet_enabled)
        self.assertTrue(config.return_enabled)
        self.assertFalse(config.sort_enabled)
        self.assertFalse(config.highlight_enabled)

    def test_index_field_config_custom(self):
        config = IndexFieldConfig(
            name="test_field",
            field_type=IndexFieldType.LITERAL,
            rank=IndexFieldRank.HIGH,
            search_enabled=True,
            facet_enabled=True,
            sort_enabled=True
        )
        self.assertEqual(config.rank, IndexFieldRank.HIGH)
        self.assertTrue(config.facet_enabled)
        self.assertTrue(config.sort_enabled)

    def test_scaling_configuration_defaults(self):
        config = ScalingConfiguration()
        self.assertEqual(config.scaling_type, ScalingType.PROVISIONED)
        self.assertEqual(config.desired_instance_type, "search.m5.large")
        self.assertEqual(config.desired_instance_count, 1)

    def test_scaling_configuration_custom(self):
        config = ScalingConfiguration(
            scaling_type=ScalingType.ON_DEMAND,
            desired_instance_type="search.m5.xlarge",
            desired_instance_count=3
        )
        self.assertEqual(config.scaling_type, ScalingType.ON_DEMAND)
        self.assertEqual(config.desired_instance_type, "search.m5.xlarge")
        self.assertEqual(config.desired_instance_count, 3)

    def test_access_policy_defaults(self):
        policy = AccessPolicy()
        self.assertEqual(policy.version, "2012-10-17")
        self.assertEqual(len(policy.statement), 0)

    def test_access_policy_custom(self):
        policy = AccessPolicy(
            statement=[
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["cloudsearch:Search"],
                    "Resource": "arn:aws:cloudsearch:us-east-1:123456789012:domain/test/*"
                }
            ]
        )
        self.assertEqual(len(policy.statement), 1)
        self.assertEqual(policy.statement[0]["Effect"], "Allow")

    def test_suggester_config_defaults(self):
        config = SuggesterConfig(name="test_suggester")
        self.assertEqual(config.name, "test_suggester")
        self.assertEqual(config.document_scorer, "tf-idf")
        self.assertEqual(config.fuzzy_matching_level, "low")
        self.assertEqual(config.sort_expression, "_score desc")

    def test_suggester_config_custom(self):
        config = SuggesterConfig(
            name="test_suggester",
            document_scorer="classic",
            fuzzy_matching_level="high"
        )
        self.assertEqual(config.document_scorer, "classic")
        self.assertEqual(config.fuzzy_matching_level, "high")

    def test_indexing_options_defaults(self):
        options = IndexingOptions()
        self.assertEqual(options.index_field_name, "_version_")
        self.assertTrue(options.indexing)

    def test_indexing_options_custom(self):
        options = IndexingOptions(index_field_name="custom_field", indexing=False)
        self.assertEqual(options.index_field_name, "custom_field")
        self.assertFalse(options.indexing)

    def test_cloud_search_config_defaults(self):
        config = CloudSearchConfig(domain_name="test-domain")
        self.assertEqual(config.domain_name, "test-domain")
        self.assertEqual(config.description, "")
        self.assertFalse(config.multi_az)
        self.assertEqual(config.minimum_instance_count, 1)
        self.assertEqual(config.maximum_instance_count, 10)

    def test_cloud_search_config_custom(self):
        config = CloudSearchConfig(
            domain_name="test-domain",
            description="Test domain",
            multi_az=True,
            minimum_instance_count=2,
            maximum_instance_count=20
        )
        self.assertEqual(config.domain_name, "test-domain")
        self.assertEqual(config.description, "Test domain")
        self.assertTrue(config.multi_az)
        self.assertEqual(config.minimum_instance_count, 2)
        self.assertEqual(config.maximum_instance_count, 20)


class TestCloudSearchIntegration(unittest.TestCase):
    """Test CloudSearchIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudsearch_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()

        with patch.object(CloudSearchIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = CloudSearchIntegration()
            self.integration.region = "us-east-1"
            self.integration.boto3_config = {}
            self.integration._client = self.mock_cloudsearch_client
            self.integration._cloudwatch_client = self.mock_cloudwatch_client
            self.integration._domain_status_cache = {}

    def test_client_property(self):
        """Test client property"""
        result = self.integration.client
        self.assertEqual(result, self.mock_cloudsearch_client)

    def test_cloudwatch_client_property(self):
        """Test cloudwatch_client property"""
        result = self.integration.cloudwatch_client
        self.assertEqual(result, self.mock_cloudwatch_client)


class TestCloudSearchDomainManagement(unittest.TestCase):
    """Test CloudSearch domain management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudsearch_client = MagicMock()

        with patch.object(CloudSearchIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = CloudSearchIntegration()
            self.integration.region = "us-east-1"
            self.integration.boto3_config = {}
            self.integration._client = self.mock_cloudsearch_client
            self.integration._cloudwatch_client = MagicMock()
            self.integration._domain_status_cache = {}

    def test_create_domain(self):
        """Test creating a domain"""
        mock_response = {
            'Domain': {
                'DomainId': '123456789012/test-domain',
                'DomainName': 'test-domain',
                'ARN': 'arn:aws:cloudsearch:us-east-1:123456789012:domain/test-domain',
                'Created': True,
                'Deleted': False
            }
        }
        self.mock_cloudsearch_client.create_domain.return_value = mock_response

        result = self.integration.create_domain("test-domain", description="Test domain")

        self.assertTrue(result['success'])
        self.assertEqual(result['domain_name'], "test-domain")

    def test_create_domain_with_config(self):
        """Test creating a domain with full configuration"""
        mock_response = {
            'Domain': {
                'DomainId': '123456789012/test-domain',
                'DomainName': 'test-domain'
            }
        }
        self.mock_cloudsearch_client.create_domain.return_value = mock_response

        config = CloudSearchConfig(
            domain_name="test-domain",
            description="Test domain",
            multi_az=True
        )

        result = self.integration.create_domain("test-domain", config=config)

        self.assertTrue(result['success'])

    def test_delete_domain(self):
        """Test deleting a domain"""
        mock_response = {
            'DomainStatus': {
                'DomainId': '123456789012/test-domain',
                'DomainName': 'test-domain',
                'Deleted': True
            }
        }
        self.mock_cloudsearch_client.delete_domain.return_value = mock_response

        result = self.integration.delete_domain("test-domain")

        self.assertTrue(result['success'])

    def test_list_domains(self):
        """Test listing domains"""
        mock_response = {
            'Domains': [
                {'DomainName': 'domain-1', 'DomainId': '123456789012/domain-1'},
                {'DomainName': 'domain-2', 'DomainId': '123456789012/domain-2'}
            ]
        }
        self.mock_cloudsearch_client.describe_domains.return_value = mock_response

        result = self.integration.list_domains()

        self.assertEqual(len(result), 2)

    def test_describe_domain(self):
        """Test describing a domain"""
        mock_response = {
            'Domains': [{
                'DomainName': 'test-domain',
                'DomainId': '123456789012/test-domain',
                'DomainStatus': {
                    'DomainId': '123456789012/test-domain',
                    'DomainName': 'test-domain',
                    'ARN': 'arn:aws:cloudsearch:us-east-1:123456789012:domain/test-domain',
                    'Created': True,
                    'Deleted': False
                }
            }]
        }
        self.mock_cloudsearch_client.describe_domains.return_value = mock_response

        result = self.integration.describe_domain("test-domain")

        self.assertEqual(result['DomainName'], "test-domain")
        self.assertIn("test-domain", self.integration._domain_status_cache)

    def test_get_domain_status(self):
        """Test getting domain status"""
        mock_response = {
            'Domains': [{
                'DomainName': 'test-domain',
                'DomainStatus': 'Active'
            }]
        }
        self.mock_cloudsearch_client.describe_domains.return_value = mock_response

        result = self.integration.get_domain_status("test-domain")

        self.assertEqual(result, DomainState.ACTIVE)

    def test_get_domain_status_cached(self):
        """Test getting domain status from cache"""
        self.integration._domain_status_cache["test-domain"] = {'DomainStatus': 'Creating'}

        result = self.integration.get_domain_status("test-domain", cached=True)

        self.assertEqual(result, DomainState.CREATING)

    def test_wait_for_domain_active(self):
        """Test waiting for domain to become active"""
        self.mock_cloudsearch_client.describe_domains.return_value = {
            'Domains': [{'DomainStatus': 'Active'}]
        }

        result = self.integration.wait_for_domain_active("test-domain", timeout=10)

        self.assertTrue(result)


class TestCloudSearchIndexFields(unittest.TestCase):
    """Test CloudSearch index field methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudsearch_client = MagicMock()

        with patch.object(CloudSearchIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = CloudSearchIntegration()
            self.integration.region = "us-east-1"
            self.integration._client = self.mock_cloudsearch_client
            self.integration._domain_status_cache = {}

    def test_define_index_field(self):
        """Test defining an index field"""
        mock_response = {
            'IndexField': {
                'IndexFieldName': 'title',
                'IndexFieldType': 'text'
            }
        }
        self.mock_cloudsearch_client.define_index_field.return_value = mock_response

        config = IndexFieldConfig(
            name="title",
            field_type=IndexFieldType.TEXT,
            rank=IndexFieldRank.HIGH,
            search_enabled=True
        )

        result = self.integration.define_index_field("test-domain", config)

        self.assertTrue(result['success'])
        self.assertEqual(result['index_field']['IndexFieldName'], 'title')

    def test_define_text_index_field(self):
        """Test defining a text index field with options"""
        mock_response = {
            'IndexField': {
                'IndexFieldName': 'title',
                'IndexFieldType': 'text',
                'TextOptions': {'SearchEnabled': True, 'ReturnEnabled': True}
            }
        }
        self.mock_cloudsearch_client.define_index_field.return_value = mock_response

        config = IndexFieldConfig(
            name="title",
            field_type=IndexFieldType.TEXT,
            search_enabled=True,
            return_enabled=True
        )

        result = self.integration.define_index_field("test-domain", config)

        self.assertTrue(result['success'])

    def test_delete_index_field(self):
        """Test deleting an index field"""
        mock_response = {
            'IndexField': {
                'IndexFieldName': 'old_field'
            }
        }
        self.mock_cloudsearch_client.delete_index_field.return_value = mock_response

        result = self.integration.delete_index_field("test-domain", "old_field")

        self.assertTrue(result['success'])

    def test_list_index_fields(self):
        """Test listing index fields"""
        mock_response = {
            'IndexFields': [
                {'IndexFieldName': 'title', 'IndexFieldType': 'text'},
                {'IndexFieldName': 'id', 'IndexFieldType': 'literal'},
                {'IndexFieldName': 'price', 'IndexFieldType': 'double'}
            ]
        }
        self.mock_cloudsearch_client.describe_index_fields.return_value = mock_response

        result = self.integration.list_index_fields("test-domain")

        self.assertEqual(len(result), 3)

    def test_configure_default_index_fields(self):
        """Test configuring default index fields"""
        self.mock_cloudsearch_client.define_index_field.return_value = {
            'IndexField': {'IndexFieldName': 'title'}
        }

        results = self.integration.configure_default_index_fields("test-domain")

        self.assertEqual(len(results), 8)  # Default fields


class TestCloudSearchScaling(unittest.TestCase):
    """Test CloudSearch scaling methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudsearch_client = MagicMock()

        with patch.object(CloudSearchIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = CloudSearchIntegration()
            self.integration.region = "us-east-1"
            self.integration._client = self.mock_cloudsearch_client
            self.integration._domain_status_cache = {}

    def test_configure_scaling(self):
        """Test configuring scaling options"""
        self.mock_cloudsearch_client.update_scaling_parameters.return_value = {
            'ScalingParameters': {'DesiredInstanceType': 'search.m5.large'}
        }

        config = ScalingConfiguration(
            scaling_type=ScalingType.PROVISIONED,
            desired_instance_type="search.m5.large",
            desired_instance_count=2
        )

        result = self.integration._configure_scaling("test-domain", config)

        self.assertTrue(result['success'])

    def test_get_scaling_configuration(self):
        """Test getting scaling configuration"""
        self.mock_cloudsearch_client.describe_scaling_parameters.return_value = {
            'ScalingParameters': {
                'DesiredInstanceType': 'search.m5.large',
                'DesiredReplicationCount': 1
            }
        }

        result = self.integration.get_scaling_configuration("test-domain")

        self.assertIn('DesiredInstanceType', result)


class TestCloudSearchAccessPolicies(unittest.TestCase):
    """Test CloudSearch access policy methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudsearch_client = MagicMock()

        with patch.object(CloudSearchIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = CloudSearchIntegration()
            self.integration.region = "us-east-1"
            self.integration._client = self.mock_cloudsearch_client
            self.integration._domain_status_cache = {}

    def test_configure_access_policy(self):
        """Test configuring access policy"""
        self.mock_cloudsearch_client.update_access_policy.return_value = {
            'AccessPolicy': {'Status': {'State': 'Active'}}
        }

        policy = AccessPolicy(
            statement=[
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                    "Action": ["cloudsearch:Search", "cloudsearch:Document"],
                    "Resource": "arn:aws:cloudsearch:us-east-1:123456789012:domain/test-domain/*"
                }
            ]
        )

        result = self.integration.configure_access_policy("test-domain", policy)

        self.assertTrue(result['success'])

    def test_get_access_policy(self):
        """Test getting access policy"""
        mock_response = {
            'AccessPolicies': {
                'Status': {'CreationDate': '2024-01-01T00:00:00Z', 'State': 'Active'},
                'Options': '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"cloudsearch:Search","Resource":"*"}]}'
            }
        }
        self.mock_cloudsearch_client.describe_access_policies.return_value = mock_response

        result = self.integration.get_access_policy("test-domain")

        self.assertIn('Options', result)

    def test_create_public_access_policy(self):
        """Test creating public access policy"""
        self.mock_cloudsearch_client.update_access_policy.return_value = {
            'AccessPolicy': {'Status': {'State': 'Active'}}
        }

        result = self.integration.create_public_access_policy("test-domain")

        self.assertTrue(result['success'])


class TestCloudSearchSuggesters(unittest.TestCase):
    """Test CloudSearch suggester methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudsearch_client = MagicMock()

        with patch.object(CloudSearchIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = CloudSearchIntegration()
            self.integration.region = "us-east-1"
            self.integration._client = self.mock_cloudsearch_client
            self.integration._domain_status_cache = {}

    def test_configure_suggester(self):
        """Test configuring a suggester"""
        mock_response = {
            'Suggester': {
                'SuggesterName': 'test_suggester',
                'DocumentScorer': 'tf-idf'
            }
        }
        self.mock_cloudsearch_client.define_suggester.return_value = mock_response

        config = SuggesterConfig(
            name="test_suggester",
            document_scorer="tf-idf",
            fuzzy_matching_level="low"
        )

        result = self.integration.configure_suggester("test-domain", config)

        self.assertTrue(result['success'])

    def test_list_suggesters(self):
        """Test listing suggesters"""
        mock_response = {
            'Suggesters': [
                {'SuggesterName': 'suggester-1'},
                {'SuggesterName': 'suggester-2'}
            ]
        }
        self.mock_cloudsearch_client.describe_suggesters.return_value = mock_response

        result = self.integration.list_suggesters("test-domain")

        self.assertEqual(len(result), 2)

    def test_delete_suggester(self):
        """Test deleting a suggester"""
        self.mock_cloudsearch_client.delete_suggester.return_value = {}

        result = self.integration.delete_suggester("test-domain", "test_suggester")

        self.assertTrue(result['success'])

    def test_get_suggester_config(self):
        """Test getting suggester config"""
        mock_response = {
            'Suggesters': [
                {'SuggesterName': 'docsuggest', 'DocumentScorer': 'tf-idf'}
            ]
        }
        self.mock_cloudsearch_client.describe_suggesters.return_value = mock_response

        result = self.integration.get_suggester_config("test-domain", "docsuggest")

        self.assertEqual(result['SuggesterName'], 'docsuggest')


class TestCloudSearchDocuments(unittest.TestCase):
    """Test CloudSearch document methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudsearch_client = MagicMock()
        self.mock_cloudsearch_domain_client = MagicMock()

        with patch.object(CloudSearchIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = CloudSearchIntegration()
            self.integration.region = "us-east-1"
            self.integration.boto3_config = {}
            self.integration._client = self.mock_cloudsearch_client
            self.integration._cloudwatch_client = MagicMock()
            self.integration._domain_status_cache = {}

    def test_upload_documents(self):
        """Test uploading documents"""
        # Mock describe_domains to return doc endpoint
        self.mock_cloudsearch_client.describe_domains.return_value = {
            'Domains': [{
                'DomainName': 'test-domain',
                'DocService': {'Endpoint': 'https://doc-test-domain.us-east-1.cloudsearch.amazonaws.com'}
            }]
        }

        # Mock cloudsearchdomain client
        mock_cloudsearchdomain = MagicMock()
        mock_cloudsearchdomain.upload_documents.return_value = {
            'Adds': 10,
            'Deletes': 0,
            'Status': 'success'
        }

        with patch('src.workflow_aws_cloudsearch.boto3') as mock_boto3:
            mock_boto3.client.return_value = mock_cloudsearchdomain
            
            documents = [
                {"id": "doc-1", "fields": {"title": "Document 1", "body": "Content 1"}},
                {"id": "doc-2", "fields": {"title": "Document 2", "body": "Content 2"}}
            ]

            result = self.integration.upload_documents("test-domain", documents)

            self.assertTrue(result['success'])
            self.assertEqual(result['adds'], 10)

    def test_delete_documents(self):
        """Test deleting documents"""
        # Mock describe_domains to return doc endpoint
        self.mock_cloudsearch_client.describe_domains.return_value = {
            'Domains': [{
                'DomainName': 'test-domain',
                'DocService': {'Endpoint': 'https://doc-test-domain.us-east-1.cloudsearch.amazonaws.com'}
            }]
        }

        # Mock cloudsearchdomain client
        mock_cloudsearchdomain = MagicMock()
        mock_cloudsearchdomain.upload_documents.return_value = {
            'Deletes': 2,
            'Status': 'success'
        }

        with patch('src.workflow_aws_cloudsearch.boto3') as mock_boto3:
            mock_boto3.client.return_value = mock_cloudsearchdomain

            result = self.integration.delete_documents("test-domain", ["doc-1", "doc-2"])

            self.assertTrue(result['success'])
            self.assertEqual(result['deletes'], 2)


class TestCloudSearchSearch(unittest.TestCase):
    """Test CloudSearch search methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudsearch_client = MagicMock()

        with patch.object(CloudSearchIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = CloudSearchIntegration()
            self.integration.region = "us-east-1"
            self.integration.boto3_config = {}
            self.integration._client = self.mock_cloudsearch_client
            self.integration._cloudwatch_client = MagicMock()
            self.integration._domain_status_cache = {}

    def test_search(self):
        """Test executing a search"""
        # Mock describe_domains to return search endpoint
        self.mock_cloudsearch_client.describe_domains.return_value = {
            'Domains': [{
                'DomainName': 'test-domain',
                'SearchService': {'Endpoint': 'https://search-test-domain.us-east-1.cloudsearch.amazonaws.com'}
            }]
        }

        # Mock cloudsearchdomain client
        mock_cloudsearchdomain = MagicMock()
        mock_cloudsearchdomain.search.return_value = {
            'hits': {
                'found': 10,
                'hit': [
                    {'id': 'doc-1', 'fields': {'title': ['Document 1']}},
                    {'id': 'doc-2', 'fields': {'title': ['Document 2']}}
                ]
            },
            'status': {'timems': 5, 'rid': 'test-rid'}
        }

        with patch('src.workflow_aws_cloudsearch.boto3') as mock_boto3:
            mock_boto3.client.return_value = mock_cloudsearchdomain

            result = self.integration.search("test-domain", "query=title:Document")

            self.assertTrue(result['success'])
            self.assertEqual(result['hits']['found'], 10)
            self.assertEqual(len(result['hits']['hit']), 2)

    def test_search_with_filters(self):
        """Test executing a search with filters"""
        # Mock describe_domains to return search endpoint
        self.mock_cloudsearch_client.describe_domains.return_value = {
            'Domains': [{
                'DomainName': 'test-domain',
                'SearchService': {'Endpoint': 'https://search-test-domain.us-east-1.cloudsearch.amazonaws.com'}
            }]
        }

        # Mock cloudsearchdomain client
        mock_cloudsearchdomain = MagicMock()
        mock_cloudsearchdomain.search.return_value = {
            'hits': {'found': 5, 'hit': []},
            'status': {'timems': 3}
        }

        with patch('src.workflow_aws_cloudsearch.boto3') as mock_boto3:
            mock_boto3.client.return_value = mock_cloudsearchdomain

            result = self.integration.search(
                "test-domain",
                "query=Document",
                filter_query="price:[100,500]"
            )

            self.assertTrue(result['success'])
            self.assertIn('hits', result)

    def test_get_suggestions(self):
        """Test getting search suggestions"""
        # Mock describe_domains to return search endpoint
        self.mock_cloudsearch_client.describe_domains.return_value = {
            'Domains': [{
                'DomainName': 'test-domain',
                'SearchService': {'Endpoint': 'https://search-test-domain.us-east-1.cloudsearch.amazonaws.com'}
            }]
        }

        # Mock cloudsearchdomain client
        mock_cloudsearchdomain = MagicMock()
        mock_cloudsearchdomain.suggest.return_value = {
            'suggest': {
                'query': 'Doc',
                'suggestions': [
                    {'suggestion': 'Document 1', 'score': 10},
                    {'suggestion': 'Document 2', 'score': 8}
                ]
            }
        }

        with patch('src.workflow_aws_cloudsearch.boto3') as mock_boto3:
            mock_boto3.client.return_value = mock_cloudsearchdomain

            result = self.integration.get_suggestions("test-domain", "Doc")

            self.assertEqual(len(result), 2)


class TestCloudSearchIndexing(unittest.TestCase):
    """Test CloudSearch indexing methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudsearch_client = MagicMock()

        with patch.object(CloudSearchIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = CloudSearchIntegration()
            self.integration.region = "us-east-1"
            self.integration._client = self.mock_cloudsearch_client
            self.integration._domain_status_cache = {}

    def test_configure_indexing_options(self):
        """Test configuring indexing options"""
        self.mock_cloudsearch_client.update_indexing_options.return_value = {
            'IndexingOptions': {'Indexing': True}
        }

        options = IndexingOptions(index_field_name="_version_", indexing=True)

        result = self.integration.configure_indexing_options("test-domain", options)

        self.assertTrue(result['success'])

    def test_get_indexing_options(self):
        """Test getting indexing options"""
        self.mock_cloudsearch_client.describe_indexing_options.return_value = {
            'IndexingOptions': {'Indexing': True}
        }

        result = self.integration.get_indexing_options("test-domain")

        self.assertIn('Indexing', result)

    def test_rebuild_index(self):
        """Test rebuilding index"""
        self.mock_cloudsearch_client.index_documents.return_value = {
            'FieldsToIndex': ['title', 'body']
        }

        result = self.integration.rebuild_index("test-domain")

        self.assertTrue(result['success'])
        self.assertIn('fields_to_index', result)


class TestCloudSearchMonitoring(unittest.TestCase):
    """Test CloudSearch CloudWatch monitoring methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudsearch_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()

        with patch.object(CloudSearchIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = CloudSearchIntegration()
            self.integration.region = "us-east-1"
            self.integration.boto3_config = {}
            self.integration._client = self.mock_cloudsearch_client
            self.integration._cloudwatch_client = self.mock_cloudwatch_client
            self.integration._domain_status_cache = {}

    def test_get_search_metrics(self):
        """Test getting search metrics"""
        self.mock_cloudwatch_client.get_metric_statistics.return_value = {
            'Datapoints': [
                {'Average': 50.0, 'Maximum': 100.0, 'Minimum': 10.0, 'Timestamp': '2024-01-01T00:00:00Z'}
            ]
        }
        self.mock_cloudsearch_client.describe_domains.return_value = {
            'Domains': [{'DomainName': 'test-domain', 'ARN': 'arn:aws:cloudsearch:us-east-1:123456789:domain/test-domain'}]
        }

        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 2)

        result = self.integration.get_search_metrics("test-domain", "SearchLatency", start_time, end_time)

        self.assertIsNotNone(result)
        self.mock_cloudwatch_client.get_metric_statistics.assert_called()

    def test_get_indexing_metrics(self):
        """Test getting indexing metrics"""
        self.mock_cloudwatch_client.get_metric_statistics.return_value = {
            'Datapoints': [
                {'Average': 20.0, 'Timestamp': '2024-01-01T00:00:00Z'}
            ]
        }
        self.mock_cloudsearch_client.describe_domains.return_value = {
            'Domains': [{'DomainName': 'test-domain', 'ARN': 'arn:aws:cloudsearch:us-east-1:123456789:domain/test-domain'}]
        }

        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 2)

        result = self.integration.get_indexing_metrics("test-domain", start_time, end_time)

        self.assertIsNotNone(result)
        self.assertIn('IndexingDocuments', result)

    def test_get_cloudwatch_dashboard(self):
        """Test getting CloudWatch dashboard"""
        self.mock_cloudwatch_client.get_metric_statistics.return_value = {
            'Datapoints': [{'Average': 50.0}]
        }
        self.mock_cloudsearch_client.describe_domains.return_value = {
            'Domains': [{'DomainName': 'test-domain', 'ARN': 'arn:aws:cloudsearch:us-east-1:123456789:domain/test-domain'}]
        }

        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 2)

        result = self.integration.get_cloudwatch_dashboard("test-domain", start_time, end_time)

        self.assertIn('search_metrics', result)
        self.assertIn('indexing_metrics', result)

    def test_set_cloudwatch_alarms(self):
        """Test setting CloudWatch alarms"""
        self.mock_cloudwatch_client.put_metric_alarm.return_value = {}
        self.mock_cloudsearch_client.describe_domains.return_value = {
            'Domains': [{'DomainName': 'test-domain', 'ARN': 'arn:aws:cloudsearch:us-east-1:123456789:domain/test-domain'}]
        }

        result = self.integration.set_cloudwatch_alarms("test-domain", search_latency_threshold=100)

        self.assertIsInstance(result, list)


class TestCloudSearchUtility(unittest.TestCase):
    """Test CloudSearch utility methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudsearch_client = MagicMock()

        with patch.object(CloudSearchIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = CloudSearchIntegration()
            self.integration.region = "us-east-1"
            self.integration._client = self.mock_cloudsearch_client
            self.integration._domain_status_cache = {}

    def test_get_service_limits(self):
        """Test getting service limits"""
        self.mock_cloudsearch_client.describe_service_access_policies.return_value = {}

        result = self.integration.get_service_limits()

        self.assertIn('max_domains', result)
        self.assertIn('max_index_fields', result)

    def test_health_check(self):
        """Test health check"""
        self.mock_cloudsearch_client.describe_domains.return_value = {
            'Domains': [{
                'DomainName': 'test-domain',
                'DomainStatus': 'Active',
                'ARN': 'arn:aws:cloudsearch:us-east-1:123456789:domain/test-domain',
                'SearchService': {'Endpoint': 'https://search-test-domain.us-east-1.cloudsearch.amazonaws.com'},
                'DocService': {'Endpoint': 'https://doc-test-domain.us-east-1.cloudsearch.amazonaws.com'}
            }]
        }
        self.mock_cloudsearch_client.describe_index_fields.return_value = {
            'IndexFields': [{'IndexFieldName': 'title'}]
        }
        self.mock_cloudsearch_client.describe_suggesters.return_value = {
            'Suggesters': []
        }
        self.mock_cloudsearch_client.describe_scaling_parameters.return_value = {
            'ScalingParameters': {}
        }
        self.mock_cloudsearch_client.describe_indexing_options.return_value = {
            'IndexingOptions': {}
        }

        result = self.integration.health_check("test-domain")

        self.assertIn('healthy', result)
        self.assertIn('checks', result)

    def test_get_domain_metrics_summary(self):
        """Test getting domain metrics summary"""
        self.mock_cloudsearch_client.describe_domains.return_value = {
            'Domains': [{
                'DomainName': 'test-domain',
                'DomainStatus': 'Active',
                'ARN': 'arn:aws:cloudsearch:us-east-1:123456789:domain/test-domain',
                'SearchService': {'Endpoint': 'https://search-test-domain.us-east-1.cloudsearch.amazonaws.com'},
                'DocService': {'Endpoint': 'https://doc-test-domain.us-east-1.cloudsearch.amazonaws.com'}
            }]
        }
        self.mock_cloudsearch_client.describe_index_fields.return_value = {
            'IndexFields': [{'IndexFieldName': 'title'}]
        }
        self.mock_cloudsearch_client.describe_suggesters.return_value = {
            'Suggesters': []
        }
        self.mock_cloudsearch_client.describe_scaling_parameters.return_value = {
            'ScalingParameters': {}
        }
        self.mock_cloudsearch_client.describe_indexing_options.return_value = {
            'IndexingOptions': {}
        }

        result = self.integration.get_domain_metrics_summary("test-domain")

        self.assertEqual(result['domain_name'], 'test-domain')


if __name__ == '__main__':
    unittest.main()

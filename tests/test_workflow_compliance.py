"""
Tests for Workflow Compliance Module
"""
import unittest
import tempfile
import shutil
import json
import os
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock, mock_open

import sys
sys.path.insert(0, '/Users/guige/my_project')

from src.workflow_compliance import (
    ComplianceManager,
    ComplianceLevel,
    DataCategory,
    DataResidencyRegion,
    RetentionPolicy,
    UserRole,
    PolicyViolationSeverity,
    ConsentStatus,
    AuditAction,
    Policy,
    PolicyViolation,
    AuditEntry,
    UserConsent,
    DataRetentionRule,
    PrivacyImpactAssessment,
    ComplianceReport,
    RolePermission
)


class TestComplianceEnums(unittest.TestCase):
    """Test compliance enums"""

    def test_compliance_level_values(self):
        """Test ComplianceLevel enum has expected values"""
        self.assertEqual(ComplianceLevel.NONE.value, 1)
        self.assertEqual(ComplianceLevel.GDPR.value, 2)
        self.assertEqual(ComplianceLevel.CCPA.value, 3)
        self.assertEqual(ComplianceLevel.HIPAA.value, 4)
        self.assertEqual(ComplianceLevel.SOC2.value, 5)
        self.assertEqual(ComplianceLevel.ISO27001.value, 6)

    def test_data_category_values(self):
        """Test DataCategory enum"""
        self.assertEqual(DataCategory.PUBLIC.value, 1)
        self.assertEqual(DataCategory.PII.value, 5)
        self.assertEqual(DataCategory.PHI.value, 6)
        self.assertEqual(DataCategory.FINANCIAL.value, 7)

    def test_retention_policy_values(self):
        """Test RetentionPolicy enum"""
        self.assertEqual(RetentionPolicy.IMMEDIATE.value, 7)
        self.assertEqual(RetentionPolicy.SHORT_TERM.value, 2)
        self.assertEqual(RetentionPolicy.LONG_TERM.value, 4)

    def test_consent_status_values(self):
        """Test ConsentStatus enum"""
        self.assertEqual(ConsentStatus.GRANTED.value, 1)
        self.assertEqual(ConsentStatus.DENIED.value, 2)
        self.assertEqual(ConsentStatus.WITHDRAWN.value, 3)
        self.assertEqual(ConsentStatus.EXPIRED.value, 4)
        self.assertEqual(ConsentStatus.PENDING.value, 5)


class TestPolicyDataclass(unittest.TestCase):
    """Test Policy dataclass"""

    def test_create_policy(self):
        """Test creating a policy"""
        policy = Policy(
            id="policy-001",
            name="Test Policy",
            description="A test policy",
            compliance_levels=[ComplianceLevel.GDPR, ComplianceLevel.HIPAA],
            enabled=True,
            priority=5,
            conditions={"user_role": "admin"},
            actions=["log", "notify"]
        )
        
        self.assertEqual(policy.id, "policy-001")
        self.assertEqual(policy.name, "Test Policy")
        self.assertEqual(len(policy.compliance_levels), 2)
        self.assertTrue(policy.enabled)
        self.assertEqual(policy.priority, 5)
        self.assertIn("user_role", policy.conditions)


class TestPolicyViolation(unittest.TestCase):
    """Test PolicyViolation dataclass"""

    def test_create_violation(self):
        """Test creating a policy violation"""
        violation = PolicyViolation(
            id="violation-001",
            policy_id="policy-001",
            policy_name="Test Policy",
            severity=PolicyViolationSeverity.HIGH,
            description="Test violation",
            workflow_id="wf-001",
            user_id="user-001"
        )
        
        self.assertEqual(violation.id, "violation-001")
        self.assertEqual(violation.severity, PolicyViolationSeverity.HIGH)
        self.assertFalse(violation.resolved)


class TestUserConsent(unittest.TestCase):
    """Test UserConsent dataclass"""

    def test_create_consent(self):
        """Test creating user consent"""
        consent = UserConsent(
            id="consent-001",
            user_id="user-001",
            consent_type="marketing",
            purpose="Email promotions",
            status=ConsentStatus.GRANTED,
            granted_at=datetime.utcnow(),
            version="1.0",
            proof="abc123"
        )
        
        self.assertEqual(consent.status, ConsentStatus.GRANTED)
        self.assertEqual(consent.user_id, "user-001")


class TestDataRetentionRule(unittest.TestCase):
    """Test DataRetentionRule dataclass"""

    def test_create_retention_rule(self):
        """Test creating a retention rule"""
        rule = DataRetentionRule(
            id="rule-001",
            name="PII Retention",
            data_type=DataCategory.PII,
            retention_period=RetentionPolicy.SHORT_TERM,
            max_age_days=30,
            auto_delete=True
        )
        
        self.assertEqual(rule.data_type, DataCategory.PII)
        self.assertEqual(rule.max_age_days, 30)
        self.assertTrue(rule.auto_delete)


class TestPrivacyImpactAssessment(unittest.TestCase):
    """Test PrivacyImpactAssessment dataclass"""

    def test_create_assessment(self):
        """Test creating a privacy impact assessment"""
        assessment = PrivacyImpactAssessment(
            id="pia-001",
            workflow_id="wf-001",
            risk_score=0.6,
            risk_factors=["PII involved", "Cross-border transfer"],
            data_types_collected=[DataCategory.PII, DataCategory.USER_CONTENT],
            data_types_shared=[],
            pii_involved=True,
            phi_involved=False,
            cross_border_transfer=True,
            automated_decisions=False,
            recommendations=["Implement enhanced access controls"]
        )
        
        self.assertEqual(assessment.risk_score, 0.6)
        self.assertTrue(assessment.pii_involved)
        self.assertTrue(assessment.cross_border_transfer)


class TestComplianceManager(unittest.TestCase):
    """Test ComplianceManager class"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.manager = None
        
        # Mock os.makedirs and file operations
        with patch('os.makedirs') as mock_makedirs, \
             patch('builtins.open', mock_open()) as mock_file, \
             patch('os.path.exists', return_value=False):
            self.manager = ComplianceManager(storage_path=self.temp_dir)

    def tearDown(self):
        """Tear down test fixtures"""
        if self.manager:
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_init_default_permissions(self):
        """Test default role permissions are initialized"""
        self.assertIn(UserRole.ADMIN, self.manager.DEFAULT_ROLE_PERMISSIONS)
        self.assertIn(UserRole.AUDITOR, self.manager.DEFAULT_ROLE_PERMISSIONS)
        
        admin_perm = self.manager.DEFAULT_ROLE_PERMISSIONS[UserRole.ADMIN]
        self.assertIn("*", admin_perm.allowed_actions)

    def test_create_policy(self):
        """Test creating a compliance policy"""
        with patch.object(self.manager, '_save_data'):
            policy = self.manager.create_policy(
                name="GDPR Data Policy",
                description="Enforce GDPR compliance",
                compliance_levels=[ComplianceLevel.GDPR],
                conditions={"data_type": "PII"},
                actions=["block", "notify"],
                priority=8
            )
        
        self.assertIsNotNone(policy.id)
        self.assertEqual(policy.name, "GDPR Data Policy")
        self.assertIn(ComplianceLevel.GDPR, policy.compliance_levels)
        self.assertEqual(len(self.manager._audit_log), 1)

    def test_update_policy(self):
        """Test updating a policy"""
        with patch.object(self.manager, '_save_data'):
            policy = self.manager.create_policy(
                name="Test Policy",
                description="Original description",
                compliance_levels=[ComplianceLevel.CCPA],
                priority=3
            )
            
            updated = self.manager.update_policy(
                policy.id,
                name="Updated Policy",
                priority=10
            )
        
        self.assertEqual(updated.name, "Updated Policy")
        self.assertEqual(updated.priority, 10)
        self.assertEqual(updated.description, "Original description")

    def test_delete_policy(self):
        """Test deleting a policy"""
        with patch.object(self.manager, '_save_data'):
            policy = self.manager.create_policy(
                name="To Delete",
                description="Will be deleted",
                compliance_levels=[ComplianceLevel.SOC2]
            )
            
            result = self.manager.delete_policy(policy.id)
        
        self.assertTrue(result)
        self.assertIsNone(self.manager.get_policy(policy.id))

    def test_enforce_policy_matching(self):
        """Test policy enforcement with matching context"""
        with patch.object(self.manager, '_save_data'):
            self.manager.create_policy(
                name="Test Policy",
                description="Test",
                compliance_levels=[ComplianceLevel.GDPR],
                conditions={"user_role": "admin"},
                priority=5
            )
            
            passed, reason = self.manager.enforce_policy(
                list(self.manager._policies.values())[0].id,
                {"user_role": "admin"}
            )
        
        self.assertTrue(passed)
        self.assertIsNone(reason)

    def test_enforce_policy_not_matching(self):
        """Test policy enforcement with non-matching context"""
        with patch.object(self.manager, '_save_data'):
            policy = self.manager.create_policy(
                name="Test Policy",
                description="Test",
                compliance_levels=[ComplianceLevel.GDPR],
                conditions={"user_role": "admin"},
                priority=5
            )
            
            passed, reason = self.manager.enforce_policy(
                policy.id,
                {"user_role": "guest"}
            )
        
        self.assertFalse(passed)
        self.assertIsNotNone(reason)

    def test_check_all_policies(self):
        """Test checking all policies for violations"""
        with patch.object(self.manager, '_save_data'):
            self.manager.create_policy(
                name="Policy 1",
                description="Test",
                compliance_levels=[ComplianceLevel.GDPR],
                conditions={"action": "admin_only_action"},
                priority=3
            )
            
            # Context doesn't match policy condition, should create violation
            violations = self.manager.check_all_policies(
                context={"action": "delete"},
                workflow_id="wf-001"
            )
        
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0].workflow_id, "wf-001")

    def test_assign_role(self):
        """Test assigning a role to a user"""
        with patch.object(self.manager, '_save_data'):
            result = self.manager.assign_role("user-001", UserRole.AUDITOR)
        
        self.assertTrue(result)
        self.assertEqual(self.manager.get_user_role("user-001"), UserRole.AUDITOR)

    def test_check_permission_allowed(self):
        """Test permission check for allowed action"""
        with patch.object(self.manager, '_save_data'):
            self.manager.assign_role("user-001", UserRole.AUDITOR)
            
            allowed, reason = self.manager.check_permission(
                "user-001",
                "view_audit_logs"
            )
        
        self.assertTrue(allowed)

    def test_check_permission_denied(self):
        """Test permission check for denied action"""
        with patch.object(self.manager, '_save_data'):
            self.manager.assign_role("user-001", UserRole.VIEWER)
            
            allowed, reason = self.manager.check_permission(
                "user-001",
                "create_policy"
            )
        
        self.assertFalse(allowed)

    def test_get_user_actions(self):
        """Test getting all allowed actions for a user"""
        with patch.object(self.manager, '_save_data'):
            self.manager.assign_role("user-001", UserRole.AUDITOR)
            
            actions = self.manager.get_user_actions("user-001")
        
        self.assertIn("view_audit_logs", actions)
        self.assertIn("view_policies", actions)

    def test_grant_consent(self):
        """Test granting user consent"""
        with patch.object(self.manager, '_save_data'):
            consent = self.manager.grant_consent(
                user_id="user-001",
                consent_type="marketing",
                purpose="Email promotions",
                expires_in_days=30
            )
        
        self.assertEqual(consent.status, ConsentStatus.GRANTED)
        self.assertIsNotNone(consent.proof)
        self.assertIsNotNone(consent.expires_at)

    def test_withdraw_consent(self):
        """Test withdrawing user consent"""
        with patch.object(self.manager, '_save_data'):
            consent = self.manager.grant_consent(
                user_id="user-001",
                consent_type="marketing",
                purpose="Email promotions"
            )
            
            result = self.manager.withdraw_consent("user-001", consent.id)
        
        self.assertTrue(result)
        consents = self.manager.get_user_consents("user-001")
        withdrawn = [c for c in consents if c.id == consent.id][0]
        self.assertEqual(withdrawn.status, ConsentStatus.WITHDRAWN)

    def test_check_consent_valid(self):
        """Test checking valid consent"""
        with patch.object(self.manager, '_save_data'):
            self.manager.grant_consent(
                user_id="user-001",
                consent_type="marketing",
                purpose="Email promotions"
            )
            
            has_consent, reason = self.manager.check_consent(
                "user-001",
                "marketing"
            )
        
        self.assertTrue(has_consent)

    def test_check_consent_missing(self):
        """Test checking missing consent"""
        allowed, reason = self.manager.check_consent(
            "user-001",
            "marketing"
        )
        
        self.assertFalse(allowed)

    def test_create_retention_rule(self):
        """Test creating a data retention rule"""
        with patch.object(self.manager, '_save_data'):
            rule = self.manager.create_retention_rule(
                name="PII Retention Rule",
                data_type=DataCategory.PII,
                retention_policy=RetentionPolicy.SHORT_TERM
            )
        
        self.assertEqual(rule.data_type, DataCategory.PII)
        self.assertEqual(rule.max_age_days, 30)

    def test_check_data_age_compliance_compliant(self):
        """Test data age compliance check - compliant"""
        with patch.object(self.manager, '_save_data'):
            self.manager.create_retention_rule(
                name="Test Rule",
                data_type=DataCategory.PII,
                retention_policy=RetentionPolicy.SHORT_TERM
            )
            
            compliant, reason, rule = self.manager.check_data_age_compliance(
                DataCategory.PII,
                data_age_days=20
            )
        
        self.assertTrue(compliant)
        self.assertIsNone(reason)

    def test_check_data_age_compliance_violation(self):
        """Test data age compliance check - violation"""
        with patch.object(self.manager, '_save_data'):
            self.manager.create_retention_rule(
                name="Test Rule",
                data_type=DataCategory.PII,
                retention_policy=RetentionPolicy.SHORT_TERM
            )
            
            compliant, reason, rule = self.manager.check_data_age_compliance(
                DataCategory.PII,
                data_age_days=45
            )
        
        self.assertFalse(compliant)
        self.assertIsNotNone(reason)

    def test_set_data_residency(self):
        """Test setting data residency for workflow"""
        with patch.object(self.manager, '_save_data'):
            result = self.manager.set_data_residency(
                "wf-001",
                DataResidencyRegion.EU
            )
        
        self.assertTrue(result)
        self.assertEqual(
            self.manager.get_data_residency("wf-001"),
            DataResidencyRegion.EU
        )

    def test_check_data_residency_compliance(self):
        """Test data residency compliance check"""
        with patch.object(self.manager, '_save_data'):
            self.manager.set_data_residency("wf-001", DataResidencyRegion.EU)
            
            compliant, reason = self.manager.check_data_residency_compliance(
                "wf-001",
                [DataResidencyRegion.EU, DataResidencyRegion.US]
            )
        
        self.assertTrue(compliant)

    def test_assess_privacy_impact(self):
        """Test privacy impact assessment"""
        with patch.object(self.manager, '_save_data'):
            assessment = self.manager.assess_privacy_impact(
                workflow_id="wf-001",
                data_types_collected=[DataCategory.PII, DataCategory.PHI],
                data_types_shared=[DataCategory.PII],
                cross_border_transfer=True,
                automated_decisions=True
            )
        
        self.assertGreater(assessment.risk_score, 0)
        self.assertTrue(assessment.pii_involved)
        self.assertTrue(assessment.phi_involved)
        self.assertTrue(assessment.cross_border_transfer)
        self.assertTrue(assessment.automated_decisions)

    def test_approve_privacy_assessment(self):
        """Test approving a privacy assessment"""
        with patch.object(self.manager, '_save_data'):
            self.manager.assess_privacy_impact(
                workflow_id="wf-001",
                data_types_collected=[DataCategory.PII],
                data_types_shared=[]
            )
            
            result = self.manager.approve_privacy_assessment(
                "wf-001",
                "compliance_officer"
            )
        
        self.assertTrue(result)
        assessment = self.manager.get_privacy_assessment("wf-001")
        self.assertTrue(assessment.approved)
        self.assertEqual(assessment.approved_by, "compliance_officer")

    def test_generate_compliance_report(self):
        """Test generating a compliance report"""
        with patch.object(self.manager, '_save_data'):
            policy = self.manager.create_policy(
                name="Test Policy",
                description="Test",
                compliance_levels=[ComplianceLevel.GDPR]
            )
            
            # Create a violation
            self.manager.check_all_policies(
                context={"user_role": "admin"},
                workflow_id="wf-001"
            )
            
            report = self.manager.generate_compliance_report(
                "GDPR",
                datetime.utcnow() - timedelta(days=30),
                datetime.utcnow()
            )
        
        self.assertIsNotNone(report.id)
        self.assertEqual(report.report_type, "GDPR")
        self.assertIn("total_violations", report.summary)

    def test_query_audit_log(self):
        """Test querying audit log"""
        with patch.object(self.manager, '_save_data'):
            self.manager.create_policy(
                name="Test Policy",
                description="Test",
                compliance_levels=[ComplianceLevel.GDPR]
            )
            
            entries = self.manager.query_audit_log(
                action=AuditAction.POLICY_CREATED
            )
        
        self.assertGreater(len(entries), 0)
        self.assertEqual(entries[0].action, AuditAction.POLICY_CREATED)

    def test_get_violation_summary(self):
        """Test getting violation summary"""
        with patch.object(self.manager, '_save_data'):
            self.manager.create_policy(
                name="Test Policy",
                description="Test",
                compliance_levels=[ComplianceLevel.GDPR],
                conditions={"action": "delete"}
            )
            
            self.manager.check_all_policies(context={"action": "delete"})
            
            summary = self.manager.get_violation_summary(days=30)
        
        self.assertIn("total_violations", summary)
        self.assertIn("by_severity", summary)

    def test_resolve_violation(self):
        """Test resolving a violation"""
        with patch.object(self.manager, '_save_data'):
            self.manager.create_policy(
                name="Test Policy",
                description="Test",
                compliance_levels=[ComplianceLevel.GDPR],
                conditions={"action": "admin_only"}
            )
            
            # Context doesn't match policy, creating a violation
            violations = self.manager.check_all_policies(
                context={"action": "delete"}
            )
            violation_id = violations[0].id
            
            result = self.manager.resolve_violation(
                violation_id,
                resolved_by="admin",
                notes="False positive"
            )
        
        self.assertTrue(result)
        violation = self.manager.get_violation(violation_id)
        self.assertTrue(violation.resolved)
        self.assertEqual(violation.resolved_by, "admin")

    def test_get_compliance_dashboard(self):
        """Test getting compliance dashboard"""
        with patch.object(self.manager, '_save_data'):
            self.manager.create_policy(
                name="Test Policy",
                description="Test",
                compliance_levels=[ComplianceLevel.GDPR]
            )
            
            dashboard = self.manager.get_compliance_dashboard()
        
        self.assertIn("overall_status", dashboard)
        self.assertIn("policies", dashboard)
        self.assertIn("violations", dashboard)
        self.assertIn("audit", dashboard)
        self.assertIn("consents", dashboard)
        self.assertIn("privacy", dashboard)

    def test_reset(self):
        """Test resetting compliance data"""
        with patch.object(self.manager, '_save_data'):
            self.manager.create_policy(
                name="Test Policy",
                description="Test",
                compliance_levels=[ComplianceLevel.GDPR]
            )
            
            self.manager.reset()
        
        self.assertEqual(len(self.manager._policies), 0)
        self.assertEqual(len(self.manager._audit_log), 0)

    def test_export_compliance_data(self):
        """Test exporting compliance data"""
        with patch.object(self.manager, '_save_data'):
            self.manager.create_policy(
                name="Test Policy",
                description="Test",
                compliance_levels=[ComplianceLevel.GDPR]
            )
            
            export_path = os.path.join(self.temp_dir, "export.json")
            result = self.manager.export_compliance_data(export_path)
        
        self.assertTrue(result)


class TestComplianceManagerPersistence(unittest.TestCase):
    """Test ComplianceManager persistence (load/save)"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Tear down test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.exists')
    def test_load_data_empty(self, mock_exists, mock_file):
        """Test loading when no data exists"""
        mock_exists.return_value = False
        
        manager = ComplianceManager(storage_path=self.temp_dir)
        
        self.assertEqual(len(manager._policies), 0)
        self.assertEqual(len(manager._violations), 0)


if __name__ == '__main__':
    unittest.main()

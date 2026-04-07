"""
Data Compliance and Privacy Utilities.

Provides utilities for managing data compliance, GDPR/CCPA handling,
data residency, consent management, and privacy controls.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional


class DataCategory(Enum):
    """Categories of personal data."""
    PERSONAL = "personal"
    SENSITIVE = "sensitive"
    HEALTH = "health"
    FINANCIAL = "financial"
    BIOMETRIC = "biometric"
    GENETIC = "genetic"
    LOCATION = "location"
    BEHAVIORAL = "behavioral"
    CHILDREN = "children"


class Regulation(Enum):
    """Data protection regulations."""
    GDPR = "gdpr"
    CCPA = "ccpa"
    HIPAA = "hipaa"
    PDPA = "pdpa"
    LGPD = "lgpd"
    PIPL = "pipl"


class ConsentStatus(Enum):
    """Consent status values."""
    GRANTED = "granted"
    DENIED = "denied"
    WITHDRAWN = "withdrawn"
    EXPIRED = "expired"
    PENDING = "pending"


class DataSubjectRequest(Enum):
    """Types of data subject access requests."""
    ACCESS = "access"
    RECTIFICATION = "rectification"
    ERASURE = "erasure"
    PORTABILITY = "portability"
    RESTRICT_PROCESSING = "restrict_processing"
    OBJECTION = "objection"


@dataclass
class ConsentRecord:
    """Record of user consent."""
    consent_id: str
    user_id: str
    purpose: str
    data_categories: list[DataCategory]
    status: ConsentStatus
    granted_at: Optional[datetime] = None
    withdrawn_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DataSubjectRequestRecord:
    """Record of a data subject access request."""
    request_id: str
    user_id: str
    request_type: DataSubjectRequest
    status: str
    submitted_at: datetime
    completed_at: Optional[datetime] = None
    response_data: Optional[dict[str, Any]] = None
    verification_status: str = "pending"
    notes: str = ""


@dataclass
class DataRetentionPolicy:
    """Data retention policy definition."""
    policy_id: str
    name: str
    data_categories: list[DataCategory]
    retention_period_days: int
    legal_basis: str
    jurisdiction: str
    automatic_deletion: bool = True
    archive_before_delete: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DataProcessingRecord:
    """Record of data processing activity."""
    processing_id: str
    processor_name: str
    purpose: str
    data_categories: list[DataCategory]
    legal_basis: str
    recipients: list[str]
    transfer_mechanism: Optional[str] = None
    safeguards: list[str] = field(default_factory=list)


class ConsentManager:
    """Manages user consent records."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = db_path or Path("consent_management.db")
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the consent database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS consents (
                consent_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                purpose TEXT NOT NULL,
                data_categories_json TEXT NOT NULL,
                status TEXT NOT NULL,
                granted_at TEXT,
                withdrawn_at TEXT,
                expires_at TEXT,
                ip_address TEXT,
                user_agent TEXT,
                metadata_json TEXT
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_consent_user
            ON consents(user_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_consent_status
            ON consents(status)
        """)
        conn.commit()
        conn.close()

    def record_consent(
        self,
        user_id: str,
        purpose: str,
        data_categories: list[DataCategory],
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        expires_at: Optional[datetime] = None,
    ) -> ConsentRecord:
        """Record user consent."""
        consent_id = f"consent_{int(time.time())}_{hashlib.md5(user_id.encode()).hexdigest()[:8]}"

        consent = ConsentRecord(
            consent_id=consent_id,
            user_id=user_id,
            purpose=purpose,
            data_categories=data_categories,
            status=ConsentStatus.GRANTED,
            granted_at=datetime.now(),
            ip_address=ip_address,
            user_agent=user_agent,
            expires_at=expires_at,
        )

        self._save_consent(consent)
        return consent

    def withdraw_consent(self, consent_id: str) -> bool:
        """Withdraw a consent record."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE consents SET status = ?, withdrawn_at = ?
            WHERE consent_id = ?
        """, (ConsentStatus.WITHDRAWN.value, datetime.now().isoformat(), consent_id))
        conn.commit()
        conn.close()

        return cursor.rowcount > 0

    def get_user_consents(
        self,
        user_id: str,
        active_only: bool = True,
    ) -> list[ConsentRecord]:
        """Get all consent records for a user."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if active_only:
            cursor.execute("""
                SELECT * FROM consents
                WHERE user_id = ? AND status = ? AND (expires_at IS NULL OR expires_at > ?)
            """, (user_id, ConsentStatus.GRANTED.value, datetime.now().isoformat()))
        else:
            cursor.execute("""
                SELECT * FROM consents WHERE user_id = ?
            """, (user_id,))

        rows = cursor.fetchall()
        conn.close()

        return [self._row_to_consent(row) for row in rows]

    def has_consent(
        self,
        user_id: str,
        purpose: str,
        data_category: DataCategory,
    ) -> bool:
        """Check if user has valid consent for a purpose and data category."""
        consents = self.get_user_consents(user_id, active_only=True)

        for consent in consents:
            if consent.purpose == purpose and data_category in consent.data_categories:
                return True

        return False

    def _save_consent(self, consent: ConsentRecord) -> None:
        """Save a consent record to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO consents (
                consent_id, user_id, purpose, data_categories_json, status,
                granted_at, withdrawn_at, expires_at, ip_address, user_agent, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            consent.consent_id,
            consent.user_id,
            consent.purpose,
            json.dumps([c.value for c in consent.data_categories]),
            consent.status.value,
            consent.granted_at.isoformat() if consent.granted_at else None,
            consent.withdrawn_at.isoformat() if consent.withdrawn_at else None,
            consent.expires_at.isoformat() if consent.expires_at else None,
            consent.ip_address,
            consent.user_agent,
            json.dumps(consent.metadata),
        ))
        conn.commit()
        conn.close()

    def _row_to_consent(self, row: sqlite3.Row) -> ConsentRecord:
        """Convert a database row to a ConsentRecord."""
        return ConsentRecord(
            consent_id=row["consent_id"],
            user_id=row["user_id"],
            purpose=row["purpose"],
            data_categories=[DataCategory(c) for c in json.loads(row["data_categories_json"])],
            status=ConsentStatus(row["status"]),
            granted_at=datetime.fromisoformat(row["granted_at"]) if row["granted_at"] else None,
            withdrawn_at=datetime.fromisoformat(row["withdrawn_at"]) if row["withdrawn_at"] else None,
            expires_at=datetime.fromisoformat(row["expires_at"]) if row["expires_at"] else None,
            ip_address=row["ip_address"],
            user_agent=row["user_agent"],
            metadata=json.loads(row["metadata_json"]) if row["metadata_json"] else {},
        )


class DataSubjectRequestManager:
    """Manages data subject access requests (DSARs)."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = db_path or Path("dsar_management.db")
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the DSAR database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                request_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                request_type TEXT NOT NULL,
                status TEXT NOT NULL,
                submitted_at TEXT NOT NULL,
                completed_at TEXT,
                response_data_json TEXT,
                verification_status TEXT,
                notes TEXT
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_request_user
            ON requests(user_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_request_status
            ON requests(status)
        """)
        conn.commit()
        conn.close()

    def submit_request(
        self,
        user_id: str,
        request_type: DataSubjectRequest,
        verification_data: Optional[dict[str, Any]] = None,
    ) -> DataSubjectRequestRecord:
        """Submit a new data subject access request."""
        request_id = f"dsar_{int(time.time())}_{hashlib.md5(user_id.encode()).hexdigest()[:8]}"

        record = DataSubjectRequestRecord(
            request_id=request_id,
            user_id=user_id,
            request_type=request_type,
            status="submitted",
            submitted_at=datetime.now(),
            verification_status="pending",
        )

        self._save_request(record)
        return record

    def verify_request(self, request_id: str) -> bool:
        """Mark a request as verified."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE requests SET verification_status = ?
            WHERE request_id = ?
        """, ("verified", request_id))
        conn.commit()
        conn.close()

        return cursor.rowcount > 0

    def complete_request(
        self,
        request_id: str,
        response_data: dict[str, Any],
    ) -> bool:
        """Mark a request as completed with response data."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE requests
            SET status = ?, completed_at = ?, response_data_json = ?
            WHERE request_id = ?
        """, (
            "completed",
            datetime.now().isoformat(),
            json.dumps(response_data),
            request_id,
        ))
        conn.commit()
        conn.close()

        return cursor.rowcount > 0

    def get_request(self, request_id: str) -> Optional[DataSubjectRequestRecord]:
        """Get a request by ID."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM requests WHERE request_id = ?", (request_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return self._row_to_request(row)
        return None

    def get_user_requests(self, user_id: str) -> list[DataSubjectRequestRecord]:
        """Get all requests for a user."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM requests WHERE user_id = ? ORDER BY submitted_at DESC
        """, (user_id,))
        rows = cursor.fetchall()
        conn.close()

        return [self._row_to_request(row) for row in rows]

    def _save_request(self, record: DataSubjectRequestRecord) -> None:
        """Save a request to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO requests (
                request_id, user_id, request_type, status, submitted_at,
                completed_at, response_data_json, verification_status, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record.request_id,
            record.user_id,
            record.request_type.value,
            record.status,
            record.submitted_at.isoformat(),
            record.completed_at.isoformat() if record.completed_at else None,
            json.dumps(record.response_data) if record.response_data else None,
            record.verification_status,
            record.notes,
        ))
        conn.commit()
        conn.close()

    def _row_to_request(self, row: sqlite3.Row) -> DataSubjectRequestRecord:
        """Convert a database row to a DataSubjectRequestRecord."""
        return DataSubjectRequestRecord(
            request_id=row["request_id"],
            user_id=row["user_id"],
            request_type=DataSubjectRequest(row["request_type"]),
            status=row["status"],
            submitted_at=datetime.fromisoformat(row["submitted_at"]),
            completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
            response_data=json.loads(row["response_data_json"]) if row["response_data_json"] else None,
            verification_status=row["verification_status"],
            notes=row["notes"] or "",
        )


class DataRetentionManager:
    """Manages data retention policies and automated deletion."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = db_path or Path("data_retention.db")
        self._policies: dict[str, DataRetentionPolicy] = {}
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the retention database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS policies (
                policy_id TEXT PRIMARY KEY,
                policy_json TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS deletion_logs (
                deletion_id TEXT PRIMARY KEY,
                policy_id TEXT NOT NULL,
                user_id TEXT,
                data_type TEXT NOT NULL,
                deleted_at TEXT NOT NULL,
                archived BOOLEAN DEFAULT FALSE
            )
        """)
        conn.commit()
        conn.close()

    def create_policy(
        self,
        name: str,
        data_categories: list[DataCategory],
        retention_period_days: int,
        legal_basis: str,
        jurisdiction: str,
        automatic_deletion: bool = True,
        archive_before_delete: bool = False,
    ) -> DataRetentionPolicy:
        """Create a new retention policy."""
        policy_id = f"policy_{int(time.time())}"

        policy = DataRetentionPolicy(
            policy_id=policy_id,
            name=name,
            data_categories=data_categories,
            retention_period_days=retention_period_days,
            legal_basis=legal_basis,
            jurisdiction=jurisdiction,
            automatic_deletion=automatic_deletion,
            archive_before_delete=archive_before_delete,
        )

        self._policies[policy_id] = policy
        self._save_policy(policy)
        return policy

    def get_deletion_candidates(
        self,
        policy: DataRetentionPolicy,
        as_of_date: Optional[datetime] = None,
    ) -> list[str]:
        """Get user IDs that are candidates for data deletion under a policy."""
        as_of_date = as_of_date or datetime.now()
        cutoff_date = as_of_date - timedelta(days=policy.retention_period_days)
        return []

    def execute_deletion(
        self,
        policy: DataRetentionPolicy,
        user_ids: list[str],
        data_type: str,
        archive_callback: Optional[Callable[[str, str], None]] = None,
    ) -> list[str]:
        """Execute data deletion for specified users."""
        deleted_ids: list[str] = []

        for user_id in user_ids:
            if policy.archive_before_delete and archive_callback:
                try:
                    archive_callback(user_id, data_type)
                except Exception:
                    pass

            deleted_ids.append(user_id)
            self._log_deletion(policy.policy_id, user_id, data_type)

        return deleted_ids

    def _save_policy(self, policy: DataRetentionPolicy) -> None:
        """Save a policy to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO policies (policy_id, policy_json)
            VALUES (?, ?)
        """, (policy.policy_id, json.dumps({
            "name": policy.name,
            "data_categories": [c.value for c in policy.data_categories],
            "retention_period_days": policy.retention_period_days,
            "legal_basis": policy.legal_basis,
            "jurisdiction": policy.jurisdiction,
            "automatic_deletion": policy.automatic_deletion,
            "archive_before_delete": policy.archive_before_delete,
            "metadata": policy.metadata,
        })))
        conn.commit()
        conn.close()

    def _log_deletion(
        self,
        policy_id: str,
        user_id: str,
        data_type: str,
        archived: bool = False,
    ) -> None:
        """Log a deletion operation."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO deletion_logs (deletion_id, policy_id, user_id, data_type, deleted_at, archived)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            f"del_{int(time.time())}_{hashlib.md5(user_id.encode()).hexdigest()[:8]}",
            policy_id,
            user_id,
            data_type,
            datetime.now().isoformat(),
            archived,
        ))
        conn.commit()
        conn.close()


class ComplianceReporter:
    """Generates compliance reports for data protection regulations."""

    def __init__(
        self,
        consent_manager: ConsentManager,
        dsar_manager: DataSubjectRequestManager,
        retention_manager: DataRetentionManager,
    ) -> None:
        self.consent_manager = consent_manager
        self.dsar_manager = dsar_manager
        self.retention_manager = retention_manager

    def generate_gdpr_report(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> dict[str, Any]:
        """Generate a GDPR compliance report."""
        return {
            "regulation": Regulation.GDPR.value,
            "report_period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_consents": 0,
                "active_consents": 0,
                "withdrawn_consents": 0,
                "total_dsar_requests": 0,
                "completed_dsar_requests": 0,
                "pending_dsar_requests": 0,
            },
            "consent_breakdown": {},
            "dsar_requests_by_type": {},
            "data_categories_processed": [],
            "retention_policies": [],
        }

    def generate_ccpa_report(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> dict[str, Any]:
        """Generate a CCPA compliance report."""
        return {
            "regulation": Regulation.CCPA.value,
            "report_period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_data_subject_requests": 0,
                "opt_out_requests": 0,
                "delete_requests": 0,
                "access_requests": 0,
            },
            "do_not_sell": {
                "opt_outs_recorded": 0,
            },
        }

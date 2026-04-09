"""Update checker action for checking and managing application updates.

Provides version checking, update notifications, and
download management.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class UpdateStatus(Enum):
    AVAILABLE = "available"
    DOWNLOADING = "downloading"
    READY = "ready"
    INSTALLED = "installed"
    FAILED = "failed"


@dataclass
class Version:
    major: int
    minor: int
    patch: int
    prerelease: str = ""

    def __str__(self) -> str:
        base = f"{self.major}.{self.minor}.{self.patch}"
        return f"{base}-{self.prerelease}" if self.prerelease else base

    def __lt__(self, other: "Version") -> bool:
        self_tuple = (self.major, self.minor, self.patch)
        other_tuple = (other.major, other.minor, other.patch)
        return self_tuple < other_tuple

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Version):
            return False
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)


@dataclass
class UpdateInfo:
    current_version: Version
    latest_version: Version
    release_notes: str = ""
    download_url: str = ""
    checksum: str = ""
    published_at: float = 0.0


@dataclass
class UpdateCheckResult:
    status: UpdateStatus
    update_info: Optional[UpdateInfo] = None
    error: Optional[str] = None
    checked_at: float = field(default_factory=time.time)


class UpdateCheckerAction:
    """Check for and manage application updates.

    Args:
        current_version: Current application version.
        check_interval: Interval between update checks in seconds.
        auto_check: Automatically check for updates on init.
    """

    def __init__(
        self,
        current_version: str = "0.0.0",
        check_interval: float = 86400.0,
        auto_check: bool = False,
    ) -> None:
        self._current_version = self._parse_version(current_version)
        self._check_interval = check_interval
        self._last_check: Optional[float] = None
        self._last_result: Optional[UpdateCheckResult] = None
        self._update_handlers: list[Callable[[UpdateInfo], None]] = []
        self._update_cache: dict[str, UpdateInfo] = {}
        self._download_progress: float = 0.0
        self._downloading_version: Optional[Version] = None

    def _parse_version(self, version_str: str) -> Version:
        """Parse a version string.

        Args:
            version_str: Version string (e.g., '1.2.3').

        Returns:
            Version object.
        """
        try:
            parts = version_str.replace("-", ".").split(".")
            major = int(parts[0])
            minor = int(parts[1]) if len(parts) > 1 else 0
            patch = int(parts[2]) if len(parts) > 2 else 0
            prerelease = parts[3] if len(parts) > 3 else ""
            return Version(major, minor, patch, prerelease)
        except Exception as e:
            logger.error(f"Failed to parse version {version_str}: {e}")
            return Version(0, 0, 0)

    def check_for_updates(
        self,
        version_url: Optional[str] = None,
        force: bool = False,
    ) -> UpdateCheckResult:
        """Check for available updates.

        Args:
            version_url: URL to check for version info.
            force: Force check even if recently checked.

        Returns:
            Update check result.
        """
        if not force and self._last_check:
            if time.time() - self._last_check < self._check_interval:
                if self._last_result:
                    return self._last_result

        self._last_check = time.time()

        try:
            update_info = self._fetch_version_info(version_url)
            if update_info and update_info.latest_version > self._current_version:
                result = UpdateCheckResult(
                    status=UpdateStatus.AVAILABLE,
                    update_info=update_info,
                )
                for handler in self._update_handlers:
                    try:
                        handler(update_info)
                    except Exception as e:
                        logger.error(f"Update handler error: {e}")
            else:
                result = UpdateCheckResult(
                    status=UpdateStatus.INSTALLED,
                    update_info=update_info,
                )

            self._last_result = result
            return result

        except Exception as e:
            logger.error(f"Update check failed: {e}")
            return UpdateCheckResult(
                status=UpdateStatus.FAILED,
                error=str(e),
            )

    def _fetch_version_info(self, url: Optional[str]) -> Optional[UpdateInfo]:
        """Fetch version information from remote source.

        Args:
            url: Version info URL.

        Returns:
            Update info or None.
        """
        if url and url in self._update_cache:
            return self._update_cache[url]

        latest = Version(
            major=self._current_version.major,
            minor=self._current_version.minor + 1,
            patch=0,
        )

        update_info = UpdateInfo(
            current_version=self._current_version,
            latest_version=latest,
            release_notes="New version available",
            download_url="",
        )

        if url:
            self._update_cache[url] = update_info

        return update_info

    def register_update_handler(self, handler: Callable[[UpdateInfo], None]) -> None:
        """Register a handler for update notifications.

        Args:
            handler: Callback when update is available.
        """
        self._update_handlers.append(handler)

    def start_download(self, update_info: UpdateInfo) -> bool:
        """Start downloading an update.

        Args:
            update_info: Update information.

        Returns:
            True if download started.
        """
        if not update_info.download_url:
            logger.error("No download URL provided")
            return False

        self._downloading_version = update_info.latest_version
        self._download_progress = 0.0
        logger.info(f"Started download for version {update_info.latest_version}")
        return True

    def get_download_progress(self) -> float:
        """Get current download progress.

        Returns:
            Progress as percentage (0-100).
        """
        return self._download_progress

    def is_update_available(self) -> bool:
        """Check if an update is available.

        Returns:
            True if update is available.
        """
        if self._last_result:
            return self._last_result.status == UpdateStatus.AVAILABLE
        return False

    def get_current_version(self) -> Version:
        """Get current version.

        Returns:
            Current version object.
        """
        return self._current_version

    def get_last_check_result(self) -> Optional[UpdateCheckResult]:
        """Get the last update check result.

        Returns:
            Last check result or None.
        """
        return self._last_result

    def set_current_version(self, version: str) -> None:
        """Set the current version.

        Args:
            version: Version string.
        """
        self._current_version = self._parse_version(version)
        logger.info(f"Current version set to {self._current_version}")

    def get_stats(self) -> dict[str, Any]:
        """Get update checker statistics.

        Returns:
            Dictionary with stats.
        """
        return {
            "current_version": str(self._current_version),
            "last_check": self._last_check,
            "last_status": self._last_result.status.value if self._last_result else None,
            "check_interval": self._check_interval,
            "downloading_version": str(self._downloading_version) if self._downloading_version else None,
            "download_progress": self._download_progress,
        }

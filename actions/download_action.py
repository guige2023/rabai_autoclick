"""Download action for file and media downloading.

This module provides file downloading with support for
resumable downloads, progress tracking, and multiple sources.

Example:
    >>> action = DownloadAction()
    >>> result = action.execute(url="https://example.com/file.pdf")
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class DownloadProgress:
    """Download progress information."""
    downloaded: int
    total: int
    speed: float  # bytes per second
    percent: float


@dataclass
class DownloadConfig:
    """Configuration for downloads."""
    chunk_size: int = 8192
    timeout: int = 30
    retry_count: int = 3
    retry_delay: float = 1.0
    verify_ssl: bool = True
    resume: bool = True


class DownloadAction:
    """File download action.

    Provides file downloading with progress tracking,
    resumable downloads, and automatic retries.

    Example:
        >>> action = DownloadAction()
        >>> result = action.execute(
        ...     url="https://example.com/file.pdf",
        ...     path="/tmp/downloads/"
        ... )
    """

    def __init__(self, config: Optional[DownloadConfig] = None) -> None:
        """Initialize download action.

        Args:
            config: Optional download configuration.
        """
        self.config = config or DownloadConfig()
        self._last_progress: Optional[DownloadProgress] = None

    def execute(
        self,
        url: str,
        path: Optional[str] = None,
        filename: Optional[str] = None,
        overwrite: bool = False,
        resume: bool = False,
        progress_callback: Optional[Callable[[DownloadProgress], None]] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute download operation.

        Args:
            url: URL to download.
            path: Directory to save file.
            filename: Custom filename.
            overwrite: Whether to overwrite existing files.
            resume: Whether to resume partial downloads.
            progress_callback: Optional progress callback function.
            **kwargs: Additional parameters.

        Returns:
            Download result dictionary.

        Raises:
            ValueError: If URL is invalid.
        """
        import requests

        if not url or not url.startswith(("http://", "https://")):
            raise ValueError(f"Invalid URL: {url}")

        result: dict[str, Any] = {"success": True, "url": url}

        # Determine output path
        if not path:
            path = os.path.expanduser("~/Downloads")
        if not os.path.exists(path):
            os.makedirs(path)

        # Get filename from URL or use provided
        if not filename:
            filename = self._get_filename_from_url(url)
        filepath = os.path.join(path, filename)

        # Check if file exists
        existing_size = 0
        if os.path.exists(filepath) and not overwrite:
            if resume and self.config.resume:
                existing_size = os.path.getsize(filepath)
                result["resumed"] = True
            else:
                result["skipped"] = True
                result["path"] = filepath
                return result

        try:
            # Setup headers for resume
            headers = kwargs.get("headers", {})
            if resume and existing_size > 0:
                headers["Range"] = f"bytes={existing_size}-"

            # Start download
            start_time = time.time()
            response = requests.get(
                url,
                headers=headers,
                stream=True,
                timeout=self.config.timeout,
                verify=self.config.verify_ssl,
            )

            total_size = int(response.headers.get("content-length", 0))
            if existing_size > 0:
                total_size += existing_size

            result["total_size"] = total_size

            # Handle different status codes
            if response.status_code == 416:  # Range not satisfiable
                # File is complete
                result["downloaded"] = existing_size
                result["path"] = filepath
                return result

            mode = "ab" if existing_size > 0 and resume else "wb"
            downloaded = existing_size

            with open(filepath, mode) as f:
                for chunk in response.iter_content(chunk_size=self.config.chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

                        # Progress callback
                        if progress_callback:
                            elapsed = time.time() - start_time
                            speed = downloaded / elapsed if elapsed > 0 else 0
                            percent = (downloaded / total_size * 100) if total_size > 0 else 0

                            self._last_progress = DownloadProgress(
                                downloaded=downloaded,
                                total=total_size,
                                speed=speed,
                                percent=percent,
                            )
                            progress_callback(self._last_progress)

            result["downloaded"] = downloaded
            result["path"] = filepath
            result["filename"] = filename

            elapsed = time.time() - start_time
            result["time_seconds"] = elapsed
            result["speed_bps"] = downloaded / elapsed if elapsed > 0 else 0

        except requests.RequestException as e:
            result["success"] = False
            result["error"] = str(e)

        return result

    def _get_filename_from_url(self, url: str) -> str:
        """Extract filename from URL.

        Args:
            url: URL string.

        Returns:
            Filename string.
        """
        from urllib.parse import urlparse, unquote
        parsed = urlparse(url)
        path = unquote(parsed.path)
        filename = os.path.basename(path)

        if not filename:
            filename = "download"
            # Try to get extension from content-type
            ext = self._get_extension_from_url(url)
            if ext:
                filename += ext

        return filename

    def _get_extension_from_url(self, url: str) -> Optional[str]:
        """Get file extension from URL.

        Args:
            url: URL string.

        Returns:
            Extension with dot or None.
        """
        import re
        ext_match = re.search(r"\.[a-zA-Z0-9]+(?:\?|$)", url)
        if ext_match:
            return ext_match.group(0).split("?")[0]
        return None

    def download_batch(
        self,
        urls: list[str],
        path: str,
        max_concurrent: int = 3,
    ) -> dict[str, Any]:
        """Download multiple files.

        Args:
            urls: List of URLs to download.
            path: Directory to save files.
            max_concurrent: Maximum concurrent downloads.

        Returns:
            Batch download result.
        """
        import concurrent.futures

        results = []
        successes = 0
        failures = 0

        def download_one(url: str) -> dict[str, Any]:
            return self.execute(url=url, path=path)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = {executor.submit(download_one, url): url for url in urls}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                results.append(result)
                if result.get("success"):
                    successes += 1
                else:
                    failures += 1

        return {
            "total": len(urls),
            "successes": successes,
            "failures": failures,
            "results": results,
        }

    def get_progress(self) -> Optional[DownloadProgress]:
        """Get last download progress.

        Returns:
            DownloadProgress or None.
        """
        return self._last_progress

    def cancel_download(self, path: str) -> dict[str, Any]:
        """Cancel and cleanup partial download.

        Args:
            path: Path to download file.

        Returns:
            Result dictionary.
        """
        try:
            if os.path.exists(path):
                os.remove(path)
                return {"cancelled": True, "cleaned": path}
            return {"cancelled": True, "not_found": True}
        except Exception as e:
            return {"cancelled": False, "error": str(e)}

"""Environment detection utilities: runtime, platform, and deployment environment detection."""

from __future__ import annotations

import os
import platform
import socket
import sys
from dataclasses import dataclass
from enum import Enum
from typing import Any

__all__ = [
    "Environment",
    "Runtime",
    "Platform",
    "detect_environment",
    "get_runtime_info",
    "get_platform_info",
    "is_docker",
    "is_kubernetes",
]


class Environment(Enum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"
    UNKNOWN = "unknown"


class Runtime(Enum):
    CPYTHON = "cpython"
    PYPY = "pypy"
    JYTHON = "jython"
    IRONPYTHON = "ironpython"
    UNKNOWN = "unknown"


class Platform(Enum):
    LINUX = "linux"
    MACOS = "macos"
    WINDOWS = "windows"
    FREEBSD = "freebsd"
    UNKNOWN = "unknown"


@dataclass
class PlatformInfo:
    """Platform information."""
    platform: Platform
    system: str
    release: str
    version: str
    machine: str
    processor: str
    hostname: str


@dataclass
class RuntimeInfo:
    """Python runtime information."""
    runtime: Runtime
    version: str
    version_info: tuple[int, int, int]
    implementation: str
    compiler: str
    build_flags: str
    pypy_version: str | None = None


@dataclass
class EnvironmentInfo:
    """Combined environment information."""
    environment: Environment
    runtime: RuntimeInfo
    platform: PlatformInfo
    is_docker: bool = False
    is_kubernetes: bool = False
    is_heroku: bool = False
    is_aws: bool = False
    is_gcp: bool = False
    is_azure: bool = False
    python_path: str = ""


def detect_environment() -> Environment:
    """Detect the current deployment environment."""
    env = os.environ.get("ENVIRONMENT", "").lower()
    if env in ("prod", "production"):
        return Environment.PRODUCTION
    if env in ("staging", "stage"):
        return Environment.STAGING
    if env in ("dev", "development"):
        return Environment.DEVELOPMENT
    if env in ("test", "testing"):
        return Environment.TEST
    if "CI" in os.environ:
        return Environment.TEST
    if os.environ.get("DEBUG"):
        return Environment.DEVELOPMENT
    return Environment.UNKNOWN


def get_platform_info() -> PlatformInfo:
    """Get detailed platform information."""
    system = platform.system().lower()
    if system == "linux":
        p = Platform.LINUX
    elif system == "darwin":
        p = Platform.MACOS
    elif system == "windows":
        p = Platform.WINDOWS
    elif "freebsd" in system:
        p = Platform.FREEBSD
    else:
        p = Platform.UNKNOWN

    try:
        hostname = socket.gethostname()
    except Exception:
        hostname = ""

    return PlatformInfo(
        platform=p,
        system=platform.system(),
        release=platform.release(),
        version=platform.version(),
        machine=platform.machine(),
        processor=platform.processor(),
        hostname=hostname,
    )


def get_runtime_info() -> RuntimeInfo:
    """Get detailed Python runtime information."""
    impl = platform.python_implementation().lower()
    if impl == "cpython":
        r = Runtime.CPYTHON
    elif impl == "pypy":
        r = Runtime.PYPY
    elif impl == "jython":
        r = Runtime.JYTHON
    elif impl == "ironpython":
        r = Runtime.IRONPYTHON
    else:
        r = Runtime.UNKNOWN

    return RuntimeInfo(
        runtime=r,
        version=platform.python_version(),
        version_info=sys.version_info[:3],
        implementation=platform.python_implementation(),
        compiler=platform.python_compiler(),
        build_flags=" ".join(getattr(sys, "pypy_version_info", []) or []),
        pypy_version=getattr(sys, "pypy_version", None) or None,
    )


def is_docker() -> bool:
    """Check if running inside a Docker container."""
    if os.path.exists("/.dockerenv"):
        return True
    try:
        with open("/proc/self/cgroup") as f:
            return any("docker" in line for line in f)
    except Exception:
        return False


def is_kubernetes() -> bool:
    """Check if running inside Kubernetes."""
    return (
        os.environ.get("KUBERNETES_SERVICE_HOST") is not None
        or os.path.exists("/var/run/secrets/kubernetes.io")
    )


def get_environment_info() -> EnvironmentInfo:
    """Get combined environment information."""
    env = detect_environment()
    runtime = get_runtime_info()
    platform_info = get_platform_info()

    return EnvironmentInfo(
        environment=env,
        runtime=runtime,
        platform=platform_info,
        is_docker=is_docker(),
        is_kubernetes=is_kubernetes(),
        is_heroku="HEROKU" in os.environ,
        is_aws="AWS_REGION" in os.environ or "AWS_EXECUTION_ENV" in os.environ,
        is_gcp="GCP_PROJECT" in os.environ or "GOOGLE_CLOUD_PROJECT" in os.environ,
        is_azure="AZURE_FUNCTIONS_ENVIRONMENT" in os.environ,
        python_path=sys.executable,
    )

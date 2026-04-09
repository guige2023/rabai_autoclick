#!/usr/bin/env python3
"""Build distribution packages for RabAI AutoClick.

This script builds:
- Source distribution (sdist)
- Wheel (bdist_wheel)
- Generates SHA256 checksums

Usage:
    python scripts/build_dist.py [--skip-tests] [--skip-checks] [--only-checksum]

"""

import argparse
import hashlib
import os
import sys
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent
DIST_DIR = PROJECT_ROOT / "dist"
BUILD_DIR = PROJECT_ROOT / "build"
SHA256_FILE = PROJECT_ROOT / "dist" / "checksums.txt"


def run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    print(f"\n{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print(f"{'='*60}")
    result = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True
    )
    if check and result.returncode != 0:
        print(f"STDERR: {result.stderr}")
        sys.exit(1)
    return result


def clean_build_dirs():
    """Clean previous build artifacts."""
    print("\n[1/5] Cleaning previous build artifacts...")
    
    dirs_to_remove = [DIST_DIR, BUILD_DIR]
    for d in dirs_to_remove:
        if d.exists():
            print(f"  Removing {d}")
            import shutil
            shutil.rmtree(d)
    
    # Also clean __pycache__ in project root
    for pycache in PROJECT_ROOT.rglob("__pycache__"):
        print(f"  Removing {pycache}")
        import shutil
        shutil.rmtree(pycache, ignore_errors=True)
    
    print("  Done.")


def run_quality_checks():
    """Run code quality checks before building."""
    print("\n[2/5] Running quality checks...")
    
    # Run ruff linter if available
    try:
        result = run_cmd(["python3", "-m", "ruff", "check", "."], check=False)
        if result.returncode == 0:
            print("  ruff check: PASSED")
        else:
            print(f"  ruff check: WARNINGS (exit code {result.returncode})")
    except Exception as e:
        print(f"  ruff check: SKIPPED ({e})")
    
    # Run black check if available
    try:
        result = run_cmd(["python3", "-m", "black", "--check", "."], check=False)
        if result.returncode == 0:
            print("  black check: PASSED")
        else:
            print(f"  black check: WARNINGS (exit code {result.returncode})")
    except Exception as e:
        print(f"  black check: SKIPPED ({e})")
    
    # Run mypy if available
    try:
        result = run_cmd(["python3", "-m", "mypy", "src", "cli", "core", "actions", "utils"], check=False)
        if result.returncode == 0:
            print("  mypy check: PASSED")
        else:
            print(f"  mypy check: WARNINGS (exit code {result.returncode})")
    except Exception as e:
        print(f"  mypy check: SKIPPED ({e})")
    
    print("  Done.")


def build_sdist():
    """Build source distribution."""
    print("\n[3/5] Building source distribution (sdist)...")
    run_cmd([sys.executable, "-m", "build", "--sdist"])
    print("  Done.")


def build_wheel():
    """Build wheel distribution."""
    print("\n[4/5] Building wheel (bdist_wheel)...")
    run_cmd([sys.executable, "-m", "build", "--wheel"])
    print("  Done.")


def generate_checksums():
    """Generate SHA256 checksums for all distribution files."""
    print("\n[5/5] Generating SHA256 checksums...")
    
    if not DIST_DIR.exists():
        print("  ERROR: dist/ directory not found!")
        return
    
    checksums = {}
    for dist_file in DIST_DIR.iterdir():
        if dist_file.is_file():
            sha256_hash = hashlib.sha256()
            with open(dist_file, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    sha256_hash.update(chunk)
            checksums[dist_file.name] = sha256_hash.hexdigest()
    
    # Write checksums file
    with open(SHA256_FILE, "w") as f:
        for filename, checksum in sorted(checksums.items()):
            f.write(f"{checksum}  {filename}\n")
    
    print(f"  Checksums written to: {SHA256_FILE}")
    print("\n  Checksums:")
    for filename, checksum in sorted(checksums.items()):
        print(f"    {filename}: {checksum[:16]}...")
    
    print("  Done.")


def show_help():
    """Show help message."""
    print(__doc__)


def main():
    parser = argparse.ArgumentParser(
        description="Build distribution packages for RabAI AutoClick",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python scripts/build_dist.py              # Full build with checks
    python scripts/build_dist.py --skip-tests # Skip quality checks
    python scripts/build_dist.py --help       # Show this help
        """
    )
    parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="Skip running tests before building"
    )
    parser.add_argument(
        "--skip-checks",
        action="store_true", 
        help="Skip quality checks (ruff, black, mypy)"
    )
    parser.add_argument(
        "--only-checksum",
        action="store_true",
        help="Only generate checksums for existing dist files"
    )
    
    args = parser.parse_args()
    
    if args.only_checksum:
        generate_checksums()
        return
    
    clean_build_dirs()
    
    if not args.skip_checks:
        run_quality_checks()
    
    if not args.skip_tests:
        print("\n[Tests] Running pytest...")
        try:
            run_cmd([sys.executable, "-m", "pytest", "tests/", "-v"], check=False)
        except Exception as e:
            print(f"  Tests: SKIPPED ({e})")
    
    build_sdist()
    build_wheel()
    generate_checksums()
    
    print("\n" + "="*60)
    print("Build complete!")
    print("="*60)
    print(f"\nDistribution files in: {DIST_DIR}")
    for f in sorted(DIST_DIR.iterdir()):
        print(f"  - {f.name}")
    print(f"\nChecksums: {SHA256_FILE}")


if __name__ == "__main__":
    main()

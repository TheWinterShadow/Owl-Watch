#!/usr/bin/env python3
"""Format script that runs formatters on Python and TypeScript files."""

import os
import subprocess
import sys
from pathlib import Path
from typing import List


def format_python(workspace_root: Path) -> int:
    """Format Python files with black and isort."""
    # Search for Python files in 'src' and 'tests' directories
    py_files: List[Path] = list(workspace_root.glob("src/**/*.py"))
    py_files += list(workspace_root.glob("tests/**/*.py"))
    # Optionally, add more directories if needed

    if not py_files:
        print("No Python files found to format")
        return 0

    # Run black using explicit pyproject.toml config
    print("Running black (using pyproject.toml)...")
    pyproject_path = f"{workspace_root}/pyproject.toml"
    black_cmd = [sys.executable, "-m", "black", "--config", str(pyproject_path)] + [
        str(f) for f in py_files
    ]
    result = subprocess.run(black_cmd, cwd=workspace_root)
    if result.returncode != 0:
        print("Black formatting failed")
        return result.returncode

    # Run isort using explicit pyproject.toml config
    print("Running isort (using pyproject.toml)...")
    isort_cmd = [
        sys.executable,
        "-m",
        "isort",
        "--settings-path",
        str(pyproject_path),
    ] + [str(f) for f in py_files]
    result = subprocess.run(isort_cmd, cwd=workspace_root)
    if result.returncode != 0:
        print("Isort formatting failed")
        return result.returncode

    return 0


def format_typescript(workspace_root: Path) -> int:
    """Format TypeScript files with prettier."""
    cdk_dir: Path = workspace_root / "cdk"
    ts_files: List[Path] = [f for f in cdk_dir.glob("**/*.ts") if not f.is_symlink()]

    if not ts_files:
        print("No TypeScript files found to format")
        return 0

    # Change to CDK directory
    original_cwd = os.getcwd()
    os.chdir(str(cdk_dir))

    try:
        # Run prettier
        print("Running prettier...")
        prettier_cmd = ["npx", "prettier", "--write"] + [
            str(f.relative_to(cdk_dir)) for f in ts_files
        ]
        result = subprocess.run(prettier_cmd)
        if result.returncode != 0:
            print("Prettier formatting failed")
            return result.returncode

        # Run ESLint fix
        print("Running ESLint fix...")
        eslint_cmd = ["npx", "eslint", "--fix", "--ext", ".ts"] + [
            str(f.relative_to(cdk_dir)) for f in ts_files
        ]
        result = subprocess.run(eslint_cmd)
        if result.returncode != 0:
            print("ESLint fix failed")
            return result.returncode

    finally:
        os.chdir(original_cwd)

    return 0


def main():
    """Run all formatters."""
    workspace_root = Path(__file__).parent.parent.parent

    # Format Python files
    result = format_python(workspace_root)
    if result != 0:
        return result

    # Format TypeScript files
    result = format_typescript(workspace_root)
    if result != 0:
        return result

    print("All formatting completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())

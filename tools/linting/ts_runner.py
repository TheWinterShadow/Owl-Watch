#!/usr/bin/env python3
"""TypeScript linting runner script."""

import os
import subprocess
import sys
from pathlib import Path


def main():
    """Run TypeScript linting tools."""
    if len(sys.argv) < 2:
        print("Usage: ts_runner.py <tool> [args...]", file=sys.stderr)
        sys.exit(1)

    tool = sys.argv[1]
    args = sys.argv[2:]

    # Get workspace root
    workspace_root = Path(__file__).parent.parent.parent
    cdk_dir = workspace_root / "cdk"

    # Change to CDK directory for TypeScript tools
    os.chdir(cdk_dir)

    if tool == "eslint":
        cmd = ["npx", "eslint"] + args
    elif tool == "prettier":
        cmd = ["npx", "prettier"] + args
    elif tool == "tsc":
        cmd = ["npx", "tsc"] + args
    else:
        print(f"Unsupported TypeScript tool: {tool}", file=sys.stderr)
        sys.exit(1)

    try:
        result = subprocess.run(cmd, check=False)
        sys.exit(result.returncode)
    except FileNotFoundError:
        print(
            f"Tool '{tool}' not found. Make sure npm dependencies are installed.",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()

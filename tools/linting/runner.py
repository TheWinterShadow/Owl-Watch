#!/usr/bin/env python3
"""Simple runner script for linting tools."""

import subprocess
import sys

if __name__ == "__main__":
    # Get the tool name and arguments
    if len(sys.argv) < 2:
        print("Usage: runner.py <tool> [args...]", file=sys.stderr)
        sys.exit(1)

    tool = sys.argv[1]
    args = sys.argv[2:]

    # Execute the tool directly
    try:
        result = subprocess.run([tool] + args, check=False)
        sys.exit(result.returncode)
    except FileNotFoundError:
        print(f"Tool '{tool}' not found", file=sys.stderr)
        sys.exit(1)

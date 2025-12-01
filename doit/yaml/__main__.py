"""CLI entry point for doit.yaml module.

Usage:
    python -m doit.yaml [options] [yaml_file]

Example:
    python -m doit.yaml build.yaml
    python -m doit.yaml --verbose --max-tasks 5000
    python -m doit.yaml --dry-run doit.yaml
"""

from .runner import main
import sys

if __name__ == '__main__':
    sys.exit(main())

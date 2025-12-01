"""YAML-based task definition for doit.

This module provides a declarative YAML format for defining TaskGenerators
with shell command actions, turning doit into a make-like tool.

Example doit.yaml:
    config:
      max_tasks: 10000
      base_path: .

    generators:
      - name: "compile:<module>"
        inputs:
          source: "src/<module>.c"
        outputs:
          - "build/<module>.o"
        action: "gcc -c {source} -o {out_0}"

Usage:
    from doit.yaml import run_yaml
    result = run_yaml('doit.yaml')

CLI:
    python -m doit.yaml doit.yaml
"""

from .parser import parse_yaml_file, YAMLConfig
from .converter import yaml_to_generator, yaml_to_generators
from .action import ShellAction
from .runner import run_yaml, main

__all__ = [
    'parse_yaml_file',
    'YAMLConfig',
    'yaml_to_generator',
    'yaml_to_generators',
    'ShellAction',
    'run_yaml',
    'main',
]

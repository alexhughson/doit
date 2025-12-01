"""YAML task runner with ReactiveEngine integration.

This module provides the main entry point for running tasks from YAML files.
"""

import sys
from pathlib import Path
from typing import List, Optional, Union

from .parser import parse_yaml_file, YAMLConfig
from .converter import yaml_to_generators


def run_yaml(
    yaml_path: Union[str, Path],
    max_tasks: int = 10000,
    base_path: Optional[Union[str, Path]] = None,
    verbose: bool = False,
) -> 'ReactiveResult':
    """Load and run tasks from a YAML file.

    This function parses a doit.yaml file, converts the generators,
    and runs them using the ReactiveEngine for fixed-point iteration.

    Args:
        yaml_path: Path to the YAML file
        max_tasks: Maximum number of tasks to execute (safety limit)
        base_path: Override base path from config
        verbose: Print progress information

    Returns:
        ReactiveResult with execution statistics

    Example:
        result = run_yaml('doit.yaml')
        if result.converged:
            print(f"Completed {result.tasks_executed} tasks")
    """
    from doit.reactive import ReactiveEngine

    # Parse YAML
    yaml_path = Path(yaml_path)
    config = parse_yaml_file(yaml_path)

    # Determine base path
    if base_path is None:
        base_path = config.config.get('base_path', yaml_path.parent)
    base_path = Path(base_path).resolve()

    # Get max_tasks from config if not overridden
    if max_tasks == 10000:  # default value
        max_tasks = config.config.get('max_tasks', max_tasks)

    # Convert to generators
    generators = yaml_to_generators(config, base_path)

    if verbose:
        print(f"Loaded {len(generators)} generator(s) from {yaml_path}")
        for gen in generators:
            print(f"  - {gen.name}")

    # Run with ReactiveEngine
    engine = ReactiveEngine(
        generators=generators,
        max_tasks=max_tasks,
    )

    if verbose:
        print(f"Starting execution (max_tasks={max_tasks})...")

    result = engine.run()

    if verbose:
        if result.converged:
            print(f"Converged after {result.tasks_executed} tasks")
        else:
            print(f"Hit limit at {result.tasks_executed} tasks")

    return result


def main(args: Optional[List[str]] = None) -> int:
    """CLI entry point for running YAML tasks.

    Usage:
        python -m doit.yaml [options] [yaml_file]

    Args:
        args: Command line arguments (defaults to sys.argv[1:])

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    import argparse

    parser = argparse.ArgumentParser(
        description='Run doit tasks from a YAML file',
        prog='python -m doit.yaml',
    )
    parser.add_argument(
        'yaml_file',
        nargs='?',
        default='doit.yaml',
        help='Path to the YAML file (default: doit.yaml)',
    )
    parser.add_argument(
        '--max-tasks',
        type=int,
        default=10000,
        help='Maximum number of tasks to execute (default: 10000)',
    )
    parser.add_argument(
        '--base-path',
        type=str,
        default=None,
        help='Override base path for file patterns',
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Print progress information',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Parse and show generators without executing',
    )

    parsed = parser.parse_args(args)

    try:
        yaml_path = Path(parsed.yaml_file)

        if parsed.dry_run:
            # Dry run: just parse and show generators
            config = parse_yaml_file(yaml_path)
            base_path = parsed.base_path or config.config.get(
                'base_path', yaml_path.parent
            )
            generators = yaml_to_generators(config, base_path)

            print(f"Parsed {yaml_path}:")
            print(f"  Config: {config.config}")
            print(f"  Generators ({len(generators)}):")
            for gen in generators:
                print(f"    - {gen.name}")
                print(f"      Inputs: {list(gen.inputs.keys())}")
                print(f"      Outputs: {len(gen.outputs)} output(s)")

            # Try to generate tasks
            print("\n  Generated tasks:")
            total_tasks = 0
            for gen in generators:
                tasks = list(gen.generate())
                total_tasks += len(tasks)
                for task in tasks:
                    print(f"    - {task.name}")
            print(f"\n  Total: {total_tasks} task(s)")

            return 0

        result = run_yaml(
            yaml_path,
            max_tasks=parsed.max_tasks,
            base_path=parsed.base_path,
            verbose=parsed.verbose,
        )

        if result.converged:
            print(f"Completed {result.tasks_executed} tasks")
            return 0
        else:
            print(
                f"Warning: Hit task limit at {result.tasks_executed} tasks. "
                f"Consider increasing --max-tasks or reviewing generators.",
                file=sys.stderr,
            )
            return 1

    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if parsed.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())

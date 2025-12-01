"""Shell action with variable injection for YAML-defined tasks.

This module provides the ShellAction class that executes shell commands
with automatic variable injection from inputs, outputs, and attributes.
"""

import os
import sys
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from doit.taskgen.groups import InputSet


@dataclass
class ShellAction:
    """Shell command action with variable injection.

    Variables are injected in TWO ways:
    1. Format string substitution: {source}, {arch}, {out_0}
    2. Environment variables: source=/path/to/file, arch=x86, out_0=/path/to/output

    Example:
        template = "gcc -c {source} -I include/{arch} -o {out_0}"

        With source=/path/main.c, arch=x86, out_0=/build/main.o:
        - Command: gcc -c /path/main.c -I include/x86 -o /build/main.o
        - Env: source=/path/main.c, arch=x86, out_0=/build/main.o
    """

    template: str
    input_set: 'InputSet'
    output_paths: List[str]
    attrs: Dict[str, str]

    def _build_substitutions(self) -> Dict[str, str]:
        """Build the substitution dictionary for format strings and env vars.

        Returns:
            Dictionary mapping variable names to their values
        """
        subs: Dict[str, str] = {}

        # Add captured attributes: {module}, {arch}, etc.
        subs.update(self.attrs)

        # Add input paths by label
        for label, item in self.input_set.items.items():
            if item is None:
                continue

            if isinstance(item, list):
                # List input: space-separated paths
                paths = []
                for dep in item:
                    if hasattr(dep, 'get_key'):
                        paths.append(dep.get_key())
                    elif hasattr(dep, 'path'):
                        paths.append(str(dep.path))
                    else:
                        paths.append(str(dep))
                subs[label] = " ".join(paths)
            else:
                # Single input
                if hasattr(item, 'get_key'):
                    subs[label] = item.get_key()
                elif hasattr(item, 'path'):
                    subs[label] = str(item.path)
                else:
                    subs[label] = str(item)

        # Add output paths by index: {out_0}, {out_1}, etc.
        for i, path in enumerate(self.output_paths):
            subs[f'out_{i}'] = path

        return subs

    def _format_command(self, subs: Dict[str, str]) -> str:
        """Format the command template with substitutions.

        Args:
            subs: Substitution dictionary

        Returns:
            Formatted command string
        """
        try:
            return self.template.format(**subs)
        except KeyError as e:
            # Provide helpful error message
            available = ', '.join(sorted(subs.keys()))
            raise KeyError(
                f"Unknown variable {e} in action template. "
                f"Available variables: {available}"
            )

    def _build_environment(self, subs: Dict[str, str]) -> Dict[str, str]:
        """Build environment dictionary with injected variables.

        Args:
            subs: Substitution dictionary

        Returns:
            Environment dictionary (copy of os.environ with additions)
        """
        env = os.environ.copy()
        for key, value in subs.items():
            env[key] = str(value)
        return env

    def __call__(self) -> bool:
        """Execute the shell command.

        This is called by doit when the task runs.

        Returns:
            True on success

        Raises:
            subprocess.CalledProcessError: If the command fails
        """
        subs = self._build_substitutions()
        cmd = self._format_command(subs)
        env = self._build_environment(subs)

        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            # Print stderr for debugging
            if result.stderr:
                print(result.stderr, file=sys.stderr)
            raise subprocess.CalledProcessError(
                result.returncode,
                cmd,
                result.stdout,
                result.stderr,
            )

        return True

    def __repr__(self) -> str:
        return f"ShellAction({self.template!r})"


class ShellActionResult:
    """Result of a shell action execution.

    Wraps subprocess result for inspection.
    """

    def __init__(
        self,
        command: str,
        returncode: int,
        stdout: str,
        stderr: str,
    ):
        self.command = command
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    @property
    def success(self) -> bool:
        return self.returncode == 0

    def __bool__(self) -> bool:
        return self.success

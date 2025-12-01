"""Output pattern index for efficient generator lookup.

When a task creates outputs, we need to quickly find which generators
might produce new tasks based on those outputs. This module provides
an efficient prefix-based index for that lookup.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from doit.taskgen import TaskGenerator


@dataclass
class OutputPatternIndex:
    """Index for fast output-to-generator pattern matching.

    Maps output path prefixes to generators whose input patterns
    could potentially match files under those prefixes.

    Example:
        If a generator has input pattern "processed/<doc>.json",
        we extract the static prefix "processed/" and register it.
        When an output like "processed/report.json" is created,
        we can quickly find this generator as potentially affected.
    """

    _prefix_to_generators: Dict[str, List['TaskGenerator']] = field(
        default_factory=dict
    )
    _generators: List['TaskGenerator'] = field(default_factory=list)

    def register_generator(self, generator: 'TaskGenerator') -> None:
        """Register a generator's input patterns for lookup.

        Extracts static prefixes from all input patterns and maps
        them to this generator. Computes absolute prefixes when
        the input has a base_path.
        """
        self._generators.append(generator)

        for label, inp in generator.inputs.items():
            prefix = self._compute_absolute_prefix(inp)
            if prefix not in self._prefix_to_generators:
                self._prefix_to_generators[prefix] = []
            if generator not in self._prefix_to_generators[prefix]:
                self._prefix_to_generators[prefix].append(generator)

    def register_generators(self, generators: List['TaskGenerator']) -> None:
        """Register multiple generators."""
        for gen in generators:
            self.register_generator(gen)

    def find_affected_generators(
        self, outputs: List[str]
    ) -> List['TaskGenerator']:
        """Find generators whose input patterns could match these outputs.

        Returns the list of generators that should be regenerated because
        their input patterns might now match new files.

        Args:
            outputs: List of output paths/keys that were created

        Returns:
            List of TaskGenerators that might produce new tasks (no duplicates)
        """
        affected: List['TaskGenerator'] = []
        seen_ids: set = set()

        for output_path in outputs:
            # Normalize the output path
            normalized = self._normalize_path(output_path)

            # Check each registered prefix
            for prefix, generators in self._prefix_to_generators.items():
                normalized_prefix = prefix.rstrip('/')
                # If output starts with prefix, or prefix starts with output
                # (for directory outputs that contain the prefix)
                if (normalized.startswith(normalized_prefix) or
                        normalized_prefix.startswith(normalized)):
                    for gen in generators:
                        if id(gen) not in seen_ids:
                            seen_ids.add(id(gen))
                            affected.append(gen)

        return affected

    def get_all_generators(self) -> List['TaskGenerator']:
        """Return all registered generators."""
        return list(self._generators)

    def _compute_absolute_prefix(self, inp) -> str:
        """Compute the absolute prefix for an input pattern.

        Uses the input's base_path (if available) to make the prefix absolute.
        This ensures proper matching with task outputs which use absolute paths.

        Args:
            inp: An Input instance with pattern and optionally base_path

        Returns:
            Absolute prefix path
        """
        relative_prefix = self._extract_static_prefix(inp.pattern)

        # If input has a base_path, make the prefix absolute
        base_path = getattr(inp, 'base_path', None)
        if base_path is not None and isinstance(base_path, Path):
            if relative_prefix:
                abs_path = base_path / relative_prefix
            else:
                abs_path = base_path
            return str(abs_path.resolve()) + '/'

        # For S3 or other inputs without base_path, use the relative prefix
        return relative_prefix

    def _extract_static_prefix(self, pattern: str) -> str:
        """Extract the static prefix before any <capture> placeholders.

        Examples:
            "processed/<doc>/<file>.json" -> "processed/"
            "raw/<dataset>.csv" -> "raw/"
            "<name>.txt" -> ""
            "data/fixed/file.txt" -> "data/fixed/"
        """
        # Find the first < character
        bracket_pos = pattern.find('<')
        if bracket_pos == -1:
            # No placeholders - the whole pattern is static
            # Return the directory portion
            last_slash = pattern.rfind('/')
            if last_slash == -1:
                return ""
            return pattern[:last_slash + 1]

        # Get everything before the first placeholder
        prefix = pattern[:bracket_pos]

        # Trim to the last directory separator
        last_slash = prefix.rfind('/')
        if last_slash == -1:
            return ""

        return prefix[:last_slash + 1]

    def _normalize_path(self, path: str) -> str:
        """Normalize a path for comparison.

        Removes trailing slashes and handles S3 URIs.
        """
        # Handle S3 URIs
        if path.startswith('s3://'):
            # Keep the s3:// prefix but normalize the rest
            path = path[5:]  # Remove s3://
            parts = path.split('/', 1)
            if len(parts) == 2:
                bucket, key = parts
                return f"s3://{bucket}/{key.rstrip('/')}"
            return f"s3://{path.rstrip('/')}"

        return path.rstrip('/')

    def clear(self) -> None:
        """Clear all registered generators."""
        self._prefix_to_generators.clear()
        self._generators.clear()

    @property
    def prefix_count(self) -> int:
        """Return the number of registered prefixes."""
        return len(self._prefix_to_generators)

    @property
    def generator_count(self) -> int:
        """Return the number of registered generators."""
        return len(self._generators)

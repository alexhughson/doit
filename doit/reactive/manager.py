"""Generator manager for holding and regenerating TaskGenerators.

This module coordinates the regeneration of task generators based on
newly created outputs. It uses the OutputPatternIndex to efficiently
determine which generators might produce new tasks.
"""

from dataclasses import dataclass, field
from typing import List, TYPE_CHECKING

from .index import OutputPatternIndex

if TYPE_CHECKING:
    from doit.task import Task
    from doit.taskgen import TaskGenerator


@dataclass
class GeneratorManager:
    """Manages TaskGenerators and their regeneration.

    Holds all generators, maintains an index for efficient lookup,
    and handles regeneration when outputs are created.
    """

    generators: List['TaskGenerator'] = field(default_factory=list)
    """List of all registered generators."""

    output_index: OutputPatternIndex = field(default_factory=OutputPatternIndex)
    """Index for finding affected generators by output path."""

    _initialized: bool = field(default=False, repr=False)
    """Whether the index has been built."""

    def __post_init__(self):
        """Build the output index from generators."""
        if self.generators and not self._initialized:
            self._build_index()

    def _build_index(self) -> None:
        """Build the output pattern index from all generators."""
        self.output_index.clear()
        self.output_index.register_generators(self.generators)
        self._initialized = True

    def add_generator(self, generator: 'TaskGenerator') -> None:
        """Add a generator and update the index."""
        self.generators.append(generator)
        self.output_index.register_generator(generator)
        self._initialized = True

    def add_generators(self, generators: List['TaskGenerator']) -> None:
        """Add multiple generators."""
        for gen in generators:
            self.add_generator(gen)

    def regenerate_all(self) -> List['Task']:
        """Regenerate all generators and return all tasks.

        Used for initial generation before any tasks have run.

        Returns:
            List of all Task objects from all generators
        """
        if not self._initialized:
            self._build_index()

        tasks: List['Task'] = []
        for gen in self.generators:
            tasks.extend(gen.generate())
        return tasks

    def regenerate_affected(self, new_outputs: List[str]) -> List['Task']:
        """Regenerate only generators whose inputs might match new outputs.

        This is the efficient path used after each task completes.
        Only generators with input patterns that could match the new
        outputs are regenerated.

        Args:
            new_outputs: List of output paths/keys created by a task

        Returns:
            List of Task objects from affected generators
        """
        if not self._initialized:
            self._build_index()

        if not new_outputs:
            return []

        affected = self.output_index.find_affected_generators(new_outputs)

        tasks: List['Task'] = []
        for gen in affected:
            tasks.extend(gen.generate())
        return tasks

    def find_affected_generators(
        self, new_outputs: List[str]
    ) -> List['TaskGenerator']:
        """Find generators that might produce tasks for these outputs.

        Useful for debugging or introspection.
        """
        if not self._initialized:
            self._build_index()

        return self.output_index.find_affected_generators(new_outputs)

    def clear(self) -> None:
        """Clear all generators and the index."""
        self.generators.clear()
        self.output_index.clear()
        self._initialized = False

    @property
    def generator_count(self) -> int:
        """Return the number of generators."""
        return len(self.generators)

    @property
    def prefix_count(self) -> int:
        """Return the number of indexed prefixes."""
        return self.output_index.prefix_count

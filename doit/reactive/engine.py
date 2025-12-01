"""Reactive engine for streaming task execution with fixed-point iteration.

This is the main entry point for reactive task generation. It coordinates:
1. Initial generation from all TaskGenerators
2. Streaming execution of tasks
3. Immediate regeneration after each task completes
4. Injection of new tasks into the running execution

The streaming model (tup-like) ensures that:
- Tasks become ready from TWO sources: generation AND dependency completion
- After each task completes, affected generators are immediately regenerated
- New tasks go into the same ready queue
- Early-stage work is processed before unrelated later-stage tasks
"""

from dataclasses import dataclass, field
from typing import List, Optional, Iterator, TYPE_CHECKING

from .manager import GeneratorManager
from .merger import TaskMerger, MergeResult
from .index import OutputPatternIndex

if TYPE_CHECKING:
    from doit.task import Task
    from doit.taskgen import TaskGenerator
    from doit.dependency import ProcessingStateStore


@dataclass
class ReactiveResult:
    """Result of running the ReactiveEngine."""

    tasks_executed: int = 0
    """Number of tasks that were actually executed."""

    total_tasks: int = 0
    """Total number of tasks that were generated."""

    hit_limit: bool = False
    """Whether the task limit was reached (potential infinite loop)."""

    regenerations: int = 0
    """Number of times generators were regenerated."""

    @property
    def converged(self) -> bool:
        """Return True if fixed-point was reached (not hit limit)."""
        return not self.hit_limit


@dataclass
class ReactiveEngine:
    """Streaming reactive task execution engine (tup-like).

    Unlike wave-based approaches, this regenerates immediately after
    each task completes. New tasks go into the ready queue and are
    processed in proper dependency order.

    Example:
        from doit.reactive import ReactiveEngine
        from doit.taskgen import TaskGenerator, FileInput, FileOutput

        gen = TaskGenerator(
            name="process:<doc>",
            inputs={"raw": FileInput("raw/<doc>.txt")},
            outputs=[FileOutput("processed/<doc>.json")],
            action=lambda inp, out, attrs: process_document(inp, out),
        )

        engine = ReactiveEngine(generators=[gen])
        result = engine.run()

        if result.converged:
            print(f"Completed {result.tasks_executed} tasks")
        else:
            print(f"Hit limit at {result.tasks_executed} tasks")
    """

    generators: List['TaskGenerator'] = field(default_factory=list)
    """List of TaskGenerators to use for task creation."""

    max_tasks: int = 10000
    """Safety limit on total tasks to prevent infinite loops."""

    store: Optional['ProcessingStateStore'] = None
    """State store for task dependency tracking."""

    _manager: GeneratorManager = field(init=False, repr=False)
    _merger: TaskMerger = field(init=False, repr=False)
    _tasks_executed: int = field(default=0, init=False, repr=False)
    _regenerations: int = field(default=0, init=False, repr=False)

    def __post_init__(self):
        """Initialize internal components."""
        self._manager = GeneratorManager(generators=list(self.generators))
        self._merger = TaskMerger()
        self._tasks_executed = 0
        self._regenerations = 0

    def run(self) -> ReactiveResult:
        """Execute until no more tasks can be generated or run.

        This is the main entry point. It:
        1. Generates initial tasks from all generators
        2. Creates a streaming iterator for execution
        3. For each completed task, regenerates affected generators
        4. Injects new tasks into the running execution
        5. Continues until fixed-point or limit reached

        Returns:
            ReactiveResult with execution statistics
        """
        # Import here to avoid circular imports
        from doit.engine import DoitEngine

        # Initial generation
        initial_tasks = self._manager.regenerate_all()
        self._merger.merge(initial_tasks)
        self._regenerations += 1

        if not self._merger.total_tasks:
            # No tasks to run
            return ReactiveResult(
                tasks_executed=0,
                total_tasks=0,
                hit_limit=False,
                regenerations=1,
            )

        # Run with streaming regeneration
        hit_limit = False

        with DoitEngine(
            self._merger.get_all_tasks(),
            store=self.store
        ) as engine:
            for wrapper in engine:
                if self._tasks_executed >= self.max_tasks:
                    hit_limit = True
                    break

                if wrapper.should_run:
                    wrapper.execute_and_submit()
                    self._merger.mark_completed(wrapper.name)
                    self._tasks_executed += 1

                    # STREAMING: Immediately regenerate affected generators
                    new_tasks = self._regenerate_for_task(wrapper.task)

                    # Inject new/updated tasks into the running engine
                    if new_tasks:
                        for task in new_tasks:
                            engine.add_task(task)

        return ReactiveResult(
            tasks_executed=self._tasks_executed,
            total_tasks=self._merger.total_tasks,
            hit_limit=hit_limit,
            regenerations=self._regenerations,
        )

    def _regenerate_for_task(self, task: 'Task') -> List['Task']:
        """Regenerate generators based on task outputs.

        Called immediately after each task completes.

        Args:
            task: The task that just completed

        Returns:
            List of new/updated tasks to inject
        """
        # Get outputs from the task
        outputs = self._get_task_outputs(task)

        if not outputs:
            return []

        # Regenerate affected generators
        new_tasks = self._manager.regenerate_affected(outputs)
        self._regenerations += 1

        if not new_tasks:
            return []

        # Merge with existing tasks
        result = self._merger.merge(new_tasks)

        # Return tasks that need to be injected
        return result.all_new_tasks

    def _get_task_outputs(self, task: 'Task') -> List[str]:
        """Get output paths from a task.

        Extracts paths from both the new Target objects and
        legacy string targets.
        """
        outputs = []

        # New-style Target objects
        if hasattr(task, 'outputs'):
            for out in task.outputs:
                if hasattr(out, 'get_key'):
                    outputs.append(out.get_key())

        # Legacy string targets
        if hasattr(task, 'targets'):
            for target in task.targets:
                if isinstance(target, str):
                    outputs.append(target)

        return outputs

    def add_generator(self, generator: 'TaskGenerator') -> None:
        """Add a generator after initialization.

        Useful for dynamically adding generators during setup.
        """
        self.generators.append(generator)
        self._manager.add_generator(generator)

    def reset(self) -> None:
        """Reset the engine for a new run.

        Clears all state but keeps the generators.
        """
        self._merger.clear()
        self._tasks_executed = 0
        self._regenerations = 0

    @property
    def tasks_executed(self) -> int:
        """Return the number of tasks executed so far."""
        return self._tasks_executed

    @property
    def total_tasks(self) -> int:
        """Return the total number of tasks generated so far."""
        return self._merger.total_tasks

    @property
    def regenerations(self) -> int:
        """Return the number of regeneration cycles."""
        return self._regenerations

"""Task merger for diffing and merging newly generated tasks.

When generators produce new tasks, we need to:
1. Detect truly new tasks (not seen before)
2. Detect tasks whose inputs have changed (need re-run)
3. Avoid re-adding unchanged tasks

This module handles that diffing logic.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from doit.task import Task


@dataclass
class MergeResult:
    """Result of merging new tasks with existing task set."""

    added: List['Task'] = field(default_factory=list)
    """Completely new tasks (never seen before)."""

    updated: List['Task'] = field(default_factory=list)
    """Existing tasks with changed inputs (need re-run)."""

    unchanged: List[str] = field(default_factory=list)
    """Task names that didn't change."""

    @property
    def has_changes(self) -> bool:
        """Return True if there are any new or updated tasks."""
        return bool(self.added or self.updated)

    @property
    def all_new_tasks(self) -> List['Task']:
        """Return all tasks that need to be injected (added + updated)."""
        return self.added + self.updated


@dataclass
class TaskMerger:
    """Merge newly generated tasks with existing task set.

    Tracks which tasks exist, which have been completed, and detects
    when task inputs change (requiring re-execution).
    """

    existing_tasks: Dict[str, 'Task'] = field(default_factory=dict)
    """Map of task name to Task object."""

    completed_tasks: Set[str] = field(default_factory=set)
    """Set of task names that have completed execution."""

    _input_hashes: Dict[str, frozenset] = field(default_factory=dict)
    """Cache of input dependency keys for change detection."""

    def merge(self, new_tasks: List['Task']) -> MergeResult:
        """Merge new tasks, detecting additions and changes.

        Args:
            new_tasks: List of newly generated Task objects

        Returns:
            MergeResult with categorized tasks
        """
        result = MergeResult()

        for task in new_tasks:
            if task.name not in self.existing_tasks:
                # Completely new task
                self.existing_tasks[task.name] = task
                self._input_hashes[task.name] = self._get_input_hash(task)
                result.added.append(task)
            else:
                # Task already exists - check if inputs changed
                old_hash = self._input_hashes.get(task.name, frozenset())
                new_hash = self._get_input_hash(task)

                if old_hash != new_hash:
                    # Inputs changed - update and mark for re-run
                    self.existing_tasks[task.name] = task
                    self._input_hashes[task.name] = new_hash

                    if task.name in self.completed_tasks:
                        # Task already ran - needs to re-run
                        self._invalidate(task.name)

                    result.updated.append(task)
                else:
                    # No change
                    result.unchanged.append(task.name)

        return result

    def mark_completed(self, task_name: str) -> None:
        """Mark a task as completed."""
        self.completed_tasks.add(task_name)

    def is_completed(self, task_name: str) -> bool:
        """Check if a task has completed."""
        return task_name in self.completed_tasks

    def get_task(self, task_name: str) -> 'Task':
        """Get a task by name."""
        return self.existing_tasks.get(task_name)

    def get_all_tasks(self) -> List['Task']:
        """Get all tasks."""
        return list(self.existing_tasks.values())

    def _get_input_hash(self, task: 'Task') -> frozenset:
        """Get a hashable representation of task inputs.

        Used to detect when a task's dependencies have changed.
        """
        dep_keys = set()
        for dep in task.dependencies:
            if hasattr(dep, 'get_key'):
                dep_keys.add(dep.get_key())
            else:
                # Legacy string dependency
                dep_keys.add(str(dep))
        return frozenset(dep_keys)

    def _invalidate(self, task_name: str) -> None:
        """Mark a completed task for re-execution.

        Removes it from the completed set so it will be re-evaluated.
        """
        self.completed_tasks.discard(task_name)

    def clear(self) -> None:
        """Clear all state."""
        self.existing_tasks.clear()
        self.completed_tasks.clear()
        self._input_hashes.clear()

    @property
    def total_tasks(self) -> int:
        """Return total number of tasks."""
        return len(self.existing_tasks)

    @property
    def completed_count(self) -> int:
        """Return number of completed tasks."""
        return len(self.completed_tasks)

    @property
    def pending_count(self) -> int:
        """Return number of pending (not completed) tasks."""
        return self.total_tasks - self.completed_count

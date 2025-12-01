"""Tests for TaskMerger."""

import pytest
from unittest.mock import MagicMock

from doit.reactive.merger import TaskMerger, MergeResult


def make_mock_task(name: str, dep_keys: list = None):
    """Create a mock task with specified dependencies."""
    task = MagicMock()
    task.name = name

    if dep_keys is None:
        dep_keys = []

    deps = []
    for key in dep_keys:
        dep = MagicMock()
        dep.get_key.return_value = key
        deps.append(dep)

    task.dependencies = deps
    return task


class TestMergeResult:
    """Tests for MergeResult dataclass."""

    def test_has_changes_with_added(self):
        """Test has_changes with added tasks."""
        result = MergeResult(added=[MagicMock()])
        assert result.has_changes is True

    def test_has_changes_with_updated(self):
        """Test has_changes with updated tasks."""
        result = MergeResult(updated=[MagicMock()])
        assert result.has_changes is True

    def test_has_changes_empty(self):
        """Test has_changes with no changes."""
        result = MergeResult()
        assert result.has_changes is False

    def test_all_new_tasks(self):
        """Test all_new_tasks combines added and updated."""
        task1 = MagicMock()
        task2 = MagicMock()
        result = MergeResult(added=[task1], updated=[task2])

        all_tasks = result.all_new_tasks
        assert task1 in all_tasks
        assert task2 in all_tasks
        assert len(all_tasks) == 2


class TestTaskMergerBasics:
    """Basic tests for TaskMerger."""

    def test_empty_merge(self):
        """Test merging empty list."""
        merger = TaskMerger()
        result = merger.merge([])

        assert len(result.added) == 0
        assert len(result.updated) == 0
        assert merger.total_tasks == 0

    def test_merge_new_task(self):
        """Test merging a completely new task."""
        merger = TaskMerger()
        task = make_mock_task("task1", ["dep1"])

        result = merger.merge([task])

        assert len(result.added) == 1
        assert result.added[0] is task
        assert merger.total_tasks == 1

    def test_merge_multiple_new_tasks(self):
        """Test merging multiple new tasks."""
        merger = TaskMerger()
        task1 = make_mock_task("task1", ["dep1"])
        task2 = make_mock_task("task2", ["dep2"])

        result = merger.merge([task1, task2])

        assert len(result.added) == 2
        assert merger.total_tasks == 2


class TestTaskMergerDuplicates:
    """Tests for handling duplicate tasks."""

    def test_merge_same_task_unchanged(self):
        """Test merging the same task with no changes."""
        merger = TaskMerger()
        task1 = make_mock_task("task1", ["dep1"])
        task2 = make_mock_task("task1", ["dep1"])  # Same name, same deps

        result1 = merger.merge([task1])
        result2 = merger.merge([task2])

        assert len(result1.added) == 1
        assert len(result2.added) == 0
        assert len(result2.unchanged) == 1
        assert merger.total_tasks == 1

    def test_merge_task_with_changed_inputs(self):
        """Test merging task with changed dependencies."""
        merger = TaskMerger()
        task1 = make_mock_task("task1", ["dep1"])
        task2 = make_mock_task("task1", ["dep1", "dep2"])  # Added a dep

        merger.merge([task1])
        result = merger.merge([task2])

        assert len(result.updated) == 1
        assert result.updated[0] is task2
        assert merger.total_tasks == 1

    def test_merge_task_with_removed_inputs(self):
        """Test merging task with removed dependency."""
        merger = TaskMerger()
        task1 = make_mock_task("task1", ["dep1", "dep2"])
        task2 = make_mock_task("task1", ["dep1"])  # Removed dep2

        merger.merge([task1])
        result = merger.merge([task2])

        assert len(result.updated) == 1


class TestTaskMergerCompletedTasks:
    """Tests for completed task handling."""

    def test_mark_completed(self):
        """Test marking a task as completed."""
        merger = TaskMerger()
        task = make_mock_task("task1", ["dep1"])
        merger.merge([task])

        assert not merger.is_completed("task1")

        merger.mark_completed("task1")

        assert merger.is_completed("task1")
        assert merger.completed_count == 1

    def test_updated_completed_task_invalidated(self):
        """Test that updating a completed task invalidates it."""
        merger = TaskMerger()
        task1 = make_mock_task("task1", ["dep1"])
        merger.merge([task1])
        merger.mark_completed("task1")

        assert merger.is_completed("task1")

        # Now merge with changed deps
        task2 = make_mock_task("task1", ["dep1", "dep2"])
        result = merger.merge([task2])

        assert len(result.updated) == 1
        assert not merger.is_completed("task1")  # Should be invalidated

    def test_updated_not_completed_task(self):
        """Test updating a task that hasn't completed."""
        merger = TaskMerger()
        task1 = make_mock_task("task1", ["dep1"])
        merger.merge([task1])
        # Don't mark as completed

        task2 = make_mock_task("task1", ["dep1", "dep2"])
        result = merger.merge([task2])

        assert len(result.updated) == 1
        assert not merger.is_completed("task1")  # Still not completed


class TestTaskMergerStats:
    """Tests for merger statistics."""

    def test_total_tasks(self):
        """Test total_tasks count."""
        merger = TaskMerger()
        merger.merge([
            make_mock_task("task1", ["dep1"]),
            make_mock_task("task2", ["dep2"]),
            make_mock_task("task3", ["dep3"]),
        ])

        assert merger.total_tasks == 3

    def test_pending_count(self):
        """Test pending_count calculation."""
        merger = TaskMerger()
        merger.merge([
            make_mock_task("task1", ["dep1"]),
            make_mock_task("task2", ["dep2"]),
            make_mock_task("task3", ["dep3"]),
        ])
        merger.mark_completed("task1")

        assert merger.completed_count == 1
        assert merger.pending_count == 2

    def test_clear(self):
        """Test clearing all state."""
        merger = TaskMerger()
        merger.merge([make_mock_task("task1", ["dep1"])])
        merger.mark_completed("task1")

        merger.clear()

        assert merger.total_tasks == 0
        assert merger.completed_count == 0


class TestTaskMergerGetTask:
    """Tests for getting tasks by name."""

    def test_get_task(self):
        """Test getting a task by name."""
        merger = TaskMerger()
        task = make_mock_task("task1", ["dep1"])
        merger.merge([task])

        retrieved = merger.get_task("task1")
        assert retrieved is task

    def test_get_missing_task(self):
        """Test getting a non-existent task."""
        merger = TaskMerger()
        retrieved = merger.get_task("nonexistent")
        assert retrieved is None

    def test_get_all_tasks(self):
        """Test getting all tasks."""
        merger = TaskMerger()
        task1 = make_mock_task("task1", ["dep1"])
        task2 = make_mock_task("task2", ["dep2"])
        merger.merge([task1, task2])

        all_tasks = merger.get_all_tasks()
        assert len(all_tasks) == 2
        assert task1 in all_tasks
        assert task2 in all_tasks

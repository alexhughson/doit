"""Tests for doit.deps module - Dependency classes."""

import os
import time
import pytest
from pathlib import Path

from doit.deps import (
    Dependency, FileDependency, TaskDependency,
    Target, FileTarget
)
from doit.task import Task
from doit.dependency import (
    Dependency as DepManager, InMemoryStateStore, MD5Checker,
    TimestampChecker, TaskState, UpToDateChecker
)


class TestFileDependency:
    """Tests for FileDependency class."""

    def test_get_key_returns_absolute_path(self, tmp_path):
        """get_key() returns absolute path."""
        f = tmp_path / "test.txt"
        f.write_text("hello")

        dep = FileDependency(str(f))
        assert dep.get_key() == str(f.absolute())

    def test_get_key_relative_path_becomes_absolute(self):
        """get_key() converts relative paths to absolute."""
        dep = FileDependency("relative/path.txt")
        key = dep.get_key()
        assert os.path.isabs(key)
        assert key.endswith("relative/path.txt")

    def test_exists_true_when_file_exists(self, tmp_path):
        """exists() returns True when file exists."""
        f = tmp_path / "exists.txt"
        f.write_text("content")

        dep = FileDependency(str(f))
        assert dep.exists() is True

    def test_exists_false_when_file_missing(self, tmp_path):
        """exists() returns False when file doesn't exist."""
        dep = FileDependency(str(tmp_path / "missing.txt"))
        assert dep.exists() is False

    def test_is_modified_true_when_no_stored_state(self, tmp_path):
        """is_modified() returns True when stored_state is None."""
        f = tmp_path / "new.txt"
        f.write_text("content")

        dep = FileDependency(str(f))
        assert dep.is_modified(None) is True

    def test_is_modified_false_when_timestamp_unchanged(self, tmp_path):
        """is_modified() returns False when timestamp matches (fast path)."""
        f = tmp_path / "unchanged.txt"
        f.write_text("content")

        dep = FileDependency(str(f))
        state = dep.get_state(None)

        # Same state should not be modified
        assert dep.is_modified(state) is False

    def test_is_modified_true_when_content_changes(self, tmp_path):
        """is_modified() detects content changes."""
        f = tmp_path / "changing.txt"
        f.write_text("original")

        dep = FileDependency(str(f))
        state = dep.get_state(None)

        # Change the file
        time.sleep(0.01)  # Ensure timestamp differs
        f.write_text("modified")

        assert dep.is_modified(state) is True

    def test_is_modified_with_timestamp_checker(self, tmp_path):
        """is_modified() works with timestamp checker."""
        f = tmp_path / "ts.txt"
        f.write_text("content")

        dep = FileDependency(str(f), checker="timestamp")
        state = dep.get_state(None)

        assert dep.is_modified(state) is False

        # Change the file
        time.sleep(0.01)
        f.write_text("changed")

        assert dep.is_modified(state) is True

    def test_get_state_md5_returns_tuple(self, tmp_path):
        """get_state() returns (timestamp, size, md5) for md5 checker."""
        f = tmp_path / "md5.txt"
        f.write_text("content")

        dep = FileDependency(str(f))
        state = dep.get_state(None)

        assert isinstance(state, tuple)
        assert len(state) == 3
        timestamp, size, md5 = state
        assert isinstance(timestamp, float)
        assert size == len("content")
        assert isinstance(md5, str)
        assert len(md5) == 32  # MD5 hex digest

    def test_get_state_timestamp_returns_float(self, tmp_path):
        """get_state() returns mtime float for timestamp checker."""
        f = tmp_path / "ts.txt"
        f.write_text("content")

        dep = FileDependency(str(f), checker="timestamp")
        state = dep.get_state(None)

        assert isinstance(state, float)

    def test_get_state_returns_none_when_unchanged(self, tmp_path):
        """get_state() returns None when timestamp unchanged (optimization)."""
        f = tmp_path / "opt.txt"
        f.write_text("content")

        dep = FileDependency(str(f))
        state1 = dep.get_state(None)

        # Same file, same timestamp -> should return None
        state2 = dep.get_state(state1)
        assert state2 is None

    def test_creates_task_dep_returns_none(self, tmp_path):
        """FileDependency.creates_task_dep() returns None."""
        f = tmp_path / "test.txt"
        f.write_text("x")
        dep = FileDependency(str(f))
        assert dep.creates_task_dep() is None


class TestTaskDependency:
    """Tests for TaskDependency class."""

    def test_get_key_returns_prefixed_name(self):
        """get_key() returns 'task:name'."""
        dep = TaskDependency("build")
        assert dep.get_key() == "task:build"

    def test_get_key_subtask(self):
        """get_key() handles subtask names."""
        dep = TaskDependency("build:lib")
        assert dep.get_key() == "task:build:lib"

    def test_exists_always_true(self):
        """exists() always returns True (validation happens elsewhere)."""
        dep = TaskDependency("nonexistent")
        assert dep.exists() is True

    def test_is_modified_always_false(self):
        """is_modified() always returns False (task deps don't affect uptodate)."""
        dep = TaskDependency("task1")
        assert dep.is_modified(None) is False
        assert dep.is_modified({"some": "state"}) is False

    def test_get_state_returns_none(self):
        """get_state() always returns None (no state to save)."""
        dep = TaskDependency("task1")
        assert dep.get_state(None) is None
        assert dep.get_state({"previous": "state"}) is None

    def test_creates_task_dep_returns_task_name(self):
        """creates_task_dep() returns the task name."""
        dep = TaskDependency("build")
        assert dep.creates_task_dep() == "build"


class TestTaskWithDependencies:
    """Tests for Task class with new dependencies parameter."""

    def test_task_accepts_dependency_objects(self, tmp_path):
        """Task accepts Dependency objects in dependencies param."""
        f = tmp_path / "src.txt"
        f.write_text("source")

        task = Task(
            "compile",
            actions=["echo compile"],
            dependencies=[
                FileDependency(str(f)),
                TaskDependency("setup"),
            ],
        )

        assert len(task.dependencies) == 2
        assert isinstance(task.dependencies[0], FileDependency)
        assert isinstance(task.dependencies[1], TaskDependency)

    def test_task_rejects_non_dependency_objects(self):
        """Task raises InvalidTask for non-Dependency objects."""
        from doit.exceptions import InvalidTask

        with pytest.raises(InvalidTask):
            Task(
                "bad",
                actions=["echo"],
                dependencies=["not_a_dependency_object"],
            )

    def test_task_accepts_empty_dependencies(self):
        """Task accepts empty dependencies list."""
        task = Task("empty", actions=["echo"])
        assert task.dependencies == []

    def test_task_file_dep_legacy_separate(self, tmp_path):
        """Legacy file_dep and new dependencies are separate."""
        f1 = tmp_path / "new.txt"
        f1.write_text("new")

        task = Task(
            "mixed",
            actions=["echo"],
            file_dep=["old.txt"],  # Legacy
            dependencies=[FileDependency(str(f1))],  # New
        )

        assert "old.txt" in task.file_dep
        assert len(task.dependencies) == 1


class TestUpToDateCheckerWithDependencies:
    """Tests for UpToDateChecker with new-style dependencies."""

    def test_file_dependency_checked(self, tmp_path):
        """UpToDateChecker checks FileDependency objects."""
        f = tmp_path / "dep.txt"
        f.write_text("content")

        task = Task(
            "test",
            actions=["echo"],
            dependencies=[FileDependency(str(f))],
            targets=[str(tmp_path / "out.txt")],
        )

        store = InMemoryStateStore()
        checker = UpToDateChecker(store, TimestampChecker())

        # First check - should need to run (no stored state)
        result = checker.check(task, {}, lambda x: {})
        assert result.status == 'run'

    def test_task_dependency_doesnt_affect_uptodate(self, tmp_path):
        """TaskDependency doesn't affect up-to-date status."""
        f = tmp_path / "source.txt"
        f.write_text("content")
        target = tmp_path / "target.txt"
        target.write_text("built")

        task = Task(
            "test",
            actions=["echo"],
            dependencies=[
                FileDependency(str(f)),
                TaskDependency("other_task"),  # Should not affect status
            ],
            targets=[str(target)],
        )

        store = InMemoryStateStore()
        state = TaskState(store, TimestampChecker())
        checker = UpToDateChecker(store, TimestampChecker())

        # Save state after "execution"
        state.save_success(task)

        # Check again - should be up-to-date
        result = checker.check(task, {}, lambda x: {})
        assert result.status == 'up-to-date'

    def test_missing_dependency_returns_error(self, tmp_path):
        """Missing dependency returns error status."""
        task = Task(
            "test",
            actions=["echo"],
            dependencies=[FileDependency(str(tmp_path / "missing.txt"))],
        )

        store = InMemoryStateStore()
        checker = UpToDateChecker(store, TimestampChecker())

        result = checker.check(task, {}, lambda x: {})
        assert result.status == 'error'


class TestTaskStateSaveWithDependencies:
    """Tests for TaskState.save_success with new-style dependencies."""

    def test_saves_file_dependency_state(self, tmp_path):
        """save_success() saves FileDependency states."""
        f = tmp_path / "dep.txt"
        f.write_text("content")

        task = Task(
            "test",
            actions=["echo"],
            dependencies=[FileDependency(str(f))],
        )

        store = InMemoryStateStore()
        state = TaskState(store, TimestampChecker())
        state.save_success(task)

        # Check that state was saved
        key = os.path.abspath(str(f))
        saved = store.get("test", key)
        assert saved is not None

    def test_task_dependency_no_state_saved(self, tmp_path):
        """save_success() doesn't save state for TaskDependency."""
        task = Task(
            "test",
            actions=["echo"],
            dependencies=[TaskDependency("other")],
        )

        store = InMemoryStateStore()
        state = TaskState(store, TimestampChecker())
        state.save_success(task)

        # TaskDependency key should not be in store
        assert store.get("test", "task:other") is None


class TestFileTarget:
    """Tests for FileTarget class."""

    def test_get_key_returns_absolute_path(self, tmp_path):
        """get_key() returns absolute path."""
        f = tmp_path / "output.txt"
        f.write_text("content")

        target = FileTarget(str(f))
        assert target.get_key() == str(f.absolute())

    def test_exists_true_when_file_exists(self, tmp_path):
        """exists() returns True when file exists."""
        f = tmp_path / "exists.txt"
        f.write_text("content")

        target = FileTarget(str(f))
        assert target.exists() is True

    def test_exists_false_when_file_missing(self, tmp_path):
        """exists() returns False when file doesn't exist."""
        target = FileTarget(str(tmp_path / "missing.txt"))
        assert target.exists() is False

    def test_matches_file_dependency(self, tmp_path):
        """FileTarget matches FileDependency with same path."""
        f = tmp_path / "file.txt"
        f.write_text("x")

        target = FileTarget(str(f))
        dep = FileDependency(str(f))

        assert target.matches_dependency(dep) is True

    def test_does_not_match_different_file(self, tmp_path):
        """FileTarget doesn't match FileDependency with different path."""
        target = FileTarget(str(tmp_path / "file1.txt"))
        dep = FileDependency(str(tmp_path / "file2.txt"))

        assert target.matches_dependency(dep) is False

    def test_does_not_match_task_dependency(self, tmp_path):
        """FileTarget doesn't match TaskDependency."""
        target = FileTarget(str(tmp_path / "file.txt"))
        dep = TaskDependency("some_task")

        assert target.matches_dependency(dep) is False


class TestIntegrationNewDependencies:
    """Integration tests for the new dependency system."""

    def test_full_cycle_with_file_dependency(self, tmp_path):
        """Full run/check cycle with FileDependency."""
        f = tmp_path / "input.txt"
        f.write_text("input")
        target = tmp_path / "output.txt"

        task = Task(
            "process",
            actions=["echo done"],
            dependencies=[FileDependency(str(f))],
            targets=[str(target)],
        )

        store = InMemoryStateStore()
        state = TaskState(store, TimestampChecker())
        checker = UpToDateChecker(store, TimestampChecker())

        # First run - needs to run
        result1 = checker.check(task, {}, lambda x: {})
        assert result1.status == 'run'

        # Simulate execution
        target.write_text("output")
        state.save_success(task)

        # Second check - up to date
        result2 = checker.check(task, {}, lambda x: {})
        assert result2.status == 'up-to-date'

        # Modify input
        time.sleep(0.01)
        f.write_text("modified input")

        # Third check - needs to run
        result3 = checker.check(task, {}, lambda x: {})
        assert result3.status == 'run'
        assert task.dep_changed  # Should have changed deps

"""Tests for doit.control type-safe classes.

Tests for:
- TaskRunStatus enum
- DispatcherSignal enum
- TaskRegistry
- TargetRegistry
- ExecNodeRegistry
- TaskSelector
"""

import pytest
from collections import OrderedDict

from doit.task import Task, DelayedLoader
from doit.control import (
    TaskRunStatus,
    DispatcherSignal,
    TaskRegistry,
    TargetRegistry,
    ExecNodeRegistry,
    TaskSelector,
    RegexGroup,
    ExecNode,
)
from doit.exceptions import InvalidCommand


class TestTaskRunStatus:
    """Tests for TaskRunStatus enum."""

    def test_enum_values(self):
        """Check all expected enum values exist."""
        assert TaskRunStatus.PENDING.value is None
        assert TaskRunStatus.RUN.value == 'run'
        assert TaskRunStatus.UPTODATE.value == 'up-to-date'
        assert TaskRunStatus.IGNORE.value == 'ignore'
        assert TaskRunStatus.DONE.value == 'done'
        assert TaskRunStatus.FAILURE.value == 'failure'

    def test_membership(self):
        """Check enum membership."""
        assert TaskRunStatus.RUN in TaskRunStatus
        assert TaskRunStatus.UPTODATE in TaskRunStatus


class TestDispatcherSignal:
    """Tests for DispatcherSignal enum."""

    def test_enum_values(self):
        """Check all expected enum values exist."""
        assert DispatcherSignal.WAIT.value == 'wait'
        assert DispatcherSignal.HOLD_ON.value == 'hold on'
        assert DispatcherSignal.RESET.value == 'reset generator'


class TestTaskRegistry:
    """Tests for TaskRegistry class."""

    def test_add_and_get(self):
        """Add and retrieve tasks."""
        registry = TaskRegistry()
        t1 = Task("task1", None)
        t2 = Task("task2", None)

        registry.add(t1)
        registry.add(t2)

        assert registry.get("task1") is t1
        assert registry.get("task2") is t2
        assert registry.get("nonexistent") is None

    def test_getitem(self):
        """Test __getitem__ access."""
        registry = TaskRegistry()
        t1 = Task("task1", None)
        registry.add(t1)

        assert registry["task1"] is t1
        with pytest.raises(KeyError):
            _ = registry["nonexistent"]

    def test_contains(self):
        """Test membership check."""
        registry = TaskRegistry()
        t1 = Task("task1", None)
        registry.add(t1)

        assert "task1" in registry
        assert "nonexistent" not in registry

    def test_len(self):
        """Test length."""
        registry = TaskRegistry()
        assert len(registry) == 0

        registry.add(Task("t1", None))
        assert len(registry) == 1

        registry.add(Task("t2", None))
        assert len(registry) == 2

    def test_iter(self):
        """Test iteration over task names."""
        registry = TaskRegistry()
        registry.add(Task("t1", None))
        registry.add(Task("t2", None))

        names = list(registry)
        assert "t1" in names
        assert "t2" in names

    def test_values(self):
        """Test iteration over Task objects."""
        registry = TaskRegistry()
        t1 = Task("t1", None)
        t2 = Task("t2", None)
        registry.add(t1)
        registry.add(t2)

        tasks = list(registry.values())
        assert t1 in tasks
        assert t2 in tasks

    def test_items(self):
        """Test iteration over (name, task) pairs."""
        registry = TaskRegistry()
        t1 = Task("t1", None)
        registry.add(t1)

        items = list(registry.items())
        assert ("t1", t1) in items


class TestTargetRegistry:
    """Tests for TargetRegistry class."""

    def test_register_legacy_and_get(self):
        """Register legacy string targets and retrieve task names."""
        registry = TargetRegistry()
        registry.register_legacy("/path/to/file.txt", "build_task")
        registry.register_legacy("/path/to/other.txt", "other_task")

        assert registry.get_task_for_target("/path/to/file.txt") == "build_task"
        assert registry.get_task_for_target("/path/to/other.txt") == "other_task"
        assert registry.get_task_for_target("/nonexistent") is None

    def test_register_target_object(self):
        """Register Target objects and retrieve via find_producer."""
        from doit.deps import FileTarget, FileDependency

        registry = TargetRegistry()
        target = FileTarget("/path/to/output.txt")
        registry.register(target, "build_task")

        dep = FileDependency("/path/to/output.txt")
        assert registry.find_producer(dep) == "build_task"

    def test_contains(self):
        """Test target membership check."""
        registry = TargetRegistry()
        registry.register_legacy("/path/file", "task1")

        assert "/path/file" in registry
        assert "/other" not in registry

    def test_getitem(self):
        """Test __getitem__ access."""
        registry = TargetRegistry()
        registry.register_legacy("/path/file", "task1")

        assert registry["/path/file"] == "task1"
        with pytest.raises(KeyError):
            _ = registry["/nonexistent"]

    def test_find_producer_by_path(self):
        """Test legacy path-based lookup."""
        registry = TargetRegistry()
        registry.register_legacy("/path/file", "task1")

        assert registry.find_producer_by_path("/path/file") == "task1"
        assert registry.find_producer_by_path("/nonexistent") is None

    def test_find_producer_with_target_object(self):
        """Test finding producer with Target object and Dependency matching."""
        from doit.deps import FileTarget, FileDependency

        registry = TargetRegistry()
        target = FileTarget("/path/to/output.txt")
        registry.register(target, "build_task")

        # Find producer using FileDependency
        dep = FileDependency("/path/to/output.txt")
        assert registry.find_producer(dep) == "build_task"

    def test_find_producer_no_match(self):
        """Test find_producer returns None when no match."""
        from doit.deps import FileTarget, FileDependency

        registry = TargetRegistry()
        target = FileTarget("/path/to/output.txt")
        registry.register(target, "build_task")

        dep = FileDependency("/different/path.txt")
        assert registry.find_producer(dep) is None

    def test_duplicate_target_raises(self):
        """Test that registering duplicate target raises InvalidTask."""
        from doit.deps import FileTarget
        from doit.exceptions import InvalidTask

        registry = TargetRegistry()
        target1 = FileTarget("/path/file.txt")
        target2 = FileTarget("/path/file.txt")

        registry.register(target1, "task1")
        with pytest.raises(InvalidTask):
            registry.register(target2, "task2")

    def test_duplicate_legacy_target_raises(self):
        """Test that registering duplicate legacy target raises InvalidTask."""
        from doit.exceptions import InvalidTask

        registry = TargetRegistry()
        registry.register_legacy("/path/file", "task1")
        with pytest.raises(InvalidTask):
            registry.register_legacy("/path/file", "task2")


class TestTargetRegistryPrefixMatching:
    """Tests for TargetRegistry prefix matching via MatchingEngine."""

    def test_directory_target_matches_file_dependency(self, tmp_path):
        """Test that file dependency under directory target matches."""
        from doit.deps import DirectoryTarget, FileDependency

        registry = TargetRegistry()
        target = DirectoryTarget(tmp_path / "output")
        registry.register(target, "generate")

        # File under the directory should match
        dep = FileDependency(str(tmp_path / "output" / "file.txt"))
        assert registry.find_producer(dep) == "generate"

    def test_directory_target_no_match_outside(self, tmp_path):
        """Test that file dependency outside directory target doesn't match."""
        from doit.deps import DirectoryTarget, FileDependency

        registry = TargetRegistry()
        target = DirectoryTarget(tmp_path / "output")
        registry.register(target, "generate")

        # File outside the directory should not match
        dep = FileDependency(str(tmp_path / "input" / "file.txt"))
        assert registry.find_producer(dep) is None

    def test_nested_directory_targets(self, tmp_path):
        """Test that nested directories match the most specific one."""
        from doit.deps import DirectoryTarget, FileDependency

        registry = TargetRegistry()
        registry.register(DirectoryTarget(tmp_path / "output"), "parent")
        registry.register(DirectoryTarget(tmp_path / "output" / "processed"), "child")

        # File in /output/raw/ matches parent
        dep1 = FileDependency(str(tmp_path / "output" / "raw" / "file.txt"))
        assert registry.find_producer(dep1) == "parent"

        # File in /output/processed/ matches child (more specific)
        dep2 = FileDependency(str(tmp_path / "output" / "processed" / "file.txt"))
        assert registry.find_producer(dep2) == "child"

    def test_exact_match_takes_priority(self, tmp_path):
        """Test that exact match takes priority over prefix match."""
        from doit.deps import DirectoryTarget, FileTarget, FileDependency

        registry = TargetRegistry()
        registry.register(DirectoryTarget(tmp_path / "output"), "dir_task")
        specific_file = str(tmp_path / "output" / "specific.txt")
        registry.register(FileTarget(specific_file), "file_task")

        # The specific file should match file_task (exact), not dir_task (prefix)
        dep = FileDependency(specific_file)
        assert registry.find_producer(dep) == "file_task"

        # Other files in directory should still match dir_task
        dep2 = FileDependency(str(tmp_path / "output" / "other.txt"))
        assert registry.find_producer(dep2) == "dir_task"

    def test_find_all_producers(self, tmp_path):
        """Test find_all_producers returns all matching tasks."""
        from doit.deps import DirectoryTarget, FileDependency

        registry = TargetRegistry()
        registry.register(DirectoryTarget(tmp_path / "output"), "parent")
        registry.register(DirectoryTarget(tmp_path / "output" / "processed"), "child")

        # File under both directories should return both
        dep = FileDependency(str(tmp_path / "output" / "processed" / "file.txt"))
        producers = registry.find_all_producers(dep)
        assert set(producers) == {"parent", "child"}

    def test_s3_prefix_matching(self):
        """Test S3 prefix targets work with S3 dependencies."""
        from doit.deps import S3PrefixTarget, S3Dependency

        registry = TargetRegistry()
        registry.register(S3PrefixTarget("bucket", "output/data/"), "s3_task")

        # S3 key under the prefix should match
        dep = S3Dependency("bucket", "output/data/file.parquet")
        assert registry.find_producer(dep) == "s3_task"

    def test_s3_prefix_no_match_different_bucket(self):
        """Test S3 prefix doesn't match different bucket."""
        from doit.deps import S3PrefixTarget, S3Dependency

        registry = TargetRegistry()
        registry.register(S3PrefixTarget("bucket-a", "output/"), "task_a")

        # Different bucket doesn't match
        dep = S3Dependency("bucket-b", "output/file.txt")
        assert registry.find_producer(dep) is None

    def test_stats_property(self, tmp_path):
        """Test stats property shows target counts by type."""
        from doit.deps import DirectoryTarget, FileTarget

        registry = TargetRegistry()
        registry.register(FileTarget(str(tmp_path / "a.txt")), "task_a")
        registry.register(FileTarget(str(tmp_path / "b.txt")), "task_b")
        registry.register(DirectoryTarget(tmp_path / "output"), "task_c")

        stats = registry.stats
        assert stats["exact_count"] == 2
        assert stats["prefix_count"] == 1
        assert stats["total_count"] == 3


class TestExecNodeRegistry:
    """Tests for ExecNodeRegistry class."""

    def test_get_empty(self):
        """Get from empty registry returns None."""
        task_registry = TaskRegistry()
        node_registry = ExecNodeRegistry(task_registry)

        assert node_registry.get("nonexistent") is None

    def test_get_or_create(self):
        """Create node lazily."""
        task_registry = TaskRegistry()
        t1 = Task("task1", None)
        task_registry.add(t1)

        node_registry = ExecNodeRegistry(task_registry)

        # Should be empty initially
        assert node_registry.get("task1") is None

        # Create node
        node = node_registry.get_or_create("task1", parent=None)
        assert node is not None
        assert node.task is t1

        # Get same node again
        node2 = node_registry.get_or_create("task1", parent=None)
        assert node2 is node

    def test_contains(self):
        """Test node existence check."""
        task_registry = TaskRegistry()
        t1 = Task("task1", None)
        task_registry.add(t1)

        node_registry = ExecNodeRegistry(task_registry)

        assert "task1" not in node_registry
        node_registry.get_or_create("task1", parent=None)
        assert "task1" in node_registry

    def test_getitem(self):
        """Test __getitem__ access after creation."""
        task_registry = TaskRegistry()
        t1 = Task("task1", None)
        task_registry.add(t1)

        node_registry = ExecNodeRegistry(task_registry)
        node = node_registry.get_or_create("task1", parent=None)

        assert node_registry["task1"] is node

    def test_iter_and_values(self):
        """Test iteration."""
        task_registry = TaskRegistry()
        t1 = Task("t1", None)
        t2 = Task("t2", None)
        task_registry.add(t1)
        task_registry.add(t2)

        node_registry = ExecNodeRegistry(task_registry)
        n1 = node_registry.get_or_create("t1", parent=None)
        n2 = node_registry.get_or_create("t2", parent=None)

        names = list(node_registry)
        assert "t1" in names
        assert "t2" in names

        nodes = list(node_registry.values())
        assert n1 in nodes
        assert n2 in nodes


class TestRegexGroup:
    """Tests for RegexGroup helper class."""

    def test_init(self):
        """Test initialization."""
        group = RegexGroup("target.o", {"task1", "task2"})

        assert group.target == "target.o"
        assert group.tasks == {"task1", "task2"}
        assert group.found is False

    def test_found_flag(self):
        """Test found flag can be set."""
        group = RegexGroup("target.o", {"task1"})
        assert group.found is False

        group.found = True
        assert group.found is True


class TestTaskSelector:
    """Tests for TaskSelector class."""

    @pytest.fixture
    def basic_tasks(self):
        """Create basic task setup."""
        tasks = OrderedDict()
        tasks["t1"] = Task("t1", None)
        tasks["t2"] = Task("t2", None)
        tasks["t3"] = Task("t3", None)
        tasks["group:a"] = Task("group:a", None)
        tasks["group:b"] = Task("group:b", None)
        return tasks

    @pytest.fixture
    def tasks_with_targets(self, basic_tasks):
        """Create tasks with targets."""
        basic_tasks["build"] = Task("build", None, targets=["output.txt"])
        targets = {"output.txt": "build"}
        return basic_tasks, targets

    def test_select_by_name(self, basic_tasks):
        """Select tasks by exact name."""
        selector = TaskSelector(basic_tasks, {})

        result = selector.select(["t1", "t2"])
        assert result == ["t1", "t2"]

    def test_select_by_pattern(self, basic_tasks):
        """Select tasks by wildcard pattern."""
        selector = TaskSelector(basic_tasks, {})

        result = selector.select(["group:*"])
        assert "group:a" in result
        assert "group:b" in result
        assert "t1" not in result

    def test_select_by_target(self, tasks_with_targets):
        """Select task by target file path."""
        tasks, targets = tasks_with_targets
        selector = TaskSelector(tasks, targets)

        result = selector.select(["output.txt"])
        assert result == ["build"]

    def test_select_not_found(self, basic_tasks):
        """Raise error for nonexistent task."""
        selector = TaskSelector(basic_tasks, {})

        with pytest.raises(InvalidCommand):
            selector.select(["nonexistent"])

    def test_get_wild_tasks(self, basic_tasks):
        """Test wildcard matching helper."""
        selector = TaskSelector(basic_tasks, {})
        task_order = list(basic_tasks.keys())

        result = selector.get_wild_tasks("t*", task_order)
        assert "t1" in result
        assert "t2" in result
        assert "t3" in result
        assert "group:a" not in result

    def test_select_mixed(self, tasks_with_targets):
        """Select using mix of names, patterns, and targets."""
        tasks, targets = tasks_with_targets
        selector = TaskSelector(tasks, targets)

        result = selector.select(["t1", "group:*", "output.txt"])
        assert "t1" in result
        assert "group:a" in result
        assert "group:b" in result
        assert "build" in result

    def test_select_delayed_subtask(self):
        """Select delayed subtask creates placeholder task."""
        tasks = OrderedDict()
        loader = DelayedLoader(lambda: None, executed='not yet')
        tasks["parent"] = Task("parent", None, loader=loader)
        targets = {}

        selector = TaskSelector(tasks, targets)
        result = selector.select(["parent:sub"])

        assert "parent:sub" in result
        assert "parent:sub" in tasks  # Placeholder was created

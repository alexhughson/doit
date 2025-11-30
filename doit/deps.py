"""Dependency and Target classes for doit tasks.

This module provides self-checking dependency objects that replace the
string-based file_dep and task_dep system. Each dependency type knows
how to determine if it has changed since the last successful task execution.

It also provides Target classes for task outputs, enabling the system to
match dependencies against targets for implicit task dependencies.

Usage:
    from doit.deps import FileDependency, TaskDependency, FileTarget

    Task(
        'compile',
        actions=['gcc -o main main.c'],
        dependencies=[
            FileDependency('main.c'),
            FileDependency('header.h'),
            TaskDependency('setup'),
        ],
        outputs=[FileTarget('main')],
    )
"""

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from doit.dependency import get_file_md5


@dataclass
class Dependency(ABC):
    """Base class for all dependency types.

    Each dependency is self-checking: it knows how to determine if it has
    changed since the last successful task execution. Subclasses must
    implement all abstract methods.

    The dependency lifecycle:
    1. Before task runs: is_modified(stored_state) checks if task needs to run
    2. After task succeeds: get_state(current_state) saves new state
    """

    @abstractmethod
    def get_key(self) -> str:
        """Return a unique storage key for this dependency.

        Used as the key in the storage backend. Must be unique within a task.
        Examples:
        - FileDependency: absolute path ("/home/user/project/data.txt")
        - TaskDependency: "task:task_name"
        - S3Dependency: "s3://bucket/key"
        """
        pass

    @abstractmethod
    def is_modified(self, stored_state: Any) -> bool:
        """Check if this dependency has changed since stored_state.

        @param stored_state: The state saved from last successful execution,
                            or None if never executed.
        @return: True if the dependency has changed (task should run)
        """
        pass

    @abstractmethod
    def get_state(self, current_state: Any) -> Any:
        """Compute the current state to be saved after successful execution.

        @param current_state: The previously stored state (for optimization)
        @return: JSON-serializable state, or None if unchanged from current_state
        """
        pass

    @abstractmethod
    def exists(self) -> bool:
        """Check if this dependency currently exists.

        For files, this means the file exists on disk.
        For tasks, this is always True (validated elsewhere).
        For S3, this means the object exists in the bucket.
        """
        pass

    def creates_task_dep(self) -> Optional[str]:
        """Return task name if this dependency creates an implicit task_dep.

        Only TaskDependency returns a value here. FileDependency and others
        return None, though implicit task deps may be added based on targets.
        """
        return None


@dataclass
class FileDependency(Dependency):
    """Local file dependency with configurable change detection.

    Supports two checker modes:
    - "md5" (default): 3-level check (timestamp -> size -> md5 hash)
    - "timestamp": Simple mtime-based checking (faster, like Make)

    Example:
        FileDependency('src/main.c')
        FileDependency('data.csv', checker='timestamp')
    """

    path: str
    checker: str = "md5"  # "md5" or "timestamp"

    def get_key(self) -> str:
        """Return absolute path as the storage key."""
        return os.path.abspath(self.path)

    def is_modified(self, stored_state: Any) -> bool:
        """Check if file has changed since stored_state.

        For md5 checker: uses 3-level optimization
          1. If timestamp unchanged -> not modified (fast path)
          2. If size changed -> modified
          3. If md5 differs -> modified

        For timestamp checker: simple mtime comparison
        """
        if stored_state is None:
            return True

        try:
            file_stat = os.stat(self.path)
        except OSError:
            # File doesn't exist - will be caught by exists() check
            return True

        if self.checker == "timestamp":
            return file_stat.st_mtime != stored_state

        # MD5 checker: 3-level check
        try:
            timestamp, size, file_md5 = stored_state
        except (TypeError, ValueError):
            # Invalid stored state format
            return True

        # Level 1: timestamp unchanged = file unchanged (fast path)
        if file_stat.st_mtime == timestamp:
            return False

        # Level 2: size changed = definitely modified
        if file_stat.st_size != size:
            return True

        # Level 3: check md5 hash (slow but thorough)
        return file_md5 != get_file_md5(self.path)

    def get_state(self, current_state: Any) -> Any:
        """Compute file state to save after execution.

        For md5 checker: returns (timestamp, size, md5)
        For timestamp checker: returns mtime float
        """
        if self.checker == "timestamp":
            return os.path.getmtime(self.path)

        # MD5 checker
        timestamp = os.path.getmtime(self.path)

        # Optimization: if timestamp unchanged, state is same
        if current_state is not None:
            try:
                old_timestamp = current_state[0]
                if old_timestamp == timestamp:
                    return None  # No change needed
            except (TypeError, IndexError):
                pass

        size = os.path.getsize(self.path)
        md5 = get_file_md5(self.path)
        return (timestamp, size, md5)

    def exists(self) -> bool:
        """Check if the file exists on disk."""
        return os.path.exists(self.path)


@dataclass
class TaskDependency(Dependency):
    """Dependency on another task's execution.

    TaskDependency only controls execution order - it does NOT affect
    up-to-date status. A task with only TaskDependency deps will always
    run (unless it has other uptodate conditions).

    Example:
        TaskDependency('setup')
        TaskDependency('build:lib')  # subtask
    """

    task_name: str

    def get_key(self) -> str:
        """Return 'task:name' as the storage key."""
        return f"task:{self.task_name}"

    def is_modified(self, stored_state: Any) -> bool:
        """Task dependencies don't affect up-to-date status."""
        return False

    def get_state(self, current_state: Any) -> Any:
        """No state to save for task dependencies."""
        return None

    def exists(self) -> bool:
        """Task existence is validated by TaskControl, not here."""
        return True

    def creates_task_dep(self) -> Optional[str]:
        """Return the task name this dependency requires."""
        return self.task_name


# =============================================================================
# Target Classes
# =============================================================================

@dataclass
class Target(ABC):
    """Base class for all target (output) types.

    Targets represent the outputs of a task. They are used for:
    1. Checking if targets exist (task needs to run if missing)
    2. Matching dependencies to targets for implicit task dependencies
    3. Clean operations

    A dependency matches a target if dep.matches_target(target) returns True.
    """

    @abstractmethod
    def get_key(self) -> str:
        """Return a unique key for this target.

        Used for matching against dependencies and for storage.
        Should use the same key format as the corresponding Dependency type.
        """
        pass

    @abstractmethod
    def exists(self) -> bool:
        """Check if this target currently exists."""
        pass

    def matches_dependency(self, dep: Dependency) -> bool:
        """Check if a dependency matches this target.

        Default implementation compares keys. Override for custom matching.
        """
        return self.get_key() == dep.get_key()


@dataclass
class FileTarget(Target):
    """Local file target (output).

    Example:
        FileTarget('build/main.o')
        FileTarget('output/report.pdf')
    """

    path: str

    def get_key(self) -> str:
        """Return absolute path as the key (matches FileDependency)."""
        return os.path.abspath(self.path)

    def exists(self) -> bool:
        """Check if the file exists on disk."""
        return os.path.exists(self.path)

    def matches_dependency(self, dep: Dependency) -> bool:
        """Check if a FileDependency matches this target."""
        if isinstance(dep, FileDependency):
            return self.get_key() == dep.get_key()
        return False

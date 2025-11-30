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

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Union

from doit.dependency import get_file_md5
from doit.matching.protocols import MatchStrategy


class CheckStatus(Enum):
    """Result of a dependency status check."""
    UP_TO_DATE = "up-to-date"
    CHANGED = "changed"
    MISSING = "missing"
    ERROR = "error"


@dataclass
class DependencyCheckResult:
    """Result of a dependency's self-check.

    Returned by Dependency.check_status() to report whether the dependency
    has changed since the last successful execution.

    Attributes:
        status: The check result status
        reason: Human-readable explanation of why task needs to run
        error_message: Error details if status is ERROR
    """
    status: CheckStatus
    reason: Optional[str] = None
    error_message: Optional[str] = None

    @property
    def is_up_to_date(self) -> bool:
        """Return True if dependency hasn't changed."""
        return self.status == CheckStatus.UP_TO_DATE

    @property
    def needs_execution(self) -> bool:
        """Return True if task should run due to this dependency."""
        return self.status in (CheckStatus.CHANGED, CheckStatus.MISSING)

    @property
    def is_error(self) -> bool:
        """Return True if an error occurred during checking."""
        return self.status == CheckStatus.ERROR


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

    def get_match_strategy(self) -> MatchStrategy:
        """Return how this dependency should be matched against targets.

        Default is EXACT matching (keys must be equal).
        Override in subclasses for different matching behavior.

        Returns:
            MatchStrategy indicating which index to use for matching.
        """
        return MatchStrategy.EXACT

    @abstractmethod
    def check_status(self, stored_state: Any) -> DependencyCheckResult:
        """Perform a complete status check for this dependency.

        This is the primary self-checking method. It combines existence checking
        and modification detection into a single call, returning a structured
        result that indicates whether the task needs to run.

        @param stored_state: The state saved from last successful execution,
                            or None if never executed.
        @return: DependencyCheckResult with status, reason, and any error
        """
        pass


@dataclass
class FileDependency(Dependency):
    """Local file dependency with configurable change detection.

    Supports two checker modes:
    - "md5" (default): 3-level check (timestamp -> size -> md5 hash)
    - "timestamp": Simple mtime-based checking (faster, like Make)

    Example:
        FileDependency('src/main.c')
        FileDependency(Path('data.csv'), checker='timestamp')
    """

    path: Union[str, Path]
    checker: str = "md5"  # "md5" or "timestamp"
    _path_obj: Path = field(init=False, repr=False)

    def __post_init__(self):
        """Convert path to Path object for internal use."""
        self._path_obj = Path(self.path)

    def get_key(self) -> str:
        """Return absolute path as the storage key."""
        return str(self._path_obj.resolve())

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
            file_stat = self._path_obj.stat()
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
        return file_md5 != get_file_md5(self._path_obj)

    def get_state(self, current_state: Any) -> Any:
        """Compute file state to save after execution.

        For md5 checker: returns (timestamp, size, md5)
        For timestamp checker: returns mtime float
        """
        file_stat = self._path_obj.stat()

        if self.checker == "timestamp":
            return file_stat.st_mtime

        # MD5 checker
        timestamp = file_stat.st_mtime

        # Optimization: if timestamp unchanged, state is same
        if current_state is not None:
            try:
                old_timestamp = current_state[0]
                if old_timestamp == timestamp:
                    return None  # No change needed
            except (TypeError, IndexError):
                pass

        size = file_stat.st_size
        md5 = get_file_md5(self._path_obj)
        return (timestamp, size, md5)

    def exists(self) -> bool:
        """Check if the file exists on disk."""
        return self._path_obj.exists()

    def check_status(self, stored_state: Any) -> DependencyCheckResult:
        """Perform complete status check for this file dependency.

        Combines existence and modification checks:
        1. If file doesn't exist -> ERROR (missing dependency)
        2. If no stored state -> CHANGED (first run)
        3. If file modified -> CHANGED
        4. Otherwise -> UP_TO_DATE
        """
        key = self.get_key()

        # Check existence first
        if not self.exists():
            return DependencyCheckResult(
                status=CheckStatus.ERROR,
                reason=f"file '{key}' does not exist",
                error_message=f"Dependency '{key}' does not exist."
            )

        # No stored state = first run or state was cleared
        if stored_state is None:
            return DependencyCheckResult(
                status=CheckStatus.CHANGED,
                reason=f"file '{key}' has no stored state (first run)"
            )

        # Check if modified
        if self.is_modified(stored_state):
            return DependencyCheckResult(
                status=CheckStatus.CHANGED,
                reason=f"file '{key}' has been modified"
            )

        return DependencyCheckResult(status=CheckStatus.UP_TO_DATE)


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

    def check_status(self, stored_state: Any) -> DependencyCheckResult:
        """Task dependencies are always up-to-date (they only affect ordering).

        TaskDependency controls execution order but does NOT affect whether
        a task is considered up-to-date. The actual task ordering is handled
        by the TaskDispatcher.
        """
        return DependencyCheckResult(status=CheckStatus.UP_TO_DATE)


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

    def get_match_strategy(self) -> MatchStrategy:
        """Return how this target should be matched against dependencies.

        Default is EXACT matching (keys must be equal).
        Override in subclasses for different matching behavior (e.g., PREFIX).

        Returns:
            MatchStrategy indicating which index to use for matching.
        """
        return MatchStrategy.EXACT

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
        FileTarget(Path('output/report.pdf'))
    """

    path: Union[str, Path]
    _path_obj: Path = field(init=False, repr=False)

    def __post_init__(self):
        """Convert path to Path object for internal use."""
        self._path_obj = Path(self.path)

    def get_key(self) -> str:
        """Return absolute path as the key (matches FileDependency)."""
        return str(self._path_obj.resolve())

    def exists(self) -> bool:
        """Check if the file exists on disk."""
        return self._path_obj.exists()

    def matches_dependency(self, dep: Dependency) -> bool:
        """Check if a FileDependency matches this target."""
        if isinstance(dep, FileDependency):
            return self.get_key() == dep.get_key()
        return False


# =============================================================================
# S3 Dependency and Target Classes
# =============================================================================

@dataclass
class S3Dependency(Dependency):
    """S3 object dependency with ETag-based change detection.

    Requires boto3 (lazy import). Change detection uses HEAD request
    to compare ETag values, which is efficient and reliable.

    Example:
        S3Dependency('my-bucket', 'data/input.csv')
        S3Dependency('my-bucket', 'data/input.csv', profile='dev')

    Attributes:
        bucket: S3 bucket name
        key: S3 object key (path within bucket)
        profile: AWS profile name (optional)
        region: AWS region (optional)
    """

    bucket: str
    key: str
    profile: Optional[str] = None
    region: Optional[str] = None
    _client: Any = field(init=False, repr=False, default=None)

    def _get_client(self):
        """Lazy-load boto3 and create S3 client."""
        if self._client is None:
            try:
                import boto3
            except ImportError:
                raise ImportError(
                    "boto3 required for S3Dependency. Install: pip install boto3"
                )
            session_kwargs = {}
            if self.profile:
                session_kwargs['profile_name'] = self.profile
            if self.region:
                session_kwargs['region_name'] = self.region
            self._client = boto3.Session(**session_kwargs).client('s3')
        return self._client

    def get_key(self) -> str:
        """Return S3 URI: s3://bucket/key"""
        return f"s3://{self.bucket}/{self.key}"

    def exists(self) -> bool:
        """Check if object exists via HEAD request."""
        try:
            self._get_client().head_object(Bucket=self.bucket, Key=self.key)
            return True
        except Exception:
            return False

    def is_modified(self, stored_state: Any) -> bool:
        """Check if ETag changed since last run.

        @param stored_state: Previously stored (etag, last_modified) tuple
        @return: True if the object has changed
        """
        if stored_state is None:
            return True
        try:
            resp = self._get_client().head_object(Bucket=self.bucket, Key=self.key)
            current_etag = resp['ETag'].strip('"')
            # stored_state is (etag, mtime) tuple
            stored_etag = stored_state[0] if isinstance(stored_state, tuple) else stored_state
            return current_etag != stored_etag
        except Exception:
            return True

    def get_state(self, current_state: Any) -> Any:
        """Return (etag, last_modified) for storage.

        @param current_state: Previously stored state (for optimization)
        @return: (etag, mtime) tuple, or None if unchanged
        """
        try:
            resp = self._get_client().head_object(Bucket=self.bucket, Key=self.key)
            etag = resp['ETag'].strip('"')
            mtime = resp['LastModified'].timestamp()
            # Optimization: skip if unchanged
            if current_state and isinstance(current_state, tuple):
                if current_state[0] == etag:
                    return None
            return (etag, mtime)
        except Exception:
            return None

    def check_status(self, stored_state: Any) -> DependencyCheckResult:
        """Complete status check for S3 dependency.

        Combines existence and modification checks into a single result.
        """
        s3_key = self.get_key()
        if not self.exists():
            return DependencyCheckResult(
                status=CheckStatus.ERROR,
                reason=f"S3 object '{s3_key}' does not exist",
                error_message=f"Dependency '{s3_key}' does not exist."
            )
        if stored_state is None:
            return DependencyCheckResult(
                status=CheckStatus.CHANGED,
                reason=f"S3 object '{s3_key}' has no stored state (first run)"
            )
        if self.is_modified(stored_state):
            return DependencyCheckResult(
                status=CheckStatus.CHANGED,
                reason=f"S3 object '{s3_key}' has been modified"
            )
        return DependencyCheckResult(status=CheckStatus.UP_TO_DATE)


@dataclass
class S3Target(Target):
    """S3 object target (output).

    Represents an S3 object produced by a task. Used for:
    1. Checking if task outputs exist
    2. Matching S3Dependency objects for implicit task dependencies

    Example:
        S3Target('my-bucket', 'output/results.csv')

    Attributes:
        bucket: S3 bucket name
        key: S3 object key (path within bucket)
        profile: AWS profile name (optional)
        region: AWS region (optional)
    """

    bucket: str
    key: str
    profile: Optional[str] = None
    region: Optional[str] = None
    _client: Any = field(init=False, repr=False, default=None)

    def _get_client(self):
        """Lazy-load boto3 and create S3 client."""
        if self._client is None:
            try:
                import boto3
            except ImportError:
                raise ImportError(
                    "boto3 required for S3Target. Install: pip install boto3"
                )
            session_kwargs = {}
            if self.profile:
                session_kwargs['profile_name'] = self.profile
            if self.region:
                session_kwargs['region_name'] = self.region
            self._client = boto3.Session(**session_kwargs).client('s3')
        return self._client

    def get_key(self) -> str:
        """Return S3 URI: s3://bucket/key"""
        return f"s3://{self.bucket}/{self.key}"

    def exists(self) -> bool:
        """Check if object exists via HEAD request."""
        try:
            self._get_client().head_object(Bucket=self.bucket, Key=self.key)
            return True
        except Exception:
            return False

    def matches_dependency(self, dep: Dependency) -> bool:
        """Match S3Dependency with same bucket/key.

        This enables implicit task dependency matching for S3 objects.
        """
        if isinstance(dep, S3Dependency):
            return self.bucket == dep.bucket and self.key == dep.key
        return False


# =============================================================================
# Directory/Prefix Dependency and Target Classes
# =============================================================================

@dataclass
class DirectoryDependency(Dependency):
    """Dependency on a directory/prefix - depends on everything under it.

    Use when you need all files under a directory but don't know exactly
    which files will exist. The dependency is satisfied by any target
    whose output is under this directory's path.

    Example:
        DirectoryDependency('/data/processed/')
        # This will depend on any task that outputs to /data/, /data/processed/, etc.

    Attributes:
        path: Directory path (will be normalized with trailing slash)
    """

    path: Union[str, Path]
    _path_obj: Path = field(init=False, repr=False)

    def __post_init__(self):
        """Convert path to Path object for internal use."""
        self._path_obj = Path(self.path) if isinstance(self.path, str) else self.path

    def get_key(self) -> str:
        """Return normalized path with trailing slash."""
        resolved = str(self._path_obj.resolve())
        return resolved if resolved.endswith('/') else resolved + '/'

    def get_match_strategy(self) -> MatchStrategy:
        """Directory dependencies use PREFIX matching."""
        return MatchStrategy.PREFIX

    def exists(self) -> bool:
        """Check if the directory exists."""
        return self._path_obj.is_dir()

    def is_modified(self, stored_state: Any) -> bool:
        """Directory dependencies always return True (force re-evaluation).

        Since we don't know what files are under the directory, we can't
        reliably determine if it changed. The actual files should be
        tracked by the producing task.
        """
        return True

    def get_state(self, current_state: Any) -> Any:
        """Return directory existence as state."""
        return self.exists()

    def check_status(self, stored_state: Any) -> DependencyCheckResult:
        """Check status - directories are always considered changed.

        This ensures the task re-runs when depending on a directory,
        which is the expected behavior for prefix dependencies.
        """
        key = self.get_key()
        return DependencyCheckResult(
            status=CheckStatus.CHANGED,
            reason=f"directory dependency '{key}' always triggers re-run"
        )


@dataclass
class DirectoryTarget(Target):
    """Target representing a directory/prefix output.

    Use when a task produces files in a directory but doesn't know exactly
    which files will be created. Any dependency under this directory will
    automatically depend on the task that produces this target.

    Example:
        DirectoryTarget('/output/generated/')
        # Any dependency on /output/generated/*, /output/generated/subdir/*, etc.
        # will automatically depend on the task that produces this target.

    Attributes:
        path: Directory path (will be normalized with trailing slash)
    """

    path: Union[str, Path]
    _path_obj: Path = field(init=False, repr=False)

    def __post_init__(self):
        """Convert path to Path object for internal use."""
        self._path_obj = Path(self.path) if isinstance(self.path, str) else self.path

    def get_key(self) -> str:
        """Return normalized path with trailing slash."""
        resolved = str(self._path_obj.resolve())
        return resolved if resolved.endswith('/') else resolved + '/'

    def get_match_strategy(self) -> MatchStrategy:
        """Directory targets use PREFIX matching."""
        return MatchStrategy.PREFIX

    def exists(self) -> bool:
        """Check if the directory exists."""
        return self._path_obj.is_dir()


@dataclass
class S3PrefixDependency(Dependency):
    """Dependency on an S3 prefix - depends on all objects under it.

    Use when you need all objects under an S3 prefix but don't know exactly
    which objects will exist.

    Example:
        S3PrefixDependency('my-bucket', 'data/processed/')
        # Depends on any task that outputs to s3://my-bucket/data/ or below

    Attributes:
        bucket: S3 bucket name
        prefix: S3 key prefix (will be normalized with trailing slash)
        profile: AWS profile name (optional)
        region: AWS region (optional)
    """

    bucket: str
    prefix: str
    profile: Optional[str] = None
    region: Optional[str] = None

    def get_key(self) -> str:
        """Return S3 URI with normalized prefix (trailing slash)."""
        normalized = self.prefix if self.prefix.endswith('/') else self.prefix + '/'
        return f"s3://{self.bucket}/{normalized}"

    def get_match_strategy(self) -> MatchStrategy:
        """S3 prefix dependencies use PREFIX matching."""
        return MatchStrategy.PREFIX

    def exists(self) -> bool:
        """S3 prefixes are considered to always exist (virtual concept)."""
        return True

    def is_modified(self, stored_state: Any) -> bool:
        """S3 prefix dependencies always return True (force re-evaluation)."""
        return True

    def get_state(self, current_state: Any) -> Any:
        """Return True as state (prefix exists as a concept)."""
        return True

    def check_status(self, stored_state: Any) -> DependencyCheckResult:
        """Check status - S3 prefix dependencies always trigger re-run."""
        key = self.get_key()
        return DependencyCheckResult(
            status=CheckStatus.CHANGED,
            reason=f"S3 prefix dependency '{key}' always triggers re-run"
        )


@dataclass
class S3PrefixTarget(Target):
    """Target representing an S3 prefix output.

    Use when a task produces objects under an S3 prefix but doesn't know
    exactly which objects will be created.

    Example:
        S3PrefixTarget('my-bucket', 'output/generated/')

    Attributes:
        bucket: S3 bucket name
        prefix: S3 key prefix (will be normalized with trailing slash)
        profile: AWS profile name (optional)
        region: AWS region (optional)
    """

    bucket: str
    prefix: str
    profile: Optional[str] = None
    region: Optional[str] = None

    def get_key(self) -> str:
        """Return S3 URI with normalized prefix (trailing slash)."""
        normalized = self.prefix if self.prefix.endswith('/') else self.prefix + '/'
        return f"s3://{self.bucket}/{normalized}"

    def get_match_strategy(self) -> MatchStrategy:
        """S3 prefix targets use PREFIX matching."""
        return MatchStrategy.PREFIX

    def exists(self) -> bool:
        """S3 prefixes are considered to always exist (virtual concept)."""
        return True

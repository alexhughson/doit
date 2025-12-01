"""Input classes for pattern-based task generation.

This module provides the Input base class and concrete implementations
for matching resources with named captures in patterns.

Pattern syntax:
- <name> - Named capture (matches any non-slash characters)
- * - Wildcard (standard glob)

Example:
    input = FileInput("src/<arch>/<module>.c")
    for match in input.match():
        print(match.captures)  # {'arch': 'x86', 'module': 'main'}
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, Dict, Any, List, Optional
import re


@dataclass
class CaptureMatch:
    """A single matched resource with its captured attributes.

    Attributes:
        key: Resource identifier (e.g., file path, S3 key)
        captures: Dict mapping capture names to matched values
        dependency: The Dependency object for this resource
    """
    key: str
    captures: Dict[str, str]
    dependency: Any


@dataclass
class Input(ABC):
    """Base class for input patterns.

    Subclasses implement resource listing and dependency creation.
    Users can create custom subclasses for databases, APIs, etc.

    Attributes:
        pattern: Pattern string with <name> captures and optional * wildcards
        required: If True, task generation fails if no match found
        is_list: If True, multiple matches are collected into a list
    """
    pattern: str
    required: bool = True
    is_list: bool = False

    # Computed fields
    _glob_pattern: str = field(init=False, repr=False, default='')
    _capture_regex: Optional[re.Pattern] = field(init=False, repr=False, default=None)
    _capture_names: List[str] = field(init=False, repr=False, default_factory=list)

    def __post_init__(self):
        self._compile_pattern()
        # Auto-detect is_list if pattern contains * in filename portion
        if '*' in self.pattern.split('/')[-1]:
            self.is_list = True

    def _compile_pattern(self) -> None:
        """Compile pattern into glob pattern and capture regex."""
        self._capture_names = []
        capture_re = re.compile(r'<(\w+)>')

        glob_parts = []
        regex_parts = []
        last_end = 0

        for match in capture_re.finditer(self.pattern):
            name = match.group(1)
            self._capture_names.append(name)

            # Literal text before this capture
            literal = self.pattern[last_end:match.start()]
            glob_parts.append(literal)
            regex_parts.append(self._escape_for_regex(literal))

            # Replace capture with * for glob, named group for regex
            glob_parts.append('*')
            regex_parts.append(f'(?P<{name}>[^/]+)')

            last_end = match.end()

        # Trailing literal
        literal = self.pattern[last_end:]
        glob_parts.append(literal)
        regex_parts.append(self._escape_for_regex(literal))

        self._glob_pattern = ''.join(glob_parts)
        self._capture_regex = re.compile('^' + ''.join(regex_parts) + '$')

    @staticmethod
    def _escape_for_regex(s: str) -> str:
        """Escape string for regex, but convert * wildcards to [^/]* pattern."""
        # Split on *, escape each part, then join with [^/]*
        parts = s.split('*')
        escaped_parts = [re.escape(p) for p in parts]
        return '[^/]*'.join(escaped_parts)

    @property
    def capture_names(self) -> List[str]:
        """Names of captures defined in this pattern."""
        return self._capture_names.copy()

    @abstractmethod
    def list_resources(self) -> Generator[str, None, None]:
        """Yield resource identifiers matching the glob pattern.

        Subclasses implement this to list files, S3 objects, database rows, etc.
        """
        pass

    @abstractmethod
    def create_dependency(self, resource_key: str) -> Any:
        """Create a Dependency object for the given resource.

        Args:
            resource_key: The resource identifier from list_resources()

        Returns:
            A Dependency subclass instance (e.g., FileDependency, S3Dependency)
        """
        pass

    def match(self) -> Generator[CaptureMatch, None, None]:
        """List matching resources and extract captures.

        Yields:
            CaptureMatch for each resource matching the pattern
        """
        for resource_key in self.list_resources():
            match_key = self._get_match_key(resource_key)
            m = self._capture_regex.match(match_key)
            if m:
                yield CaptureMatch(
                    key=resource_key,
                    captures=m.groupdict(),
                    dependency=self.create_dependency(resource_key),
                )

    def _get_match_key(self, resource_key: str) -> str:
        """Convert resource key for regex matching.

        Subclasses can override to transform the key before matching.
        Default implementation returns the key unchanged.
        """
        return resource_key


@dataclass
class FileInput(Input):
    """Input pattern for local files.

    Example:
        FileInput("src/<arch>/<module>.c")
        FileInput("/data/textract/<doc>.page*.txt")  # is_list auto-detected
    """
    base_path: Optional[Path] = None

    def __post_init__(self):
        if self.base_path is None:
            self.base_path = Path.cwd()
        elif isinstance(self.base_path, str):
            self.base_path = Path(self.base_path)
        super().__post_init__()

    def list_resources(self) -> Generator[str, None, None]:
        """Yield absolute paths of files matching the glob pattern."""
        for path in self.base_path.glob(self._glob_pattern):
            yield str(path)

    def _get_match_key(self, resource_key: str) -> str:
        """Return path relative to base_path for regex matching."""
        return str(Path(resource_key).relative_to(self.base_path))

    def create_dependency(self, resource_key: str) -> Any:
        """Create a FileDependency for the given path."""
        from doit.deps import FileDependency
        return FileDependency(resource_key)


@dataclass
class S3Input(Input):
    """Input pattern for S3 objects.

    Example:
        S3Input("raw/<dataset>/<partition>.parquet",
                bucket="my-bucket", profile="dev")
    """
    bucket: str = ""
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
                    "boto3 required for S3Input. Install: pip install boto3"
                )
            session_kwargs = {}
            if self.profile:
                session_kwargs['profile_name'] = self.profile
            if self.region:
                session_kwargs['region_name'] = self.region
            self._client = boto3.Session(**session_kwargs).client('s3')
        return self._client

    def list_resources(self) -> Generator[str, None, None]:
        """Yield S3 keys matching the glob pattern."""
        # Get prefix up to first wildcard for efficient S3 listing
        prefix = self._glob_pattern.split('*')[0]
        paginator = self._get_client().get_paginator('list_objects_v2')

        # Build regex for filtering (convert glob to regex)
        regex_pattern = self._glob_pattern.replace('.', r'\.').replace('*', '.*')
        regex = re.compile(f'^{regex_pattern}$')

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if regex.match(key):
                    yield key  # Just the key, not full URI

    def create_dependency(self, resource_key: str) -> Any:
        """Create an S3Dependency for the given key."""
        from doit.deps import S3Dependency
        return S3Dependency(
            self.bucket, resource_key,
            profile=self.profile, region=self.region
        )


@dataclass
class DirectoryInput(Input):
    """Input pattern for a directory prefix.

    Instead of listing individual files, this declares a dependency on
    a directory as a whole. Files matching the pattern under the directory
    are collected, but the dependency is on the directory itself.

    This is useful when:
    - You don't know exact filenames ahead of time
    - A producer task outputs to a directory
    - You want to match all files under a path prefix

    Example:
        DirectoryInput("data/raw/<dataset>/")  # depends on directory prefix
        DirectoryInput("/output/<partition>/")  # depends on partition dir
    """
    base_path: Optional[Path] = None

    def __post_init__(self):
        if self.base_path is None:
            self.base_path = Path.cwd()
        elif isinstance(self.base_path, str):
            self.base_path = Path(self.base_path)
        super().__post_init__()
        # Directory inputs always produce a single dependency
        self.is_list = False

    def list_resources(self) -> Generator[str, None, None]:
        """Yield directory paths matching the glob pattern.

        For directories, we match the directory path itself, not files under it.
        """
        # Remove any trailing file pattern - we want directories
        dir_pattern = self._glob_pattern.rstrip('/')
        if '*' not in dir_pattern:
            # No wildcards - just yield the pattern as-is
            yield str(self.base_path / dir_pattern)
        else:
            # Find directories matching the pattern
            for path in self.base_path.glob(dir_pattern):
                if path.is_dir():
                    yield str(path)

    def _get_match_key(self, resource_key: str) -> str:
        """Return path relative to base_path for regex matching."""
        return str(Path(resource_key).relative_to(self.base_path))

    def create_dependency(self, resource_key: str) -> Any:
        """Create a DirectoryDependency for the given path."""
        from doit.deps import DirectoryDependency
        return DirectoryDependency(resource_key)


@dataclass
class S3PrefixInput(Input):
    """Input pattern for an S3 prefix (directory-like).

    Instead of listing individual S3 objects, this declares a dependency
    on a prefix. Objects matching the pattern under the prefix are collected,
    but the dependency is on the prefix itself.

    Example:
        S3PrefixInput("raw/<dataset>/", bucket="my-bucket")
    """
    bucket: str = ""
    profile: Optional[str] = None
    region: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()
        # Prefix inputs always produce a single dependency
        self.is_list = False

    def list_resources(self) -> Generator[str, None, None]:
        """Yield prefix keys matching the pattern.

        For prefixes, we don't actually list S3 - we just yield the pattern.
        The prefix represents a virtual directory that may or may not exist.
        """
        # Remove trailing wildcards - we want the prefix
        prefix = self._glob_pattern.rstrip('/*')
        if not prefix.endswith('/'):
            prefix = prefix + '/'
        yield prefix

    def create_dependency(self, resource_key: str) -> Any:
        """Create an S3PrefixDependency for the given prefix."""
        from doit.deps import S3PrefixDependency
        return S3PrefixDependency(
            self.bucket, resource_key,
            profile=self.profile, region=self.region
        )

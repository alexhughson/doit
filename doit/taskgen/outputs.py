"""Output classes for pattern-based task generation.

This module provides the Output base class and concrete implementations
for creating targets from rendered patterns.

Pattern syntax:
- <name> - Named placeholder (substituted with attribute value)

Example:
    output = FileOutput("build/<arch>/<module>.o")
    path, target = output.create({'arch': 'x86', 'module': 'main'})
    # path = "build/x86/main.o"
    # target = FileTarget("build/x86/main.o")
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, Tuple, Optional


@dataclass
class Output(ABC):
    """Base class for output patterns.

    Subclasses implement target creation for different resource types.
    Users can create custom subclasses for databases, APIs, etc.

    Attributes:
        pattern: Pattern string with <name> placeholders for attribute substitution
    """
    pattern: str

    def render(self, attrs: Dict[str, str]) -> str:
        """Render pattern with attribute substitution.

        Args:
            attrs: Dict mapping placeholder names to values

        Returns:
            The pattern with all <name> placeholders replaced
        """
        result = self.pattern
        for name, value in attrs.items():
            result = result.replace(f'<{name}>', value)
        return result

    @abstractmethod
    def create_target(self, rendered_path: str) -> Any:
        """Create a Target object for the rendered path.

        Args:
            rendered_path: The output path after attribute substitution

        Returns:
            A Target subclass instance (e.g., FileTarget, S3Target)
        """
        pass

    def create(self, attrs: Dict[str, str]) -> Tuple[str, Any]:
        """Render pattern and create target.

        Args:
            attrs: Dict mapping placeholder names to values

        Returns:
            Tuple of (rendered_path, target_object)
        """
        path = self.render(attrs)
        return path, self.create_target(path)


@dataclass
class FileOutput(Output):
    """Output pattern for local files.

    Example:
        FileOutput("build/<arch>/<module>.o")
        FileOutput("/output/processed/<doc>.txt")
    """

    def create_target(self, rendered_path: str) -> Any:
        """Create a FileTarget for the rendered path."""
        from doit.deps import FileTarget
        return FileTarget(rendered_path)


@dataclass
class S3Output(Output):
    """Output pattern for S3 objects.

    Example:
        S3Output("processed/<dataset>/<partition>.parquet",
                 bucket="my-bucket", profile="dev")
    """
    bucket: str = ""
    profile: Optional[str] = None
    region: Optional[str] = None

    def create_target(self, rendered_path: str) -> Any:
        """Create an S3Target for the rendered key."""
        from doit.deps import S3Target
        return S3Target(
            self.bucket, rendered_path,
            profile=self.profile, region=self.region
        )


@dataclass
class DirectoryOutput(Output):
    """Output pattern for a directory prefix.

    Instead of declaring a specific file output, this declares that the
    task outputs files to a directory. This enables prefix matching where
    downstream tasks that depend on files under this directory automatically
    depend on this task.

    This is useful when:
    - A task creates multiple files with unknown names
    - Files are named dynamically at runtime
    - You want downstream tasks to match by prefix

    Example:
        DirectoryOutput("build/<arch>/")
        DirectoryOutput("/output/<partition>/processed/")
    """

    def create_target(self, rendered_path: str) -> Any:
        """Create a DirectoryTarget for the rendered path."""
        from doit.deps import DirectoryTarget
        return DirectoryTarget(rendered_path)


@dataclass
class S3PrefixOutput(Output):
    """Output pattern for an S3 prefix (directory-like).

    Instead of declaring a specific S3 object output, this declares that
    the task outputs objects to a prefix. This enables prefix matching
    where downstream tasks that depend on objects under this prefix
    automatically depend on this task.

    Example:
        S3PrefixOutput("processed/<dataset>/", bucket="my-bucket")
    """
    bucket: str = ""
    profile: Optional[str] = None
    region: Optional[str] = None

    def create_target(self, rendered_path: str) -> Any:
        """Create an S3PrefixTarget for the rendered prefix."""
        from doit.deps import S3PrefixTarget
        return S3PrefixTarget(
            self.bucket, rendered_path,
            profile=self.profile, region=self.region
        )

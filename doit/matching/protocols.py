"""Protocols and enums for the matching system.

This module defines the core abstractions that Dependency and Target
classes must implement to participate in the matching engine.
"""

from enum import Enum, auto
from typing import Protocol, runtime_checkable


class MatchStrategy(Enum):
    """How a dependency/target should be matched.

    The matching engine uses this to route targets to the appropriate
    index for efficient lookup.
    """
    EXACT = auto()      # Key must match exactly (default for files, S3 objects)
    PREFIX = auto()     # Key is a prefix - matches anything under it (directories)
    CUSTOM = auto()     # Uses custom matches() method (advanced use cases)


@runtime_checkable
class Matchable(Protocol):
    """Protocol for objects that can be matched by the matching engine.

    Both Dependency and Target classes should implement this protocol
    to participate in the implicit dependency resolution system.
    """

    def get_key(self) -> str:
        """Return the key used for matching.

        Keys should be normalized, unique identifiers. Examples:
        - Files: absolute path ("/home/user/data.txt")
        - S3: URI format ("s3://bucket/key")
        - Directories: path with trailing slash ("/output/data/")
        """
        ...

    def get_match_strategy(self) -> MatchStrategy:
        """Return how this object should be matched.

        Returns:
            MatchStrategy indicating which index to use for matching.
        """
        ...

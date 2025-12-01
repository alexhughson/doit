"""Matching engine for dependency-to-target resolution.

This module provides efficient matching of dependencies to targets
using strategy-specific indexes:

- ExactIndex: O(1) for exact key matches (files, S3 objects)
- PrefixIndex: O(k) for prefix matches (directories, S3 prefixes)
- CustomIndex: O(n) for complex custom matching logic

Example:
    from doit.matching import MatchingEngine, MatchStrategy

    engine = MatchingEngine()
    engine.register_target(file_target, "compile")
    engine.register_target(dir_target, "generate")

    producer = engine.find_producer(dependency)
"""

from .protocols import MatchStrategy, Matchable
from .trie import PrefixTrie, TrieNode
from .indexes import ExactIndex, PrefixIndex, CustomIndex
from .engine import MatchingEngine

__all__ = [
    # Protocols and enums
    'MatchStrategy',
    'Matchable',
    # Data structures
    'PrefixTrie',
    'TrieNode',
    # Indexes
    'ExactIndex',
    'PrefixIndex',
    'CustomIndex',
    # Main engine
    'MatchingEngine',
]

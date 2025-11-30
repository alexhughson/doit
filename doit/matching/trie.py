"""Prefix trie data structure for efficient prefix matching.

This module provides a trie (prefix tree) optimized for path-like keys
with a configurable separator (default '/').
"""

from dataclasses import dataclass, field
from typing import Optional, List, TypeVar, Generic, Dict

T = TypeVar('T')


@dataclass
class TrieNode(Generic[T]):
    """Node in a prefix trie.

    Attributes:
        children: Child nodes keyed by path component.
        value: Associated value if this node is a terminal.
        is_terminal: Whether this node represents a complete prefix.
    """
    children: Dict[str, 'TrieNode[T]'] = field(default_factory=dict)
    value: Optional[T] = None
    is_terminal: bool = False


class PrefixTrie(Generic[T]):
    """Trie for efficient prefix matching.

    Supports:
    - Insert prefix with associated value
    - Find longest matching prefix for a key
    - O(k) lookup where k is the path depth

    Example:
        trie = PrefixTrie()
        trie.insert("/data/output/", "task_a")
        trie.insert("/data/", "task_b")

        # Find longest matching prefix
        trie.find_longest_prefix("/data/output/file.txt")  # Returns "task_a"
        trie.find_longest_prefix("/data/other/file.txt")   # Returns "task_b"
        trie.find_longest_prefix("/other/file.txt")        # Returns None
    """

    def __init__(self, separator: str = '/'):
        """Initialize trie with given path separator.

        Args:
            separator: Character used to split paths into components.
        """
        self._root: TrieNode[T] = TrieNode()
        self._separator = separator

    def insert(self, prefix: str, value: T) -> None:
        """Insert a prefix with associated value.

        Args:
            prefix: The prefix path to insert.
            value: Value to associate with this prefix.
        """
        parts = self._split(prefix)
        node = self._root
        for part in parts:
            if part not in node.children:
                node.children[part] = TrieNode()
            node = node.children[part]
        node.value = value
        node.is_terminal = True

    def find_longest_prefix(self, key: str) -> Optional[T]:
        """Find the longest registered prefix that matches key.

        Traverses the trie following the key's path components,
        tracking the most recent terminal node encountered.

        Args:
            key: The key to find a matching prefix for.

        Returns:
            Value associated with the longest matching prefix,
            or None if no prefix matches.

        Example:
            If "/data/output/" is registered and key is
            "/data/output/processed/file.txt", returns the value
            for "/data/output/".
        """
        parts = self._split(key)
        node = self._root
        result: Optional[T] = None

        # Check root terminal (handles empty prefix edge case)
        if node.is_terminal:
            result = node.value

        for part in parts:
            if part not in node.children:
                break
            node = node.children[part]
            if node.is_terminal:
                result = node.value

        return result

    def find_all_prefixes(self, key: str) -> List[T]:
        """Find all registered prefixes that match key.

        Unlike find_longest_prefix, this returns ALL prefixes
        that the key falls under, from shortest to longest.

        Args:
            key: The key to find matching prefixes for.

        Returns:
            List of values for all matching prefixes, ordered
            from shortest to longest prefix.
        """
        parts = self._split(key)
        node = self._root
        results: List[T] = []

        if node.is_terminal and node.value is not None:
            results.append(node.value)

        for part in parts:
            if part not in node.children:
                break
            node = node.children[part]
            if node.is_terminal and node.value is not None:
                results.append(node.value)

        return results

    def contains(self, prefix: str) -> bool:
        """Check if an exact prefix is registered.

        Args:
            prefix: The prefix to check.

        Returns:
            True if this exact prefix is registered.
        """
        parts = self._split(prefix)
        node = self._root
        for part in parts:
            if part not in node.children:
                return False
            node = node.children[part]
        return node.is_terminal

    def _split(self, path: str) -> List[str]:
        """Split path into components, filtering empty strings.

        Args:
            path: Path string to split.

        Returns:
            List of non-empty path components.
        """
        return [p for p in path.split(self._separator) if p]

"""Index implementations for different matching strategies.

Each index is optimized for a specific type of matching:
- ExactIndex: O(1) dictionary lookup for exact key matches
- PrefixIndex: O(k) trie lookup for prefix matches (k = path depth)
- CustomIndex: O(n) linear scan for custom matching logic
"""

from typing import Dict, List, Tuple, Optional, Any

from .trie import PrefixTrie


class ExactIndex:
    """O(1) exact key lookup using dictionary.

    Used for FileDependency, S3Dependency, and other exact-match resources.
    """

    def __init__(self):
        self._by_key: Dict[str, str] = {}  # key -> task_name

    def register(self, key: str, task_name: str) -> None:
        """Register a key with its producing task.

        Args:
            key: The exact key to register.
            task_name: Name of the task that produces this target.

        Raises:
            ValueError: If key is already registered.
        """
        if key in self._by_key:
            raise ValueError(f"Duplicate key: {key}")
        self._by_key[key] = task_name

    def find(self, key: str) -> Optional[str]:
        """Find task that produces this exact key.

        Args:
            key: The key to look up.

        Returns:
            Task name if found, None otherwise.
        """
        return self._by_key.get(key)

    def contains(self, key: str) -> bool:
        """Check if key is registered.

        Args:
            key: The key to check.

        Returns:
            True if key is registered.
        """
        return key in self._by_key

    def __len__(self) -> int:
        """Return number of registered keys."""
        return len(self._by_key)


class PrefixIndex:
    """O(k) prefix lookup using trie (k = path depth).

    Used for DirectoryTarget, S3PrefixTarget, and other prefix-match resources.
    """

    def __init__(self):
        self._trie: PrefixTrie[str] = PrefixTrie()
        self._prefixes: Dict[str, str] = {}  # For duplicate detection and iteration

    def register(self, prefix: str, task_name: str) -> None:
        """Register a prefix with its producing task.

        Args:
            prefix: The prefix to register. Will be normalized to ensure
                   trailing '/'.
            task_name: Name of the task that produces this target.

        Raises:
            ValueError: If prefix is already registered.
        """
        # Normalize: ensure trailing /
        normalized = prefix if prefix.endswith('/') else prefix + '/'

        if normalized in self._prefixes:
            raise ValueError(f"Duplicate prefix: {normalized}")

        self._prefixes[normalized] = task_name
        self._trie.insert(normalized, task_name)

    def find(self, key: str) -> Optional[str]:
        """Find task that produces a prefix containing this key.

        Looks up the longest registered prefix that the key falls under.

        Args:
            key: The key to find a prefix for.

        Returns:
            Task name of the longest matching prefix, or None.
        """
        return self._trie.find_longest_prefix(key)

    def find_all(self, key: str) -> List[str]:
        """Find all tasks with prefixes containing this key.

        Args:
            key: The key to find prefixes for.

        Returns:
            List of task names for all matching prefixes.
        """
        return self._trie.find_all_prefixes(key)

    def contains(self, prefix: str) -> bool:
        """Check if an exact prefix is registered.

        Args:
            prefix: The prefix to check.

        Returns:
            True if this exact prefix is registered.
        """
        normalized = prefix if prefix.endswith('/') else prefix + '/'
        return normalized in self._prefixes

    def __len__(self) -> int:
        """Return number of registered prefixes."""
        return len(self._prefixes)


class CustomIndex:
    """O(n) fallback for custom matching logic.

    Used for targets that need complex matching beyond exact/prefix.
    Linear scan through all registered targets, calling matches() on each.
    """

    def __init__(self):
        self._targets: List[Tuple[Any, str]] = []  # (target, task_name)

    def register(self, target: Any, task_name: str) -> None:
        """Register a target with its producing task.

        Args:
            target: The target object (must have matches() method).
            task_name: Name of the task that produces this target.
        """
        self._targets.append((target, task_name))

    def find(self, dep: Any) -> Optional[str]:
        """Find task that produces a target matching this dependency.

        Performs linear scan, calling target.matches(dep) on each
        registered target.

        Args:
            dep: The dependency to match.

        Returns:
            Task name of first matching target, or None.
        """
        for target, task_name in self._targets:
            if hasattr(target, 'matches') and target.matches(dep):
                return task_name
        return None

    def find_all(self, dep: Any) -> List[str]:
        """Find all tasks with targets matching this dependency.

        Args:
            dep: The dependency to match.

        Returns:
            List of task names for all matching targets.
        """
        results = []
        for target, task_name in self._targets:
            if hasattr(target, 'matches') and target.matches(dep):
                results.append(task_name)
        return results

    def __len__(self) -> int:
        """Return number of registered targets."""
        return len(self._targets)

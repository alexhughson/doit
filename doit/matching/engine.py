"""Central matching engine that coordinates dependency-to-target matching.

The MatchingEngine maintains separate indexes for different matching strategies
and queries them in order of efficiency (exact -> prefix -> custom).
"""

from typing import Optional, Dict, Any, List

from .indexes import ExactIndex, PrefixIndex, CustomIndex
from .protocols import MatchStrategy, Matchable


class MatchingEngine:
    """Central coordinator for dependency-to-target matching.

    Maintains separate indexes for different matching strategies
    and queries them in order of efficiency. Results are cached
    by dependency key.

    Example:
        engine = MatchingEngine()

        # Register targets
        engine.register_target(file_target, "compile")
        engine.register_target(dir_target, "generate")

        # Find producers
        producer = engine.find_producer(file_dependency)
    """

    def __init__(self):
        """Initialize engine with empty indexes."""
        self._exact = ExactIndex()
        self._prefix = PrefixIndex()
        self._custom = CustomIndex()
        self._cache: Dict[str, Optional[str]] = {}

    def register_target(self, target: Matchable, task_name: str) -> None:
        """Register a target with its producing task.

        Routes the target to the appropriate index based on its
        match strategy.

        Args:
            target: Target object implementing Matchable protocol.
            task_name: Name of the task that produces this target.

        Raises:
            ValueError: If target key/prefix is already registered.
        """
        strategy = target.get_match_strategy()
        key = target.get_key()

        if strategy == MatchStrategy.EXACT:
            self._exact.register(key, task_name)
        elif strategy == MatchStrategy.PREFIX:
            self._prefix.register(key, task_name)
        elif strategy == MatchStrategy.CUSTOM:
            self._custom.register(target, task_name)

        # Invalidate cache on registration
        self._cache.clear()

    def find_producer(self, dep: Matchable) -> Optional[str]:
        """Find the task that produces a target matching this dependency.

        Lookup order (by efficiency):
        1. Exact key match (O(1))
        2. Prefix match (O(k) where k=path depth)
        3. Custom match (O(n))

        Results are cached by dependency key.

        Args:
            dep: Dependency object implementing Matchable protocol.

        Returns:
            Name of the task that produces a matching target,
            or None if no match found.
        """
        key = dep.get_key()

        # Check cache first
        if key in self._cache:
            return self._cache[key]

        result: Optional[str] = None

        # Try exact match first (fastest - O(1))
        result = self._exact.find(key)

        # Try prefix match (fast - O(k))
        if result is None:
            result = self._prefix.find(key)

        # Fall back to custom matching (slow - O(n))
        if result is None:
            result = self._custom.find(dep)

        # Cache result
        self._cache[key] = result
        return result

    def find_all_producers(self, dep: Matchable) -> List[str]:
        """Find all tasks that produce targets matching this dependency.

        Unlike find_producer, this returns ALL matching tasks (useful
        for detecting conflicts or understanding dependencies).

        Args:
            dep: Dependency object implementing Matchable protocol.

        Returns:
            List of task names that produce matching targets.
        """
        key = dep.get_key()
        results: List[str] = []

        # Check exact match
        exact = self._exact.find(key)
        if exact:
            results.append(exact)

        # Check all prefix matches
        results.extend(self._prefix.find_all(key))

        # Check custom matches
        results.extend(self._custom.find_all(dep))

        return results

    def clear_cache(self) -> None:
        """Clear the match result cache.

        Call this if targets are modified after initial registration.
        """
        self._cache.clear()

    @property
    def exact_count(self) -> int:
        """Number of exact-match targets registered."""
        return len(self._exact)

    @property
    def prefix_count(self) -> int:
        """Number of prefix targets registered."""
        return len(self._prefix)

    @property
    def custom_count(self) -> int:
        """Number of custom-match targets registered."""
        return len(self._custom)

    @property
    def total_count(self) -> int:
        """Total number of targets registered."""
        return self.exact_count + self.prefix_count + self.custom_count

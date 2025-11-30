"""Tests for MatchingEngine."""

import pytest
from dataclasses import dataclass

from doit.matching import MatchingEngine, MatchStrategy


# Mock classes implementing Matchable protocol
@dataclass
class MockExactTarget:
    """Mock target with EXACT match strategy."""
    key: str

    def get_key(self) -> str:
        return self.key

    def get_match_strategy(self) -> MatchStrategy:
        return MatchStrategy.EXACT


@dataclass
class MockPrefixTarget:
    """Mock target with PREFIX match strategy."""
    prefix: str

    def get_key(self) -> str:
        return self.prefix

    def get_match_strategy(self) -> MatchStrategy:
        return MatchStrategy.PREFIX


@dataclass
class MockCustomTarget:
    """Mock target with CUSTOM match strategy."""
    pattern: str

    def get_key(self) -> str:
        return f"custom:{self.pattern}"

    def get_match_strategy(self) -> MatchStrategy:
        return MatchStrategy.CUSTOM

    def matches(self, dep) -> bool:
        return self.pattern in dep.get_key()


@dataclass
class MockDependency:
    """Mock dependency for testing."""
    key: str

    def get_key(self) -> str:
        return self.key

    def get_match_strategy(self) -> MatchStrategy:
        return MatchStrategy.EXACT


class TestMatchingEngineBasic:
    """Basic tests for MatchingEngine."""

    def test_empty_engine(self):
        """Test empty engine returns None."""
        engine = MatchingEngine()
        dep = MockDependency("/any/path")

        assert engine.find_producer(dep) is None
        assert engine.total_count == 0

    def test_exact_match(self):
        """Test exact key matching."""
        engine = MatchingEngine()
        target = MockExactTarget("/data/file.txt")
        engine.register_target(target, "task_a")

        dep = MockDependency("/data/file.txt")
        assert engine.find_producer(dep) == "task_a"

        dep2 = MockDependency("/data/other.txt")
        assert engine.find_producer(dep2) is None

    def test_prefix_match(self):
        """Test prefix matching."""
        engine = MatchingEngine()
        target = MockPrefixTarget("/data/output/")
        engine.register_target(target, "task_a")

        dep = MockDependency("/data/output/file.txt")
        assert engine.find_producer(dep) == "task_a"

        dep2 = MockDependency("/data/output/subdir/file.txt")
        assert engine.find_producer(dep2) == "task_a"

        dep3 = MockDependency("/data/other/file.txt")
        assert engine.find_producer(dep3) is None

    def test_custom_match(self):
        """Test custom matching."""
        engine = MatchingEngine()
        target = MockCustomTarget("important")
        engine.register_target(target, "task_a")

        dep = MockDependency("/data/important/file.txt")
        assert engine.find_producer(dep) == "task_a"

        dep2 = MockDependency("/data/other/file.txt")
        assert engine.find_producer(dep2) is None


class TestMatchingEnginePriority:
    """Tests for matching priority (exact > prefix > custom)."""

    def test_exact_before_prefix(self):
        """Test that exact match takes priority over prefix."""
        engine = MatchingEngine()

        # Register prefix first
        prefix_target = MockPrefixTarget("/data/")
        engine.register_target(prefix_target, "prefix_task")

        # Register exact match
        exact_target = MockExactTarget("/data/file.txt")
        engine.register_target(exact_target, "exact_task")

        # Exact match should win
        dep = MockDependency("/data/file.txt")
        assert engine.find_producer(dep) == "exact_task"

        # But prefix still works for other files
        dep2 = MockDependency("/data/other.txt")
        assert engine.find_producer(dep2) == "prefix_task"

    def test_prefix_before_custom(self):
        """Test that prefix match takes priority over custom."""
        engine = MatchingEngine()

        # Register custom first
        custom_target = MockCustomTarget("data")
        engine.register_target(custom_target, "custom_task")

        # Register prefix
        prefix_target = MockPrefixTarget("/data/output/")
        engine.register_target(prefix_target, "prefix_task")

        # Prefix should win for /data/output/* paths
        dep = MockDependency("/data/output/file.txt")
        assert engine.find_producer(dep) == "prefix_task"


class TestMatchingEngineMultiple:
    """Tests with multiple targets of different types."""

    def test_multiple_exact_targets(self):
        """Test multiple exact targets."""
        engine = MatchingEngine()
        engine.register_target(MockExactTarget("/a.txt"), "task_a")
        engine.register_target(MockExactTarget("/b.txt"), "task_b")
        engine.register_target(MockExactTarget("/c.txt"), "task_c")

        assert engine.find_producer(MockDependency("/a.txt")) == "task_a"
        assert engine.find_producer(MockDependency("/b.txt")) == "task_b"
        assert engine.find_producer(MockDependency("/c.txt")) == "task_c"
        assert engine.exact_count == 3

    def test_multiple_prefix_targets(self):
        """Test multiple prefix targets with longest match."""
        engine = MatchingEngine()
        engine.register_target(MockPrefixTarget("/data/"), "task_a")
        engine.register_target(MockPrefixTarget("/data/output/"), "task_b")
        engine.register_target(MockPrefixTarget("/other/"), "task_c")

        # Longest prefix should match
        assert engine.find_producer(MockDependency("/data/file.txt")) == "task_a"
        assert engine.find_producer(MockDependency("/data/output/file.txt")) == "task_b"
        assert engine.find_producer(MockDependency("/other/file.txt")) == "task_c"
        assert engine.prefix_count == 3

    def test_mixed_strategies(self):
        """Test all strategies together."""
        engine = MatchingEngine()
        engine.register_target(MockExactTarget("/exact.txt"), "exact_task")
        engine.register_target(MockPrefixTarget("/prefix/"), "prefix_task")
        engine.register_target(MockCustomTarget("custom"), "custom_task")

        assert engine.find_producer(MockDependency("/exact.txt")) == "exact_task"
        assert engine.find_producer(MockDependency("/prefix/file.txt")) == "prefix_task"
        assert engine.find_producer(MockDependency("/has_custom_word.txt")) == "custom_task"

        assert engine.total_count == 3
        assert engine.exact_count == 1
        assert engine.prefix_count == 1
        assert engine.custom_count == 1


class TestMatchingEngineCaching:
    """Tests for result caching."""

    def test_caching_works(self):
        """Test that results are cached."""
        engine = MatchingEngine()
        engine.register_target(MockExactTarget("/data/file.txt"), "task_a")

        dep = MockDependency("/data/file.txt")

        # First lookup
        result1 = engine.find_producer(dep)
        # Second lookup (should use cache)
        result2 = engine.find_producer(dep)

        assert result1 == result2 == "task_a"

    def test_cache_cleared_on_register(self):
        """Test that cache is cleared when new target registered."""
        engine = MatchingEngine()
        engine.register_target(MockPrefixTarget("/data/"), "task_a")

        dep = MockDependency("/data/output/file.txt")
        assert engine.find_producer(dep) == "task_a"

        # Register more specific prefix - cache should be cleared
        engine.register_target(MockPrefixTarget("/data/output/"), "task_b")

        # Should now return the more specific match
        assert engine.find_producer(dep) == "task_b"

    def test_manual_cache_clear(self):
        """Test manual cache clearing."""
        engine = MatchingEngine()
        engine.register_target(MockExactTarget("/data/file.txt"), "task_a")

        dep = MockDependency("/data/file.txt")
        engine.find_producer(dep)  # Populate cache

        engine.clear_cache()
        # Should still work after cache clear
        assert engine.find_producer(dep) == "task_a"


class TestMatchingEngineFindAll:
    """Tests for find_all_producers method."""

    def test_find_all_single_match(self):
        """Test find_all with single match."""
        engine = MatchingEngine()
        engine.register_target(MockExactTarget("/data/file.txt"), "task_a")

        dep = MockDependency("/data/file.txt")
        result = engine.find_all_producers(dep)
        assert result == ["task_a"]

    def test_find_all_multiple_prefixes(self):
        """Test find_all returns all matching prefixes."""
        engine = MatchingEngine()
        engine.register_target(MockPrefixTarget("/data/"), "task_a")
        engine.register_target(MockPrefixTarget("/data/output/"), "task_b")

        dep = MockDependency("/data/output/file.txt")
        result = engine.find_all_producers(dep)
        assert set(result) == {"task_a", "task_b"}

    def test_find_all_no_match(self):
        """Test find_all with no matches."""
        engine = MatchingEngine()
        engine.register_target(MockExactTarget("/data/file.txt"), "task_a")

        dep = MockDependency("/other/file.txt")
        result = engine.find_all_producers(dep)
        assert result == []


class TestMatchingEngineDuplicates:
    """Tests for duplicate handling."""

    def test_duplicate_exact_key_raises(self):
        """Test that duplicate exact key raises."""
        engine = MatchingEngine()
        engine.register_target(MockExactTarget("/data/file.txt"), "task_a")

        with pytest.raises(ValueError, match="Duplicate key"):
            engine.register_target(MockExactTarget("/data/file.txt"), "task_b")

    def test_duplicate_prefix_raises(self):
        """Test that duplicate prefix raises."""
        engine = MatchingEngine()
        engine.register_target(MockPrefixTarget("/data/output/"), "task_a")

        with pytest.raises(ValueError, match="Duplicate prefix"):
            engine.register_target(MockPrefixTarget("/data/output/"), "task_b")


class TestMatchingEngineRealWorldScenarios:
    """Real-world scenario tests."""

    def test_file_compilation_scenario(self):
        """Test typical file compilation scenario."""
        engine = MatchingEngine()

        # Compile tasks produce object files
        engine.register_target(MockExactTarget("/build/main.o"), "compile:main")
        engine.register_target(MockExactTarget("/build/utils.o"), "compile:utils")

        # Link task depends on object files
        assert engine.find_producer(MockDependency("/build/main.o")) == "compile:main"
        assert engine.find_producer(MockDependency("/build/utils.o")) == "compile:utils"

    def test_directory_generation_scenario(self):
        """Test directory generation scenario."""
        engine = MatchingEngine()

        # Generator task produces directory
        engine.register_target(MockPrefixTarget("/output/generated/"), "generate")

        # Consumer tasks depend on files under directory
        dep1 = MockDependency("/output/generated/doc1.txt")
        dep2 = MockDependency("/output/generated/subdir/doc2.txt")

        assert engine.find_producer(dep1) == "generate"
        assert engine.find_producer(dep2) == "generate"

    def test_nested_directory_scenario(self):
        """Test nested directory outputs."""
        engine = MatchingEngine()

        # Parent task outputs to /output/
        engine.register_target(MockPrefixTarget("/output/"), "parent")
        # Child task outputs to /output/processed/
        engine.register_target(MockPrefixTarget("/output/processed/"), "child")

        # File in /output/raw/ depends on parent
        assert engine.find_producer(MockDependency("/output/raw/file.txt")) == "parent"
        # File in /output/processed/ depends on child (more specific)
        assert engine.find_producer(MockDependency("/output/processed/file.txt")) == "child"

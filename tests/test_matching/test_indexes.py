"""Tests for index implementations."""

import pytest

from doit.matching.indexes import ExactIndex, PrefixIndex, CustomIndex


class TestExactIndex:
    """Tests for ExactIndex."""

    def test_empty_index(self):
        """Test empty index returns None."""
        index = ExactIndex()
        assert index.find("/any/path") is None
        assert len(index) == 0

    def test_register_and_find(self):
        """Test basic register and find."""
        index = ExactIndex()
        index.register("/data/file.txt", "task_a")

        assert index.find("/data/file.txt") == "task_a"
        assert index.find("/data/other.txt") is None

    def test_multiple_keys(self):
        """Test multiple key registration."""
        index = ExactIndex()
        index.register("/data/a.txt", "task_a")
        index.register("/data/b.txt", "task_b")

        assert index.find("/data/a.txt") == "task_a"
        assert index.find("/data/b.txt") == "task_b"
        assert len(index) == 2

    def test_duplicate_key_raises(self):
        """Test that duplicate key raises ValueError."""
        index = ExactIndex()
        index.register("/data/file.txt", "task_a")

        with pytest.raises(ValueError, match="Duplicate key"):
            index.register("/data/file.txt", "task_b")

    def test_contains(self):
        """Test contains method."""
        index = ExactIndex()
        index.register("/data/file.txt", "task_a")

        assert index.contains("/data/file.txt") is True
        assert index.contains("/data/other.txt") is False

    def test_s3_keys(self):
        """Test with S3 URI keys."""
        index = ExactIndex()
        index.register("s3://bucket/key.txt", "task_a")

        assert index.find("s3://bucket/key.txt") == "task_a"
        assert index.find("s3://bucket/other.txt") is None


class TestPrefixIndex:
    """Tests for PrefixIndex."""

    def test_empty_index(self):
        """Test empty index returns None."""
        index = PrefixIndex()
        assert index.find("/any/path") is None
        assert len(index) == 0

    def test_register_and_find(self):
        """Test basic prefix registration and lookup."""
        index = PrefixIndex()
        index.register("/data/output/", "task_a")

        assert index.find("/data/output/file.txt") == "task_a"
        assert index.find("/data/output/subdir/file.txt") == "task_a"
        assert index.find("/data/other/file.txt") is None

    def test_normalizes_trailing_slash(self):
        """Test that prefixes are normalized to have trailing slash."""
        index = PrefixIndex()
        index.register("/data/output", "task_a")  # No trailing slash

        assert index.find("/data/output/file.txt") == "task_a"
        assert index.contains("/data/output/") is True
        assert index.contains("/data/output") is True

    def test_longest_prefix_match(self):
        """Test that longest matching prefix is returned."""
        index = PrefixIndex()
        index.register("/data/", "task_a")
        index.register("/data/output/", "task_b")

        assert index.find("/data/file.txt") == "task_a"
        assert index.find("/data/output/file.txt") == "task_b"

    def test_duplicate_prefix_raises(self):
        """Test that duplicate prefix raises ValueError."""
        index = PrefixIndex()
        index.register("/data/output/", "task_a")

        with pytest.raises(ValueError, match="Duplicate prefix"):
            index.register("/data/output/", "task_b")

    def test_find_all(self):
        """Test find_all returns all matching prefixes."""
        index = PrefixIndex()
        index.register("/data/", "task_a")
        index.register("/data/output/", "task_b")

        result = index.find_all("/data/output/file.txt")
        assert set(result) == {"task_a", "task_b"}

    def test_contains(self):
        """Test contains method."""
        index = PrefixIndex()
        index.register("/data/output/", "task_a")

        assert index.contains("/data/output/") is True
        assert index.contains("/data/output") is True  # Normalized
        assert index.contains("/data/") is False


class TestCustomIndex:
    """Tests for CustomIndex."""

    def test_empty_index(self):
        """Test empty index returns None."""
        index = CustomIndex()
        assert index.find("any") is None
        assert len(index) == 0

    def test_register_and_find_with_matches(self):
        """Test with target that has matches() method."""
        class MockTarget:
            def __init__(self, pattern):
                self.pattern = pattern

            def matches(self, dep):
                return dep.startswith(self.pattern)

        index = CustomIndex()
        index.register(MockTarget("/data/"), "task_a")

        assert index.find("/data/file.txt") == "task_a"
        assert index.find("/other/file.txt") is None

    def test_first_match_wins(self):
        """Test that first matching target wins."""
        class MockTarget:
            def __init__(self, pattern, name):
                self.pattern = pattern
                self.name = name

            def matches(self, dep):
                return self.pattern in dep

        index = CustomIndex()
        index.register(MockTarget("data", "first"), "task_a")
        index.register(MockTarget("data", "second"), "task_b")

        # First registered target should match
        assert index.find("/data/file.txt") == "task_a"

    def test_find_all(self):
        """Test find_all returns all matches."""
        class MockTarget:
            def __init__(self, pattern):
                self.pattern = pattern

            def matches(self, dep):
                return self.pattern in dep

        index = CustomIndex()
        index.register(MockTarget("data"), "task_a")
        index.register(MockTarget("file"), "task_b")

        result = index.find_all("/data/file.txt")
        assert set(result) == {"task_a", "task_b"}

    def test_target_without_matches(self):
        """Test that target without matches() method is skipped."""
        class BadTarget:
            pass

        index = CustomIndex()
        index.register(BadTarget(), "task_a")

        # Should not raise, just returns None
        assert index.find("anything") is None

    def test_len(self):
        """Test length tracking."""
        class MockTarget:
            def matches(self, dep):
                return True

        index = CustomIndex()
        assert len(index) == 0

        index.register(MockTarget(), "task_a")
        assert len(index) == 1

        index.register(MockTarget(), "task_b")
        assert len(index) == 2

"""Tests for PrefixTrie data structure."""

import pytest

from doit.matching.trie import PrefixTrie, TrieNode


class TestTrieNode:
    """Tests for TrieNode dataclass."""

    def test_default_values(self):
        """Test default node initialization."""
        node = TrieNode()
        assert node.children == {}
        assert node.value is None
        assert node.is_terminal is False

    def test_with_value(self):
        """Test node with value."""
        node = TrieNode(value="task_a", is_terminal=True)
        assert node.value == "task_a"
        assert node.is_terminal is True


class TestPrefixTrieBasic:
    """Basic tests for PrefixTrie."""

    def test_empty_trie(self):
        """Test empty trie returns None."""
        trie = PrefixTrie()
        assert trie.find_longest_prefix("/any/path") is None

    def test_insert_and_find_single(self):
        """Test insert and find with single prefix."""
        trie = PrefixTrie()
        trie.insert("/data/output/", "task_a")

        assert trie.find_longest_prefix("/data/output/") == "task_a"
        assert trie.find_longest_prefix("/data/output/file.txt") == "task_a"
        assert trie.find_longest_prefix("/data/output/subdir/file.txt") == "task_a"

    def test_no_match(self):
        """Test when no prefix matches."""
        trie = PrefixTrie()
        trie.insert("/data/output/", "task_a")

        assert trie.find_longest_prefix("/other/path") is None
        assert trie.find_longest_prefix("/data/input/file.txt") is None

    def test_partial_prefix_no_match(self):
        """Test that partial path doesn't match."""
        trie = PrefixTrie()
        trie.insert("/data/output/processed/", "task_a")

        # /data/output/ is NOT registered, so this shouldn't match
        assert trie.find_longest_prefix("/data/output/file.txt") is None


class TestPrefixTrieLongestMatch:
    """Tests for longest prefix matching."""

    def test_longest_match_nested(self):
        """Test that longest matching prefix is returned."""
        trie = PrefixTrie()
        trie.insert("/data/", "task_a")
        trie.insert("/data/output/", "task_b")
        trie.insert("/data/output/processed/", "task_c")

        # Should match the longest registered prefix
        assert trie.find_longest_prefix("/data/file.txt") == "task_a"
        assert trie.find_longest_prefix("/data/output/file.txt") == "task_b"
        assert trie.find_longest_prefix("/data/output/processed/file.txt") == "task_c"
        assert trie.find_longest_prefix("/data/output/processed/deep/file.txt") == "task_c"

    def test_sibling_prefixes(self):
        """Test sibling prefixes don't interfere."""
        trie = PrefixTrie()
        trie.insert("/data/input/", "task_a")
        trie.insert("/data/output/", "task_b")

        assert trie.find_longest_prefix("/data/input/file.txt") == "task_a"
        assert trie.find_longest_prefix("/data/output/file.txt") == "task_b"
        assert trie.find_longest_prefix("/data/other/file.txt") is None


class TestPrefixTrieFindAll:
    """Tests for find_all_prefixes method."""

    def test_find_all_single(self):
        """Test find_all with single matching prefix."""
        trie = PrefixTrie()
        trie.insert("/data/output/", "task_a")

        result = trie.find_all_prefixes("/data/output/file.txt")
        assert result == ["task_a"]

    def test_find_all_nested(self):
        """Test find_all returns all matching prefixes."""
        trie = PrefixTrie()
        trie.insert("/data/", "task_a")
        trie.insert("/data/output/", "task_b")
        trie.insert("/data/output/processed/", "task_c")

        result = trie.find_all_prefixes("/data/output/processed/file.txt")
        assert result == ["task_a", "task_b", "task_c"]

    def test_find_all_partial_match(self):
        """Test find_all with partial path."""
        trie = PrefixTrie()
        trie.insert("/data/", "task_a")
        trie.insert("/data/output/processed/", "task_b")

        # Only /data/ matches, not /data/output/processed/
        result = trie.find_all_prefixes("/data/output/file.txt")
        assert result == ["task_a"]


class TestPrefixTrieContains:
    """Tests for contains method."""

    def test_contains_exact(self):
        """Test contains with exact prefix."""
        trie = PrefixTrie()
        trie.insert("/data/output/", "task_a")

        assert trie.contains("/data/output/") is True
        assert trie.contains("/data/output") is True  # Without trailing /
        assert trie.contains("/data/") is False
        assert trie.contains("/data/output/subdir/") is False


class TestPrefixTrieEdgeCases:
    """Edge case tests for PrefixTrie."""

    def test_empty_string(self):
        """Test with empty string."""
        trie = PrefixTrie()
        trie.insert("", "root_task")

        # Empty prefix matches everything
        assert trie.find_longest_prefix("/any/path") == "root_task"

    def test_single_component(self):
        """Test with single path component."""
        trie = PrefixTrie()
        trie.insert("/data/", "task_a")

        assert trie.find_longest_prefix("/data/") == "task_a"
        assert trie.find_longest_prefix("/data/file") == "task_a"

    def test_deep_path(self):
        """Test with deeply nested path."""
        trie = PrefixTrie()
        trie.insert("/a/b/c/d/e/f/", "deep_task")

        assert trie.find_longest_prefix("/a/b/c/d/e/f/file.txt") == "deep_task"
        assert trie.find_longest_prefix("/a/b/c/d/e/file.txt") is None

    def test_custom_separator(self):
        """Test with custom separator."""
        trie = PrefixTrie(separator='.')
        trie.insert("com.example.app.", "task_a")

        assert trie.find_longest_prefix("com.example.app.Main") == "task_a"
        assert trie.find_longest_prefix("com.example.other.Main") is None

    def test_multiple_inserts_same_prefix(self):
        """Test that later insert overwrites value."""
        trie = PrefixTrie()
        trie.insert("/data/", "task_a")
        trie.insert("/data/", "task_b")  # Overwrite

        assert trie.find_longest_prefix("/data/file.txt") == "task_b"

    def test_s3_like_paths(self):
        """Test with S3-like path format."""
        trie = PrefixTrie()
        trie.insert("s3://bucket/prefix/", "task_a")

        assert trie.find_longest_prefix("s3://bucket/prefix/key.txt") == "task_a"
        assert trie.find_longest_prefix("s3://bucket/other/key.txt") is None

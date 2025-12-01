"""Tests for Directory and S3Prefix dependency/target classes."""

import pytest
from pathlib import Path

from doit.deps import (
    DirectoryDependency, DirectoryTarget,
    S3PrefixDependency, S3PrefixTarget,
    FileDependency, FileTarget,
    CheckStatus,
)
from doit.matching import MatchStrategy


class TestDirectoryDependency:
    """Tests for DirectoryDependency."""

    def test_get_key_normalizes_trailing_slash(self, tmp_path):
        """Test that get_key adds trailing slash."""
        dep = DirectoryDependency(str(tmp_path / "data"))
        key = dep.get_key()
        assert key.endswith('/')
        assert "data/" in key

    def test_get_key_preserves_trailing_slash(self, tmp_path):
        """Test that get_key preserves existing trailing slash."""
        dep = DirectoryDependency(str(tmp_path / "data") + "/")
        key = dep.get_key()
        assert key.endswith('/')
        # Should not have double slash
        assert not key.endswith('//')

    def test_get_match_strategy(self, tmp_path):
        """Test that match strategy is PREFIX."""
        dep = DirectoryDependency(tmp_path)
        assert dep.get_match_strategy() == MatchStrategy.PREFIX

    def test_exists_true(self, tmp_path):
        """Test exists returns True for existing directory."""
        dep = DirectoryDependency(tmp_path)
        assert dep.exists() is True

    def test_exists_false(self, tmp_path):
        """Test exists returns False for non-existent directory."""
        dep = DirectoryDependency(tmp_path / "nonexistent")
        assert dep.exists() is False

    def test_check_status_always_changed(self, tmp_path):
        """Test that check_status always returns CHANGED."""
        dep = DirectoryDependency(tmp_path)
        result = dep.check_status(None)
        assert result.status == CheckStatus.CHANGED
        assert "always triggers" in result.reason

    def test_accepts_path_object(self, tmp_path):
        """Test that Path objects are accepted."""
        dep = DirectoryDependency(tmp_path)
        assert dep.exists() is True


class TestDirectoryTarget:
    """Tests for DirectoryTarget."""

    def test_get_key_normalizes_trailing_slash(self, tmp_path):
        """Test that get_key adds trailing slash."""
        target = DirectoryTarget(str(tmp_path / "output"))
        key = target.get_key()
        assert key.endswith('/')

    def test_get_match_strategy(self, tmp_path):
        """Test that match strategy is PREFIX."""
        target = DirectoryTarget(tmp_path)
        assert target.get_match_strategy() == MatchStrategy.PREFIX

    def test_exists_true(self, tmp_path):
        """Test exists returns True for existing directory."""
        target = DirectoryTarget(tmp_path)
        assert target.exists() is True

    def test_exists_false(self, tmp_path):
        """Test exists returns False for non-existent directory."""
        target = DirectoryTarget(tmp_path / "nonexistent")
        assert target.exists() is False


class TestS3PrefixDependency:
    """Tests for S3PrefixDependency."""

    def test_get_key_format(self):
        """Test key format is s3://bucket/prefix/."""
        dep = S3PrefixDependency("my-bucket", "data/processed")
        assert dep.get_key() == "s3://my-bucket/data/processed/"

    def test_get_key_preserves_trailing_slash(self):
        """Test that trailing slash is preserved."""
        dep = S3PrefixDependency("my-bucket", "data/processed/")
        assert dep.get_key() == "s3://my-bucket/data/processed/"

    def test_get_match_strategy(self):
        """Test that match strategy is PREFIX."""
        dep = S3PrefixDependency("bucket", "prefix")
        assert dep.get_match_strategy() == MatchStrategy.PREFIX

    def test_exists_always_true(self):
        """Test that S3 prefixes always exist (virtual concept)."""
        dep = S3PrefixDependency("bucket", "any/prefix")
        assert dep.exists() is True

    def test_check_status_always_changed(self):
        """Test that check_status always returns CHANGED."""
        dep = S3PrefixDependency("bucket", "prefix")
        result = dep.check_status(None)
        assert result.status == CheckStatus.CHANGED

    def test_optional_credentials(self):
        """Test that profile and region are optional."""
        dep = S3PrefixDependency("bucket", "prefix", profile="dev", region="us-west-2")
        assert dep.profile == "dev"
        assert dep.region == "us-west-2"


class TestS3PrefixTarget:
    """Tests for S3PrefixTarget."""

    def test_get_key_format(self):
        """Test key format is s3://bucket/prefix/."""
        target = S3PrefixTarget("my-bucket", "output/generated")
        assert target.get_key() == "s3://my-bucket/output/generated/"

    def test_get_match_strategy(self):
        """Test that match strategy is PREFIX."""
        target = S3PrefixTarget("bucket", "prefix")
        assert target.get_match_strategy() == MatchStrategy.PREFIX

    def test_exists_always_true(self):
        """Test that S3 prefixes always exist."""
        target = S3PrefixTarget("bucket", "prefix")
        assert target.exists() is True


class TestExactClassesMatchStrategy:
    """Test that existing classes return EXACT strategy."""

    def test_file_dependency_is_exact(self, tmp_path):
        """Test FileDependency returns EXACT strategy."""
        f = tmp_path / "test.txt"
        f.write_text("test")
        dep = FileDependency(str(f))
        assert dep.get_match_strategy() == MatchStrategy.EXACT

    def test_file_target_is_exact(self, tmp_path):
        """Test FileTarget returns EXACT strategy."""
        target = FileTarget(str(tmp_path / "test.txt"))
        assert target.get_match_strategy() == MatchStrategy.EXACT


class TestPrefixMatchingIntegration:
    """Integration tests for prefix matching with MatchingEngine."""

    def test_directory_target_matches_file_under_it(self, tmp_path):
        """Test that file dependency under directory target matches."""
        from doit.matching import MatchingEngine

        engine = MatchingEngine()

        # Register directory target
        target = DirectoryTarget(tmp_path / "output")
        engine.register_target(target, "generate")

        # File dependency under that directory should match
        f = tmp_path / "output" / "file.txt"
        dep = FileDependency(str(f))

        assert engine.find_producer(dep) == "generate"

    def test_directory_target_matches_subdir_dependency(self, tmp_path):
        """Test that directory dependency under target matches."""
        from doit.matching import MatchingEngine

        engine = MatchingEngine()

        # Register parent directory target
        target = DirectoryTarget(tmp_path / "output")
        engine.register_target(target, "generate")

        # Subdirectory dependency should match
        dep = DirectoryDependency(tmp_path / "output" / "processed")

        assert engine.find_producer(dep) == "generate"

    def test_directory_target_no_match_sibling(self, tmp_path):
        """Test that sibling directory doesn't match."""
        from doit.matching import MatchingEngine

        engine = MatchingEngine()

        target = DirectoryTarget(tmp_path / "output")
        engine.register_target(target, "generate")

        # Sibling directory should not match
        dep = DirectoryDependency(tmp_path / "input")

        assert engine.find_producer(dep) is None

    def test_nested_directory_targets(self, tmp_path):
        """Test longest prefix match with nested directories."""
        from doit.matching import MatchingEngine

        engine = MatchingEngine()

        # Register parent and child directory targets
        engine.register_target(DirectoryTarget(tmp_path / "output"), "parent")
        engine.register_target(DirectoryTarget(tmp_path / "output" / "processed"), "child")

        # File in /output/raw/ should match parent
        dep1 = FileDependency(str(tmp_path / "output" / "raw" / "file.txt"))
        assert engine.find_producer(dep1) == "parent"

        # File in /output/processed/ should match child (more specific)
        dep2 = FileDependency(str(tmp_path / "output" / "processed" / "file.txt"))
        assert engine.find_producer(dep2) == "child"

    def test_s3_prefix_matching(self):
        """Test S3 prefix matching."""
        from doit.matching import MatchingEngine
        from doit.deps import S3Dependency

        engine = MatchingEngine()

        # Register S3 prefix target
        target = S3PrefixTarget("bucket", "output/data/")
        engine.register_target(target, "s3_generate")

        # S3 dependency under that prefix should match
        dep = S3Dependency("bucket", "output/data/file.parquet")

        assert engine.find_producer(dep) == "s3_generate"

    def test_s3_prefix_no_match_different_bucket(self):
        """Test that different bucket doesn't match."""
        from doit.matching import MatchingEngine
        from doit.deps import S3Dependency

        engine = MatchingEngine()

        target = S3PrefixTarget("bucket-a", "output/")
        engine.register_target(target, "task_a")

        # Different bucket should not match (despite same key prefix)
        dep = S3Dependency("bucket-b", "output/file.txt")

        # Key format is s3://bucket/key, so different buckets = different prefix
        assert engine.find_producer(dep) is None

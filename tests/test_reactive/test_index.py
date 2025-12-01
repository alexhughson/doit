"""Tests for OutputPatternIndex."""

import pytest
from unittest.mock import MagicMock

from doit.reactive.index import OutputPatternIndex


class TestPrefixExtraction:
    """Tests for _extract_static_prefix method."""

    def test_simple_pattern(self):
        """Test prefix extraction from simple pattern."""
        index = OutputPatternIndex()
        assert index._extract_static_prefix("processed/<doc>.json") == "processed/"

    def test_nested_pattern(self):
        """Test prefix extraction from nested pattern."""
        index = OutputPatternIndex()
        assert index._extract_static_prefix("data/<year>/<month>/file.csv") == "data/"

    def test_no_placeholder(self):
        """Test pattern without placeholders."""
        index = OutputPatternIndex()
        assert index._extract_static_prefix("data/fixed/file.txt") == "data/fixed/"

    def test_placeholder_at_start(self):
        """Test pattern with placeholder at start."""
        index = OutputPatternIndex()
        assert index._extract_static_prefix("<name>.txt") == ""

    def test_deep_path_with_placeholder(self):
        """Test deep path before placeholder."""
        index = OutputPatternIndex()
        assert index._extract_static_prefix("a/b/c/<doc>.txt") == "a/b/c/"

    def test_multiple_placeholders(self):
        """Test pattern with multiple placeholders."""
        index = OutputPatternIndex()
        # Should stop at first placeholder
        assert index._extract_static_prefix("data/<year>/<month>/<day>.csv") == "data/"


class TestGeneratorRegistration:
    """Tests for registering generators."""

    def test_register_single_generator(self):
        """Test registering a single generator."""
        index = OutputPatternIndex()

        gen = MagicMock()
        gen.inputs = {"data": MagicMock(pattern="processed/<doc>.json")}

        index.register_generator(gen)

        assert index.generator_count == 1
        assert index.prefix_count == 1

    def test_register_multiple_generators(self):
        """Test registering multiple generators."""
        index = OutputPatternIndex()

        gen1 = MagicMock()
        gen1.inputs = {"data": MagicMock(pattern="processed/<doc>.json")}

        gen2 = MagicMock()
        gen2.inputs = {"raw": MagicMock(pattern="raw/<file>.txt")}

        index.register_generators([gen1, gen2])

        assert index.generator_count == 2
        assert index.prefix_count == 2

    def test_register_generator_multiple_inputs(self):
        """Test generator with multiple input patterns."""
        index = OutputPatternIndex()

        gen = MagicMock()
        gen.inputs = {
            "data": MagicMock(pattern="processed/<doc>.json"),
            "config": MagicMock(pattern="config/<env>.yaml"),
        }

        index.register_generator(gen)

        assert index.generator_count == 1
        # Two different prefixes
        assert index.prefix_count == 2

    def test_register_generators_same_prefix(self):
        """Test generators sharing the same prefix."""
        index = OutputPatternIndex()

        gen1 = MagicMock()
        gen1.inputs = {"data": MagicMock(pattern="processed/<a>.json")}

        gen2 = MagicMock()
        gen2.inputs = {"data": MagicMock(pattern="processed/<b>.csv")}

        index.register_generators([gen1, gen2])

        assert index.generator_count == 2
        # Same prefix, both generators registered under it
        assert index.prefix_count == 1


class TestFindAffectedGenerators:
    """Tests for finding affected generators."""

    def test_output_matches_prefix(self):
        """Test finding generators when output matches prefix."""
        index = OutputPatternIndex()

        gen = MagicMock()
        gen.inputs = {"data": MagicMock(pattern="processed/<doc>.json")}

        index.register_generator(gen)

        affected = index.find_affected_generators(["processed/report.json"])
        assert gen in affected

    def test_output_no_match(self):
        """Test no match when output doesn't match any prefix."""
        index = OutputPatternIndex()

        gen = MagicMock()
        gen.inputs = {"data": MagicMock(pattern="processed/<doc>.json")}

        index.register_generator(gen)

        affected = index.find_affected_generators(["other/file.txt"])
        assert len(affected) == 0

    def test_multiple_outputs_multiple_matches(self):
        """Test multiple outputs matching different generators."""
        index = OutputPatternIndex()

        gen1 = MagicMock()
        gen1.inputs = {"data": MagicMock(pattern="processed/<doc>.json")}

        gen2 = MagicMock()
        gen2.inputs = {"raw": MagicMock(pattern="raw/<file>.txt")}

        index.register_generators([gen1, gen2])

        affected = index.find_affected_generators([
            "processed/report.json",
            "raw/data.txt"
        ])

        assert gen1 in affected
        assert gen2 in affected

    def test_subdirectory_matches(self):
        """Test that subdirectory outputs match parent prefix."""
        index = OutputPatternIndex()

        gen = MagicMock()
        gen.inputs = {"data": MagicMock(pattern="output/<type>/<doc>.json")}

        index.register_generator(gen)

        # Deep path should still match the "output/" prefix
        affected = index.find_affected_generators([
            "output/reports/2024/monthly.json"
        ])
        assert gen in affected

    def test_empty_outputs(self):
        """Test with empty outputs list."""
        index = OutputPatternIndex()

        gen = MagicMock()
        gen.inputs = {"data": MagicMock(pattern="processed/<doc>.json")}

        index.register_generator(gen)

        affected = index.find_affected_generators([])
        assert len(affected) == 0


class TestPathNormalization:
    """Tests for path normalization."""

    def test_trailing_slash_removed(self):
        """Test that trailing slashes are normalized."""
        index = OutputPatternIndex()

        gen = MagicMock()
        gen.inputs = {"data": MagicMock(pattern="output/<doc>.json")}

        index.register_generator(gen)

        # Both with and without trailing slash should work
        affected1 = index.find_affected_generators(["output/doc.json"])
        affected2 = index.find_affected_generators(["output/doc.json/"])

        assert gen in affected1
        assert gen in affected2

    def test_s3_uri_handling(self):
        """Test S3 URI normalization."""
        index = OutputPatternIndex()

        normalized = index._normalize_path("s3://bucket/path/to/file")
        assert normalized == "s3://bucket/path/to/file"

        normalized = index._normalize_path("s3://bucket/path/")
        assert normalized == "s3://bucket/path"


class TestClear:
    """Tests for clearing the index."""

    def test_clear_removes_all(self):
        """Test that clear removes all generators and prefixes."""
        index = OutputPatternIndex()

        gen = MagicMock()
        gen.inputs = {"data": MagicMock(pattern="processed/<doc>.json")}

        index.register_generator(gen)
        assert index.generator_count == 1

        index.clear()

        assert index.generator_count == 0
        assert index.prefix_count == 0

    def test_clear_then_register(self):
        """Test that we can register after clearing."""
        index = OutputPatternIndex()

        gen1 = MagicMock()
        gen1.inputs = {"data": MagicMock(pattern="processed/<doc>.json")}
        index.register_generator(gen1)

        index.clear()

        gen2 = MagicMock()
        gen2.inputs = {"raw": MagicMock(pattern="raw/<file>.txt")}
        index.register_generator(gen2)

        assert index.generator_count == 1
        assert gen2 in index.find_affected_generators(["raw/data.txt"])

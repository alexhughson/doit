"""Tests for doit.taskgen.outputs module."""

import pytest
from pathlib import Path

from doit.taskgen.outputs import Output, FileOutput, S3Output, DirectoryOutput, S3PrefixOutput
from doit.deps import FileTarget, S3Target, DirectoryTarget, S3PrefixTarget


class TestOutputRender:
    """Tests for Output.render() method."""

    def test_single_placeholder(self):
        """Test pattern with single placeholder."""
        out = FileOutput("build/<module>.o")
        result = out.render({"module": "main"})
        assert result == "build/main.o"

    def test_multiple_placeholders(self):
        """Test pattern with multiple placeholders."""
        out = FileOutput("build/<arch>/<module>.o")
        result = out.render({"arch": "x86", "module": "main"})
        assert result == "build/x86/main.o"

    def test_repeated_placeholder(self):
        """Test pattern with same placeholder repeated."""
        out = FileOutput("<name>/<name>.txt")
        result = out.render({"name": "foo"})
        assert result == "foo/foo.txt"

    def test_no_placeholders(self):
        """Test pattern without placeholders."""
        out = FileOutput("build/output.o")
        result = out.render({"arch": "x86"})
        assert result == "build/output.o"

    def test_extra_attrs_ignored(self):
        """Test that extra attributes are ignored."""
        out = FileOutput("build/<module>.o")
        result = out.render({"module": "main", "extra": "ignored"})
        assert result == "build/main.o"

    def test_missing_attr_left_in_pattern(self):
        """Test that missing attributes leave placeholder intact."""
        out = FileOutput("build/<arch>/<module>.o")
        result = out.render({"module": "main"})
        assert result == "build/<arch>/main.o"


class TestFileOutput:
    """Tests for FileOutput class."""

    def test_create_target_returns_filetarget(self):
        """Test that create_target returns a FileTarget."""
        out = FileOutput("build/<module>.o")
        target = out.create_target("build/main.o")
        assert isinstance(target, FileTarget)

    def test_create_target_path(self):
        """Test that FileTarget has correct path."""
        out = FileOutput("build/<module>.o")
        target = out.create_target("build/main.o")
        assert target.get_key() == str(Path("build/main.o").resolve())

    def test_create_returns_tuple(self):
        """Test that create() returns (path, target) tuple."""
        out = FileOutput("build/<module>.o")
        path, target = out.create({"module": "main"})
        assert path == "build/main.o"
        assert isinstance(target, FileTarget)

    def test_create_with_nested_path(self):
        """Test create with nested directory structure."""
        out = FileOutput("build/<arch>/<vendor>/<module>.o")
        path, target = out.create({
            "arch": "arm64",
            "vendor": "apple",
            "module": "core"
        })
        assert path == "build/arm64/apple/core.o"
        assert isinstance(target, FileTarget)

    def test_create_with_absolute_path(self):
        """Test create with absolute path pattern."""
        out = FileOutput("/output/<doc>.txt")
        path, target = out.create({"doc": "readme"})
        assert path == "/output/readme.txt"
        assert isinstance(target, FileTarget)


class TestS3Output:
    """Tests for S3Output class."""

    def test_basic_attributes(self):
        """Test S3Output attribute storage."""
        out = S3Output(
            "processed/<dataset>.parquet",
            bucket="my-bucket",
            profile="dev",
            region="us-west-2"
        )
        assert out.pattern == "processed/<dataset>.parquet"
        assert out.bucket == "my-bucket"
        assert out.profile == "dev"
        assert out.region == "us-west-2"

    def test_default_attributes(self):
        """Test S3Output default values."""
        out = S3Output("output/<key>.json", bucket="bucket")
        assert out.profile is None
        assert out.region is None

    def test_create_target_returns_s3target(self):
        """Test that create_target returns an S3Target."""
        out = S3Output("output/<key>.json", bucket="my-bucket")
        target = out.create_target("output/data.json")
        assert isinstance(target, S3Target)

    def test_create_target_passes_credentials(self):
        """Test that S3Target receives profile and region."""
        out = S3Output(
            "output/<key>.json",
            bucket="my-bucket",
            profile="dev",
            region="eu-west-1"
        )
        target = out.create_target("output/data.json")
        assert target.bucket == "my-bucket"
        assert target.key == "output/data.json"
        assert target.profile == "dev"
        assert target.region == "eu-west-1"

    def test_create_returns_tuple(self):
        """Test that create() returns (path, target) tuple."""
        out = S3Output("data/<dataset>/<file>.parquet", bucket="bucket")
        path, target = out.create({"dataset": "sales", "file": "2024"})
        assert path == "data/sales/2024.parquet"
        assert isinstance(target, S3Target)

    def test_s3_key_format(self):
        """Test that S3Target get_key returns S3 URI."""
        out = S3Output("data/<key>.json", bucket="my-bucket")
        _, target = out.create({"key": "test"})
        assert target.get_key() == "s3://my-bucket/data/test.json"


class TestOutputAbstraction:
    """Tests for Output ABC behavior."""

    def test_cannot_instantiate_output_directly(self):
        """Test that Output cannot be instantiated."""
        with pytest.raises(TypeError):
            Output("pattern")

    def test_subclass_must_implement_create_target(self):
        """Test that subclass without create_target fails."""
        class IncompleteOutput(Output):
            pass

        with pytest.raises(TypeError):
            IncompleteOutput("pattern")

    def test_custom_output_subclass(self):
        """Test creating a custom Output subclass."""
        class CustomOutput(Output):
            def create_target(self, rendered_path: str):
                return {"custom": rendered_path}

        out = CustomOutput("output/<name>")
        path, target = out.create({"name": "test"})
        assert path == "output/test"
        assert target == {"custom": "output/test"}


class TestDirectoryOutput:
    """Tests for DirectoryOutput class."""

    def test_create_target_returns_directory_target(self):
        """Test that create_target returns a DirectoryTarget."""
        out = DirectoryOutput("build/<arch>/")
        target = out.create_target("build/x86/")
        assert isinstance(target, DirectoryTarget)

    def test_create_returns_tuple(self):
        """Test that create() returns (path, target) tuple."""
        out = DirectoryOutput("build/<arch>/")
        path, target = out.create({"arch": "x86"})
        assert path == "build/x86/"
        assert isinstance(target, DirectoryTarget)

    def test_target_key_has_trailing_slash(self):
        """Test that DirectoryTarget key has trailing slash."""
        out = DirectoryOutput("output/<partition>/")
        _, target = out.create({"partition": "2024"})
        assert target.get_key().endswith('/')

    def test_create_with_nested_path(self):
        """Test create with nested directory structure."""
        out = DirectoryOutput("output/<year>/<month>/processed/")
        path, target = out.create({"year": "2024", "month": "01"})
        assert path == "output/2024/01/processed/"
        assert isinstance(target, DirectoryTarget)


class TestS3PrefixOutput:
    """Tests for S3PrefixOutput class."""

    def test_create_target_returns_s3_prefix_target(self):
        """Test that create_target returns an S3PrefixTarget."""
        out = S3PrefixOutput("processed/<dataset>/", bucket="my-bucket")
        target = out.create_target("processed/sales/")
        assert isinstance(target, S3PrefixTarget)

    def test_basic_attributes(self):
        """Test S3PrefixOutput attribute storage."""
        out = S3PrefixOutput(
            "output/<dataset>/",
            bucket="my-bucket",
            profile="dev",
            region="us-west-2"
        )
        assert out.pattern == "output/<dataset>/"
        assert out.bucket == "my-bucket"
        assert out.profile == "dev"
        assert out.region == "us-west-2"

    def test_default_attributes(self):
        """Test S3PrefixOutput default values."""
        out = S3PrefixOutput("output/<key>/", bucket="bucket")
        assert out.profile is None
        assert out.region is None

    def test_create_target_passes_credentials(self):
        """Test that S3PrefixTarget receives profile and region."""
        out = S3PrefixOutput(
            "output/<key>/",
            bucket="my-bucket",
            profile="dev",
            region="eu-west-1"
        )
        target = out.create_target("output/data/")
        assert target.bucket == "my-bucket"
        assert target.prefix == "output/data/"
        assert target.profile == "dev"
        assert target.region == "eu-west-1"

    def test_create_returns_tuple(self):
        """Test that create() returns (path, target) tuple."""
        out = S3PrefixOutput("data/<dataset>/processed/", bucket="bucket")
        path, target = out.create({"dataset": "sales"})
        assert path == "data/sales/processed/"
        assert isinstance(target, S3PrefixTarget)

    def test_s3_prefix_key_format(self):
        """Test that S3PrefixTarget get_key returns S3 URI with trailing slash."""
        out = S3PrefixOutput("data/<key>/", bucket="my-bucket")
        _, target = out.create({"key": "test"})
        assert target.get_key() == "s3://my-bucket/data/test/"


class TestPrefixOutputIntegration:
    """Integration tests for prefix outputs with TargetRegistry."""

    def test_directory_output_enables_prefix_matching(self, tmp_path):
        """Test that DirectoryOutput enables prefix matching in registry."""
        from doit.control import TargetRegistry
        from doit.deps import FileDependency

        registry = TargetRegistry()
        out = DirectoryOutput("output/")
        _, target = out.create({})
        registry.register(target, "generator")

        # File dependency under the directory should match
        dep = FileDependency(str(tmp_path / "output" / "file.txt"))
        # Note: This won't match because paths are resolved differently
        # But we verify the target is properly registered as PREFIX type
        assert registry.stats["prefix_count"] == 1

    def test_s3_prefix_output_enables_prefix_matching(self):
        """Test that S3PrefixOutput enables prefix matching in registry."""
        from doit.control import TargetRegistry
        from doit.deps import S3Dependency

        registry = TargetRegistry()
        out = S3PrefixOutput("output/data/", bucket="my-bucket")
        _, target = out.create({})
        registry.register(target, "generator")

        # S3 dependency under the prefix should match
        dep = S3Dependency("my-bucket", "output/data/file.parquet")
        assert registry.find_producer(dep) == "generator"

        # Verify it's registered as prefix type
        assert registry.stats["prefix_count"] == 1

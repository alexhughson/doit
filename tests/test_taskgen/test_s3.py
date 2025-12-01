"""Tests for S3Input and S3Output with moto mocking."""

import pytest

# Skip all tests if boto3/moto not available
boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from moto import mock_aws

from doit.taskgen.inputs import S3Input
from doit.taskgen.outputs import S3Output
from doit.taskgen.generator import TaskGenerator
from doit.taskgen.groups import build_input_sets
from doit.deps import S3Dependency, S3Target


@pytest.fixture
def s3_bucket():
    """Create a mocked S3 bucket for testing."""
    with mock_aws():
        client = boto3.client('s3', region_name='us-east-1')
        client.create_bucket(Bucket='test-bucket')
        yield client


class TestS3InputBasic:
    """Basic tests for S3Input."""

    def test_pattern_compilation(self):
        """Test that pattern compiles correctly."""
        inp = S3Input("data/<dataset>/<partition>.parquet", bucket="bucket")
        assert inp._glob_pattern == "data/*/*.parquet"
        assert inp.capture_names == ["dataset", "partition"]

    def test_bucket_and_credentials(self):
        """Test that bucket and credentials are stored."""
        inp = S3Input(
            "data/<key>.json",
            bucket="my-bucket",
            profile="dev",
            region="us-west-2"
        )
        assert inp.bucket == "my-bucket"
        assert inp.profile == "dev"
        assert inp.region == "us-west-2"


class TestS3InputListResources:
    """Tests for S3Input.list_resources()."""

    def test_list_single_object(self, s3_bucket):
        """Test listing a single object."""
        s3_bucket.put_object(Bucket='test-bucket', Key='data/sales.csv', Body=b'data')

        inp = S3Input("data/<name>.csv", bucket="test-bucket")
        resources = list(inp.list_resources())

        assert len(resources) == 1
        assert resources[0] == "data/sales.csv"

    def test_list_multiple_objects(self, s3_bucket):
        """Test listing multiple objects."""
        s3_bucket.put_object(Bucket='test-bucket', Key='data/a.csv', Body=b'a')
        s3_bucket.put_object(Bucket='test-bucket', Key='data/b.csv', Body=b'b')
        s3_bucket.put_object(Bucket='test-bucket', Key='data/c.json', Body=b'c')  # No match

        inp = S3Input("data/<name>.csv", bucket="test-bucket")
        resources = list(inp.list_resources())

        assert len(resources) == 2
        assert "data/a.csv" in resources
        assert "data/b.csv" in resources

    def test_list_nested_paths(self, s3_bucket):
        """Test listing with nested path pattern."""
        s3_bucket.put_object(Bucket='test-bucket', Key='data/2024/01/file.parquet', Body=b'')
        s3_bucket.put_object(Bucket='test-bucket', Key='data/2024/02/file.parquet', Body=b'')

        inp = S3Input("data/<year>/<month>/file.parquet", bucket="test-bucket")
        resources = list(inp.list_resources())

        assert len(resources) == 2


class TestS3InputMatch:
    """Tests for S3Input.match()."""

    def test_match_extracts_captures(self, s3_bucket):
        """Test that match() extracts captures correctly."""
        s3_bucket.put_object(Bucket='test-bucket', Key='raw/sales/2024.parquet', Body=b'')

        inp = S3Input("raw/<dataset>/<partition>.parquet", bucket="test-bucket")
        matches = list(inp.match())

        assert len(matches) == 1
        assert matches[0].captures == {"dataset": "sales", "partition": "2024"}
        assert matches[0].key == "raw/sales/2024.parquet"

    def test_match_creates_s3dependency(self, s3_bucket):
        """Test that match() creates S3Dependency objects."""
        s3_bucket.put_object(Bucket='test-bucket', Key='data/test.csv', Body=b'')

        inp = S3Input("data/<name>.csv", bucket="test-bucket")
        matches = list(inp.match())

        assert len(matches) == 1
        dep = matches[0].dependency
        assert isinstance(dep, S3Dependency)
        assert dep.bucket == "test-bucket"
        assert dep.key == "data/test.csv"


class TestS3InputWithGenerator:
    """Tests for S3Input with TaskGenerator."""

    def test_generate_tasks_from_s3(self, s3_bucket):
        """Test generating tasks from S3 objects."""
        s3_bucket.put_object(Bucket='test-bucket', Key='raw/sales.csv', Body=b'sales')
        s3_bucket.put_object(Bucket='test-bucket', Key='raw/orders.csv', Body=b'orders')

        gen = TaskGenerator(
            name="process:<dataset>",
            inputs={
                "data": S3Input("raw/<dataset>.csv", bucket="test-bucket"),
            },
            outputs=[S3Output("processed/<dataset>.parquet", bucket="test-bucket")],
            action=lambda inp, out, attrs: f"process {attrs['dataset']}",
        )

        tasks = list(gen.generate())
        assert len(tasks) == 2
        names = {t.name for t in tasks}
        assert names == {"process:sales", "process:orders"}


class TestS3Output:
    """Tests for S3Output."""

    def test_render_pattern(self):
        """Test pattern rendering."""
        out = S3Output("processed/<dataset>/<year>.parquet", bucket="bucket")
        result = out.render({"dataset": "sales", "year": "2024"})
        assert result == "processed/sales/2024.parquet"

    def test_create_target(self):
        """Test that create_target returns S3Target."""
        out = S3Output(
            "output/<key>.json",
            bucket="my-bucket",
            profile="dev",
            region="eu-west-1"
        )
        target = out.create_target("output/data.json")

        assert isinstance(target, S3Target)
        assert target.bucket == "my-bucket"
        assert target.key == "output/data.json"
        assert target.profile == "dev"
        assert target.region == "eu-west-1"

    def test_create_returns_tuple(self):
        """Test that create() returns (path, target) tuple."""
        out = S3Output("data/<name>.csv", bucket="bucket")
        path, target = out.create({"name": "test"})

        assert path == "data/test.csv"
        assert isinstance(target, S3Target)


class TestS3MultiCapture:
    """Tests for S3 with multiple captures."""

    def test_two_captures(self, s3_bucket):
        """Test S3 pattern with two captures."""
        for year in ["2023", "2024"]:
            for month in ["01", "02"]:
                key = f"data/{year}/{month}/file.parquet"
                s3_bucket.put_object(Bucket='test-bucket', Key=key, Body=b'')

        inp = S3Input("data/<year>/<month>/file.parquet", bucket="test-bucket")
        input_sets = list(build_input_sets({"data": inp}))

        assert len(input_sets) == 4
        attrs_set = {(s.attrs["year"], s.attrs["month"]) for s in input_sets}
        assert attrs_set == {
            ("2023", "01"), ("2023", "02"),
            ("2024", "01"), ("2024", "02"),
        }


class TestS3WildcardPattern:
    """Tests for S3 with wildcard patterns."""

    def test_wildcard_in_filename(self, s3_bucket):
        """Test S3 pattern with wildcard."""
        s3_bucket.put_object(Bucket='test-bucket', Key='data/doc.page1.txt', Body=b'p1')
        s3_bucket.put_object(Bucket='test-bucket', Key='data/doc.page2.txt', Body=b'p2')
        s3_bucket.put_object(Bucket='test-bucket', Key='data/doc.page3.txt', Body=b'p3')

        inp = S3Input("data/<doc>.page*.txt", bucket="test-bucket")
        assert inp.is_list is True  # Auto-detected

        matches = list(inp.match())
        assert len(matches) == 3

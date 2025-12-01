"""Integration tests for prefix-based dependency workflows.

These tests demonstrate complex workflows where:
1. A producer task outputs to a directory prefix
2. Consumer tasks depend on files under that directory
3. doit automatically detects the dependency relationship
"""

import pytest
from pathlib import Path

from doit.task import Task
from doit.engine import DoitEngine
from doit.dependency import InMemoryStateStore as MemoryStore
from doit.control import TargetRegistry
from doit.deps import (
    FileDependency, FileTarget,
    DirectoryDependency, DirectoryTarget,
    S3Dependency, S3PrefixTarget,
)
from doit.taskgen import (
    TaskGenerator, FileInput, FileOutput,
    DirectoryOutput, DirectoryInput,
)


class TestDirectoryPrefixWorkflow:
    """Test workflows where tasks output to directories."""

    def test_directory_target_creates_implicit_dependency(self, tmp_path):
        """Test that DirectoryTarget creates implicit dependency for files under it."""
        # Setup registry
        registry = TargetRegistry()

        # Producer task outputs to a directory
        output_dir = tmp_path / "output"
        producer_target = DirectoryTarget(output_dir)
        registry.register(producer_target, "producer_task")

        # Consumer depends on a file under that directory
        consumer_dep = FileDependency(str(output_dir / "data.csv"))

        # The registry should find the producer
        producer = registry.find_producer(consumer_dep)
        assert producer == "producer_task"

    def test_nested_directory_targets_longest_match(self, tmp_path):
        """Test that nested directories match the most specific producer."""
        registry = TargetRegistry()

        # Parent task outputs to /output/
        parent_target = DirectoryTarget(tmp_path / "output")
        registry.register(parent_target, "parent_task")

        # Child task outputs to /output/processed/
        child_target = DirectoryTarget(tmp_path / "output" / "processed")
        registry.register(child_target, "child_task")

        # File in /output/raw/ should depend on parent
        raw_dep = FileDependency(str(tmp_path / "output" / "raw" / "file.txt"))
        assert registry.find_producer(raw_dep) == "parent_task"

        # File in /output/processed/ should depend on child (more specific)
        processed_dep = FileDependency(str(tmp_path / "output" / "processed" / "file.txt"))
        assert registry.find_producer(processed_dep) == "child_task"

    def test_exact_file_target_overrides_directory_prefix(self, tmp_path):
        """Test that exact file target takes priority over directory prefix."""
        registry = TargetRegistry()

        # Directory target
        dir_target = DirectoryTarget(tmp_path / "output")
        registry.register(dir_target, "dir_task")

        # Exact file target under that directory
        specific_file = str(tmp_path / "output" / "important.txt")
        file_target = FileTarget(specific_file)
        registry.register(file_target, "file_task")

        # Dependency on the specific file should match file_task
        specific_dep = FileDependency(specific_file)
        assert registry.find_producer(specific_dep) == "file_task"

        # Other files should still match dir_task
        other_dep = FileDependency(str(tmp_path / "output" / "other.txt"))
        assert registry.find_producer(other_dep) == "dir_task"


class TestTaskGeneratorPrefixWorkflow:
    """Test TaskGenerator workflows with prefix matching."""

    def test_directory_output_to_file_input_dependency(self, tmp_path):
        """Test that DirectoryOutput enables dependency detection from FileInput."""
        # Create source files
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "config.yaml").write_text("config: true")

        output_dir = tmp_path / "generated"
        final_dir = tmp_path / "final"
        executed = []

        # Producer: outputs to a directory
        def producer_action(inp, out, attrs):
            def do_produce():
                executed.append(("produce", attrs.get("name", "default")))
                # Create output directory and some files
                Path(out[0]).mkdir(parents=True, exist_ok=True)
                (Path(out[0]) / "data.csv").write_text("col1,col2\n1,2")
                (Path(out[0]) / "metadata.json").write_text('{"rows": 1}')
            return do_produce

        producer_gen = TaskGenerator(
            name="generate",
            inputs={"config": FileInput("src/config.yaml", base_path=tmp_path)},
            outputs=[DirectoryOutput("generated/")],  # Relative path
            action=producer_action,
        )

        # Consumer: depends on specific file in that directory
        # First create the expected file so FileInput can find it
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "data.csv").write_text("placeholder")

        def consumer_action(inp, out, attrs):
            def do_consume():
                executed.append(("consume", "data"))
                Path(out[0]).parent.mkdir(parents=True, exist_ok=True)
                data = Path(inp["data"].path).read_text()
                Path(out[0]).write_text(f"processed: {data}")
            return do_consume

        consumer_gen = TaskGenerator(
            name="process",
            inputs={"data": FileInput("generated/*.csv", base_path=tmp_path)},
            outputs=[FileOutput("final/result.txt")],
            action=consumer_action,
        )

        producer_tasks = list(producer_gen.generate())
        consumer_tasks = list(consumer_gen.generate())
        all_tasks = producer_tasks + consumer_tasks

        assert len(producer_tasks) == 1
        assert len(consumer_tasks) == 1

        # Run with DoitEngine
        with DoitEngine(all_tasks, store=MemoryStore()) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        # Producer should run first due to dependency
        assert ("produce", "default") in executed
        assert ("consume", "data") in executed


class TestS3PrefixWorkflow:
    """Test S3 prefix-based workflows."""

    def test_s3_prefix_target_creates_implicit_dependency(self):
        """Test that S3PrefixTarget creates implicit dependency."""
        registry = TargetRegistry()

        # Producer outputs to S3 prefix
        producer_target = S3PrefixTarget("my-bucket", "output/data/")
        registry.register(producer_target, "etl_task")

        # Consumer depends on specific S3 object under that prefix
        consumer_dep = S3Dependency("my-bucket", "output/data/2024/sales.parquet")
        assert registry.find_producer(consumer_dep) == "etl_task"

    def test_s3_different_bucket_no_match(self):
        """Test that S3 prefix doesn't match different bucket."""
        registry = TargetRegistry()

        # Producer outputs to bucket-a
        producer_target = S3PrefixTarget("bucket-a", "output/")
        registry.register(producer_target, "task_a")

        # Dependency on bucket-b shouldn't match
        dep = S3Dependency("bucket-b", "output/file.txt")
        assert registry.find_producer(dep) is None

    def test_multiple_s3_prefixes_longest_match(self):
        """Test multiple S3 prefixes use longest match."""
        registry = TargetRegistry()

        # General prefix
        registry.register(S3PrefixTarget("bucket", "data/"), "general_task")

        # Specific prefix under the general one
        registry.register(S3PrefixTarget("bucket", "data/processed/"), "specific_task")

        # File under general but not specific
        raw_dep = S3Dependency("bucket", "data/raw/file.parquet")
        assert registry.find_producer(raw_dep) == "general_task"

        # File under specific
        processed_dep = S3Dependency("bucket", "data/processed/file.parquet")
        assert registry.find_producer(processed_dep) == "specific_task"


class TestComplexPipelineWorkflow:
    """Test complex multi-stage pipeline workflows."""

    def test_three_stage_pipeline_with_prefix_matching(self, tmp_path):
        """Test a realistic 3-stage data pipeline with prefix matching.

        Stage 1: Extract - reads raw files, outputs to /extracted/
        Stage 2: Transform - reads from /extracted/, outputs to /transformed/
        Stage 3: Load - reads from /transformed/, outputs to /final/
        """
        registry = TargetRegistry()

        # Stage 1: Extract
        extracted_target = DirectoryTarget(tmp_path / "extracted")
        registry.register(extracted_target, "extract")

        # Stage 2: Transform
        transformed_target = DirectoryTarget(tmp_path / "transformed")
        registry.register(transformed_target, "transform")

        # Stage 3: Load - depends on specific file from transform
        load_dep = FileDependency(str(tmp_path / "transformed" / "data.csv"))
        assert registry.find_producer(load_dep) == "transform"

        # Transform depends on file from extract
        transform_dep = FileDependency(str(tmp_path / "extracted" / "raw.json"))
        assert registry.find_producer(transform_dep) == "extract"

        # Verify stats
        assert registry.stats["prefix_count"] == 2

    def test_partition_based_workflow(self, tmp_path):
        """Test workflow with partitioned outputs (like Spark/Hive).

        Producer creates: /output/year=2024/month=01/data.parquet
        Consumer depends on files under /output/year=2024/
        """
        registry = TargetRegistry()

        # Producer outputs to partition directory
        partition_target = DirectoryTarget(tmp_path / "output" / "year=2024")
        registry.register(partition_target, "etl_2024")

        # Consumer for January data
        jan_dep = FileDependency(str(tmp_path / "output" / "year=2024" / "month=01" / "data.parquet"))
        assert registry.find_producer(jan_dep) == "etl_2024"

        # Consumer for December data (same year)
        dec_dep = FileDependency(str(tmp_path / "output" / "year=2024" / "month=12" / "data.parquet"))
        assert registry.find_producer(dec_dep) == "etl_2024"

        # Different year should not match
        registry.register(DirectoryTarget(tmp_path / "output" / "year=2023"), "etl_2023")
        old_dep = FileDependency(str(tmp_path / "output" / "year=2023" / "month=06" / "data.parquet"))
        assert registry.find_producer(old_dep) == "etl_2023"


class TestDirectoryInputWorkflow:
    """Test workflows using DirectoryInput."""

    def test_directory_input_creates_directory_dependency(self, tmp_path):
        """Test that DirectoryInput creates DirectoryDependency."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        inp = DirectoryInput("data", base_path=tmp_path)
        matches = list(inp.match())

        assert len(matches) == 1
        assert isinstance(matches[0].dependency, DirectoryDependency)

    def test_directory_input_matches_directory_target(self, tmp_path):
        """Test that DirectoryInput dependency matches DirectoryTarget."""
        registry = TargetRegistry()

        # Producer with DirectoryOutput
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        producer_target = DirectoryTarget(output_dir)
        registry.register(producer_target, "producer")

        # Consumer with DirectoryInput creates DirectoryDependency
        # The DirectoryDependency key ends with / and should match
        consumer_dep = DirectoryDependency(output_dir)
        assert registry.find_producer(consumer_dep) == "producer"


class TestFindAllProducers:
    """Test finding all matching producers (for debugging/diagnostics)."""

    def test_find_all_returns_all_prefix_matches(self, tmp_path):
        """Test that find_all_producers returns all matching prefixes."""
        registry = TargetRegistry()

        # Register nested directories
        registry.register(DirectoryTarget(tmp_path / "a"), "task_a")
        registry.register(DirectoryTarget(tmp_path / "a" / "b"), "task_b")
        registry.register(DirectoryTarget(tmp_path / "a" / "b" / "c"), "task_c")

        # File deep in the hierarchy matches all three
        dep = FileDependency(str(tmp_path / "a" / "b" / "c" / "file.txt"))
        all_producers = registry.find_all_producers(dep)

        assert set(all_producers) == {"task_a", "task_b", "task_c"}

        # But find_producer returns the most specific (longest prefix)
        assert registry.find_producer(dep) == "task_c"


class TestEdgeCases:
    """Test edge cases in prefix matching."""

    def test_similar_prefixes_dont_match(self, tmp_path):
        """Test that /output doesn't match /output_backup."""
        registry = TargetRegistry()

        registry.register(DirectoryTarget(tmp_path / "output"), "output_task")

        # /output_backup is NOT under /output/ (it's a sibling)
        dep = FileDependency(str(tmp_path / "output_backup" / "file.txt"))
        assert registry.find_producer(dep) is None

    def test_root_prefix_matches_all(self, tmp_path):
        """Test that root directory prefix matches everything under it."""
        registry = TargetRegistry()

        # Register root as target
        registry.register(DirectoryTarget(tmp_path), "root_task")

        # Any file under tmp_path should match
        dep1 = FileDependency(str(tmp_path / "a.txt"))
        dep2 = FileDependency(str(tmp_path / "deep" / "nested" / "file.txt"))

        assert registry.find_producer(dep1) == "root_task"
        assert registry.find_producer(dep2) == "root_task"

    def test_empty_directory_target_key_normalization(self, tmp_path):
        """Test that directory keys are properly normalized."""
        registry = TargetRegistry()

        # Register with no trailing slash
        target = DirectoryTarget(str(tmp_path / "output"))
        registry.register(target, "task")

        # Key should be normalized to have trailing slash
        assert target.get_key().endswith('/')

        # Should still match files under it
        dep = FileDependency(str(tmp_path / "output" / "file.txt"))
        assert registry.find_producer(dep) == "task"

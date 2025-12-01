"""Tests for S3Dependency and S3Target classes.

These tests use moto to mock AWS S3. If moto/boto3 are not installed,
the tests are automatically skipped.
"""

import pytest

# Skip all tests if boto3/moto not available
boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from moto import mock_aws

from doit.deps import (
    S3Dependency, S3Target, CheckStatus, DependencyCheckResult
)


@pytest.fixture
def s3_bucket():
    """Mocked S3 bucket for testing."""
    with mock_aws():
        client = boto3.client('s3', region_name='us-east-1')
        client.create_bucket(Bucket='test-bucket')
        yield client


class TestS3Dependency:
    """Tests for S3Dependency class."""

    def test_get_key_format(self):
        """Test that get_key returns proper S3 URI format."""
        dep = S3Dependency('bucket', 'path/file.csv')
        assert dep.get_key() == 's3://bucket/path/file.csv'

    def test_get_key_with_special_chars(self):
        """Test key with special characters in path."""
        dep = S3Dependency('bucket', 'path/to/file with spaces.csv')
        assert dep.get_key() == 's3://bucket/path/to/file with spaces.csv'

    def test_exists_true(self, s3_bucket):
        """Test exists returns True when object exists."""
        s3_bucket.put_object(Bucket='test-bucket', Key='test.txt', Body=b'data')
        dep = S3Dependency('test-bucket', 'test.txt')
        assert dep.exists() is True

    def test_exists_false(self, s3_bucket):
        """Test exists returns False when object doesn't exist."""
        dep = S3Dependency('test-bucket', 'nonexistent.txt')
        assert dep.exists() is False

    def test_is_modified_first_run(self, s3_bucket):
        """Test is_modified returns True on first run (no stored state)."""
        s3_bucket.put_object(Bucket='test-bucket', Key='test.txt', Body=b'data')
        dep = S3Dependency('test-bucket', 'test.txt')
        assert dep.is_modified(None) is True

    def test_is_modified_unchanged(self, s3_bucket):
        """Test is_modified returns False when content unchanged."""
        s3_bucket.put_object(Bucket='test-bucket', Key='test.txt', Body=b'data')
        dep = S3Dependency('test-bucket', 'test.txt')

        # Get state and check modification with same state
        state = dep.get_state(None)
        assert dep.is_modified(state) is False

    def test_is_modified_changed(self, s3_bucket):
        """Test is_modified returns True when content changes."""
        s3_bucket.put_object(Bucket='test-bucket', Key='test.txt', Body=b'v1')
        dep = S3Dependency('test-bucket', 'test.txt')
        state = dep.get_state(None)

        # Modify the object
        s3_bucket.put_object(Bucket='test-bucket', Key='test.txt', Body=b'v2')

        assert dep.is_modified(state) is True

    def test_get_state_returns_tuple(self, s3_bucket):
        """Test get_state returns (etag, mtime) tuple."""
        s3_bucket.put_object(Bucket='test-bucket', Key='test.txt', Body=b'data')
        dep = S3Dependency('test-bucket', 'test.txt')
        state = dep.get_state(None)

        assert isinstance(state, tuple)
        assert len(state) == 2
        # etag should be a string
        assert isinstance(state[0], str)
        # mtime should be a float (timestamp)
        assert isinstance(state[1], float)

    def test_get_state_unchanged_returns_none(self, s3_bucket):
        """Test get_state returns None when state unchanged."""
        s3_bucket.put_object(Bucket='test-bucket', Key='test.txt', Body=b'data')
        dep = S3Dependency('test-bucket', 'test.txt')

        state1 = dep.get_state(None)
        state2 = dep.get_state(state1)  # Should return None (unchanged)

        assert state2 is None

    def test_check_status_missing(self, s3_bucket):
        """Test check_status returns ERROR for missing object."""
        dep = S3Dependency('test-bucket', 'nonexistent.txt')
        result = dep.check_status(None)

        assert result.status == CheckStatus.ERROR
        assert 'does not exist' in result.reason

    def test_check_status_first_run(self, s3_bucket):
        """Test check_status returns CHANGED on first run."""
        s3_bucket.put_object(Bucket='test-bucket', Key='test.txt', Body=b'data')
        dep = S3Dependency('test-bucket', 'test.txt')
        result = dep.check_status(None)

        assert result.status == CheckStatus.CHANGED
        assert 'first run' in result.reason

    def test_check_status_modified(self, s3_bucket):
        """Test check_status returns CHANGED when modified."""
        s3_bucket.put_object(Bucket='test-bucket', Key='test.txt', Body=b'v1')
        dep = S3Dependency('test-bucket', 'test.txt')
        state = dep.get_state(None)

        # Modify content
        s3_bucket.put_object(Bucket='test-bucket', Key='test.txt', Body=b'v2')
        result = dep.check_status(state)

        assert result.status == CheckStatus.CHANGED
        assert 'modified' in result.reason

    def test_check_status_up_to_date(self, s3_bucket):
        """Test check_status returns UP_TO_DATE when unchanged."""
        s3_bucket.put_object(Bucket='test-bucket', Key='test.txt', Body=b'data')
        dep = S3Dependency('test-bucket', 'test.txt')
        state = dep.get_state(None)
        result = dep.check_status(state)

        assert result.status == CheckStatus.UP_TO_DATE

    def test_profile_and_region(self):
        """Test that profile and region are stored."""
        dep = S3Dependency('bucket', 'key', profile='dev', region='eu-west-1')
        assert dep.profile == 'dev'
        assert dep.region == 'eu-west-1'


class TestS3Target:
    """Tests for S3Target class."""

    def test_get_key_format(self):
        """Test that get_key returns proper S3 URI format."""
        target = S3Target('bucket', 'output/result.csv')
        assert target.get_key() == 's3://bucket/output/result.csv'

    def test_exists_true(self, s3_bucket):
        """Test exists returns True when object exists."""
        s3_bucket.put_object(Bucket='test-bucket', Key='output.txt', Body=b'data')
        target = S3Target('test-bucket', 'output.txt')
        assert target.exists() is True

    def test_exists_false(self, s3_bucket):
        """Test exists returns False when object doesn't exist."""
        target = S3Target('test-bucket', 'nonexistent.txt')
        assert target.exists() is False

    def test_matches_s3_dependency_same(self):
        """Test matches_dependency returns True for matching S3Dependency."""
        target = S3Target('bucket', 'path/file.csv')
        dep = S3Dependency('bucket', 'path/file.csv')

        assert target.matches_dependency(dep) is True

    def test_matches_s3_dependency_different_key(self):
        """Test matches_dependency returns False for different key."""
        target = S3Target('bucket', 'path/file.csv')
        dep = S3Dependency('bucket', 'other/file.csv')

        assert target.matches_dependency(dep) is False

    def test_matches_s3_dependency_different_bucket(self):
        """Test matches_dependency returns False for different bucket."""
        target = S3Target('bucket1', 'path/file.csv')
        dep = S3Dependency('bucket2', 'path/file.csv')

        assert target.matches_dependency(dep) is False

    def test_matches_file_dependency(self):
        """Test matches_dependency returns False for FileDependency."""
        from doit.deps import FileDependency

        target = S3Target('bucket', 'path/file.csv')
        dep = FileDependency('path/file.csv')

        assert target.matches_dependency(dep) is False


class TestS3ImplicitDeps:
    """Tests for implicit task dependency matching with S3."""

    def test_s3_target_creates_implicit_dep(self, s3_bucket):
        """Test that S3Target output creates implicit task dependency."""
        from doit.task import Task
        from doit.control import TaskControl

        t1 = Task("upload",
                  actions=["aws s3 cp data.csv s3://test-bucket/data.csv"],
                  outputs=[S3Target('test-bucket', 'data.csv')])

        t2 = Task("process",
                  actions=["process-s3-data"],
                  dependencies=[S3Dependency('test-bucket', 'data.csv')])

        tc = TaskControl([t1, t2])

        # t2 should have implicit dependency on t1
        assert 'upload' in t2.task_dep

    def test_mixed_file_and_s3_dependencies(self, s3_bucket):
        """Test tasks with both file and S3 dependencies work together."""
        from doit.task import Task
        from doit.deps import FileDependency, FileTarget
        from doit.control import TaskControl

        t1 = Task("generate",
                  actions=["generate"],
                  targets=['local.txt'])

        t2 = Task("upload",
                  actions=["aws s3 cp local.txt s3://test-bucket/remote.csv"],
                  dependencies=[FileDependency('local.txt')],
                  outputs=[S3Target('test-bucket', 'remote.csv')])

        t3 = Task("download",
                  actions=["aws s3 cp s3://test-bucket/remote.csv result.csv"],
                  dependencies=[S3Dependency('test-bucket', 'remote.csv')])

        tc = TaskControl([t1, t2, t3])

        # t2 depends on t1 (local file)
        assert 'generate' in t2.task_dep
        # t3 depends on t2 (S3 object)
        assert 'upload' in t3.task_dep


class TestComplexS3Pipelines:
    """Tests for complex pipeline scenarios with S3 and file dependencies."""

    def test_diamond_dependency_pattern(self, s3_bucket):
        """Test diamond: A -> B, A -> C, B -> D, C -> D with mixed types.

        Pipeline:
            source (file) -> transform1 (S3) -> merge
                          -> transform2 (S3) -> merge
        """
        from doit.task import Task
        from doit.deps import FileDependency, FileTarget
        from doit.control import TaskControl

        # Source task produces a file
        source = Task("source",
                      actions=["generate-data"],
                      targets=['data.csv'])

        # Transform1: file -> S3
        transform1 = Task("transform1",
                          actions=["transform --variant=1"],
                          dependencies=[FileDependency('data.csv')],
                          outputs=[S3Target('test-bucket', 'transformed1.csv')])

        # Transform2: file -> S3
        transform2 = Task("transform2",
                          actions=["transform --variant=2"],
                          dependencies=[FileDependency('data.csv')],
                          outputs=[S3Target('test-bucket', 'transformed2.csv')])

        # Merge: both S3 outputs -> file
        merge = Task("merge",
                     actions=["merge-data"],
                     dependencies=[
                         S3Dependency('test-bucket', 'transformed1.csv'),
                         S3Dependency('test-bucket', 'transformed2.csv'),
                     ],
                     targets=['merged.csv'])

        tc = TaskControl([source, transform1, transform2, merge])

        # Both transforms depend on source
        assert 'source' in transform1.task_dep
        assert 'source' in transform2.task_dep
        # Merge depends on both transforms
        assert 'transform1' in merge.task_dep
        assert 'transform2' in merge.task_dep

    def test_multi_bucket_pipeline(self, s3_bucket):
        """Test pipeline spanning multiple S3 buckets."""
        from doit.task import Task
        from doit.control import TaskControl

        # Create second bucket
        s3_bucket.create_bucket(Bucket='staging-bucket')
        s3_bucket.create_bucket(Bucket='production-bucket')

        # Upload to staging
        upload_staging = Task("upload_staging",
                              actions=["upload to staging"],
                              outputs=[S3Target('staging-bucket', 'data.csv')])

        # Copy from staging to production
        promote = Task("promote",
                       actions=["copy staging to prod"],
                       dependencies=[S3Dependency('staging-bucket', 'data.csv')],
                       outputs=[S3Target('production-bucket', 'data.csv')])

        # Process from production
        process = Task("process",
                       actions=["process production data"],
                       dependencies=[S3Dependency('production-bucket', 'data.csv')])

        tc = TaskControl([upload_staging, promote, process])

        assert 'upload_staging' in promote.task_dep
        assert 'promote' in process.task_dep

    def test_long_pipeline_chain(self, s3_bucket):
        """Test 6-task pipeline alternating between file and S3."""
        from doit.task import Task
        from doit.deps import FileDependency, FileTarget
        from doit.control import TaskControl

        # Task 1: generate file
        t1 = Task("step1_generate",
                  actions=["generate"],
                  targets=['step1.txt'])

        # Task 2: file -> S3
        t2 = Task("step2_upload",
                  actions=["upload"],
                  dependencies=[FileDependency('step1.txt')],
                  outputs=[S3Target('test-bucket', 'step2.csv')])

        # Task 3: S3 -> file
        t3 = Task("step3_download",
                  actions=["download"],
                  dependencies=[S3Dependency('test-bucket', 'step2.csv')],
                  targets=['step3.txt'])

        # Task 4: file -> S3
        t4 = Task("step4_process",
                  actions=["process"],
                  dependencies=[FileDependency('step3.txt')],
                  outputs=[S3Target('test-bucket', 'step4.csv')])

        # Task 5: S3 -> file
        t5 = Task("step5_finalize",
                  actions=["finalize"],
                  dependencies=[S3Dependency('test-bucket', 'step4.csv')],
                  targets=['step5.txt'])

        # Task 6: file -> S3 (archive)
        t6 = Task("step6_archive",
                  actions=["archive"],
                  dependencies=[FileDependency('step5.txt')],
                  outputs=[S3Target('test-bucket', 'archive/final.csv')])

        tc = TaskControl([t1, t2, t3, t4, t5, t6])

        # Verify full chain
        assert 'step1_generate' in t2.task_dep
        assert 'step2_upload' in t3.task_dep
        assert 'step3_download' in t4.task_dep
        assert 'step4_process' in t5.task_dep
        assert 'step5_finalize' in t6.task_dep

    def test_parallel_branches_with_fan_in(self, s3_bucket):
        """Test parallel processing branches that fan into single output.

        Pipeline:
            input -> branch_a1 -> branch_a2 -\
                  -> branch_b1 -> branch_b2 --> collector
                  -> branch_c1 -> branch_c2 -/
        """
        from doit.task import Task
        from doit.deps import FileDependency
        from doit.control import TaskControl

        # Input task
        input_task = Task("input",
                          actions=["generate input"],
                          targets=['input.csv'])

        # Branch A: file -> S3 -> S3
        branch_a1 = Task("branch_a1",
                         actions=["process A1"],
                         dependencies=[FileDependency('input.csv')],
                         outputs=[S3Target('test-bucket', 'a/step1.csv')])

        branch_a2 = Task("branch_a2",
                         actions=["process A2"],
                         dependencies=[S3Dependency('test-bucket', 'a/step1.csv')],
                         outputs=[S3Target('test-bucket', 'a/step2.csv')])

        # Branch B: file -> S3 -> S3
        branch_b1 = Task("branch_b1",
                         actions=["process B1"],
                         dependencies=[FileDependency('input.csv')],
                         outputs=[S3Target('test-bucket', 'b/step1.csv')])

        branch_b2 = Task("branch_b2",
                         actions=["process B2"],
                         dependencies=[S3Dependency('test-bucket', 'b/step1.csv')],
                         outputs=[S3Target('test-bucket', 'b/step2.csv')])

        # Branch C: file -> S3 -> S3
        branch_c1 = Task("branch_c1",
                         actions=["process C1"],
                         dependencies=[FileDependency('input.csv')],
                         outputs=[S3Target('test-bucket', 'c/step1.csv')])

        branch_c2 = Task("branch_c2",
                         actions=["process C2"],
                         dependencies=[S3Dependency('test-bucket', 'c/step1.csv')],
                         outputs=[S3Target('test-bucket', 'c/step2.csv')])

        # Collector: all three branches -> single output
        collector = Task("collector",
                         actions=["collect all branches"],
                         dependencies=[
                             S3Dependency('test-bucket', 'a/step2.csv'),
                             S3Dependency('test-bucket', 'b/step2.csv'),
                             S3Dependency('test-bucket', 'c/step2.csv'),
                         ],
                         targets=['collected.csv'])

        all_tasks = [input_task, branch_a1, branch_a2, branch_b1, branch_b2,
                     branch_c1, branch_c2, collector]
        tc = TaskControl(all_tasks)

        # All first branches depend on input
        assert 'input' in branch_a1.task_dep
        assert 'input' in branch_b1.task_dep
        assert 'input' in branch_c1.task_dep

        # Second branches depend on first
        assert 'branch_a1' in branch_a2.task_dep
        assert 'branch_b1' in branch_b2.task_dep
        assert 'branch_c1' in branch_c2.task_dep

        # Collector depends on all second branches
        assert 'branch_a2' in collector.task_dep
        assert 'branch_b2' in collector.task_dep
        assert 'branch_c2' in collector.task_dep

    def test_task_with_both_file_and_s3_outputs(self, s3_bucket):
        """Test single task producing both file and S3 outputs."""
        from doit.task import Task
        from doit.deps import FileDependency, FileTarget
        from doit.control import TaskControl

        # Producer outputs to both local file and S3
        producer = Task("producer",
                        actions=["produce data"],
                        targets=['local_copy.csv'],
                        outputs=[S3Target('test-bucket', 'remote_copy.csv')])

        # Consumer 1 uses local file
        local_consumer = Task("local_consumer",
                              actions=["process local"],
                              dependencies=[FileDependency('local_copy.csv')])

        # Consumer 2 uses S3 version
        s3_consumer = Task("s3_consumer",
                           actions=["process remote"],
                           dependencies=[S3Dependency('test-bucket', 'remote_copy.csv')])

        tc = TaskControl([producer, local_consumer, s3_consumer])

        # Both consumers depend on producer
        assert 'producer' in local_consumer.task_dep
        assert 'producer' in s3_consumer.task_dep

    def test_s3_with_task_dependency(self, s3_bucket):
        """Test combining S3Dependency with explicit TaskDependency."""
        from doit.task import Task
        from doit.deps import TaskDependency
        from doit.control import TaskControl

        # Setup task (no outputs)
        setup = Task("setup",
                     actions=["setup environment"])

        # Producer depends on setup explicitly, produces S3
        producer = Task("producer",
                        actions=["produce data"],
                        dependencies=[TaskDependency('setup')],
                        outputs=[S3Target('test-bucket', 'data.csv')])

        # Consumer depends on S3 output (implicit) and has explicit task dep
        consumer = Task("consumer",
                        actions=["consume data"],
                        dependencies=[
                            TaskDependency('setup'),  # explicit
                            S3Dependency('test-bucket', 'data.csv'),  # implicit from producer
                        ])

        tc = TaskControl([setup, producer, consumer])

        # Producer has explicit dep on setup
        assert 'setup' in producer.task_dep
        # Consumer has both explicit setup dep and implicit producer dep
        assert 'setup' in consumer.task_dep
        assert 'producer' in consumer.task_dep

    def test_multiple_s3_objects_same_bucket(self, s3_bucket):
        """Test multiple S3 dependencies/targets in same bucket."""
        from doit.task import Task
        from doit.control import TaskControl

        # Producer creates multiple objects
        producer = Task("producer",
                        actions=["split data"],
                        outputs=[
                            S3Target('test-bucket', 'split/part1.csv'),
                            S3Target('test-bucket', 'split/part2.csv'),
                            S3Target('test-bucket', 'split/part3.csv'),
                        ])

        # Consumer 1 uses part1 and part2
        consumer1 = Task("consumer1",
                         actions=["merge 1+2"],
                         dependencies=[
                             S3Dependency('test-bucket', 'split/part1.csv'),
                             S3Dependency('test-bucket', 'split/part2.csv'),
                         ],
                         outputs=[S3Target('test-bucket', 'merged/1_2.csv')])

        # Consumer 2 uses part3
        consumer2 = Task("consumer2",
                         actions=["process part3"],
                         dependencies=[
                             S3Dependency('test-bucket', 'split/part3.csv'),
                         ],
                         outputs=[S3Target('test-bucket', 'processed/3.csv')])

        # Final task uses outputs from both consumers
        final = Task("final",
                     actions=["final merge"],
                     dependencies=[
                         S3Dependency('test-bucket', 'merged/1_2.csv'),
                         S3Dependency('test-bucket', 'processed/3.csv'),
                     ])

        tc = TaskControl([producer, consumer1, consumer2, final])

        # Both consumers depend on producer
        assert 'producer' in consumer1.task_dep
        assert 'producer' in consumer2.task_dep
        # Final depends on both consumers
        assert 'consumer1' in final.task_dep
        assert 'consumer2' in final.task_dep


class TestS3EdgeCases:
    """Tests for edge cases and validation in S3 pipelines."""

    def test_no_implicit_dep_when_no_matching_target(self, s3_bucket):
        """Test that S3Dependency without matching S3Target doesn't create dep."""
        from doit.task import Task
        from doit.control import TaskControl

        # Task with S3 output
        producer = Task("producer",
                        actions=["produce"],
                        outputs=[S3Target('test-bucket', 'data.csv')])

        # Task with S3 dependency on DIFFERENT key (no match)
        consumer = Task("consumer",
                        actions=["consume"],
                        dependencies=[S3Dependency('test-bucket', 'other.csv')])

        tc = TaskControl([producer, consumer])

        # No implicit dependency should be created
        assert 'producer' not in consumer.task_dep

    def test_s3_dep_does_not_match_file_target(self, s3_bucket):
        """Test that S3Dependency doesn't match FileTarget with similar name."""
        from doit.task import Task
        from doit.control import TaskControl

        # File target
        file_producer = Task("file_producer",
                             actions=["produce file"],
                             targets=['data.csv'])

        # S3 dependency with same basename
        s3_consumer = Task("s3_consumer",
                           actions=["consume s3"],
                           dependencies=[S3Dependency('test-bucket', 'data.csv')])

        tc = TaskControl([file_producer, s3_consumer])

        # No implicit dependency - types don't match
        assert 'file_producer' not in s3_consumer.task_dep

    def test_file_dep_does_not_match_s3_target(self, s3_bucket):
        """Test that FileDependency doesn't match S3Target with similar name."""
        from doit.task import Task
        from doit.deps import FileDependency
        from doit.control import TaskControl

        # S3 target
        s3_producer = Task("s3_producer",
                           actions=["produce s3"],
                           outputs=[S3Target('test-bucket', 'data.csv')])

        # File dependency with same basename
        file_consumer = Task("file_consumer",
                             actions=["consume file"],
                             dependencies=[FileDependency('data.csv')])

        tc = TaskControl([s3_producer, file_consumer])

        # No implicit dependency - types don't match
        assert 's3_producer' not in file_consumer.task_dep

    def test_same_key_different_buckets_no_match(self, s3_bucket):
        """Test that same key in different buckets doesn't match."""
        from doit.task import Task
        from doit.control import TaskControl

        s3_bucket.create_bucket(Bucket='other-bucket')

        # Target in test-bucket
        producer = Task("producer",
                        actions=["produce"],
                        outputs=[S3Target('test-bucket', 'data.csv')])

        # Dependency in other-bucket
        consumer = Task("consumer",
                        actions=["consume"],
                        dependencies=[S3Dependency('other-bucket', 'data.csv')])

        tc = TaskControl([producer, consumer])

        # No implicit dependency - different buckets
        assert 'producer' not in consumer.task_dep

    def test_case_sensitive_s3_keys(self, s3_bucket):
        """Test that S3 keys are case-sensitive."""
        from doit.task import Task
        from doit.control import TaskControl

        producer = Task("producer",
                        actions=["produce"],
                        outputs=[S3Target('test-bucket', 'Data.CSV')])

        consumer = Task("consumer",
                        actions=["consume"],
                        dependencies=[S3Dependency('test-bucket', 'data.csv')])

        tc = TaskControl([producer, consumer])

        # No match due to case difference
        assert 'producer' not in consumer.task_dep

    def test_s3_keys_with_special_characters(self, s3_bucket):
        """Test S3 keys with special characters match correctly."""
        from doit.task import Task
        from doit.control import TaskControl

        special_key = 'path/to/file with spaces & symbols!.csv'

        producer = Task("producer",
                        actions=["produce"],
                        outputs=[S3Target('test-bucket', special_key)])

        consumer = Task("consumer",
                        actions=["consume"],
                        dependencies=[S3Dependency('test-bucket', special_key)])

        tc = TaskControl([producer, consumer])

        assert 'producer' in consumer.task_dep

    def test_deeply_nested_s3_paths(self, s3_bucket):
        """Test S3 keys with deeply nested paths."""
        from doit.task import Task
        from doit.control import TaskControl

        deep_key = 'a/b/c/d/e/f/g/h/i/j/data.csv'

        producer = Task("producer",
                        actions=["produce"],
                        outputs=[S3Target('test-bucket', deep_key)])

        consumer = Task("consumer",
                        actions=["consume"],
                        dependencies=[S3Dependency('test-bucket', deep_key)])

        tc = TaskControl([producer, consumer])

        assert 'producer' in consumer.task_dep

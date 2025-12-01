"""Tests for ReactiveEngine."""

import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

from doit.reactive.engine import ReactiveEngine, ReactiveResult


class TestReactiveResult:
    """Tests for ReactiveResult dataclass."""

    def test_converged_true(self):
        """Test converged property when not hit limit."""
        result = ReactiveResult(
            tasks_executed=10,
            total_tasks=10,
            hit_limit=False,
        )
        assert result.converged is True

    def test_converged_false(self):
        """Test converged property when hit limit."""
        result = ReactiveResult(
            tasks_executed=100,
            total_tasks=100,
            hit_limit=True,
        )
        assert result.converged is False


class TestReactiveEngineBasics:
    """Basic tests for ReactiveEngine."""

    def test_empty_generators(self):
        """Test engine with no generators."""
        engine = ReactiveEngine(generators=[])
        result = engine.run()

        assert result.tasks_executed == 0
        assert result.total_tasks == 0
        assert result.converged is True

    def test_properties(self):
        """Test engine properties."""
        engine = ReactiveEngine(generators=[], max_tasks=500)

        assert engine.max_tasks == 500
        assert engine.tasks_executed == 0
        assert engine.total_tasks == 0

    def test_reset(self):
        """Test resetting the engine."""
        engine = ReactiveEngine(generators=[])

        # Simulate some state
        engine._tasks_executed = 10
        engine._regenerations = 5

        engine.reset()

        assert engine.tasks_executed == 0
        assert engine.regenerations == 0


class TestGetTaskOutputs:
    """Tests for _get_task_outputs method."""

    def test_new_style_outputs(self):
        """Test extracting outputs from Target objects."""
        engine = ReactiveEngine(generators=[])

        task = MagicMock()
        target1 = MagicMock()
        target1.get_key.return_value = "/path/to/output1.txt"
        target2 = MagicMock()
        target2.get_key.return_value = "/path/to/output2.txt"
        task.outputs = [target1, target2]
        task.targets = []

        outputs = engine._get_task_outputs(task)

        assert "/path/to/output1.txt" in outputs
        assert "/path/to/output2.txt" in outputs

    def test_legacy_string_targets(self):
        """Test extracting outputs from legacy string targets."""
        engine = ReactiveEngine(generators=[])

        task = MagicMock()
        task.outputs = []
        task.targets = ["/path/to/target1.txt", "/path/to/target2.txt"]

        outputs = engine._get_task_outputs(task)

        assert "/path/to/target1.txt" in outputs
        assert "/path/to/target2.txt" in outputs

    def test_mixed_outputs(self):
        """Test extracting from both new and legacy outputs."""
        engine = ReactiveEngine(generators=[])

        task = MagicMock()
        target = MagicMock()
        target.get_key.return_value = "/new/output.txt"
        task.outputs = [target]
        task.targets = ["/legacy/target.txt"]

        outputs = engine._get_task_outputs(task)

        assert len(outputs) == 2
        assert "/new/output.txt" in outputs
        assert "/legacy/target.txt" in outputs

    def test_no_outputs(self):
        """Test task with no outputs."""
        engine = ReactiveEngine(generators=[])

        task = MagicMock()
        task.outputs = []
        task.targets = []

        outputs = engine._get_task_outputs(task)

        assert len(outputs) == 0


class TestAddGenerator:
    """Tests for add_generator method."""

    def test_add_generator(self):
        """Test adding a generator after construction."""
        engine = ReactiveEngine(generators=[])

        gen = MagicMock()
        gen.inputs = {"data": MagicMock(pattern="processed/<doc>.json")}

        engine.add_generator(gen)

        assert gen in engine.generators
        assert engine._manager.generator_count == 1


class TestReactiveEngineIntegration:
    """Integration tests for ReactiveEngine with real TaskGenerators."""

    def test_simple_workflow(self, tmp_path):
        """Test a simple workflow with one generator."""
        from doit.taskgen import TaskGenerator, FileInput, FileOutput

        # Create input files
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        (raw_dir / "doc1.txt").write_text("content1")
        (raw_dir / "doc2.txt").write_text("content2")

        # Create output directory
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        executed = []

        def process_action(inp, out, attrs):
            def do_process():
                executed.append(attrs["doc"])
                Path(out[0]).parent.mkdir(parents=True, exist_ok=True)
                Path(out[0]).write_text(f"processed {attrs['doc']}")
            return do_process

        gen = TaskGenerator(
            name="process:<doc>",
            inputs={"raw": FileInput("raw/<doc>.txt", base_path=tmp_path)},
            outputs=[FileOutput("processed/<doc>.json", base_path=tmp_path)],
            action=process_action,
        )

        engine = ReactiveEngine(generators=[gen])
        result = engine.run()

        # Both docs should be processed
        assert "doc1" in executed
        assert "doc2" in executed
        assert result.tasks_executed == 2
        assert result.converged is True

    def test_cascading_workflow(self, tmp_path):
        """Test cascading generators where output triggers new generation."""
        from doit.taskgen import TaskGenerator, FileInput, FileOutput

        # Create input files
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        (raw_dir / "source.txt").write_text("source content")

        # Create intermediate and final directories
        (tmp_path / "intermediate").mkdir()
        (tmp_path / "final").mkdir()

        stage1_executed = []
        stage2_executed = []

        def stage1_action(inp, out, attrs):
            def do_stage1():
                stage1_executed.append(attrs["name"])
                Path(out[0]).parent.mkdir(parents=True, exist_ok=True)
                Path(out[0]).write_text(f"stage1 {attrs['name']}")
            return do_stage1

        def stage2_action(inp, out, attrs):
            def do_stage2():
                stage2_executed.append(attrs["name"])
                Path(out[0]).parent.mkdir(parents=True, exist_ok=True)
                Path(out[0]).write_text(f"stage2 {attrs['name']}")
            return do_stage2

        # Stage 1: raw -> intermediate
        gen1 = TaskGenerator(
            name="stage1:<name>",
            inputs={"raw": FileInput("raw/<name>.txt", base_path=tmp_path)},
            outputs=[FileOutput("intermediate/<name>.json", base_path=tmp_path)],
            action=stage1_action,
        )

        # Stage 2: intermediate -> final
        gen2 = TaskGenerator(
            name="stage2:<name>",
            inputs={"data": FileInput("intermediate/<name>.json", base_path=tmp_path)},
            outputs=[FileOutput("final/<name>.out", base_path=tmp_path)],
            action=stage2_action,
        )

        engine = ReactiveEngine(generators=[gen1, gen2])
        result = engine.run()

        # Stage 1 should run first
        assert "source" in stage1_executed

        # Stage 2 should be triggered by stage 1's output
        assert "source" in stage2_executed

        assert result.converged is True
        # At least 2 tasks (stage1 and stage2 for 'source')
        assert result.tasks_executed >= 2

    def test_max_tasks_limit(self, tmp_path):
        """Test that max_tasks limit prevents infinite loops."""
        from doit.taskgen import TaskGenerator, FileInput, FileOutput

        # Create a cycle: stage1 → stage2 → stage1 (new files)
        # This tests the max_tasks safety limit
        stage1_dir = tmp_path / "stage1"
        stage1_dir.mkdir()
        stage2_dir = tmp_path / "stage2"
        stage2_dir.mkdir()

        # Seed file to start the cycle
        (stage1_dir / "seed.txt").write_text("seed")

        task_counter = [0]

        def stage1_action(inp, out, attrs):
            def do_stage1():
                task_counter[0] += 1
                Path(out[0]).parent.mkdir(parents=True, exist_ok=True)
                Path(out[0]).write_text(f"stage1 {task_counter[0]}")
            return do_stage1

        def stage2_action(inp, out, attrs):
            def do_stage2():
                task_counter[0] += 1
                Path(out[0]).parent.mkdir(parents=True, exist_ok=True)
                # Output goes to stage1, triggering gen1 again
                Path(out[0]).write_text(f"stage2 {task_counter[0]}")
            return do_stage2

        # gen1: stage1/*.txt → stage2/*.json
        gen1 = TaskGenerator(
            name="stage1:<name>",
            inputs={"data": FileInput("stage1/<name>.txt", base_path=tmp_path)},
            outputs=[FileOutput("stage2/<name>.json", base_path=tmp_path)],
            action=stage1_action,
        )

        # gen2: stage2/*.json → stage1/<name>_next.txt (creates new gen1 input!)
        gen2 = TaskGenerator(
            name="stage2:<name>",
            inputs={"data": FileInput("stage2/<name>.json", base_path=tmp_path)},
            outputs=[FileOutput("stage1/<name>_next.txt", base_path=tmp_path)],
            action=stage2_action,
        )

        # Set a low limit
        engine = ReactiveEngine(generators=[gen1, gen2], max_tasks=5)
        result = engine.run()

        # Should hit the limit - the cycle would continue indefinitely
        # seed.txt → seed.json → seed_next.txt → seed_next.json → ...
        assert result.hit_limit is True
        assert result.tasks_executed <= 5

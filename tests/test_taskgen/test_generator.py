"""Tests for doit.taskgen.generator module."""

import pytest
from pathlib import Path

from doit.taskgen.generator import TaskGenerator
from doit.taskgen.inputs import FileInput
from doit.taskgen.outputs import FileOutput
from doit.task import Task
from doit.deps import FileDependency, FileTarget


class TestTaskGeneratorBasic:
    """Basic tests for TaskGenerator."""

    def test_generate_single_task(self, tmp_path):
        """Test generating a single task."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("int main() {}")

        gen = TaskGenerator(
            name="compile:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<module>.o")],
            action=lambda inp, out, attrs: f"gcc -c {out[0]}",
        )

        tasks = list(gen.generate())
        assert len(tasks) == 1
        assert tasks[0].name == "compile:main"

    def test_generate_multiple_tasks(self, tmp_path):
        """Test generating multiple tasks."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("main")
        (tmp_path / "src" / "utils.c").write_text("utils")

        gen = TaskGenerator(
            name="compile:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<module>.o")],
            action=lambda inp, out, attrs: f"gcc -c {out[0]}",
        )

        tasks = list(gen.generate())
        assert len(tasks) == 2
        names = {t.name for t in tasks}
        assert names == {"compile:main", "compile:utils"}


class TestTaskGeneratorNameRendering:
    """Tests for task name rendering."""

    def test_single_capture_in_name(self, tmp_path):
        """Test name with single capture."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "foo.c").write_text("code")

        gen = TaskGenerator(
            name="build:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("out/<module>.o")],
            action=lambda inp, out, attrs: "cmd",
        )

        tasks = list(gen.generate())
        assert tasks[0].name == "build:foo"

    def test_multiple_captures_in_name(self, tmp_path):
        """Test name with multiple captures."""
        (tmp_path / "src" / "x86").mkdir(parents=True)
        (tmp_path / "src" / "x86" / "main.c").write_text("code")

        gen = TaskGenerator(
            name="compile:<arch>:<module>",
            inputs={"source": FileInput("src/<arch>/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<arch>/<module>.o")],
            action=lambda inp, out, attrs: "cmd",
        )

        tasks = list(gen.generate())
        assert tasks[0].name == "compile:x86:main"


class TestTaskGeneratorActions:
    """Tests for action generation."""

    def test_action_receives_correct_args(self, tmp_path):
        """Test that action callback receives correct arguments."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")

        received_args = {}

        def capture_action(inp, out, attrs):
            received_args['inp'] = inp
            received_args['out'] = out
            received_args['attrs'] = attrs
            return "cmd"

        gen = TaskGenerator(
            name="test:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<module>.o")],
            action=capture_action,
        )

        list(gen.generate())

        assert 'source' in received_args['inp'].items
        assert received_args['out'] == ["build/main.o"]
        assert received_args['attrs'] == {"module": "main"}

    def test_action_single_value(self, tmp_path):
        """Test action returning single value."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")

        gen = TaskGenerator(
            name="test:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<module>.o")],
            action=lambda inp, out, attrs: "single_action",
        )

        tasks = list(gen.generate())
        assert len(tasks[0].actions) == 1
        # Task wraps strings in CmdAction
        assert "single_action" in str(tasks[0].actions[0])

    def test_action_list_value(self, tmp_path):
        """Test action returning list."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")

        gen = TaskGenerator(
            name="test:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<module>.o")],
            action=lambda inp, out, attrs: ["action1", "action2"],
        )

        tasks = list(gen.generate())
        assert len(tasks[0].actions) == 2

    def test_action_tuple_value(self, tmp_path):
        """Test action returning list via tuple (Python action)."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")

        def my_func():
            pass

        gen = TaskGenerator(
            name="test:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<module>.o")],
            # Return a list containing a tuple for python action (callable, args)
            action=lambda inp, out, attrs: [(my_func, [])],
        )

        tasks = list(gen.generate())
        assert len(tasks[0].actions) == 1


class TestTaskGeneratorDependencies:
    """Tests for dependency handling."""

    def test_task_has_dependencies(self, tmp_path):
        """Test that task includes input dependencies."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")

        gen = TaskGenerator(
            name="test:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<module>.o")],
            action=lambda inp, out, attrs: "cmd",
        )

        tasks = list(gen.generate())
        assert len(tasks[0].dependencies) == 1
        assert isinstance(tasks[0].dependencies[0], FileDependency)

    def test_multiple_inputs_create_multiple_deps(self, tmp_path):
        """Test that multiple inputs create multiple dependencies."""
        (tmp_path / "src").mkdir()
        (tmp_path / "cfg").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")
        (tmp_path / "cfg" / "main.json").write_text("{}")

        gen = TaskGenerator(
            name="test:<module>",
            inputs={
                "source": FileInput("src/<module>.c", base_path=tmp_path),
                "config": FileInput("cfg/<module>.json", base_path=tmp_path),
            },
            outputs=[FileOutput("build/<module>.o")],
            action=lambda inp, out, attrs: "cmd",
        )

        tasks = list(gen.generate())
        assert len(tasks[0].dependencies) == 2

    def test_is_list_input_creates_multiple_deps(self, tmp_path):
        """Test that is_list input creates list of dependencies."""
        (tmp_path / "src").mkdir()
        (tmp_path / "include").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")
        (tmp_path / "include" / "types.h").write_text("types")
        (tmp_path / "include" / "defs.h").write_text("defs")

        gen = TaskGenerator(
            name="test:<module>",
            inputs={
                "source": FileInput("src/<module>.c", base_path=tmp_path),
                "headers": FileInput("include/*.h", base_path=tmp_path, is_list=True),
            },
            outputs=[FileOutput("build/<module>.o")],
            action=lambda inp, out, attrs: "cmd",
        )

        tasks = list(gen.generate())
        # 1 source + 2 headers
        assert len(tasks[0].dependencies) == 3


class TestTaskGeneratorTargets:
    """Tests for target/output handling."""

    def test_task_has_targets(self, tmp_path):
        """Test that task includes Target objects via outputs."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")

        gen = TaskGenerator(
            name="test:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<module>.o")],
            action=lambda inp, out, attrs: "cmd",
        )

        tasks = list(gen.generate())
        # TaskGenerator uses outputs (Target objects) instead of targets (strings)
        assert len(tasks[0].outputs) == 1
        assert "build/main.o" in tasks[0].outputs[0].get_key()

    def test_task_has_outputs(self, tmp_path):
        """Test that task includes Target objects."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")

        gen = TaskGenerator(
            name="test:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<module>.o")],
            action=lambda inp, out, attrs: "cmd",
        )

        tasks = list(gen.generate())
        assert len(tasks[0].outputs) == 1
        assert isinstance(tasks[0].outputs[0], FileTarget)

    def test_multiple_outputs(self, tmp_path):
        """Test task with multiple outputs."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")

        gen = TaskGenerator(
            name="test:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[
                FileOutput("build/<module>.o"),
                FileOutput("build/<module>.d"),
            ],
            action=lambda inp, out, attrs: "cmd",
        )

        tasks = list(gen.generate())
        # TaskGenerator uses outputs (Target objects) instead of targets (strings)
        assert len(tasks[0].outputs) == 2
        keys = [o.get_key() for o in tasks[0].outputs]
        assert any("build/main.o" in k for k in keys)
        assert any("build/main.d" in k for k in keys)


class TestTaskGeneratorDoc:
    """Tests for doc string handling."""

    def test_doc_rendered(self, tmp_path):
        """Test that doc string is rendered."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")

        gen = TaskGenerator(
            name="compile:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<module>.o")],
            action=lambda inp, out, attrs: "cmd",
            doc="Compile <module>.c to <module>.o",
        )

        tasks = list(gen.generate())
        assert tasks[0].doc == "Compile main.c to main.o"

    def test_doc_none(self, tmp_path):
        """Test that None doc becomes empty string (Task default)."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")

        gen = TaskGenerator(
            name="test:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<module>.o")],
            action=lambda inp, out, attrs: "cmd",
        )

        tasks = list(gen.generate())
        # Task converts None to '' by default
        assert tasks[0].doc == ''


class TestTaskGeneratorMultipleDimensions:
    """Tests for multi-dimensional generation."""

    def test_two_dimensions(self, tmp_path):
        """Test generation with two capture dimensions."""
        for arch in ["x86", "arm"]:
            for module in ["main", "utils"]:
                path = tmp_path / "src" / arch
                path.mkdir(parents=True, exist_ok=True)
                (path / f"{module}.c").write_text(f"{arch} {module}")

        gen = TaskGenerator(
            name="compile:<arch>:<module>",
            inputs={"source": FileInput("src/<arch>/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<arch>/<module>.o")],
            action=lambda inp, out, attrs: f"gcc -o {out[0]}",
        )

        tasks = list(gen.generate())
        assert len(tasks) == 4

        names = {t.name for t in tasks}
        assert names == {
            "compile:x86:main", "compile:x86:utils",
            "compile:arm:main", "compile:arm:utils",
        }

    def test_action_uses_attrs(self, tmp_path):
        """Test that action can use attrs for conditional logic."""
        for arch in ["x86", "arm"]:
            (tmp_path / "src" / arch).mkdir(parents=True)
            (tmp_path / "src" / arch / "main.c").write_text("code")

        def arch_specific_action(inp, out, attrs):
            if attrs["arch"] == "x86":
                return "gcc -m32"
            else:
                return "arm-gcc"

        gen = TaskGenerator(
            name="compile:<arch>:<module>",
            inputs={"source": FileInput("src/<arch>/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<arch>/<module>.o")],
            action=arch_specific_action,
        )

        tasks = list(gen.generate())
        task_by_name = {t.name: t for t in tasks}

        assert len(task_by_name["compile:x86:main"].actions) == 1
        assert "gcc -m32" in str(task_by_name["compile:x86:main"].actions[0])
        assert "arm-gcc" in str(task_by_name["compile:arm:main"].actions[0])


class TestTaskGeneratorEdgeCases:
    """Tests for edge cases."""

    def test_no_matches_yields_nothing(self, tmp_path):
        """Test that no matches yields no tasks."""
        (tmp_path / "src").mkdir()

        gen = TaskGenerator(
            name="compile:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<module>.o")],
            action=lambda inp, out, attrs: "cmd",
        )

        tasks = list(gen.generate())
        assert len(tasks) == 0

    def test_empty_outputs(self, tmp_path):
        """Test generator with no outputs."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")

        gen = TaskGenerator(
            name="check:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[],
            action=lambda inp, out, attrs: "lint cmd",
        )

        tasks = list(gen.generate())
        assert len(tasks) == 1
        assert tasks[0].outputs == []

    def test_callable_action(self, tmp_path):
        """Test with callable (function) action."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")

        def my_action(source, target):
            pass

        gen = TaskGenerator(
            name="test:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<module>.o")],
            # Wrap tuple in list to indicate single action
            action=lambda inp, out, attrs: [(my_action, [str(inp['source'].path), out[0]])],
        )

        tasks = list(gen.generate())
        assert len(tasks[0].actions) == 1
        # PythonAction wraps the callable
        assert "my_action" in str(tasks[0].actions[0])

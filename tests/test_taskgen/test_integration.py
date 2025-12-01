"""Integration tests for doit.taskgen with DoitEngine."""

import pytest
from pathlib import Path
import os

from doit.taskgen import TaskGenerator, FileInput, FileOutput
from doit.engine import DoitEngine
from doit.dependency import InMemoryStateStore as MemoryStore


class TestImports:
    """Test that all exports are importable."""

    def test_import_from_package(self):
        """Test importing from doit.taskgen package."""
        from doit.taskgen import (
            Input, FileInput, S3Input, CaptureMatch,
            Output, FileOutput, S3Output,
            InputSet, build_input_sets,
            TaskGenerator,
        )
        # Just verify they're importable
        assert Input is not None
        assert TaskGenerator is not None

    def test_import_from_doit(self):
        """Test that taskgen is accessible from doit."""
        from doit import taskgen
        assert hasattr(taskgen, 'TaskGenerator')
        assert hasattr(taskgen, 'FileInput')
        assert hasattr(taskgen, 'FileOutput')


class TestTaskGeneratorWithDoitEngine:
    """Test TaskGenerator integration with DoitEngine."""

    def test_single_task_execution(self, tmp_path):
        """Test generating and executing a single task."""
        # Setup: create source file
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "main.c").write_text("int main() { return 0; }")

        build_dir = tmp_path / "build"

        # Track executed actions
        executed = []

        def compile_action(inp, out, attrs):
            def do_compile():
                executed.append(attrs["module"])
                # Create output file
                Path(out[0]).parent.mkdir(parents=True, exist_ok=True)
                Path(out[0]).write_text(f"compiled: {attrs['module']}")
            return do_compile

        gen = TaskGenerator(
            name="compile:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput(str(build_dir / "<module>.o"))],
            action=compile_action,
        )

        tasks = list(gen.generate())
        assert len(tasks) == 1
        assert tasks[0].name == "compile:main"

        # Execute with DoitEngine
        with DoitEngine(tasks, store=MemoryStore()) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        # Verify execution
        assert executed == ["main"]
        assert (build_dir / "main.o").exists()

    def test_multiple_tasks_execution(self, tmp_path):
        """Test generating and executing multiple tasks."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "main.c").write_text("main code")
        (src_dir / "utils.c").write_text("utils code")
        (src_dir / "helper.c").write_text("helper code")

        build_dir = tmp_path / "build"
        executed = []

        def compile_action(inp, out, attrs):
            def do_compile():
                executed.append(attrs["module"])
                Path(out[0]).parent.mkdir(parents=True, exist_ok=True)
                Path(out[0]).write_text(f"compiled: {attrs['module']}")
            return do_compile

        gen = TaskGenerator(
            name="compile:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput(str(build_dir / "<module>.o"))],
            action=compile_action,
        )

        tasks = list(gen.generate())
        assert len(tasks) == 3

        with DoitEngine(tasks, store=MemoryStore()) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        assert set(executed) == {"main", "utils", "helper"}

    def test_multi_dimensional_tasks(self, tmp_path):
        """Test with multiple capture dimensions."""
        for arch in ["x86", "arm"]:
            arch_dir = tmp_path / "src" / arch
            arch_dir.mkdir(parents=True)
            (arch_dir / "main.c").write_text(f"{arch} main")

        build_dir = tmp_path / "build"
        executed = []

        def compile_action(inp, out, attrs):
            def do_compile():
                executed.append((attrs["arch"], attrs["module"]))
                Path(out[0]).parent.mkdir(parents=True, exist_ok=True)
                Path(out[0]).write_text(f"compiled: {attrs['arch']}/{attrs['module']}")
            return do_compile

        gen = TaskGenerator(
            name="compile:<arch>:<module>",
            inputs={"source": FileInput("src/<arch>/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput(str(build_dir / "<arch>" / "<module>.o"))],
            action=compile_action,
        )

        tasks = list(gen.generate())
        assert len(tasks) == 2

        with DoitEngine(tasks, store=MemoryStore()) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        assert set(executed) == {("x86", "main"), ("arm", "main")}


class TestTaskDependencies:
    """Test that generated tasks have proper dependencies."""

    def test_implicit_deps_between_generators(self, tmp_path):
        """Test that tasks from different generators can have implicit deps."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "main.c").write_text("source")

        build_dir = tmp_path / "build"
        executed = []

        def compile_action(inp, out, attrs):
            def do_compile():
                executed.append(("compile", attrs["module"]))
                Path(out[0]).parent.mkdir(parents=True, exist_ok=True)
                Path(out[0]).write_text("object")
            return do_compile

        def link_action(inp, out, attrs):
            def do_link():
                executed.append(("link", attrs["module"]))
                Path(out[0]).parent.mkdir(parents=True, exist_ok=True)
                Path(out[0]).write_text("binary")
            return do_link

        # Compile: src/<module>.c -> build/<module>.o
        compile_gen = TaskGenerator(
            name="compile:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput(str(build_dir / "<module>.o"))],
            action=compile_action,
        )

        # Create the .o file first so link generator can find it
        build_dir.mkdir(parents=True, exist_ok=True)
        (build_dir / "main.o").write_text("object placeholder")

        # Link: build/<module>.o -> build/<module>.bin
        link_gen = TaskGenerator(
            name="link:<module>",
            inputs={"object": FileInput("build/<module>.o", base_path=tmp_path)},
            outputs=[FileOutput(str(build_dir / "<module>.bin"))],
            action=link_action,
        )

        compile_tasks = list(compile_gen.generate())
        link_tasks = list(link_gen.generate())
        all_tasks = compile_tasks + link_tasks

        # The link task should depend on the compile task via implicit deps
        with DoitEngine(all_tasks, store=MemoryStore()) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        # Compile should run before link (due to dependencies)
        compile_idx = executed.index(("compile", "main"))
        link_idx = executed.index(("link", "main"))
        assert compile_idx < link_idx


class TestRealisticWorkflow:
    """Test realistic workflow patterns."""

    def test_ocr_like_workflow(self, tmp_path):
        """Test a workflow similar to the OCR example from requirements."""
        # Setup: create page files for a document
        textract_dir = tmp_path / "textract"
        textract_dir.mkdir()
        (textract_dir / "doc1.page1.txt").write_text("page 1 content")
        (textract_dir / "doc1.page2.txt").write_text("page 2 content")

        pdf_dir = tmp_path / "pdfs"
        pdf_dir.mkdir()
        (pdf_dir / "doc1.pdf").write_text("pdf content")

        ocr_dir = tmp_path / "ocr"
        executed = []

        def ocr_action(inp, out, attrs):
            def do_ocr():
                executed.append(attrs["doc"])
                # Collect page contents
                pages = inp["pages"]  # This is a list
                page_content = " | ".join(
                    Path(p.path).read_text() for p in pages
                )
                Path(out[0]).parent.mkdir(parents=True, exist_ok=True)
                Path(out[0]).write_text(f"OCR: {page_content}")
            return do_ocr

        gen = TaskGenerator(
            name="ocr:<doc>",
            inputs={
                # Use relative patterns with base_path
                "pages": FileInput(
                    "textract/<doc>.page*.txt",
                    base_path=tmp_path,
                    is_list=True
                ),
                "pdf": FileInput("pdfs/<doc>.pdf", base_path=tmp_path),
            },
            outputs=[FileOutput(str(ocr_dir / "<doc>.md"))],
            action=ocr_action,
        )

        tasks = list(gen.generate())
        assert len(tasks) == 1
        assert tasks[0].name == "ocr:doc1"

        # Verify pages are collected as list
        pages_input = tasks[0].dependencies
        page_deps = [d for d in pages_input if 'page' in d.path]
        assert len(page_deps) == 2

        with DoitEngine(tasks, store=MemoryStore()) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        assert executed == ["doc1"]
        assert (ocr_dir / "doc1.md").exists()
        content = (ocr_dir / "doc1.md").read_text()
        assert "page 1 content" in content
        assert "page 2 content" in content


class TestEdgeCases:
    """Test edge cases in integration."""

    def test_no_matches_no_tasks(self, tmp_path):
        """Test that no matches yields no tasks."""
        gen = TaskGenerator(
            name="compile:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[FileOutput("build/<module>.o")],
            action=lambda inp, out, attrs: "cmd",
        )

        tasks = list(gen.generate())
        assert len(tasks) == 0

    def test_task_with_no_outputs(self, tmp_path):
        """Test lint-like task with no outputs."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "main.c").write_text("code")

        executed = []

        def lint_action(inp, out, attrs):
            def do_lint():
                executed.append(attrs["module"])
            return do_lint

        gen = TaskGenerator(
            name="lint:<module>",
            inputs={"source": FileInput("src/<module>.c", base_path=tmp_path)},
            outputs=[],
            action=lint_action,
        )

        tasks = list(gen.generate())
        assert len(tasks) == 1
        assert tasks[0].targets == []

        with DoitEngine(tasks, store=MemoryStore(), always_execute=True) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        assert executed == ["main"]

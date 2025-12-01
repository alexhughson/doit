"""Integration tests for DoitEngine programmatic API.

These tests validate end-to-end behavior with:
- Task dataclass instances (not dicts)
- Complex file dependency graphs
- Multi-stage pipelines with real file I/O
- Incremental rebuild scenarios
- Dynamic task injection
- Real shell commands
"""

import pytest
from pathlib import Path

from doit import DoitEngine, TaskStatus
from doit.task import Task
from doit.deps import FileDependency, TaskDependency


class Workspace:
    """Test workspace with convenient file operations and auto-cleanup."""

    def __init__(self, tmp_path: Path):
        self.root = tmp_path
        self.db_path = str(tmp_path / '.doit.db')

    def create_file(self, path: str, content: str = '') -> Path:
        """Create a file with content. Path is relative to workspace root."""
        full_path = self.root / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(content)
        return full_path

    def read_file(self, path: str) -> str:
        """Read file content. Path is relative to workspace root."""
        return (self.root / path).read_text()

    def exists(self, path: str) -> bool:
        """Check if file exists. Path is relative to workspace root."""
        return (self.root / path).exists()

    def path(self, path: str) -> Path:
        """Get absolute Path object. Path is relative to workspace root."""
        return self.root / path

    def engine(self, tasks, **kwargs):
        """Create DoitEngine with workspace db_path."""
        return DoitEngine(tasks, db_path=self.db_path, **kwargs)


@pytest.fixture
def ws(tmp_path):
    """Workspace fixture - files auto-cleaned after each test."""
    return Workspace(tmp_path)


class TestMultiStagePipeline:
    """Test multi-stage pipelines with file dependencies."""

    def test_multistage_pipeline_with_file_deps(self, ws):
        """
        A->B->C pipeline where each stage reads input and writes output.
        Verifies: correct execution order, file contents propagate correctly.
        """
        src = ws.create_file('source.txt', 'original')
        stage1_out = ws.path('stage1.txt')
        stage2_out = ws.path('stage2.txt')
        final_out = ws.path('final.txt')

        tasks = [
            Task('stage1', actions=[f'cat {src} | tr a-z A-Z > {stage1_out}'],
                 dependencies=[FileDependency(str(src))], targets=[str(stage1_out)]),
            Task('stage2', actions=[f'cat {stage1_out} | sed "s/$/!/" > {stage2_out}'],
                 dependencies=[FileDependency(str(stage1_out))], targets=[str(stage2_out)]),
            Task('final', actions=[f'cat {stage2_out} > {final_out}'],
                 dependencies=[FileDependency(str(stage2_out))], targets=[str(final_out)]),
        ]

        with ws.engine(tasks) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        assert final_out.read_text().strip() == 'ORIGINAL!'

    def test_modify_mid_pipeline_only_downstream_reruns(self, ws):
        """
        Pipeline: source->A->B->C->D. After full run, modify B's output.
        Only C and D should re-run, not A or B.
        """
        # Create a source file so task 'a' has file_dep for up-to-date checking
        source = ws.create_file('source.txt', 'initial')
        execution_log = ws.path('log.txt')
        a_out = ws.path('a.txt')
        b_out = ws.path('b.txt')
        c_out = ws.path('c.txt')
        d_out = ws.path('d.txt')

        def log_and_copy(name, src, dst):
            def action():
                with open(execution_log, 'a') as f:
                    f.write(f'{name}\n')
                if src:
                    dst.write_text(src.read_text() + f'+{name}')
                else:
                    dst.write_text(name)
                return True
            return action

        tasks = [
            Task('a', actions=[log_and_copy('a', None, a_out)],
                 dependencies=[FileDependency(str(source))], targets=[str(a_out)]),
            Task('b', actions=[log_and_copy('b', a_out, b_out)],
                 dependencies=[FileDependency(str(a_out))], targets=[str(b_out)]),
            Task('c', actions=[log_and_copy('c', b_out, c_out)],
                 dependencies=[FileDependency(str(b_out))], targets=[str(c_out)]),
            Task('d', actions=[log_and_copy('d', c_out, d_out)],
                 dependencies=[FileDependency(str(c_out))], targets=[str(d_out)]),
        ]

        # First run - all tasks execute
        with ws.engine(tasks) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        assert execution_log.read_text() == 'a\nb\nc\nd\n'
        execution_log.write_text('')  # Clear log

        # Modify B's output (simulating external change)
        b_out.write_text('MODIFIED')

        # Second run - only C and D should re-run
        with ws.engine(tasks) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        log_content = execution_log.read_text()
        assert 'a' not in log_content
        assert 'b' not in log_content
        assert 'c' in log_content
        assert 'd' in log_content


class TestDependencyGraphs:
    """Test complex dependency graph scenarios."""

    def test_diamond_dependency_graph(self, ws):
        """
        Diamond: A->B, A->C, B->D, C->D
        D should wait for both B and C. All files should have correct content.
        """
        a_out = ws.path('a.txt')
        b_out = ws.path('b.txt')
        c_out = ws.path('c.txt')
        d_out = ws.path('d.txt')

        tasks = [
            Task('a', actions=[f'echo "A" > {a_out}'], targets=[str(a_out)]),
            Task('b', actions=[f'cat {a_out} > {b_out} && echo "B" >> {b_out}'],
                 dependencies=[FileDependency(str(a_out))], targets=[str(b_out)]),
            Task('c', actions=[f'cat {a_out} > {c_out} && echo "C" >> {c_out}'],
                 dependencies=[FileDependency(str(a_out))], targets=[str(c_out)]),
            Task('d', actions=[f'cat {b_out} {c_out} > {d_out}'],
                 dependencies=[FileDependency(str(b_out)), FileDependency(str(c_out))],
                 targets=[str(d_out)]),
        ]

        with ws.engine(tasks) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        d_content = d_out.read_text()
        assert 'A' in d_content
        assert 'B' in d_content
        assert 'C' in d_content

    def test_producer_output_is_downstream_input(self, ws):
        """
        Producer creates file, downstream uses it as file_dep.
        Even if file exists from previous run, downstream waits for producer.
        """
        data_file = ws.path('data.txt')
        result_file = ws.path('result.txt')
        execution_order = ws.path('order.txt')

        def producer():
            with open(execution_order, 'a') as f:
                f.write('producer\n')
            data_file.write_text('produced_data')
            return True

        tasks = [
            Task('producer', actions=[producer], targets=[str(data_file)]),
            Task('consumer', actions=[f'cat {data_file} > {result_file} && echo "consumer" >> {execution_order}'],
                 dependencies=[FileDependency(str(data_file)), TaskDependency('producer')],
                 targets=[str(result_file)]),  # Explicit task_dep ensures ordering
        ]

        # Pre-create the data file (stale from "previous run")
        data_file.write_text('stale_data')

        with ws.engine(tasks) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        # Producer should have overwritten stale data
        assert result_file.read_text().strip() == 'produced_data'


class TestDynamicTaskInjection:
    """Test dynamic task injection scenarios."""

    def test_dynamic_tasks_with_file_deps(self, ws):
        """
        Generator task creates files, then we inject tasks that depend on those files.
        Injected tasks should execute and process the generated files.
        """
        generated_dir = ws.path('generated')
        output_dir = ws.path('output')
        generated_dir.mkdir()
        output_dir.mkdir()

        def generator():
            files = []
            for i in range(3):
                f = generated_dir / f'file_{i}.txt'
                f.write_text(f'content_{i}')
                files.append(str(f))
            return {'files': files}

        tasks = [Task('generate', actions=[generator])]

        with ws.engine(tasks) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

                if task.name == 'generate':
                    # Inject processing tasks for each generated file
                    for fpath in task.values['files']:
                        fname = Path(fpath).stem
                        out = output_dir / f'{fname}_processed.txt'
                        engine.add_task(Task(
                            f'process_{fname}',
                            actions=[f'cat {fpath} | tr a-z A-Z > {out}'],
                            dependencies=[FileDependency(fpath), TaskDependency('generate')],
                            targets=[str(out)],
                        ))

        # Verify all processed files exist with correct content
        for i in range(3):
            processed = output_dir / f'file_{i}_processed.txt'
            assert processed.exists()
            assert processed.read_text().strip() == f'CONTENT_{i}'


class TestBuildSystemSimulation:
    """Test complex build system scenarios."""

    def test_complex_build_system(self, ws):
        """
        Simulates a real build: source files -> compile -> link -> package.
        Multiple source files, parallel compilation possible, single link step.
        """
        src_dir = ws.path('src')
        build_dir = ws.path('build')
        src_dir.mkdir()
        build_dir.mkdir()

        # Create source files
        sources = ['main', 'utils', 'config']
        for name in sources:
            (src_dir / f'{name}.c').write_text(f'/* {name} source */')

        tasks = []

        # Compile tasks (one per source)
        for name in sources:
            src = src_dir / f'{name}.c'
            obj = build_dir / f'{name}.o'
            tasks.append(Task(
                f'compile_{name}',
                actions=[f'cp {src} {obj}'],  # Simulate compilation
                dependencies=[FileDependency(str(src))],
                targets=[str(obj)],
            ))

        # Link task (depends on all objects)
        obj_files = [str(build_dir / f'{n}.o') for n in sources]
        binary = build_dir / 'app'
        tasks.append(Task(
            'link',
            actions=[f'cat {" ".join(obj_files)} > {binary}'],
            dependencies=[FileDependency(f) for f in obj_files] +
                        [TaskDependency(f'compile_{n}') for n in sources],
            targets=[str(binary)],
        ))

        # Package task
        package = build_dir / 'app.tar'
        tasks.append(Task(
            'package',
            actions=[f'tar cf {package} -C {build_dir} app'],
            dependencies=[FileDependency(str(binary)), TaskDependency('link')],
            targets=[str(package)],
        ))

        with ws.engine(tasks) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        assert binary.exists()
        assert package.exists()

    def test_incremental_rebuild_after_source_change(self, ws):
        """
        Build system, then modify one source file.
        Only that file's compile + link should re-run.
        """
        src_dir = ws.path('src')
        build_dir = ws.path('build')
        src_dir.mkdir()
        build_dir.mkdir()

        execution_log = ws.path('log.txt')

        sources = ['main', 'utils']
        for name in sources:
            (src_dir / f'{name}.c').write_text(f'/* {name} v1 */')

        def make_compile_action(name, src, obj):
            def action():
                with open(execution_log, 'a') as f:
                    f.write(f'compile_{name}\n')
                obj.write_text(src.read_text())
                return True
            return action

        def make_link_action(objs, binary):
            def action():
                with open(execution_log, 'a') as f:
                    f.write('link\n')
                binary.write_text('\n'.join(o.read_text() for o in objs))
                return True
            return action

        tasks = []
        obj_files = []
        for name in sources:
            src = src_dir / f'{name}.c'
            obj = build_dir / f'{name}.o'
            obj_files.append(obj)
            tasks.append(Task(
                f'compile_{name}',
                actions=[make_compile_action(name, src, obj)],
                dependencies=[FileDependency(str(src))],
                targets=[str(obj)],
            ))

        binary = build_dir / 'app'
        tasks.append(Task(
            'link',
            actions=[make_link_action(obj_files, binary)],
            dependencies=[FileDependency(str(o)) for o in obj_files],
            targets=[str(binary)],
        ))

        # First full build
        with ws.engine(tasks) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        assert 'compile_main' in execution_log.read_text()
        assert 'compile_utils' in execution_log.read_text()
        assert 'link' in execution_log.read_text()
        execution_log.write_text('')  # Clear log

        # Modify only utils.c
        (src_dir / 'utils.c').write_text('/* utils v2 */')

        # Incremental rebuild
        with ws.engine(tasks) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        log = execution_log.read_text()
        assert 'compile_main' not in log  # Should NOT re-run
        assert 'compile_utils' in log     # Should re-run
        assert 'link' in log              # Should re-run (dependency changed)


class TestFailureHandling:
    """Test failure handling scenarios."""

    def test_shell_command_failure(self, ws):
        """Shell command that exits non-zero is properly detected as failure."""
        tasks = [Task('fail', actions=['exit 1'])]

        with ws.engine(tasks) as engine:
            for task in engine:
                result = task.execute_and_submit()
                assert result is not None  # failure
                assert task.status == TaskStatus.FAILURE

    def test_failure_stops_dependents(self, ws):
        """When a task fails, dependent tasks don't execute."""
        marker = ws.path('after_fail_ran.txt')

        tasks = [
            Task('first', actions=[lambda: True]),
            Task('fail', actions=[lambda: False], dependencies=[TaskDependency('first')]),
            Task('after_fail', actions=[f'touch {marker}'], dependencies=[TaskDependency('fail')]),
        ]

        with ws.engine(tasks) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        assert not marker.exists()  # Should not have run


class TestValuePassing:
    """Test getargs and value passing scenarios."""

    def test_getargs_with_file_values(self, ws):
        """Getargs passes file path from producer, consumer reads that file."""
        data_file = ws.path('data.txt')
        result_file = ws.path('result.txt')

        def producer():
            data_file.write_text('secret_content')
            return {'path': str(data_file)}

        def consumer(path):
            content = Path(path).read_text()
            result_file.write_text(f'got: {content}')
            return True

        tasks = [
            Task('produce', actions=[producer]),
            Task('consume', actions=[(consumer,)],
                 getargs={'path': ('produce', 'path')},
                 dependencies=[TaskDependency('produce')]),
        ]

        with ws.engine(tasks) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        assert result_file.read_text() == 'got: secret_content'


class TestTeardown:
    """Test teardown behavior."""

    def test_teardown_runs_on_failure(self, ws):
        """Teardown actions execute even when task fails."""
        teardown_marker = ws.path('teardown_ran.txt')

        def teardown_action():
            teardown_marker.write_text('yes')

        tasks = [Task(
            'task1',
            actions=[lambda: False],  # Fail
            teardown=[teardown_action],
        )]

        with ws.engine(tasks) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        assert teardown_marker.exists()
        assert teardown_marker.read_text() == 'yes'


class TestCallbacks:
    """Test execution callbacks."""

    def test_callback_sequence_multiple_tasks(self, ws):
        """Callbacks fire in correct order across multiple tasks."""
        events = []

        class TrackingCallbacks:
            def on_status_check(self, task):
                events.append(('check', task.name))
            def on_execute(self, task):
                events.append(('exec', task.name))
            def on_success(self, task):
                events.append(('success', task.name))
            def on_failure(self, task, error):
                events.append(('fail', task.name))
            def on_skip_uptodate(self, task):
                events.append(('skip', task.name))
            def on_skip_ignored(self, task):
                pass
            def on_teardown(self, task):
                pass

        tasks = [
            Task('a', actions=[lambda: True]),
            Task('b', actions=[lambda: True], dependencies=[TaskDependency('a')]),
        ]

        with ws.engine(tasks, callbacks=TrackingCallbacks()) as engine:
            for task in engine:
                if task.should_run:
                    task.execute_and_submit()

        # Verify A completes before B starts
        a_success_idx = events.index(('success', 'a'))
        b_exec_idx = events.index(('exec', 'b'))
        assert a_success_idx < b_exec_idx

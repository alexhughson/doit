"""Integration tests for YAML task execution."""

import pytest
from pathlib import Path

pytest.importorskip("yaml")

from doit.yaml import run_yaml, parse_yaml_file, yaml_to_generators
from doit.yaml.parser import parse_yaml_string


class TestYAMLIntegration:
    """End-to-end tests for YAML task execution."""

    def test_simple_workflow(self, tmp_path):
        """Test a simple single-stage workflow."""
        # Create input files
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "main.txt").write_text("content1")
        (src_dir / "utils.txt").write_text("content2")

        # Create output directory
        build_dir = tmp_path / "build"
        build_dir.mkdir()

        # Write YAML file
        yaml_file = tmp_path / "doit.yaml"
        yaml_file.write_text("""
config:
  base_path: .

generators:
  - name: "process:<module>"
    inputs:
      source: "src/<module>.txt"
    outputs:
      - "build/<module>.out"
    action: "cp {source} {out_0}"
""")

        result = run_yaml(yaml_file, base_path=tmp_path)

        assert result.converged is True
        assert result.tasks_executed == 2

        # Check outputs were created
        assert (build_dir / "main.out").exists()
        assert (build_dir / "utils.out").exists()

    def test_cascading_workflow(self, tmp_path):
        """Test a two-stage cascading workflow."""
        # Create input files
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        (raw_dir / "data.txt").write_text("raw data")

        # Create output directories
        (tmp_path / "processed").mkdir()
        (tmp_path / "final").mkdir()

        # Write YAML file
        yaml_file = tmp_path / "doit.yaml"
        yaml_file.write_text("""
config:
  base_path: .

generators:
  - name: "stage1:<name>"
    inputs:
      raw: "raw/<name>.txt"
    outputs:
      - "processed/<name>.json"
    action: "cp {raw} {out_0}"

  - name: "stage2:<name>"
    inputs:
      data: "processed/<name>.json"
    outputs:
      - "final/<name>.out"
    action: "cp {data} {out_0}"
""")

        result = run_yaml(yaml_file, base_path=tmp_path)

        assert result.converged is True
        # Stage1 creates processed/data.json
        # Stage2 picks it up and creates final/data.out
        assert result.tasks_executed >= 2

        assert (tmp_path / "processed" / "data.json").exists()
        assert (tmp_path / "final" / "data.out").exists()

    def test_max_tasks_limit(self, tmp_path):
        """Test that max_tasks limit stops execution."""
        # Create a cycling setup
        stage1_dir = tmp_path / "stage1"
        stage1_dir.mkdir()
        (stage1_dir / "seed.txt").write_text("seed")

        (tmp_path / "stage2").mkdir()

        # Write YAML file with cycle
        yaml_file = tmp_path / "doit.yaml"
        yaml_file.write_text("""
config:
  base_path: .
  max_tasks: 3

generators:
  - name: "stage1:<name>"
    inputs:
      data: "stage1/<name>.txt"
    outputs:
      - "stage2/<name>.json"
    action: "cp {data} {out_0}"

  - name: "stage2:<name>"
    inputs:
      data: "stage2/<name>.json"
    outputs:
      - "stage1/<name>_next.txt"
    action: "cp {data} {out_0}"
""")

        result = run_yaml(yaml_file, base_path=tmp_path)

        # Should hit the limit
        assert result.hit_limit is True
        assert result.tasks_executed <= 3

    def test_env_var_usage(self, tmp_path):
        """Test that environment variables are set correctly."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "test.txt").write_text("data")

        (tmp_path / "build").mkdir()

        yaml_file = tmp_path / "doit.yaml"
        yaml_file.write_text("""
config:
  base_path: .

generators:
  - name: "test:<module>"
    inputs:
      source: "src/<module>.txt"
    outputs:
      - "build/<module>.out"
    action: "sh -c 'echo $source > {out_0}'"
""")

        result = run_yaml(yaml_file, base_path=tmp_path)

        assert result.converged is True
        # The output should contain the source path
        output = (tmp_path / "build" / "test.out").read_text()
        assert "src" in output and "test.txt" in output


class TestYAMLDryRun:
    """Tests for dry-run functionality."""

    def test_dry_run_shows_generators(self, tmp_path, capsys):
        """Test that dry run shows generator info."""
        from doit.yaml.runner import main

        # Create input files
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "main.c").write_text("")
        (src_dir / "utils.c").write_text("")

        yaml_file = tmp_path / "doit.yaml"
        yaml_file.write_text("""
generators:
  - name: "compile:<module>"
    inputs:
      source: "src/<module>.c"
    outputs:
      - "build/<module>.o"
    action: "gcc -c {source} -o {out_0}"
""")

        result = main(['--dry-run', str(yaml_file)])

        assert result == 0

        captured = capsys.readouterr()
        assert 'compile:<module>' in captured.out
        assert 'compile:main' in captured.out
        assert 'compile:utils' in captured.out


class TestYAMLMultiCapture:
    """Tests for generators with multiple captures."""

    def test_multi_capture_cartesian(self, tmp_path):
        """Test generator with multiple captures."""
        # Create input files with two dimensions
        src_dir = tmp_path / "src"
        (src_dir / "x86").mkdir(parents=True)
        (src_dir / "arm").mkdir()
        (src_dir / "x86" / "main.c").write_text("")
        (src_dir / "x86" / "utils.c").write_text("")
        (src_dir / "arm" / "main.c").write_text("")

        (tmp_path / "build" / "x86").mkdir(parents=True)
        (tmp_path / "build" / "arm").mkdir()

        yaml = """
generators:
  - name: "compile:<arch>:<module>"
    inputs:
      source: "src/<arch>/<module>.c"
    outputs:
      - "build/<arch>/<module>.o"
    action: "touch {out_0}"
"""
        config = parse_yaml_string(yaml)
        generators = yaml_to_generators(config, tmp_path)

        tasks = list(generators[0].generate())

        # Should generate: x86:main, x86:utils, arm:main
        assert len(tasks) == 3
        task_names = {t.name for t in tasks}
        assert 'compile:x86:main' in task_names
        assert 'compile:x86:utils' in task_names
        assert 'compile:arm:main' in task_names

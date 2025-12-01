"""Tests for YAML to TaskGenerator converter."""

import pytest
from pathlib import Path

pytest.importorskip("yaml")

from doit.yaml.parser import parse_yaml_string
from doit.yaml.converter import yaml_to_generator, yaml_to_generators


class TestYAMLToGenerator:
    """Tests for yaml_to_generator function."""

    def test_simple_generator(self, tmp_path):
        """Test converting a simple generator."""
        yaml = """
generators:
  - name: "compile:<module>"
    inputs:
      source: "src/<module>.c"
    outputs:
      - "build/<module>.o"
    action: "gcc -c {source} -o {out_0}"
"""
        config = parse_yaml_string(yaml)
        gen = yaml_to_generator(config.generators[0], tmp_path)

        assert gen.name == "compile:<module>"
        assert 'source' in gen.inputs
        assert len(gen.outputs) == 1

    def test_generator_with_doc(self, tmp_path):
        """Test generator with doc field."""
        yaml = """
generators:
  - name: "compile:<module>"
    inputs:
      source: "src/<module>.c"
    outputs:
      - "build/<module>.o"
    action: "gcc -c {source}"
    doc: "Compile {module}"
"""
        config = parse_yaml_string(yaml)
        gen = yaml_to_generator(config.generators[0], tmp_path)

        assert gen.doc == "Compile {module}"

    def test_long_form_input(self, tmp_path):
        """Test generator with long form input."""
        yaml = """
generators:
  - name: "test"
    inputs:
      headers:
        pattern: "include/*.h"
        is_list: true
        required: false
    outputs:
      - "out.txt"
    action: "echo {headers}"
"""
        config = parse_yaml_string(yaml)
        gen = yaml_to_generator(config.generators[0], tmp_path)

        assert 'headers' in gen.inputs
        assert gen.inputs['headers'].is_list is True
        assert gen.inputs['headers'].required is False

    def test_multiple_inputs(self, tmp_path):
        """Test generator with multiple inputs."""
        yaml = """
generators:
  - name: "compile:<arch>:<module>"
    inputs:
      source: "src/<arch>/<module>.c"
      header: "include/<arch>/<module>.h"
    outputs:
      - "build/<arch>/<module>.o"
    action: "gcc -c {source} -include {header} -o {out_0}"
"""
        config = parse_yaml_string(yaml)
        gen = yaml_to_generator(config.generators[0], tmp_path)

        assert 'source' in gen.inputs
        assert 'header' in gen.inputs

    def test_multiple_outputs(self, tmp_path):
        """Test generator with multiple outputs."""
        yaml = """
generators:
  - name: "compile:<module>"
    inputs:
      source: "src/<module>.c"
    outputs:
      - "build/<module>.o"
      - "build/<module>.d"
    action: "gcc -c {source} -o {out_0} -MD -MF {out_1}"
"""
        config = parse_yaml_string(yaml)
        gen = yaml_to_generator(config.generators[0], tmp_path)

        assert len(gen.outputs) == 2


class TestYAMLToGenerators:
    """Tests for yaml_to_generators function."""

    def test_empty_generators(self, tmp_path):
        """Test with no generators."""
        config = parse_yaml_string("generators: []")
        generators = yaml_to_generators(config, tmp_path)
        assert generators == []

    def test_multiple_generators(self, tmp_path):
        """Test converting multiple generators."""
        yaml = """
generators:
  - name: "gen1"
    inputs:
      a: "a.txt"
    outputs:
      - "b.txt"
    action: "cmd1"

  - name: "gen2"
    inputs:
      b: "b.txt"
    outputs:
      - "c.txt"
    action: "cmd2"
"""
        config = parse_yaml_string(yaml)
        generators = yaml_to_generators(config, tmp_path)

        assert len(generators) == 2
        assert generators[0].name == 'gen1'
        assert generators[1].name == 'gen2'

    def test_uses_config_base_path(self, tmp_path):
        """Test that base_path from config is used."""
        yaml = """
config:
  base_path: /custom/path

generators:
  - name: "test"
    inputs:
      source: "src/*.c"
    outputs:
      - "build/*.o"
    action: "gcc"
"""
        config = parse_yaml_string(yaml)
        # When base_path is not provided, should use config value
        generators = yaml_to_generators(config)

        # The input should have been created with /custom/path
        assert generators[0].inputs['source'].base_path == Path('/custom/path')

    def test_override_base_path(self, tmp_path):
        """Test that explicit base_path overrides config."""
        yaml = """
config:
  base_path: /config/path

generators:
  - name: "test"
    inputs:
      source: "src/*.c"
    outputs:
      - "build/*.o"
    action: "gcc"
"""
        config = parse_yaml_string(yaml)
        generators = yaml_to_generators(config, base_path=tmp_path)

        # Should use the explicit base_path
        assert generators[0].inputs['source'].base_path == tmp_path


class TestTaskGeneration:
    """Tests for actual task generation from YAML."""

    def test_generate_tasks(self, tmp_path):
        """Test generating tasks from YAML generator."""
        # Create input files
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "main.c").write_text("int main() {}")
        (src_dir / "utils.c").write_text("void util() {}")

        yaml = """
generators:
  - name: "compile:<module>"
    inputs:
      source: "src/<module>.c"
    outputs:
      - "build/<module>.o"
    action: "gcc -c {source} -o {out_0}"
"""
        config = parse_yaml_string(yaml)
        gen = yaml_to_generator(config.generators[0], tmp_path)

        tasks = list(gen.generate())

        assert len(tasks) == 2
        task_names = {t.name for t in tasks}
        assert 'compile:main' in task_names
        assert 'compile:utils' in task_names

    def test_action_factory_creates_shell_action(self, tmp_path):
        """Test that action factory creates ShellAction."""
        from doit.yaml.action import ShellAction

        # Create input file
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "test.c").write_text("")

        yaml = """
generators:
  - name: "compile:<module>"
    inputs:
      source: "src/<module>.c"
    outputs:
      - "build/<module>.o"
    action: "gcc -c {source} -o {out_0}"
"""
        config = parse_yaml_string(yaml)
        gen = yaml_to_generator(config.generators[0], tmp_path)

        tasks = list(gen.generate())
        assert len(tasks) == 1

        # The action should be a ShellAction instance (wrapped by doit)
        task = tasks[0]
        action = task.actions[0]
        # Check it's our ShellAction - doit wraps it, so check via repr
        assert 'ShellAction' in repr(action)

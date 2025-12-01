"""Tests for YAML parser."""

import pytest
from pathlib import Path

pytest.importorskip("yaml")

from doit.yaml.parser import (
    parse_yaml_file,
    parse_yaml_string,
    YAMLConfig,
    YAMLParseError,
)


class TestParseYAMLString:
    """Tests for parse_yaml_string function."""

    def test_empty_yaml(self):
        """Test parsing empty YAML."""
        config = parse_yaml_string("")
        assert config.config == {}
        assert config.generators == []

    def test_config_only(self):
        """Test parsing config without generators."""
        yaml = """
config:
  max_tasks: 5000
  base_path: /data
"""
        config = parse_yaml_string(yaml)
        assert config.config['max_tasks'] == 5000
        assert config.config['base_path'] == '/data'
        assert config.generators == []

    def test_simple_generator(self):
        """Test parsing a simple generator."""
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
        assert len(config.generators) == 1

        gen = config.generators[0]
        assert gen['name'] == "compile:<module>"
        assert 'source' in gen['inputs']
        assert gen['inputs']['source'] == "src/<module>.c"
        assert gen['outputs'] == ["build/<module>.o"]
        assert gen['action'] == "gcc -c {source} -o {out_0}"

    def test_long_form_input(self):
        """Test parsing long form input specification."""
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
        gen = config.generators[0]

        assert gen['inputs']['headers']['pattern'] == "include/*.h"
        assert gen['inputs']['headers']['is_list'] is True
        assert gen['inputs']['headers']['required'] is False

    def test_s3_input(self):
        """Test parsing S3 input specification."""
        yaml = """
generators:
  - name: "download:<file>"
    inputs:
      data:
        type: s3
        pattern: "raw/<file>.parquet"
        bucket: "my-bucket"
        profile: "dev"
    outputs:
      - "local/<file>.parquet"
    action: "aws s3 cp {data} {out_0}"
"""
        config = parse_yaml_string(yaml)
        gen = config.generators[0]

        assert gen['inputs']['data']['type'] == 's3'
        assert gen['inputs']['data']['bucket'] == 'my-bucket'
        assert gen['inputs']['data']['profile'] == 'dev'

    def test_long_form_output(self):
        """Test parsing long form output specification."""
        yaml = """
generators:
  - name: "test"
    inputs:
      data: "input.txt"
    outputs:
      - path: "output/<name>.txt"
        type: file
      - path: "generated/"
        type: directory
    action: "process {data}"
"""
        config = parse_yaml_string(yaml)
        gen = config.generators[0]

        assert len(gen['outputs']) == 2
        assert gen['outputs'][0]['path'] == "output/<name>.txt"
        assert gen['outputs'][0]['type'] == 'file'
        assert gen['outputs'][1]['type'] == 'directory'

    def test_multiple_generators(self):
        """Test parsing multiple generators."""
        yaml = """
generators:
  - name: "gen1"
    inputs:
      a: "a.txt"
    outputs:
      - "b.txt"
    action: "process1"

  - name: "gen2"
    inputs:
      b: "b.txt"
    outputs:
      - "c.txt"
    action: "process2"
"""
        config = parse_yaml_string(yaml)
        assert len(config.generators) == 2
        assert config.generators[0]['name'] == 'gen1'
        assert config.generators[1]['name'] == 'gen2'

    def test_doc_field(self):
        """Test parsing doc field."""
        yaml = """
generators:
  - name: "compile:<module>"
    inputs:
      source: "src/<module>.c"
    outputs:
      - "build/<module>.o"
    action: "gcc -c {source}"
    doc: "Compile {module} source file"
"""
        config = parse_yaml_string(yaml)
        assert config.generators[0]['doc'] == "Compile {module} source file"


class TestParseYAMLStringErrors:
    """Tests for parse error handling."""

    def test_invalid_yaml_syntax(self):
        """Test error on invalid YAML syntax."""
        with pytest.raises(YAMLParseError, match="Invalid YAML syntax"):
            parse_yaml_string("not: valid: yaml:")

    def test_non_mapping_root(self):
        """Test error when root is not a mapping."""
        with pytest.raises(YAMLParseError, match="root must be a mapping"):
            parse_yaml_string("- item1\n- item2")

    def test_non_list_generators(self):
        """Test error when generators is not a list."""
        with pytest.raises(YAMLParseError, match="'generators' must be a list"):
            parse_yaml_string("generators: not_a_list")

    def test_missing_name(self):
        """Test error when generator missing name."""
        yaml = """
generators:
  - inputs:
      a: "a.txt"
    outputs:
      - "b.txt"
    action: "cmd"
"""
        with pytest.raises(YAMLParseError, match="missing required field 'name'"):
            parse_yaml_string(yaml)

    def test_missing_inputs(self):
        """Test error when generator missing inputs."""
        yaml = """
generators:
  - name: "test"
    outputs:
      - "b.txt"
    action: "cmd"
"""
        with pytest.raises(YAMLParseError, match="missing required field 'inputs'"):
            parse_yaml_string(yaml)

    def test_missing_outputs(self):
        """Test error when generator missing outputs."""
        yaml = """
generators:
  - name: "test"
    inputs:
      a: "a.txt"
    action: "cmd"
"""
        with pytest.raises(YAMLParseError, match="missing required field 'outputs'"):
            parse_yaml_string(yaml)

    def test_missing_action(self):
        """Test error when generator missing action."""
        yaml = """
generators:
  - name: "test"
    inputs:
      a: "a.txt"
    outputs:
      - "b.txt"
"""
        with pytest.raises(YAMLParseError, match="missing required field 'action'"):
            parse_yaml_string(yaml)

    def test_s3_missing_bucket(self):
        """Test error when S3 input missing bucket."""
        yaml = """
generators:
  - name: "test"
    inputs:
      data:
        type: s3
        pattern: "data/*.parquet"
    outputs:
      - "out.txt"
    action: "cmd"
"""
        with pytest.raises(YAMLParseError, match="S3 input .* missing 'bucket'"):
            parse_yaml_string(yaml)

    def test_invalid_input_type(self):
        """Test error on invalid input type."""
        yaml = """
generators:
  - name: "test"
    inputs:
      data:
        type: invalid
        pattern: "*.txt"
    outputs:
      - "out.txt"
    action: "cmd"
"""
        with pytest.raises(YAMLParseError, match="invalid type 'invalid'"):
            parse_yaml_string(yaml)


class TestParseYAMLFile:
    """Tests for parse_yaml_file function."""

    def test_file_not_found(self, tmp_path):
        """Test error when file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            parse_yaml_file(tmp_path / "nonexistent.yaml")

    def test_parse_file(self, tmp_path):
        """Test parsing from file."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("""
generators:
  - name: "test"
    inputs:
      source: "src/*.c"
    outputs:
      - "build/*.o"
    action: "gcc -c {source}"
""")
        config = parse_yaml_file(yaml_file)
        assert len(config.generators) == 1
        assert config.generators[0]['name'] == 'test'

    def test_parse_file_path_object(self, tmp_path):
        """Test parsing with Path object."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("generators: []")
        config = parse_yaml_file(yaml_file)
        assert config.generators == []

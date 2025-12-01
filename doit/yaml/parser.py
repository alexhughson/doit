"""YAML parsing and validation for doit task definitions.

This module handles parsing doit.yaml files and validating their structure.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

try:
    import yaml
except ImportError:
    yaml = None


@dataclass
class YAMLConfig:
    """Parsed YAML configuration."""
    config: Dict[str, Any] = field(default_factory=dict)
    generators: List[Dict[str, Any]] = field(default_factory=list)


class YAMLParseError(Exception):
    """Error parsing or validating YAML file."""
    pass


def _ensure_yaml_available():
    """Raise ImportError if PyYAML is not installed."""
    if yaml is None:
        raise ImportError(
            "PyYAML is required for YAML task definitions. "
            "Install it with: pip install pyyaml"
        )


def parse_yaml_file(path: Union[str, Path]) -> YAMLConfig:
    """Parse and validate a doit.yaml file.

    Args:
        path: Path to the YAML file

    Returns:
        YAMLConfig with parsed configuration and generators

    Raises:
        YAMLParseError: If the file is invalid or missing required fields
        FileNotFoundError: If the file doesn't exist
        ImportError: If PyYAML is not installed
    """
    _ensure_yaml_available()

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"YAML file not found: {path}")

    with open(path) as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise YAMLParseError(f"Invalid YAML syntax: {e}")

    if data is None:
        data = {}

    if not isinstance(data, dict):
        raise YAMLParseError("YAML root must be a mapping")

    return _validate_yaml_data(data)


def parse_yaml_string(content: str) -> YAMLConfig:
    """Parse YAML content from a string.

    Args:
        content: YAML content as string

    Returns:
        YAMLConfig with parsed configuration and generators
    """
    _ensure_yaml_available()

    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as e:
        raise YAMLParseError(f"Invalid YAML syntax: {e}")

    if data is None:
        data = {}

    if not isinstance(data, dict):
        raise YAMLParseError("YAML root must be a mapping")

    return _validate_yaml_data(data)


def _validate_yaml_data(data: Dict[str, Any]) -> YAMLConfig:
    """Validate parsed YAML data structure.

    Args:
        data: Parsed YAML dictionary

    Returns:
        Validated YAMLConfig

    Raises:
        YAMLParseError: If validation fails
    """
    config = data.get('config', {})
    if not isinstance(config, dict):
        raise YAMLParseError("'config' must be a mapping")

    generators = data.get('generators', [])
    if not isinstance(generators, list):
        raise YAMLParseError("'generators' must be a list")

    validated_generators = []
    for i, gen in enumerate(generators):
        validated_generators.append(_validate_generator(gen, i))

    return YAMLConfig(config=config, generators=validated_generators)


def _validate_generator(gen: Dict[str, Any], index: int) -> Dict[str, Any]:
    """Validate a single generator definition.

    Args:
        gen: Generator dictionary
        index: Index in generators list (for error messages)

    Returns:
        Validated generator dictionary

    Raises:
        YAMLParseError: If validation fails
    """
    if not isinstance(gen, dict):
        raise YAMLParseError(f"Generator {index} must be a mapping")

    # Required fields
    if 'name' not in gen:
        raise YAMLParseError(f"Generator {index} missing required field 'name'")
    if not isinstance(gen['name'], str):
        raise YAMLParseError(f"Generator {index}: 'name' must be a string")

    if 'inputs' not in gen:
        raise YAMLParseError(f"Generator '{gen['name']}' missing required field 'inputs'")
    if not isinstance(gen['inputs'], dict):
        raise YAMLParseError(f"Generator '{gen['name']}': 'inputs' must be a mapping")

    if 'outputs' not in gen:
        raise YAMLParseError(f"Generator '{gen['name']}' missing required field 'outputs'")
    if not isinstance(gen['outputs'], list):
        raise YAMLParseError(f"Generator '{gen['name']}': 'outputs' must be a list")

    if 'action' not in gen:
        raise YAMLParseError(f"Generator '{gen['name']}' missing required field 'action'")
    if not isinstance(gen['action'], str):
        raise YAMLParseError(f"Generator '{gen['name']}': 'action' must be a string")

    # Validate inputs
    for label, spec in gen['inputs'].items():
        _validate_input_spec(spec, gen['name'], label)

    # Validate outputs
    for i, out_spec in enumerate(gen['outputs']):
        _validate_output_spec(out_spec, gen['name'], i)

    # Optional fields
    if 'doc' in gen and not isinstance(gen.get('doc'), str):
        raise YAMLParseError(f"Generator '{gen['name']}': 'doc' must be a string")

    return gen


def _validate_input_spec(spec: Any, gen_name: str, label: str) -> None:
    """Validate an input specification.

    Args:
        spec: Input specification (string or dict)
        gen_name: Generator name (for error messages)
        label: Input label (for error messages)

    Raises:
        YAMLParseError: If validation fails
    """
    if isinstance(spec, str):
        # Short form: just a pattern string
        return

    if not isinstance(spec, dict):
        raise YAMLParseError(
            f"Generator '{gen_name}': input '{label}' must be a string or mapping"
        )

    # Long form: dict with pattern and options
    if 'pattern' not in spec:
        raise YAMLParseError(
            f"Generator '{gen_name}': input '{label}' missing 'pattern'"
        )

    input_type = spec.get('type', 'file')
    valid_types = {'file', 's3', 'directory'}
    if input_type not in valid_types:
        raise YAMLParseError(
            f"Generator '{gen_name}': input '{label}' has invalid type '{input_type}'. "
            f"Valid types: {valid_types}"
        )

    if input_type == 's3':
        if 'bucket' not in spec:
            raise YAMLParseError(
                f"Generator '{gen_name}': S3 input '{label}' missing 'bucket'"
            )


def _validate_output_spec(spec: Any, gen_name: str, index: int) -> None:
    """Validate an output specification.

    Args:
        spec: Output specification (string or dict)
        gen_name: Generator name (for error messages)
        index: Output index (for error messages)

    Raises:
        YAMLParseError: If validation fails
    """
    if isinstance(spec, str):
        # Short form: just a pattern string
        return

    if not isinstance(spec, dict):
        raise YAMLParseError(
            f"Generator '{gen_name}': output {index} must be a string or mapping"
        )

    # Long form: dict with path and options
    if 'path' not in spec:
        raise YAMLParseError(
            f"Generator '{gen_name}': output {index} missing 'path'"
        )

    output_type = spec.get('type', 'file')
    valid_types = {'file', 's3', 'directory'}
    if output_type not in valid_types:
        raise YAMLParseError(
            f"Generator '{gen_name}': output {index} has invalid type '{output_type}'. "
            f"Valid types: {valid_types}"
        )

    if output_type == 's3':
        if 'bucket' not in spec:
            raise YAMLParseError(
                f"Generator '{gen_name}': S3 output {index} missing 'bucket'"
            )

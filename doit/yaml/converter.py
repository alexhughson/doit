"""Convert YAML definitions to TaskGenerator objects.

This module handles converting parsed YAML structures into
TaskGenerator objects that can be used with ReactiveEngine.
"""

from pathlib import Path
from typing import Dict, List, Any, Union, TYPE_CHECKING

from .parser import YAMLConfig

if TYPE_CHECKING:
    from doit.taskgen import TaskGenerator


def yaml_to_generators(
    config: YAMLConfig,
    base_path: Union[str, Path, None] = None,
) -> List['TaskGenerator']:
    """Convert all generators from a YAMLConfig.

    Args:
        config: Parsed YAML configuration
        base_path: Override base path (defaults to config.base_path or cwd)

    Returns:
        List of TaskGenerator objects
    """
    if base_path is None:
        base_path = config.config.get('base_path', '.')
    base_path = Path(base_path).resolve()

    return [
        yaml_to_generator(gen_dict, base_path)
        for gen_dict in config.generators
    ]


def yaml_to_generator(
    gen_dict: Dict[str, Any],
    base_path: Path,
) -> 'TaskGenerator':
    """Convert a YAML generator definition to a TaskGenerator.

    Args:
        gen_dict: Generator definition dictionary
        base_path: Base path for file patterns

    Returns:
        TaskGenerator instance
    """
    from doit.taskgen import TaskGenerator

    # Parse inputs
    inputs = {}
    for label, spec in gen_dict['inputs'].items():
        inputs[label] = _parse_input_spec(spec, base_path)

    # Parse outputs
    outputs = []
    for out_spec in gen_dict['outputs']:
        outputs.append(_parse_output_spec(out_spec, base_path))

    # Create action factory using the template
    action_template = gen_dict['action']

    def make_action(inp, out_paths, attrs):
        from .action import ShellAction
        return ShellAction(action_template, inp, out_paths, attrs)

    return TaskGenerator(
        name=gen_dict['name'],
        inputs=inputs,
        outputs=outputs,
        action=make_action,
        doc=gen_dict.get('doc'),
    )


def _parse_input_spec(spec: Any, base_path: Path):
    """Parse an input specification into an Input object.

    Args:
        spec: Input specification (string or dict)
        base_path: Base path for file patterns

    Returns:
        Input subclass instance
    """
    from doit.taskgen import FileInput
    from doit.taskgen.inputs import DirectoryInput

    # Short form: just a pattern string
    if isinstance(spec, str):
        return FileInput(spec, base_path=base_path)

    # Long form: dict with pattern and options
    pattern = spec['pattern']
    input_type = spec.get('type', 'file')
    is_list = spec.get('is_list', False)
    required = spec.get('required', True)

    if input_type == 'file':
        return FileInput(
            pattern,
            base_path=base_path,
            is_list=is_list,
            required=required,
        )

    elif input_type == 'directory':
        return DirectoryInput(
            pattern,
            base_path=base_path,
            required=required,
        )

    elif input_type == 's3':
        from doit.taskgen import S3Input
        return S3Input(
            pattern,
            bucket=spec['bucket'],
            profile=spec.get('profile'),
            region=spec.get('region'),
            is_list=is_list,
            required=required,
        )

    else:
        raise ValueError(f"Unknown input type: {input_type}")


def _parse_output_spec(spec: Any, base_path: Path):
    """Parse an output specification into an Output object.

    Args:
        spec: Output specification (string or dict)
        base_path: Base path for file patterns

    Returns:
        Output subclass instance
    """
    from doit.taskgen import FileOutput
    from doit.taskgen.outputs import DirectoryOutput

    # Short form: just a pattern string
    if isinstance(spec, str):
        return FileOutput(spec, base_path=base_path)

    # Long form: dict with path and options
    pattern = spec['path']
    output_type = spec.get('type', 'file')

    if output_type == 'file':
        return FileOutput(pattern, base_path=base_path)

    elif output_type == 'directory':
        return DirectoryOutput(pattern, base_path=base_path)

    elif output_type == 's3':
        from doit.taskgen import S3Output
        return S3Output(
            pattern,
            bucket=spec['bucket'],
            profile=spec.get('profile'),
            region=spec.get('region'),
        )

    else:
        raise ValueError(f"Unknown output type: {output_type}")

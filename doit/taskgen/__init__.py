"""Task generation from patterns - tup-like functionality for doit.

This module provides a higher-level API for generating doit tasks from
input patterns with named captures and output templates.

Example:
    from doit.taskgen import TaskGenerator, FileInput, FileOutput

    gen = TaskGenerator(
        name="compile:<module>",
        inputs={"source": FileInput("src/<module>.c")},
        outputs=[FileOutput("build/<module>.o")],
        action=lambda inp, out, attrs: f"gcc -c {inp['source'].path} -o {out[0]}",
    )

    # Generate and run tasks
    from doit.engine import DoitEngine
    with DoitEngine(list(gen.generate())) as engine:
        for task in engine:
            if task.should_run:
                task.execute_and_submit()

Classes:
    Input: ABC for input patterns (subclass for custom resource types)
    FileInput: Input pattern for local files
    S3Input: Input pattern for S3 objects (requires boto3)
    CaptureMatch: A matched resource with captured attributes

    Output: ABC for output patterns (subclass for custom resource types)
    FileOutput: Output pattern for local files
    S3Output: Output pattern for S3 objects

    InputSet: A grouped set of inputs sharing common attribute values
    TaskGenerator: Primary interface for pattern-based task generation

Functions:
    build_input_sets: Generate InputSets for all attribute permutations
"""

from .inputs import Input, FileInput, S3Input, CaptureMatch
from .outputs import Output, FileOutput, S3Output
from .groups import InputSet, build_input_sets
from .generator import TaskGenerator

__all__ = [
    # Input classes
    'Input', 'FileInput', 'S3Input', 'CaptureMatch',
    # Output classes
    'Output', 'FileOutput', 'S3Output',
    # Grouping
    'InputSet', 'build_input_sets',
    # Generator
    'TaskGenerator',
]

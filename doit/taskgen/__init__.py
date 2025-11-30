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
    DirectoryInput: Input pattern for directory prefixes
    S3PrefixInput: Input pattern for S3 prefix (directory-like)
    CaptureMatch: A matched resource with captured attributes

    Output: ABC for output patterns (subclass for custom resource types)
    FileOutput: Output pattern for local files
    S3Output: Output pattern for S3 objects
    DirectoryOutput: Output pattern for directory prefixes
    S3PrefixOutput: Output pattern for S3 prefix (directory-like)

    InputSet: A grouped set of inputs sharing common attribute values
    TaskGenerator: Primary interface for pattern-based task generation

Functions:
    build_input_sets: Generate InputSets for all attribute permutations
"""

from .inputs import Input, FileInput, S3Input, DirectoryInput, S3PrefixInput, CaptureMatch
from .outputs import Output, FileOutput, S3Output, DirectoryOutput, S3PrefixOutput
from .groups import InputSet, build_input_sets
from .generator import TaskGenerator

__all__ = [
    # Input classes
    'Input', 'FileInput', 'S3Input', 'DirectoryInput', 'S3PrefixInput', 'CaptureMatch',
    # Output classes
    'Output', 'FileOutput', 'S3Output', 'DirectoryOutput', 'S3PrefixOutput',
    # Grouping
    'InputSet', 'build_input_sets',
    # Generator
    'TaskGenerator',
]

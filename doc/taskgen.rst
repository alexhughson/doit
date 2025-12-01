.. meta::
   :description: Pattern-based task generation for doit - tup-like functionality
   :keywords: doit, task generation, patterns, tup, automation

====================================
Pattern-Based Task Generation
====================================

The ``doit.taskgen`` module provides a higher-level API for generating tasks from
input patterns with named captures and output templates. It's inspired by
`tup <http://gittup.org/tup/>`_ and enables declarative task specification.

.. contents::
   :local:

Overview
--------

Instead of manually writing task functions for each file, you define patterns
that automatically match files and generate tasks:

.. code-block:: python

    from doit.taskgen import TaskGenerator, FileInput, FileOutput

    gen = TaskGenerator(
        name="compile:<module>",
        inputs={"source": FileInput("src/<module>.c")},
        outputs=[FileOutput("build/<module>.o")],
        action=lambda inp, out, attrs: f"gcc -c {inp['source'].path} -o {out[0]}",
    )

    # Given src/main.c and src/utils.c, generates:
    # - Task("compile:main", ...)
    # - Task("compile:utils", ...)

Key Concepts
------------

**Named Captures**
    Patterns use ``<name>`` syntax to capture path segments. These are extracted
    from matched files and used to generate task names and output paths.

**Input/Output Classes**
    Abstract base classes allow extensibility. Built-in implementations include:

    - ``FileInput`` / ``FileOutput`` - Local files
    - ``S3Input`` / ``S3Output`` - AWS S3 objects (requires boto3)

**InputSet**
    A grouped set of matched inputs sharing common attribute values. When multiple
    inputs have the same capture name, they're matched together.

**TaskGenerator**
    The primary interface that combines inputs, outputs, and actions to generate
    Task objects.

Basic Usage
-----------

Single Capture
~~~~~~~~~~~~~~

Match files with a single pattern variable:

.. code-block:: python

    from doit.taskgen import TaskGenerator, FileInput, FileOutput

    gen = TaskGenerator(
        name="compile:<module>",
        inputs={
            "source": FileInput("src/<module>.c"),
        },
        outputs=[FileOutput("build/<module>.o")],
        action=lambda inp, out, attrs: f"gcc -c {inp['source'].path} -o {out[0]}",
    )

    # Use with DoitEngine
    from doit.engine import DoitEngine

    with DoitEngine(list(gen.generate())) as engine:
        for task in engine:
            if task.should_run:
                task.execute_and_submit()

Multiple Captures
~~~~~~~~~~~~~~~~~

Match files with multiple pattern variables (creates Cartesian product):

.. code-block:: python

    gen = TaskGenerator(
        name="compile:<arch>:<module>",
        inputs={
            "source": FileInput("src/<arch>/<module>.c"),
        },
        outputs=[FileOutput("build/<arch>/<module>.o")],
        action=lambda inp, out, attrs: (
            f"gcc -m{32 if attrs['arch'] == 'x86' else 64} "
            f"-c {inp['source'].path} -o {out[0]}"
        ),
    )

    # Given:
    # - src/x86/main.c, src/x86/utils.c
    # - src/arm/main.c
    #
    # Generates tasks:
    # - compile:x86:main
    # - compile:x86:utils
    # - compile:arm:main

List Inputs (Wildcards)
~~~~~~~~~~~~~~~~~~~~~~~

When a pattern contains ``*`` in the filename, all matching files are collected
into a list:

.. code-block:: python

    gen = TaskGenerator(
        name="ocr:<doc>",
        inputs={
            # is_list auto-detected due to * in filename
            "pages": FileInput("textract/<doc>.page*.txt"),
            "pdf": FileInput("pdfs/<doc>.pdf"),
        },
        outputs=[FileOutput("ocr/<doc>.md")],
        action=lambda inp, out, attrs: (
            refine_ocr,
            [inp['pages'], inp['pdf'], out[0]],
        ),
    )

    # inp['pages'] will be a list of all matching page files
    # inp['pdf'] will be a single FileDependency

Multiple Inputs
~~~~~~~~~~~~~~~

Combine multiple inputs with different captures:

.. code-block:: python

    gen = TaskGenerator(
        name="extract:<doc>:<section>",
        inputs={
            "ocr_file": FileInput("ocr/<doc>.md"),
            "system_prompt": FileInput("prompts/<section>/system.txt"),
            "extract_prompt": FileInput("prompts/<section>/extract.txt"),
        },
        outputs=[FileOutput("sections/<doc>/<section>.txt")],
        action=lambda inp, out, attrs: (
            extract_section,
            [inp['ocr_file'].path, attrs['section'], out[0]],
        ),
    )

    # Creates doc × section combinations

S3 Support
----------

Use ``S3Input`` and ``S3Output`` for AWS S3 objects:

.. code-block:: python

    from doit.taskgen import TaskGenerator, S3Input, S3Output

    gen = TaskGenerator(
        name="process:<dataset>:<partition>",
        inputs={
            "data": S3Input(
                "raw/<dataset>/<partition>.parquet",
                bucket="my-bucket",
                profile="dev",  # optional AWS profile
            ),
        },
        outputs=[
            S3Output(
                "processed/<dataset>/<partition>.parquet",
                bucket="my-bucket",
                profile="dev",
            ),
        ],
        action=lambda inp, out, attrs: process_data(inp, out, attrs),
    )

S3Input queries S3 to list objects matching the pattern, extracts captures,
and creates ``S3Dependency`` objects. S3Output creates ``S3Target`` objects.

Action Callbacks
----------------

The ``action`` parameter receives three arguments:

1. ``input_set`` (InputSet): Grouped inputs with matched dependencies
2. ``output_paths`` (List[str]): Rendered output paths
3. ``attrs`` (Dict[str, str]): Captured attribute values

Return a single action or a list of actions:

.. code-block:: python

    # Single command string
    action=lambda inp, out, attrs: f"cmd {inp['source'].path}"

    # List of commands
    action=lambda inp, out, attrs: [
        f"mkdir -p {Path(out[0]).parent}",
        f"compile {inp['source'].path} -o {out[0]}",
    ]

    # Python callable with args
    action=lambda inp, out, attrs: [(my_function, [inp['source'].path, out[0]])]

Optional and Required Inputs
----------------------------

By default, inputs are required. A task is only generated if all required
inputs have matches:

.. code-block:: python

    gen = TaskGenerator(
        name="build:<module>",
        inputs={
            "source": FileInput("src/<module>.c"),
            # Optional: task generated even if missing
            "config": FileInput("config/<module>.json", required=False),
        },
        outputs=[FileOutput("build/<module>.o")],
        action=...,
    )

If ``config`` files don't exist for some modules, tasks are still generated
with ``inp['config']`` set to ``None``.

Custom Input Types
------------------

Create custom input types by subclassing ``Input``:

.. code-block:: python

    from doit.taskgen import Input
    from dataclasses import dataclass

    @dataclass
    class DatabaseTableInput(Input):
        """Input from database table query."""
        connection_string: str = ""

        def list_resources(self):
            """Query database for matching records."""
            import sqlalchemy
            engine = sqlalchemy.create_engine(self.connection_string)
            # ... query logic using self._glob_pattern
            for row in results:
                yield row['id']

        def create_dependency(self, resource_key: str):
            """Create a Dependency object for this resource."""
            return DatabaseRowDependency(self.connection_string, resource_key)

Similarly, subclass ``Output`` for custom output types.

API Reference
-------------

Classes
~~~~~~~

**Input** (ABC)
    Base class for input patterns. Subclasses must implement:

    - ``list_resources()``: Yield resource identifiers matching the pattern
    - ``create_dependency(resource_key)``: Create a Dependency object

**FileInput**
    Input pattern for local files.

    Parameters:

    - ``pattern``: Pattern with ``<name>`` captures
    - ``base_path``: Base directory for glob matching (default: cwd)
    - ``required``: Whether input is required (default: True)
    - ``is_list``: Collect all matches into list (auto-detected for ``*``)

**S3Input**
    Input pattern for S3 objects.

    Parameters:

    - ``pattern``: S3 key pattern with ``<name>`` captures
    - ``bucket``: S3 bucket name
    - ``profile``: AWS profile name (optional)
    - ``region``: AWS region (optional)

**Output** (ABC)
    Base class for output patterns. Subclasses must implement:

    - ``create_target(rendered_path)``: Create a Target object

**FileOutput**
    Output pattern for local files.

**S3Output**
    Output pattern for S3 objects.

**InputSet**
    A grouped set of inputs sharing common attribute values.

    Attributes:

    - ``attrs``: Dict of capture name -> value
    - ``items``: Dict of label -> Dependency or List[Dependency]

    Methods:

    - ``__getitem__(label)``: Get input by label
    - ``get_all_dependencies()``: Flatten all dependencies into a list

**TaskGenerator**
    Generate tasks from patterns.

    Parameters:

    - ``name``: Task name template with ``<capture>`` placeholders
    - ``inputs``: Dict mapping labels to Input instances
    - ``outputs``: List of Output instances
    - ``action``: Callable receiving (input_set, output_paths, attrs)
    - ``doc``: Optional doc string template

Functions
~~~~~~~~~

**build_input_sets(inputs)**
    Generate InputSets for all attribute permutations.

    Args:
        inputs: Dict mapping labels to Input instances

    Yields:
        InputSet for each valid attribute combination

Example: Complete Workflow
--------------------------

Here's a complete example showing OCR refinement and section extraction:

.. code-block:: python

    from doit.taskgen import TaskGenerator, FileInput, FileOutput
    from doit.engine import DoitEngine

    # Step 1: OCR refinement
    ocr_gen = TaskGenerator(
        name="ocr:<doc>",
        inputs={
            "pages": FileInput("textract/<doc>.page*.txt"),
            "pdf": FileInput("pdfs/<doc>.pdf"),
        },
        outputs=[FileOutput("ocr/<doc>.md")],
        action=lambda inp, out, attrs: (
            refine_ocr_with_llm,
            [inp['pages'], inp['pdf'], out[0]],
        ),
        doc="Refine OCR for <doc>",
    )

    # Step 2: Section extraction
    extract_gen = TaskGenerator(
        name="extract:<doc>:<section>",
        inputs={
            "ocr": FileInput("ocr/<doc>.md"),
            "prompt": FileInput("prompts/<section>.txt"),
        },
        outputs=[FileOutput("sections/<doc>/<section>.txt")],
        action=lambda inp, out, attrs: (
            extract_section,
            [inp['ocr'].path, inp['prompt'].path, out[0]],
        ),
        doc="Extract <section> from <doc>",
    )

    # Generate and run all tasks
    all_tasks = list(ocr_gen.generate()) + list(extract_gen.generate())

    with DoitEngine(all_tasks) as engine:
        for task in engine:
            if task.should_run:
                print(f"Running: {task.name}")
                task.execute_and_submit()

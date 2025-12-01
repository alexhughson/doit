.. meta::
   :description: YAML-based task definition for doit - declarative build systems without Python
   :keywords: doit, yaml, declarative, make, build, automation

====================================
YAML Task Definition
====================================

The ``doit.yaml`` module provides a declarative YAML file format for defining
task generators with shell commands. This turns doit into a make-like tool
where you can define entire build systems without writing Python code.

.. contents::
   :local:

Overview
--------

Instead of writing a ``dodo.py`` file with Python task definitions, you can
create a ``doit.yaml`` file:

.. code-block:: yaml

    generators:
      - name: "compile:<module>"
        inputs:
          source: "src/<module>.c"
        outputs:
          - "build/<module>.o"
        action: "gcc -c {source} -o {out_0}"

      - name: "link"
        inputs:
          objects:
            pattern: "build/*.o"
            is_list: true
        outputs:
          - "bin/program"
        action: "gcc {objects} -o {out_0}"

Run it with:

.. code-block:: bash

    python -m doit.yaml

    # Or with the CLI entry point (after installation)
    doit-yaml

Key Features
------------

**Pattern-based Generators**
    Use ``<capture>`` placeholders to match files and generate tasks dynamically.
    Each unique combination of captured values creates a separate task.

**Variable Injection**
    Input paths and captured values are available in actions via both format
    strings (``{source}``) and environment variables (``$source``).

**Reactive Execution**
    Integrates with the ReactiveEngine for fixed-point iteration. Outputs from
    one generator can trigger tasks from another generator.

**Multiple Input Types**
    Supports file, directory (prefix), and S3 inputs with a consistent syntax.

Installation
------------

The YAML module requires PyYAML:

.. code-block:: bash

    pip install pyyaml

    # Or install doit with yaml support
    pip install doit[yaml]

Quick Start
-----------

1. Create a ``doit.yaml`` file in your project:

.. code-block:: yaml

    # doit.yaml
    generators:
      - name: "hello:<name>"
        inputs:
          greeting: "greetings/<name>.txt"
        outputs:
          - "output/<name>.out"
        action: "cat {greeting} > {out_0}"

2. Create some input files:

.. code-block:: bash

    mkdir greetings output
    echo "Hello, World!" > greetings/world.txt
    echo "Hello, User!" > greetings/user.txt

3. Run the tasks:

.. code-block:: bash

    python -m doit.yaml

    # Output:
    # Completed 2 tasks

File Format Reference
---------------------

Config Section
~~~~~~~~~~~~~~

Optional global configuration:

.. code-block:: yaml

    config:
      base_path: .           # Base path for relative patterns
      max_tasks: 10000       # Safety limit on total tasks

Generators Section
~~~~~~~~~~~~~~~~~~

List of task generators. Each generator has:

``name`` (required)
    Task name template with ``<capture>`` placeholders.

``inputs`` (required)
    Dictionary of labeled input patterns.

``outputs`` (required)
    List of output patterns.

``action`` (required)
    Shell command template with variable substitution.

``doc`` (optional)
    Documentation string (supports placeholders).

Input Specifications
~~~~~~~~~~~~~~~~~~~~

**Short form** - Just a pattern string:

.. code-block:: yaml

    inputs:
      source: "src/<module>.c"

**Long form** - Dictionary with options:

.. code-block:: yaml

    inputs:
      headers:
        pattern: "include/<arch>/*.h"
        is_list: true      # Collect multiple files
        required: false    # Optional input

**Input types**:

.. code-block:: yaml

    inputs:
      # File input (default)
      source: "src/<module>.c"

      # Explicit file input
      headers:
        type: file
        pattern: "include/*.h"
        is_list: true

      # Directory/prefix input
      generated:
        type: directory
        pattern: "output/<stage>/"

      # S3 input
      data:
        type: s3
        pattern: "raw/<dataset>.parquet"
        bucket: "my-bucket"
        profile: "dev"
        region: "us-east-1"

Output Specifications
~~~~~~~~~~~~~~~~~~~~~

**Short form** - Just a pattern string:

.. code-block:: yaml

    outputs:
      - "build/<module>.o"

**Long form** - Dictionary with type:

.. code-block:: yaml

    outputs:
      - path: "build/<module>.o"
        type: file

      - path: "output/<stage>/"
        type: directory

      - path: "processed/<dataset>.parquet"
        type: s3
        bucket: "my-bucket"

Variable Injection
------------------

Actions can access inputs, outputs, and captured values through two mechanisms:

Format Strings
~~~~~~~~~~~~~~

Use ``{name}`` syntax in the action:

.. code-block:: yaml

    action: "gcc -c {source} -I include/{arch} -o {out_0}"

Available variables:

- **Captured values**: ``{module}``, ``{arch}``, etc.
- **Input labels**: ``{source}``, ``{headers}``, etc.
- **Output indices**: ``{out_0}``, ``{out_1}``, etc.

For list inputs, paths are space-separated.

Environment Variables
~~~~~~~~~~~~~~~~~~~~~

The same variables are also set as environment variables:

.. code-block:: yaml

    action: "echo $source > $out_0"

This is useful for more complex shell scripts or when paths contain special
characters.

Examples
--------

C Project Build
~~~~~~~~~~~~~~~

.. code-block:: yaml

    # doit.yaml - C project build system
    config:
      base_path: .
      max_tasks: 1000

    generators:
      # Compile C files
      - name: "compile:<module>"
        inputs:
          source: "src/<module>.c"
          header:
            pattern: "src/<module>.h"
            required: false
        outputs:
          - "build/<module>.o"
        action: "gcc -c {source} -I src -o {out_0}"
        doc: "Compile {module}"

      # Link executable
      - name: "link"
        inputs:
          objects:
            pattern: "build/*.o"
            is_list: true
        outputs:
          - "bin/program"
        action: "gcc {objects} -o {out_0}"
        doc: "Link final executable"

Multi-Architecture Build
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    generators:
      - name: "compile:<arch>:<module>"
        inputs:
          source: "src/<arch>/<module>.c"
          headers:
            pattern: "include/<arch>/*.h"
            is_list: true
        outputs:
          - "build/<arch>/<module>.o"
        action: "gcc -c {source} -I include/{arch} -o {out_0}"

      - name: "link:<arch>"
        inputs:
          objects:
            pattern: "build/<arch>/*.o"
            is_list: true
        outputs:
          - "bin/<arch>/program"
        action: "gcc {objects} -o {out_0}"

Data Pipeline
~~~~~~~~~~~~~

.. code-block:: yaml

    generators:
      # Extract data
      - name: "extract:<source>"
        inputs:
          archive: "archives/<source>.zip"
        outputs:
          - path: "extracted/<source>/"
            type: directory
        action: "unzip -o {archive} -d extracted/{source}/"

      # Transform extracted files
      - name: "transform:<source>:<file>"
        inputs:
          data: "extracted/<source>/<file>.csv"
        outputs:
          - "processed/<source>/<file>.json"
        action: "python scripts/csv_to_json.py {data} {out_0}"

      # Aggregate per source
      - name: "aggregate:<source>"
        inputs:
          files:
            pattern: "processed/<source>/*.json"
            is_list: true
        outputs:
          - "reports/<source>.html"
        action: "python scripts/generate_report.py {files} {out_0}"

CLI Usage
---------

Basic usage:

.. code-block:: bash

    # Run with default doit.yaml
    python -m doit.yaml

    # Specify a different file
    python -m doit.yaml build.yaml

    # Verbose output
    python -m doit.yaml -v

    # Dry run (show what would be done)
    python -m doit.yaml --dry-run

    # Limit total tasks
    python -m doit.yaml --max-tasks 500

Full options:

.. code-block:: text

    usage: python -m doit.yaml [-h] [--max-tasks MAX_TASKS]
                               [--base-path BASE_PATH] [-v] [--dry-run]
                               [yaml_file]

    positional arguments:
      yaml_file             Path to the YAML file (default: doit.yaml)

    optional arguments:
      -h, --help            show this help message and exit
      --max-tasks MAX_TASKS
                            Maximum number of tasks to execute (default: 10000)
      --base-path BASE_PATH
                            Override base path for file patterns
      -v, --verbose         Print progress information
      --dry-run             Parse and show generators without executing

Python API
----------

You can also use the YAML runner from Python:

.. code-block:: python

    from doit.yaml import run_yaml

    result = run_yaml('doit.yaml')

    if result.converged:
        print(f"Completed {result.tasks_executed} tasks")
    else:
        print(f"Hit limit at {result.tasks_executed}")

With options:

.. code-block:: python

    from pathlib import Path
    from doit.yaml import run_yaml

    result = run_yaml(
        'build.yaml',
        max_tasks=5000,
        base_path=Path('/data/project'),
        verbose=True,
    )

Parsing without execution:

.. code-block:: python

    from doit.yaml import parse_yaml_file, yaml_to_generators

    config = parse_yaml_file('doit.yaml')
    generators = yaml_to_generators(config)

    for gen in generators:
        print(f"Generator: {gen.name}")
        for task in gen.generate():
            print(f"  Task: {task.name}")

Integration with Reactive Engine
--------------------------------

The YAML runner uses the ReactiveEngine internally, which means:

- Outputs from early tasks can trigger new tasks
- Fixed-point iteration continues until no new tasks are generated
- The ``max_tasks`` limit prevents infinite loops

For multi-stage pipelines, later stages automatically wait for earlier stages:

.. code-block:: yaml

    generators:
      # Stage 1: These run first
      - name: "stage1:<doc>"
        inputs:
          raw: "input/<doc>.txt"
        outputs:
          - "stage1/<doc>.json"
        action: "python process.py {raw} {out_0}"

      # Stage 2: Triggered when Stage 1 creates outputs
      - name: "stage2:<doc>"
        inputs:
          data: "stage1/<doc>.json"
        outputs:
          - "stage2/<doc>.csv"
        action: "python transform.py {data} {out_0}"

Best Practices
--------------

1. **Use meaningful capture names**: ``<module>`` is clearer than ``<x>``.

2. **Set base_path in config**: Ensures consistent path resolution.

   .. code-block:: yaml

       config:
         base_path: /data/project

3. **Use list inputs for aggregation**: When collecting multiple files.

   .. code-block:: yaml

       inputs:
         files:
           pattern: "data/*.csv"
           is_list: true

4. **Start with --dry-run**: Verify task generation before execution.

5. **Use conservative max_tasks**: Start low and increase as needed.

Comparison with dodo.py
-----------------------

YAML tasks are best for:
    - Simple shell command pipelines
    - File transformation workflows
    - Build systems with pattern-based rules
    - Projects where non-Python developers need to modify tasks

Python dodo.py is better for:
    - Complex logic in task generation
    - Python function actions
    - Custom uptodate checks
    - Integration with Python libraries

You can use both in the same project - YAML for simple tasks, Python for
complex ones.

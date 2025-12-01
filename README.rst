================
README
================

.. display some badges

.. image:: https://img.shields.io/pypi/v/doit.svg
    :target: https://pypi.python.org/pypi/doit

.. image:: https://github.com/pydoit/doit/actions/workflows/ci.yml/badge.svg?branch=master
    :target: https://github.com/pydoit/doit/actions/workflows/ci.yml?query=branch%3Amaster

.. image:: https://codecov.io/gh/pydoit/doit/branch/master/graph/badge.svg?token=wxKa1h11zn
    :target: https://codecov.io/gh/pydoit/doit

.. image:: https://zenodo.org/badge/DOI/10.5281/zenodo.4892136.svg
   :target: https://doi.org/10.5281/zenodo.4892136


Financial contributions on `Open Collective <https://opencollective.com/doit/tiers>`_


doit - automation tool
======================

*doit* comes from the idea of bringing the power of build-tools to execute any
kind of task

*doit* can be uses as a simple **Task Runner** allowing you to easily define ad hoc
tasks, helping you to organize all your project related tasks in an unified
easy-to-use & discoverable way.

*doit* scales-up with an efficient execution model like a **build-tool**.
*doit* creates a DAG (direct acyclic graph) and is able to cache task results.
It ensures that only required tasks will be executed and in the correct order
(aka incremental-builds).

The *up-to-date* check to cache task results is not restricted to looking for
file modification on dependencies.  Nor it requires "target" files.
So it is also suitable to handle **workflows** not handled by traditional build-tools.

Tasks' dependencies and creation can be done dynamically during it is execution
making it suitable to drive complex workflows and **pipelines**.

*doit* is build with a plugin architecture allowing extensible commands, custom
output, storage backend and "task loader". It also provides an API allowing
users to create new applications/tools leveraging *doit* functionality like a framework.

*doit* is a mature project being actively developed for more than 10 years.
It includes several extras like: parallel execution, auto execution (watch for file
changes), shell tab-completion, DAG visualisation, IPython integration, and more.


Key Features
============

**Multiple Interfaces**
  - **dodo.py** - Traditional Python task definitions
  - **doit.yaml** - Declarative YAML for shell command pipelines
  - **Programmatic API** - Embed doit in your Python applications

**Pattern-Based Task Generation**
  - Use ``<capture>`` patterns to match files (like ``src/<module>.c``)
  - Automatic task generation for all matching files
  - Multi-dimensional patterns for cross-product task generation

**Extensible Dependency System**
  - Built-in: Files, tasks, S3 objects
  - Custom: Extend with database rows, HTTP resources, etc.
  - Automatic implicit dependencies between tasks

**Reactive Execution**
  - Outputs from one task automatically trigger dependent tasks
  - Fixed-point iteration for dynamic pipelines
  - Streaming execution model (tup-like)


Sample Code
===========

Define functions returning python dict with task's meta-data.

Snippet from `tutorial <http://pydoit.org/tutorial-1.html>`_:

.. code:: python

  def task_imports():
      """find imports from a python module"""
      for name, module in PKG_MODULES.by_name.items():
          yield {
              'name': name,
              'file_dep': [module.path],
              'actions': [(get_imports, (PKG_MODULES, module.path))],
          }

  def task_dot():
      """generate a graphviz's dot graph from module imports"""
      return {
          'targets': ['requests.dot'],
          'actions': [module_to_dot],
          'getargs': {'imports': ('imports', 'modules')},
          'clean': True,
      }

  def task_draw():
      """generate image from a dot file"""
      return {
          'file_dep': ['requests.dot'],
          'targets': ['requests.png'],
          'actions': ['dot -Tpng %(dependencies)s -o %(targets)s'],
          'clean': True,
      }


Run from terminal::

  $ doit list
  dot       generate a graphviz's dot graph from module imports
  draw      generate image from a dot file
  imports   find imports from a python module
  $ doit
  .  imports:requests.models
  .  imports:requests.__init__
  .  imports:requests.help
  (...)
  .  dot
  .  draw


YAML Task Definition
====================

The easiest way to use doit is with a ``doit.yaml`` file. Define your build
system declaratively without writing any Python code.

Quick Start
-----------

1. Install doit with YAML support::

    pip install doit[yaml]

2. Create a ``doit.yaml`` file in your project::

    # doit.yaml
    generators:
      - name: "hello"
        inputs:
          source: "input.txt"
        outputs:
          - "output.txt"
        action: "cat {source} | tr a-z A-Z > {out_0}"

3. Create an input file::

    echo "hello world" > input.txt

4. Run::

    doit-yaml

That's it! The task runs, creating ``output.txt`` with uppercase content.
Run again and it skips (already up-to-date). Modify ``input.txt`` and it re-runs.

Pattern Matching
----------------

The real power comes from **pattern-based generators**. Use ``<name>`` placeholders
to match multiple files and generate tasks automatically:

.. code:: yaml

  generators:
    - name: "compile:<module>"
      inputs:
        source: "src/<module>.c"
      outputs:
        - "build/<module>.o"
      action: "gcc -c {source} -o {out_0}"

Given files ``src/main.c`` and ``src/utils.c``, this generates two tasks:
``compile:main`` and ``compile:utils``.

**Multi-dimensional patterns** create cross-products:

.. code:: yaml

  generators:
    - name: "compile:<arch>:<module>"
      inputs:
        source: "src/<arch>/<module>.c"
      outputs:
        - "build/<arch>/<module>.o"
      action: "gcc -c {source} -o {out_0}"

Given ``src/x86/main.c``, ``src/x86/utils.c``, and ``src/arm/main.c``,
this generates three tasks: ``compile:x86:main``, ``compile:x86:utils``,
``compile:arm:main``.

Variable Reference
------------------

In your ``action``, you can use these variables:

=================  ================================================
Variable           Description
=================  ================================================
``{input_label}``  Path to input (e.g., ``{source}``, ``{config}``)
``{out_0}``        First output path
``{out_1}``        Second output path (and so on)
``{capture}``      Captured value (e.g., ``{module}``, ``{arch}``)
=================  ================================================

Variables work as both format strings and environment variables::

  # These are equivalent:
  action: "echo {source}"
  action: "echo $source"

For **list inputs** (multiple files), paths are space-separated::

  action: "cat {files} > {out_0}"  # files="a.txt b.txt c.txt"

Input Types
-----------

**Simple string** (file input)::

  inputs:
    source: "src/<module>.c"

**List input** (collect multiple files)::

  inputs:
    files:
      pattern: "data/*.csv"
      is_list: true

**Optional input**::

  inputs:
    config:
      pattern: "config/<module>.json"
      required: false

**Directory input** (for outputs you don't know ahead of time)::

  inputs:
    generated:
      type: directory
      pattern: "generated/<stage>/"

**S3 input** (requires ``pip install doit[s3]``)::

  inputs:
    data:
      type: s3
      pattern: "raw/<dataset>.parquet"
      bucket: "my-bucket"
      profile: "dev"

Complete Example: C Build System
--------------------------------

.. code:: yaml

  # doit.yaml
  config:
    base_path: .

  generators:
    # Compile each .c file to .o
    - name: "compile:<module>"
      inputs:
        source: "src/<module>.c"
      outputs:
        - "build/<module>.o"
      action: "mkdir -p build && gcc -c {source} -o {out_0}"

    # Link all .o files into executable
    - name: "link"
      inputs:
        objects:
          pattern: "build/*.o"
          is_list: true
      outputs:
        - "bin/program"
      action: "mkdir -p bin && gcc {objects} -o {out_0}"

Run::

  $ doit-yaml
  Running compile:main...
  Running compile:utils...
  Running link...
  Completed 3 tasks

Multi-Stage Pipeline
--------------------

Outputs from one generator automatically trigger the next. This pipeline
extracts, transforms, and loads data:

.. code:: yaml

  generators:
    # Stage 1: Extract archives
    - name: "extract:<archive>"
      inputs:
        zip: "downloads/<archive>.zip"
      outputs:
        - path: "extracted/<archive>/"
          type: directory
      action: "unzip -o {zip} -d extracted/{archive}/"

    # Stage 2: Transform CSV files (triggered by Stage 1)
    - name: "transform:<archive>:<file>"
      inputs:
        csv: "extracted/<archive>/<file>.csv"
      outputs:
        - "transformed/<archive>/<file>.json"
      action: "python csv2json.py {csv} {out_0}"

    # Stage 3: Aggregate (triggered by Stage 2)
    - name: "aggregate:<archive>"
      inputs:
        files:
          pattern: "transformed/<archive>/*.json"
          is_list: true
      outputs:
        - "reports/<archive>.html"
      action: "python make_report.py {files} {out_0}"

When you add a new ``.zip`` file to ``downloads/``, the entire pipeline runs
automatically.

CLI Options
-----------

::

  doit-yaml [options] [yaml_file]

  Options:
    yaml_file           Path to YAML file (default: doit.yaml)
    --dry-run           Show what would run without executing
    --max-tasks N       Limit total tasks (default: 10000)
    --base-path PATH    Override base path for patterns
    -v, --verbose       Print detailed progress

Examples::

  doit-yaml                      # Run doit.yaml in current directory
  doit-yaml build.yaml           # Run specific file
  doit-yaml --dry-run            # Preview tasks
  doit-yaml -v                   # Verbose output

Python API
----------

You can also use YAML definitions from Python::

  from doit.yaml import run_yaml

  result = run_yaml('doit.yaml')
  print(f"Completed {result.tasks_executed} tasks")

Or parse without executing::

  from doit.yaml import parse_yaml_file, yaml_to_generators

  config = parse_yaml_file('doit.yaml')
  generators = yaml_to_generators(config)

  for gen in generators:
      for task in gen.generate():
          print(f"Would run: {task.name}")

See full documentation: `YAML Task Definition <http://pydoit.org/yaml.html>`_


Programmatic API
================

Embed doit in Python applications with full control over execution:

.. code:: python

  from doit.engine import DoitEngine
  from doit.task import Task
  from doit.deps import FileDependency, FileTarget

  tasks = [
      Task(
          name="build",
          actions=["gcc -o program main.c"],
          dependencies=[FileDependency("main.c")],
          outputs=[FileTarget("program")],
      ),
  ]

  with DoitEngine(tasks) as engine:
      for task in engine:
          if task.should_run:
              task.execute_and_submit()

**Pattern-based task generation** with automatic dependency detection:

.. code:: python

  from doit.taskgen import TaskGenerator, FileInput, FileOutput
  from doit.reactive import ReactiveEngine

  gen = TaskGenerator(
      name="compile:<module>",
      inputs={"source": FileInput("src/<module>.c")},
      outputs=[FileOutput("build/<module>.o")],
      action=lambda inp, out, attrs: f"gcc -c {inp['source'].path} -o {out[0]}",
  )

  engine = ReactiveEngine(generators=[gen])
  result = engine.run()

**S3 dependencies** for cloud data pipelines:

.. code:: python

  from doit.deps import S3Dependency, S3Target

  Task(
      name="process",
      dependencies=[S3Dependency("bucket", "input/data.csv")],
      outputs=[S3Target("bucket", "output/results.csv")],
      actions=[process_data],
  )

See full documentation:

- `Programmatic Interface <http://pydoit.org/programmatic.html>`_
- `Pattern-Based Task Generation <http://pydoit.org/taskgen.html>`_
- `Reactive Execution <http://pydoit.org/reactive.html>`_
- `Dependencies <http://pydoit.org/dependencies.html>`_


Project Details
===============

 - Website & docs - http://pydoit.org
 - Project management on github - https://github.com/pydoit/doit
 - Discussion group - https://groups.google.com/forum/#!forum/python-doit
 - News/twitter - https://twitter.com/pydoit
 - Plugins, extensions and projects based on doit - https://github.com/pydoit/doit/wiki/powered-by-doit

license
=======

The MIT License
Copyright (c) 2008-2021 Eduardo Naufel Schettino

see LICENSE file


developers / contributors
==========================

see AUTHORS file


install
=======

*doit* is tested on python 3.8+.

The last version supporting python 2 is version 0.29.

.. code:: bash

 # Basic installation
 $ pip install doit

 # With YAML task definition support
 $ pip install doit[yaml]

 # With S3 dependency support
 $ pip install doit[s3]

 # All extras
 $ pip install doit[yaml,s3]


dependencies
=============

Core:

- importlib-metadata

Optional extras:

- **yaml**: pyyaml (for doit.yaml task definitions)
- **s3**: boto3 (for S3 dependencies and targets)
- **toml**: tomli (for TOML configuration, Python <3.11)
- **cloudpickle**: cloudpickle (for parallel execution)

Platform-specific:

- pyinotify (linux)
- macfsevents (mac)

Tools required for development:

- git * VCS
- py.test * unit-tests
- coverage * code coverage
- sphinx * doc tool
- pyflakes * syntax checker
- doit-py * helper to run dev tasks


development setup
==================

The best way to setup an environment to develop *doit* itself is to
create a virtualenv...

.. code:: bash

  doit$ virtualenv dev
  doit$ source dev/bin/activate

install ``doit`` as "editable", and add development dependencies
from `dev_requirements.txt`:

.. code:: bash

  (dev) doit$ pip install --editable .
  (dev) doit$ pip install --requirement dev_requirements.txt



tests
=======

Use py.test - http://pytest.org

.. code:: bash

  $ py.test



documentation
=============

``doc`` folder contains ReST documentation based on Sphinx.

.. code:: bash

 doc$ make html

They are the base for creating the website. The only difference is
that the website includes analytics tracking.
To create it (after installing *doit*):

.. code:: bash

 $ doit website



spell checking
--------------

All documentation is spell checked using the task `spell`:

.. code:: bash

  $ doit spell

It is a bit annoying that code snippets and names always fails the check,
these words must be added into the file `doc/dictionary.txt`.

The spell checker currently uses `hunspell`, to install it on debian based
systems install the hunspell package: `apt-get install hunspell`.


profiling
---------

.. code:: bash

  python -m cProfile -o output.pstats `which doit` list

  gprof2dot -f pstats output.pstats | dot -Tpng -o output.png


releases
========

Update version number at:

- doit/version.py
- setup.py
- doc/conf.py
- doc/index.html

.. code:: bash

   python setup.py sdist
   python setup.py bdist_wheel
   twine upload dist/doit-X.Y.Z.tar.gz
   twine upload dist/doit-X.Y.Z-py3-none-any.whl

Remember to push GIT tags::

  git push --tags



contributing
==============

On github create pull requests using a named feature branch.

Financial contribution to support maintenance welcome.

.. image:: https://opencollective.com/doit/tiers/backers.svg?avatarHeight=50
    :target: https://opencollective.com/doit/tiers

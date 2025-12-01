.. meta::
   :description: Reactive task generation for doit - incremental computation with fixed-point iteration
   :keywords: doit, reactive, streaming, incremental, tup, automation

====================================
Reactive Task Generation
====================================

The ``doit.reactive`` module provides a streaming reactive engine for incremental
task generation. Tasks execute and create outputs, which trigger regeneration of
task generators, creating new tasks in a continuous flow until no more tasks can
be generated (fixed-point).

.. contents::
   :local:

Overview
--------

Reactive task generation is useful when task outputs create conditions for new
tasks. For example:

- Processing documents that each generate multiple output files
- Extracting archives where contents trigger further processing
- Cascading transformations where stage N's output is stage N+1's input

.. code-block:: python

    from doit.reactive import ReactiveEngine
    from doit.taskgen import TaskGenerator, FileInput, FileOutput

    # Stage 1: Process raw files
    gen1 = TaskGenerator(
        name="process:<doc>",
        inputs={"raw": FileInput("raw/<doc>.txt")},
        outputs=[FileOutput("processed/<doc>.json")],
        action=lambda inp, out, attrs: process_document(inp, out),
    )

    # Stage 2: Validate processed files (triggered by Stage 1 outputs)
    gen2 = TaskGenerator(
        name="validate:<doc>",
        inputs={"data": FileInput("processed/<doc>.json")},
        outputs=[FileOutput("validated/<doc>.ok")],
        action=lambda inp, out, attrs: validate_document(inp, out),
    )

    engine = ReactiveEngine(generators=[gen1, gen2])
    result = engine.run()

    if result.converged:
        print(f"Completed {result.tasks_executed} tasks")

Key Concepts
------------

**Streaming Execution**
    Unlike batch/wave approaches, the reactive engine regenerates immediately
    after each task completes. New tasks are added to the ready queue and
    processed in dependency order (tup-like model).

**Fixed-Point Iteration**
    Execution continues until no generators produce new tasks. This is the
    "fixed-point" where the system has converged.

**Output-to-Generator Matching**
    When a task creates outputs, the engine finds generators whose input
    patterns could match those outputs. Only affected generators are regenerated.

**Task Limit Safety**
    To prevent infinite loops (e.g., generators that always create new work),
    a configurable ``max_tasks`` limit stops execution. The ``hit_limit``
    result flag indicates whether this happened.

Basic Usage
-----------

Simple Workflow
~~~~~~~~~~~~~~~

Process files in a single stage:

.. code-block:: python

    from doit.reactive import ReactiveEngine
    from doit.taskgen import TaskGenerator, FileInput, FileOutput

    gen = TaskGenerator(
        name="compile:<module>",
        inputs={"source": FileInput("src/<module>.c")},
        outputs=[FileOutput("build/<module>.o")],
        action=lambda inp, out, attrs: f"gcc -c {inp['source'].path} -o {out[0]}",
    )

    engine = ReactiveEngine(generators=[gen])
    result = engine.run()

    print(f"Executed {result.tasks_executed} tasks")
    print(f"Converged: {result.converged}")

Cascading Workflow
~~~~~~~~~~~~~~~~~~

Multiple stages where each stage's output triggers the next:

.. code-block:: python

    from pathlib import Path
    from doit.reactive import ReactiveEngine
    from doit.taskgen import TaskGenerator, FileInput, FileOutput

    # Stage 1: Extract archives
    def extract_action(inp, out, attrs):
        def do_extract():
            import zipfile
            with zipfile.ZipFile(inp['archive'].path) as zf:
                zf.extractall(Path(out[0]).parent)
        return do_extract

    gen1 = TaskGenerator(
        name="extract:<archive>",
        inputs={"archive": FileInput("downloads/<archive>.zip")},
        outputs=[FileOutput("extracted/<archive>/")],  # Directory output
        action=extract_action,
    )

    # Stage 2: Process extracted files (triggered by Stage 1)
    def process_action(inp, out, attrs):
        def do_process():
            Path(out[0]).write_text(f"processed {attrs['name']}")
        return do_process

    gen2 = TaskGenerator(
        name="process:<name>",
        inputs={"data": FileInput("extracted/<name>.txt")},
        outputs=[FileOutput("final/<name>.json")],
        action=process_action,
    )

    engine = ReactiveEngine(generators=[gen1, gen2])
    result = engine.run()

Setting Task Limits
~~~~~~~~~~~~~~~~~~~

Prevent infinite loops with a task limit:

.. code-block:: python

    engine = ReactiveEngine(
        generators=[gen1, gen2],
        max_tasks=1000  # Stop after 1000 tasks
    )
    result = engine.run()

    if result.hit_limit:
        print(f"Warning: Hit task limit at {result.tasks_executed} tasks")
        print("Consider increasing max_tasks or reviewing generators")

ReactiveResult
--------------

The ``run()`` method returns a ``ReactiveResult`` with:

``tasks_executed``
    Number of tasks that were actually executed.

``total_tasks``
    Total number of unique tasks generated.

``hit_limit``
    Whether the ``max_tasks`` limit was reached.

``regenerations``
    Number of times generators were regenerated.

``converged``
    Property: ``True`` if fixed-point was reached (not hit_limit).

Architecture
------------

The reactive engine consists of several components:

**ReactiveEngine**
    Main entry point. Coordinates generators, task execution, and regeneration.

**OutputPatternIndex**
    Efficiently maps output paths to affected generators using static prefix
    matching. When a task creates ``/data/processed/doc.json``, it quickly
    finds generators with input patterns like ``processed/<name>.json``.

**TaskMerger**
    Tracks which tasks exist and their state. When a generator is regenerated,
    the merger detects:

    - New tasks (added to execution queue)
    - Updated tasks (re-queued if inputs changed)
    - Unchanged tasks (skipped)

**GeneratorManager**
    Holds all generators and handles regeneration requests. Uses the
    OutputPatternIndex to find affected generators.

Integration with DoitEngine
---------------------------

The ReactiveEngine uses ``DoitEngine`` internally for task execution. It
provides the same dependency tracking and up-to-date checking:

.. code-block:: python

    from doit.dependency import ProcessingStateStore

    # Use a custom state store
    store = ProcessingStateStore(db_file="reactive.db")

    engine = ReactiveEngine(
        generators=[gen1, gen2],
        store=store
    )
    result = engine.run()

Best Practices
--------------

1. **Use base_path consistently**: Set ``base_path`` on both ``FileInput`` and
   ``FileOutput`` to ensure paths match correctly.

   .. code-block:: python

       from pathlib import Path
       base = Path("/data/project")

       gen = TaskGenerator(
           name="process:<doc>",
           inputs={"raw": FileInput("raw/<doc>.txt", base_path=base)},
           outputs=[FileOutput("processed/<doc>.json", base_path=base)],
           action=action_fn,
       )

2. **Design for convergence**: Ensure your generator patterns eventually stop
   producing new tasks. Avoid patterns that create infinite chains.

3. **Monitor with regenerations**: The ``regenerations`` count helps debug
   performance issues. High regeneration counts may indicate inefficient patterns.

4. **Start with conservative limits**: Use a low ``max_tasks`` during development
   to catch infinite loops early.

Comparison with Traditional doit
--------------------------------

Traditional doit:
    Tasks are defined statically in ``dodo.py``. The task graph is fixed before
    execution begins.

Reactive doit:
    Tasks are generated dynamically based on filesystem state. The task graph
    evolves as tasks execute and create outputs.

Choose reactive when:
    - You don't know all inputs upfront
    - Tasks create conditions for more tasks
    - You want tup-like incremental behavior

"""Reactive task generation with streaming fixed-point iteration.

This module provides a tup-like reactive execution model where:
1. Generators create tasks based on filesystem/input state
2. Tasks execute and create new outputs
3. New outputs immediately trigger generators to create NEW tasks
4. The process continues until no new tasks are generated (fixed-point)

Key difference from batch processing: after EACH task completes, we
immediately check if its outputs match any generator's input patterns
and inject new tasks into the ready queue. This streaming approach
ensures early-stage work is processed before unrelated later-stage tasks.

Example:
    from doit.reactive import ReactiveEngine
    from doit.taskgen import TaskGenerator, FileInput, FileOutput

    # Generator that processes raw files
    process_gen = TaskGenerator(
        name="process:<doc>",
        inputs={"raw": FileInput("raw/<doc>.txt")},
        outputs=[FileOutput("processed/<doc>.json")],
        action=lambda inp, out, attrs: process_document(inp, out),
    )

    # Generator that aggregates processed files
    aggregate_gen = TaskGenerator(
        name="aggregate",
        inputs={"data": FileInput("processed/*.json", is_list=True)},
        outputs=[FileOutput("output/summary.json")],
        action=lambda inp, out, attrs: aggregate_all(inp, out),
    )

    # Run reactive engine - streams tasks, regenerates on each completion
    engine = ReactiveEngine(generators=[process_gen, aggregate_gen])
    result = engine.run()

    print(f"Converged after {result.tasks_executed} tasks")
"""

from .engine import ReactiveEngine, ReactiveResult
from .manager import GeneratorManager
from .merger import TaskMerger, MergeResult
from .index import OutputPatternIndex

__all__ = [
    'ReactiveEngine',
    'ReactiveResult',
    'GeneratorManager',
    'TaskMerger',
    'MergeResult',
    'OutputPatternIndex',
]

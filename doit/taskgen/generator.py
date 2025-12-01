"""TaskGenerator - the primary interface for pattern-based task generation.

This module provides the TaskGenerator class which combines input patterns,
output patterns, and actions to automatically generate doit Task objects.

Example:
    gen = TaskGenerator(
        name="compile:<arch>:<module>",
        inputs={
            "source": FileInput("src/<arch>/<module>.c"),
            "headers": FileInput("include/<arch>/*.h", is_list=True),
        },
        outputs=[FileOutput("build/<arch>/<module>.o")],
        action=lambda inputs, outputs, attrs:
            f"gcc -c {inputs['source'].path} -o {outputs[0]}",
    )

    tasks = list(gen.generate())
"""

from dataclasses import dataclass
from typing import Dict, List, Callable, Any, Generator, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from doit.task import Task
    from .inputs import Input
    from .outputs import Output
    from .groups import InputSet


@dataclass
class TaskGenerator:
    """Generate tasks from input patterns, output patterns, and an action.

    This is the primary interface for pattern-based task generation.

    Attributes:
        name: Task name template with <capture> placeholders
        inputs: Dict mapping labels to Input instances
        outputs: List of Output instances for target generation
        action: Callable that receives (input_set, output_paths, attrs) and
                returns an action or list of actions
        doc: Optional doc string template with <capture> placeholders

    Example:
        gen = TaskGenerator(
            name="compile:<module>",
            inputs={"source": FileInput("src/<module>.c")},
            outputs=[FileOutput("build/<module>.o")],
            action=lambda inp, out, attrs: f"gcc -c {inp['source'].path} -o {out[0]}",
        )

        # Generate and use tasks
        for task in gen.generate():
            print(task.name)  # "compile:main", "compile:utils", etc.
    """

    name: str
    inputs: Dict[str, 'Input']
    outputs: List['Output']
    action: Callable[['InputSet', List[str], Dict[str, str]], Any]
    doc: Optional[str] = None

    def _render_template(self, template: str, attrs: Dict[str, str]) -> str:
        """Render a template string with attribute values."""
        result = template
        for key, value in attrs.items():
            result = result.replace(f'<{key}>', value)
        return result

    def _render_name(self, attrs: Dict[str, str]) -> str:
        """Render task name with attribute values."""
        return self._render_template(self.name, attrs)

    def _render_doc(self, attrs: Dict[str, str]) -> Optional[str]:
        """Render doc string with attribute values."""
        if self.doc is None:
            return None
        return self._render_template(self.doc, attrs)

    def generate(self) -> Generator['Task', None, None]:
        """Generate Task objects for all input permutations.

        For each valid combination of capture values, creates a Task with:
        - Name rendered from the name template
        - Dependencies from all matched inputs
        - Targets from rendered output patterns
        - Actions from the action callable

        Yields:
            Task objects ready for use with DoitEngine
        """
        from doit.task import Task
        from .groups import build_input_sets

        for input_set in build_input_sets(self.inputs):
            # Create outputs
            output_paths = []
            output_targets = []
            for out in self.outputs:
                path, target = out.create(input_set.attrs)
                output_paths.append(path)
                output_targets.append(target)

            # Build action
            action_result = self.action(input_set, output_paths, input_set.attrs)
            if isinstance(action_result, (list, tuple)):
                actions = list(action_result)
            else:
                actions = [action_result]

            # Get all dependencies
            dependencies = input_set.get_all_dependencies()

            # Create task
            # Note: We use outputs (Target objects) instead of targets (strings)
            # to leverage the new extensible dependency system. The Task will
            # derive string targets from the Target.get_key() method.
            yield Task(
                name=self._render_name(input_set.attrs),
                actions=actions,
                dependencies=dependencies,
                outputs=output_targets,
                doc=self._render_doc(input_set.attrs),
            )

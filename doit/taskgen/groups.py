"""Input grouping for pattern-based task generation.

This module provides InputSet and build_input_sets for grouping
matched inputs by their captured attribute values.

The permutation engine generates InputSets for every valid combination
of captured attribute values across all inputs.

Example:
    inputs = {
        "source": FileInput("src/<arch>/<module>.c"),
        "headers": FileInput("include/<arch>/*.h", is_list=True),
    }
    # Given files: src/x86/main.c, src/arm/main.c, include/x86/types.h

    for input_set in build_input_sets(inputs):
        print(input_set.attrs)  # {'arch': 'x86', 'module': 'main'}
        print(input_set['source'])  # FileDependency for src/x86/main.c
        print(input_set['headers'])  # [FileDependency, ...] for x86 headers
"""

from dataclasses import dataclass
from typing import Dict, Any, List, Generator, TYPE_CHECKING
from itertools import product

if TYPE_CHECKING:
    from .inputs import Input, CaptureMatch


@dataclass
class InputSet:
    """A grouped set of inputs sharing common attribute values.

    Attributes:
        attrs: Dict mapping capture names to their values for this set
        items: Dict mapping input labels to Dependency or List[Dependency]
    """
    attrs: Dict[str, str]
    items: Dict[str, Any]

    def __getitem__(self, label: str) -> Any:
        """Get input by label."""
        return self.items[label]

    def get(self, label: str, default: Any = None) -> Any:
        """Get input by label with optional default."""
        return self.items.get(label, default)

    def get_all_dependencies(self) -> List[Any]:
        """Flatten all dependencies into a single list.

        Returns:
            List of all Dependency objects from all inputs
        """
        deps = []
        for item in self.items.values():
            if isinstance(item, list):
                deps.extend(item)
            elif item is not None:
                deps.append(item)
        return deps


def build_input_sets(
    inputs: Dict[str, 'Input']
) -> Generator[InputSet, None, None]:
    """Generate InputSets for all attribute permutations.

    This function:
    1. Collects all matches from each input
    2. Extracts unique values for each capture name
    3. Generates all permutations of capture values
    4. For each permutation, collects matching dependencies

    Args:
        inputs: Dict mapping labels to Input instances

    Yields:
        InputSet for each valid attribute combination

    Example:
        inputs = {
            "source": FileInput("src/<arch>/<module>.c"),
            "config": FileInput("config/<arch>.json"),
        }

        # Given files:
        #   src/x86/main.c, src/x86/utils.c
        #   src/arm/main.c
        #   config/x86.json, config/arm.json

        for iset in build_input_sets(inputs):
            # Yields:
            # InputSet(attrs={'arch': 'x86', 'module': 'main'}, ...)
            # InputSet(attrs={'arch': 'x86', 'module': 'utils'}, ...)
            # InputSet(attrs={'arch': 'arm', 'module': 'main'}, ...)
            pass
    """
    # Handle empty inputs
    if not inputs:
        return

    # Step 1: Collect all matches per input
    matches_by_label: Dict[str, List['CaptureMatch']] = {}
    for label, inp in inputs.items():
        matches_by_label[label] = list(inp.match())

    # Step 2: Collect all unique values per capture name
    all_capture_names = set()
    for inp in inputs.values():
        all_capture_names.update(inp.capture_names)

    # Handle case with no captures
    if not all_capture_names:
        # No captures means we create one InputSet with all matches
        items: Dict[str, Any] = {}
        missing_required = False

        for label, inp in inputs.items():
            matches = matches_by_label.get(label, [])
            if inp.is_list:
                items[label] = [m.dependency for m in matches]
            elif matches:
                items[label] = matches[0].dependency
            else:
                items[label] = None

            if inp.required and not matches:
                missing_required = True

        if not missing_required:
            yield InputSet(attrs={}, items=items)
        return

    attr_values: Dict[str, set] = {name: set() for name in all_capture_names}
    for label, inp in inputs.items():
        for m in matches_by_label.get(label, []):
            for name in inp.capture_names:
                if name in m.captures:
                    attr_values[name].add(m.captures[name])

    # Handle case where any capture has no values
    if any(len(vals) == 0 for vals in attr_values.values()):
        return

    # Step 3: Permute all attribute combinations
    attr_names = sorted(attr_values.keys())
    value_lists = [sorted(attr_values[name]) for name in attr_names]

    for values in product(*value_lists):
        attrs = dict(zip(attr_names, values))
        items: Dict[str, Any] = {}
        missing_required = False

        for label, inp in inputs.items():
            # Find matches compatible with this attr combination
            matching = []
            for m in matches_by_label.get(label, []):
                match_ok = all(
                    m.captures.get(name) == attrs[name]
                    for name in inp.capture_names
                    if name in attrs
                )
                if match_ok:
                    matching.append(m.dependency)

            if inp.is_list:
                items[label] = matching
            elif matching:
                items[label] = matching[0]
            else:
                items[label] = None

            if inp.required and not matching:
                missing_required = True

        if not missing_required:
            yield InputSet(attrs=attrs, items=items)

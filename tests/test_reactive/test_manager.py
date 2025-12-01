"""Tests for GeneratorManager."""

import pytest
from unittest.mock import MagicMock

from doit.reactive.manager import GeneratorManager
from doit.reactive.index import OutputPatternIndex


def make_mock_generator(name: str, input_patterns: dict = None, tasks: list = None):
    """Create a mock generator."""
    gen = MagicMock()
    gen.name = name

    if input_patterns is None:
        input_patterns = {}

    inputs = {}
    for label, pattern in input_patterns.items():
        inp = MagicMock()
        inp.pattern = pattern
        inputs[label] = inp

    gen.inputs = inputs

    if tasks is None:
        tasks = []
    gen.generate.return_value = iter(tasks)

    return gen


class TestGeneratorManagerBasics:
    """Basic tests for GeneratorManager."""

    def test_empty_manager(self):
        """Test manager with no generators."""
        manager = GeneratorManager()
        assert manager.generator_count == 0

    def test_add_generator(self):
        """Test adding a single generator."""
        manager = GeneratorManager()
        gen = make_mock_generator("gen1", {"data": "processed/<doc>.json"})

        manager.add_generator(gen)

        assert manager.generator_count == 1

    def test_add_generators(self):
        """Test adding multiple generators."""
        manager = GeneratorManager()
        gen1 = make_mock_generator("gen1", {"data": "processed/<doc>.json"})
        gen2 = make_mock_generator("gen2", {"raw": "raw/<file>.txt"})

        manager.add_generators([gen1, gen2])

        assert manager.generator_count == 2

    def test_init_with_generators(self):
        """Test initializing with generators."""
        gen1 = make_mock_generator("gen1", {"data": "processed/<doc>.json"})
        gen2 = make_mock_generator("gen2", {"raw": "raw/<file>.txt"})

        manager = GeneratorManager(generators=[gen1, gen2])

        assert manager.generator_count == 2
        assert manager.prefix_count == 2


class TestRegenerateAll:
    """Tests for regenerate_all method."""

    def test_regenerate_all_empty(self):
        """Test regenerating with no generators."""
        manager = GeneratorManager()
        tasks = manager.regenerate_all()
        assert len(tasks) == 0

    def test_regenerate_all_single(self):
        """Test regenerating a single generator."""
        task = MagicMock()
        gen = make_mock_generator(
            "gen1",
            {"data": "processed/<doc>.json"},
            tasks=[task]
        )

        manager = GeneratorManager(generators=[gen])
        tasks = manager.regenerate_all()

        assert len(tasks) == 1
        assert task in tasks
        gen.generate.assert_called_once()

    def test_regenerate_all_multiple(self):
        """Test regenerating multiple generators."""
        task1 = MagicMock()
        task2 = MagicMock()

        gen1 = make_mock_generator("gen1", {"data": "processed/<doc>.json"}, [task1])
        gen2 = make_mock_generator("gen2", {"raw": "raw/<file>.txt"}, [task2])

        manager = GeneratorManager(generators=[gen1, gen2])
        tasks = manager.regenerate_all()

        assert len(tasks) == 2
        assert task1 in tasks
        assert task2 in tasks


class TestRegenerateAffected:
    """Tests for regenerate_affected method."""

    def test_regenerate_affected_no_match(self):
        """Test regenerating with no matching outputs."""
        gen = make_mock_generator("gen1", {"data": "processed/<doc>.json"}, [])

        manager = GeneratorManager(generators=[gen])
        tasks = manager.regenerate_affected(["other/file.txt"])

        assert len(tasks) == 0
        # Generator should not have been called again after init
        gen.generate.assert_not_called()

    def test_regenerate_affected_with_match(self):
        """Test regenerating when output matches a pattern."""
        task = MagicMock()
        gen = make_mock_generator(
            "gen1",
            {"data": "processed/<doc>.json"},
            [task]
        )

        manager = GeneratorManager(generators=[gen])
        tasks = manager.regenerate_affected(["processed/report.json"])

        assert len(tasks) == 1
        assert task in tasks
        gen.generate.assert_called_once()

    def test_regenerate_affected_empty_outputs(self):
        """Test regenerating with empty outputs list."""
        gen = make_mock_generator("gen1", {"data": "processed/<doc>.json"}, [])

        manager = GeneratorManager(generators=[gen])
        tasks = manager.regenerate_affected([])

        assert len(tasks) == 0

    def test_regenerate_affected_multiple_generators(self):
        """Test regenerating multiple affected generators."""
        task1 = MagicMock()
        task2 = MagicMock()

        gen1 = make_mock_generator("gen1", {"data": "processed/<doc>.json"}, [task1])
        gen2 = make_mock_generator("gen2", {"raw": "processed/<file>.csv"}, [task2])

        manager = GeneratorManager(generators=[gen1, gen2])
        # Both generators have patterns matching "processed/"
        tasks = manager.regenerate_affected(["processed/data.json"])

        # Both generators should produce tasks
        assert len(tasks) == 2
        assert task1 in tasks
        assert task2 in tasks


class TestFindAffectedGenerators:
    """Tests for find_affected_generators method."""

    def test_find_affected_empty(self):
        """Test finding with no generators."""
        manager = GeneratorManager()
        affected = manager.find_affected_generators(["processed/file.json"])
        assert len(affected) == 0

    def test_find_affected_with_match(self):
        """Test finding generators that match."""
        gen = make_mock_generator("gen1", {"data": "processed/<doc>.json"})

        manager = GeneratorManager(generators=[gen])
        affected = manager.find_affected_generators(["processed/report.json"])

        assert gen in affected

    def test_find_affected_no_match(self):
        """Test finding when no generators match."""
        gen = make_mock_generator("gen1", {"data": "processed/<doc>.json"})

        manager = GeneratorManager(generators=[gen])
        affected = manager.find_affected_generators(["other/file.txt"])

        assert gen not in affected
        assert len(affected) == 0


class TestClear:
    """Tests for clearing the manager."""

    def test_clear(self):
        """Test clearing all state."""
        gen = make_mock_generator("gen1", {"data": "processed/<doc>.json"})
        manager = GeneratorManager(generators=[gen])

        assert manager.generator_count == 1

        manager.clear()

        assert manager.generator_count == 0
        assert manager.prefix_count == 0

    def test_clear_then_add(self):
        """Test adding generators after clear."""
        gen1 = make_mock_generator("gen1", {"data": "processed/<doc>.json"})
        manager = GeneratorManager(generators=[gen1])

        manager.clear()

        gen2 = make_mock_generator("gen2", {"raw": "raw/<file>.txt"})
        manager.add_generator(gen2)

        assert manager.generator_count == 1

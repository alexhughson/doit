"""Tests for ShellAction with variable injection."""

import pytest
from unittest.mock import MagicMock

pytest.importorskip("yaml")

from doit.yaml.action import ShellAction


def make_mock_input_set(items=None, attrs=None):
    """Create a mock InputSet for testing."""
    mock = MagicMock()
    mock.items = items or {}
    mock.attrs = attrs or {}
    return mock


def make_mock_dependency(key):
    """Create a mock dependency with get_key method."""
    mock = MagicMock()
    mock.get_key.return_value = key
    return mock


class TestShellActionSubstitution:
    """Tests for variable substitution."""

    def test_attribute_substitution(self):
        """Test substitution of captured attributes."""
        input_set = make_mock_input_set(
            items={},
            attrs={'module': 'main', 'arch': 'x86'}
        )

        action = ShellAction(
            template="echo {module} {arch}",
            input_set=input_set,
            output_paths=[],
            attrs={'module': 'main', 'arch': 'x86'},
        )

        subs = action._build_substitutions()
        assert subs['module'] == 'main'
        assert subs['arch'] == 'x86'

    def test_single_input_substitution(self):
        """Test substitution of single input."""
        dep = make_mock_dependency('/path/to/main.c')
        input_set = make_mock_input_set(
            items={'source': dep},
        )

        action = ShellAction(
            template="gcc -c {source}",
            input_set=input_set,
            output_paths=[],
            attrs={},
        )

        subs = action._build_substitutions()
        assert subs['source'] == '/path/to/main.c'

    def test_list_input_substitution(self):
        """Test substitution of list input."""
        deps = [
            make_mock_dependency('/path/h1.h'),
            make_mock_dependency('/path/h2.h'),
        ]
        input_set = make_mock_input_set(
            items={'headers': deps},
        )

        action = ShellAction(
            template="gcc {headers}",
            input_set=input_set,
            output_paths=[],
            attrs={},
        )

        subs = action._build_substitutions()
        assert subs['headers'] == '/path/h1.h /path/h2.h'

    def test_output_substitution(self):
        """Test substitution of output paths."""
        input_set = make_mock_input_set()

        action = ShellAction(
            template="cmd -o {out_0} -d {out_1}",
            input_set=input_set,
            output_paths=['/build/main.o', '/build/main.d'],
            attrs={},
        )

        subs = action._build_substitutions()
        assert subs['out_0'] == '/build/main.o'
        assert subs['out_1'] == '/build/main.d'

    def test_combined_substitution(self):
        """Test all substitutions together."""
        dep = make_mock_dependency('/src/main.c')
        input_set = make_mock_input_set(
            items={'source': dep},
        )

        action = ShellAction(
            template="gcc -c {source} -DARCH={arch} -o {out_0}",
            input_set=input_set,
            output_paths=['/build/main.o'],
            attrs={'module': 'main', 'arch': 'x86'},
        )

        subs = action._build_substitutions()
        assert subs['source'] == '/src/main.c'
        assert subs['module'] == 'main'
        assert subs['arch'] == 'x86'
        assert subs['out_0'] == '/build/main.o'


class TestShellActionFormatCommand:
    """Tests for command formatting."""

    def test_format_simple(self):
        """Test simple command formatting."""
        input_set = make_mock_input_set()
        action = ShellAction(
            template="echo hello",
            input_set=input_set,
            output_paths=[],
            attrs={},
        )

        cmd = action._format_command({})
        assert cmd == "echo hello"

    def test_format_with_substitutions(self):
        """Test command formatting with substitutions."""
        input_set = make_mock_input_set()
        action = ShellAction(
            template="gcc -c {source} -o {out_0}",
            input_set=input_set,
            output_paths=[],
            attrs={},
        )

        subs = {'source': '/main.c', 'out_0': '/main.o'}
        cmd = action._format_command(subs)
        assert cmd == "gcc -c /main.c -o /main.o"

    def test_format_unknown_variable_error(self):
        """Test error on unknown variable."""
        input_set = make_mock_input_set()
        action = ShellAction(
            template="echo {unknown_var}",
            input_set=input_set,
            output_paths=[],
            attrs={},
        )

        with pytest.raises(KeyError, match="Unknown variable"):
            action._format_command({'known': 'value'})


class TestShellActionEnvironment:
    """Tests for environment variable injection."""

    def test_env_includes_attrs(self):
        """Test that attrs are included in environment."""
        input_set = make_mock_input_set()
        action = ShellAction(
            template="echo",
            input_set=input_set,
            output_paths=[],
            attrs={'module': 'main'},
        )

        subs = {'module': 'main'}
        env = action._build_environment(subs)
        assert env['module'] == 'main'

    def test_env_includes_inputs(self):
        """Test that inputs are included in environment."""
        dep = make_mock_dependency('/path/file.c')
        input_set = make_mock_input_set(items={'source': dep})
        action = ShellAction(
            template="echo",
            input_set=input_set,
            output_paths=[],
            attrs={},
        )

        subs = {'source': '/path/file.c'}
        env = action._build_environment(subs)
        assert env['source'] == '/path/file.c'

    def test_env_includes_outputs(self):
        """Test that outputs are included in environment."""
        input_set = make_mock_input_set()
        action = ShellAction(
            template="echo",
            input_set=input_set,
            output_paths=['/out.txt'],
            attrs={},
        )

        subs = {'out_0': '/out.txt'}
        env = action._build_environment(subs)
        assert env['out_0'] == '/out.txt'


class TestShellActionExecution:
    """Tests for actual command execution."""

    def test_execute_simple_command(self):
        """Test executing a simple command."""
        input_set = make_mock_input_set()
        action = ShellAction(
            template="echo hello",
            input_set=input_set,
            output_paths=[],
            attrs={},
        )

        result = action()
        assert result is True

    def test_execute_with_substitution(self):
        """Test executing command with substitutions."""
        dep = make_mock_dependency('world')
        input_set = make_mock_input_set(items={'name': dep})
        action = ShellAction(
            template="echo {name}",
            input_set=input_set,
            output_paths=[],
            attrs={},
        )

        result = action()
        assert result is True

    def test_execute_failing_command(self):
        """Test that failing command raises error."""
        import subprocess

        input_set = make_mock_input_set()
        action = ShellAction(
            template="exit 1",
            input_set=input_set,
            output_paths=[],
            attrs={},
        )

        with pytest.raises(subprocess.CalledProcessError):
            action()

    def test_repr(self):
        """Test string representation."""
        input_set = make_mock_input_set()
        action = ShellAction(
            template="gcc -c {source}",
            input_set=input_set,
            output_paths=[],
            attrs={},
        )

        assert repr(action) == "ShellAction('gcc -c {source}')"


class TestShellActionNoneHandling:
    """Tests for handling None values in inputs."""

    def test_none_input_skipped(self):
        """Test that None input is skipped."""
        input_set = make_mock_input_set(
            items={'optional': None, 'required': make_mock_dependency('/file.c')},
        )

        action = ShellAction(
            template="gcc {required}",
            input_set=input_set,
            output_paths=[],
            attrs={},
        )

        subs = action._build_substitutions()
        assert 'optional' not in subs
        assert subs['required'] == '/file.c'

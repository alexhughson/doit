"""Tests for doit.taskgen.groups module."""

import pytest
from pathlib import Path

from doit.taskgen.groups import InputSet, build_input_sets
from doit.taskgen.inputs import FileInput
from doit.deps import FileDependency


class TestInputSet:
    """Tests for InputSet class."""

    def test_basic_access(self):
        """Test basic item access."""
        dep = FileDependency("/path/to/file.c")
        iset = InputSet(
            attrs={"arch": "x86"},
            items={"source": dep}
        )
        assert iset.attrs == {"arch": "x86"}
        assert iset["source"] is dep

    def test_getitem(self):
        """Test __getitem__ method."""
        dep = FileDependency("/path/to/file.c")
        iset = InputSet(attrs={}, items={"source": dep})
        assert iset["source"] is dep

    def test_getitem_missing_raises(self):
        """Test that missing label raises KeyError."""
        iset = InputSet(attrs={}, items={})
        with pytest.raises(KeyError):
            _ = iset["missing"]

    def test_get_with_default(self):
        """Test get() with default value."""
        iset = InputSet(attrs={}, items={"a": "value"})
        assert iset.get("a") == "value"
        assert iset.get("missing") is None
        assert iset.get("missing", "default") == "default"

    def test_get_all_dependencies_single(self):
        """Test get_all_dependencies with single items."""
        dep1 = FileDependency("/a.c")
        dep2 = FileDependency("/b.c")
        iset = InputSet(attrs={}, items={"a": dep1, "b": dep2})
        deps = iset.get_all_dependencies()
        assert len(deps) == 2
        assert dep1 in deps
        assert dep2 in deps

    def test_get_all_dependencies_list(self):
        """Test get_all_dependencies with list items."""
        dep1 = FileDependency("/a.c")
        dep2 = FileDependency("/b.c")
        dep3 = FileDependency("/c.c")
        iset = InputSet(
            attrs={},
            items={"single": dep1, "multi": [dep2, dep3]}
        )
        deps = iset.get_all_dependencies()
        assert len(deps) == 3

    def test_get_all_dependencies_none(self):
        """Test get_all_dependencies skips None values."""
        dep = FileDependency("/a.c")
        iset = InputSet(attrs={}, items={"a": dep, "b": None})
        deps = iset.get_all_dependencies()
        assert len(deps) == 1
        assert dep in deps

    def test_get_all_dependencies_empty_list(self):
        """Test get_all_dependencies handles empty lists."""
        dep = FileDependency("/a.c")
        iset = InputSet(attrs={}, items={"a": dep, "b": []})
        deps = iset.get_all_dependencies()
        assert len(deps) == 1


class TestBuildInputSetsSingleCapture:
    """Tests for build_input_sets with single capture."""

    def test_single_file_single_capture(self, tmp_path):
        """Test with one file matching pattern."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "main.c").write_text("code")

        inputs = {
            "source": FileInput("src/<module>.c", base_path=tmp_path)
        }
        sets = list(build_input_sets(inputs))

        assert len(sets) == 1
        assert sets[0].attrs == {"module": "main"}
        assert isinstance(sets[0]["source"], FileDependency)

    def test_multiple_files_single_capture(self, tmp_path):
        """Test with multiple files matching pattern."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "main.c").write_text("main")
        (src_dir / "utils.c").write_text("utils")

        inputs = {
            "source": FileInput("src/<module>.c", base_path=tmp_path)
        }
        sets = list(build_input_sets(inputs))

        assert len(sets) == 2
        modules = {s.attrs["module"] for s in sets}
        assert modules == {"main", "utils"}


class TestBuildInputSetsMultipleCaptures:
    """Tests for build_input_sets with multiple captures."""

    def test_two_captures_permutation(self, tmp_path):
        """Test permutation with two captures."""
        for arch in ["x86", "arm"]:
            arch_dir = tmp_path / "src" / arch
            arch_dir.mkdir(parents=True)
            (arch_dir / "main.c").write_text(f"{arch} main")

        inputs = {
            "source": FileInput("src/<arch>/<module>.c", base_path=tmp_path)
        }
        sets = list(build_input_sets(inputs))

        assert len(sets) == 2
        attrs_set = {(s.attrs["arch"], s.attrs["module"]) for s in sets}
        assert attrs_set == {("x86", "main"), ("arm", "main")}

    def test_full_permutation(self, tmp_path):
        """Test full permutation of all values."""
        for arch in ["x86", "arm"]:
            for module in ["main", "utils"]:
                path = tmp_path / "src" / arch
                path.mkdir(parents=True, exist_ok=True)
                (path / f"{module}.c").write_text(f"{arch} {module}")

        inputs = {
            "source": FileInput("src/<arch>/<module>.c", base_path=tmp_path)
        }
        sets = list(build_input_sets(inputs))

        assert len(sets) == 4
        attrs_set = {(s.attrs["arch"], s.attrs["module"]) for s in sets}
        assert attrs_set == {
            ("x86", "main"), ("x86", "utils"),
            ("arm", "main"), ("arm", "utils"),
        }


class TestBuildInputSetsMultipleInputs:
    """Tests for build_input_sets with multiple inputs."""

    def test_two_inputs_same_capture(self, tmp_path):
        """Test two inputs sharing same capture name."""
        src_dir = tmp_path / "src"
        cfg_dir = tmp_path / "config"
        src_dir.mkdir()
        cfg_dir.mkdir()
        (src_dir / "main.c").write_text("code")
        (cfg_dir / "main.json").write_text("{}")

        inputs = {
            "source": FileInput("src/<module>.c", base_path=tmp_path),
            "config": FileInput("config/<module>.json", base_path=tmp_path),
        }
        sets = list(build_input_sets(inputs))

        assert len(sets) == 1
        assert sets[0].attrs == {"module": "main"}
        assert isinstance(sets[0]["source"], FileDependency)
        assert isinstance(sets[0]["config"], FileDependency)

    def test_two_inputs_different_captures(self, tmp_path):
        """Test two inputs with different capture names."""
        (tmp_path / "src").mkdir()
        (tmp_path / "config").mkdir()
        (tmp_path / "src" / "main.c").write_text("code")
        (tmp_path / "config" / "dev.json").write_text("{}")

        inputs = {
            "source": FileInput("src/<module>.c", base_path=tmp_path),
            "config": FileInput("config/<env>.json", base_path=tmp_path),
        }
        sets = list(build_input_sets(inputs))

        # Should permute: module=main × env=dev
        assert len(sets) == 1
        assert sets[0].attrs == {"env": "dev", "module": "main"}

    def test_cross_product_of_captures(self, tmp_path):
        """Test cross product when inputs have different captures."""
        (tmp_path / "src").mkdir()
        (tmp_path / "config").mkdir()
        (tmp_path / "src" / "main.c").write_text("main")
        (tmp_path / "src" / "utils.c").write_text("utils")
        (tmp_path / "config" / "dev.json").write_text("{}")
        (tmp_path / "config" / "prod.json").write_text("{}")

        inputs = {
            "source": FileInput("src/<module>.c", base_path=tmp_path),
            "config": FileInput("config/<env>.json", base_path=tmp_path),
        }
        sets = list(build_input_sets(inputs))

        # 2 modules × 2 envs = 4 sets
        assert len(sets) == 4


class TestBuildInputSetsIsList:
    """Tests for build_input_sets with is_list inputs."""

    def test_is_list_collects_multiple(self, tmp_path):
        """Test that is_list collects all matching files."""
        arch_dir = tmp_path / "include" / "x86"
        arch_dir.mkdir(parents=True)
        (arch_dir / "types.h").write_text("types")
        (arch_dir / "defs.h").write_text("defs")

        src_dir = tmp_path / "src" / "x86"
        src_dir.mkdir(parents=True)
        (src_dir / "main.c").write_text("main")

        inputs = {
            "source": FileInput("src/<arch>/<module>.c", base_path=tmp_path),
            "headers": FileInput(
                "include/<arch>/*.h",
                base_path=tmp_path,
                is_list=True
            ),
        }
        sets = list(build_input_sets(inputs))

        assert len(sets) == 1
        assert isinstance(sets[0]["source"], FileDependency)
        assert isinstance(sets[0]["headers"], list)
        assert len(sets[0]["headers"]) == 2


class TestBuildInputSetsRequired:
    """Tests for build_input_sets with required inputs."""

    def test_required_missing_skips_set(self, tmp_path):
        """Test that missing required input skips that permutation."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("main")

        inputs = {
            "source": FileInput("src/<module>.c", base_path=tmp_path),
            "config": FileInput(
                "config/<module>.json",
                base_path=tmp_path,
                required=True
            ),
        }
        sets = list(build_input_sets(inputs))

        # No config files exist, so no sets should be yielded
        assert len(sets) == 0

    def test_optional_missing_included(self, tmp_path):
        """Test that missing optional input still yields set."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("main")

        inputs = {
            "source": FileInput("src/<module>.c", base_path=tmp_path),
            "config": FileInput(
                "config/<module>.json",
                base_path=tmp_path,
                required=False
            ),
        }
        sets = list(build_input_sets(inputs))

        assert len(sets) == 1
        assert sets[0]["source"] is not None
        assert sets[0]["config"] is None  # Optional, missing

    def test_partial_required_match(self, tmp_path):
        """Test some permutations have required, some don't."""
        (tmp_path / "src").mkdir()
        (tmp_path / "config").mkdir()
        (tmp_path / "src" / "main.c").write_text("main")
        (tmp_path / "src" / "utils.c").write_text("utils")
        (tmp_path / "config" / "main.json").write_text("{}")  # Only main has config

        inputs = {
            "source": FileInput("src/<module>.c", base_path=tmp_path),
            "config": FileInput(
                "config/<module>.json",
                base_path=tmp_path,
                required=True
            ),
        }
        sets = list(build_input_sets(inputs))

        # Only main should be yielded (utils missing config)
        assert len(sets) == 1
        assert sets[0].attrs["module"] == "main"


class TestBuildInputSetsEdgeCases:
    """Tests for edge cases in build_input_sets."""

    def test_empty_inputs(self):
        """Test with empty inputs dict."""
        sets = list(build_input_sets({}))
        assert len(sets) == 0

    def test_no_matches(self, tmp_path):
        """Test when no files match pattern."""
        (tmp_path / "src").mkdir()

        inputs = {
            "source": FileInput("src/<module>.c", base_path=tmp_path)
        }
        sets = list(build_input_sets(inputs))

        assert len(sets) == 0

    def test_no_captures(self, tmp_path):
        """Test pattern without captures."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.c").write_text("main")

        inputs = {
            "source": FileInput("src/main.c", base_path=tmp_path)
        }
        sets = list(build_input_sets(inputs))

        assert len(sets) == 1
        assert sets[0].attrs == {}
        assert isinstance(sets[0]["source"], FileDependency)

    def test_is_list_empty_for_some_attrs(self, tmp_path):
        """Test is_list input with no matches for some attr values."""
        (tmp_path / "src" / "x86").mkdir(parents=True)
        (tmp_path / "src" / "arm").mkdir(parents=True)
        (tmp_path / "include" / "x86").mkdir(parents=True)
        # arm has no headers

        (tmp_path / "src" / "x86" / "main.c").write_text("x86 main")
        (tmp_path / "src" / "arm" / "main.c").write_text("arm main")
        (tmp_path / "include" / "x86" / "types.h").write_text("types")

        inputs = {
            "source": FileInput("src/<arch>/<module>.c", base_path=tmp_path),
            "headers": FileInput(
                "include/<arch>/*.h",
                base_path=tmp_path,
                is_list=True,
                required=False
            ),
        }
        sets = list(build_input_sets(inputs))

        assert len(sets) == 2
        for s in sets:
            if s.attrs["arch"] == "x86":
                assert len(s["headers"]) == 1
            else:
                assert s["headers"] == []

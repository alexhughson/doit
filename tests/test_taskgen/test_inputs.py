"""Tests for doit.taskgen.inputs module."""

import pytest
from pathlib import Path

from doit.taskgen.inputs import Input, FileInput, CaptureMatch
from doit.deps import FileDependency


class TestCaptureMatch:
    """Tests for CaptureMatch dataclass."""

    def test_basic_attributes(self):
        dep = FileDependency("/path/to/file.txt")
        match = CaptureMatch(
            key="/path/to/file.txt",
            captures={"name": "file"},
            dependency=dep,
        )
        assert match.key == "/path/to/file.txt"
        assert match.captures == {"name": "file"}
        assert match.dependency is dep


class TestInputPatternCompilation:
    """Tests for pattern compilation in Input base class."""

    def test_single_capture(self):
        """Test pattern with single capture."""
        inp = FileInput("src/<module>.c")
        assert inp._glob_pattern == "src/*.c"
        assert inp.capture_names == ["module"]

    def test_multiple_captures(self):
        """Test pattern with multiple captures."""
        inp = FileInput("src/<arch>/<module>.c")
        assert inp._glob_pattern == "src/*/*.c"
        assert inp.capture_names == ["arch", "module"]

    def test_no_captures(self):
        """Test pattern without captures."""
        inp = FileInput("src/main.c")
        assert inp._glob_pattern == "src/main.c"
        assert inp.capture_names == []

    def test_capture_with_wildcard(self):
        """Test pattern with both capture and wildcard."""
        inp = FileInput("src/<module>.page*.txt")
        assert inp._glob_pattern == "src/*.page*.txt"
        assert inp.capture_names == ["module"]
        # is_list should be auto-detected
        assert inp.is_list is True

    def test_capture_regex_matches(self):
        """Test that compiled regex correctly matches and extracts captures."""
        inp = FileInput("src/<arch>/<module>.c")
        # Test matching
        match = inp._capture_regex.match("src/x86/main.c")
        assert match is not None
        assert match.groupdict() == {"arch": "x86", "module": "main"}

        # Test non-matching
        assert inp._capture_regex.match("src/main.c") is None
        assert inp._capture_regex.match("other/x86/main.c") is None

    def test_special_regex_chars_escaped(self):
        """Test that special regex chars in pattern are escaped."""
        inp = FileInput("data/<name>.json")
        # The . should be escaped in regex
        assert inp._capture_regex.match("data/test.json") is not None
        assert inp._capture_regex.match("data/testXjson") is None

    def test_absolute_path_pattern(self):
        """Test pattern with absolute path."""
        inp = FileInput("/data/textract/<doc>.txt")
        assert inp._glob_pattern == "/data/textract/*.txt"
        assert inp.capture_names == ["doc"]


class TestFileInputListResources:
    """Tests for FileInput.list_resources()."""

    def test_list_single_file(self, tmp_path):
        """Test listing a single file."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "main.c").write_text("int main() {}")

        inp = FileInput("src/<module>.c", base_path=tmp_path)
        resources = list(inp.list_resources())

        assert len(resources) == 1
        assert resources[0] == str(src_dir / "main.c")

    def test_list_multiple_files(self, tmp_path):
        """Test listing multiple files."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "main.c").write_text("main")
        (src_dir / "utils.c").write_text("utils")
        (src_dir / "helper.h").write_text("header")  # Should not match

        inp = FileInput("src/<module>.c", base_path=tmp_path)
        resources = list(inp.list_resources())

        assert len(resources) == 2
        assert any("main.c" in r for r in resources)
        assert any("utils.c" in r for r in resources)

    def test_list_nested_directories(self, tmp_path):
        """Test listing files in nested directories."""
        for arch in ["x86", "arm"]:
            arch_dir = tmp_path / "src" / arch
            arch_dir.mkdir(parents=True)
            (arch_dir / "main.c").write_text(f"{arch} main")

        inp = FileInput("src/<arch>/<module>.c", base_path=tmp_path)
        resources = list(inp.list_resources())

        assert len(resources) == 2

    def test_list_no_matches(self, tmp_path):
        """Test when no files match the pattern."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()

        inp = FileInput("src/<module>.c", base_path=tmp_path)
        resources = list(inp.list_resources())

        assert len(resources) == 0


class TestFileInputMatch:
    """Tests for FileInput.match()."""

    def test_match_extracts_captures(self, tmp_path):
        """Test that match() extracts capture values correctly."""
        src_dir = tmp_path / "src" / "x86"
        src_dir.mkdir(parents=True)
        (src_dir / "main.c").write_text("code")

        inp = FileInput("src/<arch>/<module>.c", base_path=tmp_path)
        matches = list(inp.match())

        assert len(matches) == 1
        assert matches[0].captures == {"arch": "x86", "module": "main"}

    def test_match_creates_dependencies(self, tmp_path):
        """Test that match() creates FileDependency objects."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "main.c").write_text("code")

        inp = FileInput("src/<module>.c", base_path=tmp_path)
        matches = list(inp.match())

        assert len(matches) == 1
        assert isinstance(matches[0].dependency, FileDependency)

    def test_match_multiple_captures(self, tmp_path):
        """Test matching with multiple capture dimensions."""
        for arch in ["x86", "arm"]:
            for module in ["main", "utils"]:
                path = tmp_path / "src" / arch
                path.mkdir(parents=True, exist_ok=True)
                (path / f"{module}.c").write_text(f"{arch} {module}")

        inp = FileInput("src/<arch>/<module>.c", base_path=tmp_path)
        matches = list(inp.match())

        assert len(matches) == 4
        captures_set = {(m.captures["arch"], m.captures["module"]) for m in matches}
        assert captures_set == {
            ("x86", "main"), ("x86", "utils"),
            ("arm", "main"), ("arm", "utils"),
        }


class TestFileInputIsListAutoDetection:
    """Tests for is_list auto-detection."""

    def test_wildcard_in_filename_sets_is_list(self):
        """Test that * in filename auto-sets is_list=True."""
        inp = FileInput("src/<doc>.page*.txt")
        assert inp.is_list is True

    def test_no_wildcard_keeps_is_list_false(self):
        """Test that pattern without * keeps is_list=False."""
        inp = FileInput("src/<module>.c")
        assert inp.is_list is False

    def test_explicit_is_list_overrides(self):
        """Test that explicit is_list=True is preserved."""
        inp = FileInput("src/<module>.c", is_list=True)
        assert inp.is_list is True

    def test_wildcard_in_directory_does_not_set_is_list(self):
        """Test that * in directory path does not auto-set is_list."""
        # * is in the path, not the filename
        inp = FileInput("src/*/<module>.c")
        # The filename is <module>.c which has no *, so is_list stays False
        assert inp.is_list is False


class TestFileInputRequired:
    """Tests for the required attribute."""

    def test_required_default_true(self):
        """Test that required defaults to True."""
        inp = FileInput("src/<module>.c")
        assert inp.required is True

    def test_required_can_be_set_false(self):
        """Test that required can be set to False."""
        inp = FileInput("src/<module>.c", required=False)
        assert inp.required is False


class TestFileInputBasePath:
    """Tests for base_path handling."""

    def test_base_path_defaults_to_cwd(self):
        """Test that base_path defaults to cwd."""
        inp = FileInput("src/<module>.c")
        assert inp.base_path == Path.cwd()

    def test_base_path_accepts_string(self):
        """Test that base_path accepts a string."""
        inp = FileInput("src/<module>.c", base_path="/tmp/project")
        assert inp.base_path == Path("/tmp/project")

    def test_base_path_accepts_path(self):
        """Test that base_path accepts a Path."""
        inp = FileInput("src/<module>.c", base_path=Path("/tmp/project"))
        assert inp.base_path == Path("/tmp/project")

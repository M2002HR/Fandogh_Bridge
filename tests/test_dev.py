from __future__ import annotations

from pathlib import Path

from bridge.dev import BridgeDevFilter, _format_changes, _resolve_watch_paths, load_dev_config
from watchfiles import Change


def test_resolve_watch_paths_keeps_existing(tmp_path) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    env_file = tmp_path / ".env"
    env_file.write_text("X=1", encoding="utf-8")

    paths = _resolve_watch_paths(tmp_path, ["src", ".env", "missing.txt"])
    assert paths == [src_dir.resolve(), env_file.resolve()]


def test_format_changes_compacts_names() -> None:
    changes = {
        (Change.modified, "/tmp/project/src/a.py"),
        (Change.modified, "/tmp/project/src/b.py"),
    }
    assert _format_changes(changes) == "a.py, b.py"


def test_bridge_dev_filter_ignores_configured_dirs(tmp_path) -> None:
    filt = BridgeDevFilter({"app_data"})
    assert filt(Change.modified, str(tmp_path / "src" / "x.py")) is True
    assert filt(Change.modified, str(tmp_path / "app_data" / "bridge.db")) is False


def test_load_dev_config_uses_current_interpreter(tmp_path, monkeypatch) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / ".env").write_text("", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DEV_WATCH_COMMAND", raising=False)
    monkeypatch.delenv("DEV_WATCH_PATHS", raising=False)

    config = load_dev_config()
    assert config.command[0]
    assert config.command[1:] == ["-m", "bridge.main"]
    assert Path(config.command[0]).name.startswith("python")
    assert [path.name for path in config.watch_paths] == ["src", ".env"]

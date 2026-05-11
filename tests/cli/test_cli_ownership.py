from __future__ import annotations

import os
import subprocess
from pathlib import Path

from cli.session import CLISession


def test_cli_session_owns_typed_runner_config(tmp_path: Path) -> None:
    session = CLISession(
        workspace_path=str(tmp_path),
        api_url="http://127.0.0.1:8082/v1",
        allowed_dirs=[str(tmp_path)],
        plans_directory=".plans",
        claude_bin="claude-test",
        skip_permissions=False,
    )

    assert session.config.workspace_path == str(tmp_path)
    assert session.config.api_url == "http://127.0.0.1:8082/v1"
    assert session.config.allowed_dirs == [str(tmp_path)]
    assert session.config.plans_directory == ".plans"
    assert session.config.claude_bin == "claude-test"
    assert session.config.skip_permissions is False


def test_claude_pick_uses_project_python_runner() -> None:
    script = Path(__file__).resolve().parents[2] / "claude-pick"
    text = script.read_text(encoding="utf-8")

    assert "uv run python" in text
    assert "python3 -c" not in text


def test_claude_pick_supports_all_configured_providers() -> None:
    script = Path(__file__).resolve().parents[2] / "claude-pick"
    text = script.read_text(encoding="utf-8")

    for provider in [
        "nvidia_nim",
        "open_router",
        "deepseek",
        "lmstudio",
        "llamacpp",
        "ollama",
    ]:
        assert provider in text


def test_claude_pick_inferrs_provider_from_model(tmp_path: Path) -> None:
    script = Path(__file__).resolve().parents[2] / "claude-pick"
    env_file = tmp_path / ".env"
    env_file.write_text('MODEL="deepseek/deepseek-chat"\n', encoding="utf-8")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fzf = bin_dir / "fzf"
    fzf.write_text("#!/usr/bin/env bash\nhead -n 1\n", encoding="utf-8")
    fzf.chmod(0o755)
    claude = bin_dir / "claude"
    claude.write_text(
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$ANTHROPIC_AUTH_TOKEN\"\n",
        encoding="utf-8",
    )
    claude.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "CLAUDE_PICK_ENV_FILE": str(env_file),
        "ANTHROPIC_AUTH_TOKEN": "",
    }
    env.pop("CLAUDE_PICK_PROVIDER", None)

    result = subprocess.run(
        ["bash", str(script)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == "freecc:deepseek-chat"

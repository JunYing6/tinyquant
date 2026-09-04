from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).parents[1]


def _run_example(name: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(ROOT / "examples" / name)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def test_in_memory_backtest_example_runs_without_credentials() -> None:
    result = _run_example("in_memory_backtest.py")

    assert result.returncode == 0, result.stderr
    assert "final_equity" in result.stdout


def test_in_memory_live_example_runs_without_credentials() -> None:
    result = _run_example("in_memory_live.py")

    assert result.returncode == 0, result.stderr
    assert "orders" in result.stdout

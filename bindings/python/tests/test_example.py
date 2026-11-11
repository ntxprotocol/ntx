"""Surface test: run the example the way a user would."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

EXAMPLE = Path(__file__).parent.parent / "examples" / "hello.py"


def test_hello_example() -> None:
    result = subprocess.run(
        [sys.executable, str(EXAMPLE)], capture_output=True, text=True, timeout=60, check=True
    )
    assert result.stdout.splitlines() == [
        "connected to Hello Source",
        "program: Hello from Python",
        "John 3:16 · KJV",
    ]

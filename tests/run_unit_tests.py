import sys
from pathlib import Path
import pytest
import subprocess

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"

sys.path.insert(0, str(SRC_ROOT))

# raise SystemExit(pytest.main(["tests"]))




def main():
    cmd = [
        sys.executable,
        "pytest",
        "tests/unit",
    ]

    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()

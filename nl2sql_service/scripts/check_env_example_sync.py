"""
CI guard: ensure nl2sql_service/.env.example is generated and synced.

This script:
- runs scripts/update_env_example.py
- asserts git diff is clean for .env.example
"""
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str]) -> None:
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    env_example = repo_root / ".env.example"
    if not env_example.exists():
        raise SystemExit(f"Missing file: {env_example}")

    # 强制在 nl2sql_service 目录执行，避免 cwd 影响相对路径
    subprocess.run(
        [sys.executable, "scripts/update_env_example.py"],
        cwd=str(repo_root),
        check=True,
    )
    subprocess.run(
        ["git", "diff", "--exit-code", "--", ".env.example"],
        cwd=str(repo_root),
        check=True,
    )


if __name__ == "__main__":
    main()


from pathlib import Path


def find_repo_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for parent in [current] + list(current.parents):
        if (
            (parent / "config.yaml").exists()
            or (parent / "config.toml").exists()
            or (parent / "config.json").exists()
            or (parent / "requirements.txt").exists()
        ):
            return parent
    return current


def resolve_path(root: Path, relative: str) -> Path:
    return (root / relative).resolve()

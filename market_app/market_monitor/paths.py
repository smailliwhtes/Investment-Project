from pathlib import Path


def find_repo_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for parent in [current] + list(current.parents):
        if (
            (parent / ".git").exists()
            or (parent / "config.yaml").exists()
            or (parent / "config.toml").exists()
            or (parent / "config.json").exists()
            or (parent / "requirements.txt").exists()
        ):
            return parent
    return current


def resolve_path(base_dir: Path, path: str | Path | None) -> Path | None:
    if not path:
        return None
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (base_dir / candidate).resolve()

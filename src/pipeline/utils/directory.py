from pathlib import Path


def create_filepath(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)
    return

try:
    import tomllib
except ImportError:
    import tomli as tomllib

from pathlib import Path

PROJECT_NAME = "protobunny"
PACKAGE_NAME = "protobunny"
GENERATED_PACKAGE_NAME = "core"
ROOT_GENERATED_PACKAGE_NAME = f"{PACKAGE_NAME}.{GENERATED_PACKAGE_NAME}."
# PREFIX_MESSAGES = "pb"
MESSAGES_DIRECTORY = "protobuf"
VERSION = "0.1.0"


def load_config(start_path: str | Path | None = None) -> dict:
    """Load user config `[tool.protobunny]` from the nearest pyproject.toml."""
    start_path = Path(start_path or Path.cwd())
    res = {
        "messages-directory": "messages",
        "messages-prefix": "pb",
        "generated-package-name": "codegen",
        "project-root": "./",
    }
    for folder in [start_path, *start_path.parents]:
        pyproject = folder / "pyproject.toml"
        if pyproject.exists():
            data = tomllib.loads(pyproject.read_text())
            res.update(data.get("tool", {}).get("protobunny", {}))
            res["project-root"] = folder.name
    return res

import dataclasses
import functools
import typing as tp

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore

from pathlib import Path

PROJECT_NAME = "protobunny"
PACKAGE_NAME = "protobunny"
GENERATED_PACKAGE_NAME = "core"
ROOT_GENERATED_PACKAGE_NAME = f"{PACKAGE_NAME}.{GENERATED_PACKAGE_NAME}"
PREFIX_MESSAGES = "pb"
MESSAGES_DIRECTORY = "protobuf/protobunny"
VERSION = "0.1.1b1"


@dataclasses.dataclass
class Config:
    messages_directory: str = "messages"
    messages_prefix: str = PREFIX_MESSAGES
    project_name: str = PROJECT_NAME
    project_root: str = "./"
    force_required_fields: bool = False
    generated_package_name: str = "codegen"
    mode: str = "sync"

    @property
    def use_async(self) -> bool:
        return self.mode == "async"


@functools.cache
def load_config() -> Config:
    """Load user config `[tool.protobunny]` from the nearest pyproject.toml."""
    config, folder = get_config_from_pyproject()
    if "generated-package-name" not in config:
        config["generated-package-name"] = f"{config['project-name']}.codegen"
    return Config(**{k.replace("-", "_"): v for k, v in config.items()})


def get_config_from_pyproject() -> tuple[dict[str, tp.Any], Path | None]:
    start_path = Path.cwd()
    config, folder = dict(), None
    for folder in [start_path, *start_path.parents]:
        pyproject = folder / "pyproject.toml"
        if pyproject.exists():
            data = tomllib.loads(pyproject.read_text())
            config = data.get("tool", {}).get("protobunny", {})
            config["project-root"] = folder.name
            config["project-name"] = data["project"].get("name", PROJECT_NAME).replace("-", "_")
    return config, folder

import configparser
import dataclasses
import functools
import os
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

ENV_PREFIX = "PROTOBUNNY_"
INI_FILE = "protobunny.ini"

AvailableBackends = tp.Literal["rabbitmq", "python", "redis"]


@dataclasses.dataclass
class Config:
    messages_directory: str = "messages"
    messages_prefix: str = PREFIX_MESSAGES
    project_name: str = PROJECT_NAME
    project_root: str = "./"
    force_required_fields: bool = False
    generated_package_name: str = "codegen"
    mode: tp.Literal["sync", "async"] = "sync"
    backend: AvailableBackends = "rabbitmq"
    available_backends = ("rabbitmq", "python", "redis")

    def __post_init__(self) -> None:
        if self.mode not in ("sync", "async"):
            raise ValueError(f"Invalid mode: {self.mode}. Must be one of: sync, async")

    @property
    def use_async(self) -> bool:
        return self.mode == "async"

    @functools.cached_property
    def logger_prefix(self) -> str:
        return f"{self.messages_prefix}.#"


@functools.cache
def load_config() -> Config:
    """Load user config
    1. `[tool.protobunny]` from the nearest pyproject.toml.
    2. `protobunny.ini` in the current directory.
    3. Environment variables prefixed with PROTOBUNNY_.
    """
    config = get_config_from_pyproject()

    ini_config = get_config_from_ini()
    config.update(ini_config)

    env_config = get_config_from_env()
    config.update(env_config)

    if "generated-package-name" not in config:
        config["generated-package-name"] = (
            f"{config['project-name']}.codegen" if "project-name" in config else "codegen"
        )
    return Config(**{k.replace("-", "_"): v for k, v in config.items()})


def get_config_from_env() -> dict[str, tp.Any]:
    """Read config from environment variables prefixed with PROTOBUNNY_."""
    config = {}
    # Map of config keys to their expected types for casting
    type_hints = {
        "force_required_fields": bool,
    }
    for key, value in os.environ.items():
        if not key.startswith(ENV_PREFIX):
            continue
        config_key = key[len(ENV_PREFIX) :].lower().replace("_", "-")
        # Basic type casting for booleans
        if type_hints.get(config_key.replace("-", "_")) is bool:
            config[config_key] = value.lower() in ("true", "1", "yes", "on")
        else:
            config[config_key] = value
    return config


def _get_pyproject_path() -> Path | None:
    start_path = Path.cwd()
    for folder in [start_path, *start_path.parents]:
        pyproject = folder / "pyproject.toml"
        if pyproject.exists():
            return pyproject


def get_config_from_pyproject() -> dict[str, tp.Any]:
    config = dict()
    pyproject = _get_pyproject_path()
    if not pyproject:
        return config
    data = tomllib.loads(pyproject.read_text())
    config = data.get("tool", {}).get("protobunny", {})
    config["project-root"] = pyproject.parent.name
    config["project-name"] = data["project"].get("name", PROJECT_NAME).replace("-", "_")
    return config


def get_config_from_ini() -> dict[str, tp.Any]:
    """Read config from protobunny.ini in the current directory."""
    path = Path.cwd() / INI_FILE
    if not path.exists():
        return {}

    parser = configparser.ConfigParser()
    parser.read(path)
    if "protobunny" not in parser:
        return {}

    config = {}
    for key, value in parser["protobunny"].items():
        # Handle boolean casting for specific keys
        if key == "force-required-fields":
            config[key] = parser["protobunny"].getboolean(key)
        else:
            config[key] = value
    return config


# Get project version from pyproject.toml for releasing
def get_project_version() -> str:
    """Read version from pyproject.toml in the project root."""
    pyproject = _get_pyproject_path()
    if not pyproject:
        return "0.0.0-dev"
    data = tomllib.loads(pyproject.read_text())
    return data.get("project", {}).get("version", "0.0.0-dev")


VERSION = get_project_version()
configuration = load_config()

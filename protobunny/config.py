import functools

try:
    import tomllib
except ImportError:
    import tomli as tomllib

from pathlib import Path

PROJECT_NAME = "protobunny"
PACKAGE_NAME = "protobunny"
GENERATED_PACKAGE_NAME = "core"
ROOT_GENERATED_PACKAGE_NAME = f"{PACKAGE_NAME}.{GENERATED_PACKAGE_NAME}"
PREFIX_MESSAGES = "pb"
MESSAGES_DIRECTORY = "protobuf"
VERSION = "0.1.0"


class Config:
    def __init__(
        self,
        messages_directory: str = "messages",
        messages_prefix: str = PREFIX_MESSAGES,
        project_name: str = PROJECT_NAME,
        project_root: str = "./",
        force_required_fields: bool = False,
        generated_package_name: str = "codegen",
    ):
        self.messages_directory = messages_directory
        self.message_prefix = messages_prefix
        self.project_name = project_name
        self.project_root = project_root
        self.force_required_fields = force_required_fields
        self.generated_package_name = generated_package_name

    def __str__(self):
        return (
            f"Config(messages_directory={self.messages_directory!r}, "
            f"message_prefix={self.message_prefix!r}, "
            f"project_name={self.project_name!r}, "
            f"project_root={self.project_root!r}, "
            f"force_required_fields={self.force_required_fields!r}, "
            f"generated_package_name={self.generated_package_name!r})"
        )


@functools.cache
def load_config() -> Config:
    """Load user config `[tool.protobunny]` from the nearest pyproject.toml."""
    config, folder = get_config_from_pyproject()
    if "generated-package-name" not in config:
        config["generated-package-name"] = f"{config['project-name']}.codegen"
    return Config(**{k.replace("-", "_"): v for k, v in config.items()})


def get_config_from_pyproject() -> tuple[dict, Path] | None:
    start_path = Path.cwd()
    config, folder = None, None
    for folder in [start_path, *start_path.parents]:
        pyproject = folder / "pyproject.toml"
        if pyproject.exists():
            data = tomllib.loads(pyproject.read_text())
            config = data.get("tool", {}).get("protobunny", {})
            config["project-root"] = folder.name
            config["project-name"] = data["project"].get("name", PROJECT_NAME).replace("-", "_")
    return config, folder

"""
Protobunny tool

.. code-block:: shell

    protobunny generate

Generate betterproto classes and automatically includes the path to the custom proto types
and add the ProtoBunny mixin for the configured package (i.e. ``generated-package-name``).

See protobunny generate --help for more options.


.. code-block:: shell

    protobunny log

Start a logger in console. See protobunny log --help for more options.


Full configuration for pyproject.toml

.. code-block:: toml

    [tool.protobunny]
    messages-directory = 'messages'
    messages-prefix = 'acme'
    generated-package-name = 'mymessagelib.codegen'
    generated-package-root = "./"
    backend = "rabbitmq"
    force-required-fields = true
    mode = "async"
    log-redis-tasks = true

The following command will generate protobunny decorated betterproto classes in the `mymessagelib/codegen` directory:

.. code-block:: shell

    protobunny generate

"""

import asyncio
import functools
import glob
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterator

import click

config_error = None
try:
    import protobunny

    from . import __version__, get_backend
except (ModuleNotFoundError, ValueError, ImportError) as e:
    config_error = e

if config_error:
    sys.exit(config_error)


from .conf import load_config
from .logger import log_callback, start_logger, start_logger_sync


@click.group()
@click.version_option(version=__version__, prog_name="Protobunny", message="protobunny %(version)s")
def cli():
    """Protobunny tool: Generate betterproto classes and manage message logger."""
    pass


@cli.command()
@click.option("-I", "--proto_path", multiple=True, help="Protobuf search path.")
@click.option("--python_betterproto_out", help="Output directory for generated classes.")
@click.option("--source-dir", help="Root directory for generated classes., defaults to current dir")
@click.argument("rest", nargs=-1)
def generate(proto_path, python_betterproto_out, source_dir, rest) -> None:
    config = load_config()
    # betterproto_out it can be different from the configured package name so it can optionally be set on cli
    # (e.g. when generating messages for tests instead that main lib `mymessagelib.codegen`)
    betterproto_out = python_betterproto_out
    if not betterproto_out:
        betterproto_out = os.path.join(
            config.generated_package_root, config.generated_package_name.replace(".", os.sep)
        )
    proto_paths = list(proto_path) or [config.messages_directory]
    lib_proto_path = Path(__file__).parent / "protobuf"  # path to internal protobuf files
    proto_paths.append(str(lib_proto_path))
    proto_paths = [f"--proto_path={pp}" for pp in proto_paths]

    Path(betterproto_out).mkdir(parents=True, exist_ok=True)
    protobuffer_files: Iterator[str] = glob.iglob(
        f"./{config.messages_directory}/**/*.proto", recursive=True
    )
    cmd = [
        "python",
        "-m",
        "grpc_tools.protoc",
        f"--python_betterproto_out={betterproto_out}",
    ]
    cmd.extend(proto_paths)
    if rest:
        cmd.extend(rest)
    else:
        cmd.extend(protobuffer_files)

    # Generate py files with protoc for user protobuf messages
    result = subprocess.run(cmd)
    if result.returncode > 0:
        sys.exit(result.returncode)

    # Execute internal post compile script for user's betterproto generated classes
    post_compile_script = Path(__file__).parent.parent / "scripts" / "post_compile.py"
    source_dir = (
        source_dir or "./" if python_betterproto_out else config.generated_package_root or "./"
    )
    cmd = [
        "python",
        str(post_compile_script),
        f"--proto-pkg={python_betterproto_out or config.generated_package_name}",
        f"--source-dir={source_dir or config.generated_package_root or './'}",
    ]
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


@cli.command()
@click.option("-f", "--filter", "filter_pattern", help="Regex to filter messages.")
@click.option("-l", "--max-length", default=60, type=int, help="Cut off messages longer than this.")
@click.option("-p", "--prefix", help="Logger prefix (defaults to config prefix).")
def log(filter_pattern, max_length, prefix) -> None:
    config = load_config()
    filter_regex = re.compile(filter_pattern) if filter_pattern else None
    func = functools.partial(log_callback, max_length, filter_regex)
    if config.use_async:
        asyncio.run(start_logger(func, prefix))
    else:
        start_logger_sync(func, prefix)


def main():
    cli()


if __name__ == "__main__":
    main()

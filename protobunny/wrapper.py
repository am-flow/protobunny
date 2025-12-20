"""
Protobunny tool

.. code-block:: shell

    protobunny generate

Generate betterproto classes and automatically includes the path to the custom proto types
and add the ProtoBunny mixin for the configured package (i.e. ``generated-package-name``).


.. code-block:: shell

    protobunny log

Start a logger in console

Configuration for pyproject.toml

.. code-block:: toml

    [tool.protobunny]
    messages-directory = 'messages'
    messages-prefix = 'acme'
    generated-package-name = 'mymessagelib.codegen'

The following command generates betterproto python classes in the `mymessagelib.codegen` directory:

.. code-block:: shell

    protobunny generate

"""


import argparse
import asyncio
import functools
import glob
import os
import re
import subprocess
import sys
from pathlib import Path

from .config import load_config
from .logger import log_callback, start_logger, start_logger_sync


def generate(parsed_args: argparse.Namespace, rest: list[str]) -> None:
    config = load_config()
    # betterproto_out it can be different from the configured package name so it can optionally be set on cli
    # (e.g. when generating messages for tests instead that main lib `mymessagelib.codegen`)
    betterproto_out = parsed_args.python_betterproto_out or config.generated_package_name.replace(
        ".", os.sep
    )
    proto_paths = parsed_args.proto_path or [config.messages_directory]
    lib_proto_path = Path(__file__).parent / "protobuf"  # path to internal protobuf files
    proto_paths.append(str(lib_proto_path))
    proto_paths = [f"--proto_path={pp}" for pp in proto_paths]

    generated_package_name = betterproto_out.replace(os.sep, ".")
    Path(betterproto_out).mkdir(parents=True, exist_ok=True)
    protofiles = glob.iglob(f"./{config.messages_directory}/**/*.proto", recursive=True)
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
        cmd.extend(protofiles)
    # Generate py files with protoc for user protobuf messages
    result = subprocess.run(cmd)
    if result.returncode > 0:
        sys.exit(result.returncode)
    # Execute internal post compile script for user's betterproto generated classes
    post_compile_path = Path(__file__).parent.parent / "scripts" / "post_compile.py"
    cmd = ["python", str(post_compile_path), f"--proto-pkg={generated_package_name}"]
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


def start_logger_service(parsed_args: argparse.Namespace) -> None:
    filter_regex = re.compile(parsed_args.filter) if parsed_args.filter else None
    prefix = parsed_args.prefix
    func = functools.partial(log_callback, parsed_args.max_length, filter_regex)
    if parsed_args.mode == "async":
        asyncio.run(start_logger(func, prefix))
    else:
        start_logger_sync(func, prefix)


def parse_args() -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(description="Protobunny tool")
    parser.add_argument("action", type=str, help="Action to perform", choices=["generate", "log"])
    gen_group = parser.add_argument_group(
        title="Compiling protobuf", description="Args for `generate` command"
    )
    log_group = parser.add_argument_group(
        title="Logger Service", description="Args for `log` command"
    )

    gen_group.add_argument("-I", "--proto_path", type=str, required=False, nargs="*")
    gen_group.add_argument("--python_betterproto_out", type=str, required=False)
    # Logger group
    log_group.add_argument(
        "-f", "--filter", type=str, help="filter messages matching this regex", required=False
    )
    log_group.add_argument(
        "-l", "--max-length", type=int, default=60, help="cut off messages longer than this"
    )
    log_group.add_argument(
        "-m", "--mode", type=str, default="async", help="Set async or sync mode."
    )
    log_group.add_argument(
        "-p",
        "--prefix",
        type=str,
        required=False,
        help="Set the prefix for the logger if different from the configured messages-prefix",
    )
    return parser.parse_known_args()


def main() -> None:
    args, unparsed = parse_args()
    if args.action == "generate":
        generate(args, unparsed)
    elif args.action == "log":
        start_logger_service(args)


if __name__ == "__main__":
    main()

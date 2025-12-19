"""grpc_tools wrapper

Automatically includes the path to the custom proto types and generates the python classes for the configured package (i.e. generated-package-name)

Usage
Configuration for pyproject.toml

[tool.protobunny]
messages-directory = "messages"
messages-prefix = "acme"
generated-package-name = "mymessagelib.codegen"  # or even "mymessagelib"

The following command generates betterproto python classes in the `mymessagelib.codegen` directory:
protobunny -I messages messages/**/*.proto messages/*.proto
where `messages` is a directory containing the protobuf files
"""

import os
import subprocess
import sys
from argparse import ArgumentParser
from pathlib import Path

from .config import load_config


def main():
    config = load_config()
    arg_parser = ArgumentParser()
    arg_parser.add_argument("--python_betterproto_out", type=str, required=False)
    args, argv = arg_parser.parse_known_args()
    # it can be different from the configured package name so it can optionally be set on cli
    # (e.g. when generating messages for tests instead that main lib `mymessagelib.codegen`)
    betterproto_out = args.python_betterproto_out or config.generated_package_name.replace(
        ".", os.sep
    )
    generated_package_name = betterproto_out.replace(os.sep, ".")
    Path(betterproto_out).mkdir(parents=True, exist_ok=True)
    lib_proto_path = Path(__file__).parent / "protobuf"  # path to internal protobuf files
    # TODO read -I messages messages/**/*.proto messages/*.proto from pyproject.toml messages-directory configuration
    cmd = [
        "python",
        "-m",
        "grpc_tools.protoc",
        *argv,
        f"--python_betterproto_out={betterproto_out}",
        f"--proto_path={lib_proto_path}",
    ]
    # Generate py files with protoc for user protobuf messages
    result = subprocess.run(cmd)
    if result.returncode > 0:
        sys.exit(result.returncode)
    # Execute post compile script for user betterproto generated classes
    post_compile_path = Path(__file__).parent.parent / "scripts" / "post_compile.py"
    cmd = ["python", post_compile_path, f"--proto-pkg={generated_package_name}"]
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()

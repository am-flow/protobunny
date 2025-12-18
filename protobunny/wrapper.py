"""grpc_tools wrapper
Automatically includes the path to the custom proto types.

Usage

The following command generates betterproto python classes in the `mymessagelib.codegen` directory:
protobunny -I messages --python_betterproto_out=mymessagelib.codegen messages/**/*.proto messages/*.proto
where `messages` is a directory containing the protobuf files.

Configuration for pyproject.toml

[tool.protobunny]
messages-directory = "messages"
messages-prefix = "acme"
generated-package-name = "mymessagelib.codegen" # or "mymessagelib" if you don't need to reuse a module
"""
import os
import subprocess
import sys
from pathlib import Path

from .config import load_config


def main():
    config = load_config()
    from argparse import ArgumentParser

    arg_parser = ArgumentParser()
    arg_parser.add_argument("--python_betterproto_out", type=str)
    args, _ = arg_parser.parse_known_args()
    # it can be different from the configured package name
    # (e.g. generating messages for tests instead that main lib `mymessagelib.codegen`)
    generated_package_name = args.python_betterproto_out.replace(os.sep, ".")
    Path(config.generated_package_name.replace(".", os.sep)).mkdir(parents=True, exist_ok=True)
    lib_proto_path = Path(__file__).parent / "protobuf"  # path to internal protobuf files
    cmd = ["python", "-m", "grpc_tools.protoc", *sys.argv[1:], f"--proto_path={lib_proto_path}"]
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

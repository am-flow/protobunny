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
    Path(config.generated_package_name.replace(".", os.sep)).mkdir(parents=True, exist_ok=True)
    lib_proto_path = Path(__file__).parent / "protobuf"  # path to internal protobuf files
    cmd = ["python", "-m", "grpc_tools.protoc", f"--proto_path={lib_proto_path}", *sys.argv[1:]]
    # Generate py files with protoc for user protobuf messages
    result = subprocess.run(cmd)
    if result.returncode > 0:
        sys.exit(result.returncode)
    # Post compile step

    # Execute post compile script for user betterproto generated classes
    post_compile_path = Path(__file__).parent.parent / "scripts" / "post_compile.py"
    cmd = ["python", post_compile_path, f"--proto-pkg={config.generated_package_name}"]
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()

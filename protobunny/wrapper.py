"""grpc_tools wrapper
Automatically includes the path to the custom proto types.

Usage

Suppose you have a directory called messages with your proto files.
The following command generates betterproto python classes in the codegen directory:
protobunny -I messages --python_betterproto_out=codegen messages/**/*.proto messages/*.proto
"""

import subprocess
import sys
from pathlib import Path


def main():
    lib_proto_path = Path(__file__).parent / "protobuf"
    cmd = ["python", "-m", "grpc_tools.protoc", f"--proto_path={lib_proto_path}", *sys.argv[1:]]

    # Execute protoc
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()

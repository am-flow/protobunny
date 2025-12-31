import glob
import logging
import os
import subprocess
import sys
from pathlib import Path

import setuptools
from setuptools import setup
from setuptools.command.install import install

# Add the package directory to sys.path to allow importing config during setup
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "protobunny")))
import typing as tp

from config import (
    GENERATED_PACKAGE_NAME,
    PACKAGE_NAME,
    PROJECT_NAME,
    ROOT_GENERATED_PACKAGE_NAME,
    VERSION,
)

log = logging.getLogger(__name__)


def generate_python_classes(install_dir: str) -> None:
    # Compile internal protobuf files
    log.info("Generating betterproto classes for %s", PACKAGE_NAME)
    # proto_dir = str(os.path.join(install_dir, PACKAGE_NAME, MESSAGES_DIRECTORY))
    proto_dir = str(os.path.join(install_dir, PACKAGE_NAME, "protobuf"))
    proto_files = glob.glob(proto_dir + "/**/*.proto", recursive=True)
    target_dir = str(os.path.join(install_dir, PACKAGE_NAME, GENERATED_PACKAGE_NAME))
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    protoc_command = "python -m grpc_tools.protoc".split()
    protoc_args = f"-I {proto_dir} --python_betterproto_out={target_dir}".split() + proto_files
    log.info(protoc_args)
    try:
        subprocess.check_output(protoc_command + protoc_args)
    except subprocess.CalledProcessError as e:
        log.error(f"Failed to generate betterproto classes for {PACKAGE_NAME}")
        log.error(e.output)
        raise e


def post_compile(install_dir: str) -> None:
    # Post-compile internal betterproto python classes
    post_compile_command = f"python {install_dir}/scripts/post_compile.py".split()
    post_compile_args = (
        f"--proto-pkg={ROOT_GENERATED_PACKAGE_NAME} --source-dir={install_dir}".split()
    )
    log.info(post_compile_command + post_compile_args)
    try:
        subprocess.check_output(post_compile_command + post_compile_args)
    except subprocess.CalledProcessError as e:
        log.error(f"Failed to post compile betterproto classes for {PACKAGE_NAME}")
        log.error(e.output)
        raise e


class GenerateProtoCommand(install):  # type: ignore
    description = "Generate protobuf files"
    user_options: tp.ClassVar[list[tuple[str, str | None, str]]] = []

    def run(self) -> None:
        install_dir: str = os.path.abspath(self.build_lib)
        log.info(f"Installation directory: {install_dir}")
        generate_python_classes(install_dir)
        post_compile(install_dir)
        install.run(self)


# Specify the package data
package_data = {
    "protobunny": [
        "protobunny/protobuf/protobunny/*.proto",
        f"{PACKAGE_NAME}/__init__.py.j2",
        "scripts/*.py",
    ],
}
packages = setuptools.find_namespace_packages() + [
    ROOT_GENERATED_PACKAGE_NAME,
]
project_urls = {
    "repository": "https://github.com/am-flow/protobunny",
    "issues": "https://github.com/am-flow/protobunny/issues",
    "homepage": "https://am-flow.github.io/protobunny",
}
setup(
    name=PROJECT_NAME,
    version=VERSION,
    classifiers=["Development Status :: 3 - Alpha"],
    author="Domenico Nappo, Sander Koelstra, Sem Mulder",
    include_package_data=True,
    package_data=package_data,
    packages=packages,
    cmdclass={
        "install": GenerateProtoCommand,
    },
    python_requires=">=3.10,<3.14",
    description="Protobuf messages and python mqtt messaging toolkit",
    entry_points={
        "console_scripts": [
            "protobunny=protobunny.wrapper:main",
        ],
    },
)

"""A script that add imports for submodules in betterproto messages' packages.

It allows code assistance in IDEs.
>>> import mylib as ml
>>> msg = ml.machine.control.MoveUp()
Note: It must be executed after code generation/post install script.
- It's included in make command `compile` and in setup.py for lib installations.
- It's part of the protobunny compile command
"""

import argparse
import ast
import black
import logging
import os
import re
from argparse import Namespace
from collections import defaultdict
from pathlib import Path
from typing import Any, TextIO

from jinja2 import Environment, FileSystemLoader

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("protobunny")


def get_package_path(package_name: str, base_dir: str) -> str:
    """Give the package path given a package name and base directory, return the package path.
    Raise ImportError if package is not found.

    Args:
        package_name: The package name.
        base_dir: The base directory.
    Returns:
        The package path.
    Raises:
        ImportError: If package is not found.
    """
    package_path = os.path.join(base_dir, package_name.replace(".", os.sep))
    if os.path.isdir(package_path) and os.path.exists(os.path.join(package_path, "__init__.py")):
        log.info("Found package %s", package_path)
        return package_path
    raise ImportError(f"Package {package_name} not found in {base_dir}")


def get_modules_with_subpackages(package_name: str, base_dir: str = "./") -> dict[str, list[str]]:
    """Walks the directory corresponding to a package (located in base_dir) for subpackages,
       and returns a dictionary mapping package names to a list of their subpackage names,
       useful to determine the imports to add later.

    Args:
        package_name: The root package name for the generated python code e.g. 'mylib.codegen'
        base_dir: The base directory e.g. '/home/john/projects/myproject'

    Returns:
        A dictionary mapping package names to a list of their subpackage names.
    """
    res = defaultdict(list)
    try:
        package_path = get_package_path(package_name, base_dir)
    except ImportError:
        # Exit from recursion
        return res

    # Iterate through the items in the package directory.
    for item in os.listdir(package_path):
        item_path = os.path.join(package_path, item)
        # Check if the item is a directory and has an __init__.py file.
        if os.path.isdir(item_path) and os.path.exists(os.path.join(item_path, "__init__.py")):
            res[package_name].append(item)
            full_name = f"{package_name}.{item}"
            # Recursively update the dictionary with subpackages.
            res.update(get_modules_with_subpackages(full_name, base_dir))
    return res


def post_compile_step_1(pkg: str, subpackages: list[str], source_dir: str = "./") -> None:
    init_path = Path(source_dir) / pkg.replace(".", os.sep) / "__init__.py"
    with init_path.open("a") as init_file:
        # Append subpackages imports to the __init__.py file
        write_header(init_file, breaklines=2)
        for subpackage in subpackages:
            init_file.write(f"from . import {subpackage.split('.')[-1]}  # noqa\n")


def post_compile_step_2(pkg: str, source_dir: str = "./") -> None:
    init_path = Path(source_dir) / pkg.replace(".", os.sep) / "__init__.py"
    source = init_path.read_text()
    source = ensure_message_mixin(source)
    source = ensure_dict_type(source)
    source = black.format_str(
        src_contents=source,
        mode=black.Mode(),
    )
    init_path.write_text(source)


def write_main_init(main_imports: list[str], main_package: str, source_dir: str = "./") -> None:
    """
    Imports main packages into the `__init__.py` file at the specified source directory. This function
    handles rendering the `__init__.py` file using a Jinja2 template if available, formats the content
    using the Black code formatter, and writes the final output to the file.

    Args:
        main_imports (list[str]): A list of main imports to be included in the `__init__.py` file.
        main_package (str): The full dotted path of the main package associated with the imports.
        source_dir (str): The directory where the top-level package is located. Defaults to `"./"`.
    """
    # Import main packages in __init__.py
    # find the root package for the messaging library
    packages = main_package.split(".")
    top_level_package, generated_package_name = (packages[-1], "") if len(packages) == 1 else (packages[0], packages[1])
    path = Path(source_dir) / top_level_package
    if not path.exists():
        return
    init_path = path / "__init__.py"
    # look for a jinja template in the main directory
    if (path / "__init__.py.j2").exists():
        write_from_template(generated_package_name, init_path, main_imports, path)
    else:
        with open(init_path, mode="w+") as fh:
            write_header(fh)
            for subpackage in main_imports:
                fh.write(f"from .{generated_package_name} import {subpackage.split('.')[-1]}  # noqa\n")


def write_from_template(generated_package_name: str, init_path: Path, main_imports: list[str], path: Path):
    environment = Environment(loader=FileSystemLoader(path))
    template = environment.get_template("__init__.py.j2")
    # Render the templates with the main imports
    main_init_source = template.render(
        generated_package_name=generated_package_name, main_imports=main_imports
    )
    main_init_source = black.format_str(
        src_contents=main_init_source,
        mode=black.Mode(),
    )
    with open(init_path, mode="w+") as fh:
        fh.write(main_init_source)


def write_header(fh: TextIO, breaklines: int = 0):
    fh.write("\n" * breaklines)
    fh.write("#" * 55)
    fh.write("\n# Dynamically added by protobunny post_compile.py\n\n")


def replace_typing_import(source: str, new_import: str) -> str:
    pattern = re.compile(
        r"^(\s*)from\s+typing\s+import\s*"
        r"(?:"
        r"([^\(].*?)$"  # single-line: from typing import X, Y
        r"|"
        r"\(\s*\n(.+?)\n\s*\)"  # multiline: from typing import (\n    X,\n    Y\n)
        r")",
        re.MULTILINE | re.DOTALL,
    )

    def repl(match):
        indent = match.group(1)  # preserve original indentation
        return indent + new_import.replace("\n", "\n" + indent)

    return pattern.sub(repl, source, count=1)


def ensure_dict_type(source: str) -> str:
    """Add Dict type annotation to JsonContent fields.

    Args:
        source: the python source file intended for modification.
    """
    # Pattern 1: Optional["_commons__.JsonContent"] or
    # Matches: options: Optional["_commons__.JsonContent"] = betterproto.message_field(
    pattern1 = re.compile(
        r'(\w+):\s*Optional\["(_*commons__\.JsonContent)"\]\s*=\s*betterproto\.message_field\(',
        re.MULTILINE,
    )
    # Pattern 2: _commons__.JsonContent (without Optional)
    # Matches: options: _commons__.JsonContent = betterproto.message_field(
    pattern2 = re.compile(
        r"(\w+):\s*\"(_*commons__\.JsonContent)\"\s*=\s*betterproto\.message_field\(",
        re.MULTILINE,
    )

    def replace1(match):
        field_name = match.group(1)
        type_name = match.group(2)
        return f'{field_name}: Optional["{type_name} | dict"] = betterproto.message_field('

    def replace2(match):
        field_name = match.group(1)
        type_name = match.group(2)
        return f"{field_name}: {type_name} | dict = betterproto.message_field("
    new_source = pattern1.sub(replace1, source)
    new_source = pattern2.sub(replace2, new_source)
    return new_source


def ensure_message_mixin(source: str) -> str:
    new_source = source.replace("(betterproto.Message):", "(base.MessageMixin, betterproto.Message):")
    new_source = f"from protobunny import base as base\n{new_source}"
    return new_source


def main() -> None:
    args = cli_args()
    main_package = args.proto_pkg
    source_dir = args.source_dir
    packages = get_modules_with_subpackages(main_package, source_dir)
    all_packages = get_all_submodules(packages)
    log.info("Packages found to have submodules: %s", packages)
    log.info("All packages: %s", all_packages)
    main_imports = packages.pop(main_package)
    # Write main protobunny init file using jinja template,
    # to use the full namespace syntax
    # In [1]: import mylib as ml
    # In [2]: ml.main.MyMessage
    # Out[3]: mylib.codegen.main.MyMessage
    write_main_init(main_imports, main_package, source_dir)
    # For each of the main subpackages, check if it has subpackages.
    # If so, we append imports to the __init__.py file
    for pkg in packages:
        post_compile_step_1(pkg, packages[pkg], source_dir)
    # For all subpackages
    # 1 - add MixIn to all Message classes
    # 2 - ensure that JsonContent fields are typed as Union[JsonContent, Dict]
    for pkg in all_packages:
        log.info("Type hinting package %s", pkg)
        post_compile_step_2(pkg, source_dir)


def get_all_submodules(packages: dict[str, list[str]]) -> set[Any]:
    all_packages = set()
    for pkg_name, subpackages in packages.items():
        for subpackage in subpackages:
            full_name = f"{pkg_name}.{subpackage}"
            all_packages.add(full_name)
    return all_packages


def cli_args() -> Namespace:
    parser = argparse.ArgumentParser(
        description="""
         protobunny post compile:
           add imports for subpackages
           add dict type annotation to JsonContent
        """
    )
    parser.add_argument(
        "-s",
        "--source-dir",
        type=str,
        help="Base source directory",
        required=False,
        default="./",
    )
    parser.add_argument(
        "-p",
        "--proto-pkg",
        type=str,
        required=False,
        default="codegen",
        help="betterproto code generated root package (i.e. codegen)",
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()

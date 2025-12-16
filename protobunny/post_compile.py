"""A script that add imports for submodules in betterproto messages' packages.

It allows code assistance in IDEs.
>>> import protobunny as pb
>>> msg = pb.quality.machine.mmfcontrol.MoveMmf()
Note: It must be executed after code generation/post install script.
It's included in make command `compile` and in setup.py for lib installations.

It can't use pkgutil.walk_packages and importlib but os.path utilities and checking for __init__.py files
to allow the script to be executed at installation time.
"""

import argparse
import ast
import logging
import os
import re
from argparse import Namespace
from collections import defaultdict
from pathlib import Path

import black
from config import PACKAGE_NAME
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
    and returns a dictionary mapping package names to a list of their subpackage names, useful to determine the imports.

    Args:
        package_name: The package name.
        base_dir: The base directory.

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
        init_file.write(f"\n\n{'#' * 55}")
        init_file.write("\n# Dynamically added by post_compile.py\n\n")
        for subpackage in subpackages:
            init_file.write(f"from . import {subpackage.split('.')[-1]}  # noqa\n")


def post_compile_step_2(pkg: str, source_dir: str = "./") -> None:
    init_path = Path(source_dir) / pkg.replace(".", os.sep) / "__init__.py"
    ensure_dict_type(init_path)


def write_main_init(main_imports: list[str], main_package: str, source_dir: str = "./") -> None:
    # Import main packages in __init__.py, using the jinja template
    generated_init_path = os.path.join(source_dir, PACKAGE_NAME, "__init__.py")
    environment = Environment(loader=FileSystemLoader(os.path.join(source_dir, PACKAGE_NAME)))
    template = environment.get_template("__init__.py.j2")
    generated_package_name = main_package.split(".")[-1]  # e.g codegen
    # Render the templates with the main imports
    main_init_file = template.render(
        generated_package_name=generated_package_name, main_imports=main_imports
    )
    main_init_file = black.format_str(
        src_contents=main_init_file,
        mode=black.Mode(),
    )
    with open(generated_init_path, mode="w") as fh:
        fh.write(main_init_file)


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


def ensure_imports(source: str) -> str:
    # Check if there's already a typing import
    tree = ast.parse(source)
    imported = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "typing":
            for alias in node.names:
                imported.add(alias.name)
    # typing_import_pattern = re.compile(r"^from typing import (.+)$", re.MULTILINE)
    # match = typing_import_pattern.search(source)
    if not imported:
        # No typing import found, add one at the top
        first_code_pattern = re.compile(r"^(import |from |class |def )", re.MULTILINE)
        match = first_code_pattern.search(source)
        insert_pos = match.start()
        new_import = "from typing import Dict, Union\n\n"
        source = source[:insert_pos] + new_import + source[insert_pos:]
    else:
        # There's already a typing import, check if Union and Dict are there
        # imports = match.group(1)
        # imports_list = [imp.strip().strip("(").strip(")") for imp in imports.split(",")]

        needs_union = "Union" not in imported
        needs_dict = "Dict" not in imported

        if needs_union or needs_dict:
            # Add missing imports
            if needs_union:
                imported.add("Union")
            if needs_dict:
                imported.add("Dict")
        imported = list(imported)
        imported.sort()
        new_import = f"from typing import ({', '.join(imported)})"
        source = replace_typing_import(source, new_import)
    return source


def ensure_dict_type(module_path: Path) -> None:
    source = module_path.read_text()
    # Pattern 1: Optional["_commons__.JsonContent"] or
    # Matches: options: Optional["_commons__.JsonContent"] = betterproto.message_field(
    pattern1 = re.compile(
        r'(\w+):\s*Optional\["(_*commons__\.JsonContent)"\]\s*=\s*betterproto\.message_field\(',
        re.MULTILINE,
    )
    # Pattern 2: _commons__.JsonContent (without Optional)
    # Matches: options: _commons__.JsonContent = betterproto.message_field(
    # But NOT if it's already wrapped in Optional or Union
    pattern2 = re.compile(
        r"(\w+):\s*(?!Optional|Union)(_*commons__\.JsonContent)\s*=\s*betterproto\.message_field\(",
        re.MULTILINE,
    )
    hits = 0

    def replace1(match):
        nonlocal hits
        hits += 1
        field_name = match.group(1)
        type_name = match.group(2)
        return f'{field_name}: Optional[Union["{type_name}", Dict]] = betterproto.message_field('

    def replace2(match):
        nonlocal hits
        hits += 1
        field_name = match.group(1)
        type_name = match.group(2)
        return f"{field_name}: Union[{type_name}, Dict] = betterproto.message_field("

    new_source = pattern1.sub(replace1, source)
    new_source = pattern2.sub(replace2, new_source)
    if hits:
        # Ensure Union and Dict are imported from typing
        new_source = ensure_imports(new_source)
    module_path.write_text(new_source)


def main() -> None:
    args = cli_args()
    main_package = args.proto_pkg
    source_dir = args.source_dir
    packages = get_modules_with_subpackages(main_package, source_dir)
    all_packages = set()
    for pkg_name, subpackages in packages.items():
        for subpackage in subpackages:
            full_name = f"{pkg_name}.{subpackage}"
            all_packages.add(full_name)

    # all_packages = set([pkg for pkg_list in packages.values() for pkg in pkg_list])
    log.info("Packages found to have submodules: %s", packages)
    log.info("All packages: %s", all_packages)
    main_imports = packages.pop(main_package)

    # Write main init file using jinja template, to import current messages main packages
    write_main_init(main_imports, main_package, source_dir)
    # For each of the main subpackages, check if it has subpackages.
    # If so, we append imports to the __init__.py file
    for pkg in packages:
        post_compile_step_1(pkg, packages[pkg], source_dir)
    # For all subpackages, ensure that JsonContent fields are typed as Optional[Union[JsonContent, Dict]]
    for pkg in all_packages:
        log.info("Type hinting package %s", pkg)
        post_compile_step_2(pkg, source_dir)


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
        "--source_dir",
        type=str,
        help="Base source directory",
        required=False,
        default="./",
    )
    parser.add_argument(
        "-p",
        "--proto_pkg",
        type=str,
        required=False,
        default="codegen",
        help="betterproto code generated root package (i.e. codegen)",
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()

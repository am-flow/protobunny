# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

from protobunny import PACKAGE_NAME, __version__

project = PACKAGE_NAME
copyright = "2026, AM-Flow"
author = "Domenico Nappo, Sander Koelstra, Sem Mulder"
release = __version__


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

import os
import sys

sys.path.insert(0, os.path.abspath("../../"))  # Point to project root

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",  # Supports Google/NumPy style docstrings
    "sphinx.ext.viewcode",
]


templates_path = ["_templates"]
exclude_patterns: list[str] = []
html_favicon = "_images/favicon.svg"
html_logo = "_images/logo.png"
html_theme_options = {
    "announcement": "<em>Important</em> protobunny is in alpha!",
}
html_theme = "furo"

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "protobunny"
copyright = "2026, AM-Flow"
author = "Domenico Nappo, Sander Koelstra, Sem Mulder"
release = "0.1.1"

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

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"

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
html_theme = "furo"
html_css_files = [
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/fontawesome.min.css",
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/solid.min.css",
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/brands.min.css",
]
html_title = f"v{release}"
html_theme_options = {
    "announcement": "<em>Important</em> protobunny is in alpha!",
    "light_css_variables": {
        "color-brand-primary": "#f8ba26",
        "color-brand-content": "#f8ba26",
    },
    "dark_css_variables": {
        "color-brand-primary": "#f8ba26",
        "color-brand-content": "#f8ba26",
    },
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/am-flow/protobunny",
            "html": "",
            "class": "fa-brands fa-solid fa-github fa-2x",
        },
    ],
}

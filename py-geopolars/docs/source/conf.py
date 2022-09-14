# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
from datetime import date

# add geopolars directory
sys.path.insert(0, os.path.abspath("../.."))

# -- Project information -----------------------------------------------------

project = "GeoPolars"
author = "Kyle Barron"
copyright = f"{date.today().year}, {author}"

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "numpydoc",  # numpy docstrings
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.coverage",
    "sphinx.ext.doctest",
    "sphinx.ext.extlinks",
    "sphinx.ext.ifconfig",
    "sphinx.ext.intersphinx",
    # "sphinx.ext.linkcode",
    "sphinx.ext.mathjax",
    "sphinx.ext.todo",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# Config to point to Polars docs
# https://www.sphinx-doc.org/en/master/usage/extensions/intersphinx.html
intersphinx_mapping = {
    "polars": ("https://pola-rs.github.io/polars/py-polars/html/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "pyarrow": ("https://arrow.apache.org/docs/", None),
    "python": ("https://docs.python.org/3", None),
}

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
# html_theme = 'alabaster'
html_theme = "pydata_sphinx_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_css_files = ["css/custom.css"]  # relative to html_static_path

html_logo = "../img/geopolars_logo.png"
autosummary_generate = True

html_theme_options = {
    "external_links": [
        # {
        #     "name": "User Guide",
        #     "url": "https://pola-rs.github.io/polars-book/user-guide/index.html",
        # },
    ],
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/geopolars/geopolars",
            "icon": "fab fa-github-square",
        },
    ],
}


# TODO: add linkcode_resolve
# https://github.com/pola-rs/polars/blob/527ae86f7996bea60b7ae1bb4be48229d515e115/py-polars/docs/source/conf.py#L104

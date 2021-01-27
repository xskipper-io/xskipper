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
python_docs_root_dir = os.path.dirname(os.path.realpath(__file__))
docs_dir = os.path.dirname(python_docs_root_dir)
root_dir = os.path.dirname(docs_dir)

# mock the py4j and pyspark modules since we don't document it
import mock

MOCK_MODULES = ['py4j', 'py4j.java_collections', 'pyspark.sql.dataframe']
for mod_name in MOCK_MODULES:
    sys.modules[mod_name] = mock.Mock()

sys.path.insert(0, os.path.abspath(os.path.join(root_dir, 'python')))

# -- Project information -----------------------------------------------------

project = 'xskipper-core'
copyright = '2021, Xskipper Committers'
author = 'Xskipper Committers'

# The full version, including alpha/beta/rc tags
version_file_path = os.path.join(root_dir, 'version.sbt')
release = '0.0.0'
for line in open(version_file_path):
    if "ThisBuild" in line:
        release = line.split("\"")[1]


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ['sphinx.ext.autodoc']

# Add any paths that contain templates here, relative to this directory.
templates_path = []

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'classic'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = []

# Fix for doc generation to work on older version of sphinx as well.
master_doc = 'index'
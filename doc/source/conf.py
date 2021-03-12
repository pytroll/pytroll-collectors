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
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------
from datetime import datetime

project = 'pytroll-collectors'
project_copyright = f"2014-{datetime.now():%Y}, Pytroll developers"
author = 'Panu Lahtinen, Martin Raspaud, Trygve Aspenes, Adam Dybbroe'

# The full version, including alpha/beta/rc tags
release = 'v0.10.0'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ['sphinx.ext.autodoc', 'sphinx.ext.intersphinx', 'sphinx.ext.todo', 'sphinx.ext.coverage',
              'sphinx.ext.doctest', 'sphinx.ext.napoleon', 'sphinx.ext.autosummary',
              'sphinx.ext.viewcode']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

intersphinx_mapping = {
    'dask': ('https://docs.dask.org/en/latest', None),
    'geoviews': ('http://geoviews.org', None),
    'jobqueue': ('https://jobqueue.dask.org/en/latest', None),
    'numpy': ('https://docs.scipy.org/doc/numpy', None),
    'pydecorate': ('https://pydecorate.readthedocs.io/en/stable', None),
    'pyorbital': ('https://pyorbital.readthedocs.io/en/stable', None),
    'pyproj': ('https://pyproj4.github.io/pyproj/dev', None),
    'pyresample': ('https://pyresample.readthedocs.io/en/stable', None),
    'pytest': ('https://docs.pytest.org/en/stable/', None),
    'python': ('https://docs.python.org/3', None),
    'scipy': ('https://docs.scipy.org/doc/scipy/reference', None),
    'trollimage': ('https://trollimage.readthedocs.io/en/stable', None),
    'trollsift': ('https://trollsift.readthedocs.io/en/stable', None),
    'xarray': ('https://xarray.pydata.org/en/stable', None),
    'rasterio': ('https://rasterio.readthedocs.io/en/latest', None),
    "satpy": ("https://satpy.readthedocs.io/en/latest", None),
    "posttroll": ("https://posttroll.readthedocs.io/en/latest", None),
    "pytroll-schedule": ("https://pytroll-schedule.readthedocs.io/en/latest", None),
    "trollflow2": ("https://trollflow2.readthedocs.io/en/latest", None),
}

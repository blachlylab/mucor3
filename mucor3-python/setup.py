# coding: utf-8
from __future__ import absolute_import, division, print_function

__author__ = "Charles Gregory"
__email__ = "charles.gregory@osumc.edu"


import sys

try:
    from setuptools import setup
except ImportError:
    print("Could not load setuptools. Please install the setuptools package.", file=sys.stderr)

__version__=0.3


setup(
    name="mucor3",
    version=__version__,
    author="Charles Gregory",
    author_email="charles.gregory@osumc.edu",
    url="https://github.com/blachlylab/mucor3",
    packages=["mucor", "mucor_util"],
    package_dir={'mucor_util':'utils'},
    include_package_data=True,
    zip_safe=False,
    install_requires=["pandas"],
    entry_points={
        "console_scripts": [
            "mucor3 = mucor.mucor:main",
            "sample_indexer = mucor_util.sample_indexer:main"
            ]},
)

#!/usr/bin/env python

"""The setup script."""

from typing import List

from setuptools import find_packages, setup

with open("README.rst") as readme_file:
    readme = readme_file.read()

# https://packaging.python.org/discussions/install-requires-vs-requirements/#install-requires
install_requires: List[str] = ["matplotlib", "numpy", "pandas" "tqdm", "python-dotenv"]
test_requires = ["pytest>=3"]

setup(
    author="e3sm_metrics developers",
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache-2.0 License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    description=(
        "A Python package that parses ESGF Apache logs for E3SM data download metrics "
        "on a quarterly basis.",
    ),
    install_requires=install_requires,
    license="Apache-2.0",
    long_description=readme,
    include_package_data=True,
    keywords="esgf_metrics",
    name="esgf_metrics",
    packages=find_packages(include=["esgf_metrics", "esgf_metrics.*"]),
    test_suite="tests",
    tests_require=test_requires,
    url="https://github.com/tomvothecoder/esgf_metrics",
    version="0.1.0",
    zip_safe=False,
)

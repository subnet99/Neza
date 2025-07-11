import re
import os
import codecs
import pathlib
from os import path
from io import open
from setuptools import setup, find_packages


def read_requirements(path):
    """Read and process requirements from file"""
    with open(path, "r") as f:
        requirements = f.read().splitlines()
        processed_requirements = []

        for req in requirements:
            if req.startswith("git+") or "@" in req:
                pkg_name = re.search(r"(#egg=)([\w\-_]+)", req)
                if pkg_name:
                    processed_requirements.append(pkg_name.group(2))
            else:
                processed_requirements.append(req)
        return processed_requirements


here = path.abspath(path.dirname(__file__))
requirements = read_requirements("requirements.txt")

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

with codecs.open(os.path.join(here, "neza/__init__.py"), encoding="utf-8") as init_file:
    version_match = re.search(
        r"^__version__ = ['\"]([^'\"]*)['\"]", init_file.read(), re.M
    )
    version_string = version_match.group(1)

setup(
    name="bittensor-subnet-99",
    version=version_string,
    description="Neza - Video Generation Subnet for Bittensor",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/subnet99",
    author="Neza Team",
    packages=find_packages(),
    include_package_data=True,
    author_email="",
    license="MIT",
    python_requires=">=3.8",
    install_requires=requirements,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Scientific/Engineering :: Image Processing",
        "Topic :: Multimedia :: Video",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)

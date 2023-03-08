#!/usr/bin/env python3
# Copyright (C) 2019-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from io import open
from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


def parse_requirements(name=None):
    if name:
        reqf = "requirements-%s.txt" % name
    else:
        reqf = "requirements.txt"

    requirements = []
    if not path.exists(reqf):
        return requirements

    with open(reqf) as f:
        for line in f.readlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            requirements.append(line)
    return requirements


GITLAB_URL = "https://gitlab.softwareheritage.org/swh/devel/swh-objstorage-replayer"
setup(
    name="swh.objstorage.replayer",
    description="Software Heritage content replayer",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    author="Software Heritage developers",
    author_email="swh-devel@inria.fr",
    url=GITLAB_URL,
    packages=find_packages(),  # packages's modules
    install_requires=parse_requirements() + parse_requirements("swh"),
    tests_require=parse_requirements("test"),
    setup_requires=["setuptools_scm"],
    extras_require={"testing": parse_requirements("test")},
    use_scm_version=True,
    include_package_data=True,
    entry_points="""
        [swh.cli.subcommands]
        content-replayer=swh.objstorage.replayer.cli
    """,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
    ],
    project_urls={
        "Bug Reports": f"{GITLAB_URL}/-/issue",
        "Funding": "https://www.softwareheritage.org/donate",
        "Source": f"{GITLAB_URL}.git",
    },
)

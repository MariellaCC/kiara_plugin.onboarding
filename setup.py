#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

from setuptools import setup

try:
    from pkg_resources import VersionConflict, require

    require("setuptools>=38.3")
except VersionConflict:
    print("Error: version of setuptools is too old (<38.3)!")
    sys.exit(1)


def get_extra_requires(add_all=True, add_all_dev=True):

    from distutils.dist import Distribution

    dist = Distribution()
    dist.parse_config_files()
    dist.parse_command_line()

    extras = {}
    extra_deps = dist.get_option_dict("options.extras_require")

    for extra_name, data in extra_deps.items():

        _, dep_string = data
        deps = []
        d = dep_string.split("\n")
        for line in d:
            if not line:
                continue
            deps.append(line)
        extras[extra_name] = deps

    if add_all:
        all = set()
        for e_n, deps in extras.items():
            if not e_n.startswith("dev_"):
                all.update(deps)
        extras["all"] = all

    # add tag `all` at the end
    if add_all_dev:
        extras["all_dev"] = set(vv for v in extras.values() for vv in v)
        extras["dev_all"] = extras["all_dev"]

    return extras


if __name__ in ["__main__", "builtins", "__builtin__"]:
    setup(
        use_scm_version={"write_to": "src/kiara_plugin/onboarding/version.txt"},
        extras_require=get_extra_requires(),
    )

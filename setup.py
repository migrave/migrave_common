#!/usr/bin/env python3

from distutils.core import setup
from catkin_pkg.python_setup import generate_distutils_setup

d = generate_distutils_setup(
    packages=['migrave_common_test',
              'migrave_common_ros',
              'migrave_common'],
    package_dir={'migrave_common_test': 'common/src/migrave_common_test',
                 'migrave_common_ros': 'ros/src/migrave_common_ros',
                 'migrave_common': 'common/src/migrave_common'}
)

setup(**d)

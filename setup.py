#!/usr/bin/env python3

from distutils.core import setup
from catkin_pkg.python_setup import generate_distutils_setup

d = generate_distutils_setup(
    packages=['migrave_common_ros_utils',
              'migrave_common_utils'],
    package_dir={'migrave_common_ros_utils': 'ros/src/migrave_common_ros_utils',
                 'migrave_common_utils': 'common/src/migrave_common_utils'}
)

setup(**d)

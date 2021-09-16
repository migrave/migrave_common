#!/usr/bin/env python3

from distutils.core import setup
from catkin_pkg.python_setup import generate_distutils_setup

d = generate_distutils_setup(
    packages=['ros_vision_utils'],
    package_dir={'ros_vision_utils': 'ros/src/ros_vision_utils'}
)

setup(**d)

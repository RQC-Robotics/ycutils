from setuptools import setup, find_packages
from ycutils.utils import parse_requirements

setup(
    name="ycutils",
    version="0.0.1",
    packages=find_packages("."),
    install_requires=parse_requirements("requirements.txt")
)
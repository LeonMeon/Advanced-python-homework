from setuptools import setup, find_packages

setup(
    name='stem',
    version='0.0.1',
    packages=find_packages(include=['stem', 'stem.*']),
    install_requires=["matplotlib", "numpy"]
)
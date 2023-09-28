from setuptools import setup, find_packages

setup(
    name='pyspark-attribution-models',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'pyspark>=3.0.0',
    ],
)

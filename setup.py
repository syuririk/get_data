from setuptools import setup, find_packages

setup(
    name="get_data",
    version="0.1.0",
    packages=find_packages(include=["get_data_func", "get_data_func.*"]),
    install_requires=[
        requests=2.32.4,
        pandas=2.2.2
    ],
    include_package_data=True,
    description="Get data from ECOS",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
)
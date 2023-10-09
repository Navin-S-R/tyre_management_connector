from setuptools import setup, find_packages

with open("requirements.txt") as f:
	install_requires = f.read().strip().split("\n")

# get version from __version__ variable in tyre_management_connector/__init__.py
from tyre_management_connector import __version__ as version

setup(
	name="tyre_management_connector",
	version=version,
	description="Tyre Management Connector",
	author="Aerele",
	author_email="hello@aerele.in",
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires
)

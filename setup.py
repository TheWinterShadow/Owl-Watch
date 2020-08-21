# Created by The White Wolf
# Date: 8/21/20
# Time: 7:40 PM


from setuptools import setup, find_packages

with open("README.md", "r") as f:
    long_description = f.read()

NAME = 'owl-watch'
DESCRIPTION = 'COVID dashboard.'
URL = 'https://git.netsecure.dev/Dumbledore/Owl-Watch'
EMAIL = 'admin@netsecure.dev'
AUTHOR = 'Dumbledore & Neville'
REQUIRES_PYTHON = '>=3.7.0'
VERSION = '1.0'


setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    install_requires=["flask", "requests", "pandas"],
    include_package_data=True,
    license='MIT',
)

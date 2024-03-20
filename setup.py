from setuptools import setup, find_packages

setup(
    name='smartpy',
    version='0.1',
    description='Smartpy first release',
    packages=find_packages(),
    author='Othmane Zoheir',
    author_email='othmane@rumorz.io',
    url='',
    install_requires=[
        'pandas',
        'matplotlib',
        'numpy',
        'psutil'
    ]
)

from setuptools import setup, find_packages

setup(
    name="py-protodb",
    version="0.8",
    description='Python Proto and CRUDL code generation that maps to a PostgreSQL database',
    author='Bryan Hughes',
    author_email='hughesb@gmail.com',
    packages=find_packages(),
    include_package_data=True
)
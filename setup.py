import os
import setuptools

setuptools.setup(
    name='afs2-datasource',
    version='1.0.11',
    author='WISE-PaaS/AFS',
    author_email='stacy.yeh@advantech.com.tw',
    packages=setuptools.find_packages(),
    install_requires=[
      "pymongo==3.7.2",
      "pandas==0.24.2",
      "psycopg2-binary==2.8.1",
      "influxdb==5.2.2"
    ],
    description='For AFS developer to access Datasource',
    keywords=['AFS'],
    license='Apache License 2.0'
)

# python setup.py bdist_wheel
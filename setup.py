import os
import setuptools
try: # for pip >= 10
  from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
  from pip.req import parse_requirements

requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
install_requires = parse_requirements(requirements_path, session='hack')
install_requires = [str(ir.req) for ir in install_requires]

with open(os.path.join(os.path.dirname(__file__), 'VERSION'), 'r') as f:
  version = f.read()

with open(os.path.join(os.path.dirname(__file__), 'README.md'), 'r') as f:
  long_description = f.read()

setuptools.setup(
  name='afs2-datasource',
  version=version,
  description='For AFS developer to access Datasource',
  long_description=long_description,
  long_description_content_type='text/markdown',
  author='WISE-PaaS/AFS',
  author_email='stacy.yeh@advantech.com.tw',
  packages=setuptools.find_packages(),
  install_requires=install_requires,
  keywords=['AFS'],
  license='Apache License 2.0',
  url='https://github.com/stacy0416/afs2-datasource'
)

# python setup.py bdist_wheel
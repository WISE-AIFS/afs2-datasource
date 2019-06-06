@RD /S /Q "dist/"
python setup.py bdist_wheel
C:/Users/stacy.yeh/AppData/Local/Programs/Python/Python36/Scripts/twine.exe upload --repository-url https://test.pypi.org/legacy/ dist/*
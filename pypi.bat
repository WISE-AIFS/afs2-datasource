@RD /S /Q "dist/"
python setup.py bdist_wheel
twine upload dist/*

rem username: stacy.yeh
rem password:
VENV_BIN=envs/2.7/bin

all:

test:
	$(VENV_BIN)/pytest --cov=dbcluster --cov-report=term-missing tests/

tox:
	$(VENV_BIN)/tox

tox-recreate:
	$(VENV_BIN)/tox -r

pypi-upload:
	rm -rf dist
	$(VENV_BIN)/python setup.py sdist
	#$(VENV_BIN)/python setup.py bdist
	$(VENV_BIN)/python setup.py bdist_wheel
	$(VENV_BIN)/twine upload dist/*

.PHONY: install install-dev install-pre-commit test unit style check

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

install-pre-commit:
	pre-commit install

test:
	pytest --cov=abnosql --cov-report=xml --cov-report html:/tmp/htmlcov

style:
	pre-commit run --all-files

check: style test

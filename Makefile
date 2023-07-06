.PHONY: install install-dev install-pre-commit test unit style check

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

install-pre-commit:
	pre-commit install

test:
	pytest --cov=abnosql tests/ --cov-report html:/tmp/htmlcov --cov-fail-under 80

lint:
	flake8 .

style:
	pre-commit run --all-files

check: lint style test

#!/bin/bash
set -e
pytest --cov=nosql tests/ --cov-report html:/tmp/htmlcov --cov-fail-under 80
flake8 .
mypy nosql
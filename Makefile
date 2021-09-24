DIRS ?= dagger_contrib/ tests/


.PHONY: install
install:
	poetry install --extras all

.PHONY: test
test:
	poetry run pytest --cov=dagger_contrib --cov-fail-under=90 --cov-report=xml tests/

.PHONY: lint
lint:
	poetry run flake8 $(DIRS)

.PHONY: check-types
check-types:
	poetry run mypy --ignore-missing-imports $(DIRS)

.PHONY: check-docstrings
check-docstrings:
	poetry run pydocstyle --explain $(DIRS)

.PHONY: check-format
check-format:
	poetry run black --check --diff $(DIRS)
	poetry run isort --check --diff $(DIRS)

.PHONY: ci
ci: lint check-types check-format check-docstrings test
	@echo "All checks have passed"


.PHONY: set-version-from-git
set-version-from-git:
	@echo "Version will be $$(git describe --tags --abbrev=0)"
	poetry version $$(git describe --tags --abbrev=0)
	sed -i "s/0.0.0/$$(git describe --tags --abbrev=0)/" dagger_contrib/__init__.py
	cat dagger_contrib/__init__.py

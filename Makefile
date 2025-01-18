# Run tests separately to avoid class variable pollution
test:
	@echo "Running all tests..."
	@find tests -name "test_*.py" | while read testfile; do \
		poetry run coverage run --parallel-mode -m unittest "$$testfile"; \
	done
	@poetry run coverage combine
	@poetry run coverage report -m

flake8:
	@echo "Running flake8..."
	@poetry run flake8 simpledbpy tests

black:
	@echo "Running black..."
	@poetry run black --check --diff simpledbpy tests

isort:
	@echo "Running isort..."
	@poetry run isort -c --diff simpledbpy tests

mypy:
	@echo "Running mypy..."
	@poetry run mypy --pretty simpledbpy tests

check: test flake8 black isort mypy

test:
	@echo "Running all tests..."
	@poetry run python -m unittest discover -s tests

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

CODE := idu_kafka_client
TEST := tests

lint:
	poetry run pylint $(CODE)

lint-tests:
	poetry run pylint $(TEST)

format:
	poetry run isort $(CODE)
	poetry run black $(CODE)
	poetry run isort $(TEST)
	poetry run black $(TEST)

install:
	pip install .

install-dev:
	poetry install --with dev

install-dev-pip:
	pip install -e . --config-settings editable_mode=strict

clean:
	rm -rf ./dist

build:
	poetry build

install-from-build:
	python -m wheel install dist/$(CODE)-*.whl

test:
	poetry run pytest --verbose tests/

test-cov:
	poetry run pytest --verbose tests/ --cov $(CODE)/

PATH  := $(PATH):$(HOME)/.local/bin
SHELL := env PATH=$(PATH) /bin/bash

help : Makefile
	@sed -n 's/^##//p' $<

.PHONY: build
build:
	uv sync --all-extas

compile:
	uv run protobunny generate
	make format

.PHONY: format
format:
	uv run ruff check . --select I --fix
	uv run ruff format .
	uv run toml-sort -i ./pyproject.toml --sort-first project

.PHONY: lint
lint:
	uv run ruff check . --diff
	uv run ruff format . --check --diff
	uv run toml-sort --check ./pyproject.toml --sort-first project
	uv run yamllint -d "{extends: relaxed, rules: {line-length: {max: 120}}}" .

.PHONY: test integration-test t
test:
	uv run protobunny generate -I tests/proto --python_betterproto_out=tests tests/proto/*.proto
	make format
	uv run pytest tests/ -m "not integration"

t:
	# Usage: make t t=tests/test_connection.py::test_sync_get_message_count
	PYTHONASYNCIODEBUG=1 PYTHONBREAKPOINT=ipdb.set_trace uv run pytest ${t} -s -vvvv --durations=0

integration-test:
	uv run pytest tests/ -m "integration" -k rabbitmq ${t}
	uv run pytest tests/ -m "integration" -k redis ${t}
	uv run pytest tests/ -m "integration" -k python ${t}
	uv run pytest tests/ -m "integration" -k mosquitto ${t}
	uv run pytest tests/ -m "integration" -k nats ${t}  # run ./nats-server -js -sd nats_storage

integration-test-py310:
	source .venv310/bin/activate
	UV_PROJECT_ENVIRONMENT=.venv310 uv sync --all-extras --dev
	UV_PROJECT_ENVIRONMENT=.venv310 uv run protobunny generate -I tests/proto --python_betterproto_out=tests tests/proto/*.proto
	make format
	UV_PROJECT_ENVIRONMENT=.venv310 uv run pytest ${t} -m "integration" -vvv -s

integration-test-py311:
	source .venv311/bin/activate
	UV_PROJECT_ENVIRONMENT=.venv311 uv sync --all-extras --dev
	UV_PROJECT_ENVIRONMENT=.venv311 uv run protobunny generate -I tests/proto --python_betterproto_out=tests tests/proto/*.proto
	make format
	UV_PROJECT_ENVIRONMENT=.venv311 uv run pytest ${t} -m "integration" -vvv -s

integration-test-py312:
	source .venv312/bin/activate
	UV_PROJECT_ENVIRONMENT=.venv312 uv sync --all-extras --dev
	UV_PROJECT_ENVIRONMENT=.venv312 uv run protobunny generate -I tests/proto --python_betterproto_out=tests tests/proto/*.proto
	make format
	UV_PROJECT_ENVIRONMENT=.venv312 uv run pytest ${t} -m "integration" -vvv -s

integration-test-py313:
	source .venv313/bin/activate
	UV_PROJECT_ENVIRONMENT=.venv313 uv sync --all-extras --dev
	UV_PROJECT_ENVIRONMENT=.venv313 uv run protobunny generate -I tests/proto --python_betterproto_out=tests tests/proto/*.proto
	make format
	UV_PROJECT_ENVIRONMENT=.venv313 uv run pytest ${t} -m "integration" -vvv -s

test-py310:
	source .venv310/bin/activate
	VIRTUAL_ENV=.venv310 UV_PROJECT_ENVIRONMENT=.venv310 uv sync --all-extras --dev
	VIRTUAL_ENV=.venv310 UV_PROJECT_ENVIRONMENT=.venv310 uv run protobunny generate -I tests/proto --python_betterproto_out=tests tests/proto/*.proto
	make format
	VIRTUAL_ENV=.venv310 PYTHONASYNCIODEBUG=1 UV_PROJECT_ENVIRONMENT=.venv310 uv run pytest tests/ -m "not integration" -vvv -s

test-py311:
	source .venv311/bin/activate
	VIRTUAL_ENV=.venv311 UV_PROJECT_ENVIRONMENT=.venv311 uv sync --all-extras --dev
	VIRTUAL_ENV=.venv311 UV_PROJECT_ENVIRONMENT=.venv311 uv run protobunny generate -I tests/proto --python_betterproto_out=tests tests/proto/*.proto
	make format
	VIRTUAL_ENV=.venv311 PYTHONASYNCIODEBUG=1 UV_PROJECT_ENVIRONMENT=.venv311 uv run pytest tests/ -m "not integration" -vvv -s

test-py312:
	source .venv312/bin/activate
	VIRTUAL_ENV=.venv312 UV_PROJECT_ENVIRONMENT=.venv312 uv sync --all-extras --dev
	VIRTUAL_ENV=.venv312 UV_PROJECT_ENVIRONMENT=.venv312 uv run protobunny generate -I tests/proto --python_betterproto_out=tests tests/proto/*.proto
	make format
	VIRTUAL_ENV=.venv312 UV_PROJECT_ENVIRONMENT=.venv312 uv run pytest tests/ -m "not integration" -vvv -s

test-py313:
	source .venv313/bin/activate
	VIRTUAL_ENV=.venv313 UV_PROJECT_ENVIRONMENT=.venv313 uv sync --all-extras --dev
	VIRTUAL_ENV=.venv313 UV_PROJECT_ENVIRONMENT=.venv313 uv run protobunny generate -I tests/proto --python_betterproto_out=tests tests/proto/*.proto
	make format
	VIRTUAL_ENV=.venv313 UV_PROJECT_ENVIRONMENT=.venv313 uv run pytest tests/ -m "not integration" -vvv -s

# Releasing
.PHONY: docs clean build-package publish-test publish-pypi copy-md
copy-md:
	cp ./README.md docs/source/intro.md
	cp ./QUICK_START.md docs/source/quick_start.md
	cp ./RECIPES.md docs/source/recipes.md

docs: copy-md
	uv run sphinx-build -b html docs/source docs/build/html

clean:
	rm -rf dist build *.egg-info

build-package: clean
	uv build

publish-test: build-package
	$(eval SECTION := testpypi)
	$(eval PYPI_TOKEN := $(shell sed -n '/^\[$(SECTION)\]/,/^\[.*\]/ { /password *=/ s/.*= *//p; }' $(HOME)/.pypirc))
	uv publish --publish-url https://test.pypi.org/legacy/ --token $(PYPI_TOKEN)

publish: build-package
	$(eval SECTION := pypi)
	$(eval PYPI_TOKEN := $(shell sed -n '/^\[$(SECTION)\]/,/^\[.*\]/ { /password *=/ s/.*= *//p; }' $(HOME)/.pypirc))
	uv publish --token $(PYPI_TOKEN)


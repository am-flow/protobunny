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

.PHONY: lint
lint:
	uv run ruff check . --diff
	uv run ruff format . --check --diff

.PHONY: test integration-test
test:
	uv run protobunny generate -I tests/proto --python_betterproto_out=tests tests/proto/*.proto
	make format
	uv run pytest tests/ -m "not integration"
integration-test:
	uv run pytest tests/ -m "integration"

# Releasing
.PHONY: docs clean build-package publish-test publish-pypi convert-md
convert-md:
	uv run python scripts/convert_md.py
docs:
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


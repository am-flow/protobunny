PATH  := $(PATH):$(HOME)/.local/bin
SHELL := env PATH=$(PATH) /bin/bash

help : Makefile
	@sed -n 's/^##//p' $<

.PHONY: build
build:
	uv sync

compile:
	mkdir -p protobunny/core
	uv run python -m grpc_tools.protoc -I protobunny/protobuf/protobunny --python_betterproto_out=protobunny/core protobunny/protobuf/protobunny/*.proto
	uv run python scripts/post_compile.py --proto-pkg=protobunny.core
	make format

.PHONY: format
format:
	uv run ruff check . --select I --fix
	uv run ruff format .

.PHONY: lint
lint:
	uv run ruff check . --diff
	uv run ruff format . --check --diff


.PHONY: test integration_test
test:
	uv run protobunny -I tests/proto --python_betterproto_out=tests tests/proto/*.proto
	make format
	uv run pytest tests/ -m "not integration"
integration_test:
	uv run pytest tests/ -m "integration"

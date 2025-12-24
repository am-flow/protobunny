from unittest import mock

import pytest

from protobunny.config import load_config


@pytest.fixture(autouse=True)
def clear_config_cache():
    """Clear the cache before each test since load_config is cached."""
    load_config.cache_clear()
    yield
    load_config.cache_clear()


def test_config_default_values():
    """Test that default values are applied when no config files exist."""
    with mock.patch("pathlib.Path.exists", return_value=False):
        config = load_config()
        assert config.messages_prefix == "pb"
        assert config.backend == "rabbitmq"
        assert config.mode == "sync"


def test_config_from_env(monkeypatch):
    """Test that environment variables override everything else."""
    monkeypatch.setenv("PROTOBUNNY_BACKEND", "redis")
    monkeypatch.setenv("PROTOBUNNY_MODE", "async")
    monkeypatch.setenv("PROTOBUNNY_FORCE_REQUIRED_FIELDS", "true")

    # Mocking pyproject and ini to be missing
    with mock.patch("pathlib.Path.exists", return_value=False):
        config = load_config()
        assert config.backend == "redis"
        assert config.mode == "async"
        assert config.force_required_fields is True


def test_config_from_ini(tmp_path, monkeypatch):
    """Test reading from protobunny.ini."""
    monkeypatch.chdir(tmp_path)
    ini_content = """
[protobunny]
backend = python
mode = async
force-required-fields = yes
"""
    (tmp_path / "protobunny.ini").write_text(ini_content)

    # Ensure pyproject doesn't interfere
    with mock.patch("protobunny.config.get_config_from_pyproject", return_value={}):
        config = load_config()
        assert config.backend == "python"
        assert config.mode == "async"
        assert config.force_required_fields is True


def test_config_precedence(tmp_path, monkeypatch):
    """Test that ENV > INI > pyproject."""
    monkeypatch.chdir(tmp_path)

    # 1. Setup INI
    ini_file = tmp_path / "protobunny.ini"
    ini_file.write_text("[protobunny]\nbackend = python\nmessages_prefix = ini_pref")

    # 2. Setup ENV (should override INI)
    monkeypatch.setenv("PROTOBUNNY_BACKEND", "redis")

    # Mock pyproject to provide a base
    mock_pyproject = {"backend": "rabbitmq", "messages_prefix": "py_pref", "project-name": "test"}

    with mock.patch("protobunny.config.get_config_from_pyproject", return_value=mock_pyproject):
        config = load_config()

        # Overridden by ENV
        assert config.backend == "redis"
        # Overridden by INI
        assert config.messages_prefix == "ini_pref"
        # Taken from pyproject
        assert config.project_name == "test"

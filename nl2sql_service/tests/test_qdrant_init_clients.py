import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# 按你的项目真实 import 路径调整
import core.semantic_registry as sr


def _make_registry_instance():
    """
    避免 SemanticRegistry.__init__ 做太多事（比如加载 YAML/建索引）。
    用 __new__ 创建一个“空实例”，只测试 _init_clients 的行为。
    """
    reg = sr.SemanticRegistry.__new__(sr.SemanticRegistry)
    return reg


def test_init_clients_memory_mode(monkeypatch):
    monkeypatch.setenv("VECTOR_STORE_MODE", "memory")

    mock_client_cls = MagicMock()
    monkeypatch.setattr(sr, "AsyncQdrantClient", mock_client_cls)

    reg = _make_registry_instance()
    sr.SemanticRegistry._init_clients(reg)

    mock_client_cls.assert_called_once_with(location=":memory:")
    assert reg.qdrant_client == mock_client_cls.return_value


def test_init_clients_remote_mode_with_url(monkeypatch):
    monkeypatch.setenv("VECTOR_STORE_MODE", "remote")
    monkeypatch.setenv("QDRANT_URL", "http://localhost:6333")
    monkeypatch.setenv("QDRANT_API_KEY", "test_key")

    mock_client_cls = MagicMock()
    monkeypatch.setattr(sr, "AsyncQdrantClient", mock_client_cls)

    reg = _make_registry_instance()
    sr.SemanticRegistry._init_clients(reg)

    mock_client_cls.assert_called_once()
    kwargs = mock_client_cls.call_args.kwargs
    assert kwargs["url"] == "http://localhost:6333"
    assert kwargs["api_key"] == "test_key"
    assert reg.qdrant_client == mock_client_cls.return_value


def test_init_clients_remote_mode_host_port_fallback(monkeypatch):
    monkeypatch.setenv("VECTOR_STORE_MODE", "remote")
    monkeypatch.delenv("QDRANT_URL", raising=False)
    monkeypatch.setenv("QDRANT_HOST", "localhost")
    monkeypatch.setenv("QDRANT_PORT", "6333")

    mock_client_cls = MagicMock()
    monkeypatch.setattr(sr, "AsyncQdrantClient", mock_client_cls)

    reg = _make_registry_instance()
    sr.SemanticRegistry._init_clients(reg)

    mock_client_cls.assert_called_once()
    kwargs = mock_client_cls.call_args.kwargs
    assert kwargs["host"] == "localhost"
    assert kwargs["port"] == 6333  # 必须是 int
    assert "url" not in kwargs
    assert reg.qdrant_client == mock_client_cls.return_value


def test_init_clients_local_default_path(monkeypatch, tmp_path):
    # 不设置 mode -> 默认 local
    monkeypatch.delenv("VECTOR_STORE_MODE", raising=False)
    monkeypatch.delenv("VECTOR_STORE_PATH", raising=False)

    # 把默认目录指到 pytest 的临时目录，避免污染项目根目录
    default_dir = tmp_path / "qdrant_data"
    monkeypatch.setattr(sr, "DEFAULT_STORAGE_PATH", default_dir)

    mock_client_cls = MagicMock()
    monkeypatch.setattr(sr, "AsyncQdrantClient", mock_client_cls)

    reg = _make_registry_instance()
    sr.SemanticRegistry._init_clients(reg)

    assert default_dir.exists() and default_dir.is_dir()
    mock_client_cls.assert_called_once_with(path=str(default_dir))
    assert reg.qdrant_client == mock_client_cls.return_value


def test_init_clients_local_custom_vector_store_path(monkeypatch, tmp_path):
    monkeypatch.setenv("VECTOR_STORE_MODE", "local")
    custom_dir = tmp_path / "custom_qdrant"
    monkeypatch.setenv("VECTOR_STORE_PATH", str(custom_dir))

    mock_client_cls = MagicMock()
    monkeypatch.setattr(sr, "AsyncQdrantClient", mock_client_cls)

    reg = _make_registry_instance()
    sr.SemanticRegistry._init_clients(reg)

    assert custom_dir.exists() and custom_dir.is_dir()
    mock_client_cls.assert_called_once_with(path=str(custom_dir))

    # 关键：local 不应使用 location=目录（location 传普通字符串会被当成 url）:contentReference[oaicite:1]{index=1}
    called_kwargs = mock_client_cls.call_args.kwargs
    assert "location" not in called_kwargs

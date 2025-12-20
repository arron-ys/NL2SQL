"""
【简述】
验证 SemanticRegistry 的 Qdrant 客户端初始化逻辑在不同模式（memory/remote/local）下的配置正确性。

【范围/不测什么】
- 不覆盖真实 Qdrant 连接；仅验证客户端初始化参数的配置规则与路径解析。

【用例概述】
- test_init_clients_memory_mode:
  -- 验证 VECTOR_STORE_MODE=memory 时使用 :memory: location
- test_init_clients_remote_mode_with_url:
  -- 验证 remote 模式使用 QDRANT_URL 和 QDRANT_API_KEY 配置
- test_init_clients_remote_mode_host_port_fallback:
  -- 验证 remote 模式在无 URL 时降级使用 host/port 配置
- test_init_clients_local_default_path:
  -- 验证 local 模式使用默认路径
- test_init_clients_local_custom_vector_store_path:
  -- 验证 local 模式使用自定义 VECTOR_STORE_PATH
"""

import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# 按你的项目真实 import 路径调整
import core.semantic_registry as sr


def _make_registry_instance():
    """
    避免 SemanticRegistry.__init__ 做太多事（比如加载 YAML/建索引）。
    用 __new__ 创建一个"空实例"，只测试 _init_clients 的行为。
    """
    reg = sr.SemanticRegistry.__new__(sr.SemanticRegistry)
    return reg


@pytest.mark.unit
def test_init_clients_memory_mode(monkeypatch):
    """
    【测试目标】
    1. 验证 VECTOR_STORE_MODE=memory 时使用 :memory: location

    【执行过程】
    1. 设置 VECTOR_STORE_MODE=memory
    2. mock AsyncQdrantClient
    3. 调用 _init_clients
    4. 验证客户端初始化参数

    【预期结果】
    1. AsyncQdrantClient 被调用一次
    2. location 参数为 ":memory:"
    3. qdrant_client 属性被设置
    """
    monkeypatch.setenv("VECTOR_STORE_MODE", "memory")

    mock_client_cls = MagicMock()
    monkeypatch.setattr(sr, "AsyncQdrantClient", mock_client_cls)

    reg = _make_registry_instance()
    sr.SemanticRegistry._init_clients(reg)

    mock_client_cls.assert_called_once_with(location=":memory:")
    assert reg.qdrant_client == mock_client_cls.return_value


@pytest.mark.unit
def test_init_clients_remote_mode_with_url(monkeypatch):
    """
    【测试目标】
    1. 验证 remote 模式使用 QDRANT_URL 和 QDRANT_API_KEY 配置

    【执行过程】
    1. 设置 VECTOR_STORE_MODE=remote, QDRANT_URL, QDRANT_API_KEY
    2. mock AsyncQdrantClient
    3. 调用 _init_clients
    4. 验证客户端初始化参数

    【预期结果】
    1. AsyncQdrantClient 被调用一次
    2. url 参数为配置的 QDRANT_URL
    3. api_key 参数为配置的 QDRANT_API_KEY
    """
    # 明确禁用离线模式，以便测试 remote 模式
    monkeypatch.delenv("NO_NETWORK", raising=False)
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


@pytest.mark.unit
def test_init_clients_remote_mode_host_port_fallback(monkeypatch):
    """
    【测试目标】
    1. 验证 remote 模式在无 URL 时降级使用 host/port 配置

    【执行过程】
    1. 设置 VECTOR_STORE_MODE=remote, QDRANT_HOST, QDRANT_PORT
    2. 删除 QDRANT_URL 环境变量
    3. mock AsyncQdrantClient
    4. 调用 _init_clients
    5. 验证客户端初始化参数

    【预期结果】
    1. AsyncQdrantClient 被调用一次
    2. host 参数为 "localhost"，port 参数为 6333（int）
    3. kwargs 中不包含 url 参数
    """
    # 明确禁用离线模式，以便测试 remote 模式
    monkeypatch.delenv("NO_NETWORK", raising=False)
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


@pytest.mark.unit
def test_init_clients_local_default_path(monkeypatch, tmp_path):
    """
    【测试目标】
    1. 验证 local 模式使用默认路径

    【执行过程】
    1. 删除 VECTOR_STORE_MODE 和 VECTOR_STORE_PATH 环境变量
    2. 设置 DEFAULT_STORAGE_PATH 为临时目录
    3. mock AsyncQdrantClient
    4. 调用 _init_clients
    5. 验证目录创建与客户端初始化

    【预期结果】
    1. 默认目录被创建
    2. AsyncQdrantClient 使用 path 参数调用
    3. path 为默认目录路径
    """
    # 明确禁用离线模式，以便测试 local 模式
    monkeypatch.delenv("NO_NETWORK", raising=False)
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


@pytest.mark.unit
def test_init_clients_local_custom_vector_store_path(monkeypatch, tmp_path):
    """
    【测试目标】
    1. 验证 local 模式使用自定义 VECTOR_STORE_PATH

    【执行过程】
    1. 设置 VECTOR_STORE_MODE=local, VECTOR_STORE_PATH 为自定义路径
    2. mock AsyncQdrantClient
    3. 调用 _init_clients
    4. 验证目录创建与客户端初始化

    【预期结果】
    1. 自定义目录被创建
    2. AsyncQdrantClient 使用 path 参数调用
    3. path 为自定义目录路径
    """
    # 明确禁用离线模式，以便测试 local 模式
    monkeypatch.delenv("NO_NETWORK", raising=False)
    monkeypatch.setenv("VECTOR_STORE_MODE", "local")
    custom_dir = tmp_path / "custom_qdrant"
    monkeypatch.setenv("VECTOR_STORE_PATH", str(custom_dir))

    mock_client_cls = MagicMock()
    monkeypatch.setattr(sr, "AsyncQdrantClient", mock_client_cls)

    reg = _make_registry_instance()
    sr.SemanticRegistry._init_clients(reg)

    assert custom_dir.exists() and custom_dir.is_dir()
    mock_client_cls.assert_called_once_with(path=str(custom_dir))
    assert reg.qdrant_client == mock_client_cls.return_value

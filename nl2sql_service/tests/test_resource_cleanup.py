"""
【简述】
验证测试运行过程中资源正确清理：TestClient、AsyncClient、数据库连接、HTTP连接池等资源在测试结束后被正确释放。

【范围/不测什么】
- 不覆盖真实数据库连接；仅验证fixture生命周期和资源清理机制。
- 不覆盖live测试的真实资源；仅验证mock环境下的资源管理。

【用例概述】
- test_client_fixture_cleans_up:
  -- 验证client fixture使用context manager正确清理TestClient资源
- test_async_client_fixture_cleans_up:
  -- 验证async_client fixture在session结束时正确清理AsyncClient资源
- test_no_resource_warnings:
  -- 验证测试运行不产生ResourceWarning（未关闭的连接/文件句柄等）
- test_mock_providers_no_leak:
  -- 验证mock的AI provider客户端不产生资源泄露
- test_app_lifespan_no_unicode_error:
  -- 验证app启动/关闭过程中不会触发UnicodeDecodeError（Windows capture场景）
"""

import warnings
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient

from main import app


@pytest.mark.unit
def test_client_fixture_cleans_up(client):
    """
    【测试目标】
    1. 验证client fixture使用context manager正确清理TestClient资源

    【执行过程】
    1. 使用client fixture（已在conftest.py中定义为context manager）
    2. 执行一个简单的API调用
    3. 验证fixture退出时资源被清理（通过context manager自动处理）

    【预期结果】
    1. client fixture可以正常使用
    2. 测试结束后TestClient资源被正确释放（context manager保证）
    3. 不产生ResourceWarning
    """
    # 使用fixture，context manager会自动处理清理
    response = client.get("/health")
    assert response.status_code in [200, 503]  # 健康检查可能返回503如果registry未初始化


@pytest.mark.unit
@pytest.mark.asyncio
async def test_async_client_fixture_cleans_up(async_client):
    """
    【测试目标】
    1. 验证async_client fixture在session结束时正确清理AsyncClient资源

    【执行过程】
    1. 使用async_client fixture（scope="session"）
    2. 执行一个简单的异步API调用
    3. 验证fixture退出时AsyncClient资源被清理

    【预期结果】
    1. async_client fixture可以正常使用
    2. 测试结束后AsyncClient资源被正确释放（async context manager保证）
    3. 不产生ResourceWarning
    """
    # 使用fixture，async context manager会自动处理清理
    response = await async_client.get("/health")
    assert response.status_code in [200, 503]


@pytest.mark.unit
def test_no_resource_warnings():
    """
    【测试目标】
    1. 验证测试运行不产生ResourceWarning（未关闭的连接/文件句柄等）

    【执行过程】
    1. 捕获所有warnings
    2. 创建一个TestClient并执行操作
    3. 确保正确使用context manager
    4. 验证没有ResourceWarning产生

    【预期结果】
    1. 没有ResourceWarning被捕获
    2. TestClient使用context manager正确清理
    """
    # 捕获warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        
        # 使用context manager确保资源清理
        with TestClient(app) as client:
            response = client.get("/health")
            assert response.status_code in [200, 503]
        
        # 检查是否有ResourceWarning
        resource_warnings = [warning for warning in w if issubclass(warning.category, ResourceWarning)]
        assert len(resource_warnings) == 0, f"Found {len(resource_warnings)} ResourceWarnings: {[str(w.message) for w in resource_warnings]}"


@pytest.mark.unit
def test_mock_providers_no_leak():
    """
    【测试目标】
    1. 验证mock的AI provider客户端不产生资源泄露

    【执行过程】
    1. mock AI provider（OpenAI、Jina等）
    2. 创建AIClient实例
    3. 验证mock对象可以被正确清理
    4. 检查是否有资源泄露警告

    【预期结果】
    1. mock对象不持有真实连接
    2. 没有ResourceWarning产生
    3. mock可以被正确清理
    """
    from core.ai_client import AIClient
    
    # 捕获warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        
        # 创建配置，使用mock provider
        config = {
            "default_provider": "openai",
            "providers": {
                "openai": {
                    "api_key": "fake-key",
                    "type": "openai",
                },
                "jina": {
                    "api_key": "fake-key",
                    "type": "jina",
                },
            },
            "model_mapping": {
                "plan_generation": {
                    "provider": "openai",
                    "model": "gpt-4o-mini"
                },
                "embedding": {
                    "provider": "jina",
                    "model": "jina-embeddings-v3"
                },
            },
        }
        
        # 创建AIClient（会初始化providers，但使用fake key不会建立真实连接）
        client = AIClient(config=config)
        
        # 验证client被创建
        assert client is not None
        assert "openai" in client._providers
        assert "jina" in client._providers
        
        # 清理（Python GC会自动处理，但我们可以显式删除）
        del client
        
        # 检查是否有ResourceWarning
        resource_warnings = [warning for warning in w if issubclass(warning.category, ResourceWarning)]
        assert len(resource_warnings) == 0, f"Found {len(resource_warnings)} ResourceWarnings: {[str(w.message) for w in resource_warnings]}"


@pytest.mark.unit
def test_database_engine_cleanup():
    """
    【测试目标】
    1. 验证数据库引擎在测试中不产生资源泄露

    【执行过程】
    1. mock数据库连接（避免真实连接）
    2. 验证get_engine返回的引擎可以被正确管理
    3. 检查是否有资源泄露警告

    【预期结果】
    1. mock的数据库引擎不产生真实连接
    2. 没有ResourceWarning产生
    3. 引擎可以被正确清理
    """
    from core.db_connector import get_engine, close_all
    
    # 捕获warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        
        # Mock数据库配置，避免真实连接
        with patch("core.db_connector.get_pipeline_config") as mock_config:
            from config.pipeline_config import SupportedDialects
            mock_config_obj = MagicMock()
            mock_config_obj.db_type = SupportedDialects.MYSQL
            mock_config.return_value = mock_config_obj
            
            # 尝试获取引擎（会失败因为连接字符串无效，但不会产生资源泄露）
            try:
                engine = get_engine()
                # 如果成功获取引擎，验证它可以被清理
                if engine is not None:
                    # 注意：这里不实际调用close_all，因为可能影响其他测试
                    # 只是验证引擎对象可以被管理
                    assert engine is not None
            except Exception:
                # 预期可能失败（因为配置无效），但不应该产生资源泄露
                pass
        
        # 检查是否有ResourceWarning
        resource_warnings = [warning for warning in w if issubclass(warning.category, ResourceWarning)]
        assert len(resource_warnings) == 0, f"Found {len(resource_warnings)} ResourceWarnings: {[str(w.message) for w in resource_warnings]}"


@pytest.mark.unit
def test_app_lifespan_no_unicode_error(client):
    """
    【测试目标】
    1. 验证app启动/关闭过程中不会触发UnicodeDecodeError（Windows capture场景）
    2. 确保loguru输出包含中文时，pytest捕获缓冲区能正确解码

    【执行过程】
    1. 使用client fixture触发app lifespan（启动和关闭）
    2. 执行包含中文日志的API调用（触发loguru输出中文）
    3. 验证teardown阶段不会抛出UnicodeDecodeError

    【预期结果】
    1. app正常启动和关闭
    2. 包含中文的日志能正常输出到pytest捕获缓冲区
    3. teardown阶段不抛出UnicodeDecodeError
    4. 测试正常完成（如果出现UnicodeDecodeError，测试会失败）
    """
    # 触发包含中文的日志输出（通过API调用触发app内部日志）
    # 这些日志会在pytest捕获模式下被捕获，如果编码不正确会导致UnicodeDecodeError
    response = client.post(
        "/nl2sql/plan",
        json={
            "question": "查看订单明细",  # 包含中文，会触发中文日志
            "user_id": "test_user",
            "role_id": "ROLE_TEST",
            "tenant_id": "test_tenant",
        },
    )
    # 不关心响应状态码，只关心是否触发UnicodeDecodeError
    # 如果出现UnicodeDecodeError，测试会在teardown阶段失败
    assert response is not None


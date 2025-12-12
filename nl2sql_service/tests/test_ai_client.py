"""
AI Client Test Suite

验证多提供商 AI 客户端的正确性，包括：
- 配置初始化
- 路由逻辑
- 方法委托
- Stage 集成
"""
from datetime import date
from typing import Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.ai_client import AIClient, get_ai_client
from core.providers.jina_provider import JinaProvider
from core.providers.openai_provider import OpenAIProvider
from schemas.request import RequestContext, SubQueryItem


# ============================================================
# Test Fixtures and Helper Classes
# ============================================================

class FakeSettings:
    """模拟设置对象，用于测试 init_from_settings"""
    OPENAI_API_KEY = "fake-openai-key"
    OPENAI_BASE_URL = None
    JINA_API_KEY = "fake-jina-key"
    JINA_BASE_URL = None
    DEEPSEEK_API_KEY = "fake-deepseek-key"
    DEEPSEEK_BASE_URL = "https://api.deepseek.com"


# ============================================================
# Test Case 1: init_from_settings Produces a Valid AIClient
# ============================================================

def test_init_from_settings_produces_valid_client():
    """测试 init_from_settings 能正确构造 AIClient 实例"""
    # 创建 FakeSettings 实例
    settings = FakeSettings()
    
    # 调用 init_from_settings
    client = AIClient.init_from_settings(settings)
    
    # 断言：client 是 AIClient 实例
    assert isinstance(client, AIClient)
    
    # 断言：providers 字典包含 openai 和 jina
    assert "openai" in client._providers
    assert "jina" in client._providers
    
    # 断言：provider 类型正确
    assert isinstance(client._providers["openai"], OpenAIProvider)
    assert isinstance(client._providers["jina"], JinaProvider)
    
    # 断言：如果提供了 DeepSeek API Key，deepseek provider 应该被初始化
    if settings.DEEPSEEK_API_KEY:
        assert "deepseek" in client._providers
        assert isinstance(client._providers["deepseek"], OpenAIProvider)
        # 验证 DeepSeek Base URL
        assert "api.deepseek.com" in str(client._providers["deepseek"].client.base_url)


# ============================================================
# Test Case 2: Usage-Based Routing Logic
# ============================================================

def test_routing_embedding_to_jina():
    """测试 embedding usage_key 路由到 JinaProvider"""
    # 创建 AIClient 实例
    settings = FakeSettings()
    client = AIClient.init_from_settings(settings)
    
    # 调用路由解析器
    provider, model = client._resolve_model("embedding")
    
    # 断言：provider 是 JinaProvider 实例
    assert isinstance(provider, JinaProvider)
    
    # 断言：model 是 jina-embeddings-v3
    assert model == "jina-embeddings-v3"
    
    # 断言：Jina Base URL 验证（Jina 有默认 base_url）
    assert provider.base_url is not None
    assert "jina.ai" in provider.base_url


def test_routing_plan_generation_to_openai():
    """测试 plan_generation usage_key 路由到 OpenAIProvider"""
    # 创建 AIClient 实例
    settings = FakeSettings()
    client = AIClient.init_from_settings(settings)
    
    # 调用路由解析器
    provider, model = client._resolve_model("plan_generation")
    
    # 断言：provider 是 OpenAIProvider 实例
    assert isinstance(provider, OpenAIProvider)
    
    # 断言：model 是 gpt-4o-mini（根据实际配置）
    assert model == "gpt-4o-mini"
    
    # 断言：Base URL 验证（OpenAI 默认使用官方 API 或 None）
    # 如果设置了 base_url，应该包含 api.openai.com
    if provider.client.base_url:
        assert "api.openai.com" in str(provider.client.base_url)


def test_deepseek_routing_is_correct():
    """验证 DeepSeek provider 正确初始化，Base URL 指向 api.deepseek.com"""
    # 创建包含 DeepSeek 配置的 AIClient
    config = {
        "default_provider": "openai",
        "providers": {
            "openai": {
                "api_key": "fake-openai-key",
                "base_url": None,
                "type": "openai",
            },
            "deepseek": {
                "api_key": "fake-deepseek-key",
                "base_url": "https://api.deepseek.com",
                "type": "openai",  # DeepSeek 使用 OpenAI 兼容的 API
            },
            "jina": {
                "api_key": "fake-jina-key",
                "type": "jina",
            },
        },
        "model_mapping": {
            "plan_generation": {
                "provider": "deepseek",
                "model": "deepseek-reasoner"
            },
            "embedding": {
                "provider": "jina",
                "model": "jina-embeddings-v3"
            },
        },
    }
    
    client = AIClient(config=config)
    
    # 验证 deepseek provider 被初始化
    assert "deepseek" in client._providers
    
    # 解析 plan_generation（配置指向 deepseek）
    provider, model = client._resolve_model("plan_generation")
    
    # 验证使用的是 OpenAIProvider 适配器（架构设计要求）
    assert isinstance(provider, OpenAIProvider)
    
    # 关键验证：验证 Base URL 是否真的切到了 DeepSeek
    assert "api.deepseek.com" in str(provider.client.base_url)
    assert provider.client.api_key == "fake-deepseek-key"
    
    # 验证模型名称
    assert model == "deepseek-reasoner"


def test_init_with_deepseek_config():
    """测试直接通过 config 字典初始化包含 deepseek 的 AIClient"""
    config = {
        "default_provider": "openai",
        "providers": {
            "openai": {
                "api_key": "fake-openai-key",
                "base_url": "https://api.openai.com/v1",
                "type": "openai",
            },
            "deepseek": {
                "api_key": "fake-deepseek-key",
                "base_url": "https://api.deepseek.com",
                "type": "openai",
            },
            "jina": {
                "api_key": "fake-jina-key",
                "type": "jina",
            },
        },
        "model_mapping": {
            "plan_generation": {
                "provider": "deepseek",
                "model": "deepseek-reasoner"
            },
            "answer_generation": {
                "provider": "openai",
                "model": "gpt-4o"
            },
            "embedding": {
                "provider": "jina",
                "model": "jina-embeddings-v3"
            },
        },
    }
    
    client = AIClient(config=config)
    
    # 验证所有 provider 都被初始化
    assert "openai" in client._providers
    assert "deepseek" in client._providers
    assert "jina" in client._providers
    
    # 验证 provider 类型
    assert isinstance(client._providers["openai"], OpenAIProvider)
    assert isinstance(client._providers["deepseek"], OpenAIProvider)
    assert isinstance(client._providers["jina"], JinaProvider)
    
    # 验证 Base URL
    assert "api.deepseek.com" in str(client._providers["deepseek"].client.base_url)
    assert "api.openai.com" in str(client._providers["openai"].client.base_url)


# ============================================================
# Test Case 3: Public Method → Provider Method Wiring (Mocked)
# ============================================================

@pytest.mark.asyncio
async def test_get_embeddings_calls_jina_provider_embed():
    """测试 get_embeddings 正确调用 JinaProvider.embed"""
    # 创建 AIClient 实例
    settings = FakeSettings()
    client = AIClient.init_from_settings(settings)
    
    # 获取 Jina provider 实例
    jina_provider = client._providers["jina"]
    
    # 验证 Jina provider 的 Base URL（在 mock 之前）
    assert jina_provider.base_url is not None
    assert "jina.ai" in jina_provider.base_url
    
    # 替换 embed 方法为 AsyncMock
    mock_embedding = [[0.1, 0.2]]
    jina_provider.embed = AsyncMock(return_value=mock_embedding)
    
    # 调用 get_embeddings
    texts = ["hello"]
    result = await client.get_embeddings(texts=texts)
    
    # 断言：embed 被调用一次
    jina_provider.embed.assert_awaited_once()
    
    # 获取调用参数
    args, kwargs = jina_provider.embed.await_args
    
    # 断言：kwargs 中的 texts 和 model 正确
    assert kwargs["texts"] == texts
    assert kwargs["model"] == "jina-embeddings-v3"
    
    # 断言：返回值正确
    assert result == mock_embedding


@pytest.mark.asyncio
async def test_generate_plan_calls_openai_provider_chat_json():
    """测试 generate_plan 正确调用 OpenAIProvider.chat_json"""
    # 创建 AIClient 实例
    settings = FakeSettings()
    client = AIClient.init_from_settings(settings)
    
    # 获取 OpenAI provider 实例
    openai_provider = client._providers["openai"]
    
    # 验证 OpenAI provider 的 Base URL（在 mock 之前）
    # OpenAI 默认 base_url 为 None（使用官方 API）
    # 如果设置了 base_url，应该包含 api.openai.com
    if openai_provider.client.base_url:
        assert "api.openai.com" in str(openai_provider.client.base_url)
    
    # 替换 chat_json 方法为 AsyncMock
    fake_plan = {"plan_id": "test-plan"}
    openai_provider.chat_json = AsyncMock(return_value=fake_plan)
    
    # 调用 generate_plan
    messages = [{"role": "user", "content": "test"}]
    result = await client.generate_plan(messages=messages)
    
    # 断言：chat_json 被调用一次
    openai_provider.chat_json.assert_awaited_once()
    
    # 获取调用参数
    args, kwargs = openai_provider.chat_json.await_args
    
    # 断言：kwargs 中的 messages 和 model 正确
    assert kwargs["messages"] == messages
    assert kwargs["model"] == "gpt-4o-mini"
    
    # 断言：返回值正确
    assert result == fake_plan


@pytest.mark.asyncio
async def test_generate_plan_uses_deepseek_when_configured():
    """验证 generate_plan 方法在配置指向 deepseek 时调用 deepseek provider"""
    # 创建包含 DeepSeek 配置的 AIClient
    config = {
        "default_provider": "openai",
        "providers": {
            "openai": {
                "api_key": "fake-openai-key",
                "base_url": "https://api.openai.com/v1",
                "type": "openai",
            },
            "deepseek": {
                "api_key": "fake-deepseek-key",
                "base_url": "https://api.deepseek.com",
                "type": "openai",
            },
        },
        "model_mapping": {
            "plan_generation": {
                "provider": "deepseek",
                "model": "deepseek-reasoner"
            },
        },
    }
    
    client = AIClient(config=config)
    
    # 获取 deepseek provider 实例
    deepseek_provider = client._providers["deepseek"]
    
    # 验证 Base URL
    assert "api.deepseek.com" in str(deepseek_provider.client.base_url)
    
    # Mock 它的 chat_json 方法
    expected_resp = {"intent": "AGG", "metrics": [], "dimensions": []}
    deepseek_provider.chat_json = AsyncMock(return_value=expected_resp)
    
    # 调用上层接口
    messages = [{"role": "user", "content": "hi"}]
    res = await client.generate_plan(messages=messages)
    
    # 验证调用了 deepseek provider 的 chat_json
    deepseek_provider.chat_json.assert_awaited_once()
    
    # 获取调用参数
    args, kwargs = deepseek_provider.chat_json.await_args
    
    # 验证传给底层的是 deepseek-reasoner
    assert kwargs["model"] == "deepseek-reasoner"
    assert kwargs["messages"] == messages
    assert res == expected_resp


# ============================================================
# Test Case 4: Stage 2 Integration with get_ai_client
# ============================================================

@pytest.mark.asyncio
@patch("stages.stage2_plan_generation.get_ai_client")
@patch("stages.stage2_plan_generation.get_pipeline_config")
async def test_stage2_integration_with_ai_client(mock_get_pipeline_config, mock_get_ai_client):
    """测试 stage2_plan_generation 正确使用 get_ai_client 和 generate_plan"""
    # 导入要测试的函数
    from stages.stage2_plan_generation import process_subquery
    
    # 创建 mock AI client
    mock_ai_client = MagicMock()
    mock_ai_client.generate_plan = AsyncMock(return_value={
        "intent": "AGG",
        "metrics": [],
        "dimensions": [],
        "filters": [],
        "order_by": []
    })
    
    # 配置 patched 函数
    mock_get_ai_client.return_value = mock_ai_client
    
    # Mock pipeline config
    mock_config = MagicMock()
    mock_config.vector_search_top_k = 20
    mock_config.similarity_threshold = 0.4
    mock_config.max_term_recall = 20
    mock_get_pipeline_config.return_value = mock_config
    
    # 准备测试数据
    # sub_query: 使用真实的 SubQueryItem Pydantic 模型实例
    sub_query = SubQueryItem(id="q1", description="test subquery")
    
    # context: 使用真实的 RequestContext Pydantic 模型实例
    context = RequestContext(
        user_id="u1",
        role_id="ROLE_TEST",
        request_id="req-123",
        current_date=date.today()
    )
    
    # registry: 使用 MagicMock，提供所有需要的方法
    mock_registry = MagicMock()
    mock_registry.get_allowed_ids.return_value = set()
    mock_registry.keyword_index = {}  # 空的关键词索引
    mock_registry.search_similar_terms = AsyncMock(return_value=[])  # 空的向量搜索结果
    mock_registry.get_term.return_value = None  # 简化：不返回术语定义
    
    # 调用 process_subquery
    # 注意：process_subquery 可能会抛出异常（因为其他依赖未完全 mock），
    # 但我们可以验证它调用了 get_ai_client 和 generate_plan
    try:
        await process_subquery(sub_query, context, mock_registry)
    except Exception:
        # 忽略其他异常，我们只关心 get_ai_client 和 generate_plan 的调用
        pass
    
    # 断言：get_ai_client 被调用一次
    mock_get_ai_client.assert_called_once()
    
    # 断言：generate_plan 被调用一次
    mock_ai_client.generate_plan.assert_awaited_once()

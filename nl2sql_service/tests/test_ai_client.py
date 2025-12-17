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
    QWEN_API_KEY = "fake-qwen-key"
    QWEN_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/"


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


def test_openai_provider_disables_unreachable_proxy(monkeypatch):
    """
    配置了不可达代理时应 fail-open 自动禁用，避免本地开发/CI 直接炸掉。
    """
    monkeypatch.setenv("PROXY_MODE", "explicit")
    monkeypatch.setenv("PROXY_STRICT", "0")
    monkeypatch.setenv("OPENAI_PROXY", "http://127.0.0.1:1")
    provider = OpenAIProvider(api_key="fake-openai-key", base_url="https://api.openai.com/v1", provider_name="openai")
    # 不可达代理应被禁用
    # OpenAIProvider 会记录 has_proxy=False（通过日志），这里验证初始化不抛异常即可
    assert provider.client is not None


def test_openai_provider_strict_proxy_raises(monkeypatch):
    """严格模式下，代理不可达应直接报错提示启动代理。"""
    monkeypatch.setenv("PROXY_MODE", "explicit")
    monkeypatch.setenv("PROXY_STRICT", "1")
    monkeypatch.setenv("OPENAI_PROXY", "http://127.0.0.1:1")
    with pytest.raises(ConnectionError):
        OpenAIProvider(api_key="fake-openai-key", base_url="https://api.openai.com/v1", provider_name="openai")


def test_proxy_mode_none_ignores_system_env_proxy(monkeypatch):
    """
    系统 env 存在 HTTP_PROXY/HTTPS_PROXY，但 PROXY_MODE=none 时必须 trust_env=False 且不传 proxy。
    """
    # 模拟系统代理（根因环境）
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:7897")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:7897")
    monkeypatch.setenv("PROXY_MODE", "none")
    monkeypatch.setenv("PROXY_STRICT", "0")

    captured = {}

    import httpx
    real_init = httpx.AsyncClient.__init__

    def _spy_init(self, *args, **kwargs):
        captured["kwargs"] = dict(kwargs)
        return real_init(self, *args, **kwargs)

    with patch("httpx.AsyncClient.__init__", new=_spy_init):
        provider = OpenAIProvider(api_key="fake-openai-key", base_url="https://api.openai.com/v1", provider_name="openai")
        assert provider.client is not None

    assert captured["kwargs"].get("trust_env") is False
    assert "proxy" not in captured["kwargs"]


def test_unreachable_explicit_proxy_strict_false_downgrades_and_disables_env(monkeypatch):
    """
    OPENAI_PROXY 不可达 + strict=false：必须降级直连，并强制 trust_env=False（避免再被 HTTP_PROXY 劫持）。
    """
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:7897")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:7897")
    monkeypatch.setenv("PROXY_MODE", "explicit")
    monkeypatch.setenv("PROXY_STRICT", "0")
    monkeypatch.setenv("OPENAI_PROXY", "http://127.0.0.1:1")

    captured = {}
    import httpx
    real_init = httpx.AsyncClient.__init__

    def _spy_init(self, *args, **kwargs):
        captured["kwargs"] = dict(kwargs)
        return real_init(self, *args, **kwargs)

    with patch("httpx.AsyncClient.__init__", new=_spy_init):
        provider = OpenAIProvider(api_key="fake-openai-key", base_url="https://api.openai.com/v1", provider_name="openai")
        assert provider.client is not None

    assert captured["kwargs"].get("trust_env") is False
    assert "proxy" not in captured["kwargs"]


def test_unreachable_explicit_proxy_strict_true_error_message_contains_proxy(monkeypatch):
    """OPENAI_PROXY 不可达 + strict=true：错误信息必须包含 proxy_url 与建议。"""
    monkeypatch.setenv("PROXY_MODE", "explicit")
    monkeypatch.setenv("PROXY_STRICT", "1")
    monkeypatch.setenv("OPENAI_PROXY", "http://127.0.0.1:1")
    with pytest.raises(ConnectionError) as exc:
        OpenAIProvider(api_key="fake-openai-key", base_url="https://api.openai.com/v1", provider_name="openai")
    msg = str(exc.value)
    assert "OPENAI_PROXY" in msg
    assert "http://127.0.0.1:1" in msg


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


def test_qwen_routing_is_correct():
    """验证 Qwen provider 正确初始化，Base URL 指向 dashscope.aliyuncs.com"""
    # 创建包含 Qwen 配置的 AIClient
    config = {
        "default_provider": "openai",
        "providers": {
            "openai": {
                "api_key": "fake-openai-key",
                "base_url": None,
                "type": "openai",
            },
            "jina": {
                "api_key": "fake-jina-key",
                "type": "jina",
            },
            "qwen": {
                "api_key": "fake-qwen-key",
                "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
                "type": "openai",  # Qwen 使用 OpenAI 兼容的 API
            },
        },
        "model_mapping": {
            "plan_generation": {
                "provider": "qwen",
                "model": "qwen-max"
            },
            "embedding": {
                "provider": "jina",
                "model": "jina-embeddings-v3"
            },
        },
    }
    
    client = AIClient(config=config)
    
    # 验证 qwen provider 被初始化
    assert "qwen" in client._providers
    
    # 解析 plan_generation（配置指向 qwen）
    provider, model = client._resolve_model("plan_generation")
    
    # 验证使用的是 OpenAIProvider 适配器（架构设计要求）
    assert isinstance(provider, OpenAIProvider)
    
    # 关键验证：验证 Base URL 是否真的切到了 Qwen
    assert "dashscope" in str(provider.client.base_url)
    assert provider.client.api_key == "fake-qwen-key"
    
    # 验证模型名称
    assert model == "qwen-max"


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


@pytest.mark.asyncio
async def test_generate_plan_uses_qwen_when_configured():
    """验证 generate_plan 方法在配置指向 qwen 时调用 qwen provider"""
    # 创建包含 Qwen 配置的 AIClient
    config = {
        "default_provider": "openai",
        "providers": {
            "openai": {
                "api_key": "fake-openai-key",
                "base_url": "https://api.openai.com/v1",
                "type": "openai",
            },
            "qwen": {
                "api_key": "fake-qwen-key",
                "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
                "type": "openai",
            },
        },
        "model_mapping": {
            "plan_generation": {
                "provider": "qwen",
                "model": "qwen-max"
            },
        },
    }
    
    client = AIClient(config=config)
    
    # 获取 qwen provider 实例
    qwen_provider = client._providers["qwen"]
    
    # 验证 Base URL
    assert "dashscope" in str(qwen_provider.client.base_url)
    
    # Mock 它的 chat_json 方法
    expected_resp = {"intent": "AGG", "metrics": [], "dimensions": []}
    qwen_provider.chat_json = AsyncMock(return_value=expected_resp)
    
    # 调用上层接口
    messages = [{"role": "user", "content": "hi"}]
    res = await client.generate_plan(messages=messages)
    
    # 验证调用了 qwen provider 的 chat_json
    qwen_provider.chat_json.assert_awaited_once()
    
    # 获取调用参数
    args, kwargs = qwen_provider.chat_json.await_args
    
    # 验证传给底层的是 qwen-max
    assert kwargs["model"] == "qwen-max"
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


# ============================================================
# Test Case 5: 补充缺失的测试场景
# ============================================================

def test_routing_query_decomposition_to_openai():
    """测试 query_decomposition usage_key 路由到 OpenAIProvider"""
    # 创建 AIClient 实例
    settings = FakeSettings()
    client = AIClient.init_from_settings(settings)
    
    # 调用路由解析器
    provider, model = client._resolve_model("query_decomposition")
    
    # 断言：provider 是 OpenAIProvider 实例
    assert isinstance(provider, OpenAIProvider)
    
    # 断言：model 是 gpt-4o-mini（根据实际配置）
    assert model == "gpt-4o-mini"


def test_deepseek_base_url_from_init_from_settings():
    """验证 init_from_settings 中 deepseek provider 使用配置的 DEEPSEEK_BASE_URL"""
    # 创建包含 DeepSeek 配置的 FakeSettings
    settings = FakeSettings()
    
    # 调用 init_from_settings
    client = AIClient.init_from_settings(settings)
    
    # 验证 deepseek provider 被初始化
    assert "deepseek" in client._providers
    assert isinstance(client._providers["deepseek"], OpenAIProvider)
    
    # 关键验证：验证 Base URL 是配置的 DEEPSEEK_BASE_URL，而不是默认的 OpenAI URL
    # 注意：OpenAI 客户端库可能会自动规范化 URL（添加/移除尾随斜杠），所以需要规范化后再比较
    deepseek_provider = client._providers["deepseek"]
    assert "api.deepseek.com" in str(deepseek_provider.client.base_url)
    # 规范化 URL（去掉尾随斜杠）后再比较
    actual_url = str(deepseek_provider.client.base_url).rstrip('/')
    expected_url = settings.DEEPSEEK_BASE_URL.rstrip('/')
    assert actual_url == expected_url


def test_init_from_settings_includes_qwen_when_configured():
    """验证 init_from_settings 中 qwen provider 使用配置的 QWEN_BASE_URL"""
    # 创建包含 Qwen 配置的 FakeSettings
    settings = FakeSettings()
    
    # 调用 init_from_settings
    client = AIClient.init_from_settings(settings)
    
    # 验证 qwen provider 被初始化
    assert "qwen" in client._providers
    assert isinstance(client._providers["qwen"], OpenAIProvider)
    
    # 关键验证：验证 Base URL 是配置的 QWEN_BASE_URL，而不是默认的 OpenAI URL
    # 注意：OpenAI 客户端库可能会自动规范化 URL（添加/移除尾随斜杠），所以需要规范化后再比较
    qwen_provider = client._providers["qwen"]
    assert "dashscope" in str(qwen_provider.client.base_url)
    # 规范化 URL（去掉尾随斜杠）后再比较
    actual_url = str(qwen_provider.client.base_url).rstrip('/')
    expected_url = settings.QWEN_BASE_URL.rstrip('/')
    assert actual_url == expected_url


@pytest.mark.asyncio
async def test_generate_decomposition_calls_openai_provider_chat_json():
    """测试 generate_decomposition 正确调用 OpenAIProvider.chat_json"""
    # 创建 AIClient 实例
    settings = FakeSettings()
    client = AIClient.init_from_settings(settings)
    
    # 获取 OpenAI provider 实例
    openai_provider = client._providers["openai"]
    
    # 替换 chat_json 方法为 AsyncMock
    fake_decomposition = {"sub_queries": [{"id": "q1", "description": "test"}]}
    openai_provider.chat_json = AsyncMock(return_value=fake_decomposition)
    
    # 调用 generate_decomposition
    messages = [{"role": "user", "content": "test"}]
    result = await client.generate_decomposition(messages=messages)
    
    # 断言：chat_json 被调用一次
    openai_provider.chat_json.assert_awaited_once()
    
    # 获取调用参数
    args, kwargs = openai_provider.chat_json.await_args
    
    # 断言：kwargs 中的 messages 和 model 正确
    assert kwargs["messages"] == messages
    assert kwargs["model"] == "gpt-4o-mini"
    
    # 断言：返回值正确
    assert result == fake_decomposition


@pytest.mark.asyncio
async def test_call_model_plan_generation_routing():
    """测试 call_model 对 plan_generation 的正确路由"""
    # 创建 AIClient 实例
    settings = FakeSettings()
    client = AIClient.init_from_settings(settings)
    
    # 获取 OpenAI provider 实例
    openai_provider = client._providers["openai"]
    
    # 替换 chat_json 方法为 AsyncMock
    fake_plan = {"intent": "AGG", "metrics": []}
    openai_provider.chat_json = AsyncMock(return_value=fake_plan)
    
    # 调用 call_model
    messages = [{"role": "user", "content": "test"}]
    result = await client.call_model("plan_generation", messages=messages)
    
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
async def test_call_model_query_decomposition_routing():
    """测试 call_model 对 query_decomposition 的正确路由"""
    # 创建 AIClient 实例
    settings = FakeSettings()
    client = AIClient.init_from_settings(settings)
    
    # 获取 OpenAI provider 实例
    openai_provider = client._providers["openai"]
    
    # 替换 chat_json 方法为 AsyncMock
    fake_decomposition = {"sub_queries": [{"id": "q1", "description": "test"}]}
    openai_provider.chat_json = AsyncMock(return_value=fake_decomposition)
    
    # 调用 call_model
    messages = [{"role": "user", "content": "test"}]
    result = await client.call_model("query_decomposition", messages=messages)
    
    # 断言：chat_json 被调用一次
    openai_provider.chat_json.assert_awaited_once()
    
    # 获取调用参数
    args, kwargs = openai_provider.chat_json.await_args
    
    # 断言：kwargs 中的 messages 和 model 正确
    assert kwargs["messages"] == messages
    assert kwargs["model"] == "gpt-4o-mini"
    
    # 断言：返回值正确
    assert result == fake_decomposition


def test_call_model_with_unknown_usage_key():
    """测试 call_model 在未配置的 usage_key 时抛出合理异常，并包含可用 usage_key 列表"""
    # 创建 AIClient 实例
    settings = FakeSettings()
    client = AIClient.init_from_settings(settings)
    
    # 尝试使用未配置的 usage_key
    with pytest.raises(ValueError) as exc_info:
        client._resolve_model("unknown_usage_key")
    
    # 断言：异常消息包含 usage_key 信息
    error_message = str(exc_info.value)
    assert "unknown_usage_key" in error_message.lower()
    
    # 断言：异常消息包含可用 usage_key 列表
    assert "available" in error_message.lower() or "keys" in error_message.lower()
    
    # 验证异常消息中确实包含了可用的 usage_key
    # 根据默认配置，应该包含这些 key
    available_keys = list(client.config.get("model_mapping", {}).keys())
    assert len(available_keys) > 0
    # 异常消息应该提到这些可用的 key（至少提到一个）
    assert any(key in error_message for key in available_keys)


@pytest.mark.asyncio
async def test_call_model_embedding_routing():
    """测试 call_model 对 embedding 的正确路由"""
    # 创建 AIClient 实例
    settings = FakeSettings()
    client = AIClient.init_from_settings(settings)
    
    # 获取 Jina provider 实例
    jina_provider = client._providers["jina"]
    
    # 替换 embed 方法为 AsyncMock
    mock_embedding = [[0.1, 0.2, 0.3]]
    jina_provider.embed = AsyncMock(return_value=mock_embedding)
    
    # 调用 call_model
    texts = ["hello world"]
    result = await client.call_model("embedding", texts=texts)
    
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
async def test_call_model_with_missing_messages_raises_error():
    """测试 call_model 在缺少 messages 参数时抛出异常"""
    # 创建 AIClient 实例
    settings = FakeSettings()
    client = AIClient.init_from_settings(settings)
    
    # 尝试调用 call_model 但缺少 messages 参数
    with pytest.raises(ValueError) as exc_info:
        await client.call_model("plan_generation")
    
    # 断言：异常消息提到 messages 参数
    error_message = str(exc_info.value)
    assert "messages" in error_message.lower()


@pytest.mark.asyncio
async def test_call_model_with_missing_texts_raises_error():
    """测试 call_model 在缺少 texts 参数时抛出异常"""
    # 创建 AIClient 实例
    settings = FakeSettings()
    client = AIClient.init_from_settings(settings)
    
    # 尝试调用 call_model 但缺少 texts 参数
    with pytest.raises(ValueError) as exc_info:
        await client.call_model("embedding")
    
    # 断言：异常消息提到 texts 参数
    error_message = str(exc_info.value)
    assert "texts" in error_message.lower()

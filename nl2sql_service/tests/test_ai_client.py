"""
【简述】
验证 AIClient 多提供商架构的配置初始化、模型路由、代理连接韧性与 Stage 集成正确性。

【范围/不测什么】
- 不覆盖真实 AI 模型推理质量；仅验证路由逻辑、配置加载、方法委托与错误处理。

【用例概述】
- test_init_from_settings_produces_valid_client:
  -- 验证从 settings 正确初始化多提供商 AIClient 实例
- test_openai_provider_disables_unreachable_proxy:
  -- 验证不可达代理在非严格模式下自动禁用，避免阻塞
- test_openai_provider_strict_proxy_raises:
  -- 验证严格模式下不可达代理直接抛出 ConnectionError
- test_proxy_mode_none_ignores_system_env_proxy:
  -- 验证 PROXY_MODE=none 时忽略系统环境代理变量
- test_unreachable_explicit_proxy_strict_false_downgrades_and_disables_env:
  -- 验证显式代理不可达且非严格时降级直连并禁用 trust_env
- test_unreachable_explicit_proxy_strict_true_error_message_contains_proxy:
  -- 验证严格模式代理失败时错误消息包含代理地址与建议
- test_routing_embedding_to_jina:
  -- 验证 embedding usage_key 正确路由到 JinaProvider
- test_routing_plan_generation_to_openai:
  -- 验证 plan_generation usage_key 正确路由到 OpenAIProvider
- test_deepseek_routing_is_correct:
  -- 验证 DeepSeek provider 初始化与 Base URL 配置正确性
- test_qwen_routing_is_correct:
  -- 验证 Qwen provider 初始化与 Base URL 配置正确性
- test_init_with_deepseek_config:
  -- 验证通过 config 字典初始化包含 DeepSeek 的 AIClient
- test_get_embeddings_calls_jina_provider_embed:
  -- 验证 get_embeddings 正确委托给 JinaProvider.embed
- test_generate_plan_calls_openai_provider_chat_json:
  -- 验证 generate_plan 正确委托给 OpenAIProvider.chat_json
- test_generate_plan_uses_deepseek_when_configured:
  -- 验证配置指向 deepseek 时 generate_plan 调用 deepseek provider
- test_generate_plan_uses_qwen_when_configured:
  -- 验证配置指向 qwen 时 generate_plan 调用 qwen provider
- test_stage2_integration_with_ai_client:
  -- 验证 stage2_plan_generation 正确使用 get_ai_client 与 generate_plan
- test_routing_query_decomposition_to_openai:
  -- 验证 query_decomposition usage_key 正确路由到 OpenAIProvider
- test_deepseek_base_url_from_init_from_settings:
  -- 验证 init_from_settings 中 DeepSeek Base URL 配置生效
- test_init_from_settings_includes_qwen_when_configured:
  -- 验证 init_from_settings 中 Qwen Base URL 配置生效
- test_generate_decomposition_calls_openai_provider_chat_json:
  -- 验证 generate_decomposition 正确委托给 OpenAIProvider.chat_json
- test_call_model_plan_generation_routing:
  -- 验证 call_model 对 plan_generation 的正确路由
- test_call_model_query_decomposition_routing:
  -- 验证 call_model 对 query_decomposition 的正确路由
- test_call_model_with_unknown_usage_key:
  -- 验证未配置 usage_key 时抛出异常并提示可用列表
- test_call_model_embedding_routing:
  -- 验证 call_model 对 embedding 的正确路由
- test_call_model_with_missing_messages_raises_error:
  -- 验证 call_model 缺少 messages 参数时抛出异常
- test_call_model_with_missing_texts_raises_error:
  -- 验证 call_model 缺少 texts 参数时抛出异常
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

@pytest.mark.unit
def test_init_from_settings_produces_valid_client():
    """
    【测试目标】
    1. 验证 AIClient.init_from_settings 能从配置对象正确初始化多提供商实例

    【执行过程】
    1. 创建 FakeSettings 包含 openai/jina/deepseek/qwen 配置
    2. 调用 AIClient.init_from_settings(settings)
    3. 检查返回实例类型与 _providers 字典内容

    【预期结果】
    1. 返回 AIClient 实例
    2. _providers 包含 openai、jina、deepseek、qwen 键
    3. 各 provider 类型正确（OpenAIProvider 或 JinaProvider）
    4. DeepSeek Base URL 包含 api.deepseek.com
    """
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


@pytest.mark.unit
def test_openai_provider_disables_unreachable_proxy(monkeypatch):
    """
    【测试目标】
    1. 验证不可达代理在 PROXY_STRICT=0 时自动禁用，实现 fail-open 韧性

    【执行过程】
    1. 设置 PROXY_MODE=explicit, PROXY_STRICT=0
    2. 设置不可达代理 OPENAI_PROXY=http://127.0.0.1:1
    3. 初始化 OpenAIProvider

    【预期结果】
    1. 初始化不抛异常
    2. provider.client 不为 None
    """
    monkeypatch.setenv("PROXY_MODE", "explicit")
    monkeypatch.setenv("PROXY_STRICT", "0")
    monkeypatch.setenv("OPENAI_PROXY", "http://127.0.0.1:1")
    provider = OpenAIProvider(api_key="fake-openai-key", base_url="https://api.openai.com/v1", provider_name="openai")
    # 不可达代理应被禁用
    # OpenAIProvider 会记录 has_proxy=False（通过日志），这里验证初始化不抛异常即可
    assert provider.client is not None


@pytest.mark.unit
def test_openai_provider_strict_proxy_raises(monkeypatch):
    """
    【测试目标】
    1. 验证严格模式（PROXY_STRICT=1）下不可达代理直接抛出 ConnectionError

    【执行过程】
    1. 设置 PROXY_MODE=explicit, PROXY_STRICT=1
    2. 设置不可达代理 OPENAI_PROXY=http://127.0.0.1:1
    3. 初始化 OpenAIProvider

    【预期结果】
    1. 抛出 ConnectionError 异常
    """
    monkeypatch.setenv("PROXY_MODE", "explicit")
    monkeypatch.setenv("PROXY_STRICT", "1")
    monkeypatch.setenv("OPENAI_PROXY", "http://127.0.0.1:1")
    with pytest.raises(ConnectionError):
        OpenAIProvider(api_key="fake-openai-key", base_url="https://api.openai.com/v1", provider_name="openai")


@pytest.mark.unit
def test_proxy_mode_none_ignores_system_env_proxy(monkeypatch):
    """
    【测试目标】
    1. 验证 PROXY_MODE=none 时强制忽略系统环境代理变量

    【执行过程】
    1. 设置系统环境变量 HTTP_PROXY 和 HTTPS_PROXY
    2. 设置 PROXY_MODE=none, PROXY_STRICT=0
    3. spy httpx.AsyncClient.__init__ 捕获初始化参数
    4. 初始化 OpenAIProvider

    【预期结果】
    1. trust_env 为 False
    2. kwargs 中不包含 proxy 参数
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


@pytest.mark.unit
def test_unreachable_explicit_proxy_strict_false_downgrades_and_disables_env(monkeypatch):
    """
    【测试目标】
    1. 验证显式代理不可达时在非严格模式下降级直连并禁用 trust_env

    【执行过程】
    1. 设置系统代理 HTTP_PROXY 和 HTTPS_PROXY
    2. 设置 PROXY_MODE=explicit, PROXY_STRICT=0
    3. 设置不可达 OPENAI_PROXY=http://127.0.0.1:1
    4. spy httpx.AsyncClient.__init__ 捕获参数
    5. 初始化 OpenAIProvider

    【预期结果】
    1. trust_env 为 False（防止被系统代理劫持）
    2. kwargs 中不包含 proxy 参数
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


@pytest.mark.unit
def test_unreachable_explicit_proxy_strict_true_error_message_contains_proxy(monkeypatch):
    """
    【测试目标】
    1. 验证严格模式代理失败时错误消息包含代理地址与诊断建议

    【执行过程】
    1. 设置 PROXY_MODE=explicit, PROXY_STRICT=1
    2. 设置不可达 OPENAI_PROXY=http://127.0.0.1:1
    3. 初始化 OpenAIProvider 并捕获异常

    【预期结果】
    1. 抛出 ConnectionError
    2. 错误消息包含 "OPENAI_PROXY" 字符串
    3. 错误消息包含代理地址 "http://127.0.0.1:1"
    """
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

@pytest.mark.unit
def test_routing_embedding_to_jina():
    """
    【测试目标】
    1. 验证 embedding usage_key 正确路由到 JinaProvider 与 jina-embeddings-v3 模型

    【执行过程】
    1. 从 FakeSettings 初始化 AIClient
    2. 调用 _resolve_model("embedding")
    3. 检查返回的 provider 和 model

    【预期结果】
    1. provider 是 JinaProvider 实例
    2. model 为 "jina-embeddings-v3"
    3. Base URL 包含 "jina.ai"
    """
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


@pytest.mark.unit
def test_routing_plan_generation_to_openai():
    """
    【测试目标】
    1. 验证 plan_generation usage_key 正确路由到 OpenAIProvider 与 gpt-4o-mini 模型

    【执行过程】
    1. 禁用环境变量中的 DEEPSEEK_API_KEY 和 QWEN_API_KEY
    2. 设置 DEFAULT_LLM_PROVIDER=openai
    3. 从 FakeSettings 初始化 AIClient
    4. 调用 _resolve_model("plan_generation")

    【预期结果】
    1. provider 是 OpenAIProvider 实例
    2. model 为 "gpt-4o-mini"
    3. Base URL 包含 "api.openai.com"
    """
    # 强制禁用环境变量里的 deepseek/qwen（main.py 会 load_dotenv，CI/本地可能有真实值）
    # 这里必须覆盖 env，否则 init_from_settings 会自动优先 deepseek。
    import os
    with patch.dict(
        os.environ,
        {"DEEPSEEK_API_KEY": "", "QWEN_API_KEY": "", "DEFAULT_LLM_PROVIDER": "openai"},
        clear=False,
    ):
        # 创建 AIClient 实例
        settings = FakeSettings()
        # 该用例只验证 OpenAI 路由：禁用 DeepSeek/Qwen 自动优先级，避免 drift
        settings.DEEPSEEK_API_KEY = None
        settings.QWEN_API_KEY = None
        settings.OPENAI_BASE_URL = "https://api.openai.com/v1"
        client = AIClient.init_from_settings(settings)
        
        # 调用路由解析器
        provider, model = client._resolve_model("plan_generation")
        
        # 断言：provider 是 OpenAIProvider 实例
        assert isinstance(provider, OpenAIProvider)
        
        # 断言：model 是 gpt-4o-mini（根据实际配置）
        assert model == "gpt-4o-mini"
        
        # 断言：Base URL 验证（OpenAI 默认使用官方 API 或 None）
        # 如果设置了 base_url，应该包含 api.openai.com
        assert "api.openai.com" in str(provider.client.base_url)


@pytest.mark.unit
def test_deepseek_routing_is_correct():
    """
    【测试目标】
    1. 验证 DeepSeek provider 正确初始化且 Base URL 指向 api.deepseek.com

    【执行过程】
    1. 创建 config 字典包含 deepseek provider 配置
    2. 初始化 AIClient(config=config)
    3. 调用 _resolve_model("plan_generation")
    4. 检查 provider 类型与 Base URL

    【预期结果】
    1. _providers 包含 deepseek 键
    2. provider 是 OpenAIProvider 实例
    3. Base URL 包含 "api.deepseek.com"
    4. model 为 "deepseek-reasoner"
    """
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


@pytest.mark.unit
def test_qwen_routing_is_correct():
    """
    【测试目标】
    1. 验证 Qwen provider 正确初始化且 Base URL 指向 dashscope.aliyuncs.com

    【执行过程】
    1. 创建 config 字典包含 qwen provider 配置
    2. 初始化 AIClient(config=config)
    3. 调用 _resolve_model("plan_generation")
    4. 检查 provider 类型与 Base URL

    【预期结果】
    1. _providers 包含 qwen 键
    2. provider 是 OpenAIProvider 实例
    3. Base URL 包含 "dashscope"
    4. model 为 "qwen-max"
    """
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


@pytest.mark.unit
def test_init_with_deepseek_config():
    """
    【测试目标】
    1. 验证通过 config 字典初始化包含 DeepSeek 的 AIClient 成功

    【执行过程】
    1. 创建包含 openai/deepseek/jina 的 config 字典
    2. 初始化 AIClient(config=config)
    3. 检查所有 provider 实例类型与 Base URL

    【预期结果】
    1. _providers 包含 openai、deepseek、jina
    2. 各 provider 类型正确
    3. deepseek Base URL 包含 "api.deepseek.com"
    4. openai Base URL 包含 "api.openai.com"
    """
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

@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_embeddings_calls_jina_provider_embed():
    """
    【测试目标】
    1. 验证 get_embeddings 正确委托给 JinaProvider.embed 方法

    【执行过程】
    1. 初始化 AIClient 并 mock JinaProvider.embed
    2. 调用 client.get_embeddings(texts=["hello"])
    3. 验证 embed 被调用且参数正确

    【预期结果】
    1. jina_provider.embed 被调用一次
    2. 传入参数 texts=["hello"], model="jina-embeddings-v3"
    3. 返回 mock 的 embedding 结果
    """
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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_generate_plan_calls_openai_provider_chat_json():
    """
    【测试目标】
    1. 验证 generate_plan 正确委托给 OpenAIProvider.chat_json 方法

    【执行过程】
    1. 禁用 DEEPSEEK/QWEN 环境变量，强制使用 openai
    2. 初始化 AIClient 并 mock OpenAIProvider.chat_json
    3. 调用 client.generate_plan(messages=[...])
    4. 验证 chat_json 被调用且参数正确

    【预期结果】
    1. openai_provider.chat_json 被调用一次
    2. 传入参数 messages 正确，model="gpt-4o-mini"
    3. 返回 mock 的 plan 结果
    """
    import os
    with patch.dict(
        os.environ,
        {"DEEPSEEK_API_KEY": "", "QWEN_API_KEY": "", "DEFAULT_LLM_PROVIDER": "openai"},
        clear=False,
    ):
        # 创建 AIClient 实例
        settings = FakeSettings()
        # 强制走 OpenAI（避免默认 deepseek 导致未 mock 时发真实请求）
        settings.DEEPSEEK_API_KEY = None
        settings.QWEN_API_KEY = None
        settings.OPENAI_BASE_URL = "https://api.openai.com/v1"
        client = AIClient.init_from_settings(settings)
        
        # 获取 OpenAI provider 实例
        openai_provider = client._providers["openai"]
        
        # 验证 OpenAI provider 的 Base URL（在 mock 之前）
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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_generate_plan_uses_deepseek_when_configured():
    """
    【测试目标】
    1. 验证配置指向 deepseek 时 generate_plan 调用 deepseek provider

    【执行过程】
    1. 创建 model_mapping 指向 deepseek 的 config
    2. 初始化 AIClient 并 mock deepseek_provider.chat_json
    3. 调用 client.generate_plan(messages=[...])
    4. 验证调用参数

    【预期结果】
    1. deepseek_provider.chat_json 被调用一次
    2. 传入 model="deepseek-reasoner"
    3. 返回 mock 的响应
    """
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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_generate_plan_uses_qwen_when_configured():
    """
    【测试目标】
    1. 验证配置指向 qwen 时 generate_plan 调用 qwen provider

    【执行过程】
    1. 创建 model_mapping 指向 qwen 的 config
    2. 初始化 AIClient 并 mock qwen_provider.chat_json
    3. 调用 client.generate_plan(messages=[...])
    4. 验证调用参数

    【预期结果】
    1. qwen_provider.chat_json 被调用一次
    2. 传入 model="qwen-max"
    3. 返回 mock 的响应
    """
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

@pytest.mark.integration
@pytest.mark.asyncio
@patch("stages.stage2_plan_generation.get_ai_client")
@patch("stages.stage2_plan_generation.get_pipeline_config")
async def test_stage2_integration_with_ai_client(mock_get_pipeline_config, mock_get_ai_client):
    """
    【测试目标】
    1. 验证 stage2_plan_generation 正确使用 get_ai_client 与 generate_plan

    【执行过程】
    1. mock get_ai_client 和 get_pipeline_config
    2. 准备 SubQueryItem 和 RequestContext
    3. mock registry 提供必要方法
    4. 调用 process_subquery
    5. 忽略其他依赖未完全 mock 的异常

    【预期结果】
    1. get_ai_client 被调用一次
    2. mock_ai_client.generate_plan 被调用一次
    """
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

@pytest.mark.unit
def test_routing_query_decomposition_to_openai():
    """
    【测试目标】
    1. 验证 query_decomposition usage_key 正确路由到 OpenAIProvider

    【执行过程】
    1. 禁用 DEEPSEEK/QWEN 环境变量，强制 openai
    2. 初始化 AIClient
    3. 调用 _resolve_model("query_decomposition")

    【预期结果】
    1. provider 是 OpenAIProvider 实例
    2. model 为 "gpt-4o-mini"
    """
    import os
    with patch.dict(
        os.environ,
        {"DEEPSEEK_API_KEY": "", "QWEN_API_KEY": "", "DEFAULT_LLM_PROVIDER": "openai"},
        clear=False,
    ):
        # 创建 AIClient 实例
        settings = FakeSettings()
        # 该用例只验证 OpenAI 路由：禁用 DeepSeek/Qwen 自动优先级，避免 drift
        settings.DEEPSEEK_API_KEY = None
        settings.QWEN_API_KEY = None
        client = AIClient.init_from_settings(settings)
        
        # 调用路由解析器
        provider, model = client._resolve_model("query_decomposition")
        
        # 断言：provider 是 OpenAIProvider 实例
        assert isinstance(provider, OpenAIProvider)
        
        # 断言：model 是 gpt-4o-mini（根据实际配置）
        assert model == "gpt-4o-mini"


@pytest.mark.unit
def test_deepseek_base_url_from_init_from_settings():
    """
    【测试目标】
    1. 验证 init_from_settings 中 deepseek provider Base URL 使用 DEEPSEEK_BASE_URL

    【执行过程】
    1. 创建包含 DeepSeek 配置的 FakeSettings
    2. 调用 AIClient.init_from_settings(settings)
    3. 检查 deepseek_provider.client.base_url

    【预期结果】
    1. deepseek provider 被初始化
    2. Base URL 包含 "api.deepseek.com"
    3. 规范化后的 URL 与 settings.DEEPSEEK_BASE_URL 一致
    """
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


@pytest.mark.unit
def test_init_from_settings_includes_qwen_when_configured():
    """
    【测试目标】
    1. 验证 init_from_settings 中 qwen provider Base URL 使用 QWEN_BASE_URL

    【执行过程】
    1. 创建包含 Qwen 配置的 FakeSettings
    2. 调用 AIClient.init_from_settings(settings)
    3. 检查 qwen_provider.client.base_url

    【预期结果】
    1. qwen provider 被初始化
    2. Base URL 包含 "dashscope"
    3. 规范化后的 URL 与 settings.QWEN_BASE_URL 一致
    """
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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_generate_decomposition_calls_openai_provider_chat_json():
    """
    【测试目标】
    1. 验证 generate_decomposition 正确委托给 OpenAIProvider.chat_json

    【执行过程】
    1. 禁用 DEEPSEEK/QWEN 环境变量
    2. 初始化 AIClient 并 mock OpenAIProvider.chat_json
    3. 调用 client.generate_decomposition(messages=[...])
    4. 验证调用参数

    【预期结果】
    1. openai_provider.chat_json 被调用一次
    2. 传入 messages 正确，model="gpt-4o-mini"
    3. 返回 mock 的 decomposition 结果
    """
    import os
    with patch.dict(
        os.environ,
        {"DEEPSEEK_API_KEY": "", "QWEN_API_KEY": "", "DEFAULT_LLM_PROVIDER": "openai"},
        clear=False,
    ):
        # 创建 AIClient 实例
        settings = FakeSettings()
        # 强制走 OpenAI（避免默认 deepseek 导致未 mock 时发真实请求）
        settings.DEEPSEEK_API_KEY = None
        settings.QWEN_API_KEY = None
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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_model_plan_generation_routing():
    """
    【测试目标】
    1. 验证 call_model 对 plan_generation 的正确路由与委托

    【执行过程】
    1. 禁用 DEEPSEEK/QWEN 环境变量
    2. 初始化 AIClient 并 mock OpenAIProvider.chat_json
    3. 调用 client.call_model("plan_generation", messages=[...])
    4. 验证调用参数

    【预期结果】
    1. openai_provider.chat_json 被调用一次
    2. 传入 messages 正确，model="gpt-4o-mini"
    3. 返回 mock 的 plan 结果
    """
    import os
    with patch.dict(
        os.environ,
        {"DEEPSEEK_API_KEY": "", "QWEN_API_KEY": "", "DEFAULT_LLM_PROVIDER": "openai"},
        clear=False,
    ):
        # 创建 AIClient 实例
        settings = FakeSettings()
        # 强制走 OpenAI（避免默认 deepseek 导致未 mock 时发真实请求）
        settings.DEEPSEEK_API_KEY = None
        settings.QWEN_API_KEY = None
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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_model_query_decomposition_routing():
    """
    【测试目标】
    1. 验证 call_model 对 query_decomposition 的正确路由与委托

    【执行过程】
    1. 禁用 DEEPSEEK/QWEN 环境变量
    2. 初始化 AIClient 并 mock OpenAIProvider.chat_json
    3. 调用 client.call_model("query_decomposition", messages=[...])
    4. 验证调用参数

    【预期结果】
    1. openai_provider.chat_json 被调用一次
    2. 传入 messages 正确，model="gpt-4o-mini"
    3. 返回 mock 的 decomposition 结果
    """
    import os
    with patch.dict(
        os.environ,
        {"DEEPSEEK_API_KEY": "", "QWEN_API_KEY": "", "DEFAULT_LLM_PROVIDER": "openai"},
        clear=False,
    ):
        # 创建 AIClient 实例
        settings = FakeSettings()
        # 强制走 OpenAI（避免默认 deepseek 导致未 mock 时发真实请求）
        settings.DEEPSEEK_API_KEY = None
        settings.QWEN_API_KEY = None
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


@pytest.mark.unit
def test_call_model_with_unknown_usage_key():
    """
    【测试目标】
    1. 验证未配置 usage_key 时抛出异常并提示可用 usage_key 列表

    【执行过程】
    1. 初始化 AIClient
    2. 调用 _resolve_model("unknown_usage_key")
    3. 捕获 ValueError 异常并检查消息内容

    【预期结果】
    1. 抛出 ValueError 异常
    2. 错误消息包含 "unknown_usage_key"
    3. 错误消息包含可用 usage_key 提示
    4. 错误消息包含至少一个可用 key
    """
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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_model_embedding_routing():
    """
    【测试目标】
    1. 验证 call_model 对 embedding 的正确路由与委托

    【执行过程】
    1. 初始化 AIClient
    2. mock JinaProvider.embed
    3. 调用 client.call_model("embedding", texts=["hello world"])
    4. 验证调用参数

    【预期结果】
    1. jina_provider.embed 被调用一次
    2. 传入 texts=["hello world"], model="jina-embeddings-v3"
    3. 返回 mock 的 embedding 结果
    """
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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_model_with_missing_messages_raises_error():
    """
    【测试目标】
    1. 验证 call_model 在缺少 messages 参数时抛出 ValueError

    【执行过程】
    1. 初始化 AIClient
    2. 调用 client.call_model("plan_generation") 但不传 messages
    3. 捕获 ValueError 异常

    【预期结果】
    1. 抛出 ValueError 异常
    2. 错误消息包含 "messages" 字符串
    """
    # 创建 AIClient 实例
    settings = FakeSettings()
    client = AIClient.init_from_settings(settings)
    
    # 尝试调用 call_model 但缺少 messages 参数
    with pytest.raises(ValueError) as exc_info:
        await client.call_model("plan_generation")
    
    # 断言：异常消息提到 messages 参数
    error_message = str(exc_info.value)
    assert "messages" in error_message.lower()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_call_model_with_missing_texts_raises_error():
    """
    【测试目标】
    1. 验证 call_model 在缺少 texts 参数时抛出 ValueError

    【执行过程】
    1. 初始化 AIClient
    2. 调用 client.call_model("embedding") 但不传 texts
    3. 捕获 ValueError 异常

    【预期结果】
    1. 抛出 ValueError 异常
    2. 错误消息包含 "texts" 字符串
    """
    # 创建 AIClient 实例
    settings = FakeSettings()
    client = AIClient.init_from_settings(settings)
    
    # 尝试调用 call_model 但缺少 texts 参数
    with pytest.raises(ValueError) as exc_info:
        await client.call_model("embedding")
    
    # 断言：异常消息提到 texts 参数
    error_message = str(exc_info.value)
    assert "texts" in error_message.lower()

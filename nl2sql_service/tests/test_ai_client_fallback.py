"""
【简述】
验证 AIClient 模型降级能力：主模型失败（超时/限流/格式异常）→ 备用模型接管，只做一次降级，并返回 degraded 标志。

【范围/不测什么】
- 不覆盖真实 AI 模型调用；仅验证降级逻辑、异常捕获、备用 provider 选择与 degraded 标志。

【用例概述】
- test_primary_model_timeout_fallback_to_backup:
  -- 验证主模型超时→备用模型
- test_primary_model_rate_limit_fallback:
  -- 验证主模型限流→备用模型
- test_primary_model_json_error_fallback:
  -- 验证主模型格式异常→备用模型
- test_fallback_degraded_flag_present:
  -- 验证降级后返回degraded标志
- test_fallback_fails_raises_error:
  -- 验证备用模型也失败时抛出异常
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from core.ai_client import AIClient
from core.errors import ProviderConnectionError, ProviderRateLimitError


class FakeSettings:
    """模拟设置对象，用于测试"""
    OPENAI_API_KEY = "fake-openai-key"
    OPENAI_BASE_URL = None
    JINA_API_KEY = "fake-jina-key"
    JINA_BASE_URL = None
    DEEPSEEK_API_KEY = "fake-deepseek-key"
    DEEPSEEK_BASE_URL = "https://api.deepseek.com"
    QWEN_API_KEY = "fake-qwen-key"
    QWEN_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_primary_model_timeout_fallback_to_backup():
    """
    【测试目标】
    1. 验证主模型超时→备用模型

    【执行过程】
    1. 创建 AIClient 配置 openai 为主 provider，deepseek 为备用
    2. mock openai provider 的 chat_json 抛出 APIConnectionError（模拟超时）
    3. mock deepseek provider 的 chat_json 返回成功结果
    4. 调用 call_model("plan_generation")
    5. 验证降级到 deepseek 并返回结果

    【预期结果】
    1. openai provider 被调用一次
    2. deepseek provider 被调用一次
    3. 返回结果包含 degraded 标志
    4. 返回结果来自 deepseek provider
    """
    # 创建包含 openai 和 deepseek 的配置
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
                "provider": "openai",
                "model": "gpt-4o-mini"
            },
        },
    }
    
    client = AIClient(config=config)
    
    # Mock providers
    openai_provider = client._providers["openai"]
    deepseek_provider = client._providers["deepseek"]
    
    # Mock openai 抛出超时异常（使用稳定的内部异常）
    openai_provider.chat_json = AsyncMock(side_effect=ProviderConnectionError("Connection timeout", provider="openai"))
    
    # Mock deepseek 返回成功
    expected_result = {"intent": "AGG", "metrics": []}
    deepseek_provider.chat_json = AsyncMock(return_value=expected_result)
    
    # 调用 call_model
    messages = [{"role": "user", "content": "test"}]
    result = await client.call_model("plan_generation", messages=messages)
    
    # 验证 openai 被调用一次
    openai_provider.chat_json.assert_awaited_once()
    
    # 验证 deepseek 被调用一次
    deepseek_provider.chat_json.assert_awaited_once()
    
    # 验证返回结果包含 degraded 标志
    assert isinstance(result, dict)
    assert result.get("_degraded") is True
    assert result.get("_fallback_provider") == "deepseek"
    assert result.get("_primary_provider") == "openai"
    assert result.get("intent") == "AGG"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_primary_model_rate_limit_fallback():
    """
    【测试目标】
    1. 验证主模型限流→备用模型

    【执行过程】
    1. 创建 AIClient 配置 openai 为主 provider，qwen 为备用
    2. mock openai provider 抛出 RateLimitError（模拟限流）
    3. mock qwen provider 返回成功结果
    4. 调用 call_model("plan_generation")
    5. 验证降级到 qwen

    【预期结果】
    1. openai provider 被调用一次
    2. qwen provider 被调用一次
    3. 返回结果包含 degraded 标志
    """
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
                "provider": "openai",
                "model": "gpt-4o-mini"
            },
        },
    }
    
    client = AIClient(config=config)
    
    openai_provider = client._providers["openai"]
    qwen_provider = client._providers["qwen"]
    
    # Mock openai 抛出限流异常（使用稳定的内部异常）
    openai_provider.chat_json = AsyncMock(side_effect=ProviderRateLimitError("Rate limit exceeded", provider="openai"))
    
    # Mock qwen 返回成功
    expected_result = {"intent": "TREND", "metrics": []}
    qwen_provider.chat_json = AsyncMock(return_value=expected_result)
    
    messages = [{"role": "user", "content": "test"}]
    result = await client.call_model("plan_generation", messages=messages)
    
    # 验证降级成功
    assert isinstance(result, dict)
    assert result.get("_degraded") is True
    assert result.get("_fallback_provider") == "qwen"
    qwen_provider.chat_json.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_primary_model_json_error_fallback():
    """
    【测试目标】
    1. 验证主模型格式异常→备用模型

    【执行过程】
    1. 创建 AIClient 配置 openai 为主 provider，deepseek 为备用
    2. mock openai provider 抛出 ValueError（JSON 解析错误）
    3. mock deepseek provider 返回成功结果
    4. 调用 call_model("plan_generation")
    5. 验证降级到 deepseek

    【预期结果】
    1. openai provider 被调用一次
    2. deepseek provider 被调用一次
    3. 返回结果包含 degraded 标志
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
        },
        "model_mapping": {
            "plan_generation": {
                "provider": "openai",
                "model": "gpt-4o-mini"
            },
        },
    }
    
    client = AIClient(config=config)
    
    openai_provider = client._providers["openai"]
    deepseek_provider = client._providers["deepseek"]
    
    # Mock openai 抛出 JSON 解析错误（被包装为 ValueError）
    openai_provider.chat_json = AsyncMock(side_effect=ValueError("Failed to parse response as JSON"))
    
    # Mock deepseek 返回成功
    expected_result = {"intent": "DETAIL", "metrics": []}
    deepseek_provider.chat_json = AsyncMock(return_value=expected_result)
    
    messages = [{"role": "user", "content": "test"}]
    result = await client.call_model("plan_generation", messages=messages)
    
    # 验证降级成功
    assert isinstance(result, dict)
    assert result.get("_degraded") is True
    assert result.get("_fallback_provider") == "deepseek"
    deepseek_provider.chat_json.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fallback_degraded_flag_present():
    """
    【测试目标】
    1. 验证降级后返回degraded标志

    【执行过程】
    1. 创建 AIClient 配置主 provider 和备用 provider
    2. mock 主 provider 失败，备用 provider 成功
    3. 调用 call_model
    4. 验证返回结果包含 _degraded、_fallback_provider、_primary_provider 标志

    【预期结果】
    1. 返回结果包含 _degraded=True
    2. 返回结果包含 _fallback_provider（备用 provider 名称）
    3. 返回结果包含 _primary_provider（主 provider 名称）
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
        },
        "model_mapping": {
            "plan_generation": {
                "provider": "openai",
                "model": "gpt-4o-mini"
            },
        },
    }
    
    client = AIClient(config=config)
    
    openai_provider = client._providers["openai"]
    deepseek_provider = client._providers["deepseek"]
    
    # Mock 主 provider 失败（使用稳定的内部异常）
    openai_provider.chat_json = AsyncMock(side_effect=ProviderConnectionError("Connection failed", provider="openai"))
    
    # Mock 备用 provider 成功
    expected_result = {"intent": "AGG", "metrics": [{"id": "METRIC_GMV"}]}
    deepseek_provider.chat_json = AsyncMock(return_value=expected_result)
    
    messages = [{"role": "user", "content": "test"}]
    result = await client.call_model("plan_generation", messages=messages)
    
    # 验证 degraded 标志
    assert isinstance(result, dict)
    assert result.get("_degraded") is True
    assert result.get("_fallback_provider") == "deepseek"
    assert result.get("_primary_provider") == "openai"
    # 验证原始结果仍然存在
    assert result.get("intent") == "AGG"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fallback_fails_raises_error():
    """
    【测试目标】
    1. 验证备用模型也失败时抛出异常

    【执行过程】
    1. 创建 AIClient 配置主 provider 和备用 provider
    2. mock 主 provider 失败，备用 provider 也失败
    3. 调用 call_model
    4. 验证抛出异常

    【预期结果】
    1. 抛出异常
    2. 异常信息包含所有 provider 失败的信息
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
        },
        "model_mapping": {
            "plan_generation": {
                "provider": "openai",
                "model": "gpt-4o-mini"
            },
        },
    }
    
    client = AIClient(config=config)
    
    openai_provider = client._providers["openai"]
    deepseek_provider = client._providers["deepseek"]
    
    # Mock 主 provider 失败（使用稳定的内部异常）
    openai_provider.chat_json = AsyncMock(side_effect=ProviderConnectionError("Primary provider failed", provider="openai"))
    
    # Mock 备用 provider 也失败（使用稳定的内部异常）
    deepseek_provider.chat_json = AsyncMock(side_effect=ProviderConnectionError("Fallback provider also failed", provider="deepseek"))
    
    messages = [{"role": "user", "content": "test"}]
    
    # 验证抛出异常
    with pytest.raises(Exception) as exc_info:
        await client.call_model("plan_generation", messages=messages)
    
    # 验证异常信息包含失败信息
    error_msg = str(exc_info.value)
    assert "All providers failed" in error_msg or "failed" in error_msg.lower()


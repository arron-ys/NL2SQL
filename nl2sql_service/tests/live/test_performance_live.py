"""
Live Performance Test Suite

测试真实外部服务（OpenAI/Jina）的性能指标。
需要真实的 API Key，如果 Key 不可用则跳过测试。
"""
import os
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from main import app
from tests.live.helpers import get_openai_api_key, get_jina_api_key, is_placeholder_key


# ============================================================
# Test Fixtures
# ============================================================


@pytest.fixture
def client():
    """创建 TestClient 实例"""
    return TestClient(app)


# ============================================================
# Skip Conditions
# ============================================================


def _should_skip_live_tests():
    """
    检查是否应该跳过 Live 测试
    
    根据 .env 中的 DEFAULT_LLM_PROVIDER 配置或自动选择逻辑（与 AIClient._default_config() 一致）
    检查对应的 LLM provider API Key 是否可用。
    """
    # 读取 LLM provider 配置（与 AIClient._default_config() 逻辑一致）
    default_llm_provider = os.getenv("DEFAULT_LLM_PROVIDER", "").lower()
    
    # 读取所有可能的 API Keys
    openai_key = os.getenv("OPENAI_API_KEY", "")
    deepseek_key = os.getenv("DEEPSEEK_API_KEY", "")
    qwen_key = os.getenv("QWEN_API_KEY", "")
    jina_key = os.getenv("JINA_API_KEY", "")
    
    # 确定实际使用的 LLM provider（与 AIClient._default_config() 逻辑一致）
    if default_llm_provider:
        # 验证指定的 provider 是否配置了 API Key
        if default_llm_provider == "deepseek" and not deepseek_key:
            # 如果指定了 deepseek 但没有 key，fallback 到自动选择
            default_llm_provider = ""
        elif default_llm_provider == "qwen" and not qwen_key:
            # 如果指定了 qwen 但没有 key，fallback 到自动选择
            default_llm_provider = ""
        elif default_llm_provider == "openai" and not openai_key:
            # 如果指定了 openai 但没有 key，fallback 到自动选择
            default_llm_provider = ""
        elif default_llm_provider not in ["openai", "deepseek", "qwen"]:
            # 无效的 provider，fallback 到自动选择
            default_llm_provider = ""
    
    # 如果没有明确指定或指定无效，使用自动选择逻辑（DeepSeek > Qwen > OpenAI）
    if not default_llm_provider:
        if deepseek_key:
            default_llm_provider = "deepseek"
        elif qwen_key:
            default_llm_provider = "qwen"
        else:
            default_llm_provider = "openai"
    
    # 根据确定的 provider 检查对应的 API Key
    if default_llm_provider == "deepseek":
        if not deepseek_key or is_placeholder_key(deepseek_key):
            return True, f"DEEPSEEK_API_KEY not available or is placeholder (DEFAULT_LLM_PROVIDER={default_llm_provider})"
    elif default_llm_provider == "qwen":
        if not qwen_key or is_placeholder_key(qwen_key):
            return True, f"QWEN_API_KEY not available or is placeholder (DEFAULT_LLM_PROVIDER={default_llm_provider})"
    else:  # openai
        if not openai_key or is_placeholder_key(openai_key):
            return True, f"OPENAI_API_KEY not available or is placeholder (DEFAULT_LLM_PROVIDER={default_llm_provider})"
    
    # Jina Key 可选，但如果提供了但为占位符，也跳过
    if jina_key and is_placeholder_key(jina_key):
        return True, "Jina API Key is placeholder"
    
    return False, None


# 在模块级别计算 skip 条件（用于装饰器）
_SKIP_LIVE_TESTS, _SKIP_REASON = _should_skip_live_tests()


# ============================================================
# 延迟测试
# ============================================================


class TestLatency:
    """测试请求延迟（真实外部服务）"""

    @pytest.mark.asyncio
    @pytest.mark.performance
    @pytest.mark.live
    @pytest.mark.slow
    @pytest.mark.skipif(
        _SKIP_LIVE_TESTS,
        reason=_SKIP_REASON or "Live services not available"
    )
    async def test_single_request_latency_p50(self, client):
        """测试单请求延迟 P50 < 2s（真实 LLM）"""
        
        latencies = []
        num_requests = 10

        for _ in range(num_requests):
            start_time = time.time()
            response = client.post(
                "/nl2sql/plan",
                json={
                    "question": "统计员工数量",
                    "user_id": "user_001",
                    "role_id": "ROLE_HR_HEAD",
                    "tenant_id": "tenant_001",
                },
            )
            end_time = time.time()
            latency = end_time - start_time
            latencies.append(latency)
            
            # 如果请求失败，记录但继续
            if response.status_code != 200:
                continue

        if len(latencies) == 0:
            pytest.skip("All requests failed, cannot measure latency")

        # 计算 P50
        latencies.sort()
        p50 = latencies[len(latencies) // 2]

        # P50 应该 < 2s
        assert p50 < 2.0, f"P50 latency {p50}s exceeds 2s threshold"

    @pytest.mark.asyncio
    @pytest.mark.performance
    @pytest.mark.live
    @pytest.mark.slow
    @pytest.mark.skipif(
        _SKIP_LIVE_TESTS,
        reason=_SKIP_REASON or "Live services not available"
    )
    async def test_single_request_latency_p95(self, client):
        """测试单请求延迟 P95 < 5s（真实 LLM）"""
        
        latencies = []
        num_requests = 20

        for _ in range(num_requests):
            start_time = time.time()
            response = client.post(
                "/nl2sql/plan",
                json={
                    "question": "统计员工数量",
                    "user_id": "user_001",
                    "role_id": "ROLE_HR_HEAD",
                    "tenant_id": "tenant_001",
                },
            )
            end_time = time.time()
            latency = end_time - start_time
            latencies.append(latency)
            
            # 如果请求失败，记录但继续
            if response.status_code != 200:
                continue

        if len(latencies) == 0:
            pytest.skip("All requests failed, cannot measure latency")

        # 计算 P95
        latencies.sort()
        p95_index = int(len(latencies) * 0.95)
        p95 = latencies[p95_index] if p95_index < len(latencies) else latencies[-1]

        # P95 应该 < 5s
        assert p95 < 5.0, f"P95 latency {p95}s exceeds 5s threshold"


# ============================================================
# 并发测试
# ============================================================


class TestConcurrency:
    """测试并发处理能力（真实外部服务）"""

    @pytest.mark.asyncio
    @pytest.mark.performance
    @pytest.mark.live
    @pytest.mark.slow
    @pytest.mark.skipif(
        _SKIP_LIVE_TESTS,
        reason=_SKIP_REASON or "Live services not available"
    )
    async def test_concurrent_requests_success_rate(self, client):
        """测试10并发请求，成功率 > 95%"""
        
        num_concurrent = 10
        request_data = {
            "question": "统计员工数量",
            "user_id": "user_001",
            "role_id": "ROLE_HR_HEAD",
            "tenant_id": "tenant_001",
        }

        def make_request():
            response = client.post("/nl2sql/plan", json=request_data, timeout=30)
            return response.status_code

        # 使用线程池执行并发请求
        with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            futures = [executor.submit(make_request) for _ in range(num_concurrent)]
            results = [future.result() for future in futures]

        # 计算成功率（200 为成功）
        success_count = sum(1 for code in results if code == 200)
        success_rate = success_count / num_concurrent

        # 成功率应该 > 95%
        assert success_rate > 0.95, f"Success rate {success_rate} is below 95% threshold"

    @pytest.mark.asyncio
    @pytest.mark.performance
    @pytest.mark.live
    @pytest.mark.slow
    @pytest.mark.skipif(
        _SKIP_LIVE_TESTS,
        reason=_SKIP_REASON or "Live services not available"
    )
    async def test_concurrent_requests_no_crash(self, client):
        """测试并发请求不会导致服务崩溃"""
        
        num_concurrent = 10
        request_data = {
            "question": "统计员工数量",
            "user_id": "user_001",
            "role_id": "ROLE_HR_HEAD",
            "tenant_id": "tenant_001",
        }

        def make_request():
            try:
                response = client.post("/nl2sql/plan", json=request_data, timeout=30)
                return response.status_code
            except Exception as e:
                return f"ERROR: {str(e)}"

        # 使用线程池执行并发请求
        with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            futures = [executor.submit(make_request) for _ in range(num_concurrent)]
            results = [future.result() for future in futures]

        # 所有请求都应该有响应（不应该是连接错误）
        error_count = sum(1 for result in results if isinstance(result, str) and "ERROR" in result)
        assert error_count == 0, f"{error_count} requests failed with errors"


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
    """检查是否应该跳过 Live 测试"""
    # 直接检查环境变量并使用 is_placeholder_key 判断
    openai_key_env = os.getenv("OPENAI_API_KEY", "")
    
    # 如果 OpenAI Key 缺失或为占位符，跳过测试
    if not openai_key_env or is_placeholder_key(openai_key_env):
        return True, "OpenAI API Key not available or is placeholder"
    
    # Jina Key 可选，但如果提供了但为占位符，也跳过
    jina_key_env = os.getenv("JINA_API_KEY", "")
    if jina_key_env and is_placeholder_key(jina_key_env):
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


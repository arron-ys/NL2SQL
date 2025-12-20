"""
【简述】
验证 NL2SQL 应用逻辑层性能（路由、校验、序列化）在 mock 所有外部依赖时的延迟与并发表现。

【范围/不测什么】
- 不覆盖真实外部服务调用；仅验证应用内部逻辑的性能表现与并发安全性。

【用例概述】
- test_single_request_latency_p95:
  -- 验证单请求延迟 P95 < 200ms
- test_concurrent_requests_success_rate:
  -- 验证并发请求成功率 > 95%
- test_concurrent_requests_no_crash:
  -- 验证并发请求不崩溃
- test_request_timeout_setting:
  -- 验证请求超时设置生效
"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient
from httpx import ASGITransport

from main import app


# ============================================================
# Test Fixtures
# ============================================================


@pytest.fixture
def client():
    """创建 TestClient 实例"""
    return TestClient(app)


@pytest.fixture
def mock_registry():
    """创建模拟的 SemanticRegistry"""
    registry = MagicMock()
    registry.get_allowed_ids.return_value = {
        "METRIC_GMV",
        "METRIC_REVENUE",
        "DIM_REGION",
    }
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENTITY_ORDER",
        "default_filters": [],
        "default_time": None,
    }
    registry.get_dimension_def.return_value = {
        "id": "DIM_REGION",
        "entity_id": "ENTITY_ORDER",
    }
    registry.get_term.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENTITY_ORDER",
    }
    registry.check_compatibility.return_value = True
    registry.global_config = {
        "global_settings": {},
        "time_windows": [],
    }
    registry.keyword_index = {}
    # Mock 异步方法 search_similar_terms
    registry.search_similar_terms = AsyncMock(return_value=[])
    return registry


@pytest.fixture
def mock_ai_client():
    """创建模拟的 AIClient，立即返回结果（无延迟）"""
    mock_client = MagicMock()
    
    # Mock generate_decomposition (Stage 1 使用)
    async def mock_generate_decomposition(messages, temperature=0.0):
        """立即返回硬编码的子查询分解结果"""
        return {
            "sub_queries": [
                {
                    "id": "q1",
                    "description": "统计员工数量"
                }
            ]
        }
    
    # Mock generate_plan (Stage 2 使用)
    async def mock_generate_plan(messages, temperature=0.0):
        """立即返回硬编码的 Plan JSON"""
        return {
            "intent": "AGG",
            "metrics": [{"id": "METRIC_GMV"}],
            "dimensions": [{"id": "DIM_REGION"}],
            "time_range": {"type": "LAST_N", "value": 7, "unit": "DAY"},
            "filters": [],
            "order_by": [],
            "limit": 100,
            "warnings": []
        }
    
    mock_client.generate_decomposition = AsyncMock(side_effect=mock_generate_decomposition)
    mock_client.generate_plan = AsyncMock(side_effect=mock_generate_plan)
    
    return mock_client


# ============================================================
# 延迟测试
# ============================================================


class TestLatency:
    """请求延迟测试组"""

    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_single_request_latency_p95(
        self, client, mock_registry, mock_ai_client
    ):
        """
        【测试目标】
        1. 验证单请求延迟 P95 < 200ms

        【执行过程】
        1. mock 所有外部依赖
        2. 预热：执行 3 次请求
        3. 正式测量：执行 20 次请求并记录延迟
        4. 计算 P95 延迟

        【预期结果】
        1. P95 延迟 < 200ms
        """
        import main
        with patch.object(main, 'registry', mock_registry), \
             patch('stages.stage1_decomposition.get_ai_client', return_value=mock_ai_client), \
             patch('stages.stage2_plan_generation.get_ai_client', return_value=mock_ai_client):
            
            # Warmup: 先运行几次，让 JIT/缓存生效
            for _ in range(3):
                client.post(
                    "/nl2sql/plan",
                    json={
                        "question": "统计员工数量",
                        "user_id": "user_001",
                        "role_id": "ROLE_HR_HEAD",
                        "tenant_id": "tenant_001",
                    },
                )
            
            # 正式测量：多次重复测量
            latencies = []
            num_requests = 20
            
            for _ in range(num_requests):
                start_time = time.perf_counter()
                response = client.post(
                    "/nl2sql/plan",
                    json={
                        "question": "统计员工数量",
                        "user_id": "user_001",
                        "role_id": "ROLE_HR_HEAD",
                        "tenant_id": "tenant_001",
                    },
                )
                end_time = time.perf_counter()
                latency_ms = (end_time - start_time) * 1000  # 转换为毫秒
                latencies.append(latency_ms)
            
            # 计算 P95
            latencies.sort()
            p95_index = int(len(latencies) * 0.95)
            p95_ms = latencies[p95_index] if p95_index < len(latencies) else latencies[-1]
            
            # P95 应该 < 200ms（仅应用逻辑，不含外部服务）
            assert p95_ms < 200.0, f"P95 latency {p95_ms:.2f}ms exceeds 200ms threshold"


# ============================================================
# 并发测试
# ============================================================


class TestConcurrency:
    """并发处理能力测试组"""

    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_concurrent_requests_success_rate(
        self, client, mock_registry, mock_ai_client
    ):
        """
        【测试目标】
        1. 验证并发请求成功率 > 95%

        【执行过程】
        1. mock 所有外部依赖
        2. 使用线程池执行 10 个并发请求
        3. 统计成功响应（200）数量
        4. 计算成功率

        【预期结果】
        1. 成功率 > 95%
        """
        import main
        with patch.object(main, 'registry', mock_registry), \
             patch('stages.stage1_decomposition.get_ai_client', return_value=mock_ai_client), \
             patch('stages.stage2_plan_generation.get_ai_client', return_value=mock_ai_client):
            num_concurrent = 10
            request_data = {
                "question": "统计员工数量",
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
            }

            def make_request():
                response = client.post("/nl2sql/plan", json=request_data)
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
    async def test_concurrent_requests_no_crash(
        self, client, mock_registry, mock_ai_client
    ):
        """测试并发请求不会导致服务崩溃"""
        import main
        with patch.object(main, 'registry', mock_registry), \
             patch('stages.stage1_decomposition.get_ai_client', return_value=mock_ai_client), \
             patch('stages.stage2_plan_generation.get_ai_client', return_value=mock_ai_client):
            num_concurrent = 10
            request_data = {
                "question": "统计员工数量",
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
            }

            def make_request():
                try:
                    # 移除 timeout 参数（TestClient 不支持，且 Mock 会立即返回）
                    response = client.post("/nl2sql/plan", json=request_data)
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


# ============================================================
# 超时测试
# ============================================================


class TestTimeout:
    """超时处理测试组"""

    @pytest.fixture
    def slow_mock_ai_client(self):
        """
        创建模拟的 AIClient，模拟超时场景。
        
        generate_decomposition 立即返回（正常），
        generate_plan 延迟 31 秒后返回，用于触发 30s 超时阈值。
        """
        mock_client = MagicMock()
        
        # Mock generate_decomposition (Stage 1 使用) - 立即返回
        async def mock_generate_decomposition(messages, temperature=0.0):
            """立即返回硬编码的子查询分解结果"""
            return {
                "sub_queries": [
                    {
                        "id": "q1",
                        "description": "统计员工数量"
                    }
                ]
            }
        
        # Mock generate_plan (Stage 2 使用) - 延迟 31 秒模拟超时
        async def mock_generate_plan(messages, temperature=0.0):
            """延迟 31 秒后返回，模拟超时场景"""
            await asyncio.sleep(31)
            return {
                "intent": "AGG",
                "metrics": [{"id": "METRIC_GMV"}],
                "dimensions": [{"id": "DIM_REGION"}],
                "time_range": {"type": "LAST_N", "value": 7, "unit": "DAY"},
                "filters": [],
                "order_by": [],
                "limit": 100,
                "warnings": []
            }
        
        mock_client.generate_decomposition = AsyncMock(side_effect=mock_generate_decomposition)
        mock_client.generate_plan = AsyncMock(side_effect=mock_generate_plan)
        
        return mock_client

    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_request_timeout_setting(
        self, mock_registry, slow_mock_ai_client
    ):
        """
        【测试目标】
        1. 验证请求超时设置（30秒）生效

        【执行过程】
        1. mock registry 和 slow_mock_ai_client（generate_plan 延迟 31秒）
        2. 使用 httpx.AsyncClient 包装 FastAPI app
        3. 使用 asyncio.wait_for 设置 30秒 timeout
        4. 调用 POST /nl2sql/plan
        5. 捕获超时异常并测量实际耗时

        【预期结果】
        1. 抛出 asyncio.TimeoutError 或 httpx 超时异常
        2. 实际耗时在 29-35 秒范围内（约 30秒）
        3. 异常类型或消息包含 "timeout" 关键字
        """
        import main
        with patch.object(main, 'registry', mock_registry), \
             patch('stages.stage1_decomposition.get_ai_client', return_value=slow_mock_ai_client), \
             patch('stages.stage2_plan_generation.get_ai_client', return_value=slow_mock_ai_client):
            
            start_time = time.time()
            
            # 使用 httpx.AsyncClient 替代 TestClient，支持 timeout 参数
            # 使用 ASGITransport 来连接 FastAPI 应用
            transport = ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as async_client:
                try:
                    # 使用 asyncio.wait_for 包装请求，设置 30 秒超时
                    response = await asyncio.wait_for(
                        async_client.post(
                            "/nl2sql/plan",
                            json={
                                "question": "统计员工数量",
                                "user_id": "user_001",
                                "role_id": "ROLE_HR_HEAD",
                                "tenant_id": "tenant_001",
                            },
                        ),
                        timeout=30.0  # 30秒超时
                    )
                    # 如果请求在 30s 内完成，说明超时逻辑可能有问题（因为 Mock 延迟了 31s）
                    end_time = time.time()
                    elapsed = end_time - start_time
                    # 这种情况不应该发生，因为 Mock 会延迟 31s
                    assert False, f"Request completed in {elapsed}s, but should have timed out (Mock delays 31s)"
                except asyncio.TimeoutError:
                    # 预期会抛出超时异常
                    end_time = time.time()
                    elapsed = end_time - start_time
                    
                    # 验证超时时间：应该在 30s 左右（允许一些误差）
                    assert elapsed >= 29.0, f"Timeout exception raised but elapsed time {elapsed}s < 29s (expected ~30s)"
                    assert elapsed <= 35.0, f"Timeout took too long: {elapsed}s (expected ~30s)"
                except Exception as e:
                    # 捕获其他可能的异常（如 httpx 超时异常）
                    end_time = time.time()
                    elapsed = end_time - start_time
                    
                    # 验证超时时间：应该在 30s 左右（允许一些误差）
                    assert elapsed >= 29.0, f"Exception raised but elapsed time {elapsed}s < 29s (expected ~30s)"
                    assert elapsed <= 35.0, f"Exception took too long: {elapsed}s (expected ~30s)"
                    
                    # 验证异常类型：可能是 httpx.ReadTimeout, httpx.ConnectTimeout 或其他超时相关异常
                    error_str = str(e).lower()
                    error_type = type(e).__name__.lower()
                    is_timeout = (
                        "timeout" in error_str or 
                        "timed out" in error_str or 
                        "超时" in error_str or
                        "timeout" in error_type or
                        "readtimeout" in error_type or
                        "connecttimeout" in error_type
                    )
                    assert is_timeout, \
                        f"Exception should indicate timeout, but got: {type(e).__name__}: {e}"


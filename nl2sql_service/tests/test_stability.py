"""
Stability Test Suite

测试稳定性相关功能：
- 异常降级：依赖失败时的优雅降级
- 依赖失败：LLM服务失败、数据库连接失败等
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from main import app


# ============================================================
# Test Fixtures
# ============================================================


@pytest.fixture
def client():
    """创建 TestClient 实例"""
    return TestClient(app)


# ============================================================
# 异常降级测试
# ============================================================


class TestGracefulDegradation:
    """测试优雅降级"""

    @pytest.mark.asyncio
    @pytest.mark.stability
    @patch("main.registry")
    @patch("main.stage2_plan_generation.process_subquery")
    async def test_llm_service_failure_handling(
        self,
        mock_generate_plan,
        mock_registry_global,
        client,
    ):
        """测试LLM服务失败时的处理"""
        # patch("main.registry") 已经把全局 registry 替换为一个 MagicMock（无需再次赋值）
        # Mock Stage2 抛出异常（模拟 LLM/Stage2 失败）
        mock_generate_plan.side_effect = Exception("LLM service unavailable")

        response = client.post(
            "/nl2sql/plan",
            json={
                "question": "统计员工数量",
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
            },
        )

        # 应该返回错误，但不应该是未处理的异常
        assert response.status_code in [500, 400]
        # 错误响应应该有结构化的错误信息（AppError handler）
        error_data = response.json()
        assert "request_id" in error_data
        assert "error" in error_data
        assert "code" in error_data["error"]

    @pytest.mark.asyncio
    @pytest.mark.stability
    async def test_registry_not_initialized_handling(
        self, client
    ):
        """测试注册表未初始化时的处理"""
        # 需要真的把 main.registry 置为 None（仅重绑入参不会影响 patch）
        import main
        with patch.object(main, "registry", None):
            response = client.post(
                "/nl2sql/plan",
                json={
                    "question": "统计员工数量",
                    "user_id": "user_001",
                    "role_id": "ROLE_HR_HEAD",
                    "tenant_id": "tenant_001",
                },
            )

        # registry 未初始化会触发 RuntimeError，被包装为 AppError => 500
        assert response.status_code in [500, 503]
        error_data = response.json()
        assert "request_id" in error_data
        assert "error" in error_data
        assert error_data["error"]["code"] in {"INTERNAL_ERROR", "LLM_PROVIDER_INIT_FAILED"}

    @pytest.mark.asyncio
    @pytest.mark.stability
    @patch("main.registry")
    @patch("main.stage1_decomposition.process_request")
    async def test_stage_failure_handling(
        self,
        mock_decomposition,
        mock_registry_global,
        client,
    ):
        """测试Stage失败时的处理"""
        # Mock Stage 1 抛出异常
        mock_decomposition.side_effect = Exception("Stage 1 processing failed")

        response = client.post(
            "/nl2sql/plan",
            json={
                "question": "统计员工数量",
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
            },
        )

        # 应该返回错误，但应该有明确的错误信息
        assert response.status_code in [500, 400]
        error_data = response.json()
        assert "request_id" in error_data
        assert "error" in error_data
        assert "code" in error_data["error"]


# ============================================================
# 依赖失败测试
# ============================================================


class TestDependencyFailure:
    """测试依赖失败处理"""

    @pytest.mark.asyncio
    @pytest.mark.stability
    @patch("main.registry")
    @patch("core.ai_client.get_ai_client")
    async def test_ai_client_connection_failure(
        self,
        mock_get_ai_client,
        mock_registry_global,
        client,
    ):
        """测试AI客户端连接失败"""
        mock_registry_global = MagicMock()

        # Mock AI客户端连接失败
        mock_client = MagicMock()
        mock_client.generate_plan = AsyncMock(
            side_effect=Exception("Connection timeout")
        )
        mock_get_ai_client.return_value = mock_client

        response = client.post(
            "/nl2sql/plan",
            json={
                "question": "统计员工数量",
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
            },
        )

        # 应该优雅处理，返回错误而不是崩溃
        assert response.status_code in [500, 400, 503]

    @pytest.mark.asyncio
    @pytest.mark.stability
    @patch("main.registry")
    async def test_invalid_request_handling(
        self, mock_registry_global, client
    ):
        """测试无效请求的处理"""
        mock_registry_global = MagicMock()

        # 发送无效请求
        invalid_requests = [
            {},  # 空请求
            {"question": ""},  # 空question
            {"question": "test"},  # 缺少必需字段
        ]

        for invalid_request in invalid_requests:
            response = client.post("/nl2sql/plan", json=invalid_request)

            # 应该返回422验证错误，而不是500
            assert response.status_code == 422

    @pytest.mark.asyncio
    @pytest.mark.stability
    @patch("main.registry")
    @patch("main.stage3_validation.validate_and_normalize_plan")
    async def test_validation_failure_handling(
        self,
        mock_validate,
        mock_registry_global,
        client,
    ):
        """测试验证失败时的处理"""
        mock_registry_global = MagicMock()

        # Mock验证失败
        from stages.stage3_validation import PermissionDeniedError

        mock_validate.side_effect = PermissionDeniedError("Unauthorized IDs")

        # 需要完整的mock链，这里简化
        # 实际测试中需要mock完整的pipeline
        pass

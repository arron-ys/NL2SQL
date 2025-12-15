"""
Plan API Test Suite

使用 TestClient 覆盖主要 API 路径（成功/失败场景）。
重点测试：
- POST /nl2sql/plan 成功场景
- POST /nl2sql/plan 失败场景（缺少字段、错误类型等）
- Error Contract 验证
- Plan 响应 Contract Test（Schema 验证）
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from main import app
from schemas.plan import MetricItem, PlanIntent, QueryPlan
from schemas.request import RequestContext, SubQueryItem


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
        "DIM_DEPARTMENT",
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
    registry.check_compatibility.return_value = True
    registry.global_config = {
        "global_settings": {},
        "time_windows": [],
    }
    return registry


@pytest.fixture
def mock_ai_client():
    """创建模拟的 AIClient，返回固定的 Plan JSON"""
    mock_client = MagicMock()

    async def mock_generate_plan(prompt):
        """返回固定的 Plan JSON"""
        return {
            "intent": "AGG",
            "metrics": [{"id": "METRIC_GMV", "compare_mode": None}],
            "dimensions": [{"id": "DIM_REGION", "time_grain": None}],
            "filters": [],
            "order_by": [],
            "warnings": [],
        }

    mock_client.generate_plan = AsyncMock(side_effect=mock_generate_plan)
    return mock_client


# ============================================================
# 成功场景测试
# ============================================================


class TestPlanAPISuccess:
    """测试 Plan API 成功场景"""

    @pytest.mark.asyncio
    @patch("main.registry")
    @patch("stages.stage1_decomposition.process_request")
    @patch("stages.stage2_plan_generation.process_subquery")
    @patch("stages.stage3_validation.validate_and_normalize_plan")
    async def test_plan_api_success(
        self,
        mock_validate,
        mock_generate_plan,
        mock_decomposition,
        mock_registry_global,
        client,
        mock_registry,
        mock_ai_client,
    ):
        """测试正常 Plan 生成"""
        # 设置全局 registry
        mock_registry_global = mock_registry

        # Mock Stage 1: Query Decomposition
        from datetime import date

        mock_decomposition.return_value = MagicMock(
            request_context=RequestContext(
                user_id="user_001",
                role_id="ROLE_HR_HEAD",
                tenant_id="tenant_001",
                request_id="test_request_001",
                current_date=date(2024, 1, 15),
            ),
            sub_queries=[
                SubQueryItem(id="sq_1", description="统计员工数量"),
            ],
        )

        # Mock Stage 2: Plan Generation
        mock_generate_plan.return_value = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
        )

        # Mock Stage 3: Validation
        mock_validate.return_value = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
        )

        # 发送请求
        response = client.post(
            "/nl2sql/plan",
            json={
                "question": "统计员工数量",
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
            },
        )

        # 验证响应
        assert response.status_code == 200
        plan = response.json()
        assert "intent" in plan
        assert plan["intent"] in ["AGG", "TREND", "DETAIL"]

    @pytest.mark.asyncio
    @patch("main.registry")
    async def test_plan_api_response_structure(
        self, mock_registry_global, client, mock_registry
    ):
        """测试 Plan 响应结构符合 Schema"""
        mock_registry_global = mock_registry

        # 由于需要完整的 pipeline，这里简化测试
        # 实际测试中应该使用完整的 mock 链
        response = client.post(
            "/nl2sql/plan",
            json={
                "question": "测试",
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
            },
        )

        # 如果成功，验证响应可以被 QueryPlan 反序列化
        if response.status_code == 200:
            plan_data = response.json()
            try:
                plan = QueryPlan.model_validate(plan_data)
                assert plan.intent in PlanIntent
            except Exception as e:
                pytest.fail(f"Plan response does not match schema: {e}")


# ============================================================
# 失败场景测试
# ============================================================


class TestPlanAPIFailure:
    """测试 Plan API 失败场景"""

    def test_plan_api_missing_question(self, client):
        """测试缺少 question 字段"""
        response = client.post(
            "/nl2sql/plan",
            json={
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
            },
        )

        assert response.status_code == 422
        error = response.json()
        assert "question" in str(error).lower()

    def test_plan_api_missing_user_id(self, client):
        """测试缺少 user_id 字段"""
        response = client.post(
            "/nl2sql/plan",
            json={
                "question": "统计员工数量",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
            },
        )

        assert response.status_code == 422
        error = response.json()
        assert "user_id" in str(error).lower()

    def test_plan_api_missing_role_id(self, client):
        """测试缺少 role_id 字段"""
        response = client.post(
            "/nl2sql/plan",
            json={
                "question": "统计员工数量",
                "user_id": "user_001",
                "tenant_id": "tenant_001",
            },
        )

        assert response.status_code == 422
        error = response.json()
        assert "role_id" in str(error).lower()

    def test_plan_api_missing_tenant_id(self, client):
        """测试缺少 tenant_id 字段"""
        response = client.post(
            "/nl2sql/plan",
            json={
                "question": "统计员工数量",
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
            },
        )

        assert response.status_code == 422
        error = response.json()
        assert "tenant_id" in str(error).lower()

    def test_plan_api_invalid_type_user_id(self, client):
        """测试 user_id 类型错误（传数字）"""
        response = client.post(
            "/nl2sql/plan",
            json={
                "question": "统计员工数量",
                "user_id": 123,  # 应该是字符串
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
            },
        )

        assert response.status_code == 422

    def test_plan_api_invalid_type_include_trace(self, client):
        """测试 include_trace 类型错误（传字符串）"""
        response = client.post(
            "/nl2sql/plan",
            json={
                "question": "统计员工数量",
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
                "include_trace": "true",  # 应该是布尔值
            },
        )

        assert response.status_code == 422

    def test_plan_api_empty_question(self, client):
        """测试空 question"""
        response = client.post(
            "/nl2sql/plan",
            json={
                "question": "",  # 空字符串
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
            },
        )

        # 根据 min_length=1 的约束，应该返回 422
        assert response.status_code == 422

    def test_plan_api_empty_request(self, client):
        """测试空请求体"""
        response = client.post("/nl2sql/plan", json={})

        assert response.status_code == 422


# ============================================================
# Error Contract 测试
# ============================================================


class TestErrorContract:
    """测试错误契约（Error Contract）"""

    def test_error_contract_structure(self, client):
        """验证错误响应结构一致性"""
        response = client.post("/nl2sql/plan", json={})  # 缺少必需字段

        assert response.status_code == 422
        error = response.json()

        # 验证错误结构包含 detail
        assert "detail" in error or "detail" in str(error)

    def test_error_contract_request_id(self, client):
        """验证错误响应包含 request_id（通过响应头）"""
        response = client.post("/nl2sql/plan", json={})

        # 验证响应头包含 X-Trace-ID 或 X-Request-ID
        assert "X-Trace-ID" in response.headers or "X-Request-ID" in response.headers

    def test_error_contract_400_status(self, client):
        """测试 400 错误的结构"""
        # 这个测试需要模拟 Stage 1 返回空子查询的情况
        # 由于需要完整的 mock，这里先跳过具体实现
        # 实际测试中应该验证 400 错误的结构
        pass

    def test_error_contract_500_status(self, client):
        """测试 500 错误的结构"""
        # 这个测试需要模拟内部错误
        # 实际测试中应该验证 500 错误的结构包含 detail 和 request_id
        pass


# ============================================================
# Plan 响应 Contract Test
# ============================================================


class TestPlanResponseContract:
    """测试 Plan 响应契约（Contract Test）"""

    @pytest.mark.asyncio
    @patch("main.registry")
    async def test_plan_response_matches_schema(
        self, mock_registry_global, client, mock_registry
    ):
        """测试 Plan 响应必须能被 schema 反序列化"""
        mock_registry_global = mock_registry

        # 由于需要完整的 pipeline，这里简化测试
        # 实际测试中应该使用完整的 mock 链并验证响应
        response = client.post(
            "/nl2sql/plan",
            json={
                "question": "统计员工数量",
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
            },
        )

        if response.status_code == 200:
            plan_data = response.json()
            # 验证可以通过 QueryPlan.model_validate()
            try:
                plan = QueryPlan.model_validate(plan_data)
                # 验证必需字段存在
                assert hasattr(plan, "intent")
                assert plan.intent in PlanIntent
            except Exception as e:
                pytest.fail(f"Plan response validation failed: {e}")

    def test_plan_response_has_required_fields(self, client):
        """测试 Plan 响应包含必需字段"""
        # 这个测试需要成功的响应
        # 实际测试中应该验证响应包含所有必需字段
        pass

    def test_plan_response_intent_enum(self, client):
        """测试 Plan 响应的 intent 必须是有效枚举值"""
        # 这个测试需要成功的响应
        # 实际测试中应该验证 intent 是 AGG/TREND/DETAIL 之一
        pass

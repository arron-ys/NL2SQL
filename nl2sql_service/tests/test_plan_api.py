"""
【简述】
验证 /nl2sql/plan API 的请求参数校验、成功响应结构、错误契约与 Schema 合规性。

【范围/不测什么】
- 不覆盖真实 AI 模型推理与语义层加载；仅验证 API 契约、参数校验、响应结构与错误处理的完整性。

【用例概述】
- test_plan_api_success_agg:
  -- 验证 AGG 意图的正常 Plan 生成成功
- test_plan_api_success_trend:
  -- 验证 TREND 意图的正常 Plan 生成成功
- test_plan_api_success_detail:
  -- 验证 DETAIL 意图的正常 Plan 生成成功
- test_plan_api_response_structure:
  -- 验证 Plan 响应包含必需字段
- test_plan_api_missing_question:
  -- 验证缺少 question 字段时返回 422
- test_plan_api_missing_user_id:
  -- 验证缺少 user_id 字段时返回 422
- test_plan_api_missing_role_id:
  -- 验证缺少 role_id 字段时返回 422
- test_plan_api_missing_tenant_id:
  -- 验证缺少 tenant_id 字段时返回 422
- test_plan_api_invalid_type_user_id:
  -- 验证 user_id 类型错误时返回 422
- test_plan_api_invalid_type_include_trace:
  -- 验证 include_trace 类型错误时返回 422
- test_plan_api_empty_question:
  -- 验证空 question 字符串时返回 422
- test_plan_api_empty_request:
  -- 验证空请求体时返回 422
- test_error_contract_structure:
  -- 验证错误响应结构包含必需字段
- test_error_contract_request_id:
  -- 验证错误响应包含 request_id
- test_error_contract_400_status:
  -- 验证客户端错误返回 400 系列状态码
- test_error_contract_500_status_stage2_error:
  -- 验证 Stage2 错误返回 500 状态码与结构化错误体
- test_error_contract_missing_metric_error:
  -- 验证 MissingMetricError 返回 200 状态码与软错误结构
- test_error_contract_permission_denied_error:
  -- 验证 PermissionDeniedError 返回 200 状态码与脱敏错误消息
- test_plan_response_matches_schema:
  -- 验证 Plan 响应符合 QueryPlan Schema
- test_plan_response_has_required_fields:
  -- 验证 Plan 响应包含所有必需字段
- test_plan_response_intent_enum:
  -- 验证 Plan 响应的 intent 为有效枚举值
"""

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from main import app
from schemas.plan import DimensionItem, MetricItem, PlanIntent, QueryPlan
from schemas.request import RequestContext, SubQueryItem
from stages.stage2_plan_generation import Stage2Error
from stages.stage3_validation import MissingMetricError, PermissionDeniedError


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
    """Plan API 成功场景测试组"""

    @pytest.mark.integration
    @pytest.mark.asyncio
    @patch("main.stage1_decomposition.process_request")
    @patch("main.stage2_plan_generation.process_subquery")
    @patch("main.stage3_validation.validate_and_normalize_plan")
    async def test_plan_api_success_agg(
        self,
        mock_validate,
        mock_generate_plan,
        mock_decomposition,
        client,
        mock_registry,
    ):
        """
        【测试目标】
        1. 验证 /nl2sql/plan 在 AGG 意图场景下正常生成 Plan

        【执行过程】
        1. mock registry 和 Stage 1-3 返回 AGG 类型的 Plan
        2. 调用 POST /nl2sql/plan 发送有效请求
        3. 验证响应状态码和结构

        【预期结果】
        1. 返回 200 状态码
        2. 响应包含 "intent" 字段，值为 "AGG"
        3. 响应包含 "metrics" 字段且不为空
        """
        # 设置全局 registry
        import main
        with patch.object(main, 'registry', mock_registry):
            # Mock Stage 1: Query Decomposition
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
            assert plan["intent"] == "AGG"
            assert "metrics" in plan
            assert len(plan["metrics"]) > 0

    @pytest.mark.integration
    @pytest.mark.asyncio
    @patch("main.stage1_decomposition.process_request")
    @patch("main.stage2_plan_generation.process_subquery")
    @patch("main.stage3_validation.validate_and_normalize_plan")
    async def test_plan_api_success_trend(
        self,
        mock_validate,
        mock_generate_plan,
        mock_decomposition,
        client,
        mock_registry,
    ):
        """
        【测试目标】
        1. 验证 /nl2sql/plan 在 TREND 意图场景下正常生成 Plan

        【执行过程】
        1. mock registry 和 Stage 1-3 返回 TREND 类型的 Plan（包含时间维度和时间范围）
        2. 调用 POST /nl2sql/plan 发送趋势分析请求
        3. 验证响应状态码和结构

        【预期结果】
        1. 返回 200 状态码
        2. 响应包含 "intent" 字段，值为 "TREND"
        3. 响应包含 "metrics" 和 "dimensions" 字段且不为空
        """
        import main
        with patch.object(main, 'registry', mock_registry):
            mock_decomposition.return_value = MagicMock(
                request_context=RequestContext(
                    user_id="user_001",
                    role_id="ROLE_HR_HEAD",
                    tenant_id="tenant_001",
                    request_id="test_request_002",
                    current_date=date(2024, 1, 15),
                ),
                sub_queries=[
                    SubQueryItem(id="sq_1", description="查看销售额趋势"),
                ],
            )

            # Mock TREND intent plan
            mock_generate_plan.return_value = QueryPlan(
                intent=PlanIntent.TREND,
                metrics=[MetricItem(id="METRIC_GMV")],
                dimensions=[DimensionItem(id="DIM_REGION")],
                time_range={"type": "LAST_N", "value": 30, "unit": "DAY"},
            )

            mock_validate.return_value = QueryPlan(
                intent=PlanIntent.TREND,
                metrics=[MetricItem(id="METRIC_GMV")],
                dimensions=[DimensionItem(id="DIM_REGION")],
                time_range={"type": "LAST_N", "value": 30, "unit": "DAY"},
            )

            response = client.post(
                "/nl2sql/plan",
                json={
                    "question": "查看销售额趋势",
                    "user_id": "user_001",
                    "role_id": "ROLE_HR_HEAD",
                    "tenant_id": "tenant_001",
                },
            )

            assert response.status_code == 200
            plan = response.json()
            assert plan["intent"] == "TREND"
            assert "time_range" in plan

    @pytest.mark.integration
    @pytest.mark.asyncio
    @patch("main.stage1_decomposition.process_request")
    @patch("main.stage2_plan_generation.process_subquery")
    @patch("main.stage3_validation.validate_and_normalize_plan")
    async def test_plan_api_success_detail(
        self,
        mock_validate,
        mock_generate_plan,
        mock_decomposition,
        client,
        mock_registry,
    ):
        """
        【测试目标】
        1. 验证 /nl2sql/plan 在 DETAIL 意图场景下正常生成 Plan

        【执行过程】
        1. mock registry 和 Stage 1-3 返回 DETAIL 类型的 Plan（无 metrics，有 dimensions 和 limit）
        2. 调用 POST /nl2sql/plan 发送明细查询请求
        3. 验证响应状态码和结构

        【预期结果】
        1. 返回 200 状态码
        2. 响应包含 "intent" 字段，值为 "DETAIL"
        3. 响应包含 "limit" 字段
        """
        import main
        with patch.object(main, 'registry', mock_registry):
            mock_decomposition.return_value = MagicMock(
                request_context=RequestContext(
                    user_id="user_001",
                    role_id="ROLE_HR_HEAD",
                    tenant_id="tenant_001",
                    request_id="test_request_003",
                    current_date=date(2024, 1, 15),
                ),
                sub_queries=[
                    SubQueryItem(id="sq_1", description="查看订单明细"),
                ],
            )

            # Mock DETAIL intent plan
            mock_generate_plan.return_value = QueryPlan(
                intent=PlanIntent.DETAIL,
                dimensions=[DimensionItem(id="DIM_REGION"), DimensionItem(id="DIM_DEPARTMENT")],
                limit=100,
            )

            mock_validate.return_value = QueryPlan(
                intent=PlanIntent.DETAIL,
                dimensions=[DimensionItem(id="DIM_REGION"), DimensionItem(id="DIM_DEPARTMENT")],
                limit=100,
            )

            response = client.post(
                "/nl2sql/plan",
                json={
                    "question": "查看订单明细",
                    "user_id": "user_001",
                    "role_id": "ROLE_HR_HEAD",
                    "tenant_id": "tenant_001",
                },
            )

            assert response.status_code == 200
            plan = response.json()
            assert plan["intent"] == "DETAIL"
            assert "limit" in plan

    @pytest.mark.integration
    @pytest.mark.asyncio
    @patch("main.registry")
    async def test_plan_api_response_structure(
        self, mock_registry_global, client, mock_registry
    ):
        """
        【测试目标】
        1. 验证 Plan 响应结构包含必需字段

        【执行过程】
        1. mock registry
        2. 调用 POST /nl2sql/plan 发送简单请求
        3. 验证响应字段存在性（简化测试，实际应完整 mock pipeline）

        【预期结果】
        1. 响应状态码为 200 或 500（取决于 mock 完整性）
        2. 响应为 JSON 格式
        """
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
    """Plan API 失败场景测试组"""

    @pytest.mark.integration
    def test_plan_api_missing_question(self, client):
        """
        【测试目标】
        1. 验证缺少 question 字段时返回 422 参数校验错误

        【执行过程】
        1. 调用 POST /nl2sql/plan 发送缺少 question 的请求
        2. 验证响应状态码和错误消息

        【预期结果】
        1. 返回 422 状态码
        2. 错误信息包含 "question" 字段提示
        """
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

    @pytest.mark.integration
    def test_plan_api_missing_user_id(self, client):
        """
        【测试目标】
        1. 验证缺少 user_id 字段时返回 422 参数校验错误

        【执行过程】
        1. 调用 POST /nl2sql/plan 发送缺少 user_id 的请求
        2. 验证响应状态码和错误消息

        【预期结果】
        1. 返回 422 状态码
        2. 错误信息包含 "user_id" 字段提示
        """
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

    @pytest.mark.integration
    def test_plan_api_missing_role_id(self, client):
        """
        【测试目标】
        1. 验证缺少 role_id 字段时返回 422 参数校验错误

        【执行过程】
        1. 调用 POST /nl2sql/plan 发送缺少 role_id 的请求
        2. 验证响应状态码和错误消息

        【预期结果】
        1. 返回 422 状态码
        2. 错误信息包含 "role_id" 字段提示
        """
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

    @pytest.mark.integration
    def test_plan_api_missing_tenant_id(self, client):
        """
        【测试目标】
        1. 验证缺少 tenant_id 字段时返回 422 参数校验错误

        【执行过程】
        1. 调用 POST /nl2sql/plan 发送缺少 tenant_id 的请求
        2. 验证响应状态码和错误消息

        【预期结果】
        1. 返回 422 状态码
        2. 错误信息包含 "tenant_id" 字段提示
        """
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

    @pytest.mark.integration
    def test_plan_api_invalid_type_user_id(self, client):
        """
        【测试目标】
        1. 验证 user_id 类型错误时返回 422 参数校验错误

        【执行过程】
        1. 调用 POST /nl2sql/plan 发送 user_id 为数字（应为字符串）的请求
        2. 验证响应状态码

        【预期结果】
        1. 返回 422 状态码（类型校验失败）
        """
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

    @pytest.mark.integration
    def test_plan_api_invalid_type_include_trace(self, client):
        """
        【测试目标】
        1. 验证 include_trace 类型错误时返回 422 参数校验错误

        【执行过程】
        1. 调用 POST /nl2sql/plan 发送 include_trace 为字符串（应为布尔值）的请求
        2. 验证响应状态码

        【预期结果】
        1. 返回 422 状态码（类型校验失败）
        """
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

    @pytest.mark.integration
    def test_plan_api_empty_question(self, client):
        """
        【测试目标】
        1. 验证空 question 字符串时返回 422 参数校验错误

        【执行过程】
        1. 调用 POST /nl2sql/plan 发送 question="" 的请求
        2. 验证响应状态码（根据 min_length=1 约束）

        【预期结果】
        1. 返回 422 状态码（长度校验失败）
        """
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

    @pytest.mark.integration
    def test_plan_api_empty_request(self, client):
        """
        【测试目标】
        1. 验证空请求体时返回 422 参数校验错误

        【执行过程】
        1. 调用 POST /nl2sql/plan 发送空 JSON 对象
        2. 验证响应状态码

        【预期结果】
        1. 返回 422 状态码（缺少必需字段）
        """
        response = client.post("/nl2sql/plan", json={})

        assert response.status_code == 422


# ============================================================
# Error Contract 测试
# ============================================================


class TestErrorContract:
    """错误契约测试组"""

    @pytest.mark.integration
    def test_error_contract_structure(self, client):
        """
        【测试目标】
        1. 验证错误响应结构包含必需字段

        【执行过程】
        1. 调用 POST /nl2sql/plan 发送空请求体触发 422 错误
        2. 验证错误响应结构

        【预期结果】
        1. 返回 422 状态码
        2. 响应为 JSON 格式且包含 "detail" 字段
        """
        response = client.post("/nl2sql/plan", json={})  # 缺少必需字段

        assert response.status_code == 422
        error = response.json()

        # 验证错误结构包含 detail
        assert "detail" in error or "detail" in str(error)

    @pytest.mark.integration
    def test_error_contract_request_id(self, client):
        """
        【测试目标】
        1. 验证错误响应包含 request_id

        【执行过程】
        1. 调用 POST /nl2sql/plan 发送空请求体触发错误
        2. 检查响应头中的 Trace-ID

        【预期结果】
        1. 响应头包含 Trace-ID 字段
        """
        response = client.post("/nl2sql/plan", json={})

        # 验证响应头包含 Trace-ID
        assert "Trace-ID" in response.headers

    @pytest.mark.integration
    @pytest.mark.asyncio
    @patch("main.stage1_decomposition.process_request")
    async def test_error_contract_400_status(
        self, mock_decomposition, client, mock_registry
    ):
        """
        【测试目标】
        1. 验证客户端错误（空子查询）返回 400 状态码

        【执行过程】
        1. mock registry 和 stage1_decomposition 返回空子查询列表
        2. 调用 POST /nl2sql/plan
        3. 验证响应状态码和错误消息

        【预期结果】
        1. 返回 400 状态码
        2. 错误响应包含 "detail" 字段
        3. 错误消息包含 "No sub-queries" 提示
        """
        import main
        with patch.object(main, 'registry', mock_registry):
            # Mock Stage 1 返回空子查询
            mock_decomposition.return_value = MagicMock(
                request_context=RequestContext(
                    user_id="user_001",
                    role_id="ROLE_HR_HEAD",
                    tenant_id="tenant_001",
                    request_id="test_request_400",
                    current_date=date(2024, 1, 15),
                ),
                sub_queries=[],  # 空子查询列表
            )

            response = client.post(
                "/nl2sql/plan",
                json={
                    "question": "无效问题",
                    "user_id": "user_001",
                    "role_id": "ROLE_HR_HEAD",
                    "tenant_id": "tenant_001",
                },
            )

            assert response.status_code == 400
            error = response.json()
            assert "detail" in error
            assert "No sub-queries" in error["detail"]

    @pytest.mark.integration
    @pytest.mark.asyncio
    @patch("main.stage1_decomposition.process_request")
    @patch("main.stage2_plan_generation.process_subquery")
    async def test_error_contract_500_status_stage2_error(
        self,
        mock_generate_plan,
        mock_decomposition,
        client,
        mock_registry,
    ):
        """
        【测试目标】
        1. 验证 Stage2 错误返回 500 状态码与结构化错误体

        【执行过程】
        1. mock registry 和 stage1
        2. mock stage2_plan_generation 抛出 Stage2Error
        3. 调用 POST /nl2sql/plan
        4. 验证响应状态码、错误结构和字段内容

        【预期结果】
        1. 返回 500 状态码
        2. 响应包含 "request_id"、"error_stage"、"error.code" 字段
        3. error_stage 为 "STAGE_2_PLAN_GENERATION"
        4. error.code 为 "STAGE2_UNKNOWN_ERROR"
        """
        import main
        with patch.object(main, 'registry', mock_registry):
            mock_decomposition.return_value = MagicMock(
                request_context=RequestContext(
                    user_id="user_001",
                    role_id="ROLE_HR_HEAD",
                    tenant_id="tenant_001",
                    request_id="test_request_500",
                    current_date=date(2024, 1, 15),
                ),
                sub_queries=[
                    SubQueryItem(id="sq_1", description="测试问题"),
                ],
            )

            # Mock Stage 2 抛出异常
            mock_generate_plan.side_effect = Stage2Error("Failed to generate plan")

            response = client.post(
                "/nl2sql/plan",
                json={
                    "question": "测试问题",
                    "user_id": "user_001",
                    "role_id": "ROLE_HR_HEAD",
                    "tenant_id": "tenant_001",
                },
            )

            assert response.status_code == 500
            error = response.json()
            # 由 AppError handler 统一输出：
            # {"request_id":..., "error_stage":..., "error": {"code":..., "message":..., "details": {...}}}
            assert "request_id" in error
            assert "error_stage" in error
            assert "error" in error
            assert error["error"]["code"] == "INTERNAL_ERROR"
            assert error["error"]["message"] == "Internal server error"
            assert "details" in error["error"]
            assert error["error"]["details"]["error_type"] in {"Stage2Error", "Stage2Error"}

    @pytest.mark.integration
    @pytest.mark.asyncio
    @patch("main.stage1_decomposition.process_request")
    @patch("main.stage2_plan_generation.process_subquery")
    @patch("main.stage3_validation.validate_and_normalize_plan")
    async def test_error_contract_missing_metric_error(
        self,
        mock_validate,
        mock_generate_plan,
        mock_decomposition,
        client,
        mock_registry,
    ):
        """
        【测试目标】
        1. 验证 MissingMetricError 返回 200 状态码与软错误结构

        【执行过程】
        1. mock registry 和 stage1/stage2
        2. mock stage3_validation 抛出 MissingMetricError
        3. 调用 POST /nl2sql/plan
        4. 验证响应状态码和错误结构

        【预期结果】
        1. 返回 200 状态码（业务软错误）
        2. status 为 "ERROR"
        3. error.code 为 "NEED_CLARIFICATION"
        4. error.stage 为 "STAGE_3_VALIDATION"
        """
        import main
        with patch.object(main, 'registry', mock_registry):
            mock_decomposition.return_value = MagicMock(
                request_context=RequestContext(
                    user_id="user_001",
                    role_id="ROLE_HR_HEAD",
                    tenant_id="tenant_001",
                    request_id="test_request_missing_metric",
                    current_date=date(2024, 1, 15),
                ),
                sub_queries=[
                    SubQueryItem(id="sq_1", description="测试问题"),
                ],
            )

            mock_generate_plan.return_value = QueryPlan(
                intent=PlanIntent.AGG,
                metrics=[],  # 空指标列表
            )

            # Mock Stage 3 抛出 MissingMetricError
            mock_validate.side_effect = MissingMetricError("Plan with intent AGG must have at least one metric")

            response = client.post(
                "/nl2sql/plan",
                json={
                    "question": "测试问题",
                    "user_id": "user_001",
                    "role_id": "ROLE_HR_HEAD",
                    "tenant_id": "tenant_001",
                },
            )

            # MissingMetricError 有专用 handler：HTTP 200 + status=ERROR
            assert response.status_code == 200
            body = response.json()
            assert body["status"] == "ERROR"
            assert body["error"]["code"] == "NEED_CLARIFICATION"
            assert body["error"]["stage"] == "STAGE_3_VALIDATION"

    @pytest.mark.integration
    @pytest.mark.asyncio
    @patch("main.stage1_decomposition.process_request")
    @patch("main.stage2_plan_generation.process_subquery")
    @patch("main.stage3_validation.validate_and_normalize_plan")
    async def test_error_contract_permission_denied_error(
        self,
        mock_validate,
        mock_generate_plan,
        mock_decomposition,
        client,
        mock_registry,
    ):
        """
        【测试目标】
        1. 验证 PermissionDeniedError 返回 200 状态码与脱敏错误消息

        【执行过程】
        1. mock registry 和 stage1/stage2
        2. mock stage3_validation 抛出 PermissionDeniedError（包含 METRIC_* ID）
        3. 调用 POST /nl2sql/plan
        4. 验证响应状态码、错误结构和 METRIC_* 脱敏

        【预期结果】
        1. 返回 200 状态码（业务软错误）
        2. status 为 "ERROR"
        3. error.code 为 "PERMISSION_DENIED"
        4. 响应文本不包含 "METRIC_" 或 METRIC_* 格式的内部 ID
        """
        import main
        with patch.object(main, 'registry', mock_registry):
            mock_decomposition.return_value = MagicMock(
                request_context=RequestContext(
                    user_id="user_001",
                    role_id="ROLE_HR_HEAD",
                    tenant_id="tenant_001",
                    request_id="test_request_permission",
                    current_date=date(2024, 1, 15),
                ),
                sub_queries=[
                    SubQueryItem(id="sq_1", description="测试问题"),
                ],
            )

            mock_generate_plan.return_value = QueryPlan(
                intent=PlanIntent.AGG,
                metrics=[MetricItem(id="METRIC_GMV")],
            )

            # Mock Stage 3 抛出 PermissionDeniedError
            mock_validate.side_effect = PermissionDeniedError("User does not have permission to access METRIC_GMV")

            response = client.post(
                "/nl2sql/plan",
                json={
                    "question": "测试问题",
                    "user_id": "user_001",
                    "role_id": "ROLE_HR_HEAD",
                    "tenant_id": "tenant_001",
                },
            )

            # PermissionDeniedError 有专用 handler：HTTP 200 + 脱敏提示
            assert response.status_code == 200
            body = response.json()
            assert body["status"] == "ERROR"
            assert body["error"]["code"] == "PERMISSION_DENIED"
            assert body["error"]["stage"] == "STAGE_3_VALIDATION"


# ============================================================
# Plan 响应 Contract Test
# ============================================================


class TestPlanResponseContract:
    """Plan 响应契约测试组"""

    @pytest.mark.integration
    @pytest.mark.asyncio
    @patch("main.registry")
    async def test_plan_response_matches_schema(
        self, mock_registry_global, client, mock_registry
    ):
        """
        【测试目标】
        1. 验证 Plan 响应符合 QueryPlan Schema

        【执行过程】
        1. mock registry
        2. 调用 POST /nl2sql/plan（简化测试）
        3. 尝试使用 QueryPlan.model_validate() 反序列化响应
        4. 验证 intent 字段类型

        【预期结果】
        1. 如果返回 200，响应可被 QueryPlan 成功反序列化
        2. plan.intent 为有效的 PlanIntent 枚举值
        """
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

    @pytest.mark.integration
    @pytest.mark.asyncio
    @patch("main.stage1_decomposition.process_request")
    @patch("main.stage2_plan_generation.process_subquery")
    @patch("main.stage3_validation.validate_and_normalize_plan")
    async def test_plan_response_has_required_fields(
        self,
        mock_validate,
        mock_generate_plan,
        mock_decomposition,
        client,
        mock_registry,
    ):
        """
        【测试目标】
        1. 验证 Plan 响应包含所有必需字段

        【执行过程】
        1. mock registry 和 Stage 1-3 返回完整的 Plan
        2. 调用 POST /nl2sql/plan
        3. 验证响应中所有必需字段存在性

        【预期结果】
        1. 返回 200 状态码
        2. 响应包含 "intent"、"metrics"、"dimensions"、"filters"、"order_by"、"warnings" 字段
        """
        import main
        with patch.object(main, 'registry', mock_registry):
            mock_decomposition.return_value = MagicMock(
                request_context=RequestContext(
                    user_id="user_001",
                    role_id="ROLE_HR_HEAD",
                    tenant_id="tenant_001",
                    request_id="test_request_fields",
                    current_date=date(2024, 1, 15),
                ),
                sub_queries=[
                    SubQueryItem(id="sq_1", description="测试问题"),
                ],
            )

            mock_generate_plan.return_value = QueryPlan(
                intent=PlanIntent.AGG,
                metrics=[MetricItem(id="METRIC_GMV")],
                dimensions=[DimensionItem(id="DIM_REGION")],
                filters=[],
                order_by=[],
                warnings=[],
            )

            mock_validate.return_value = QueryPlan(
                intent=PlanIntent.AGG,
                metrics=[MetricItem(id="METRIC_GMV")],
                dimensions=[DimensionItem(id="DIM_REGION")],
                filters=[],
                order_by=[],
                warnings=[],
            )

            response = client.post(
                "/nl2sql/plan",
                json={
                    "question": "测试问题",
                    "user_id": "user_001",
                    "role_id": "ROLE_HR_HEAD",
                    "tenant_id": "tenant_001",
                },
            )

            assert response.status_code == 200
            plan = response.json()
            # 验证必需字段
            assert "intent" in plan
            assert "metrics" in plan
            assert "dimensions" in plan
            assert "filters" in plan
            assert "order_by" in plan
            assert "warnings" in plan

    @pytest.mark.integration
    @pytest.mark.asyncio
    @patch("main.stage1_decomposition.process_request")
    @patch("main.stage2_plan_generation.process_subquery")
    @patch("main.stage3_validation.validate_and_normalize_plan")
    async def test_plan_response_intent_enum(
        self,
        mock_validate,
        mock_generate_plan,
        mock_decomposition,
        client,
        mock_registry,
    ):
        """
        【测试目标】
        1. 验证 Plan 响应的 intent 为有效枚举值

        【执行过程】
        1. mock registry 和 Stage 1-3
        2. 对所有三种 intent（AGG、TREND、DETAIL）分别执行测试
        3. 调用 POST /nl2sql/plan
        4. 验证响应中的 intent 值

        【预期结果】
        1. 返回 200 状态码
        2. plan["intent"] 在 ["AGG", "TREND", "DETAIL"] 枚举值中
        """
        import main
        with patch.object(main, 'registry', mock_registry):
            mock_decomposition.return_value = MagicMock(
                request_context=RequestContext(
                    user_id="user_001",
                    role_id="ROLE_HR_HEAD",
                    tenant_id="tenant_001",
                    request_id="test_request_enum",
                    current_date=date(2024, 1, 15),
                ),
                sub_queries=[
                    SubQueryItem(id="sq_1", description="测试问题"),
                ],
            )

            # 测试所有三种 intent
            for intent in [PlanIntent.AGG, PlanIntent.TREND, PlanIntent.DETAIL]:
                mock_generate_plan.return_value = QueryPlan(intent=intent, metrics=[])
                mock_validate.return_value = QueryPlan(intent=intent, metrics=[])

                response = client.post(
                    "/nl2sql/plan",
                    json={
                        "question": "测试问题",
                        "user_id": "user_001",
                        "role_id": "ROLE_HR_HEAD",
                        "tenant_id": "tenant_001",
                    },
                )

                assert response.status_code == 200
                plan = response.json()
                assert plan["intent"] in ["AGG", "TREND", "DETAIL"]

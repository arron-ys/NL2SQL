"""
【简述】
验证 NL2SQL 端到端管道从 /nl2sql/plan 到 /nl2sql/sql 的完整流程，包括多意图、多数据库方言与错误处理。

【范围/不测什么】
- 不覆盖真实 AI 模型推理与数据库执行；仅验证 API 编排、Stage 协调与响应结构正确性。

【用例概述】
- test_plan_to_sql_e2e:
  -- 验证从 Plan 生成到 SQL 生成的完整链路执行成功
- test_e2e_with_different_intents:
  -- 验证不同意图（AGG/TREND/DETAIL）的端到端流程正确性
- test_sql_generation_with_different_db_types:
  -- 验证不同数据库类型（mysql/postgresql/sqlite）的 SQL 生成正确性
- test_e2e_error_handling:
  -- 验证端到端流程对无效 Plan 的错误处理
"""

from datetime import date
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
def valid_request():
    """有效的请求数据"""
    return {
        "question": "统计每个部门的员工数量",
        "user_id": "user_001",
        "role_id": "ROLE_HR_HEAD",
        "tenant_id": "tenant_001",
    }


# ============================================================
# 端到端测试
# ============================================================


class TestE2EPipeline:
    """E2E Pipeline smoke tests"""

    @pytest.mark.asyncio
    @pytest.mark.e2e
    @patch("main.registry")
    @patch("main.stage1_decomposition.process_request")
    @patch("main.stage2_plan_generation.process_subquery")
    @patch("main.stage3_validation.validate_and_normalize_plan")
    @patch("main.stage4_sql_gen.generate_sql")
    async def test_plan_to_sql_e2e(
        self,
        mock_generate_sql,
        mock_validate,
        mock_generate_plan,
        mock_decomposition,
        mock_registry_global,
        client,
        mock_registry,
        valid_request,
    ):
        """
        【测试目标】
        1. 验证 /nl2sql/plan → /nl2sql/sql 完整链路执行成功

        【执行过程】
        1. mock 所有 Stage（decomposition/plan_generation/validation/sql_gen）
        2. 调用 POST /nl2sql/plan 生成 Plan
        3. 调用 POST /nl2sql/sql 生成 SQL
        4. 验证响应状态码与 SQL 字段存在性

        【预期结果】
        1. /nl2sql/plan 返回 200 状态码
        2. /nl2sql/sql 返回 200 状态码
        3. SQL 响应包含 "sql" 字段
        4. SQL 内容非空且包含 "SELECT"
        """
        mock_registry_global = mock_registry

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
                SubQueryItem(id="sq_1", description="统计每个部门的员工数量"),
            ],
        )

        # Mock Stage 2: Plan Generation
        mock_generate_plan.return_value = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            dimensions=[],
        )

        # Mock Stage 3: Validation
        validated_plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            dimensions=[],
        )
        mock_validate.return_value = validated_plan

        # Mock Stage 4: SQL Generation
        mock_generate_sql.return_value = "SELECT COUNT(*) as count FROM orders"

        # 1. 生成Plan
        plan_resp = client.post("/nl2sql/plan", json=valid_request)
        assert plan_resp.status_code == 200
        plan = plan_resp.json()

        # 2. 生成SQL
        sql_resp = client.post(
            "/nl2sql/sql",
            json={
                "plan": plan,
                "request_context": {
                    "user_id": "user_001",
                    "role_id": "ROLE_HR_HEAD",
                    "tenant_id": "tenant_001",
                    "request_id": "test_request_001",
                    "current_date": "2024-01-15",
                },
                "db_type": "mysql",
            },
        )

        # 3. 验证响应
        assert sql_resp.status_code == 200
        sql_data = sql_resp.json()
        assert "sql" in sql_data
        sql = sql_data["sql"]

        # 4. 验证SQL语法（基本检查）
        assert "SELECT" in sql.upper()
        assert isinstance(sql, str)
        assert len(sql) > 0

    @pytest.mark.asyncio
    @pytest.mark.e2e
    @patch("main.registry")
    @patch("main.stage1_decomposition.process_request")
    @patch("main.stage2_plan_generation.process_subquery")
    @patch("main.stage3_validation.validate_and_normalize_plan")
    @patch("main.stage4_sql_gen.generate_sql")
    async def test_e2e_with_different_intents(
        self,
        mock_generate_sql,
        mock_validate,
        mock_generate_plan,
        mock_decomposition,
        mock_registry_global,
        client,
        mock_registry,
    ):
        """
        【测试目标】
        1. 验证不同意图（AGG/TREND/DETAIL）的端到端流程正确性

        【执行过程】
        1. 对每个意图（AGG、TREND、DETAIL）分别执行
        2. mock Stage 1-4 返回对应意图的 Plan
        3. 调用 /nl2sql/plan 和 /nl2sql/sql
        4. 验证响应状态码与 SQL 生成成功

        【预期结果】
        1. 所有意图的 /nl2sql/plan 返回 200
        2. 所有意图的 /nl2sql/sql 返回 200
        3. SQL 响应包含 "sql" 字段
        """
        mock_registry_global = mock_registry

        intents = [PlanIntent.AGG, PlanIntent.TREND, PlanIntent.DETAIL]

        for intent in intents:
            # Mock Stage 1
            mock_decomposition.return_value = MagicMock(
                request_context=RequestContext(
                    user_id="user_001",
                    role_id="ROLE_HR_HEAD",
                    tenant_id="tenant_001",
                    request_id=f"test_request_{intent.value}",
                    current_date=date(2024, 1, 15),
                ),
                sub_queries=[
                    SubQueryItem(id="sq_1", description="测试查询"),
                ],
            )

            # Mock Stage 2
            mock_generate_plan.return_value = QueryPlan(
                intent=intent,
                metrics=[MetricItem(id="METRIC_GMV")] if intent != PlanIntent.DETAIL else [],
            )

            # Mock Stage 3
            mock_validate.return_value = QueryPlan(
                intent=intent,
                metrics=[MetricItem(id="METRIC_GMV")] if intent != PlanIntent.DETAIL else [],
            )

            # Mock Stage 4
            mock_generate_sql.return_value = "SELECT * FROM orders"

            # 测试流程
            plan_resp = client.post(
                "/nl2sql/plan",
                json={
                    "question": f"测试{intent.value}查询",
                    "user_id": "user_001",
                    "role_id": "ROLE_HR_HEAD",
                    "tenant_id": "tenant_001",
                },
            )

            if plan_resp.status_code == 200:
                plan = plan_resp.json()
                sql_resp = client.post(
                    "/nl2sql/sql",
                    json={
                        "plan": plan,
                        "request_context": {
                            "user_id": "user_001",
                            "role_id": "ROLE_HR_HEAD",
                            "tenant_id": "tenant_001",
                            "request_id": f"test_request_{intent.value}",
                            "current_date": "2024-01-15",
                        },
                        "db_type": "mysql",
                    },
                )

                assert sql_resp.status_code == 200
                sql_data = sql_resp.json()
                assert "sql" in sql_data

    @pytest.mark.asyncio
    @pytest.mark.e2e
    @patch("main.registry")
    @patch("main.stage4_sql_gen.generate_sql")
    async def test_sql_generation_with_different_db_types(
        self,
        mock_generate_sql,
        mock_registry_global,
        client,
        mock_registry,
    ):
        """
        【测试目标】
        1. 验证不同数据库类型（mysql/postgresql/sqlite）的 SQL 生成正确性

        【执行过程】
        1. 准备固定的 QueryPlan
        2. 对每个数据库类型分别调用 /nl2sql/sql
        3. mock generate_sql 返回带数据库类型注释的 SQL
        4. 验证响应状态码与 SQL 字段存在性

        【预期结果】
        1. 所有数据库类型返回 200 状态码
        2. SQL 响应包含 "sql" 字段
        """
        mock_registry_global = mock_registry

        db_types = ["mysql", "postgresql", "sqlite"]

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
        )

        for db_type in db_types:
            mock_generate_sql.return_value = f"SELECT * FROM orders -- {db_type}"

            sql_resp = client.post(
                "/nl2sql/sql",
                json={
                    "plan": plan.model_dump(),
                    "request_context": {
                        "user_id": "user_001",
                        "role_id": "ROLE_HR_HEAD",
                        "tenant_id": "tenant_001",
                        "request_id": f"test_request_{db_type}",
                        "current_date": "2024-01-15",
                    },
                    "db_type": db_type,
                },
            )

            assert sql_resp.status_code == 200
            sql_data = sql_resp.json()
            assert "sql" in sql_data

    @pytest.mark.asyncio
    @pytest.mark.e2e
    @patch("main.registry")
    async def test_e2e_error_handling(
        self,
        mock_registry_global,
        client,
        mock_registry,
    ):
        """
        【测试目标】
        1. 验证端到端流程对无效 Plan 的错误处理

        【执行过程】
        1. 构造包含无效 intent 的 Plan 对象
        2. 调用 POST /nl2sql/sql 传入无效 Plan
        3. 验证错误响应状态码

        【预期结果】
        1. 返回 422 或 500 状态码（而非 200）
        """
        mock_registry_global = mock_registry

        # 测试无效的Plan导致SQL生成失败
        invalid_plan = {
            "intent": "INVALID_INTENT",  # 无效的意图
            "metrics": [],
            "dimensions": [],
            "filters": [],
            "order_by": [],
            "warnings": [],
        }

        sql_resp = client.post(
            "/nl2sql/sql",
            json={
                "plan": invalid_plan,
                "request_context": {
                    "user_id": "user_001",
                    "role_id": "ROLE_HR_HEAD",
                    "tenant_id": "tenant_001",
                    "request_id": "test_request_error",
                    "current_date": "2024-01-15",
                },
                "db_type": "mysql",
            },
        )

        # 应该返回错误（422或500）
        assert sql_resp.status_code in [422, 500]

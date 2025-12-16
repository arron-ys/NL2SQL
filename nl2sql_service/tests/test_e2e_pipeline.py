"""
End-to-End Pipeline Test Suite

测试 Plan → SQL 完整链路。
重点测试：
- /plan → /sql 完整流程
- SQL 语法验证
- 数据流完整性
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
    """测试端到端流程"""

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
        """测试 Plan → SQL 完整链路"""
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
        """测试不同意图的端到端流程"""
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
        """测试不同数据库类型的SQL生成"""
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
        """测试端到端流程的错误处理"""
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

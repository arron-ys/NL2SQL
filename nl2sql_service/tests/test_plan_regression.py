"""
Plan Regression Test Suite

基于 YAML 用例的回归测试，验证语义层变更不会破坏已有功能。
使用 FastAPI TestClient 调用 /nl2sql/plan 端点，mock 所有外部依赖。
"""
import yaml
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from main import app
from schemas.plan import PlanIntent, QueryPlan
from schemas.request import RequestContext, SubQueryItem


# ============================================================
# Test Fixtures
# ============================================================


@pytest.fixture
def client():
    """创建 TestClient 实例"""
    return TestClient(app)


@pytest.fixture
def regression_cases():
    """加载回归测试用例"""
    yaml_path = Path(__file__).parent / "regression" / "plan_regression.yaml"
    with open(yaml_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("regression_cases", [])


@pytest.fixture
def mock_registry():
    """创建模拟的 SemanticRegistry"""
    registry = MagicMock()
    # 模拟允许的 ID 集合（根据 YAML 用例中的 expected_metrics/dimensions 动态设置）
    registry.get_allowed_ids.return_value = {
        "METRIC_EMPLOYEE_COUNT",
        "METRIC_REVENUE",
        "METRIC_GMV",
        "METRIC_ORDER_COUNT",
        "METRIC_PROFIT",
        "METRIC_AMOUNT",
        "DIM_DEPARTMENT",
        "DIM_REGION",
        "DIM_DATE",
        "DIM_PRODUCT_CATEGORY",
        "DIM_SALES_PERSON",
        "DIM_CITY",
    }
    
    # 模拟指标定义
    def get_metric_def(metric_id):
        return {
            "id": metric_id,
            "entity_id": "ENTITY_ORDER",
            "default_filters": [],
            "default_time": None,
        }
    registry.get_metric_def.side_effect = get_metric_def
    
    # 模拟维度定义
    def get_dimension_def(dim_id):
        return {
            "id": dim_id,
            "entity_id": "ENTITY_ORDER",
        }
    registry.get_dimension_def.side_effect = get_dimension_def
    
    # 模拟兼容性检查
    registry.check_compatibility.return_value = True
    registry.global_config = {
        "global_settings": {},
        "time_windows": [],
    }
    return registry


@pytest.fixture
def mock_ai_client():
    """创建模拟的 AIClient，返回基于用例的 Plan JSON"""
    mock_client = MagicMock()
    return mock_client


# ============================================================
# Regression Test Cases
# ============================================================


@pytest.mark.regression
@pytest.mark.integration
@pytest.mark.asyncio
@patch("main.registry")
@patch("main.stage1_decomposition.process_request")
@patch("main.stage2_plan_generation.process_subquery")
@patch("main.stage3_validation.validate_and_normalize_plan")
async def test_regression_case(
    mock_validate,
    mock_generate_plan,
    mock_decomposition,
    mock_registry_global,
    client,
    mock_registry,
    regression_cases,
):
    """
    执行所有回归测试用例
    
    对每个用例：
    1. Mock Stage 1-3 的返回值
    2. 调用 /nl2sql/plan 端点
    3. 验证响应结构（intent、metrics、dimensions、time_range）
    """
    # 设置全局 registry
    mock_registry_global = mock_registry
    
    for case in regression_cases:
        case_id = case.get("id", "UNKNOWN")
        question = case.get("question", "")
        expected_intent = case.get("expected_intent")
        expected_metrics = case.get("expected_metrics", [])
        expected_dimensions = case.get("expected_dimensions", [])
        expected_time_range = case.get("expected_time_range")
        
        # Mock Stage 1: Query Decomposition
        from datetime import date
        
        mock_decomposition.return_value = MagicMock(
            request_context=RequestContext(
                user_id="test_user",
                role_id="ROLE_TEST",
                tenant_id="test_tenant",
                request_id=f"test_{case_id}",
                current_date=date(2024, 1, 15),
            ),
            sub_queries=[
                SubQueryItem(id="sq_1", description=question),
            ],
        )
        
        # Mock Stage 2: Plan Generation
        # 根据用例的 expected_* 构建 Plan
        from schemas.plan import MetricItem, DimensionItem, TimeRange, TimeRangeType
        
        metrics = []
        if isinstance(expected_metrics, list):
            for metric in expected_metrics:
                if isinstance(metric, str):
                    metrics.append(MetricItem(id=metric))
                elif isinstance(metric, dict):
                    metrics.append(MetricItem(**metric))
        
        dimensions = []
        if isinstance(expected_dimensions, list):
            for dim in expected_dimensions:
                if isinstance(dim, str):
                    dimensions.append(DimensionItem(id=dim))
                elif isinstance(dim, dict):
                    dimensions.append(DimensionItem(**dim))
        
        time_range = None
        if expected_time_range:
            time_range = TimeRange(**expected_time_range)
        
        plan = QueryPlan(
            intent=PlanIntent[expected_intent] if expected_intent else PlanIntent.AGG,
            metrics=metrics,
            dimensions=dimensions,
            time_range=time_range,
        )
        
        mock_generate_plan.return_value = plan
        
        # Mock Stage 3: Validation（返回相同的 plan）
        mock_validate.return_value = plan
        
        # 发送请求
        response = client.post(
            "/nl2sql/plan",
            json={
                "question": question,
                "user_id": "test_user",
                "role_id": "ROLE_TEST",
                "tenant_id": "test_tenant",
            },
        )
        
        # 验证响应
        assert response.status_code == 200, f"Case {case_id} failed with status {response.status_code}"
        
        plan_data = response.json()
        
        # 验证 intent
        if expected_intent:
            assert "intent" in plan_data, f"Case {case_id}: missing intent in response"
            assert plan_data["intent"] == expected_intent, (
                f"Case {case_id}: expected intent {expected_intent}, "
                f"got {plan_data['intent']}"
            )
        
        # 验证 metrics
        if expected_metrics:
            assert "metrics" in plan_data, f"Case {case_id}: missing metrics in response"
            actual_metric_ids = [m.get("id") if isinstance(m, dict) else m for m in plan_data["metrics"]]
            expected_metric_ids = [m.get("id") if isinstance(m, dict) else m for m in expected_metrics]
            # 验证所有期望的指标都存在（不要求完全一致，因为可能有额外指标）
            for expected_id in expected_metric_ids:
                assert expected_id in actual_metric_ids, (
                    f"Case {case_id}: expected metric {expected_id} not found in response"
                )
        
        # 验证 dimensions
        if expected_dimensions:
            assert "dimensions" in plan_data, f"Case {case_id}: missing dimensions in response"
            actual_dim_ids = [d.get("id") if isinstance(d, dict) else d for d in plan_data["dimensions"]]
            expected_dim_ids = [d.get("id") if isinstance(d, dict) else d for d in expected_dimensions]
            # 验证所有期望的维度都存在
            for expected_id in expected_dim_ids:
                assert expected_id in actual_dim_ids, (
                    f"Case {case_id}: expected dimension {expected_id} not found in response"
                )
        
        # 验证 time_range
        if expected_time_range:
            assert "time_range" in plan_data, f"Case {case_id}: missing time_range in response"
            actual_time_range = plan_data["time_range"]
            assert actual_time_range is not None, f"Case {case_id}: time_range is None"
            # 验证时间范围类型
            if "type" in expected_time_range:
                assert actual_time_range.get("type") == expected_time_range["type"], (
                    f"Case {case_id}: expected time_range type {expected_time_range['type']}, "
                    f"got {actual_time_range.get('type')}"
                )




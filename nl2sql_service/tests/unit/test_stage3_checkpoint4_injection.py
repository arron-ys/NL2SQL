"""
Unit tests for Stage3 Checkpoint4 injection logic (Step 4)

Tests:
1. test_stage3_skips_time_injection_when_all_time_even_with_vague_cue
2. test_trend_injects_time_dimension_and_default_grain_and_warning
3. test_default_order_by_for_trend_time_asc
4. test_default_order_by_for_agg_metric_desc
5. test_mandatory_lf_skipped_when_user_has_target_dim_filter (Step 5)
6. test_mandatory_lf_raw_sql_still_injected_with_trace_marker (Step 5)
"""
import pytest
from unittest.mock import MagicMock
from datetime import date

pytestmark = pytest.mark.unit

from schemas.plan import (
    QueryPlan, PlanIntent, MetricItem, DimensionItem, 
    TimeRange, TimeRangeType, TimeGrain, OrderDirection
)
from schemas.request import RequestContext
from stages.stage3_validation import validate_and_normalize_plan, ConfigurationError
from core.semantic_registry import SemanticRegistry


@pytest.mark.asyncio
async def test_stage3_skips_time_injection_when_all_time_even_with_vague_cue():
    """Test that Stage3 skips time injection when time_range.type == ALL_TIME, even with vague time cue"""
    registry = MagicMock(spec=SemanticRegistry)
    registry.get_allowed_ids.return_value = {"METRIC_GMV", "DIM_ORDER_DATE"}
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "default_filters": []
    }
    registry.check_compatibility.return_value = True
    registry.global_config = {}
    
    # Plan with ALL_TIME (should skip injection even with vague cue)
    plan = QueryPlan(
        intent=PlanIntent.AGG,
        metrics=[MetricItem(id="METRIC_GMV")],
        dimensions=[],
        filters=[],
        time_range=TimeRange(type=TimeRangeType.ALL_TIME),
        order_by=[],
        warnings=[]
    )
    
    context = RequestContext(
        request_id="test_all_time",
        user_id="test_user",
        tenant_id="test_tenant",
        role_id="analyst",
        current_date=date(2024, 1, 15)
    )
    
    # Validate with vague time cue in description
    validated_plan = await validate_and_normalize_plan(
        plan=plan,
        context=context,
        registry=registry,
        sub_query_description="最近的销售额",  # Vague time cue
        raw_question="最近的销售额"
    )
    
    # Should NOT inject time window, should keep ALL_TIME
    assert validated_plan.time_range is not None
    assert validated_plan.time_range.type == TimeRangeType.ALL_TIME
    # Should NOT add time warning
    assert not any("模糊时间" in w for w in validated_plan.warnings)


@pytest.mark.asyncio
async def test_trend_injects_time_dimension_and_default_grain_and_warning():
    """Test that TREND intent injects time dimension with default_time_grain and warning"""
    registry = MagicMock(spec=SemanticRegistry)
    registry.get_allowed_ids.return_value = {"METRIC_GMV", "DIM_ORDER_DATE"}
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "default_filters": []
    }
    registry.get_entity_def.return_value = {
        "id": "ENT_SALES_ORDER_ITEM",
        "default_time_field_id": "ORDER_DATE"
    }
    registry.resolve_dimension_id_by_time_field_id.return_value = "DIM_ORDER_DATE"
    registry.get_dimension_def.return_value = {
        "id": "DIM_ORDER_DATE",
        "name": "订单日期",
        "is_time_dimension": True,
        "time_field_id": "ORDER_DATE",
        "default_time_grain": "DAY",
        "allowed_time_grains": ["DAY", "MONTH", "YEAR"]
    }
    registry.check_compatibility.return_value = True
    registry.global_config = {}
    
    # TREND plan without time dimension
    plan = QueryPlan(
        intent=PlanIntent.TREND,
        metrics=[MetricItem(id="METRIC_GMV")],
        dimensions=[],  # No time dimension
        filters=[],
        order_by=[],
        warnings=[]
    )
    
    context = RequestContext(
        request_id="test_trend",
        user_id="test_user",
        tenant_id="test_tenant",
        role_id="analyst",
        current_date=date(2024, 1, 15)
    )
    
    validated_plan = await validate_and_normalize_plan(
        plan=plan,
        context=context,
        registry=registry,
        sub_query_description="测试查询",
        raw_question="测试查询"
    )
    
    # Should inject time dimension
    assert len(validated_plan.dimensions) == 1
    assert validated_plan.dimensions[0].id == "DIM_ORDER_DATE"
    assert validated_plan.dimensions[0].time_grain == TimeGrain.DAY
    
    # Should add warning
    assert any("已自动按 '订单日期' 以 DAY 粒度进行趋势统计" in w for w in validated_plan.warnings)


@pytest.mark.asyncio
async def test_default_order_by_for_trend_time_asc():
    """Test that TREND intent gets default order_by: time dimension ASC"""
    registry = MagicMock(spec=SemanticRegistry)
    registry.get_allowed_ids.return_value = {"METRIC_GMV", "DIM_ORDER_DATE"}
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "default_filters": []
    }
    registry.get_dimension_def.return_value = {
        "id": "DIM_ORDER_DATE",
        "name": "订单日期",
        "is_time_dimension": True,
        "time_field_id": "ORDER_DATE",
        "default_time_grain": "DAY"
    }
    registry.check_compatibility.return_value = True
    registry.global_config = {}
    
    # TREND plan with time dimension but no order_by
    plan = QueryPlan(
        intent=PlanIntent.TREND,
        metrics=[MetricItem(id="METRIC_GMV")],
        dimensions=[DimensionItem(id="DIM_ORDER_DATE", time_grain=TimeGrain.DAY)],
        filters=[],
        order_by=[],  # Empty order_by
        warnings=[]
    )
    
    context = RequestContext(
        request_id="test_trend_order",
        user_id="test_user",
        tenant_id="test_tenant",
        role_id="analyst",
        current_date=date(2024, 1, 15)
    )
    
    validated_plan = await validate_and_normalize_plan(
        plan=plan,
        context=context,
        registry=registry,
        sub_query_description="测试查询",
        raw_question="测试查询"
    )
    
    # Should set default order_by: time dimension ASC
    assert len(validated_plan.order_by) == 1
    assert validated_plan.order_by[0].id == "DIM_ORDER_DATE"
    assert validated_plan.order_by[0].direction == OrderDirection.ASC


@pytest.mark.asyncio
async def test_default_order_by_for_agg_metric_desc():
    """Test that AGG intent gets default order_by: primary metric DESC"""
    registry = MagicMock(spec=SemanticRegistry)
    registry.get_allowed_ids.return_value = {"METRIC_GMV", "METRIC_REVENUE", "DIM_REGION"}
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "default_filters": []
    }
    registry.get_dimension_def.return_value = {
        "id": "DIM_REGION",
        "entity_id": "ENT_SALES_ORDER_ITEM"
    }
    registry.check_compatibility.return_value = True
    registry.global_config = {}
    
    # AGG plan with no order_by
    plan = QueryPlan(
        intent=PlanIntent.AGG,
        metrics=[MetricItem(id="METRIC_GMV"), MetricItem(id="METRIC_REVENUE")],
        dimensions=[DimensionItem(id="DIM_REGION")],
        filters=[],
        order_by=[],  # Empty order_by
        warnings=[]
    )
    
    context = RequestContext(
        request_id="test_agg_order",
        user_id="test_user",
        tenant_id="test_tenant",
        role_id="analyst",
        current_date=date(2024, 1, 15)
    )
    
    validated_plan = await validate_and_normalize_plan(
        plan=plan,
        context=context,
        registry=registry,
        sub_query_description="测试查询",
        raw_question="测试查询"
    )
    
    # Should set default order_by: primary metric (first metric) DESC
    assert len(validated_plan.order_by) == 1
    assert validated_plan.order_by[0].id == "METRIC_GMV"
    assert validated_plan.order_by[0].direction == OrderDirection.DESC


@pytest.mark.asyncio
async def test_mandatory_lf_skipped_when_user_has_target_dim_filter():
    """Test that mandatory LF is skipped when user already has filter on target dimension (Step 5)"""
    from schemas.plan import FilterItem, FilterOp
    
    registry = MagicMock(spec=SemanticRegistry)
    registry.get_allowed_ids.return_value = {
        "METRIC_REVENUE", "DIM_ORDER_STATUS", "LF_REVENUE_VALID_ORDER"
    }
    registry.get_metric_def.return_value = {
        "id": "METRIC_REVENUE",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "default_filters": ["LF_REVENUE_VALID_ORDER"]  # Mandatory filter
    }
    # LF_REVENUE_VALID_ORDER targets DIM_ORDER_STATUS
    registry.get_logical_filter_def.return_value = {
        "id": "LF_REVENUE_VALID_ORDER",
        "filters": [
            {
                "target_id": "DIM_ORDER_STATUS",
                "operator": "IN_SET",
                "value_set_id": "STATUS_VALID_FOR_REVENUE"
            }
        ]
    }
    registry.check_compatibility.return_value = True
    registry.global_config = {}
    
    # User already has filter on DIM_ORDER_STATUS
    plan = QueryPlan(
        intent=PlanIntent.AGG,
        metrics=[MetricItem(id="METRIC_REVENUE")],
        dimensions=[],
        filters=[
            FilterItem(id="DIM_ORDER_STATUS", op=FilterOp.EQ, values=["Shipped"])
        ],
        order_by=[],
        warnings=[]
    )
    
    context = RequestContext(
        request_id="test_lf_conflict",
        user_id="test_user",
        tenant_id="test_tenant",
        role_id="analyst",
        current_date=date(2024, 1, 15)
    )
    
    validated_plan = await validate_and_normalize_plan(
        plan=plan,
        context=context,
        registry=registry,
        sub_query_description="测试查询",
        raw_question="测试查询"
    )
    
    # LF_REVENUE_VALID_ORDER should NOT be injected (user has DIM_ORDER_STATUS filter)
    filter_ids = [f.id for f in validated_plan.filters]
    assert "DIM_ORDER_STATUS" in filter_ids  # User's filter preserved
    assert "LF_REVENUE_VALID_ORDER" not in filter_ids  # LF skipped due to conflict


@pytest.mark.asyncio
async def test_mandatory_lf_raw_sql_still_injected_with_trace_marker():
    """Test that mandatory LF with RAW_SQL is still injected with trace marker (Step 5)"""
    registry = MagicMock(spec=SemanticRegistry)
    registry.get_allowed_ids.return_value = {
        "METRIC_ACTIVE_EMPLOYEES", "LF_HR_ACTIVE_EMPLOYEE", "DIM_EMPLOYMENT_STATUS"
    }
    registry.get_metric_def.return_value = {
        "id": "METRIC_ACTIVE_EMPLOYEES",
        "entity_id": "ENT_EMPLOYEE",
        "default_filters": ["LF_HR_ACTIVE_EMPLOYEE"]  # Mandatory filter with RAW_SQL
    }
    # LF_HR_ACTIVE_EMPLOYEE has both target_id and RAW_SQL
    registry.get_logical_filter_def.return_value = {
        "id": "LF_HR_ACTIVE_EMPLOYEE",
        "filters": [
            {
                "target_id": "DIM_EMPLOYMENT_STATUS",
                "operator": "IN_SET",
                "value_set_id": "STATUS_EMPLOYMENT_ACTIVE"
            },
            {
                "target_id": None,  # RAW_SQL has no target_id
                "operator": "RAW_SQL",
                "sql": "status_start_date <= CURDATE()"
            }
        ]
    }
    registry.check_compatibility.return_value = True
    registry.global_config = {}
    
    # Plan without any filters
    plan = QueryPlan(
        intent=PlanIntent.AGG,
        metrics=[MetricItem(id="METRIC_ACTIVE_EMPLOYEES")],
        dimensions=[],
        filters=[],
        order_by=[],
        warnings=[]
    )
    
    context = RequestContext(
        request_id="test_lf_raw_sql",
        user_id="test_user",
        tenant_id="test_tenant",
        role_id="analyst",
        current_date=date(2024, 1, 15)
    )
    
    validated_plan = await validate_and_normalize_plan(
        plan=plan,
        context=context,
        registry=registry,
        sub_query_description="测试查询",
        raw_question="测试查询"
    )
    
    # LF_HR_ACTIVE_EMPLOYEE should be injected (has RAW_SQL, default behavior)
    filter_ids = [f.id for f in validated_plan.filters]
    assert "LF_HR_ACTIVE_EMPLOYEE" in filter_ids


@pytest.mark.asyncio
async def test_trend_raises_config_error_when_entity_missing_default_time_field_id():
    """Test that TREND intent raises ConfigurationError when entity_def missing default_time_field_id"""
    registry = MagicMock(spec=SemanticRegistry)
    registry.get_allowed_ids.return_value = {"METRIC_GMV", "DIM_ORDER_DATE"}
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "default_filters": []
    }
    # Entity definition exists but missing default_time_field_id
    registry.get_entity_def.return_value = {
        "id": "ENT_SALES_ORDER_ITEM"
        # Missing default_time_field_id
    }
    registry.check_compatibility.return_value = True
    registry.global_config = {}
    
    # TREND plan without time dimension
    plan = QueryPlan(
        intent=PlanIntent.TREND,
        metrics=[MetricItem(id="METRIC_GMV")],
        dimensions=[],  # No time dimension
        filters=[],
        order_by=[],
        warnings=[]
    )
    
    context = RequestContext(
        request_id="test_trend_missing_time_field",
        user_id="test_user",
        tenant_id="test_tenant",
        role_id="analyst",
        current_date=date(2024, 1, 15)
    )
    
    # Should raise ConfigurationError
    with pytest.raises(ConfigurationError) as exc_info:
        await validate_and_normalize_plan(
            plan=plan,
            context=context,
            registry=registry,
            sub_query_description="测试查询",
            raw_question="测试查询"
        )
    
    # Verify error details
    assert "missing default_time_field_id" in exc_info.value.message
    assert exc_info.value.details["intent"] == "TREND"
    assert exc_info.value.details["metric_ids"] == ["METRIC_GMV"]
    assert exc_info.value.details["primary_entity_id"] == "ENT_SALES_ORDER_ITEM"


@pytest.mark.asyncio
async def test_trend_raises_config_error_when_resolve_dimension_id_returns_none():
    """Test that TREND intent raises ConfigurationError when resolve_dimension_id_by_time_field_id returns None"""
    registry = MagicMock(spec=SemanticRegistry)
    registry.get_allowed_ids.return_value = {"METRIC_GMV", "DIM_ORDER_DATE"}
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "default_filters": []
    }
    registry.get_entity_def.return_value = {
        "id": "ENT_SALES_ORDER_ITEM",
        "default_time_field_id": "ORDER_DATE"
    }
    # resolve_dimension_id_by_time_field_id returns None (time_field_id not found in index)
    registry.resolve_dimension_id_by_time_field_id.return_value = None
    registry.check_compatibility.return_value = True
    registry.global_config = {}
    
    # TREND plan without time dimension
    plan = QueryPlan(
        intent=PlanIntent.TREND,
        metrics=[MetricItem(id="METRIC_GMV")],
        dimensions=[],  # No time dimension
        filters=[],
        order_by=[],
        warnings=[]
    )
    
    context = RequestContext(
        request_id="test_trend_resolve_none",
        user_id="test_user",
        tenant_id="test_tenant",
        role_id="analyst",
        current_date=date(2024, 1, 15)
    )
    
    # Should raise ConfigurationError
    with pytest.raises(ConfigurationError) as exc_info:
        await validate_and_normalize_plan(
            plan=plan,
            context=context,
            registry=registry,
            sub_query_description="测试查询",
            raw_question="测试查询"
        )
    
    # Verify error details
    assert "cannot resolve dimension_id" in exc_info.value.message
    assert exc_info.value.details["intent"] == "TREND"
    assert exc_info.value.details["metric_ids"] == ["METRIC_GMV"]
    assert exc_info.value.details["primary_entity_id"] == "ENT_SALES_ORDER_ITEM"
    assert exc_info.value.details["default_time_field_id"] == "ORDER_DATE"


@pytest.mark.asyncio
async def test_trend_raises_config_error_when_time_dim_missing_default_time_grain():
    """Test that TREND intent raises ConfigurationError when time_dim_def missing default_time_grain"""
    registry = MagicMock(spec=SemanticRegistry)
    registry.get_allowed_ids.return_value = {"METRIC_GMV", "DIM_ORDER_DATE"}
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "default_filters": []
    }
    registry.get_entity_def.return_value = {
        "id": "ENT_SALES_ORDER_ITEM",
        "default_time_field_id": "ORDER_DATE"
    }
    registry.resolve_dimension_id_by_time_field_id.return_value = "DIM_ORDER_DATE"
    # Time dimension definition exists but missing default_time_grain
    registry.get_dimension_def.return_value = {
        "id": "DIM_ORDER_DATE",
        "name": "订单日期",
        "is_time_dimension": True,
        "time_field_id": "ORDER_DATE"
        # Missing default_time_grain
    }
    registry.check_compatibility.return_value = True
    registry.global_config = {}
    
    # TREND plan without time dimension
    plan = QueryPlan(
        intent=PlanIntent.TREND,
        metrics=[MetricItem(id="METRIC_GMV")],
        dimensions=[],  # No time dimension
        filters=[],
        order_by=[],
        warnings=[]
    )
    
    context = RequestContext(
        request_id="test_trend_missing_grain",
        user_id="test_user",
        tenant_id="test_tenant",
        role_id="analyst",
        current_date=date(2024, 1, 15)
    )
    
    # Should raise ConfigurationError
    with pytest.raises(ConfigurationError) as exc_info:
        await validate_and_normalize_plan(
            plan=plan,
            context=context,
            registry=registry,
            sub_query_description="测试查询",
            raw_question="测试查询"
        )
    
    # Verify error details
    assert "missing default_time_grain" in exc_info.value.message
    assert exc_info.value.details["intent"] == "TREND"
    assert exc_info.value.details["metric_ids"] == ["METRIC_GMV"]
    assert exc_info.value.details["primary_entity_id"] == "ENT_SALES_ORDER_ITEM"
    assert exc_info.value.details["default_time_field_id"] == "ORDER_DATE"
    assert exc_info.value.details["time_dim_id"] == "DIM_ORDER_DATE"

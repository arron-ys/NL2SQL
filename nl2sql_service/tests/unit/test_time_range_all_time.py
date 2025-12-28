"""
Unit tests for TimeRangeType.ALL_TIME feature (Step 1)

Tests:
1. test_time_range_all_time_valid_minimal - Valid ALL_TIME with no extra fields
2. test_time_range_all_time_rejects_extra_fields - ALL_TIME rejects value/unit/start/end
3. test_stage4_skips_time_where_when_all_time - Stage4 skips time WHERE when ALL_TIME
"""
import pytest

pytestmark = pytest.mark.unit
from datetime import date
from pydantic import ValidationError

from schemas.plan import TimeRange, TimeRangeType, QueryPlan, PlanIntent, MetricItem
from stages.stage4_sql_gen import generate_sql
from schemas.request import RequestContext
from core.semantic_registry import SemanticRegistry


def test_time_range_all_time_valid_minimal():
    """Test that ALL_TIME type accepts minimal valid structure"""
    # Valid: type=ALL_TIME with all other fields as None
    time_range = TimeRange(type=TimeRangeType.ALL_TIME)
    
    assert time_range.type == TimeRangeType.ALL_TIME
    assert time_range.value is None
    assert time_range.unit is None
    assert time_range.start is None
    assert time_range.end is None


def test_time_range_all_time_rejects_extra_fields():
    """Test that ALL_TIME type rejects any extra fields (value/unit/start/end)"""
    # Should reject value
    with pytest.raises(ValidationError) as exc_info:
        TimeRange(type=TimeRangeType.ALL_TIME, value=7)
    assert "value/unit/start/end to be None" in str(exc_info.value)
    
    # Should reject unit
    with pytest.raises(ValidationError) as exc_info:
        TimeRange(type=TimeRangeType.ALL_TIME, unit="day")
    assert "value/unit/start/end to be None" in str(exc_info.value)
    
    # Should reject start
    with pytest.raises(ValidationError) as exc_info:
        TimeRange(type=TimeRangeType.ALL_TIME, start="2024-01-01")
    assert "value/unit/start/end to be None" in str(exc_info.value)
    
    # Should reject end
    with pytest.raises(ValidationError) as exc_info:
        TimeRange(type=TimeRangeType.ALL_TIME, end="2024-12-31")
    assert "value/unit/start/end to be None" in str(exc_info.value)
    
    # Should reject combination
    with pytest.raises(ValidationError) as exc_info:
        TimeRange(type=TimeRangeType.ALL_TIME, value=7, unit="day")
    assert "value/unit/start/end to be None" in str(exc_info.value)


@pytest.mark.asyncio
async def test_stage4_skips_time_where_when_all_time():
    """Test that Stage4 skips time WHERE clause when time_range.type == ALL_TIME"""
    from unittest.mock import MagicMock
    
    # Create mock registry
    registry = MagicMock(spec=SemanticRegistry)
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENTITY_ORDER",
        "expression": {"sql": "SUM(amount)"},
        "default_time": {"time_field_id": "ORDER_DATE"}
    }
    registry.get_entity_def.return_value = {
        "id": "ENTITY_ORDER",
        "semantic_view": "v_sales_order_item",
        "default_time_field_id": "ORDER_DATE"
    }
    registry.get_dimension_def.return_value = {
        "id": "DIM_ORDER_DATE",
        "column": "order_date"
    }
    registry.get_rls_policies.return_value = []
    
    # Create a plan with ALL_TIME
    plan = QueryPlan(
        intent=PlanIntent.AGG,
        metrics=[MetricItem(id="METRIC_GMV")],
        dimensions=[],
        filters=[],
        time_range=TimeRange(type=TimeRangeType.ALL_TIME),
        order_by=[],
        limit=100,
        warnings=[]
    )
    
    context = RequestContext(
        request_id="test_all_time",
        user_id="test_user",
        tenant_id="test_tenant",
        role_id="analyst",
        current_date=date(2024, 1, 15)
    )
    
    # Generate SQL
    sql, diag_ctx = await generate_sql(
        plan=plan,
        context=context,
        registry=registry,
        db_type="postgresql"
    )
    
    # Verify: SQL should NOT contain time filter
    # The SQL should not have WHERE clause with time field
    assert "order_date" not in sql.lower() or "WHERE" not in sql.upper()
    
    # Verify: diag_ctx should have no time range
    assert diag_ctx["time_start"] is None
    assert diag_ctx["time_end"] is None

"""
Unit tests for Stage4 ORDER BY generation

Tests:
1. test_stage4_order_by_agg_metric_desc - AGG intent with metric DESC
2. test_stage4_order_by_trend_time_asc - TREND intent with time dimension ASC
"""
import pytest
from unittest.mock import MagicMock
from datetime import date

pytestmark = pytest.mark.unit

from schemas.plan import (
    QueryPlan, PlanIntent, MetricItem, DimensionItem, 
    OrderItem, OrderDirection, TimeGrain
)
from schemas.request import RequestContext
from stages.stage4_sql_gen import generate_sql
from core.semantic_registry import SemanticRegistry


@pytest.mark.asyncio
async def test_stage4_order_by_agg_metric_desc():
    """Test that Stage4 generates ORDER BY for AGG intent with metric DESC"""
    registry = MagicMock(spec=SemanticRegistry)
    
    # Mock metric definition
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "expression": {
            "sql": "SUM(order_amount)"
        }
    }
    
    # Mock entity definition
    registry.get_entity_def.return_value = {
        "id": "ENT_SALES_ORDER_ITEM",
        "semantic_view": "v_sales_order_item"
    }
    
    # Mock dimension definition (for compatibility check)
    registry.get_dimension_def.return_value = {
        "id": "DIM_REGION",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "column": "region"
    }
    
    # Mock term lookup for order_by
    registry.get_term.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENT_SALES_ORDER_ITEM"
    }
    
    registry.check_compatibility.return_value = True
    registry.get_rls_policies.return_value = []
    
    # AGG plan with order_by metric DESC
    plan = QueryPlan(
        intent=PlanIntent.AGG,
        metrics=[MetricItem(id="METRIC_GMV")],
        dimensions=[DimensionItem(id="DIM_REGION")],
        filters=[],
        order_by=[OrderItem(id="METRIC_GMV", direction=OrderDirection.DESC)],
        warnings=[]
    )
    
    context = RequestContext(
        request_id="test_order_agg",
        user_id="test_user",
        tenant_id="test_tenant",
        role_id="analyst",
        current_date=date(2024, 1, 15)
    )
    
    # Generate SQL
    sql_string, _ = await generate_sql(
        plan=plan,
        context=context,
        registry=registry,
        db_type="mysql"
    )
    
    # Assertions
    assert "ORDER BY" in sql_string.upper()
    assert "METRIC_GMV" in sql_string
    # MySQL uses DESC keyword
    assert "DESC" in sql_string.upper() or sql_string.upper().endswith("DESC")


@pytest.mark.asyncio
async def test_stage4_order_by_trend_time_asc():
    """Test that Stage4 generates ORDER BY for TREND intent with time dimension ASC"""
    registry = MagicMock(spec=SemanticRegistry)
    
    # Mock metric definition
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "expression": {
            "sql": "SUM(order_amount)"
        }
    }
    
    # Mock entity definition
    registry.get_entity_def.return_value = {
        "id": "ENT_SALES_ORDER_ITEM",
        "semantic_view": "v_sales_order_item"
    }
    
    # Mock time dimension definition
    registry.get_dimension_def.return_value = {
        "id": "DIM_ORDER_DATE",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "column": "order_date",
        "is_time_dimension": True
    }
    
    # Mock term lookup for order_by
    def get_term_side_effect(term_id):
        if term_id == "DIM_ORDER_DATE":
            return {
                "id": "DIM_ORDER_DATE",
                "entity_id": "ENT_SALES_ORDER_ITEM",
                "column": "order_date"
            }
        return None
    
    registry.get_term.side_effect = get_term_side_effect
    registry.check_compatibility.return_value = True
    registry.get_rls_policies.return_value = []
    
    # TREND plan with order_by time dimension ASC
    plan = QueryPlan(
        intent=PlanIntent.TREND,
        metrics=[MetricItem(id="METRIC_GMV")],
        dimensions=[DimensionItem(id="DIM_ORDER_DATE", time_grain=TimeGrain.DAY)],
        filters=[],
        order_by=[OrderItem(id="DIM_ORDER_DATE", direction=OrderDirection.ASC)],
        warnings=[]
    )
    
    context = RequestContext(
        request_id="test_order_trend",
        user_id="test_user",
        tenant_id="test_tenant",
        role_id="analyst",
        current_date=date(2024, 1, 15)
    )
    
    # Generate SQL
    sql_string, _ = await generate_sql(
        plan=plan,
        context=context,
        registry=registry,
        db_type="postgresql"
    )
    
    # Assertions
    assert "ORDER BY" in sql_string.upper()
    assert "DIM_ORDER_DATE" in sql_string
    # PostgreSQL: ASC is default, but should not have DESC
    assert "DESC" not in sql_string.upper() or sql_string.upper().count("DESC") == 0



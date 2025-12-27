"""
Stage 4 SQL Generation - Logical Filter Expansion Tests

测试逻辑过滤器（LF_*）展开为真实 SQL WHERE 条件的功能。
"""
import pytest
from datetime import date
from unittest.mock import MagicMock, patch

from schemas.plan import (
    FilterItem,
    FilterOp,
    MetricItem,
    PlanIntent,
    QueryPlan,
)
from schemas.request import RequestContext
from stages.stage4_sql_gen import generate_sql


@pytest.fixture
def mock_context():
    """创建 mock RequestContext"""
    return RequestContext(
        user_id="test_user",
        role_id="ROLE_SALES_HEAD",
        tenant_id="tenant_001",
        request_id="test_req_001",
        current_date=date(2025, 12, 27)
    )


@pytest.fixture
def mock_registry():
    """创建 mock SemanticRegistry"""
    registry = MagicMock()
    
    # Mock metric definition
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "name": "GMV",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "expression": {
            "expr_type": "SQL",
            "sql": "SUM(line_gmv)"
        }
    }
    
    # Mock entity definition
    registry.get_entity_def.return_value = {
        "id": "ENT_SALES_ORDER_ITEM",
        "semantic_view": "v_sales_order_item",
        "default_time_field_id": "ORDER_DATE"
    }
    
    # Mock dimension definition for DIM_ORDER_STATUS
    registry.get_dimension_def.return_value = {
        "id": "DIM_ORDER_STATUS",
        "name": "订单状态",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "column": "order_status"
    }
    
    # Mock logical filter definition for LF_REVENUE_VALID_ORDER
    registry.get_logical_filter_def.return_value = {
        "id": "LF_REVENUE_VALID_ORDER",
        "name": "收入口径有效订单过滤",
        "domain_id": "SALES",
        "filters": [
            {
                "target_id": "DIM_ORDER_STATUS",
                "operator": "IN_SET",
                "value_set_id": "STATUS_VALID_FOR_REVENUE"
            }
        ]
    }
    
    # Mock enum values for STATUS_VALID_FOR_REVENUE
    registry.get_enum_values.return_value = ["Resolved", "Shipped"]
    
    # Mock RLS policies (empty for this test)
    registry.get_rls_policies.return_value = []
    
    return registry


@pytest.mark.unit
@pytest.mark.asyncio
async def test_logical_filter_expansion_to_sql_where(mock_context, mock_registry):
    """
    【测试目标】
    1. 验证逻辑过滤器（LF_REVENUE_VALID_ORDER）被展开为真实 SQL WHERE 条件（改动2）

    【执行过程】
    1. 构造包含 LF_REVENUE_VALID_ORDER 的 QueryPlan
    2. 调用 generate_sql
    3. 检查生成的 SQL

    【预期结果】
    1. SQL 包含 order_status IN ('Resolved','Shipped') 条件
    2. SQL 包含 tenant_id='tenant_001' 条件
    3. SQL 不包含 "Skipping logical filter" 的日志路径
    """
    plan = QueryPlan(
        intent=PlanIntent.AGG,
        metrics=[MetricItem(id="METRIC_GMV")],
        filters=[FilterItem(id="LF_REVENUE_VALID_ORDER", op=FilterOp.IN, values=[])],
        time_range=None
    )
    
    sql, diag_ctx = await generate_sql(
        plan=plan,
        context=mock_context,
        registry=mock_registry,
        db_type="mysql"
    )
    
    # 【改动2验证】逻辑过滤器被展开为 SQL WHERE 条件
    assert sql is not None
    assert "order_status" in sql.lower()
    assert "'Resolved'" in sql or "'Shipped'" in sql
    assert "tenant_id" in sql.lower()
    assert "tenant_001" in sql
    
    # 验证调用了正确的 registry 方法
    mock_registry.get_logical_filter_def.assert_called_with("LF_REVENUE_VALID_ORDER")
    mock_registry.get_enum_values.assert_called_with("STATUS_VALID_FOR_REVENUE")
    mock_registry.get_dimension_def.assert_called_with("DIM_ORDER_STATUS")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_sql_without_time_filter_when_time_range_is_none(mock_context, mock_registry):
    """
    【测试目标】
    1. 验证当 time_range=None 时，SQL 不包含时间过滤条件（配合改动1）

    【执行过程】
    1. 构造 time_range=None 的 QueryPlan
    2. 调用 generate_sql
    3. 检查生成的 SQL

    【预期结果】
    1. SQL 不包含 order_date >= 或 order_date <= 条件
    2. SQL 包含 tenant_id 和逻辑过滤器条件
    """
    plan = QueryPlan(
        intent=PlanIntent.AGG,
        metrics=[MetricItem(id="METRIC_GMV")],
        filters=[FilterItem(id="LF_REVENUE_VALID_ORDER", op=FilterOp.IN, values=[])],
        time_range=None  # 无时间范围
    )
    
    sql, diag_ctx = await generate_sql(
        plan=plan,
        context=mock_context,
        registry=mock_registry,
        db_type="mysql"
    )
    
    # 验证 SQL 不包含时间过滤
    assert "order_date>=" not in sql
    assert "order_date<=" not in sql
    
    # 验证 SQL 包含其他必要条件
    assert "order_status" in sql.lower()
    assert "tenant_id" in sql.lower()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_time_grain_month_mysql(mock_context):
    """
    【测试目标】
    1. 验证 MySQL 的 time_grain=MONTH 能正确生成 DATE_FORMAT(..., '%Y-%m-01') 表达式
    2. 验证 CustomFunction 被正确调用，返回的 Function 对象支持 .as_() 方法
    
    【执行过程】
    1. 构造包含 DIM_SHIPPED_DATE (time_grain=MONTH) 的 QueryPlan
    2. 调用 generate_sql with db_type=mysql
    3. 检查生成的 SQL
    
    【预期结果】
    1. 不抛出 AttributeError: 'CustomFunction' object has no attribute 'as_'
    2. SQL 包含 DATE_FORMAT(...,'%Y-%m-01') AS DIM_SHIPPED_DATE
    3. GROUP BY 包含相同的 DATE_FORMAT 表达式
    """
    from schemas.plan import DimensionItem, TimeGrain
    
    registry = MagicMock()
    
    # Mock metric
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "name": "GMV",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "expression": {"expr_type": "SQL", "sql": "SUM(line_gmv)"}
    }
    
    # Mock entity
    registry.get_entity_def.return_value = {
        "id": "ENT_SALES_ORDER_ITEM",
        "semantic_view": "v_sales_order_item",
        "default_time_field_id": "SHIPPED_DATE"
    }
    
    # Mock dimension with time_grain
    registry.get_dimension_def.return_value = {
        "id": "DIM_SHIPPED_DATE",
        "name": "发货日期",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "column": "shipped_date",
        "dimension_type": "TIME"
    }
    
    registry.get_rls_policies.return_value = []
    
    plan = QueryPlan(
        intent=PlanIntent.AGG,
        metrics=[MetricItem(id="METRIC_GMV")],
        dimensions=[DimensionItem(id="DIM_SHIPPED_DATE", time_grain=TimeGrain.MONTH)],
        filters=[],
        time_range=None
    )
    
    # 关键：不应抛出 AttributeError
    sql, diag_ctx = await generate_sql(
        plan=plan,
        context=mock_context,
        registry=registry,
        db_type="mysql"
    )
    
    # 验证 SQL 包含正确的 DATE_FORMAT 表达式和别名
    assert sql is not None
    assert "DATE_FORMAT" in sql
    assert "%Y-%m-01" in sql
    assert "DIM_SHIPPED_DATE" in sql
    
    # 验证 GROUP BY 包含 DATE_FORMAT 表达式
    assert "GROUP BY" in sql
    sql_upper = sql.upper()
    assert "DATE_FORMAT" in sql_upper.split("GROUP BY")[1]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_time_grain_year_mysql(mock_context):
    """
    【测试目标】
    验证 MySQL 的 time_grain=YEAR 能正确生成 DATE_FORMAT(..., '%Y-01-01') 表达式
    """
    from schemas.plan import DimensionItem, TimeGrain
    
    registry = MagicMock()
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "expression": {"expr_type": "SQL", "sql": "SUM(line_gmv)"}
    }
    registry.get_entity_def.return_value = {
        "id": "ENT_SALES_ORDER_ITEM",
        "semantic_view": "v_sales_order_item"
    }
    registry.get_dimension_def.return_value = {
        "id": "DIM_SHIPPED_DATE",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "column": "shipped_date"
    }
    registry.get_rls_policies.return_value = []
    
    plan = QueryPlan(
        intent=PlanIntent.AGG,
        metrics=[MetricItem(id="METRIC_GMV")],
        dimensions=[DimensionItem(id="DIM_SHIPPED_DATE", time_grain=TimeGrain.YEAR)],
        filters=[]
    )
    
    sql, diag_ctx = await generate_sql(
        plan=plan,
        context=mock_context,
        registry=registry,
        db_type="mysql"
    )
    
    assert "DATE_FORMAT" in sql
    assert "%Y-01-01" in sql
    assert "DIM_SHIPPED_DATE" in sql


@pytest.mark.unit
@pytest.mark.asyncio
async def test_time_grain_month_postgresql(mock_context):
    """
    【测试目标】
    验证 PostgreSQL 的 time_grain=MONTH 能正确生成 DATE_TRUNC('month', ...) 表达式
    
    【预期结果】
    1. 不抛出 AttributeError
    2. SQL 包含 DATE_TRUNC('month',...) AS DIM_SHIPPED_DATE
    """
    from schemas.plan import DimensionItem, TimeGrain
    
    registry = MagicMock()
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "expression": {"expr_type": "SQL", "sql": "SUM(line_gmv)"}
    }
    registry.get_entity_def.return_value = {
        "id": "ENT_SALES_ORDER_ITEM",
        "semantic_view": "v_sales_order_item"
    }
    registry.get_dimension_def.return_value = {
        "id": "DIM_SHIPPED_DATE",
        "entity_id": "ENT_SALES_ORDER_ITEM",
        "column": "shipped_date"
    }
    registry.get_rls_policies.return_value = []
    
    plan = QueryPlan(
        intent=PlanIntent.AGG,
        metrics=[MetricItem(id="METRIC_GMV")],
        dimensions=[DimensionItem(id="DIM_SHIPPED_DATE", time_grain=TimeGrain.MONTH)],
        filters=[]
    )
    
    sql, diag_ctx = await generate_sql(
        plan=plan,
        context=mock_context,
        registry=registry,
        db_type="postgresql"
    )
    
    assert "DATE_TRUNC" in sql
    assert "'month'" in sql.lower()
    assert "DIM_SHIPPED_DATE" in sql

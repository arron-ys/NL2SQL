"""
Stage 4: SQL Generation (SQL 生成)

将验证后的 QueryPlan 转换为物理 SQL 查询字符串。
使用 PyPika 构建 SQL，支持 MySQL 和 PostgreSQL。
对应详细设计文档 3.4 的定义。
"""
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from pypika import (
    Criterion,
    CustomFunction,
    Field,
    Query,
    Table,
    functions as fn
)
from pypika.dialects import MySQLQuery, PostgreSQLQuery
from pypika.terms import Term

from core.dialect_adapter import DialectAdapter
from core.semantic_registry import SemanticRegistry
from schemas.plan import (
    FilterItem,
    FilterOp,
    OrderItem,
    PlanIntent,
    QueryPlan,
    TimeRange,
    TimeRangeType
)
from schemas.request import RequestContext
from utils.log_manager import get_logger

logger = get_logger(__name__)


# ============================================================
# 自定义类：用于包装原始 SQL 字符串
# ============================================================
class CustomCriterion(Term):
    """
    自定义 Criterion 类，用于包装原始 SQL 字符串
    
    在 PyPika 中，当需要直接使用原始 SQL 字符串时（如复杂的表达式、RLS 策略等），
    可以使用此类来包装 SQL 字符串，使其可以作为 SELECT 字段或 WHERE 条件使用。
    """
    def __init__(self, sql: str, alias: Optional[str] = None):
        """
        初始化 CustomCriterion
        
        Args:
            sql: 原始 SQL 字符串
            alias: 可选的别名
        """
        super().__init__(alias=alias)
        self.sql = sql
    
    def get_sql(self, **kwargs) -> str:
        """
        返回原始 SQL 字符串
        
        Args:
            **kwargs: 额外的参数（用于兼容 PyPika API）
        
        Returns:
            str: 原始 SQL 字符串
        """
        return self.sql


# ============================================================
# 异常定义
# ============================================================
class Stage4Error(Exception):
    """
    Stage 4 处理异常
    
    用于表示 SQL 生成阶段的错误，包括：
    - 实体定义缺失
    - 指标/维度定义缺失
    - SQL 构建失败
    """
    pass


# ============================================================
# 辅助函数
# ============================================================
def _calculate_time_range_bounds(
    time_range: TimeRange,
    current_date: date
) -> tuple[Optional[str], Optional[str]]:
    """
    计算时间范围的开始和结束日期
    
    Args:
        time_range: 时间范围对象
        current_date: 当前日期
    
    Returns:
        tuple[Optional[str], Optional[str]]: (start_date, end_date) ISO 8601 格式字符串
    """
    if time_range.type == TimeRangeType.LAST_N:
        # LAST_N 类型：从当前日期往前推 N 个时间单位
        value = time_range.value
        unit = time_range.unit.lower()
        
        if unit == "day":
            start_date = current_date - timedelta(days=value)
            end_date = current_date
        elif unit == "week":
            start_date = current_date - timedelta(weeks=value)
            end_date = current_date
        elif unit == "month":
            # 简化处理：使用 30 天作为一个月
            start_date = current_date - timedelta(days=value * 30)
            end_date = current_date
        elif unit == "quarter":
            # 简化处理：使用 90 天作为一个季度
            start_date = current_date - timedelta(days=value * 90)
            end_date = current_date
        elif unit == "year":
            # 简化处理：使用 365 天作为一年
            start_date = current_date - timedelta(days=value * 365)
            end_date = current_date
        else:
            raise ValueError(f"Unsupported time unit: {unit}")
        
        return start_date.isoformat(), end_date.isoformat()
    
    elif time_range.type == TimeRangeType.ABSOLUTE:
        # ABSOLUTE 类型：直接使用 start 和 end
        start = time_range.start
        end = time_range.end
        
        # 处理特殊值 "CURRENT_DATE"
        if end == "CURRENT_DATE":
            end = current_date.isoformat()
        if start == "CURRENT_DATE":
            start = current_date.isoformat()
        
        return start, end
    
    else:
        raise ValueError(f"Unsupported time range type: {time_range.type}")


def _map_filter_op_to_pypika(
    field: Field,
    op: FilterOp,
    values: List[Any]
) -> Criterion:
    """
    将过滤器操作符映射到 PyPika Criterion
    
    Args:
        field: PyPika Field 对象
        op: 过滤器操作符
        values: 过滤值列表
    
    Returns:
        Criterion: PyPika 条件对象
    """
    if op == FilterOp.EQ:
        if len(values) != 1:
            raise ValueError(f"EQ operator requires exactly 1 value, got {len(values)}")
        return field == values[0]
    
    elif op == FilterOp.NEQ:
        if len(values) != 1:
            raise ValueError(f"NEQ operator requires exactly 1 value, got {len(values)}")
        return field != values[0]
    
    elif op == FilterOp.IN:
        if len(values) == 0:
            raise ValueError("IN operator requires at least 1 value")
        return field.isin(values)
    
    elif op == FilterOp.NOT_IN:
        if len(values) == 0:
            raise ValueError("NOT_IN operator requires at least 1 value")
        return ~field.isin(values)
    
    elif op == FilterOp.GT:
        if len(values) != 1:
            raise ValueError(f"GT operator requires exactly 1 value, got {len(values)}")
        return field > values[0]
    
    elif op == FilterOp.LT:
        if len(values) != 1:
            raise ValueError(f"LT operator requires exactly 1 value, got {len(values)}")
        return field < values[0]
    
    elif op == FilterOp.GTE:
        if len(values) != 1:
            raise ValueError(f"GTE operator requires exactly 1 value, got {len(values)}")
        return field >= values[0]
    
    elif op == FilterOp.LTE:
        if len(values) != 1:
            raise ValueError(f"LTE operator requires exactly 1 value, got {len(values)}")
        return field <= values[0]
    
    elif op == FilterOp.BETWEEN:
        if len(values) != 2:
            raise ValueError(f"BETWEEN operator requires exactly 2 values, got {len(values)}")
        return field.between(values[0], values[1])
    
    elif op == FilterOp.LIKE:
        if len(values) != 1:
            raise ValueError(f"LIKE operator requires exactly 1 value, got {len(values)}")
        return field.like(values[0])
    
    else:
        raise ValueError(f"Unsupported filter operator: {op}")


# ============================================================
# 核心处理函数
# ============================================================
async def generate_sql(
    plan: QueryPlan,
    context: RequestContext,
    registry: SemanticRegistry,
    db_type: str
) -> str:
    """
    生成 SQL 查询字符串
    
    Args:
        plan: 验证后的查询计划
        context: 请求上下文
        registry: SemanticRegistry 实例
        db_type: 数据库类型（"mysql" 或 "postgresql"）
    
    Returns:
        str: SQL 查询字符串
    
    Raises:
        Stage4Error: 当 SQL 生成失败时抛出
    """
    # Step 1: Initialization (FROM Clause)
    # 确定主实体
    primary_entity_id = None
    if plan.metrics:
        # 逻辑 1: 通过第一个 metric 确定实体 (适用于 AGG/TREND)
        primary_metric = plan.metrics[0]
        metric_def = registry.get_metric_def(primary_metric.id)
        if not metric_def:
            raise Stage4Error(f"Metric definition not found: {primary_metric.id}")
        primary_entity_id = metric_def.get("entity_id")
        if not primary_entity_id:
            raise Stage4Error(f"Metric {primary_metric.id} has no entity_id")
    elif plan.dimensions:
        # 逻辑 2: 通过第一个 dimension 确定实体 (适用于 DETAIL)
        primary_dimension = plan.dimensions[0]
        dimension_def = registry.get_dimension_def(primary_dimension.id)
        if not dimension_def:
            raise Stage4Error(f"Dimension definition not found: {primary_dimension.id}")
        primary_entity_id = dimension_def.get("entity_id")
        if not primary_entity_id:
            raise Stage4Error(f"Dimension {primary_dimension.id} has no entity_id")
    else:
        # 逻辑 3: 既无 metric 也无 dimension，这是真正的非法 Plan
        raise Stage4Error("无法生成 SQL：查询计划中既没有指标也没有维度。")
    
    # 获取实体定义
    entity_def = registry.get_entity_def(primary_entity_id)
    if not entity_def:
        raise Stage4Error(f"Entity definition not found: {primary_entity_id}")
    
    # 获取物理表名
    semantic_view = entity_def.get("semantic_view")
    if not semantic_view:
        raise Stage4Error(f"Entity {primary_entity_id} has no semantic_view")
    
    # 创建 PyPika Table 和 Query 对象
    # 根据数据库类型选择对应的 Query 类，以生成正确的 SQL 方言
    table = Table(semantic_view)
    
    db_type_lower = db_type.lower()
    if db_type_lower == "mysql":
        # MySQL 使用反引号 `table_name`
        query = MySQLQuery.from_(table)
    elif db_type_lower == "postgresql":
        # PostgreSQL 使用双引号 "table_name"
        query = PostgreSQLQuery.from_(table)
    else:
        # 默认使用通用 Query（PostgreSQL 风格）
        query = Query.from_(table)
    
    logger.debug(
        f"Initialized query from entity {primary_entity_id}",
        extra={
            "semantic_view": semantic_view,
            "db_type": db_type,
            "query_class": query.__class__.__name__
        }
    )
    
    # Step 2: Projections (SELECT Clause)
    select_fields = []
    group_by_fields = []
    
    # 处理指标
    for metric in plan.metrics:
        metric_def = registry.get_metric_def(metric.id)
        if not metric_def:
            raise Stage4Error(f"Metric definition not found: {metric.id}")
        
        # 获取指标表达式
        expression = metric_def.get("expression", {})
        sql_expr = expression.get("sql")
        if not sql_expr:
            raise Stage4Error(f"Metric {metric.id} has no SQL expression")
        
        # 解析 SQL 表达式并构建 PyPika 对象
        # 支持格式：FUNC(column_name) 或 FUNC(DISTINCT column_name)
        import re
        # 匹配函数调用：FUNC(...)
        match = re.match(r"(\w+)\s*\((?:\s*DISTINCT\s+)?(\w+)\s*\)", sql_expr, re.IGNORECASE)
        if match:
            func_name = match.group(1).upper()
            col_name = match.group(2)
            col_field = Field(col_name, table=table)
            
            # 检查是否有 DISTINCT
            has_distinct = "DISTINCT" in sql_expr.upper()
            
            if func_name == "SUM":
                metric_expr = fn.Sum(col_field)
            elif func_name == "COUNT":
                if has_distinct:
                    metric_expr = fn.Count(col_field).distinct()
                else:
                    metric_expr = fn.Count(col_field)
            elif func_name == "AVG":
                metric_expr = fn.Avg(col_field)
            elif func_name == "MAX":
                metric_expr = fn.Max(col_field)
            elif func_name == "MIN":
                metric_expr = fn.Min(col_field)
            else:
                # 使用 CustomFunction
                custom_func = CustomFunction(func_name, [col_field])
                metric_expr = custom_func
        else:
            # 如果无法解析，尝试使用 CustomCriterion 包装原始 SQL
            # 注意：这不是最佳实践，但在 MVP 阶段可以接受
            logger.warning(
                f"Could not parse metric expression: {sql_expr}, using CustomCriterion",
                extra={"metric_id": metric.id, "sql_expr": sql_expr}
            )
            # 使用 CustomCriterion 包装原始 SQL 表达式
            # 注意：这需要确保 SQL 表达式中的列名正确引用表
            # 简化处理：直接使用原始 SQL（假设列名已经正确）
            metric_expr = CustomCriterion(sql_expr)
        
        # 添加别名
        metric_expr_alias = metric_expr.as_(metric.id)
        select_fields.append(metric_expr_alias)
    
    # 处理维度
    for dimension in plan.dimensions:
        dim_def = registry.get_dimension_def(dimension.id)
        if not dim_def:
            raise Stage4Error(f"Dimension definition not found: {dimension.id}")
        
        # 获取列名
        col_name = dim_def.get("column")
        if not col_name:
            raise Stage4Error(f"Dimension {dimension.id} has no column")
        
        col_field = Field(col_name, table=table)
        
        # 处理时间粒度
        if dimension.time_grain:
            grain = dimension.time_grain.value
            
            # 根据数据库类型和时间粒度构建 PyPika 表达式
            if db_type.lower() == "mysql":
                # MySQL 的时间截断函数
                if grain == "DAY":
                    dim_expr = fn.Date(col_field)
                elif grain == "MONTH":
                    # DATE_FORMAT(order_date, '%Y-%m-01')
                    date_format_func = CustomFunction("DATE_FORMAT", [col_field, "%Y-%m-01"])
                    dim_expr = date_format_func
                elif grain == "YEAR":
                    date_format_func = CustomFunction("DATE_FORMAT", [col_field, "%Y-01-01"])
                    dim_expr = date_format_func
                elif grain == "WEEK":
                    # DATE_SUB(order_date, INTERVAL WEEKDAY(order_date) DAY)
                    # 这是一个复杂的表达式，使用 CustomCriterion
                    truncation_sql = DialectAdapter.get_time_truncation_sql(
                        col_name=col_name,
                        grain=grain,
                        db_type=db_type
                    )
                    dim_expr = CustomCriterion(truncation_sql)
                elif grain == "QUARTER":
                    # MAKEDATE(YEAR(order_date), 1) + INTERVAL (QUARTER(order_date) - 1) QUARTER
                    # 这是一个复杂的表达式，使用 CustomCriterion
                    truncation_sql = DialectAdapter.get_time_truncation_sql(
                        col_name=col_name,
                        grain=grain,
                        db_type=db_type
                    )
                    dim_expr = CustomCriterion(truncation_sql)
                else:
                    raise ValueError(f"Unsupported time grain for MySQL: {grain}")
            else:
                # PostgreSQL: 使用 DATE_TRUNC 函数
                # DATE_TRUNC('day', order_date)
                date_trunc_func = CustomFunction("DATE_TRUNC", [grain.lower(), col_field])
                dim_expr = date_trunc_func
        else:
            dim_expr = col_field
        
        # 添加到 SELECT 和 GROUP BY
        dim_expr_alias = dim_expr.as_(dimension.id)
        select_fields.append(dim_expr_alias)
        group_by_fields.append(dim_expr)
    
    # 设置 SELECT 子句
    if select_fields:
        query = query.select(*select_fields)
    else:
        # 如果没有选择字段，选择所有列（不应该发生）
        query = query.select("*")
        logger.warning("No select fields, using SELECT *")
    
    # Step 3: Filtering (WHERE Clause)
    where_criteria = []
    
    # 时间范围过滤
    if plan.time_range:
        # 获取时间字段
        # 优先从 metric 获取，其次从 entity 获取
        time_field_id = None
        
        if plan.metrics:
            # 有 metrics：优先使用第一个 metric 的 default_time 配置
            primary_metric_def = registry.get_metric_def(plan.metrics[0].id)
            if primary_metric_def:
                time_field_id = primary_metric_def.get("default_time", {}).get("time_field_id")
        
        if not time_field_id:
            # 降级：从实体获取默认时间字段（适用于 DETAIL 查询或 metric 无配置的情况）
            time_field_id = entity_def.get("default_time_field_id")
        
        if time_field_id:
            # 获取时间字段的列名
            # 注意：time_field_id 可能是维度 ID（如 "DIM_ORDER_DATE"）
            # 或者直接是字段名（如 "ORDER_DATE"）
            time_dim_def = registry.get_dimension_def(time_field_id)
            if time_dim_def:
                time_col_name = time_dim_def.get("column")
            else:
                # 假设 time_field_id 就是列名
                time_col_name = time_field_id.lower()
            
            time_field = Field(time_col_name, table=table)
            
            # 计算时间范围边界
            start_date, end_date = _calculate_time_range_bounds(
                plan.time_range,
                context.current_date
            )
            
            if start_date:
                where_criteria.append(time_field >= start_date)
            if end_date:
                where_criteria.append(time_field <= end_date)
            
            logger.debug(
                "Added time range filter",
                extra={
                    "time_field": time_col_name,
                    "start": start_date,
                    "end": end_date
                }
            )
    
    # 标准过滤器
    for filter_item in plan.filters:
        filter_id = filter_item.id
        
        # 检查是否是逻辑过滤器（LF_*）
        if filter_id.startswith("LF_"):
            # 逻辑过滤器在后端处理，这里跳过
            # 或者可以从 registry 获取逻辑过滤器的 SQL
            logger.debug(f"Skipping logical filter {filter_id} (handled by backend)")
            continue
        
        # 获取过滤器对象的定义（可能是维度或指标）
        filter_def = registry.get_term(filter_id)
        if not filter_def:
            logger.warning(f"Filter definition not found: {filter_id}, skipping")
            continue
        
        # 获取列名
        col_name = filter_def.get("column")
        if not col_name:
            logger.warning(f"Filter {filter_id} has no column, skipping")
            continue
        
        filter_field = Field(col_name, table=table)
        
        # 映射操作符
        try:
            criterion = _map_filter_op_to_pypika(
                filter_field,
                filter_item.op,
                filter_item.values
            )
            where_criteria.append(criterion)
        except Exception as e:
            logger.error(
                f"Failed to map filter {filter_id} to PyPika criterion",
                extra={"error": str(e), "op": filter_item.op.value, "values": filter_item.values}
            )
            raise Stage4Error(f"Failed to process filter {filter_id}: {str(e)}") from e
    
    # 租户 ID 过滤（多租户支持）
    if context.tenant_id:
        tenant_field = Field("tenant_id", table=table)
        where_criteria.append(tenant_field == context.tenant_id)
        logger.debug(f"Added tenant filter: {context.tenant_id}")
    
    # RLS (Row-Level Security) 策略
    rls_policies = registry.get_rls_policies(context.role_id, primary_entity_id)
    for rls_sql in rls_policies:
        if rls_sql:
            # 使用 CustomCriterion 包装 RLS SQL 片段
            rls_criterion = CustomCriterion(rls_sql)
            where_criteria.append(rls_criterion)
            logger.debug(f"Added RLS policy: {rls_sql}")
    
    # 组合所有 WHERE 条件
    if where_criteria:
        combined_criterion = Criterion.all(where_criteria)
        query = query.where(combined_criterion)
    
    # GROUP BY 子句
    if group_by_fields and plan.intent in [PlanIntent.AGG, PlanIntent.TREND]:
        query = query.groupby(*group_by_fields)
    
    # Step 4: Final Assembly & Rendering
    # ORDER BY 子句
    for order_item in plan.order_by:
        order_def = registry.get_term(order_item.id)
        if not order_def:
            logger.warning(f"Order field definition not found: {order_item.id}, skipping")
            continue
        
        # 在 ORDER BY 中，我们应该使用 SELECT 中的别名
        # 这样可以正确处理有时间粒度的维度和聚合指标
        # PyPika 中，使用 Field(alias_name) 可以引用 SELECT 中的别名
        order_field = Field(order_item.id)
        
        # 添加排序
        if order_item.direction.value == "ASC":
            query = query.orderby(order_field)
        else:
            query = query.orderby(order_field, order=Query.Order.desc)
    
    # LIMIT 子句
    if plan.limit:
        query = query.limit(plan.limit)
    
    # 渲染 SQL
    try:
        sql_string = query.get_sql()
        
        logger.info(
            f"Stage 4 完成 | SQL 生成 | SELECT: {len(select_fields)}, WHERE: {len(where_criteria)}, GROUP BY: {len(group_by_fields)} | SQL 长度: {len(sql_string)}",
            extra={
                "sql_length": len(sql_string),
                "select_count": len(select_fields),
                "where_count": len(where_criteria),
                "group_by_count": len(group_by_fields),
                "order_by_count": len(plan.order_by),
                "has_limit": plan.limit is not None
            }
        )
        
        return sql_string
    
    except Exception as e:
        logger.error(
            "Failed to render SQL",
            extra={"error": str(e), "query": str(query)}
        )
        raise Stage4Error(f"Failed to render SQL: {str(e)}") from e







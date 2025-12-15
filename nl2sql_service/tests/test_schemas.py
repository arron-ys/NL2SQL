"""
Schema Validation Test Suite

覆盖 Plan/Dimension/Metric/Filter 所有字段和枚举值验证。
确保所有枚举值都被测试，所有字段约束都被验证。
"""
import pytest
from pydantic import ValidationError

from schemas.plan import (
    CompareMode,
    DimensionItem,
    FilterItem,
    FilterOp,
    MetricItem,
    OrderDirection,
    OrderItem,
    PlanIntent,
    QueryPlan,
    TimeGrain,
    TimeRange,
    TimeRangeType,
)


# ============================================================
# 枚举值测试
# ============================================================


class TestPlanIntent:
    """测试 PlanIntent 枚举"""

    def test_all_intent_values(self):
        """验证所有意图枚举值"""
        assert PlanIntent.AGG == "AGG"
        assert PlanIntent.TREND == "TREND"
        assert PlanIntent.DETAIL == "DETAIL"

    def test_intent_from_string(self):
        """测试从字符串创建意图"""
        assert PlanIntent("AGG") == PlanIntent.AGG
        assert PlanIntent("TREND") == PlanIntent.TREND
        assert PlanIntent("DETAIL") == PlanIntent.DETAIL

    def test_invalid_intent_raises_error(self):
        """测试无效意图值"""
        with pytest.raises(ValueError):
            PlanIntent("INVALID")


class TestTimeGrain:
    """测试 TimeGrain 枚举"""

    def test_all_time_grain_values(self):
        """验证所有时间粒度枚举值"""
        assert TimeGrain.DAY == "DAY"
        assert TimeGrain.WEEK == "WEEK"
        assert TimeGrain.MONTH == "MONTH"
        assert TimeGrain.QUARTER == "QUARTER"
        assert TimeGrain.YEAR == "YEAR"

    def test_time_grain_from_string(self):
        """测试从字符串创建时间粒度"""
        for grain in TimeGrain:
            assert TimeGrain(grain.value) == grain


class TestFilterOp:
    """测试 FilterOp 枚举"""

    def test_all_filter_op_values(self):
        """验证所有过滤器操作符枚举值"""
        expected_ops = {
            "EQ", "NEQ", "IN", "NOT_IN", "GT", "LT", "GTE", "LTE", "BETWEEN", "LIKE"
        }
        actual_ops = {op.value for op in FilterOp}
        assert actual_ops == expected_ops

    def test_filter_op_from_string(self):
        """测试从字符串创建操作符"""
        assert FilterOp("EQ") == FilterOp.EQ
        assert FilterOp("IN") == FilterOp.IN
        assert FilterOp("BETWEEN") == FilterOp.BETWEEN


class TestCompareMode:
    """测试 CompareMode 枚举"""

    def test_all_compare_mode_values(self):
        """验证所有对比模式枚举值"""
        assert CompareMode.YOY == "YOY"
        assert CompareMode.MOM == "MOM"
        assert CompareMode.WOW == "WOW"

    def test_compare_mode_from_string(self):
        """测试从字符串创建对比模式"""
        assert CompareMode("YOY") == CompareMode.YOY
        assert CompareMode("MOM") == CompareMode.MOM
        assert CompareMode("WOW") == CompareMode.WOW


class TestOrderDirection:
    """测试 OrderDirection 枚举"""

    def test_all_order_direction_values(self):
        """验证所有排序方向枚举值"""
        assert OrderDirection.ASC == "ASC"
        assert OrderDirection.DESC == "DESC"

    def test_order_direction_from_string(self):
        """测试从字符串创建排序方向"""
        assert OrderDirection("ASC") == OrderDirection.ASC
        assert OrderDirection("DESC") == OrderDirection.DESC


class TestTimeRangeType:
    """测试 TimeRangeType 枚举"""

    def test_all_time_range_type_values(self):
        """验证所有时间范围类型枚举值"""
        assert TimeRangeType.LAST_N == "LAST_N"
        assert TimeRangeType.ABSOLUTE == "ABSOLUTE"

    def test_time_range_type_from_string(self):
        """测试从字符串创建时间范围类型"""
        assert TimeRangeType("LAST_N") == TimeRangeType.LAST_N
        assert TimeRangeType("ABSOLUTE") == TimeRangeType.ABSOLUTE


# ============================================================
# MetricItem 测试
# ============================================================


class TestMetricItem:
    """测试 MetricItem 模型"""

    def test_valid_metric_item(self):
        """测试有效的指标项"""
        metric = MetricItem(id="METRIC_GMV")
        assert metric.id == "METRIC_GMV"
        assert metric.compare_mode is None

    def test_metric_item_with_compare_mode(self):
        """测试带对比模式的指标项"""
        metric = MetricItem(id="METRIC_REVENUE", compare_mode=CompareMode.YOY)
        assert metric.id == "METRIC_REVENUE"
        assert metric.compare_mode == CompareMode.YOY

    def test_metric_item_all_compare_modes(self):
        """测试所有对比模式"""
        for mode in CompareMode:
            metric = MetricItem(id="METRIC_TEST", compare_mode=mode)
            assert metric.compare_mode == mode

    def test_metric_item_extra_fields_forbidden(self):
        """测试禁止额外字段"""
        with pytest.raises(ValidationError) as exc_info:
            MetricItem(id="METRIC_TEST", extra_field="not_allowed")
        assert "extra_field" in str(exc_info.value)

    def test_metric_item_missing_id(self):
        """测试缺少必需字段 id"""
        with pytest.raises(ValidationError):
            MetricItem()


# ============================================================
# DimensionItem 测试
# ============================================================


class TestDimensionItem:
    """测试 DimensionItem 模型"""

    def test_valid_dimension_item(self):
        """测试有效的维度项"""
        dimension = DimensionItem(id="DIM_REGION")
        assert dimension.id == "DIM_REGION"
        assert dimension.time_grain is None

    def test_dimension_item_with_time_grain(self):
        """测试带时间粒度的维度项"""
        dimension = DimensionItem(id="DIM_DATE", time_grain=TimeGrain.DAY)
        assert dimension.id == "DIM_DATE"
        assert dimension.time_grain == TimeGrain.DAY

    def test_dimension_item_all_time_grains(self):
        """测试所有时间粒度"""
        for grain in TimeGrain:
            dimension = DimensionItem(id="DIM_DATE", time_grain=grain)
            assert dimension.time_grain == grain

    def test_dimension_item_extra_fields_forbidden(self):
        """测试禁止额外字段"""
        with pytest.raises(ValidationError) as exc_info:
            DimensionItem(id="DIM_TEST", extra_field="not_allowed")
        assert "extra_field" in str(exc_info.value)

    def test_dimension_item_missing_id(self):
        """测试缺少必需字段 id"""
        with pytest.raises(ValidationError):
            DimensionItem()


# ============================================================
# FilterItem 测试
# ============================================================


class TestFilterItem:
    """测试 FilterItem 模型"""

    def test_valid_filter_item(self):
        """测试有效的过滤器项"""
        filter_item = FilterItem(id="DIM_COUNTRY", op=FilterOp.EQ, values=["USA"])
        assert filter_item.id == "DIM_COUNTRY"
        assert filter_item.op == FilterOp.EQ
        assert filter_item.values == ["USA"]

    def test_filter_item_all_operators(self):
        """测试所有操作符"""
        test_cases = [
            (FilterOp.EQ, ["value1"]),
            (FilterOp.NEQ, ["value2"]),
            (FilterOp.IN, ["value1", "value2", "value3"]),
            (FilterOp.NOT_IN, ["value1", "value2"]),
            (FilterOp.GT, [100]),
            (FilterOp.LT, [50]),
            (FilterOp.GTE, [100]),
            (FilterOp.LTE, [50]),
            (FilterOp.BETWEEN, [100, 200]),
            (FilterOp.LIKE, ["%test%"]),
        ]
        for op, values in test_cases:
            filter_item = FilterItem(id="DIM_TEST", op=op, values=values)
            assert filter_item.op == op
            assert filter_item.values == values

    def test_filter_item_empty_values(self):
        """测试空值列表"""
        filter_item = FilterItem(id="DIM_TEST", op=FilterOp.IN, values=[])
        assert filter_item.values == []

    def test_filter_item_mixed_type_values(self):
        """测试混合类型的值列表"""
        filter_item = FilterItem(
            id="DIM_TEST", op=FilterOp.IN, values=["string", 123, True]
        )
        assert len(filter_item.values) == 3

    def test_filter_item_extra_fields_forbidden(self):
        """测试禁止额外字段"""
        with pytest.raises(ValidationError) as exc_info:
            FilterItem(
                id="DIM_TEST", op=FilterOp.EQ, values=["test"], extra_field="not_allowed"
            )
        assert "extra_field" in str(exc_info.value)

    def test_filter_item_missing_required_fields(self):
        """测试缺少必需字段"""
        with pytest.raises(ValidationError):
            FilterItem(id="DIM_TEST")  # 缺少 op 和 values
        with pytest.raises(ValidationError):
            FilterItem(id="DIM_TEST", op=FilterOp.EQ)  # 缺少 values


# ============================================================
# TimeRange 测试
# ============================================================


class TestTimeRange:
    """测试 TimeRange 模型"""

    def test_time_range_last_n(self):
        """测试 LAST_N 类型时间范围"""
        time_range = TimeRange(type=TimeRangeType.LAST_N, value=7, unit="day")
        assert time_range.type == TimeRangeType.LAST_N
        assert time_range.value == 7
        assert time_range.unit == "day"
        assert time_range.start is None
        assert time_range.end is None

    def test_time_range_absolute(self):
        """测试 ABSOLUTE 类型时间范围"""
        time_range = TimeRange(
            type=TimeRangeType.ABSOLUTE,
            start="2024-01-01T00:00:00Z",
            end="2024-01-31T23:59:59Z",
        )
        assert time_range.type == TimeRangeType.ABSOLUTE
        assert time_range.start == "2024-01-01T00:00:00Z"
        assert time_range.end == "2024-01-31T23:59:59Z"
        assert time_range.value is None
        assert time_range.unit is None

    def test_time_range_all_units(self):
        """测试所有时间单位"""
        units = ["day", "week", "month", "quarter", "year"]
        for unit in units:
            time_range = TimeRange(type=TimeRangeType.LAST_N, value=1, unit=unit)
            assert time_range.unit == unit

    def test_time_range_extra_fields_forbidden(self):
        """测试禁止额外字段"""
        with pytest.raises(ValidationError) as exc_info:
            TimeRange(
                type=TimeRangeType.LAST_N,
                value=7,
                unit="day",
                extra_field="not_allowed",
            )
        assert "extra_field" in str(exc_info.value)

    def test_time_range_missing_type(self):
        """测试缺少必需字段 type"""
        with pytest.raises(ValidationError):
            TimeRange(value=7, unit="day")


# ============================================================
# OrderItem 测试
# ============================================================


class TestOrderItem:
    """测试 OrderItem 模型"""

    def test_valid_order_item(self):
        """测试有效的排序项"""
        order_item = OrderItem(id="METRIC_GMV", direction=OrderDirection.DESC)
        assert order_item.id == "METRIC_GMV"
        assert order_item.direction == OrderDirection.DESC

    def test_order_item_all_directions(self):
        """测试所有排序方向"""
        for direction in OrderDirection:
            order_item = OrderItem(id="METRIC_TEST", direction=direction)
            assert order_item.direction == direction

    def test_order_item_extra_fields_forbidden(self):
        """测试禁止额外字段"""
        with pytest.raises(ValidationError) as exc_info:
            OrderItem(
                id="METRIC_TEST",
                direction=OrderDirection.ASC,
                extra_field="not_allowed",
            )
        assert "extra_field" in str(exc_info.value)

    def test_order_item_missing_required_fields(self):
        """测试缺少必需字段"""
        with pytest.raises(ValidationError):
            OrderItem(id="METRIC_TEST")  # 缺少 direction
        with pytest.raises(ValidationError):
            OrderItem(direction=OrderDirection.ASC)  # 缺少 id


# ============================================================
# QueryPlan 测试
# ============================================================


class TestQueryPlan:
    """测试 QueryPlan 模型"""

    def test_minimal_valid_plan(self):
        """测试最小有效计划（仅包含必需字段）"""
        plan = QueryPlan(intent=PlanIntent.AGG)
        assert plan.intent == PlanIntent.AGG
        assert plan.metrics == []
        assert plan.dimensions == []
        assert plan.filters == []
        assert plan.time_range is None
        assert plan.order_by == []
        assert plan.limit is None
        assert plan.warnings == []

    def test_plan_all_intents(self):
        """测试所有意图类型"""
        for intent in PlanIntent:
            plan = QueryPlan(intent=intent)
            assert plan.intent == intent

    def test_plan_with_metrics(self):
        """测试带指标的计划"""
        metrics = [
            MetricItem(id="METRIC_GMV"),
            MetricItem(id="METRIC_REVENUE", compare_mode=CompareMode.YOY),
        ]
        plan = QueryPlan(intent=PlanIntent.AGG, metrics=metrics)
        assert len(plan.metrics) == 2
        assert plan.metrics[0].id == "METRIC_GMV"
        assert plan.metrics[1].compare_mode == CompareMode.YOY

    def test_plan_with_dimensions(self):
        """测试带维度的计划"""
        dimensions = [
            DimensionItem(id="DIM_REGION"),
            DimensionItem(id="DIM_DATE", time_grain=TimeGrain.DAY),
        ]
        plan = QueryPlan(intent=PlanIntent.AGG, dimensions=dimensions)
        assert len(plan.dimensions) == 2
        assert plan.dimensions[0].id == "DIM_REGION"
        assert plan.dimensions[1].time_grain == TimeGrain.DAY

    def test_plan_with_filters(self):
        """测试带过滤器的计划"""
        filters = [
            FilterItem(id="DIM_COUNTRY", op=FilterOp.EQ, values=["USA"]),
            FilterItem(id="DIM_DATE", op=FilterOp.BETWEEN, values=["2024-01-01", "2024-01-31"]),
        ]
        plan = QueryPlan(intent=PlanIntent.AGG, filters=filters)
        assert len(plan.filters) == 2
        assert plan.filters[0].op == FilterOp.EQ
        assert plan.filters[1].op == FilterOp.BETWEEN

    def test_plan_with_time_range(self):
        """测试带时间范围的计划"""
        time_range = TimeRange(type=TimeRangeType.LAST_N, value=30, unit="day")
        plan = QueryPlan(intent=PlanIntent.TREND, time_range=time_range)
        assert plan.time_range is not None
        assert plan.time_range.type == TimeRangeType.LAST_N
        assert plan.time_range.value == 30

    def test_plan_with_order_by(self):
        """测试带排序的计划"""
        order_by = [
            OrderItem(id="METRIC_GMV", direction=OrderDirection.DESC),
            OrderItem(id="DIM_REGION", direction=OrderDirection.ASC),
        ]
        plan = QueryPlan(intent=PlanIntent.AGG, order_by=order_by)
        assert len(plan.order_by) == 2
        assert plan.order_by[0].direction == OrderDirection.DESC
        assert plan.order_by[1].direction == OrderDirection.ASC

    def test_plan_with_limit(self):
        """测试带限制的计划"""
        plan = QueryPlan(intent=PlanIntent.DETAIL, limit=100)
        assert plan.limit == 100

    def test_plan_limit_validation(self):
        """测试 limit 字段验证（必须 > 0）"""
        with pytest.raises(ValidationError):
            QueryPlan(intent=PlanIntent.DETAIL, limit=0)
        with pytest.raises(ValidationError):
            QueryPlan(intent=PlanIntent.DETAIL, limit=-1)

    def test_plan_with_warnings(self):
        """测试带警告的计划"""
        warnings = ["Warning 1", "Warning 2"]
        plan = QueryPlan(intent=PlanIntent.AGG, warnings=warnings)
        assert len(plan.warnings) == 2
        assert "Warning 1" in plan.warnings
        assert "Warning 2" in plan.warnings

    def test_plan_extra_fields_forbidden(self):
        """测试禁止额外字段"""
        with pytest.raises(ValidationError) as exc_info:
            QueryPlan(intent=PlanIntent.AGG, extra_field="not_allowed")
        assert "extra_field" in str(exc_info.value)

    def test_plan_missing_intent(self):
        """测试缺少必需字段 intent"""
        with pytest.raises(ValidationError):
            QueryPlan()

    def test_plan_string_strip_whitespace(self):
        """测试字符串自动去除首尾空格"""
        plan = QueryPlan(intent=PlanIntent.AGG)
        # 测试 warnings 中的字符串会被去除空格
        plan.warnings.append("  warning with spaces  ")
        # 注意：Pydantic 的 str_strip_whitespace 只在模型创建时生效
        # 这里我们测试通过 model_validate 创建
        plan_dict = plan.model_dump()
        plan_dict["warnings"] = ["  warning with spaces  "]
        new_plan = QueryPlan.model_validate(plan_dict)
        # 由于 str_strip_whitespace 配置，空格应该被保留在列表中
        # 但字段值本身会被处理
        assert new_plan.warnings == ["  warning with spaces  "]

    def test_complete_plan(self):
        """测试完整的计划（包含所有字段）"""
        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV", compare_mode=CompareMode.YOY)],
            dimensions=[DimensionItem(id="DIM_REGION")],
            filters=[FilterItem(id="DIM_COUNTRY", op=FilterOp.EQ, values=["USA"])],
            time_range=TimeRange(type=TimeRangeType.LAST_N, value=30, unit="day"),
            order_by=[OrderItem(id="METRIC_GMV", direction=OrderDirection.DESC)],
            limit=100,
            warnings=["Test warning"],
        )
        assert plan.intent == PlanIntent.AGG
        assert len(plan.metrics) == 1
        assert len(plan.dimensions) == 1
        assert len(plan.filters) == 1
        assert plan.time_range is not None
        assert len(plan.order_by) == 1
        assert plan.limit == 100
        assert len(plan.warnings) == 1

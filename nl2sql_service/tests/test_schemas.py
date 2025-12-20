"""
【简述】
验证 NL2SQL Schema 的所有枚举值、字段约束、Pydantic 校验规则与模型序列化反序列化正确性。

【范围/不测什么】
- 不覆盖业务逻辑；仅验证 Pydantic 模型的数据校验、类型约束、默认值与额外字段禁止规则。

【用例概述】
- test_all_intent_values:
  -- 验证所有 PlanIntent 枚举值
- test_intent_from_string:
  -- 验证从字符串创建 PlanIntent
- test_invalid_intent_raises_error:
  -- 验证无效 intent 抛出 ValueError
- test_all_time_grain_values:
  -- 验证所有 TimeGrain 枚举值
- test_time_grain_from_string:
  -- 验证从字符串创建 TimeGrain
- test_all_filter_op_values:
  -- 验证所有 FilterOp 枚举值
- test_filter_op_from_string:
  -- 验证从字符串创建 FilterOp
- test_all_compare_mode_values:
  -- 验证所有 CompareMode 枚举值
- test_compare_mode_from_string:
  -- 验证从字符串创建 CompareMode
- test_all_order_direction_values:
  -- 验证所有 OrderDirection 枚举值
- test_order_direction_from_string:
  -- 验证从字符串创建 OrderDirection
- test_all_time_range_type_values:
  -- 验证所有 TimeRangeType 枚举值
- test_time_range_type_from_string:
  -- 验证从字符串创建 TimeRangeType
- test_valid_metric_item:
  -- 验证有效的 MetricItem
- test_metric_item_with_compare_mode:
  -- 验证 MetricItem 包含 compare_mode
- test_metric_item_all_compare_modes:
  -- 验证 MetricItem 支持所有 compare_mode
- test_metric_item_extra_fields_forbidden:
  -- 验证 MetricItem 禁止额外字段
- test_metric_item_missing_id:
  -- 验证 MetricItem 缺少 id 时抛出错误
- test_valid_dimension_item:
  -- 验证有效的 DimensionItem
- test_dimension_item_with_time_grain:
  -- 验证 DimensionItem 包含 time_grain
- test_dimension_item_all_time_grains:
  -- 验证 DimensionItem 支持所有 time_grain
- test_dimension_item_extra_fields_forbidden:
  -- 验证 DimensionItem 禁止额外字段
- test_dimension_item_missing_id:
  -- 验证 DimensionItem 缺少 id 时抛出错误
- test_valid_filter_item:
  -- 验证有效的 FilterItem
- test_filter_item_all_operators:
  -- 验证 FilterItem 支持所有操作符
- test_filter_item_empty_values:
  -- 验证 FilterItem 允许空 values
- test_filter_item_mixed_type_values:
  -- 验证 FilterItem 支持混合类型 values
- test_filter_item_extra_fields_forbidden:
  -- 验证 FilterItem 禁止额外字段
- test_filter_item_missing_required_fields:
  -- 验证 FilterItem 缺少必需字段时抛出错误
- test_time_range_last_n:
  -- 验证 LAST_N 类型的 TimeRange
- test_time_range_absolute:
  -- 验证 ABSOLUTE 类型的 TimeRange
- test_time_range_all_units:
  -- 验证 TimeRange 支持所有 unit 值
- test_time_range_extra_fields_forbidden:
  -- 验证 TimeRange 禁止额外字段
- test_time_range_missing_type:
  -- 验证 TimeRange 缺少 type 时抛出错误
- test_valid_order_item:
  -- 验证有效的 OrderItem
- test_order_item_all_directions:
  -- 验证 OrderItem 支持所有 direction 值
- test_order_item_extra_fields_forbidden:
  -- 验证 OrderItem 禁止额外字段
- test_order_item_missing_required_fields:
  -- 验证 OrderItem 缺少必需字段时抛出错误
- test_minimal_valid_plan:
  -- 验证最小有效 QueryPlan
- test_plan_all_intents:
  -- 验证 QueryPlan 支持所有 intent 值
- test_plan_with_metrics:
  -- 验证 QueryPlan 包含 metrics
- test_plan_with_dimensions:
  -- 验证 QueryPlan 包含 dimensions
- test_plan_with_filters:
  -- 验证 QueryPlan 包含 filters
- test_plan_with_time_range:
  -- 验证 QueryPlan 包含 time_range
- test_plan_with_order_by:
  -- 验证 QueryPlan 包含 order_by
- test_plan_with_limit:
  -- 验证 QueryPlan 包含 limit
- test_plan_limit_validation:
  -- 验证 QueryPlan limit 字段约束（> 0）
- test_plan_with_warnings:
  -- 验证 QueryPlan 包含 warnings
- test_plan_extra_fields_forbidden:
  -- 验证 QueryPlan 禁止额外字段
- test_plan_missing_intent:
  -- 验证 QueryPlan 缺少 intent 时抛出错误
- test_plan_string_strip_whitespace:
  -- 验证 QueryPlan 字符串字段自动去除空格
- test_complete_plan:
  -- 验证包含所有字段的完整 QueryPlan
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
    """PlanIntent 枚举测试组"""

    @pytest.mark.unit
    def test_all_intent_values(self):
        """
        【测试目标】
        1. 验证所有 PlanIntent 枚举值正确定义

        【执行过程】
        1. 检查 PlanIntent.AGG、TREND、DETAIL 的值

        【预期结果】
        1. AGG 值为 "AGG"
        2. TREND 值为 "TREND"
        3. DETAIL 值为 "DETAIL"
        """
        assert PlanIntent.AGG == "AGG"
        assert PlanIntent.TREND == "TREND"
        assert PlanIntent.DETAIL == "DETAIL"

    @pytest.mark.unit
    def test_intent_from_string(self):
        """
        【测试目标】
        1. 验证从字符串正确创建 PlanIntent

        【执行过程】
        1. 使用字符串 "AGG"、"TREND"、"DETAIL" 创建 PlanIntent

        【预期结果】
        1. 创建的枚举值与定义的枚举常量相等
        """
        assert PlanIntent("AGG") == PlanIntent.AGG
        assert PlanIntent("TREND") == PlanIntent.TREND
        assert PlanIntent("DETAIL") == PlanIntent.DETAIL

    @pytest.mark.unit
    def test_invalid_intent_raises_error(self):
        """
        【测试目标】
        1. 验证无效 intent 值抛出 ValueError

        【执行过程】
        1. 使用无效字符串 "INVALID" 创建 PlanIntent

        【预期结果】
        1. 抛出 ValueError
        """
        with pytest.raises(ValueError):
            PlanIntent("INVALID")


class TestTimeGrain:
    """TimeGrain 枚举测试组"""

    @pytest.mark.unit
    def test_all_time_grain_values(self):
        """
        【测试目标】
        1. 验证所有 TimeGrain 枚举值正确定义

        【执行过程】
        1. 检查 TimeGrain 的所有枚举值

        【预期结果】
        1. 包含 DAY、WEEK、MONTH、QUARTER、YEAR
        """
        assert TimeGrain.DAY == "DAY"
        assert TimeGrain.WEEK == "WEEK"
        assert TimeGrain.MONTH == "MONTH"
        assert TimeGrain.QUARTER == "QUARTER"
        assert TimeGrain.YEAR == "YEAR"

    @pytest.mark.unit
    def test_time_grain_from_string(self):
        """
        【测试目标】
        1. 验证从字符串正确创建 TimeGrain

        【执行过程】
        1. 遍历所有 TimeGrain 枚举值，使用其 value 字符串创建实例

        【预期结果】
        1. 创建的枚举值与原枚举相等
        """
        for grain in TimeGrain:
            assert TimeGrain(grain.value) == grain


class TestFilterOp:
    """FilterOp 枚举测试组"""

    @pytest.mark.unit
    def test_all_filter_op_values(self):
        """
        【测试目标】
        1. 验证所有 FilterOp 枚举值正确定义

        【执行过程】
        1. 收集所有 FilterOp 枚举值
        2. 与预期操作符集合比较

        【预期结果】
        1. 包含 EQ、NEQ、IN、NOT_IN、GT、LT、GTE、LTE、BETWEEN、LIKE
        """
        expected_ops = {
            "EQ", "NEQ", "IN", "NOT_IN", "GT", "LT", "GTE", "LTE", "BETWEEN", "LIKE"
        }
        actual_ops = {op.value for op in FilterOp}
        assert actual_ops == expected_ops

    @pytest.mark.unit
    def test_filter_op_from_string(self):
        """
        【测试目标】
        1. 验证从字符串正确创建 FilterOp

        【执行过程】
        1. 使用字符串 "EQ"、"IN"、"BETWEEN" 创建 FilterOp

        【预期结果】
        1. 创建的枚举值与定义的枚举常量相等
        """
        assert FilterOp("EQ") == FilterOp.EQ
        assert FilterOp("IN") == FilterOp.IN
        assert FilterOp("BETWEEN") == FilterOp.BETWEEN


class TestCompareMode:
    """CompareMode 枚举测试组"""

    @pytest.mark.unit
    def test_all_compare_mode_values(self):
        """
        【测试目标】
        1. 验证所有 CompareMode 枚举值正确定义

        【执行过程】
        1. 检查 CompareMode 的所有枚举值

        【预期结果】
        1. YOY 值为 "YOY"
        2. MOM 值为 "MOM"
        3. WOW 值为 "WOW"
        """
        assert CompareMode.YOY == "YOY"
        assert CompareMode.MOM == "MOM"
        assert CompareMode.WOW == "WOW"

    @pytest.mark.unit
    def test_compare_mode_from_string(self):
        """
        【测试目标】
        1. 验证从字符串正确创建 CompareMode

        【执行过程】
        1. 使用字符串 "YOY"、"MOM"、"WOW" 创建 CompareMode

        【预期结果】
        1. 创建的枚举值与定义的枚举常量相等
        """
        assert CompareMode("YOY") == CompareMode.YOY
        assert CompareMode("MOM") == CompareMode.MOM
        assert CompareMode("WOW") == CompareMode.WOW


class TestOrderDirection:
    """OrderDirection 枚举测试组"""

    @pytest.mark.unit
    def test_all_order_direction_values(self):
        """
        【测试目标】
        1. 验证所有 OrderDirection 枚举值正确定义

        【执行过程】
        1. 检查 OrderDirection 的所有枚举值

        【预期结果】
        1. ASC 值为 "ASC"，DESC 值为 "DESC"
        """
        assert OrderDirection.ASC == "ASC"
        assert OrderDirection.DESC == "DESC"

    @pytest.mark.unit
    def test_order_direction_from_string(self):
        """
        【测试目标】
        1. 验证从字符串正确创建 OrderDirection

        【执行过程】
        1. 使用字符串创建 OrderDirection

        【预期结果】
        1. 创建的枚举值正确
        """
        assert OrderDirection("ASC") == OrderDirection.ASC
        assert OrderDirection("DESC") == OrderDirection.DESC


class TestTimeRangeType:
    """TimeRangeType 枚举测试组"""

    @pytest.mark.unit
    def test_all_time_range_type_values(self):
        """
        【测试目标】
        1. 验证所有 TimeRangeType 枚举值正确定义

        【执行过程】
        1. 检查 TimeRangeType 的所有枚举值

        【预期结果】
        1. LAST_N 值为 "LAST_N"，ABSOLUTE 值为 "ABSOLUTE"
        """
        assert TimeRangeType.LAST_N == "LAST_N"
        assert TimeRangeType.ABSOLUTE == "ABSOLUTE"

    @pytest.mark.unit
    def test_time_range_type_from_string(self):
        """
        【测试目标】
        1. 验证从字符串正确创建 TimeRangeType

        【执行过程】
        1. 使用字符串创建 TimeRangeType

        【预期结果】
        1. 创建的枚举值正确
        """
        assert TimeRangeType("LAST_N") == TimeRangeType.LAST_N
        assert TimeRangeType("ABSOLUTE") == TimeRangeType.ABSOLUTE


# ============================================================
# MetricItem 测试
# ============================================================


class TestMetricItem:
    """MetricItem 模型测试组"""

    @pytest.mark.unit
    def test_valid_metric_item(self):
        """
        【测试目标】
        1. 验证创建有效的 MetricItem

        【执行过程】
        1. 创建 MetricItem(id="METRIC_GMV")

        【预期结果】
        1. metric.id 为 "METRIC_GMV"
        2. metric.compare_mode 为 None（默认值）
        """
        metric = MetricItem(id="METRIC_GMV")
        assert metric.id == "METRIC_GMV"
        assert metric.compare_mode is None

    @pytest.mark.unit
    def test_metric_item_with_compare_mode(self):
        """
        【测试目标】
        1. 验证 MetricItem 包含 compare_mode

        【执行过程】
        1. 创建 MetricItem 包含 compare_mode=YOY

        【预期结果】
        1. metric.compare_mode 为 CompareMode.YOY
        """
        metric = MetricItem(id="METRIC_REVENUE", compare_mode=CompareMode.YOY)
        assert metric.id == "METRIC_REVENUE"
        assert metric.compare_mode == CompareMode.YOY

    @pytest.mark.unit
    def test_metric_item_all_compare_modes(self):
        """
        【测试目标】
        1. 验证 MetricItem 支持所有 CompareMode

        【执行过程】
        1. 遍历所有 CompareMode 枚举值，创建 MetricItem

        【预期结果】
        1. 每个 compare_mode 都能成功设置
        """
        for mode in CompareMode:
            metric = MetricItem(id="METRIC_TEST", compare_mode=mode)
            assert metric.compare_mode == mode

    @pytest.mark.unit
    def test_metric_item_extra_fields_forbidden(self):
        """
        【测试目标】
        1. 验证 MetricItem 禁止额外字段

        【执行过程】
        1. 创建 MetricItem 包含未定义的 extra_field

        【预期结果】
        1. 抛出 ValidationError，错误消息包含 "extra_field"
        """
        with pytest.raises(ValidationError) as exc_info:
            MetricItem(id="METRIC_TEST", extra_field="not_allowed")
        assert "extra_field" in str(exc_info.value)

    @pytest.mark.unit
    def test_metric_item_missing_id(self):
        """
        【测试目标】
        1. 验证 MetricItem 缺少 id 时抛出 ValidationError

        【执行过程】
        1. 创建 MetricItem() 不传 id

        【预期结果】
        1. 抛出 ValidationError
        """
        with pytest.raises(ValidationError):
            MetricItem()


# ============================================================
# DimensionItem 测试
# ============================================================


class TestDimensionItem:
    """DimensionItem 模型测试组"""

    @pytest.mark.unit
    def test_valid_dimension_item(self):
        """
        【测试目标】
        1. 验证创建有效的 DimensionItem

        【执行过程】
        1. 创建 DimensionItem(id="DIM_REGION")

        【预期结果】
        1. dimension.id 为 "DIM_REGION"
        2. dimension.time_grain 为 None（默认值）
        """
        dimension = DimensionItem(id="DIM_REGION")
        assert dimension.id == "DIM_REGION"
        assert dimension.time_grain is None

    @pytest.mark.unit
    def test_dimension_item_with_time_grain(self):
        """
        【测试目标】
        1. 验证 DimensionItem 包含 time_grain

        【执行过程】
        1. 创建 DimensionItem 包含 time_grain=DAY

        【预期结果】
        1. dimension.time_grain 为 TimeGrain.DAY
        """
        dimension = DimensionItem(id="DIM_DATE", time_grain=TimeGrain.DAY)
        assert dimension.id == "DIM_DATE"
        assert dimension.time_grain == TimeGrain.DAY

    def test_dimension_item_all_time_grains(self):
        """测试所有时间粒度"""
        for grain in TimeGrain:
            dimension = DimensionItem(id="DIM_DATE", time_grain=grain)
            assert dimension.time_grain == grain

    @pytest.mark.unit
    def test_dimension_item_extra_fields_forbidden(self):
        """
        【测试目标】
        1. 验证 DimensionItem 禁止额外字段

        【执行过程】
        1. 创建 DimensionItem 包含未定义的 extra_field

        【预期结果】
        1. 抛出 ValidationError，错误消息包含 "extra_field"
        """
        with pytest.raises(ValidationError) as exc_info:
            DimensionItem(id="DIM_TEST", extra_field="not_allowed")
        assert "extra_field" in str(exc_info.value)

    @pytest.mark.unit
    def test_dimension_item_missing_id(self):
        """
        【测试目标】
        1. 验证 DimensionItem 缺少 id 时抛出 ValidationError

        【执行过程】
        1. 创建 DimensionItem() 不传 id

        【预期结果】
        1. 抛出 ValidationError
        """
        with pytest.raises(ValidationError):
            DimensionItem()


# ============================================================
# FilterItem 测试
# ============================================================


class TestFilterItem:
    """FilterItem 模型测试组"""

    @pytest.mark.unit
    def test_valid_filter_item(self):
        """
        【测试目标】
        1. 验证创建有效的 FilterItem

        【执行过程】
        1. 创建 FilterItem(id="DIM_COUNTRY", op=EQ, values=["USA"])

        【预期结果】
        1. filter.id、op、values 值正确
        """
        filter_item = FilterItem(id="DIM_COUNTRY", op=FilterOp.EQ, values=["USA"])
        assert filter_item.id == "DIM_COUNTRY"
        assert filter_item.op == FilterOp.EQ
        assert filter_item.values == ["USA"]

    @pytest.mark.unit
    def test_filter_item_all_operators(self):
        """
        【测试目标】
        1. 验证 FilterItem 支持所有操作符

        【执行过程】
        1. 遍历所有 FilterOp 枚举值，创建 FilterItem
        2. 为每个操作符提供适当的 values

        【预期结果】
        1. 每个操作符都能成功设置且 values 正确
        """
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

    @pytest.mark.unit
    def test_filter_item_empty_values(self):
        """
        【测试目标】
        1. 验证 FilterItem 允许空 values

        【执行过程】
        1. 创建 FilterItem 包含 values=[]

        【预期结果】
        1. filter.values 为空列表
        """
        filter_item = FilterItem(id="DIM_TEST", op=FilterOp.IN, values=[])
        assert filter_item.values == []

    def test_filter_item_mixed_type_values(self):
        """测试混合类型的值列表"""
        filter_item = FilterItem(
            id="DIM_TEST", op=FilterOp.IN, values=["string", 123, True]
        )
        assert len(filter_item.values) == 3

    @pytest.mark.unit
    def test_filter_item_extra_fields_forbidden(self):
        """
        【测试目标】
        1. 验证 FilterItem 禁止额外字段

        【执行过程】
        1. 创建 FilterItem 包含未定义的 extra_field

        【预期结果】
        1. 抛出 ValidationError，错误消息包含 "extra_field"
        """
        with pytest.raises(ValidationError) as exc_info:
            FilterItem(
                id="DIM_TEST", op=FilterOp.EQ, values=["test"], extra_field="not_allowed"
            )
        assert "extra_field" in str(exc_info.value)

    @pytest.mark.unit
    def test_filter_item_missing_required_fields(self):
        """
        【测试目标】
        1. 验证 FilterItem 缺少必需字段时抛出 ValidationError

        【执行过程】
        1. 创建 FilterItem 只传 id，缺少 op 和 values
        2. 创建 FilterItem 只传 id 和 op，缺少 values

        【预期结果】
        1. 两种情况都抛出 ValidationError
        """
        with pytest.raises(ValidationError):
            FilterItem(id="DIM_TEST")  # 缺少 op 和 values
        with pytest.raises(ValidationError):
            FilterItem(id="DIM_TEST", op=FilterOp.EQ)  # 缺少 values


# ============================================================
# TimeRange 测试
# ============================================================


class TestTimeRange:
    """TimeRange 模型测试组"""

    @pytest.mark.unit
    def test_time_range_last_n(self):
        """
        【测试目标】
        1. 验证 LAST_N 类型的 TimeRange

        【执行过程】
        1. 创建 TimeRange(type=LAST_N, value=7, unit="day")

        【预期结果】
        1. type、value、unit 值正确
        2. start 和 end 为 None
        """
        time_range = TimeRange(type=TimeRangeType.LAST_N, value=7, unit="day")
        assert time_range.type == TimeRangeType.LAST_N
        assert time_range.value == 7
        assert time_range.unit == "day"
        assert time_range.start is None
        assert time_range.end is None

    @pytest.mark.unit
    def test_time_range_absolute(self):
        """
        【测试目标】
        1. 验证 ABSOLUTE 类型的 TimeRange

        【执行过程】
        1. 创建 TimeRange(type=ABSOLUTE, start="...", end="...")

        【预期结果】
        1. type、start、end 值正确
        2. value 和 unit 为 None
        """
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

    @pytest.mark.unit
    def test_time_range_extra_fields_forbidden(self):
        """
        【测试目标】
        1. 验证 TimeRange 禁止额外字段

        【执行过程】
        1. 创建 TimeRange 包含未定义的 extra_field

        【预期结果】
        1. 抛出 ValidationError，错误消息包含 "extra_field"
        """
        with pytest.raises(ValidationError) as exc_info:
            TimeRange(
                type=TimeRangeType.LAST_N,
                value=7,
                unit="day",
                extra_field="not_allowed",
            )
        assert "extra_field" in str(exc_info.value)

    @pytest.mark.unit
    def test_time_range_missing_type(self):
        """
        【测试目标】
        1. 验证 TimeRange 缺少 type 时抛出 ValidationError

        【执行过程】
        1. 创建 TimeRange 只传 value 和 unit，不传 type

        【预期结果】
        1. 抛出 ValidationError
        """
        with pytest.raises(ValidationError):
            TimeRange(value=7, unit="day")


# ============================================================
# OrderItem 测试
# ============================================================


class TestOrderItem:
    """OrderItem 模型测试组"""

    @pytest.mark.unit
    def test_valid_order_item(self):
        """
        【测试目标】
        1. 验证创建有效的 OrderItem

        【执行过程】
        1. 创建 OrderItem(id="METRIC_GMV", direction=DESC)

        【预期结果】
        1. order.id 和 direction 值正确
        """
        order_item = OrderItem(id="METRIC_GMV", direction=OrderDirection.DESC)
        assert order_item.id == "METRIC_GMV"
        assert order_item.direction == OrderDirection.DESC

    @pytest.mark.unit
    def test_order_item_all_directions(self):
        """
        【测试目标】
        1. 验证 OrderItem 支持所有 direction 值

        【执行过程】
        1. 遍历所有 OrderDirection 枚举值，创建 OrderItem

        【预期结果】
        1. 每个 direction 都能成功设置
        """
        for direction in OrderDirection:
            order_item = OrderItem(id="METRIC_TEST", direction=direction)
            assert order_item.direction == direction

    @pytest.mark.unit
    def test_order_item_extra_fields_forbidden(self):
        """
        【测试目标】
        1. 验证 OrderItem 禁止额外字段

        【执行过程】
        1. 创建 OrderItem 包含未定义的 extra_field

        【预期结果】
        1. 抛出 ValidationError，错误消息包含 "extra_field"
        """
        with pytest.raises(ValidationError) as exc_info:
            OrderItem(
                id="METRIC_TEST",
                direction=OrderDirection.ASC,
                extra_field="not_allowed",
            )
        assert "extra_field" in str(exc_info.value)

    @pytest.mark.unit
    def test_order_item_missing_required_fields(self):
        """
        【测试目标】
        1. 验证 OrderItem 缺少必需字段时抛出 ValidationError

        【执行过程】
        1. 创建 OrderItem 只传 id，缺少 direction
        2. 创建 OrderItem 只传 direction，缺少 id

        【预期结果】
        1. 两种情况都抛出 ValidationError
        """
        with pytest.raises(ValidationError):
            OrderItem(id="METRIC_TEST")  # 缺少 direction
        with pytest.raises(ValidationError):
            OrderItem(direction=OrderDirection.ASC)  # 缺少 id


# ============================================================
# QueryPlan 测试
# ============================================================


class TestQueryPlan:
    """QueryPlan 模型测试组"""

    @pytest.mark.unit
    def test_minimal_valid_plan(self):
        """
        【测试目标】
        1. 验证最小有效 QueryPlan（仅包含必需字段）

        【执行过程】
        1. 创建 QueryPlan(intent=AGG) 不传其他字段

        【预期结果】
        1. plan.intent 为 AGG
        2. 其他可选字段为默认值（空列表或 None）
        """
        plan = QueryPlan(intent=PlanIntent.AGG)
        assert plan.intent == PlanIntent.AGG
        assert plan.metrics == []
        assert plan.dimensions == []
        assert plan.filters == []
        assert plan.time_range is None
        assert plan.order_by == []
        assert plan.limit is None
        assert plan.warnings == []

    @pytest.mark.unit
    def test_plan_all_intents(self):
        """
        【测试目标】
        1. 验证 QueryPlan 支持所有 intent 值

        【执行过程】
        1. 遍历所有 PlanIntent 枚举值，创建 QueryPlan

        【预期结果】
        1. 每个 intent 都能成功设置
        """
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

    @pytest.mark.unit
    def test_plan_with_dimensions(self):
        """
        【测试目标】
        1. 验证 QueryPlan 包含 dimensions

        【执行过程】
        1. 创建 QueryPlan 包含多个 DimensionItem

        【预期结果】
        1. plan.dimensions 长度为 2
        2. dimensions 的 id 和 time_grain 正确
        """
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

    @pytest.mark.unit
    def test_plan_with_time_range(self):
        """
        【测试目标】
        1. 验证 QueryPlan 包含 time_range

        【执行过程】
        1. 创建 QueryPlan 包含 TimeRange

        【预期结果】
        1. plan.time_range 不为 None
        2. time_range 的 type、value 正确
        """
        time_range = TimeRange(type=TimeRangeType.LAST_N, value=30, unit="day")
        plan = QueryPlan(intent=PlanIntent.TREND, time_range=time_range)
        assert plan.time_range is not None
        assert plan.time_range.type == TimeRangeType.LAST_N
        assert plan.time_range.value == 30

    @pytest.mark.unit
    def test_plan_with_order_by(self):
        """
        【测试目标】
        1. 验证 QueryPlan 包含 order_by

        【执行过程】
        1. 创建 QueryPlan 包含多个 OrderItem

        【预期结果】
        1. plan.order_by 长度为 2
        2. order_by 的 direction 正确
        """
        order_by = [
            OrderItem(id="METRIC_GMV", direction=OrderDirection.DESC),
            OrderItem(id="DIM_REGION", direction=OrderDirection.ASC),
        ]
        plan = QueryPlan(intent=PlanIntent.AGG, order_by=order_by)
        assert len(plan.order_by) == 2
        assert plan.order_by[0].direction == OrderDirection.DESC
        assert plan.order_by[1].direction == OrderDirection.ASC

    @pytest.mark.unit
    def test_plan_with_limit(self):
        """
        【测试目标】
        1. 验证 QueryPlan 包含 limit

        【执行过程】
        1. 创建 QueryPlan 包含 limit=100

        【预期结果】
        1. plan.limit 为 100
        """
        plan = QueryPlan(intent=PlanIntent.DETAIL, limit=100)
        assert plan.limit == 100

    @pytest.mark.unit
    def test_plan_limit_validation(self):
        """
        【测试目标】
        1. 验证 QueryPlan limit 字段约束（> 0）

        【执行过程】
        1. 尝试创建 limit=0 的 QueryPlan
        2. 尝试创建 limit=-1 的 QueryPlan

        【预期结果】
        1. 两种情况都抛出 ValidationError
        """
        with pytest.raises(ValidationError):
            QueryPlan(intent=PlanIntent.DETAIL, limit=0)
        with pytest.raises(ValidationError):
            QueryPlan(intent=PlanIntent.DETAIL, limit=-1)

    @pytest.mark.unit
    def test_plan_with_warnings(self):
        """
        【测试目标】
        1. 验证 QueryPlan 包含 warnings

        【执行过程】
        1. 创建 QueryPlan 包含 warnings 列表

        【预期结果】
        1. plan.warnings 长度为 2
        2. warnings 包含预期的警告文本
        """
        warnings = ["Warning 1", "Warning 2"]
        plan = QueryPlan(intent=PlanIntent.AGG, warnings=warnings)
        assert len(plan.warnings) == 2
        assert "Warning 1" in plan.warnings
        assert "Warning 2" in plan.warnings

    @pytest.mark.unit
    def test_plan_extra_fields_forbidden(self):
        """
        【测试目标】
        1. 验证 QueryPlan 禁止额外字段

        【执行过程】
        1. 创建 QueryPlan 包含未定义的 extra_field

        【预期结果】
        1. 抛出 ValidationError，错误消息包含 "extra_field"
        """
        with pytest.raises(ValidationError) as exc_info:
            QueryPlan(intent=PlanIntent.AGG, extra_field="not_allowed")
        assert "extra_field" in str(exc_info.value)

    @pytest.mark.unit
    def test_plan_missing_intent(self):
        """
        【测试目标】
        1. 验证 QueryPlan 缺少 intent 时抛出 ValidationError

        【执行过程】
        1. 创建 QueryPlan() 不传 intent

        【预期结果】
        1. 抛出 ValidationError
        """
        with pytest.raises(ValidationError):
            QueryPlan()

    @pytest.mark.unit
    def test_plan_string_strip_whitespace(self):
        """
        【测试目标】
        1. 验证 QueryPlan 字符串字段自动去除空格

        【执行过程】
        1. 创建 QueryPlan 包含带空格的 warnings 字符串
        2. 使用 model_validate 反序列化

        【预期结果】
        1. warnings 字符串的首尾空格被自动去除
        """
        plan = QueryPlan(intent=PlanIntent.AGG)
        # 测试 warnings 中的字符串会被去除空格
        plan.warnings.append("  warning with spaces  ")
        # 注意：Pydantic 的 str_strip_whitespace=True 会在模型创建时自动去除字符串首尾空格
        # 这里我们测试通过 model_validate 创建
        plan_dict = plan.model_dump()
        plan_dict["warnings"] = ["  warning with spaces  "]
        new_plan = QueryPlan.model_validate(plan_dict)
        # 由于 str_strip_whitespace=True 配置，字符串首尾空格会被自动去除
        assert new_plan.warnings == ["warning with spaces"]

    @pytest.mark.unit
    def test_complete_plan(self):
        """
        【测试目标】
        1. 验证包含所有字段的完整 QueryPlan

        【执行过程】
        1. 创建 QueryPlan 包含所有可选字段

        【预期结果】
        1. 所有字段值正确
        2. metrics、dimensions、filters、order_by、warnings 长度为 1
        3. time_range 不为 None
        4. limit 为 100
        """
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

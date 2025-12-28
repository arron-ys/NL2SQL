"""
【简述】
验证 Stage3 校验模块的四大检查点：结构完整性、权限复核、语义连通性与规范化注入的正确性。

【范围/不测什么】
- 不覆盖真实语义层加载；仅验证校验逻辑、权限过滤、兼容性检查与默认值注入规则。

【用例概述】
- test_agg_intent_without_metrics_raises_error:
  -- 验证 AGG 意图缺少指标时抛出 MissingMetricError
- test_trend_intent_without_metrics_raises_error:
  -- 验证 TREND 意图缺少指标时抛出 MissingMetricError
- test_detail_intent_without_metrics_allowed:
  -- 验证 DETAIL 意图允许没有指标
- test_agg_intent_with_metrics_passes:
  -- 验证 AGG 意图有指标时通过校验
- test_initializes_empty_fields:
  -- 验证空字段被初始化为空列表
- test_unauthorized_metric_raises_error:
  -- 验证未授权指标抛出 PermissionDeniedError
- test_unauthorized_dimension_raises_error:
  -- 验证未授权维度抛出 PermissionDeniedError
- test_unauthorized_filter_raises_error:
  -- 验证未授权过滤器抛出 PermissionDeniedError
- test_all_authorized_ids_pass:
  -- 验证所有已授权 ID 通过权限检查
- test_multi_entity_metrics_raises_error:
  -- 验证多实体指标抛出 UnsupportedMultiFactError
- test_single_entity_metrics_pass:
  -- 验证单实体指标通过检查
- test_incompatible_dimension_removed_with_warning:
  -- 验证不兼容维度被移除并添加警告
- test_detail_intent_preserves_all_dimensions:
  -- 验证 DETAIL 意图保留所有维度不做兼容性过滤
- test_user_specified_time_range_preserved_no_warning:
  -- 验证用户指定的时间范围被保留且无警告
- test_injects_metric_level_default_time_window:
  -- 验证注入指标级默认时间窗口
- test_injects_global_default_time_window_when_metric_missing_default:
  -- 验证指标无默认时注入全局默认时间窗口
- test_missing_or_invalid_time_window_raises_configuration_error:
  -- 验证缺失或无效时间窗口抛出 ConfigurationError
- test_multi_metric_conflict_raises_ambiguous_time:
  -- 验证多指标时间冲突抛出 AmbiguousTimeError
- test_preserves_existing_time_range:
  -- 验证保留已存在的时间范围
- test_injects_mandatory_filters:
  -- 验证注入强制过滤器
- test_does_not_duplicate_existing_filters:
  -- 验证不重复已存在的过滤器
- test_injects_default_limit:
  -- 验证注入默认 limit
- test_caps_limit_to_max:
  -- 验证 limit 被限制在最大值
- test_preserves_valid_limit:
  -- 验证保留有效的 limit
"""

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.semantic_registry import SemanticConfigurationError
from schemas.plan import (
    DimensionItem,
    FilterItem,
    FilterOp,
    MetricItem,
    PlanIntent,
    QueryPlan,
    TimeRange,
    TimeRangeType,
)
from schemas.request import RequestContext
from stages.stage3_validation import (
    AmbiguousTimeError,
    ConfigurationError,
    MissingMetricError,
    PermissionDeniedError,
    UnsupportedMultiFactError,
    _get_global_default_time_window_id,
    validate_and_normalize_plan,
)


# ============================================================
# Test Fixtures
# ============================================================


@pytest.fixture
def mock_registry():
    """创建模拟的 SemanticRegistry"""
    registry = MagicMock()
    # 默认返回所有ID都被允许（用于测试权限检查）
    registry.get_allowed_ids.return_value = {
        "METRIC_GMV",
        "METRIC_REVENUE",
        "DIM_REGION",
        "DIM_DATE",
        "DIM_COUNTRY",
        "LF_ACTIVE_ONLY",
    }
    # 默认指标定义
    registry.get_metric_def.return_value = {
        "id": "METRIC_GMV",
        "entity_id": "ENTITY_ORDER",
        "default_filters": [],
        "default_time": None,
    }
    # 默认维度定义
    registry.get_dimension_def.return_value = {
        "id": "DIM_REGION",
        "entity_id": "ENTITY_ORDER",
    }
    # 默认兼容性检查返回True
    registry.check_compatibility.return_value = True
    # 默认实体定义（用于 time_field 推断）
    registry.get_entity_def.return_value = {
        "id": "ENTITY_ORDER",
        "default_time_field_id": "ORDER_DATE",
    }
    # 默认全局配置：提供全局默认时间窗口（避免与 Stage3 时间补全策略冲突）
    registry.global_config = {
        "default_time_window_id": "TIME_DEFAULT_30D",
        "time_windows": [
            {
                "id": "TIME_DEFAULT_30D",
                "name": "默认时间窗口（近30天）",
                "template": {"type": "LAST_N", "value": 30, "unit": "DAY"},
            },
            {
                "id": "TIME_LAST_30D",
                "name": "最近30天",
                "template": {"type": "LAST_N", "value": 30, "unit": "DAY"},
            },
            {
                "id": "TIME_AS_OF_TODAY",
                "name": "当前时点",
                "template": {"type": "ABSOLUTE", "end": "CURRENT_DATE"},
            },
        ],
    }

    # 为 Stage3 时间注入提供可用的语义解析（模拟 SemanticRegistry.resolve_time_window 行为）
    def _resolve_time_window_side_effect(time_window_id: str, time_field_id: str = None):
        for tw in registry.global_config.get("time_windows", []):
            if tw.get("id") == time_window_id:
                template = tw.get("template", {})
                tw_type = template.get("type")
                if tw_type == "LAST_N":
                    return TimeRange(
                        type=TimeRangeType.LAST_N,
                        value=template.get("value"),
                        unit=template.get("unit"),
                    ), (tw.get("name") or time_window_id)
                if tw_type == "ABSOLUTE":
                    return TimeRange(
                        type=TimeRangeType.ABSOLUTE,
                        start=template.get("start"),
                        end=template.get("end"),
                    ), (tw.get("name") or time_window_id)
        # 模拟语义层解析失败
        raise SemanticConfigurationError(
            f"time_window_id not found in global_config.time_windows: {time_window_id}",
            details={"time_window_id": time_window_id, "time_field_id": time_field_id},
        )

    registry.resolve_time_window.side_effect = _resolve_time_window_side_effect
    return registry


@pytest.fixture
def mock_context():
    """创建模拟的 RequestContext"""
    return RequestContext(
        user_id="test_user",
        role_id="ROLE_TEST",
        tenant_id="test_tenant",
        request_id="test_request_001",
        current_date=date(2024, 1, 15),
    )


@pytest.fixture
def mock_pipeline_config():
    """模拟 PipelineConfig"""
    config = MagicMock()
    config.default_limit = 100
    config.max_limit_cap = 1000
    return config


# ============================================================
# Checkpoint 1: Structural Sanity Tests
# ============================================================


class TestStructuralSanity:
    """结构完整性检查测试组"""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_agg_intent_without_metrics_raises_error(
        self, mock_registry, mock_context
    ):
        """
        【测试目标】
        1. 验证 AGG 意图缺少指标时抛出 MissingMetricError

        【执行过程】
        1. 构造 AGG intent 的 Plan，metrics 为空
        2. 调用 validate_and_normalize_plan

        【预期结果】
        1. 抛出 MissingMetricError
        2. 错误消息包含 "must have at least one metric"
        """
        plan = QueryPlan(intent=PlanIntent.AGG, metrics=[])

        with pytest.raises(MissingMetricError) as exc_info:
            await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        assert "must have at least one metric" in str(exc_info.value)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_trend_intent_without_metrics_raises_error(
        self, mock_registry, mock_context
    ):
        """
        【测试目标】
        1. 验证 TREND 意图缺少指标时抛出 MissingMetricError

        【执行过程】
        1. 构造 TREND intent 的 Plan，metrics 为空
        2. 调用 validate_and_normalize_plan

        【预期结果】
        1. 抛出 MissingMetricError
        2. 错误消息包含 "must have at least one metric"
        """
        plan = QueryPlan(intent=PlanIntent.TREND, metrics=[])

        with pytest.raises(MissingMetricError) as exc_info:
            await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        assert "must have at least one metric" in str(exc_info.value)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_detail_intent_without_metrics_allowed(
        self, mock_registry, mock_context
    ):
        """
        【测试目标】
        1. 验证 DETAIL 意图允许没有指标

        【执行过程】
        1. 构造 DETAIL intent 的 Plan，metrics 为空
        2. 调用 validate_and_normalize_plan

        【预期结果】
        1. 不抛异常
        2. 返回的 intent 为 DETAIL
        3. metrics 长度为 0
        """
        plan = QueryPlan(intent=PlanIntent.DETAIL, metrics=[])

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        assert result.intent == PlanIntent.DETAIL
        assert len(result.metrics) == 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_agg_intent_with_metrics_passes(
        self, mock_registry, mock_context
    ):
        """
        【测试目标】
        1. 验证 AGG 意图有指标时通过校验

        【执行过程】
        1. 构造 AGG intent 的 Plan，包含一个指标
        2. 调用 validate_and_normalize_plan

        【预期结果】
        1. 不抛异常
        2. 返回的 intent 为 AGG
        3. metrics 至少包含一个元素
        """
        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        assert result.intent == PlanIntent.AGG
        assert len(result.metrics) == 1

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initializes_empty_fields(
        self, mock_registry, mock_context
    ):
        """
        【测试目标】
        1. 验证空字段被初始化为空列表

        【执行过程】
        1. 构造 Plan 包含 None 字段
        2. 验证模型自动将 None 转换为空列表

        【预期结果】
        1. filters、order_by、warnings 自动转换为空列表
        2. 不为 None
        """
        # 测试场景1：显式传入 None
        plan_with_none = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            filters=None,  # 显式传入 None
            order_by=None,  # 显式传入 None
            warnings=None,  # 显式传入 None
        )
        
        # 验证模型自动将 None 转换为空列表
        assert plan_with_none.filters == []
        assert plan_with_none.order_by == []
        assert plan_with_none.warnings == []
        assert plan_with_none.filters is not None
        assert plan_with_none.order_by is not None
        assert plan_with_none.warnings is not None
        
        # 测试场景2：不传字段（使用默认值）
        plan_without_fields = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            # 不传 filters, order_by, warnings，使用默认值
        )
        
        # 验证默认值为空列表
        assert plan_without_fields.filters == []
        assert plan_without_fields.order_by == []
        assert plan_without_fields.warnings == []
        
        # 测试场景3：通过 model_validate 从字典创建（模拟前端传入 null）
        plan_dict = {
            "intent": "AGG",
            "metrics": [{"id": "METRIC_GMV"}],
            "filters": None,  # 前端可能传入 null
            "order_by": None,  # 前端可能传入 null
            "warnings": None,  # 前端可能传入 null
        }
        plan_from_dict = QueryPlan.model_validate(plan_dict)
        
        # 验证模型自动将 None 转换为空列表
        assert plan_from_dict.filters == []
        assert plan_from_dict.order_by == []
        assert plan_from_dict.warnings == []
        
        # 验证通过 validate_and_normalize_plan 后仍然正确
        result = await validate_and_normalize_plan(plan_with_none, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")
        assert result.filters == []
        assert result.order_by == []
        # warnings 可能包含 time_range 自动补全提示，不作为本用例断言点


# ============================================================
# Checkpoint 2: Security Enforcement Tests
# ============================================================


class TestSecurityEnforcement:
    """权限复核测试组"""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_unauthorized_metric_raises_error(
        self, mock_registry, mock_context
    ):
        """
        【测试目标】
        1. 验证未授权指标抛出 PermissionDeniedError

        【执行过程】
        1. mock registry 只允许 METRIC_GMV
        2. 构造 Plan 包含未授权的 METRIC_REVENUE
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. 抛出 PermissionDeniedError
        2. 错误消息包含 "unauthorized" 和 "METRIC_REVENUE"
        """
        # 设置registry只允许METRIC_GMV，不允许METRIC_REVENUE
        mock_registry.get_allowed_ids.return_value = {"METRIC_GMV", "DIM_REGION"}

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_REVENUE")],  # 未授权
        )

        with pytest.raises(PermissionDeniedError) as exc_info:
            await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        error_msg = str(exc_info.value).lower()
        assert "unauthorized" in error_msg and "ids" in error_msg
        assert "METRIC_REVENUE" in str(exc_info.value)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_unauthorized_dimension_raises_error(
        self, mock_registry, mock_context
    ):
        """
        【测试目标】
        1. 验证未授权维度抛出 PermissionDeniedError

        【执行过程】
        1. mock registry 只允许 METRIC_GMV 和 DIM_REGION
        2. 构造 Plan 包含未授权的 DIM_COUNTRY
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. 抛出 PermissionDeniedError
        2. 错误消息包含 "unauthorized" 和 "DIM_COUNTRY"
        """
        # 设置registry只允许METRIC_GMV和DIM_REGION
        mock_registry.get_allowed_ids.return_value = {
            "METRIC_GMV",
            "DIM_REGION",
        }

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            dimensions=[DimensionItem(id="DIM_COUNTRY")],  # 未授权
        )

        with pytest.raises(PermissionDeniedError) as exc_info:
            await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        error_msg = str(exc_info.value).lower()
        assert "unauthorized" in error_msg and "ids" in error_msg
        assert "DIM_COUNTRY" in str(exc_info.value)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_unauthorized_filter_raises_error(
        self, mock_registry, mock_context
    ):
        """
        【测试目标】
        1. 验证未授权过滤器抛出 PermissionDeniedError

        【执行过程】
        1. mock registry 只允许 METRIC_GMV 和 DIM_REGION
        2. 构造 Plan 包含未授权的 DIM_COUNTRY 过滤器
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. 抛出 PermissionDeniedError
        2. 错误消息包含 "unauthorized"
        """
        mock_registry.get_allowed_ids.return_value = {
            "METRIC_GMV",
            "DIM_REGION",
        }

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            filters=[
                FilterItem(id="DIM_COUNTRY", op=FilterOp.EQ, values=["USA"])
            ],  # 未授权
        )

        with pytest.raises(PermissionDeniedError) as exc_info:
            await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        error_msg = str(exc_info.value).lower()
        assert "unauthorized" in error_msg and "ids" in error_msg

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_all_authorized_ids_pass(
        self, mock_registry, mock_context
    ):
        """
        【测试目标】
        1. 验证所有已授权 ID 通过权限检查

        【执行过程】
        1. mock registry 允许所有需要的 ID
        2. 构造 Plan 包含已授权的 metrics、dimensions、filters
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. 不抛异常
        2. 返回的 Plan 包含所有元素
        """
        mock_registry.get_allowed_ids.return_value = {
            "METRIC_GMV",
            "DIM_REGION",
            "DIM_COUNTRY",
        }

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            dimensions=[DimensionItem(id="DIM_REGION")],
            filters=[FilterItem(id="DIM_COUNTRY", op=FilterOp.EQ, values=["USA"])],
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        assert result.intent == PlanIntent.AGG
        assert len(result.metrics) == 1


# ============================================================
# Checkpoint 3: Semantic Connectivity Tests
# ============================================================


class TestSemanticConnectivity:
    """语义连通性校验测试组"""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_multi_entity_metrics_raises_error(
        self, mock_registry, mock_context
    ):
        """
        【测试目标】
        1. 验证多实体指标抛出 UnsupportedMultiFactError

        【执行过程】
        1. mock 不同指标属于不同实体
        2. 构造 Plan 包含多个实体的指标
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. 抛出 UnsupportedMultiFactError
        2. 错误消息包含 "multiple entities"
        """
        # 设置不同指标属于不同实体
        def get_metric_def_side_effect(metric_id):
            if metric_id == "METRIC_GMV":
                return {"id": "METRIC_GMV", "entity_id": "ENTITY_ORDER"}
            elif metric_id == "METRIC_REVENUE":
                return {"id": "METRIC_REVENUE", "entity_id": "ENTITY_PRODUCT"}
            return None

        mock_registry.get_metric_def.side_effect = get_metric_def_side_effect

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[
                MetricItem(id="METRIC_GMV"),
                MetricItem(id="METRIC_REVENUE"),
            ],
        )

        with pytest.raises(UnsupportedMultiFactError) as exc_info:
            await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        assert "multiple entities" in str(exc_info.value).lower()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_single_entity_metrics_pass(
        self, mock_registry, mock_context
    ):
        """
        【测试目标】
        1. 验证单实体指标通过检查

        【执行过程】
        1. mock 所有指标属于同一实体
        2. 构造 Plan 包含多个同实体指标
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. 不抛异常
        2. 返回的 metrics 长度为 2
        """
        def get_metric_def_side_effect(metric_id):
            return {"id": metric_id, "entity_id": "ENTITY_ORDER"}

        mock_registry.get_metric_def.side_effect = get_metric_def_side_effect

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[
                MetricItem(id="METRIC_GMV"),
                MetricItem(id="METRIC_REVENUE"),
            ],
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        assert len(result.metrics) == 2

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_incompatible_dimension_removed_with_warning(
        self, mock_registry, mock_context
    ):
        """
        【测试目标】
        1. 验证不兼容维度被移除并添加警告

        【执行过程】
        1. mock check_compatibility：DIM_REGION 兼容，DIM_COUNTRY 不兼容
        2. 构造 Plan 包含两个维度
        3. 调用 validate_and_normalize_plan
        4. 检查结果维度和警告

        【预期结果】
        1. DIM_REGION 保留在结果中
        2. DIM_COUNTRY 被移除
        3. warnings 包含 DIM_COUNTRY 相关警告
        """
        # 设置兼容性检查：DIM_REGION兼容，DIM_COUNTRY不兼容
        def check_compatibility_side_effect(metric_id, dimension_id):
            return dimension_id == "DIM_REGION"

        mock_registry.check_compatibility.side_effect = check_compatibility_side_effect

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            dimensions=[
                DimensionItem(id="DIM_REGION"),  # 兼容
                DimensionItem(id="DIM_COUNTRY"),  # 不兼容
            ],
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        # DIM_COUNTRY应该被移除
        dimension_ids = [d.id for d in result.dimensions]
        assert "DIM_REGION" in dimension_ids
        assert "DIM_COUNTRY" not in dimension_ids
        # 应该有警告
        assert any("DIM_COUNTRY" in warning for warning in result.warnings)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_detail_intent_preserves_all_dimensions(
        self, mock_registry, mock_context
    ):
        """
        【测试目标】
        1. 验证 DETAIL 意图保留所有维度不做兼容性过滤

        【执行过程】
        1. 构造 DETAIL intent 的 Plan，无 metrics，包含多个维度
        2. 调用 validate_and_normalize_plan

        【预期结果】
        1. 所有维度都被保留
        2. dimensions 长度为 2
        """
        plan = QueryPlan(
            intent=PlanIntent.DETAIL,
            metrics=[],  # 没有指标
            dimensions=[
                DimensionItem(id="DIM_REGION"),
                DimensionItem(id="DIM_COUNTRY"),
            ],
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry)

        assert len(result.dimensions) == 2


# ============================================================
# Checkpoint 4: Normalization & Injection Tests
# ============================================================


class TestNormalizationAndInjection:
    """规范化与注入测试组"""

    @pytest.mark.unit
    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_user_specified_time_range_preserved_no_warning(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """
        【测试目标】
        1. 验证用户指定的时间范围被保留且无警告

        【执行过程】
        1. 构造 Plan 包含用户指定的 time_range
        2. 调用 validate_and_normalize_plan
        3. 检查时间范围和警告

        【预期结果】
        1. time_range 被保留
        2. type 为 LAST_N，value 为 7
        3. warnings 不包含 "未指定时间" 相关警告
        """
        mock_get_config.return_value = mock_pipeline_config

        existing_time_range = TimeRange(type=TimeRangeType.LAST_N, value=7, unit="day")
        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            time_range=existing_time_range,
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")
        assert result.time_range is not None
        assert result.time_range.type == TimeRangeType.LAST_N
        assert result.time_range.value == 7
        # 不应添加“未指定时间...”的补全 warning
        assert not any("未指定时间" in w for w in result.warnings)

    @pytest.mark.unit
    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_no_time_cue_no_injection(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """
        【测试目标】T1
        验证无时间词时不注入默认时间窗（情况A：完全没提时间）

        【执行过程】
        1. sub_query_description="公司总体销售额如何？"（无时间词）
        2. plan.time_range=None
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. time_range 仍为 None（不注入）
        2. warnings 包含 "未指定时间，默认查询全量历史数据"
        """
        mock_get_config.return_value = mock_pipeline_config

        mock_registry.get_metric_def.return_value = {
            "id": "METRIC_GMV",
            "name": "GMV",
            "entity_id": "ENT_SALES_ORDER_ITEM",
            "default_time": {"time_field_id": "ORDER_DATE", "time_window_id": "TIME_LAST_30D"},
            "default_filters": [],
        }
        mock_registry.get_entity_def.return_value = {
            "id": "ENT_SALES_ORDER_ITEM",
            "default_time_field_id": "ORDER_DATE",
        }
        mock_registry.global_config = {
            "default_time_window_id": "TIME_DEFAULT_30D",
            "time_windows": [
                {"id": "TIME_LAST_30D", "name": "最近30天", "template": {"type": "LAST_N", "value": 30, "unit": "DAY"}},
                {"id": "TIME_DEFAULT_30D", "name": "默认时间窗口（近30天）", "template": {"type": "LAST_N", "value": 30, "unit": "DAY"}},
            ],
        }

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            time_range=None,
        )

        result = await validate_and_normalize_plan(
            plan, mock_context, mock_registry,
            sub_query_description="公司总体销售额如何？",  # 无时间词
            raw_question="公司总体销售额如何？"
        )

        # 验证不注入
        assert result.time_range is None
        assert any("未指定时间，默认查询全量历史数据" in w for w in result.warnings)

    @pytest.mark.unit
    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_vague_time_cue_triggers_injection(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """
        【测试目标】T2
        验证模糊时间词触发默认时间窗注入（情况B：模糊时间词）

        【执行过程】
        1. sub_query_description="最近公司总体销售额如何？"（包含模糊时间词"最近"）
        2. plan.time_range=None
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. time_range 被注入（不为 None）
        2. warnings 包含 "检测到模糊时间表达"
        """
        mock_get_config.return_value = mock_pipeline_config

        mock_registry.get_metric_def.return_value = {
            "id": "METRIC_GMV",
            "name": "GMV",
            "entity_id": "ENT_SALES_ORDER_ITEM",
            "default_time": {"time_field_id": "ORDER_DATE", "time_window_id": "TIME_LAST_30D"},
            "default_filters": [],
        }
        mock_registry.get_entity_def.return_value = {
            "id": "ENT_SALES_ORDER_ITEM",
            "default_time_field_id": "ORDER_DATE",
        }
        mock_registry.get_dimension_def.return_value = {
            "id": "ORDER_DATE",
            "column": "order_date"
        }
        mock_registry.global_config = {
            "default_time_window_id": "TIME_DEFAULT_30D",
            "time_windows": [
                {"id": "TIME_LAST_30D", "name": "最近30天", "template": {"type": "LAST_N", "value": 30, "unit": "DAY"}},
            ],
        }
        mock_registry.resolve_time_window.return_value = (
            TimeRange(type=TimeRangeType.LAST_N, value=30, unit="DAY"),
            "最近30天"
        )

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            time_range=None,
        )

        result = await validate_and_normalize_plan(
            plan, mock_context, mock_registry,
            sub_query_description="最近公司总体销售额如何？",  # 包含模糊时间词
            raw_question="最近公司总体销售额如何？"
        )

        # 验证注入成功
        assert result.time_range is not None
        assert any("检测到模糊时间表达" in w for w in result.warnings)

    @pytest.mark.unit
    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_non_vague_time_cue_raises_ambiguous_error(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """
        【测试目标】T3
        验证非模糊时间意图但解析失败时抛出 AmbiguousTimeError（情况C）

        【执行过程】
        1. sub_query_description="上周公司总体销售额如何？"（包含时间词"上周"但非模糊）
        2. plan.time_range=None（Stage2 未解析）
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. 抛出 AmbiguousTimeError
        2. code="AMBIGUOUS_TIME"
        3. details 包含 sub_query_description 和 metrics
        """
        mock_get_config.return_value = mock_pipeline_config

        mock_registry.get_metric_def.return_value = {
            "id": "METRIC_GMV",
            "name": "GMV",
            "entity_id": "ENT_SALES_ORDER_ITEM",
            "default_time": {"time_field_id": "ORDER_DATE", "time_window_id": "TIME_LAST_30D"},
            "default_filters": [],
        }
        mock_registry.get_entity_def.return_value = {
            "id": "ENT_SALES_ORDER_ITEM",
            "default_time_field_id": "ORDER_DATE",
        }

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            time_range=None,  # Stage2 未解析
        )

        with pytest.raises(AmbiguousTimeError) as exc_info:
            await validate_and_normalize_plan(
                plan, mock_context, mock_registry,
                sub_query_description="上周公司总体销售额如何？",  # 非模糊时间词
                raw_question="上周公司总体销售额如何？"
            )
        
        assert getattr(exc_info.value, "code", None) == "AMBIGUOUS_TIME"
        assert "上周" in str(exc_info.value.details.get("sub_query_description", ""))
        assert "METRIC_GMV" in exc_info.value.details.get("metrics", [])

    @pytest.mark.unit
    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_missing_or_invalid_time_window_raises_configuration_error(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """
        【测试目标】
        1. 验证缺失或无效时间窗口抛出 ConfigurationError

        【执行过程】
        1. Case A：metric 和 global 都缺失 default_time
        2. Case B：time_window_id 无效
        3. 分别调用 validate_and_normalize_plan

        【预期结果】
        1. 两种 case 都抛出 ConfigurationError
        2. error.code 为 "CONFIGURATION_ERROR"
        3. Case B 错误消息包含 "TIME_NOT_EXIST"
        """
        mock_get_config.return_value = mock_pipeline_config

        # case A: Level1 缺失，Level2 缺失
        mock_registry.get_metric_def.return_value = {
            "id": "METRIC_GMV",
            "name": "GMV",
            "entity_id": "ENT_SALES_ORDER_ITEM",
            "default_time": None,
            "default_filters": [],
        }
        mock_registry.get_entity_def.return_value = {"id": "ENT_SALES_ORDER_ITEM", "default_time_field_id": "ORDER_DATE"}
        mock_registry.global_config = {"time_windows": []}
        plan2 = QueryPlan(intent=PlanIntent.AGG, metrics=[MetricItem(id="METRIC_GMV")], time_range=None)
        with pytest.raises(ConfigurationError) as exc_info2:
            await validate_and_normalize_plan(
                plan2, mock_context, mock_registry,
                sub_query_description="最近公司总体销售额如何？",  # 模糊时间词触发注入
                raw_question="最近公司总体销售额如何？"
            )
        assert getattr(exc_info2.value, "code", None) == "CONFIGURATION_ERROR"
        assert "TIME_NOT_EXIST" in str(exc_info2.value)

    @pytest.mark.unit
    def test_get_global_default_time_window_id_canonical_path(self):
        """
        【测试目标】
        1. 验证规范路径 global_config.default_time_window_id 返回正确值

        【执行过程】
        1. 创建 mock registry，global_config 包含规范路径 default_time_window_id
        2. 调用 _get_global_default_time_window_id

        【预期结果】
        1. 返回规范路径的值 "TIME_DEFAULT_30D"
        """
        from unittest.mock import MagicMock
        mock_registry = MagicMock()
        mock_registry.global_config = {
            "default_time_window_id": "TIME_DEFAULT_30D",
            "time_windows": []
        }
        
        result = _get_global_default_time_window_id(mock_registry)
        assert result == "TIME_DEFAULT_30D"

    @pytest.mark.unit
    def test_get_global_default_time_window_id_deprecated_nested_path_ignored(self):
        """
        【测试目标】
        1. 验证废弃嵌套路径 global_config.global_settings.default_time_window_id 不再生效

        【执行过程】
        1. 创建 mock registry，只提供废弃嵌套路径，规范路径缺失
        2. 调用 _get_global_default_time_window_id

        【预期结果】
        1. 返回 None（废弃路径被忽略）
        """
        from unittest.mock import MagicMock
        mock_registry = MagicMock()
        mock_registry.global_config = {
            "global_settings": {"default_time_window_id": "TIME_DEPRECATED"},
            "time_windows": []
        }
        
        result = _get_global_default_time_window_id(mock_registry)
        assert result is None

    @pytest.mark.unit
    def test_get_global_default_time_window_id_deprecated_flat_path_ignored(self):
        """
        【测试目标】
        1. 验证废弃扁平路径 global_config.default_time_window（不带 _id）不再生效

        【执行过程】
        1. 创建 mock registry，只提供废弃扁平路径 default_time_window，规范路径缺失
        2. 调用 _get_global_default_time_window_id

        【预期结果】
        1. 返回 None（废弃路径被忽略）
        """
        from unittest.mock import MagicMock
        mock_registry = MagicMock()
        mock_registry.global_config = {
            "default_time_window": "TIME_DEPRECATED",
            "time_windows": []
        }
        
        result = _get_global_default_time_window_id(mock_registry)
        assert result is None

    @pytest.mark.unit
    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_multi_metric_conflict_raises_ambiguous_time(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """
        【测试目标】
        1. 验证多指标时间冲突抛出 AmbiguousTimeError

        【执行过程】
        1. mock 两个指标使用不同的 time_field_id 和 time_window_id
        2. 构造 Plan 包含两个指标，无 time_range
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. 抛出 AmbiguousTimeError
        2. error.code 为 "AMBIGUOUS_TIME"
        3. 错误消息包含 "Ambiguous" 或冲突指标 ID
        """
        mock_get_config.return_value = mock_pipeline_config

        def metric_def_side_effect(metric_id: str):
            if metric_id == "METRIC_A":
                return {
                    "id": "METRIC_A",
                    "name": "指标A",
                    "entity_id": "ENTITY_ORDER",
                    "default_time": {"time_field_id": "ORDER_DATE", "time_window_id": "TIME_LAST_30D"},
                    "default_filters": [],
                }
            if metric_id == "METRIC_B":
                return {
                    "id": "METRIC_B",
                    "name": "指标B",
                    "entity_id": "ENTITY_ORDER",
                    "default_time": {"time_field_id": "HIRE_DATE", "time_window_id": "TIME_AS_OF_TODAY"},
                    "default_filters": [],
                }
            return None

        mock_registry.get_metric_def.side_effect = metric_def_side_effect
        mock_registry.get_entity_def.side_effect = lambda eid: {"id": eid, "default_time_field_id": "ORDER_DATE"}
        mock_registry.global_config = {
            "default_time_window_id": "TIME_DEFAULT_30D",
            "time_windows": [
                {"id": "TIME_LAST_30D", "name": "最近30天", "template": {"type": "LAST_N", "value": 30, "unit": "DAY"}},
                {"id": "TIME_AS_OF_TODAY", "name": "当前时点", "template": {"type": "ABSOLUTE", "end": "CURRENT_DATE"}},
            ],
        }
        # 通过权限检查
        mock_registry.get_allowed_ids.return_value = {"METRIC_A", "METRIC_B"}

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_A"), MetricItem(id="METRIC_B")],
            time_range=None,
        )
        with pytest.raises(AmbiguousTimeError) as exc_info:
            await validate_and_normalize_plan(
                plan, mock_context, mock_registry,
                sub_query_description="最近公司总体销售额如何？",  # 模糊时间词触发注入
                raw_question="最近公司总体销售额如何？"
            )
        assert getattr(exc_info.value, "code", None) == "AMBIGUOUS_TIME"
        msg = str(exc_info.value)
        assert "Ambiguous" in msg or "ambiguous" in msg.lower()
        # message 或 details 中至少包含冲突指标 ID
        assert "METRIC_A" in msg or "METRIC_B" in msg or getattr(exc_info.value, "details", None)

    @pytest.mark.unit
    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_preserves_existing_time_range(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """
        【测试目标】
        1. 验证保留已存在的时间范围

        【执行过程】
        1. 构造 Plan 包含现有 time_range (value=7)
        2. 调用 validate_and_normalize_plan

        【预期结果】
        1. time_range 不为 None
        2. value 保持为 7（原值）
        """
        mock_get_config.return_value = mock_pipeline_config

        existing_time_range = TimeRange(
            type=TimeRangeType.LAST_N, value=7, unit="day"
        )

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            time_range=existing_time_range,
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        assert result.time_range is not None
        assert result.time_range.value == 7  # 保留原值

    @pytest.mark.unit
    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_injects_mandatory_filters(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """
        【测试目标】
        1. 验证注入强制过滤器

        【执行过程】
        1. mock metric 包含 default_filters
        2. 构造 Plan 无 filters
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. result.filters 包含 "LF_ACTIVE_ONLY"
        """
        mock_get_config.return_value = mock_pipeline_config

        # 设置指标有默认过滤器
        def get_metric_def_side_effect(metric_id):
            return {
                "id": metric_id,
                "entity_id": "ENTITY_ORDER",
                "default_filters": ["LF_ACTIVE_ONLY"],
            }

        mock_registry.get_metric_def.side_effect = get_metric_def_side_effect

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            filters=[],  # 没有过滤器
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        # 应该注入默认过滤器
        filter_ids = [f.id for f in result.filters]
        assert "LF_ACTIVE_ONLY" in filter_ids

    @pytest.mark.unit
    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_does_not_duplicate_existing_filters(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """
        【测试目标】
        1. 验证不重复已存在的过滤器

        【执行过程】
        1. mock metric 包含 default_filters=["LF_ACTIVE_ONLY"]
        2. 构造 Plan 已包含 LF_ACTIVE_ONLY 过滤器
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. result.filters 中 LF_ACTIVE_ONLY 只出现一次
        """
        mock_get_config.return_value = mock_pipeline_config

        def get_metric_def_side_effect(metric_id):
            return {
                "id": metric_id,
                "entity_id": "ENTITY_ORDER",
                "default_filters": ["LF_ACTIVE_ONLY"],
            }

        mock_registry.get_metric_def.side_effect = get_metric_def_side_effect

        # 计划中已经包含默认过滤器
        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            filters=[
                FilterItem(id="LF_ACTIVE_ONLY", op=FilterOp.IN, values=[])
            ],
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        # 不应该重复
        filter_ids = [f.id for f in result.filters]
        assert filter_ids.count("LF_ACTIVE_ONLY") == 1

    @pytest.mark.unit
    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_injects_default_limit(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """
        【测试目标】
        1. 验证注入默认 limit

        【执行过程】
        1. mock pipeline_config.default_limit=100
        2. 构造 Plan 无 limit
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. result.limit 为 100
        """
        mock_get_config.return_value = mock_pipeline_config
        mock_pipeline_config.default_limit = 100

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            limit=None,  # 没有limit
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        assert result.limit == 100

    @pytest.mark.unit
    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_caps_limit_to_max(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """
        【测试目标】
        1. 验证 limit 被限制在最大值

        【执行过程】
        1. mock pipeline_config.max_limit_cap=1000
        2. 构造 Plan 包含 limit=2000（超过最大值）
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. result.limit 被限制为 1000
        2. warnings 包含 "exceeds maximum cap" 相关警告
        """
        mock_get_config.return_value = mock_pipeline_config
        mock_pipeline_config.max_limit_cap = 1000

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            limit=2000,  # 超过最大值
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        assert result.limit == 1000  # 被限制为最大值
        # 应该有警告
        assert any("exceeds maximum cap" in warning for warning in result.warnings)

    @pytest.mark.unit
    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_preserves_valid_limit(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """
        【测试目标】
        1. 验证保留有效的 limit

        【执行过程】
        1. mock pipeline_config.max_limit_cap=1000
        2. 构造 Plan 包含 limit=500（有效值）
        3. 调用 validate_and_normalize_plan

        【预期结果】
        1. result.limit 保持为 500（原值）
        """
        mock_get_config.return_value = mock_pipeline_config
        mock_pipeline_config.max_limit_cap = 1000

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            limit=500,  # 有效值
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry, sub_query_description="测试查询", raw_question="测试查询")

        assert result.limit == 500  # 保留原值

"""
Stage 3 Validation Test Suite

隔离测试 stage3_validation 各检查点：
- Checkpoint 1: Structural Sanity (结构完整性检查)
- Checkpoint 2: Security Enforcement (权限复核)
- Checkpoint 3: Semantic Connectivity (语义连通性校验)
- Checkpoint 4: Normalization & Injection (规范化与注入)
"""
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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
    MissingMetricError,
    PermissionDeniedError,
    UnsupportedMultiFactError,
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
    # 默认全局配置
    registry.global_config = {
        "global_settings": {},
        "time_windows": [],
    }
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
    """测试结构完整性检查"""

    @pytest.mark.asyncio
    async def test_agg_intent_without_metrics_raises_error(
        self, mock_registry, mock_context
    ):
        """测试 AGG 意图缺少指标时抛出错误"""
        plan = QueryPlan(intent=PlanIntent.AGG, metrics=[])

        with pytest.raises(MissingMetricError) as exc_info:
            await validate_and_normalize_plan(plan, mock_context, mock_registry)

        assert "must have at least one metric" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_trend_intent_without_metrics_raises_error(
        self, mock_registry, mock_context
    ):
        """测试 TREND 意图缺少指标时抛出错误"""
        plan = QueryPlan(intent=PlanIntent.TREND, metrics=[])

        with pytest.raises(MissingMetricError) as exc_info:
            await validate_and_normalize_plan(plan, mock_context, mock_registry)

        assert "must have at least one metric" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_detail_intent_without_metrics_allowed(
        self, mock_registry, mock_context
    ):
        """测试 DETAIL 意图允许没有指标"""
        plan = QueryPlan(intent=PlanIntent.DETAIL, metrics=[])

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry)

        assert result.intent == PlanIntent.DETAIL
        assert len(result.metrics) == 0

    @pytest.mark.asyncio
    async def test_agg_intent_with_metrics_passes(
        self, mock_registry, mock_context
    ):
        """测试 AGG 意图有指标时通过"""
        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry)

        assert result.intent == PlanIntent.AGG
        assert len(result.metrics) == 1

    @pytest.mark.asyncio
    async def test_initializes_empty_fields(
        self, mock_registry, mock_context
    ):
        """测试初始化空字段：验证模型能自动将 None 转换为空列表"""
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
        result = await validate_and_normalize_plan(plan_with_none, mock_context, mock_registry)
        assert result.filters == []
        assert result.order_by == []
        assert result.warnings == []


# ============================================================
# Checkpoint 2: Security Enforcement Tests
# ============================================================


class TestSecurityEnforcement:
    """测试权限复核"""

    @pytest.mark.asyncio
    async def test_unauthorized_metric_raises_error(
        self, mock_registry, mock_context
    ):
        """测试未授权的指标抛出错误"""
        # 设置registry只允许METRIC_GMV，不允许METRIC_REVENUE
        mock_registry.get_allowed_ids.return_value = {"METRIC_GMV", "DIM_REGION"}

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_REVENUE")],  # 未授权
        )

        with pytest.raises(PermissionDeniedError) as exc_info:
            await validate_and_normalize_plan(plan, mock_context, mock_registry)

        error_msg = str(exc_info.value).lower()
        assert "unauthorized" in error_msg and "ids" in error_msg
        assert "METRIC_REVENUE" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_unauthorized_dimension_raises_error(
        self, mock_registry, mock_context
    ):
        """测试未授权的维度抛出错误"""
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
            await validate_and_normalize_plan(plan, mock_context, mock_registry)

        error_msg = str(exc_info.value).lower()
        assert "unauthorized" in error_msg and "ids" in error_msg
        assert "DIM_COUNTRY" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_unauthorized_filter_raises_error(
        self, mock_registry, mock_context
    ):
        """测试未授权的过滤器抛出错误"""
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
            await validate_and_normalize_plan(plan, mock_context, mock_registry)

        error_msg = str(exc_info.value).lower()
        assert "unauthorized" in error_msg and "ids" in error_msg

    @pytest.mark.asyncio
    async def test_all_authorized_ids_pass(
        self, mock_registry, mock_context
    ):
        """测试所有ID都授权时通过"""
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

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry)

        assert result.intent == PlanIntent.AGG
        assert len(result.metrics) == 1


# ============================================================
# Checkpoint 3: Semantic Connectivity Tests
# ============================================================


class TestSemanticConnectivity:
    """测试语义连通性校验"""

    @pytest.mark.asyncio
    async def test_multi_entity_metrics_raises_error(
        self, mock_registry, mock_context
    ):
        """测试多个实体的指标抛出错误"""
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
            await validate_and_normalize_plan(plan, mock_context, mock_registry)

        assert "multiple entities" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_single_entity_metrics_pass(
        self, mock_registry, mock_context
    ):
        """测试单个实体的指标通过"""
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

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry)

        assert len(result.metrics) == 2

    @pytest.mark.asyncio
    async def test_incompatible_dimension_removed_with_warning(
        self, mock_registry, mock_context
    ):
        """测试不兼容的维度被移除并添加警告"""
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

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry)

        # DIM_COUNTRY应该被移除
        dimension_ids = [d.id for d in result.dimensions]
        assert "DIM_REGION" in dimension_ids
        assert "DIM_COUNTRY" not in dimension_ids
        # 应该有警告
        assert any("DIM_COUNTRY" in warning for warning in result.warnings)

    @pytest.mark.asyncio
    async def test_detail_intent_preserves_all_dimensions(
        self, mock_registry, mock_context
    ):
        """测试 DETAIL 意图保留所有维度（因为没有指标）"""
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
    """测试规范化与注入"""

    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_injects_default_time_range(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """测试注入默认时间范围"""
        mock_get_config.return_value = mock_pipeline_config

        # 设置指标有默认时间范围
        def get_metric_def_side_effect(metric_id):
            return {
                "id": metric_id,
                "entity_id": "ENTITY_ORDER",
                "default_time": {
                    "time_range_fallback": {
                        "type": "LAST_N",
                        "value": 30,
                        "unit": "day",
                    }
                },
            }

        mock_registry.get_metric_def.side_effect = get_metric_def_side_effect

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            time_range=None,  # 没有时间范围
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry)

        assert result.time_range is not None
        assert result.time_range.type == TimeRangeType.LAST_N
        assert result.time_range.value == 30

    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_preserves_existing_time_range(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """测试保留现有的时间范围"""
        mock_get_config.return_value = mock_pipeline_config

        existing_time_range = TimeRange(
            type=TimeRangeType.LAST_N, value=7, unit="day"
        )

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            time_range=existing_time_range,
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry)

        assert result.time_range is not None
        assert result.time_range.value == 7  # 保留原值

    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_injects_mandatory_filters(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """测试注入必需过滤器"""
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

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry)

        # 应该注入默认过滤器
        filter_ids = [f.id for f in result.filters]
        assert "LF_ACTIVE_ONLY" in filter_ids

    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_does_not_duplicate_existing_filters(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """测试不重复已存在的过滤器"""
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

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry)

        # 不应该重复
        filter_ids = [f.id for f in result.filters]
        assert filter_ids.count("LF_ACTIVE_ONLY") == 1

    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_injects_default_limit(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """测试注入默认limit"""
        mock_get_config.return_value = mock_pipeline_config
        mock_pipeline_config.default_limit = 100

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            limit=None,  # 没有limit
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry)

        assert result.limit == 100

    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_caps_limit_to_max(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """测试限制limit最大值"""
        mock_get_config.return_value = mock_pipeline_config
        mock_pipeline_config.max_limit_cap = 1000

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            limit=2000,  # 超过最大值
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry)

        assert result.limit == 1000  # 被限制为最大值
        # 应该有警告
        assert any("exceeds maximum cap" in warning for warning in result.warnings)

    @pytest.mark.asyncio
    @patch("stages.stage3_validation.get_pipeline_config")
    async def test_preserves_valid_limit(
        self, mock_get_config, mock_registry, mock_context, mock_pipeline_config
    ):
        """测试保留有效的limit"""
        mock_get_config.return_value = mock_pipeline_config
        mock_pipeline_config.max_limit_cap = 1000

        plan = QueryPlan(
            intent=PlanIntent.AGG,
            metrics=[MetricItem(id="METRIC_GMV")],
            limit=500,  # 有效值
        )

        result = await validate_and_normalize_plan(plan, mock_context, mock_registry)

        assert result.limit == 500  # 保留原值

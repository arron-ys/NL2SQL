"""
Stage 3: Validation and Normalization (验证与规范化)

对 Stage 2 生成的 QueryPlan 进行验证、规范化和安全检查。
对应详细设计文档 3.3 的定义。
"""
import time
from typing import Dict, List, Optional, Set, Any

from config.pipeline_config import get_pipeline_config
from core.errors import AppError
from core.semantic_registry import SemanticRegistry, SemanticConfigurationError
from schemas.plan import (
    FilterItem,
    FilterOp,
    PlanIntent,
    QueryPlan,
    TimeRange,
    TimeRangeType
)
from schemas.request import RequestContext
from utils.log_manager import get_logger

logger = get_logger(__name__)


# ============================================================
# 异常定义
# ============================================================
class Stage3Error(Exception):
    """Stage 3 处理异常基类"""
    pass


class MissingMetricError(Stage3Error):
    """缺少指标错误"""
    pass


class PermissionDeniedError(Stage3Error):
    """权限拒绝错误"""
    pass


class UnsupportedMultiFactError(Stage3Error):
    """不支持多事实表错误"""
    pass


class AmbiguousTimeError(AppError):
    """
    时间口径不明确（需要 Stage6 追问用户）。
    """

    def __init__(self, message: str, *, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            code="AMBIGUOUS_TIME",
            message=message,
            error_stage="STAGE_3_VALIDATION",
            details=details or {},
            status_code=500,
        )


class ConfigurationError(AppError):
    """
    系统配置错误（语义配置缺失/不可解析/自相矛盾）。
    """

    def __init__(self, message: str, *, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            code="CONFIGURATION_ERROR",
            message=message,
            error_stage="STAGE_3_VALIDATION",
            details=details or {},
            status_code=500,
        )


# ============================================================
# 辅助函数
# ============================================================
def _get_default_time_range(
    metric_id: str,
    registry: SemanticRegistry,
    context: RequestContext
) -> TimeRange:
    """
    兼容旧入口：此函数不再允许硬编码默认时间。

    新设计已迁移到“Time Window Injection”策略（语义配置驱动 + 冲突检测）。
    这里保留函数名仅用于最小侵入改造；实际实现会严格依赖语义配置解析。
    """
    raise ConfigurationError(
        "Legacy _get_default_time_range() is no longer supported. "
        "Time window injection must be driven by semantic configuration only.",
        details={"metric_id": metric_id},
    )


def _get_global_default_time_window_id(registry: SemanticRegistry) -> Optional[str]:
    """
    读取全局默认 time_window_id。

    兼容两种配置形态：
    - 新：global_config.default_time_window
    - 旧：global_config.global_settings.default_time_window_id
    """
    if not isinstance(registry.global_config, dict):
        return None
    if registry.global_config.get("default_time_window"):
        return registry.global_config.get("default_time_window")
    return registry.global_config.get("global_settings", {}).get("default_time_window_id")


def _infer_time_field_id(metric_id: str, registry: SemanticRegistry) -> Optional[str]:
    """
    推断 time_field_id（用于冲突检测与 resolve_time_window 参数对齐）。
    优先级：
    1) metric.default_time.time_field_id
    2) entity.default_time_field_id（metric.entity_id -> entity def）
    """
    metric_def = registry.get_metric_def(metric_id) or {}
    default_time = metric_def.get("default_time") or {}
    time_field_id = default_time.get("time_field_id")
    if time_field_id:
        return time_field_id
    entity_id = metric_def.get("entity_id")
    if entity_id:
        entity_def = registry.get_entity_def(entity_id)
        if entity_def:
            return entity_def.get("default_time_field_id")
    return None


def _get_metric_name(metric_id: str, registry: SemanticRegistry) -> str:
    metric_def = registry.get_metric_def(metric_id) or {}
    return metric_def.get("name") or metric_id


def _compute_metric_time_candidate(
    metric_id: str,
    registry: SemanticRegistry,
) -> Dict[str, Any]:
    """
    为单个指标计算候选默认时间（只允许 Level1 -> Level2，禁止硬编码）。
    返回结构中包含：
    - metric_id, metric_name
    - level: "METRIC_DEFAULT" | "GLOBAL_DEFAULT"
    - time_window_id, time_field_id
    - time_range (TimeRange) & time_desc（通过 resolve_time_window 得到）
    """
    metric_def = registry.get_metric_def(metric_id)
    metric_name = _get_metric_name(metric_id, registry)
    default_time = metric_def.get("default_time") if isinstance(metric_def, dict) else None

    time_window_id = None
    level = None

    # Level 1: 指标级默认
    if isinstance(default_time, dict) and default_time.get("time_window_id"):
        time_window_id = default_time.get("time_window_id")
        level = "METRIC_DEFAULT"

    # Level 2: 全局默认
    if not time_window_id:
        time_window_id = _get_global_default_time_window_id(registry)
        if time_window_id:
            level = "GLOBAL_DEFAULT"

    if not time_window_id:
        raise ConfigurationError(
            "No default time_window_id for metric and no global default configured",
            details={
                "metric_id": metric_id,
                "metric_name": metric_name,
                "missing": [
                    "metrics[*].default_time.time_window_id",
                    "global_config.default_time_window (or global_config.global_settings.default_time_window_id)",
                ],
            },
        )

    time_field_id = _infer_time_field_id(metric_id, registry)
    if level == "GLOBAL_DEFAULT" and not time_field_id:
        # 语义层无法确定 time_field_id，需要 Stage6 追问口径
        raise AmbiguousTimeError(
            "Global default time window requires time_field_id but semantic layer cannot determine it",
            details={
                "metric_id": metric_id,
                "metric_name": metric_name,
                "time_window_id": time_window_id,
                "time_field_id": time_field_id,
            },
        )

    try:
        time_range, time_desc = registry.resolve_time_window(time_window_id, time_field_id)
    except SemanticConfigurationError as e:
        raise ConfigurationError(
            f"Failed to resolve time_window_id={time_window_id} for metric={metric_id}: {str(e)}",
            details={
                "metric_id": metric_id,
                "metric_name": metric_name,
                "time_window_id": time_window_id,
                "time_field_id": time_field_id,
                "upstream_error": str(e),
                "upstream_details": getattr(e, "details", {}),
            },
        ) from e

    return {
        "metric_id": metric_id,
        "metric_name": metric_name,
        "level": level,
        "time_window_id": time_window_id,
        "time_field_id": time_field_id,
        "time_range": time_range,
        "time_desc": time_desc,
    }


def _inject_time_window_if_needed(plan: QueryPlan, plan_dict: Dict[str, Any], registry: SemanticRegistry) -> None:
    """
    步骤四 子步骤1：时间窗口补全 Time Window Injection（严格按最终设计 0~4）。
    """
    # 0) 用户显式指定 time_range -> 跳过
    if plan_dict.get("time_range") is not None:
        return
    if not plan.metrics:
        return

    # 3) 多指标冲突检测：仅当 metrics > 1 且用户未指定 time_range 时执行
    if len(plan.metrics) > 1:
        candidates = []
        for m in plan.metrics:
            candidates.append(_compute_metric_time_candidate(m.id, registry))

        window_ids = {c["time_window_id"] for c in candidates}
        field_ids = {c["time_field_id"] for c in candidates}

        if len(window_ids) > 1 or len(field_ids) > 1:
            # 必须抛 AMBIGUOUS_TIME（交给 Stage6 追问用户）
            conflict_summary = [
                {
                    "metric_id": c["metric_id"],
                    "metric_name": c["metric_name"],
                    "time_field_id": c["time_field_id"],
                    "time_window_id": c["time_window_id"],
                    "time_desc": c["time_desc"],
                }
                for c in candidates
            ]
            raise AmbiguousTimeError(
                "Ambiguous default time window across multiple metrics",
                details={
                    "metrics": [m.id for m in plan.metrics],
                    "candidates": conflict_summary,
                },
            )

        # 无冲突：继续用主指标注入（但已确认与其他指标一致）
        primary_metric_id = plan.metrics[0].id
        primary = next(c for c in candidates if c["metric_id"] == primary_metric_id)
        plan_dict["time_range"] = primary["time_range"].model_dump()

        # 4) warnings：仅当用户未指定时间而系统做了补全时追加
        if primary["level"] == "METRIC_DEFAULT":
            plan_dict["warnings"].append(
                f"未指定时间，已按主指标 '{primary['metric_name']}' 的默认配置（{primary['time_desc']}）展示数据"
            )
        else:
            plan_dict["warnings"].append(
                f"未指定时间且指标未配置默认时间，已按系统全局默认（{primary['time_desc']}）展示数据"
            )
        return

    # 单指标：按 Level1->Level2 解析并注入
    primary_metric_id = plan.metrics[0].id
    primary = _compute_metric_time_candidate(primary_metric_id, registry)
    plan_dict["time_range"] = primary["time_range"].model_dump()

    if primary["level"] == "METRIC_DEFAULT":
        plan_dict["warnings"].append(
            f"未指定时间，已按主指标 '{primary['metric_name']}' 的默认配置（{primary['time_desc']}）展示数据"
        )
    else:
        plan_dict["warnings"].append(
            f"未指定时间且指标未配置默认时间，已按系统全局默认（{primary['time_desc']}）展示数据"
        )


def _extract_all_ids_from_plan(plan: QueryPlan) -> Set[str]:
    """
    从计划中提取所有 ID 字段
    
    Args:
        plan: 查询计划对象
    
    Returns:
        Set[str]: 所有 ID 的集合
    """
    ids = set()
    
    # 从 metrics 中提取
    for metric in plan.metrics:
        ids.add(metric.id)
    
    # 从 dimensions 中提取
    for dimension in plan.dimensions:
        ids.add(dimension.id)
    
    # 从 filters 中提取
    for filter_item in plan.filters:
        ids.add(filter_item.id)
    
    # 从 order_by 中提取
    for order_item in plan.order_by:
        ids.add(order_item.id)
    
    return ids


# ============================================================
# 核心处理函数
# ============================================================
async def validate_and_normalize_plan(
    plan: QueryPlan,
    context: RequestContext,
    registry: SemanticRegistry
) -> QueryPlan:
    """
    验证和规范化查询计划
    
    Args:
        plan: 原始查询计划
        context: 请求上下文
        registry: 语义注册表实例
    
    Returns:
        QueryPlan: 验证和规范化后的查询计划
    
    Raises:
        MissingMetricError: 当 AGG/TREND 意图缺少指标时
        PermissionDeniedError: 当计划包含未授权的 ID 时
        UnsupportedMultiFactError: 当计划包含多个事实表时
    """
    stage3_start = time.perf_counter()
    logger.info(
        "Starting Stage 3: Validation and Normalization",
        extra={
            "intent": plan.intent.value,
            "request_id": context.request_id
        }
    )
    
    # 创建计划的副本用于修改
    # 注意：Pydantic 模型默认是不可变的，我们需要通过 model_copy 或重新构建
    plan_dict = plan.model_dump()
    
    # Checkpoint 1: Structural Sanity (结构完整性检查)
    # 检查 AGG/TREND 意图是否缺少指标
    
    # 详细记录接收到的计划信息（用于调试）
    logger.debug(
        "Plan received in Stage 3 validation",
        extra={
            "intent": plan.intent.value,
            "metrics_count": len(plan.metrics),
            "metrics": [{"id": m.id, "compare_mode": m.compare_mode.value if m.compare_mode else None} for m in plan.metrics],
            "dimensions_count": len(plan.dimensions),
            "dimensions": [{"id": d.id, "time_grain": d.time_grain.value if d.time_grain else None} for d in plan.dimensions],
            "filters_count": len(plan.filters),
            "filters": [{"id": f.id, "op": f.op.value, "values": f.values} for f in plan.filters],
            "warnings": plan.warnings if hasattr(plan, 'warnings') and plan.warnings else [],
        }
    )
    
    if plan.intent in [PlanIntent.AGG, PlanIntent.TREND]:
        if not plan.metrics or len(plan.metrics) == 0:
            # Permission Shadow Check 熔断：
            # 如果 Stage2 在 warnings 中写入了 [PERMISSION_DENIED] 前缀，
            # 则说明“指标为空”是由于 RBAC 拦截导致，应返回明确的权限错误，
            # 而不是泛化的 MissingMetricError。
            try:
                warnings_list = plan.warnings if hasattr(plan, "warnings") and plan.warnings else []
                permission_mark = next(
                    (w for w in warnings_list if isinstance(w, str) and w.startswith("[PERMISSION_DENIED]")),
                    None,
                )
            except Exception:
                permission_mark = None

            if permission_mark:
                logger.error(
                    "Plan metrics empty due to permission denied",
                    extra={
                        "intent": plan.intent.value,
                        "request_id": context.request_id,
                        "role_id": context.role_id,
                        "permission_warning": permission_mark,
                    },
                )
                raise PermissionDeniedError(permission_mark)

            # 详细记录为什么缺少 metrics（用于调试）
            logger.error(
                f"Plan with intent {plan.intent.value} must have at least one metric",
                extra={
                    "intent": plan.intent.value,
                    "metrics_count": len(plan.metrics) if plan.metrics else 0,
                    "metrics_list": [m.id for m in plan.metrics] if plan.metrics else [],
                    "dimensions_count": len(plan.dimensions),
                    "dimensions": [d.id for d in plan.dimensions],
                    "filters": [f.id for f in plan.filters],
                    "warnings": plan.warnings if hasattr(plan, 'warnings') and plan.warnings else [],
                    # 检查注册表中是否有相关的 metrics
                    "available_metrics_in_registry": [
                        term_id for term_id in registry.metadata_map.keys() 
                        if term_id.startswith("METRIC_")
                    ][:10],  # 只显示前10个
                    # 检查注册表中是否有相关的 dimensions（可能有助于理解问题）
                    "available_dimensions_in_registry": [
                        term_id for term_id in registry.metadata_map.keys() 
                        if term_id.startswith("DIM_")
                    ][:10],  # 只显示前10个
                }
            )
            raise MissingMetricError(
                f"Plan with intent {plan.intent.value} must have at least one metric"
            )
    
    # 初始化空字段
    if plan_dict.get("filters") is None:
        plan_dict["filters"] = []
    if plan_dict.get("order_by") is None:
        plan_dict["order_by"] = []
    if plan_dict.get("warnings") is None:
        plan_dict["warnings"] = []
    
    # Checkpoint 2: Security Enforcement (权限复核)
    # 获取计划中所有 ID
    plan_ids = _extract_all_ids_from_plan(plan)
    
    # 获取用户允许的 ID
    allowed_ids = registry.get_allowed_ids(context.role_id)
    
    # 检查是否有未授权的 ID
    unauthorized_ids = plan_ids - allowed_ids
    if unauthorized_ids:
        unauthorized_ids_list = list(unauthorized_ids)
        logger.error(
            "Plan contains unauthorized IDs",
            extra={
                "unauthorized_ids": unauthorized_ids_list,
                "role_id": context.role_id
            }
        )
        raise PermissionDeniedError(
            f"Plan contains unauthorized IDs: {unauthorized_ids}. "
            f"This may be a security violation (prompt injection attempt)."
        )
    
    logger.debug(f"Security check passed: all {len(plan_ids)} IDs are authorized")
    
    # Checkpoint 3: Semantic Connectivity (语义连通性校验)
    # Single Entity Rule (MVP)
    if plan.metrics:
        entity_ids = set()
        for metric in plan.metrics:
            metric_def = registry.get_metric_def(metric.id)
            if metric_def:
                entity_id = metric_def.get("entity_id")
                if entity_id:
                    entity_ids.add(entity_id)
        
        if len(entity_ids) > 1:
            entity_ids_list = list(entity_ids)
            logger.error(
                "Plan contains metrics from multiple entities",
                extra={"entity_ids": entity_ids_list}
            )
            raise UnsupportedMultiFactError(
                f"Plan contains metrics from multiple entities: {entity_ids}. "
                f"Multi-fact queries are not supported in MVP."
            )
        
        primary_entity_id = list(entity_ids)[0] if entity_ids else None
        logger.debug(f"Primary entity ID: {primary_entity_id}")
    
    # Metric-Dimension Compatibility
    compatible_dimensions = []
    if plan.metrics:
        # 只有当存在指标时才检查维度兼容性
        for dimension in plan.dimensions:
            # 对于每个指标，检查维度是否兼容
            is_compatible = False
            for metric in plan.metrics:
                if registry.check_compatibility(metric.id, dimension.id):
                    is_compatible = True
                    break
            
            if is_compatible:
                compatible_dimensions.append(dimension)
            else:
                warning_msg = (
                    f"Dimension '{dimension.id}' is not compatible with any metric in the plan. "
                    f"Removed from dimensions list."
                )
                plan_dict["warnings"].append(warning_msg)
                logger.warning(
                    warning_msg,
                    extra={
                        "dimension_id": dimension.id,
                        "metric_ids": [m.id for m in plan.metrics]
                    }
                )
    else:
        # 如果没有指标（DETAIL 意图），保留所有维度
        compatible_dimensions = plan.dimensions
    
    plan_dict["dimensions"] = [d.model_dump() for d in compatible_dimensions]
    
    # Checkpoint 4: Normalization & Injection (规范化与注入)
    # Time Window
    try:
        _inject_time_window_if_needed(plan, plan_dict, registry)
    except (AmbiguousTimeError, ConfigurationError):
        # 按设计：一旦进入 AMBIGUOUS_TIME / CONFIGURATION_ERROR，不写入 time_range，不追加 time 补全 warnings
        raise
    
    # Mandatory Filters
    existing_filter_ids = {f.id for f in plan.filters}
    
    for metric in plan.metrics:
        metric_def = registry.get_metric_def(metric.id)
        if metric_def:
            default_filters = metric_def.get("default_filters", [])
            for filter_id in default_filters:
                if filter_id not in existing_filter_ids:
                    # 创建逻辑过滤器项
                    # 注意：逻辑过滤器（LF_*）是预定义的过滤器组合
                    # 它们在后端 SQL 生成时会被特殊处理
                    # 这里我们使用占位符 op 和 values 以满足 FilterItem 模型要求
                    # 实际的值将在后续阶段从 registry 中获取
                    filter_item = FilterItem(
                        id=filter_id,
                        op=FilterOp.IN,  # 占位符，逻辑过滤器在后端处理时会忽略此值
                        values=[]  # 占位符，逻辑过滤器在后端处理时会忽略此值
                    )
                    plan_dict["filters"].append(filter_item.model_dump())
                    existing_filter_ids.add(filter_id)
                    logger.debug(
                        f"Injected mandatory filter {filter_id} from metric {metric.id}"
                    )
    
    # Default Limit
    config = get_pipeline_config()
    if plan_dict.get("limit") is None:
        plan_dict["limit"] = config.default_limit
        logger.debug(f"Set default limit: {config.default_limit}")
    else:
        # 限制最大值
        if plan_dict["limit"] > config.max_limit_cap:
            plan_dict["limit"] = config.max_limit_cap
            warning_msg = (
                f"Limit {plan.limit} exceeds maximum cap {config.max_limit_cap}. "
                f"Capped to {config.max_limit_cap}."
            )
            plan_dict["warnings"].append(warning_msg)
            logger.warning(
                warning_msg,
                extra={
                    "original_limit": plan.limit,
                    "capped_limit": config.max_limit_cap
                }
            )
    
    # 重新构建 QueryPlan 对象
    try:
        validated_plan = QueryPlan(**plan_dict)
        
        stage3_ms = int((time.perf_counter() - stage3_start) * 1000)
        logger.info(
            "Stage 3 completed successfully",
            extra={
                "intent": validated_plan.intent.value,
                "metrics_count": len(validated_plan.metrics),
                "dimensions_count": len(validated_plan.dimensions),
                "filters_count": len(validated_plan.filters),
                "has_time_range": validated_plan.time_range is not None,
                "limit": validated_plan.limit,
                "warnings_count": len(validated_plan.warnings),
                "stage3_ms": stage3_ms,
                "validated_metrics": [{"id": m.id, "compare_mode": m.compare_mode.value if m.compare_mode else None} for m in validated_plan.metrics],
                "validated_dimensions": [{"id": d.id, "time_grain": d.time_grain.value if d.time_grain else None} for d in validated_plan.dimensions],
                "validated_filters": [{"id": f.id, "op": f.op.value, "values": f.values} for f in validated_plan.filters],
                "validated_time_range": validated_plan.time_range.model_dump() if validated_plan.time_range else None,
                "validated_order_by": [{"id": o.id, "direction": o.direction.value} for o in validated_plan.order_by] if validated_plan.order_by else []
            }
        )
        
        return validated_plan
    
    except Exception as e:
        logger.error(
            "Failed to rebuild QueryPlan after validation",
            extra={"error": str(e), "plan_dict": plan_dict}
        )
        raise Stage3Error(f"Failed to rebuild QueryPlan: {str(e)}") from e

"""
Stage 3: Validation and Normalization (验证与规范化)

对 Stage 2 生成的 QueryPlan 进行验证、规范化和安全检查。
对应详细设计文档 3.3 的定义。
"""
from typing import Dict, List, Optional, Set, Any

from config.pipeline_config import get_pipeline_config
from core.semantic_registry import SemanticRegistry
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


# ============================================================
# 辅助函数
# ============================================================
def _get_default_time_range(
    metric_id: str,
    registry: SemanticRegistry,
    context: RequestContext
) -> TimeRange:
    """
    获取默认时间范围
    
    优先级：
    1. 指标的 default_time.time_range_fallback
    2. 指标的 default_time.time_window_id（从 time_windows 查找）
    3. 全局配置的 default_time_window_id
    4. 硬编码默认值（30天）
    
    Args:
        metric_id: 指标 ID
        registry: 语义注册表实例
        context: 请求上下文
    
    Returns:
        TimeRange: 默认时间范围对象
    """
    metric_def = registry.get_metric_def(metric_id)
    
    # 尝试从指标的 default_time 获取
    if metric_def:
        default_time = metric_def.get("default_time")
        if default_time:
            # 优先使用 time_range_fallback
            time_range_fallback = default_time.get("time_range_fallback")
            if time_range_fallback:
                fallback_type = time_range_fallback.get("type")
                return TimeRange(
                    type=TimeRangeType.LAST_N if fallback_type == "LAST_N" else TimeRangeType.ABSOLUTE,
                    value=time_range_fallback.get("value"),
                    unit=time_range_fallback.get("unit"),
                    start=time_range_fallback.get("start"),
                    end=time_range_fallback.get("end")
                )
            
            # 其次使用 time_window_id
            time_window_id = default_time.get("time_window_id")
            if time_window_id:
                # 尝试从 global_config 中查找 time_windows
                time_windows = registry.global_config.get("time_windows", [])
                for tw in time_windows:
                    if tw.get("id") == time_window_id:
                        template = tw.get("template", {})
                        tw_type = template.get("type")
                        if tw_type == "LAST_N":
                            return TimeRange(
                                type=TimeRangeType.LAST_N,
                                value=template.get("value"),
                                unit=template.get("unit")
                            )
                        elif tw_type == "ABSOLUTE":
                            return TimeRange(
                                type=TimeRangeType.ABSOLUTE,
                                start=template.get("start"),
                                end=template.get("end")
                            )
    
    # 使用全局默认配置
    global_config = registry.global_config
    default_time_window_id = global_config.get("global_settings", {}).get("default_time_window_id")
    
    if default_time_window_id:
        # 尝试从 time_windows 查找
        time_windows = global_config.get("time_windows", [])
        for tw in time_windows:
            if tw.get("id") == default_time_window_id:
                template = tw.get("template", {})
                tw_type = template.get("type")
                if tw_type == "LAST_N":
                    return TimeRange(
                        type=TimeRangeType.LAST_N,
                        value=template.get("value"),
                        unit=template.get("unit")
                    )
                elif tw_type == "ABSOLUTE":
                    return TimeRange(
                        type=TimeRangeType.ABSOLUTE,
                        start=template.get("start"),
                        end=template.get("end")
                    )
    
    # 最终 fallback：使用硬编码的默认值（30天）
    logger.warning(
        f"Using hardcoded default time range for metric {metric_id}",
        extra={"metric_id": metric_id}
    )
    return TimeRange(
        type=TimeRangeType.LAST_N,
        value=30,
        unit="DAY"
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
    if plan_dict.get("time_range") is None and plan.metrics:
        # 使用第一个指标的默认时间范围
        primary_metric_id = plan.metrics[0].id
        default_time_range = _get_default_time_range(primary_metric_id, registry, context)
        plan_dict["time_range"] = default_time_range.model_dump()
        logger.debug(
            f"Injected default time range from metric {primary_metric_id}",
            extra={"time_range": plan_dict["time_range"]}
        )
    
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

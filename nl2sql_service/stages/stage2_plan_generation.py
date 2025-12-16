"""
Stage 2: Plan Generation (计划生成)

将子查询转换为结构化的查询计划（QueryPlan）。
使用 RAG 进行语义检索，生成 Skeleton Plan。
对应详细设计文档 3.2 的定义。
"""
from typing import Any, Dict, List, Set, Tuple

from config.pipeline_config import get_pipeline_config
from core.ai_client import get_ai_client
from core.semantic_registry import SemanticRegistry
from schemas.plan import QueryPlan
from schemas.request import RequestContext, SubQueryItem
from utils.log_manager import get_logger
from utils.prompt_templates import PROMPT_PLAN_GENERATION

logger = get_logger(__name__)


# ============================================================
# 异常定义
# ============================================================
class Stage2Error(Exception):
    """
    Stage 2 处理异常
    
    用于表示计划生成阶段的错误，包括：
    - RAG 检索失败
    - LLM 输出解析失败
    - 计划结构验证失败
    """
    pass


# ============================================================
# 辅助函数
# ============================================================
def _format_schema_context(terms: List[str], registry: SemanticRegistry) -> str:
    """
    格式化语义资源上下文，生成 LLM 可读的文本块
    
    Args:
        terms: 术语 ID 列表
        registry: 语义注册表实例
    
    Returns:
        str: 格式化后的上下文字符串
    """
    metrics_lines = []
    dimensions_lines = []
    
    for term_id in terms:
        term_def = registry.get_term(term_id)
        if not term_def:
            continue
        
        # 判断是指标还是维度
        is_metric = term_id.startswith("METRIC_")
        is_dimension = term_id.startswith("DIM_")
        
        if not (is_metric or is_dimension):
            continue  # 跳过非指标/维度的术语
        
        # 提取基本信息
        name = term_def.get("name", "")
        aliases = term_def.get("aliases", [])
        description = term_def.get("description", "")
        
        # 格式化别名
        aliases_str = ", ".join(aliases) if aliases else "无"
        
        # 构建基础行
        base_line = f"ID: {term_id} | Name: {name} | Aliases: {aliases_str} | Desc: {description}"
        
        # 对于维度，检查是否有枚举值
        if is_dimension:
            enum_value_set_id = term_def.get("enum_value_set_id")
            if enum_value_set_id:
                # 获取枚举定义
                enum_def = registry.get_term(enum_value_set_id)
                if enum_def and "values" in enum_def:
                    enum_values = enum_def["values"]
                    # 只显示前几个枚举值（反幻觉技术）
                    if len(enum_values) > 5:
                        values_str = ", ".join(enum_values[:5]) + f"... (共{len(enum_values)}个)"
                    else:
                        values_str = ", ".join(enum_values)
                    base_line += f" | Values: [{values_str}]"
            
            dimensions_lines.append(base_line)
        elif is_metric:
            metrics_lines.append(base_line)
    
    # 组装最终文本
    context_parts = []
    
    if metrics_lines:
        context_parts.append("[METRICS]")
        context_parts.extend(metrics_lines)
        context_parts.append("")  # 空行分隔
    
    if dimensions_lines:
        context_parts.append("[DIMENSIONS]")
        context_parts.extend(dimensions_lines)
    
    return "\n".join(context_parts)


def _extract_all_ids_from_plan(plan_dict: Dict[str, Any]) -> Set[str]:
    """
    从计划字典中提取所有 ID 字段
    
    Args:
        plan_dict: 计划字典
    
    Returns:
        Set[str]: 所有 ID 的集合
    """
    ids = set()
    
    # 从 metrics 中提取
    for metric in plan_dict.get("metrics", []):
        if isinstance(metric, dict) and "id" in metric:
            ids.add(metric["id"])
    
    # 从 dimensions 中提取
    for dimension in plan_dict.get("dimensions", []):
        if isinstance(dimension, dict) and "id" in dimension:
            ids.add(dimension["id"])
    
    # 从 filters 中提取
    for filter_item in plan_dict.get("filters", []):
        if isinstance(filter_item, dict) and "id" in filter_item:
            ids.add(filter_item["id"])
    
    # 从 order_by 中提取
    for order_item in plan_dict.get("order_by", []):
        if isinstance(order_item, dict) and "id" in order_item:
            ids.add(order_item["id"])
    
    return ids


def _perform_anti_hallucination_check(
    plan_dict: Dict[str, Any],
    registry: SemanticRegistry
) -> Tuple[Dict[str, Any], List[str]]:
    """
    执行反幻觉检查，移除无效的 ID
    
    Args:
        plan_dict: 计划字典
        registry: 语义注册表实例
    
    Returns:
        Tuple[Dict[str, Any], List[str]]: (清理后的计划字典, 警告列表)
    """
    warnings = list(plan_dict.get("warnings", []))
    cleaned_plan = plan_dict.copy()
    
    # 提取所有 ID
    all_ids = _extract_all_ids_from_plan(plan_dict)
    
    # 检查每个 ID
    invalid_ids = []
    for term_id in all_ids:
        if not registry.get_term(term_id):
            invalid_ids.append(term_id)
            warnings.append(f"Invalid ID '{term_id}' removed (hallucination detected)")
    
    # 移除无效的 metrics
    if invalid_ids:
        cleaned_plan["metrics"] = [
            m for m in cleaned_plan.get("metrics", [])
            if isinstance(m, dict) and m.get("id") not in invalid_ids
        ]
        
        # 移除无效的 dimensions
        cleaned_plan["dimensions"] = [
            d for d in cleaned_plan.get("dimensions", [])
            if isinstance(d, dict) and d.get("id") not in invalid_ids
        ]
        
        # 移除无效的 filters
        cleaned_plan["filters"] = [
            f for f in cleaned_plan.get("filters", [])
            if isinstance(f, dict) and f.get("id") not in invalid_ids
        ]
        
        # 移除无效的 order_by
        cleaned_plan["order_by"] = [
            o for o in cleaned_plan.get("order_by", [])
            if isinstance(o, dict) and o.get("id") not in invalid_ids
        ]
    
    cleaned_plan["warnings"] = warnings
    return cleaned_plan, warnings


# ============================================================
# 核心处理函数
# ============================================================
async def process_subquery(
    sub_query: SubQueryItem,
    context: RequestContext,
    registry: SemanticRegistry
) -> QueryPlan:
    """
    处理子查询，生成结构化的查询计划
    
    Args:
        sub_query: 子查询项
        context: 请求上下文
        registry: 语义注册表实例
    
    Returns:
        QueryPlan: 查询计划对象
    
    Raises:
        Stage2Error: 当处理失败时抛出
    """
    logger.info(
        "Starting Stage 2: Plan Generation",
        extra={
            "sub_query_id": sub_query.id,
            "request_id": context.request_id
        }
    )
    
    # Step 1: RAG - Security-First Hybrid Retrieval
    # 获取允许的 ID 列表（权限过滤）
    allowed_ids_set = registry.get_allowed_ids(context.role_id)
    allowed_ids_list = list(allowed_ids_set)
    
    logger.debug(
        f"Retrieved {len(allowed_ids_list)} allowed IDs for role {context.role_id}"
    )
    
    # 关键词搜索（精确匹配）
    keyword_matches: Set[str] = set()
    query_text = sub_query.description.lower()
    
    # 在 keyword_index 中搜索
    for keyword, term_ids in registry.keyword_index.items():
        if keyword.lower() in query_text:
            # 只添加允许的 ID
            for term_id in term_ids:
                if term_id in allowed_ids_set:
                    keyword_matches.add(term_id)
    
    logger.debug(f"Keyword search found {len(keyword_matches)} matches")
    
    # 向量搜索
    try:
        config = get_pipeline_config()
        vector_results = await registry.search_similar_terms(
            query=sub_query.description,
            allowed_ids=allowed_ids_list,
            top_k=config.vector_search_top_k
        )
        
        # 应用相似度阈值过滤
        vector_matches: Set[str] = set()
        for term_id, score in vector_results:
            if score >= config.similarity_threshold:
                vector_matches.add(term_id)
        
        logger.debug(
            f"Vector search found {len(vector_matches)} matches "
            f"(threshold: {config.similarity_threshold})"
        )
    except Exception as e:
        logger.error(f"Vector search failed: {e}")
        raise Stage2Error(f"Vector search failed: {str(e)}") from e
    
    # 合并结果并去重
    all_matches = keyword_matches | vector_matches
    
    # 截断到 max_term_recall
    config = get_pipeline_config()
    max_recall = config.max_term_recall
    
    # 转换为列表并截断（保持顺序：关键词匹配优先）
    final_terms = list(keyword_matches) + [t for t in vector_matches if t not in keyword_matches]
    final_terms = final_terms[:max_recall]
    
    logger.info(
        f"RAG retrieval completed: {len(final_terms)} terms "
        f"(keyword: {len(keyword_matches)}, vector: {len(vector_matches)})"
    )
    
    # Step 2: RAG - Schema Context Formatting
    schema_context = _format_schema_context(final_terms, registry)
    
    if not schema_context.strip():
        logger.warning("Schema context is empty, LLM may struggle to generate plan")
    
    logger.debug(f"Schema context length: {len(schema_context)} characters")
    
    # Step 3: LLM Prompt Generation
    current_date_str = context.current_date.strftime("%Y-%m-%d")
    
    formatted_prompt = PROMPT_PLAN_GENERATION.format(
        sub_query_description=sub_query.description,
        available_resources=schema_context,
        current_date=current_date_str
    )
    
    messages = [
        {
            "role": "user",
            "content": formatted_prompt
        }
    ]
    
    # 调用 LLM
    try:
        ai_client = get_ai_client()
        plan_dict = await ai_client.generate_plan(
            messages=messages,
            temperature=0.0
        )
        
        logger.debug(
            "LLM response received",
            extra={"response_keys": list(plan_dict.keys())}
        )
    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        raise Stage2Error(f"Failed to call LLM for plan generation: {str(e)}") from e
    
    # Step 4: Anti-Hallucination
    # plan_dict 已经是解析后的 JSON 对象（从 ai_client.generate_plan 返回）
    # JSON 解析错误已在 provider 层处理，无需在此重复处理
    # 执行反幻觉检查
    cleaned_plan, warnings = _perform_anti_hallucination_check(plan_dict, registry)
    
    if warnings:
        logger.warning(
            f"Anti-hallucination check found {len(warnings)} issues",
            extra={"warnings": warnings}
        )
    
    # Step 5: Pydantic Instantiation
    try:
        query_plan = QueryPlan(**cleaned_plan)
        
        # 记录 Plan 的详细信息
        logger.info(
            "Stage 2 completed successfully",
            extra={
                "intent": query_plan.intent.value,
                "metrics_count": len(query_plan.metrics),
                "dimensions_count": len(query_plan.dimensions),
                "filters_count": len(query_plan.filters),
                "warnings_count": len(query_plan.warnings),
                "metrics": [{"id": m.id, "compare_mode": m.compare_mode.value if m.compare_mode else None} for m in query_plan.metrics],
                "dimensions": [{"id": d.id, "time_grain": d.time_grain.value if d.time_grain else None} for d in query_plan.dimensions],
                "filters": [{"id": f.id, "op": f.op.value, "values": f.values} for f in query_plan.filters],
                "time_range": query_plan.time_range.model_dump() if query_plan.time_range else None,
                "order_by": [{"id": o.id, "direction": o.direction.value} for o in query_plan.order_by] if query_plan.order_by else [],
                "limit": query_plan.limit
            }
        )
        
        return query_plan
    
    except Exception as e:
        logger.error(
            "Failed to instantiate QueryPlan",
            extra={"error": str(e), "plan_dict": cleaned_plan}
        )
        raise Stage2Error(f"Failed to instantiate QueryPlan: {str(e)}") from e

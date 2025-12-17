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
    
    # 详细记录反幻觉检查开始
    logger.debug(
        "Anti-hallucination check: checking IDs",
        extra={
            "total_ids": len(all_ids),
            "all_ids": list(all_ids),
            "metrics_ids": [m.get("id") if isinstance(m, dict) else m.id for m in plan_dict.get("metrics", [])],
            "dimensions_ids": [d.get("id") if isinstance(d, dict) else d.id for d in plan_dict.get("dimensions", [])],
        }
    )
    
    # 检查每个 ID
    invalid_ids = []
    invalid_metrics = []
    invalid_dimensions = []
    invalid_filters = []
    invalid_order_by = []
    
    for term_id in all_ids:
        term_def = registry.get_term(term_id)
        if not term_def:
            invalid_ids.append(term_id)
            warnings.append(f"Invalid ID '{term_id}' removed (hallucination detected)")
            logger.warning(
                f"Invalid ID '{term_id}' not found in registry (hallucination detected)",
                extra={
                    "term_id": term_id,
                    "registry_has_term": False,
                }
            )
        else:
            logger.debug(
                f"ID '{term_id}' validated in registry",
                extra={
                    "term_id": term_id,
                    "term_type": term_def.get("type", "UNKNOWN"),
                    "term_name": term_def.get("name", ""),
                }
            )
    
    # 分类无效的术语（用于控制台显示）
    if invalid_ids:
        for metric in cleaned_plan.get("metrics", []):
            if isinstance(metric, dict) and metric.get("id") in invalid_ids:
                invalid_metrics.append(metric.get("id"))
        for dimension in cleaned_plan.get("dimensions", []):
            if isinstance(dimension, dict) and dimension.get("id") in invalid_ids:
                invalid_dimensions.append(dimension.get("id"))
        for filter_item in cleaned_plan.get("filters", []):
            if isinstance(filter_item, dict) and filter_item.get("id") in invalid_ids:
                invalid_filters.append(filter_item.get("id"))
        for order_item in cleaned_plan.get("order_by", []):
            if isinstance(order_item, dict) and order_item.get("id") in invalid_ids:
                invalid_order_by.append(order_item.get("id"))
        
        # 在控制台明确显示无效术语
        logger.warning("-" * 80)
        logger.warning(f"[反幻觉检查] 发现 {len(invalid_ids)} 个无效术语 (LLM 幻觉):")
        if invalid_metrics:
            logger.warning(f"  无效指标 ({len(invalid_metrics)} 个): {', '.join(invalid_metrics)}")
        if invalid_dimensions:
            logger.warning(f"  无效维度 ({len(invalid_dimensions)} 个): {', '.join(invalid_dimensions)}")
        if invalid_filters:
            logger.warning(f"  无效过滤器 ({len(invalid_filters)} 个): {', '.join(invalid_filters)}")
        if invalid_order_by:
            logger.warning(f"  无效排序字段 ({len(invalid_order_by)} 个): {', '.join(invalid_order_by)}")
        logger.warning(f"  所有无效术语: {', '.join(invalid_ids)}")
        logger.warning("-" * 80)
        
        # 详细记录被移除的 metrics（用于调试）
        removed_metrics = [
            m for m in cleaned_plan.get("metrics", [])
            if isinstance(m, dict) and m.get("id") in invalid_ids
        ]
        if removed_metrics:
            logger.warning(
                f"Removing {len(removed_metrics)} invalid metrics from plan",
                extra={
                    "removed_metrics": [m.get("id") for m in removed_metrics],
                    "invalid_ids": invalid_ids,
                }
            )
    
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
    # ============================================================
    # 明确显示：进入 Stage 2 的原始子查询（被拆解后的原始问题）
    # ============================================================
    logger.info(
        "=" * 80
    )
    logger.info(
        f"[Stage 2] 原始子查询 (Sub-Query {sub_query.id}):",
        extra={
            "sub_query_id": sub_query.id,
            "request_id": context.request_id,
            "original_query": sub_query.description
        }
    )
    logger.info(f"  {sub_query.description}")
    logger.info("=" * 80)
    
    # Step 1: RAG - Security-First Hybrid Retrieval
    # 获取允许的 ID 列表（权限过滤）
    allowed_ids_set = registry.get_allowed_ids(context.role_id)
    allowed_ids_list = list(allowed_ids_set)
    
    logger.debug(
        f"已获取角色 {context.role_id} 的 {len(allowed_ids_list)} 个允许访问的术语 ID"
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
    
    # 详细记录关键词匹配结果 - 在控制台明确显示
    keyword_metrics = [m for m in keyword_matches if m.startswith("METRIC_")]
    keyword_dimensions = [m for m in keyword_matches if m.startswith("DIM_")]
    keyword_others = [m for m in keyword_matches if not m.startswith("METRIC_") and not m.startswith("DIM_")]
    
    logger.info("-" * 80)
    logger.info(f"[关键词匹配] 共匹配到 {len(keyword_matches)} 个术语:")
    if keyword_metrics:
        logger.info(f"  指标 ({len(keyword_metrics)} 个): {', '.join(keyword_metrics)}")
    if keyword_dimensions:
        logger.info(f"  维度 ({len(keyword_dimensions)} 个): {', '.join(keyword_dimensions)}")
    if keyword_others:
        logger.info(f"  其他类型 ({len(keyword_others)} 个): {', '.join(keyword_others)}")
    if not keyword_matches:
        logger.info("  (无匹配)")
    # 显示所有匹配到的术语（用于调试）
    logger.info(f"  所有匹配术语: {', '.join(sorted(keyword_matches))}")
    logger.info("-" * 80)
    
    logger.debug(
        "Keyword search results",
        extra={
            "query": sub_query.description,
            "matches_count": len(keyword_matches),
            "matches": list(keyword_matches),
            "metric_matches": keyword_metrics,
            "dimension_matches": keyword_dimensions,
        }
    )
    
    # 向量搜索
    vector_matches: Set[str] = set()  # 在 try 块外初始化，确保即使异常也不会导致变量未定义
    try:
        config = get_pipeline_config()
        vector_results = await registry.search_similar_terms(
            query=sub_query.description,
            allowed_ids=allowed_ids_list,
            top_k=config.vector_search_top_k
        )
        
        # 应用相似度阈值过滤
        vector_matches_with_scores = []
        for result_item in vector_results:
            try:
                # 处理不同的返回格式
                if isinstance(result_item, tuple) and len(result_item) == 2:
                    term_id, score = result_item
                elif isinstance(result_item, dict):
                    term_id = result_item.get("id") or result_item.get("term_id")
                    score = result_item.get("score") or result_item.get("similarity")
                else:
                    logger.warning(f"Unexpected vector result format: {result_item}")
                    continue
                
                # 确保 score 是数字类型
                try:
                    score = float(score)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid score type for term {term_id}: {score}")
                    continue
                
                if score >= config.similarity_threshold:
                    vector_matches.add(term_id)
                    vector_matches_with_scores.append((term_id, score))
            except Exception as e:
                logger.warning(f"Error processing vector result {result_item}: {e}")
                continue
        
        # 详细记录向量搜索结果 - 在控制台明确显示
        vector_metrics = [(t, s) for t, s in vector_matches_with_scores if t and t.startswith("METRIC_")]
        vector_dimensions = [(t, s) for t, s in vector_matches_with_scores if t and t.startswith("DIM_")]
        
        logger.info("-" * 80)
        logger.info(f"[向量匹配] 共匹配到 {len(vector_matches)} 个术语 (相似度阈值: {config.similarity_threshold}):")
        if vector_metrics:
            logger.info(f"  指标 ({len(vector_metrics)} 个):")
            try:
                for term_id, score in sorted(vector_metrics, key=lambda x: x[1] if isinstance(x[1], (int, float)) else 0, reverse=True):
                    try:
                        term_def = registry.get_term(term_id)
                        term_name = term_def.get("name", "") if term_def else ""
                        score_str = f"{score:.4f}" if isinstance(score, (int, float)) else str(score)
                        logger.info(f"    - {term_id} ({term_name}) [相似度: {score_str}]")
                    except Exception as e:
                        logger.warning(f"Error displaying metric {term_id}: {e}")
                        logger.info(f"    - {term_id} [相似度: {score}]")
            except Exception as e:
                logger.warning(f"Error sorting/displaying vector metrics: {e}")
                for term_id, score in vector_metrics:
                    logger.info(f"    - {term_id} [相似度: {score}]")
        if vector_dimensions:
            logger.info(f"  维度 ({len(vector_dimensions)} 个):")
            try:
                for term_id, score in sorted(vector_dimensions, key=lambda x: x[1] if isinstance(x[1], (int, float)) else 0, reverse=True):
                    try:
                        term_def = registry.get_term(term_id)
                        term_name = term_def.get("name", "") if term_def else ""
                        score_str = f"{score:.4f}" if isinstance(score, (int, float)) else str(score)
                        logger.info(f"    - {term_id} ({term_name}) [相似度: {score_str}]")
                    except Exception as e:
                        logger.warning(f"Error displaying dimension {term_id}: {e}")
                        logger.info(f"    - {term_id} [相似度: {score}]")
            except Exception as e:
                logger.warning(f"Error sorting/displaying vector dimensions: {e}")
                for term_id, score in vector_dimensions:
                    logger.info(f"    - {term_id} [相似度: {score}]")
        if not vector_matches:
            logger.info("  (无匹配)")
        logger.info("-" * 80)
        
        logger.debug(
            "Vector search results",
            extra={
                "query": sub_query.description,
                "raw_results_count": len(vector_results),
                "raw_results": [(term_id, score) for term_id, score in vector_results],
                "filtered_matches_count": len(vector_matches),
                "matches": list(vector_matches),
                "metric_results": [(term_id, score) for term_id, score in vector_results if term_id.startswith("METRIC_")],
                "dimension_results": [(term_id, score) for term_id, score in vector_results if term_id.startswith("DIM_")],
                "metric_matches": [m for m in vector_matches if m.startswith("METRIC_")],
                "dimension_matches": [m for m in vector_matches if m.startswith("DIM_")],
                "similarity_threshold": config.similarity_threshold,
            }
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
    
    # 详细记录最终合并结果 - 在控制台明确显示
    final_metrics = [t for t in final_terms if t and t.startswith("METRIC_")]
    final_dimensions = [t for t in final_terms if t and t.startswith("DIM_")]
    
    logger.info("-" * 80)
    logger.info(f"[RAG 检索完成] 最终检索到 {len(final_terms)} 个术语 (关键词: {len(keyword_matches)}, 向量: {len(vector_matches)}, 最大召回: {max_recall}):")
    if final_metrics:
        logger.info(f"  指标 ({len(final_metrics)} 个): {', '.join(final_metrics)}")
        for term_id in final_metrics:
            try:
                term_def = registry.get_term(term_id)
                if term_def:
                    term_name = term_def.get('name', '') if isinstance(term_def, dict) else str(term_def)
                    logger.info(f"    - {term_id}: {term_name}")
                else:
                    logger.info(f"    - {term_id}: (未找到定义)")
            except Exception as e:
                logger.warning(f"Error getting term definition for {term_id}: {e}")
                logger.info(f"    - {term_id}: (获取失败)")
    if final_dimensions:
        logger.info(f"  维度 ({len(final_dimensions)} 个): {', '.join(final_dimensions)}")
        for term_id in final_dimensions:
            try:
                term_def = registry.get_term(term_id)
                if term_def:
                    term_name = term_def.get('name', '') if isinstance(term_def, dict) else str(term_def)
                    logger.info(f"    - {term_id}: {term_name}")
                else:
                    logger.info(f"    - {term_id}: (未找到定义)")
            except Exception as e:
                logger.warning(f"Error getting term definition for {term_id}: {e}")
                logger.info(f"    - {term_id}: (获取失败)")
    if not final_terms:
        logger.info("  (无检索结果)")
    logger.info("-" * 80)
    
    logger.info(
        f"RAG 检索完成: 共检索到 {len(final_terms)} 个术语 "
        f"(关键词匹配: {len(keyword_matches)} 个, 向量匹配: {len(vector_matches)} 个)",
        extra={
            "final_terms": final_terms,
            "final_metric_terms": final_metrics,
            "final_dimension_terms": final_dimensions,
            "keyword_matches": list(keyword_matches),
            "vector_matches": list(vector_matches),
            "max_recall": max_recall,
        }
    )
    
    # Step 2: RAG - Schema Context Formatting
    schema_context = _format_schema_context(final_terms, registry)
    
    if not schema_context.strip():
        logger.warning("Schema context is empty, LLM may struggle to generate plan")
    
    # 详细记录 Schema Context 格式化结果
    logger.debug(
        "Schema context formatted for LLM",
        extra={
            "context_length": len(schema_context),
            "context_preview": schema_context[:500] if len(schema_context) > 500 else schema_context,
            "terms_used": final_terms,
            "metrics_in_context": [t for t in final_terms if t.startswith("METRIC_")],
            "dimensions_in_context": [t for t in final_terms if t.startswith("DIM_")],
        }
    )
    
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
        
        # 详细记录 LLM 返回的原始计划
        logger.debug(
            "LLM response received",
            extra={
                "response_keys": list(plan_dict.keys()),
                "intent": plan_dict.get("intent"),
                "metrics_count": len(plan_dict.get("metrics", [])),
                "metrics": [m.get("id") if isinstance(m, dict) else m.id for m in plan_dict.get("metrics", [])],
                "dimensions_count": len(plan_dict.get("dimensions", [])),
                "dimensions": [d.get("id") if isinstance(d, dict) else d.id for d in plan_dict.get("dimensions", [])],
                "available_metrics_in_context": [t for t in final_terms if t.startswith("METRIC_")],
                "available_dimensions_in_context": [t for t in final_terms if t.startswith("DIM_")],
            }
        )
    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        raise Stage2Error(f"Failed to call LLM for plan generation: {str(e)}") from e
    
    # Step 4: Anti-Hallucination
    # plan_dict 已经是解析后的 JSON 对象（从 ai_client.generate_plan 返回）
    # JSON 解析错误已在 provider 层处理，无需在此重复处理
    
    # 记录 LLM 生成的原始计划（用于调试）
    logger.debug(
        "LLM generated plan (before anti-hallucination)",
        extra={
            "intent": plan_dict.get("intent"),
            "metrics_count": len(plan_dict.get("metrics", [])),
            "metrics": [m.get("id") for m in plan_dict.get("metrics", [])],
            "dimensions_count": len(plan_dict.get("dimensions", [])),
            "dimensions": [d.get("id") for d in plan_dict.get("dimensions", [])],
        }
    )
    
    # 记录检索到的术语（用于调试）
    logger.debug(
        "Retrieved terms from RAG",
        extra={
            "total_terms": len(final_terms),
            "terms": final_terms,
            "metric_terms": [t for t in final_terms if t.startswith("METRIC_")],
            "dimension_terms": [t for t in final_terms if t.startswith("DIM_")],
        }
    )
    
    # 执行反幻觉检查
    cleaned_plan, warnings = _perform_anti_hallucination_check(plan_dict, registry)
    
    # 记录反幻觉检查后的计划（用于调试）
    logger.debug(
        "Plan after anti-hallucination check",
        extra={
            "intent": cleaned_plan.get("intent"),
            "metrics_count": len(cleaned_plan.get("metrics", [])),
            "metrics": [m.get("id") if isinstance(m, dict) else m.id for m in cleaned_plan.get("metrics", [])],
            "dimensions_count": len(cleaned_plan.get("dimensions", [])),
            "dimensions": [d.get("id") if isinstance(d, dict) else d.id for d in cleaned_plan.get("dimensions", [])],
            "warnings_count": len(warnings),
        }
    )
    
    if warnings:
        logger.warning(
            f"反幻觉检查发现 {len(warnings)} 个问题",
            extra={"warnings": warnings}
        )
    
    cleaned_plan["warnings"] = warnings
    
    # Step 5: Pydantic Instantiation
    try:
        query_plan = QueryPlan(**cleaned_plan)
        
        # ============================================================
        # 明确显示：最终生成的 Skeleton Plan
        # ============================================================
        import json
        
        # 构建格式化的 Plan 显示
        plan_display = {
            "intent": query_plan.intent.value,
            "metrics": [
                {
                    "id": m.id,
                    "compare_mode": m.compare_mode.value if m.compare_mode else None
                }
                for m in query_plan.metrics
            ],
            "dimensions": [
                {
                    "id": d.id,
                    "time_grain": d.time_grain.value if d.time_grain else None
                }
                for d in query_plan.dimensions
            ],
            "filters": [
                {
                    "id": f.id,
                    "op": f.op.value,
                    "values": f.values
                }
                for f in query_plan.filters
            ],
            "time_range": query_plan.time_range.model_dump() if query_plan.time_range else None,
            "order_by": [
                {
                    "id": o.id,
                    "direction": o.direction.value
                }
                for o in query_plan.order_by
            ] if query_plan.order_by else [],
            "limit": query_plan.limit,
            "warnings": query_plan.warnings if query_plan.warnings else []
        }
        
        # 在控制台明确打印最终生成的 Plan
        logger.info("=" * 80)
        logger.info(f"[Stage 2] 最终生成的 Skeleton Plan (Sub-Query {sub_query.id}):")
        logger.info(json.dumps(plan_display, ensure_ascii=False, indent=2))
        logger.info("=" * 80)
        
        # 同时记录到 extra 中（用于日志系统）
        logger.info(
            "Stage 2 completed successfully",
            extra={
                "intent": query_plan.intent.value,
                "metrics_count": len(query_plan.metrics),
                "dimensions_count": len(query_plan.dimensions),
                "filters_count": len(query_plan.filters),
                "warnings_count": len(query_plan.warnings),
                "metrics": plan_display["metrics"],
                "dimensions": plan_display["dimensions"],
                "filters": plan_display["filters"],
                "time_range": plan_display["time_range"],
                "order_by": plan_display["order_by"],
                "limit": plan_display["limit"],
                "warnings": plan_display["warnings"],
                "full_plan_json": json.dumps(plan_display, ensure_ascii=False)
            }
        )
        
        return query_plan
    
    except Exception as e:
        logger.error(
            "Failed to instantiate QueryPlan",
            extra={"error": str(e), "plan_dict": cleaned_plan}
        )
        raise Stage2Error(f"Failed to instantiate QueryPlan: {str(e)}") from e

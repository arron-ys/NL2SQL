"""
Pipeline Orchestrator Module

并发流水线编排器，负责并行执行多个子查询流水线。

对应详细设计文档 2.3 的定义。
"""
import asyncio
import re
from typing import Any, Dict, List, Union

from config.pipeline_config import get_pipeline_config
from core.semantic_registry import SemanticRegistry
from schemas.error import PipelineError
from schemas.request import QueryRequestDescription, RequestContext, SubQueryItem
from schemas.result import ExecutionResult
from stages import stage2_plan_generation
from stages import stage3_validation
from stages import stage4_sql_gen
from stages import stage5_execution
from stages.stage3_validation import (
    MissingMetricError,
    PermissionDeniedError,
    UnsupportedMultiFactError,
)
from utils.log_manager import get_logger

logger = get_logger(__name__)

_METRIC_ID_RE = re.compile(r"\bMETRIC_[A-Z0-9_]+\b")


def _extract_domain_from_permission_warning(detail: str) -> str:
    """
    从 Stage2 的 warning 格式中提取 domain：
    "[PERMISSION_DENIED] ... (Domain: SALES)"
    """
    if not isinstance(detail, str):
        return "UNKNOWN"
    m = re.search(r"\(Domain:\s*([A-Za-z0-9_]+)\)", detail)
    if not m:
        return "UNKNOWN"
    return m.group(1) or "UNKNOWN"


def _sanitize_permission_denied_detail(detail: str) -> str:
    """
    对 PermissionDeniedError 的 detail 进行脱敏，避免把 METRIC_* ID 透传到 Stage6/前端。
    """
    if not isinstance(detail, str) or not detail:
        return "权限不足"
    # 注意：占位符不要包含 "METRIC_" 字样，避免前端/测试误判为泄露
    sanitized = _METRIC_ID_RE.sub("[REDACTED]", detail)
    if len(sanitized) > 300:
        sanitized = sanitized[:300] + "..."
    return sanitized


# ============================================================
# 辅助函数
# ============================================================
def _map_exception_to_pipeline_error(exception: Exception, stage: str) -> PipelineError:
    """
    将异常映射为 PipelineError
    
    Args:
        exception: 捕获的异常
        stage: 发生错误的阶段名称
    
    Returns:
        PipelineError: 标准化的错误对象
    """
    # 确定错误代码
    error_code = "UNKNOWN_ERROR"

    # 优先使用异常自身的稳定 code（用于 Stage3/语义配置类错误对齐 Stage6）
    if hasattr(exception, "code") and getattr(exception, "code"):
        error_code = getattr(exception, "code")
        return PipelineError(stage=stage, code=error_code, message=str(exception))
    
    if isinstance(exception, PermissionDeniedError):
        error_code = "PERMISSION_DENIED"
        # 对外/给 Stage6：脱敏且给出明确域级语义（不暴露 METRIC_*）
        domain_id = _extract_domain_from_permission_warning(str(exception))
        return PipelineError(
            stage=stage,
            code=error_code,
            message=f"您当前的角色没有权限访问查询中涉及的业务域数据（Domain: {domain_id}）。",
        )
    elif isinstance(exception, MissingMetricError):
        error_code = "MISSING_METRIC"
    elif isinstance(exception, UnsupportedMultiFactError):
        error_code = "UNSUPPORTED_MULTI_FACT"
    elif "timeout" in str(exception).lower() or "超时" in str(exception):
        error_code = "TIMEOUT"
    elif "connection" in str(exception).lower() or "连接" in str(exception):
        error_code = "CONNECTION_ERROR"
    elif "syntax" in str(exception).lower() or "语法" in str(exception):
        error_code = "SQL_SYNTAX_ERROR"
    elif "not found" in str(exception).lower() or "未找到" in str(exception):
        error_code = "RESOURCE_NOT_FOUND"
    
    return PipelineError(
        stage=stage,
        code=error_code,
        message=_sanitize_permission_denied_detail(str(exception))
        if error_code == "PERMISSION_DENIED"
        else str(exception)
    )


async def _process_single_subquery(
    sub_query: SubQueryItem,
    context: RequestContext,
    registry: SemanticRegistry
) -> Union[ExecutionResult, PipelineError]:
    """
    处理单个子查询的完整流水线（Stage 2-5）
    
    这是每个子查询的完整处理流程，包括：
    - Stage 2: Plan Generation
    - Stage 3: Validation
    - Stage 4: SQL Generation
    - Stage 5: SQL Execution
    
    Args:
        sub_query: 子查询项
        context: 请求上下文
        registry: 语义注册表实例
    
    Returns:
        Union[ExecutionResult, PipelineError]: 执行结果或错误对象
    """
    try:
        # Stage 2: Plan Generation
        try:
            plan = await stage2_plan_generation.process_subquery(
                sub_query=sub_query,
                context=context,
                registry=registry
            )
        except Exception as e:
            logger.error(
                "Stage 2 failed",
                extra={
                    "sub_query_id": sub_query.id,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            return _map_exception_to_pipeline_error(e, "STAGE_2_PLAN_GENERATION")
        
        # Stage 3: Validation
        try:
            validated_plan = await stage3_validation.validate_and_normalize_plan(
                plan=plan,
                context=context,
                registry=registry
            )
        except Exception as e:
            logger.error(
                "Stage 3 failed",
                extra={
                    "sub_query_id": sub_query.id,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            return _map_exception_to_pipeline_error(e, "STAGE_3_VALIDATION")
        
        # Stage 4: SQL Generation
        try:
            config = get_pipeline_config()
            db_type = config.db_type.value
            
            sql = await stage4_sql_gen.generate_sql(
                plan=validated_plan,
                context=context,
                registry=registry,
                db_type=db_type
            )
        except Exception as e:
            logger.error(
                "Stage 4 failed",
                extra={
                    "sub_query_id": sub_query.id,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            return _map_exception_to_pipeline_error(e, "STAGE_4_SQL_GENERATION")
        
        # Stage 5: SQL Execution
        try:
            result = await stage5_execution.execute_sql(
                sql=sql,
                context=context,
                db_type=db_type
            )
            logger.debug(
                f"子查询完成 | {sub_query.id} | 状态: {result.status.value}",
                extra={
                    "sub_query_id": sub_query.id,
                    "status": result.status.value,
                    "row_count": result.execution_meta.get("row_count", 0)
                }
            )
            return result
        except Exception as e:
            logger.error(
                "Stage 5 failed",
                extra={
                    "sub_query_id": sub_query.id,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            return _map_exception_to_pipeline_error(e, "STAGE_5_SQL_EXECUTION")
    
    except Exception as e:
        # 捕获任何未预期的异常
        logger.error(
            "Unexpected error in sub-query pipeline",
            extra={
                "sub_query_id": sub_query.id,
                "error": str(e),
                "error_type": type(e).__name__
            }
        )
        return _map_exception_to_pipeline_error(e, "UNKNOWN_STAGE")


# ============================================================
# 核心处理函数
# ============================================================
async def run_pipeline(
    query_desc: QueryRequestDescription,
    registry: SemanticRegistry
) -> List[Dict[str, Any]]:
    """
    并发执行多个子查询流水线
    
    为每个子查询创建独立的异步任务，并发执行 Stage 2-5，
    然后聚合结果并格式化为 Stage 6 期望的格式。
    
    Args:
        query_desc: 查询请求描述，包含请求上下文和子查询列表
        registry: 语义注册表实例
    
    Returns:
        List[Dict[str, Any]]: 批量结果列表，每个元素包含：
            - sub_query_id: 子查询ID
            - sub_query_description: 子查询描述
            - payload: ExecutionResult 或 PipelineError 对象
    
    Raises:
        RuntimeError: 当所有子查询都失败时（可选，当前实现不抛出）
    """
    logger.info(
        "Starting pipeline orchestration",
        extra={
            "request_id": query_desc.request_context.request_id,
            "sub_query_count": len(query_desc.sub_queries)
        }
    )
    
    # 创建任务列表
    tasks = []
    for sub_query in query_desc.sub_queries:
        task = _process_single_subquery(
            sub_query=sub_query,
            context=query_desc.request_context,
            registry=registry
        )
        tasks.append(task)
    
    # 并发执行所有任务
    logger.debug(
        f"Executing {len(tasks)} sub-query pipelines concurrently"
    )
    
    try:
        results = await asyncio.gather(*tasks, return_exceptions=False)
    except Exception as e:
        logger.error(
            "asyncio.gather failed",
            extra={"error": str(e), "error_type": type(e).__name__}
        )
        # 如果 gather 本身失败，创建错误结果
        results = []
        for sub_query in query_desc.sub_queries:
            error = PipelineError(
                stage="PIPELINE_ORCHESTRATION",
                code="ORCHESTRATION_ERROR",
                message=f"Failed to execute sub-query pipeline: {str(e)}"
            )
            results.append(error)
    
    # 处理结果并格式化为 Stage 6 期望的格式
    batch_results: List[Dict[str, Any]] = []
    
    for idx, (sub_query, payload) in enumerate(zip(query_desc.sub_queries, results)):
        # payload 可能是 ExecutionResult 或 PipelineError
        # 如果是 PipelineError，需要转换为 ExecutionResult 的错误状态
        if isinstance(payload, PipelineError):
            # 将 PipelineError 转换为 ExecutionResult 的错误状态
            execution_result = ExecutionResult.create_error(
                error=f"[{payload.stage}] {payload.code}: {payload.message}",
                latency_ms=0  # PipelineError 没有执行时间信息
            )
        elif isinstance(payload, ExecutionResult):
            execution_result = payload
        else:
            # 未知类型，创建错误结果
            logger.warning(
                f"Unexpected payload type for sub-query {idx + 1}",
                extra={
                    "sub_query_id": sub_query.id,
                    "payload_type": type(payload).__name__
                }
            )
            execution_result = ExecutionResult.create_error(
                error=f"Unexpected result type: {type(payload).__name__}",
                latency_ms=0
            )
        
        # Stage 6 期望的格式
        batch_result = {
            "sub_query_id": sub_query.id,
            "sub_query_description": sub_query.description,
            "execution_result": execution_result
        }
        batch_results.append(batch_result)
    
    # 统计成功和失败的数量
    success_count = sum(
        1 for r in results
        if isinstance(r, ExecutionResult) and r.status.value == "SUCCESS"
    )
    failed_count = len(results) - success_count
    
    logger.info(
        f"流水线编排完成 | 总数: {len(results)} | 成功: {success_count}, 失败: {failed_count}",
        extra={
            "request_id": query_desc.request_context.request_id,
            "total_count": len(results),
            "success_count": success_count,
            "failed_count": failed_count
        }
    )
    
    return batch_results

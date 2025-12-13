"""
FastAPI Entry Point

NL2SQL 服务的 FastAPI 入口点，连接所有组件形成可运行的 Web 服务。

对应详细设计文档 Section 5 的定义。
"""
import os
from typing import Any, Dict, Optional, Union

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from core.db_connector import close_all
from core.pipeline_orchestrator import run_pipeline
from core.semantic_registry import SemanticRegistry
from schemas.answer import FinalAnswer
from schemas.error import PipelineError
from schemas.plan import QueryPlan
from schemas.request import RequestContext
from stages import stage1_decomposition
from stages import stage2_plan_generation
from stages import stage3_validation
from stages import stage4_sql_gen
from stages import stage6_answer
from utils.log_manager import get_logger, set_request_id

logger = get_logger(__name__)

# ============================================================
# FastAPI 应用实例
# ============================================================
app = FastAPI(
    title="NL2SQL Service",
    description="自然语言转 SQL 查询服务",
    version="1.0.0"
)

# 全局语义注册表实例
registry: Optional[SemanticRegistry] = None


# ============================================================
# 请求/响应模型
# ============================================================
class QueryRequest(BaseModel):
    """
    查询请求模型
    
    用户提交的自然语言查询请求。
    用于 `/nl2sql/execute` 和 `/nl2sql/plan` 端点。
    """
    question: str = Field(
        ...,
        description="用户的自然语言查询问题",
        min_length=1
    )
    
    user_id: str = Field(
        ...,
        description="用户唯一标识符",
        min_length=1
    )
    
    role_id: str = Field(
        ...,
        description="用户角色 ID，用于权限控制",
        min_length=1
    )
    
    tenant_id: str = Field(
        ...,
        description="租户 ID，用于多租户场景的数据隔离"
    )
    
    include_trace: bool = Field(
        default=False,
        description="是否包含调试信息（中间产物）"
    )


class SqlGenRequest(BaseModel):
    """
    SQL 生成请求模型
    
    用于 `/nl2sql/sql` 端点，直接基于已验证的计划生成 SQL。
    """
    plan: QueryPlan = Field(
        ...,
        description="已验证的查询计划对象"
    )
    
    request_context: RequestContext = Field(
        ...,
        description="请求上下文，包含用户信息和 RLS 策略所需的数据"
    )
    
    db_type: Optional[str] = Field(
        default=None,
        description="数据库类型（如 'mysql', 'postgresql'），如果未提供则使用配置中的默认值"
    )


class ErrorResponse(BaseModel):
    """
    错误响应模型
    
    标准化的错误响应结构。
    """
    status: str = Field(
        default="error",
        description="响应状态"
    )
    
    error: Dict[str, Any] = Field(
        ...,
        description="错误详情，包含 stage, code, message"
    )


class DebugInfo(BaseModel):
    """
    调试信息模型
    
    包含流水线执行过程中的中间产物。
    """
    sub_queries: list = Field(
        ...,
        description="子查询列表（Stage 1 输出）"
    )
    
    plans: list = Field(
        ...,
        description="查询计划列表（Stage 2 输出）"
    )
    
    validated_plans: list = Field(
        ...,
        description="验证后的查询计划列表（Stage 3 输出）"
    )
    
    sql_queries: list = Field(
        ...,
        description="SQL 查询列表（Stage 4 输出）"
    )


class DebugResponse(BaseModel):
    """
    调试模式响应模型
    
    包含最终答案和调试信息。
    """
    answer: FinalAnswer = Field(
        ...,
        description="最终答案"
    )
    
    debug_info: DebugInfo = Field(
        ...,
        description="调试信息（中间产物）"
    )


# ============================================================
# 生命周期事件
# ============================================================
@app.on_event("startup")
async def startup_event():
    """
    应用启动事件
    
    初始化语义注册表，加载 YAML 配置。
    """
    global registry
    
    logger.info("Starting NL2SQL Service...")
    
    try:
        # 获取语义注册表单例
        registry = await SemanticRegistry.get_instance()
        
        # 获取 YAML 文件路径（从环境变量或使用默认值）
        yaml_path = os.getenv("SEMANTICS_YAML_PATH", "semantics")
        
        # 初始化并加载 YAML 配置
        await registry.initialize(yaml_path)
        
        logger.info(
            "Semantic registry initialized successfully",
            extra={"yaml_path": yaml_path}
        )
    except Exception as e:
        logger.error(
            "Failed to initialize semantic registry",
            extra={"error": str(e)}
        )
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """
    应用关闭事件
    
    清理资源，关闭数据库连接。
    """
    logger.info("Shutting down NL2SQL Service...")
    
    try:
        # 关闭数据库连接池
        await close_all()
        logger.info("Database connections closed")
    except Exception as e:
        logger.error(
            "Error during shutdown",
            extra={"error": str(e)}
        )


# ============================================================
# 全局异常处理器
# ============================================================
@app.exception_handler(PipelineError)
async def pipeline_error_handler(request: Request, exc: PipelineError):
    """
    处理 PipelineError 异常
    
    将 PipelineError 转换为标准化的错误响应。
    """
    logger.error(
        "Pipeline error occurred",
        extra={
            "stage": exc.stage,
            "code": exc.code,
            "message": exc.message,
            "path": request.url.path
        }
    )
    
    error_response = ErrorResponse(
        status="error",
        error={
            "stage": exc.stage,
            "code": exc.code,
            "message": exc.message
        }
    )
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=error_response.model_dump()
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    处理所有其他异常
    
    捕获未预期的异常并转换为标准化的错误响应。
    """
    logger.error(
        "Unhandled exception occurred",
        extra={
            "error": str(exc),
            "error_type": type(exc).__name__,
            "path": request.url.path
        },
        exc_info=True
    )
    
    # 尝试从异常中提取信息
    error_stage = "UNKNOWN_STAGE"
    error_code = "INTERNAL_ERROR"
    error_message = str(exc)
    
    # 检查是否是已知的异常类型
    if hasattr(exc, "__class__"):
        error_type = exc.__class__.__name__
        if "Stage" in error_type:
            # 尝试从异常类型中提取阶段信息
            if "Stage1" in error_type or "Decomposition" in error_type:
                error_stage = "STAGE_1_DECOMPOSITION"
            elif "Stage2" in error_type or "Plan" in error_type:
                error_stage = "STAGE_2_PLAN_GENERATION"
            elif "Stage3" in error_type or "Validation" in error_type:
                error_stage = "STAGE_3_VALIDATION"
            elif "Stage4" in error_type or "SQL" in error_type:
                error_stage = "STAGE_4_SQL_GENERATION"
            elif "Stage5" in error_type or "Execution" in error_type:
                error_stage = "STAGE_5_SQL_EXECUTION"
            elif "Stage6" in error_type or "Answer" in error_type:
                error_stage = "STAGE_6_ANSWER_GENERATION"
    
    error_response = ErrorResponse(
        status="error",
        error={
            "stage": error_stage,
            "code": error_code,
            "message": error_message
        }
    )
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=error_response.model_dump()
    )


# ============================================================
# API 端点
# ============================================================
@app.get("/")
async def health_check():
    """
    健康检查端点
    
    Returns:
        Dict[str, str]: 健康状态
    """
    return {"status": "ok"}


@app.post("/nl2sql/execute")
async def execute_nl2sql(
    request: QueryRequest
) -> Union[FinalAnswer, DebugResponse]:
    """
    执行 NL2SQL 查询
    
    这是主要的 API 端点，执行完整的 NL2SQL 流水线：
    1. Stage 1: Query Decomposition
    2. Stage 2-5: Pipeline Orchestration (并发执行)
    3. Stage 6: Answer Generation
    
    Args:
        request: 查询请求对象
    
    Returns:
        FinalAnswer: 最终答案对象
    
    Raises:
        HTTPException: 当处理失败时抛出
    """
    # 设置请求 ID（将在 Stage 1 中生成，这里先设置一个临时值）
    temp_request_id = f"temp-{os.getpid()}-{id(request)}"
    set_request_id(temp_request_id)
    
    logger.info(
        "Received NL2SQL request",
        extra={
            "user_id": request.user_id,
            "role_id": request.role_id,
            "tenant_id": request.tenant_id,
            "question_length": len(request.question),
            "include_trace": request.include_trace
        }
    )
    
    try:
        # 确保注册表已初始化
        if registry is None:
            raise RuntimeError("Semantic registry not initialized")
        
        # Stage 1: Query Decomposition
        query_desc = await stage1_decomposition.process_request(
            question=request.question,
            user_id=request.user_id,
            role_id=request.role_id,
            tenant_id=request.tenant_id
        )
        
        # 更新请求 ID（Stage 1 会生成新的 request_id）
        actual_request_id = query_desc.request_context.request_id
        set_request_id(actual_request_id)
        
        logger.info(
            "Stage 1 completed",
            extra={
                "request_id": actual_request_id,
                "sub_query_count": len(query_desc.sub_queries)
            }
        )
        
        # 处理调试模式
        if request.include_trace:
            # 调试模式：需要收集中间产物
            debug_info = await _execute_with_debug(query_desc, registry, request.question)
            
            # 返回调试响应
            debug_response = DebugResponse(
                answer=debug_info["final_answer"],
                debug_info=DebugInfo(
                    sub_queries=[sq.model_dump() for sq in query_desc.sub_queries],
                    plans=debug_info["plans"],
                    validated_plans=debug_info["validated_plans"],
                    sql_queries=debug_info["sql_queries"]
                )
            )
            
            # 返回调试响应
            return debug_response
        
        # 正常模式：执行完整流水线
        # Stage 2-5: Pipeline Orchestration
        batch_results = await run_pipeline(
            query_desc=query_desc,
            registry=registry
        )
        
        logger.info(
            "Pipeline orchestration completed",
            extra={
                "request_id": actual_request_id,
                "batch_count": len(batch_results)
            }
        )
        
        # Stage 6: Answer Generation
        final_answer = await stage6_answer.generate_final_answer(
            batch_results=batch_results,
            original_question=request.question
        )
        
        logger.info(
            "NL2SQL request completed successfully",
            extra={
                "request_id": actual_request_id,
                "status": final_answer.status.value,
                "answer_length": len(final_answer.answer_text)
            }
        )
        
        return final_answer
    
    except Exception as e:
        logger.error(
            "NL2SQL request failed",
            extra={
                "error": str(e),
                "error_type": type(e).__name__
            },
            exc_info=True
        )
        # 异常会被全局异常处理器捕获
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        ) from e


@app.post("/nl2sql/plan")
async def generate_plan(
    request: QueryRequest
) -> QueryPlan:
    """
    生成查询计划（调试端点）
    
    执行 Stage 1-3，返回验证后的查询计划：
    1. Stage 1: Query Decomposition
    2. Stage 2: Plan Generation（仅处理第一个子查询）
    3. Stage 3: Validation
    
    Args:
        request: 查询请求对象
    
    Returns:
        QueryPlan: 验证后的查询计划 JSON
    
    Raises:
        HTTPException: 当处理失败时抛出
    """
    # 设置请求 ID
    temp_request_id = f"temp-{os.getpid()}-{id(request)}"
    set_request_id(temp_request_id)
    
    logger.info(
        "Received plan generation request",
        extra={
            "user_id": request.user_id,
            "role_id": request.role_id,
            "tenant_id": request.tenant_id,
            "question_length": len(request.question)
        }
    )
    
    try:
        # 确保注册表已初始化
        if registry is None:
            raise RuntimeError("Semantic registry not initialized")
        
        # Stage 1: Query Decomposition
        query_desc = await stage1_decomposition.process_request(
            question=request.question,
            user_id=request.user_id,
            role_id=request.role_id,
            tenant_id=request.tenant_id
        )
        
        # 更新请求 ID
        actual_request_id = query_desc.request_context.request_id
        set_request_id(actual_request_id)
        
        logger.info(
            "Stage 1 completed",
            extra={
                "request_id": actual_request_id,
                "sub_query_count": len(query_desc.sub_queries)
            }
        )
        
        # 检查是否有子查询
        if not query_desc.sub_queries:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No sub-queries generated from the question"
            )
        
        # 简化逻辑：只处理第一个子查询
        first_sub_query = query_desc.sub_queries[0]
        
        # Stage 2: Plan Generation
        plan = await stage2_plan_generation.process_subquery(
            sub_query=first_sub_query,
            context=query_desc.request_context,
            registry=registry
        )
        
        logger.info(
            "Stage 2 completed",
            extra={
                "request_id": actual_request_id,
                "intent": plan.intent.value
            }
        )
        
        # Stage 3: Validation
        validated_plan = await stage3_validation.validate_and_normalize_plan(
            plan=plan,
            context=query_desc.request_context,
            registry=registry
        )
        
        logger.info(
            "Plan generation completed successfully",
            extra={
                "request_id": actual_request_id,
                "intent": validated_plan.intent.value,
                "metrics_count": len(validated_plan.metrics),
                "dimensions_count": len(validated_plan.dimensions)
            }
        )
        
        return validated_plan
    
    except Exception as e:
        logger.error(
            "Plan generation failed",
            extra={
                "error": str(e),
                "error_type": type(e).__name__
            },
            exc_info=True
        )
        # 异常会被全局异常处理器捕获
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        ) from e


@app.post("/nl2sql/sql")
async def generate_sql_from_plan(
    request: SqlGenRequest
) -> Dict[str, str]:
    """
    从查询计划生成 SQL（调试端点）
    
    直接基于已验证的计划生成 SQL，不涉及 LLM。
    执行 Stage 4: SQL Generation
    
    Args:
        request: SQL 生成请求对象，包含计划、上下文和数据库类型
    
    Returns:
        Dict[str, str]: 包含生成的 SQL 查询字符串
    
    Raises:
        HTTPException: 当处理失败时抛出
    """
    # 设置请求 ID
    actual_request_id = request.request_context.request_id
    set_request_id(actual_request_id)
    
    logger.info(
        "Received SQL generation request",
        extra={
            "request_id": actual_request_id,
            "intent": request.plan.intent.value,
            "db_type": request.db_type
        }
    )
    
    try:
        # 确保注册表已初始化
        if registry is None:
            raise RuntimeError("Semantic registry not initialized")
        
        # 获取数据库类型
        from config.pipeline_config import get_pipeline_config
        config = get_pipeline_config()
        db_type = request.db_type if request.db_type else config.db_type.value
        
        # Stage 4: SQL Generation（不使用 LLM）
        sql = await stage4_sql_gen.generate_sql(
            plan=request.plan,
            context=request.request_context,
            registry=registry,
            db_type=db_type
        )
        
        logger.info(
            "SQL generation completed successfully",
            extra={
                "request_id": actual_request_id,
                "sql_length": len(sql),
                "db_type": db_type
            }
        )
        
        return {"sql": sql}
    
    except Exception as e:
        logger.error(
            "SQL generation failed",
            extra={
                "error": str(e),
                "error_type": type(e).__name__
            },
            exc_info=True
        )
        # 异常会被全局异常处理器捕获
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        ) from e


async def _execute_with_debug(
    query_desc,
    registry: SemanticRegistry,
    original_question: str
) -> Dict[str, Any]:
    """
    执行带调试信息的流水线
    
    在调试模式下，需要收集每个阶段的中间产物。
    
    Args:
        query_desc: 查询请求描述
        registry: 语义注册表实例
        original_question: 原始问题
    
    Returns:
        Dict[str, Any]: 包含最终答案和调试信息的字典
    """
    from config.pipeline_config import get_pipeline_config
    from stages import stage2_plan_generation
    from stages import stage3_validation
    from stages import stage4_sql_gen
    from stages import stage5_execution
    
    plans = []
    validated_plans = []
    sql_queries = []
    batch_results = []
    
    # 为每个子查询执行 Stage 2-5 并收集中间产物
    for sub_query in query_desc.sub_queries:
        try:
            # Stage 2: Plan Generation
            plan = await stage2_plan_generation.process_subquery(
                sub_query=sub_query,
                context=query_desc.request_context,
                registry=registry
            )
            plans.append(plan.model_dump())
            
            # Stage 3: Validation
            validated_plan = await stage3_validation.validate_and_normalize_plan(
                plan=plan,
                context=query_desc.request_context,
                registry=registry
            )
            validated_plans.append(validated_plan.model_dump())
            
            # Stage 4: SQL Generation
            config = get_pipeline_config()
            db_type = config.db_type.value
            
            sql = await stage4_sql_gen.generate_sql(
                plan=validated_plan,
                context=query_desc.request_context,
                registry=registry,
                db_type=db_type
            )
            sql_queries.append(sql)
            
            # Stage 5: SQL Execution
            result = await stage5_execution.execute_sql(
                sql=sql,
                context=query_desc.request_context,
                db_type=db_type
            )
            
            # 添加到批量结果
            batch_results.append({
                "sub_query_id": sub_query.id,
                "sub_query_description": sub_query.description,
                "execution_result": result
            })
        
        except Exception as e:
            # 如果某个阶段失败，创建错误结果
            logger.error(
                "Sub-query failed in debug mode",
                extra={
                    "sub_query_id": sub_query.id,
                    "error": str(e)
                }
            )
            
            from schemas.result import ExecutionResult
            error_result = ExecutionResult.create_error(
                error=f"Debug mode execution failed: {str(e)}",
                latency_ms=0
            )
            
            batch_results.append({
                "sub_query_id": sub_query.id,
                "sub_query_description": sub_query.description,
                "execution_result": error_result
            })
    
    # Stage 6: Answer Generation
    final_answer = await stage6_answer.generate_final_answer(
        batch_results=batch_results,
        original_question=original_question
    )
    
    return {
        "final_answer": final_answer,
        "plans": plans,
        "validated_plans": validated_plans,
        "sql_queries": sql_queries
    }

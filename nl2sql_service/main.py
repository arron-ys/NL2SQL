"""
FastAPI Entry Point

NL2SQL 服务的 FastAPI 入口点，连接所有组件形成可运行的 Web 服务。

对应详细设计文档 Section 5 的定义。
"""
import asyncio
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union

# ============================================================
# 设置 UTF-8 编码（修复中文乱码问题）
# ============================================================
if sys.platform == "win32":
    # Windows 系统需要设置控制台编码为 UTF-8
    try:
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8')
        if hasattr(sys.stderr, 'reconfigure'):
            sys.stderr.reconfigure(encoding='utf-8')
        os.environ['PYTHONIOENCODING'] = 'utf-8'
    except Exception:
        pass

# 在导入其他模块之前，先加载 .env 文件
# 这样 os.getenv() 才能读取到 .env 文件中的环境变量
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from core.db_connector import close_all
from core.pipeline_orchestrator import run_pipeline
from core.semantic_registry import SemanticRegistry
from core.errors import AppError, sanitize_details
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
from utils.log_manager import get_request_id
from core.semantic_registry import SecurityConfigError, SecurityPolicyNotFound
from core.ai_client import AIProviderInitError

logger = get_logger(__name__)

# 全局语义注册表实例
registry: Optional[SemanticRegistry] = None

# 健康检查后台任务
healthcheck_task: Optional[asyncio.Task] = None


# ============================================================
# 健康检查后台任务（长期：连接健康检查 + 自愈）
# ============================================================
async def healthcheck_loop():
    """
    健康检查后台任务
    
    每 HEALTH_INTERVAL_SEC（默认 120s）执行一次健康检查：
    - 对所有 provider 做连通性探测
    - 若探测失败，provider 会自动 reset_client()
    - 记录健康状态到日志
    """
    from core.ai_client import get_ai_client
    
    # 从环境变量读取配置
    interval_sec = float(os.getenv("HEALTH_INTERVAL_SEC", "120"))
    
    logger.info(
        "Healthcheck loop started",
        extra={"interval_sec": interval_sec}
    )
    
    try:
        while True:
            await asyncio.sleep(interval_sec)
            
            try:
                ai_client = get_ai_client()
                results = await ai_client.healthcheck_all()
                
                # 检查是否有失败的 provider
                failed_providers = [name for name, ok in results.items() if not ok]
                
                if failed_providers:
                    logger.warning(
                        "Healthcheck detected unhealthy providers",
                        extra={
                            "failed_providers": failed_providers,
                            "results": results,
                            "metrics": ai_client.get_metrics(),
                        }
                    )
                else:
                    logger.debug(
                        "Healthcheck passed for all providers",
                        extra={"results": results}
                    )
            
            except Exception as e:
                logger.error(
                    "Healthcheck loop error",
                    extra={"error": str(e)}
                )
    
    except asyncio.CancelledError:
        logger.info("Healthcheck loop cancelled")
        raise


# ============================================================
# 生命周期管理
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    应用生命周期管理
    
    使用 lifespan context manager 替代已废弃的 @app.on_event。
    在 yield 之前执行启动逻辑，在 yield 之后执行关闭逻辑。
    """
    global registry, healthcheck_task
    
    # ========== 启动逻辑 ==========
    logger.info("NL2SQL 服务启动中...")
    
    try:
        # 【双重保险】显式初始化 AIClient，确保在服务启动的第一时间暴露配置错误（Fail Fast）
        # 这可以确保：
        # 1. 环境变量已正确加载
        # 2. 所有必需的 provider 都已初始化
        # 3. 如果配置有问题，在启动阶段就能发现，而不是等到第一个请求
        from core.ai_client import get_ai_client
        ai_client = get_ai_client()
        
        # 获取语义注册表单例
        registry = await SemanticRegistry.get_instance()
        
        # 获取 YAML 文件路径（从环境变量或使用默认值）
        env_yaml_path = os.getenv("SEMANTICS_YAML_PATH")
        if not env_yaml_path:
            # 默认路径：nl2sql_service/semantics（相对于 main.py 所在目录）
            yaml_path = str(Path(__file__).parent / "semantics")
        else:
            # 如果是相对路径，转换为相对于 main.py 所在目录的绝对路径
            yaml_path_obj = Path(env_yaml_path)
            if not yaml_path_obj.is_absolute():
                yaml_path = str(Path(__file__).parent / env_yaml_path)
            else:
                yaml_path = env_yaml_path
        
        # 初始化并加载 YAML 配置
        await registry.initialize(yaml_path)
        
        # 启动健康检查后台任务（长期：连接健康检查 + 自愈）
        healthcheck_task = asyncio.create_task(healthcheck_loop())

        # 服务启动完成提示（便于前端/运维快速定位启动状态）
        logger.info("✓ NL2SQL 服务已启动，等待请求")
    except Exception as e:
        logger.error(
            "Failed to initialize semantic registry",
            extra={"error": str(e)}
        )
        raise
    
    # ========== 运行阶段 ==========
    try:
        yield
    finally:
        # ========== 关闭逻辑 ==========
        logger.info("Shutting down NL2SQL Service...")
        
        try:
            # 取消健康检查后台任务
            if healthcheck_task and not healthcheck_task.done():
                healthcheck_task.cancel()
                try:
                    await healthcheck_task
                except asyncio.CancelledError:
                    pass
                logger.info("Healthcheck background task stopped")
            
            # 关闭 AI 客户端连接（Option B：资源管理）
            # ⚠️ 直接导入变量，避免在关闭流程中触发延迟初始化
            from core.ai_client import _ai_client
            if _ai_client is not None:
                await _ai_client.close()
                logger.info("AI client connections closed")
            else:
                logger.debug("AI client not initialized, skip close")
            
            # 关闭数据库连接池
            await close_all()
            logger.info("Database connections closed")
        except Exception as e:
            logger.error(
                "Error during shutdown",
                extra={"error": str(e)}
            )


# ============================================================
# FastAPI 应用实例
# ============================================================
app = FastAPI(
    title="NL2SQL Service",
    description="自然语言转 SQL 查询服务",
    version="1.0.0",
    lifespan=lifespan
)


# ============================================================
# 健康检查和监控端点（中期：监控&告警）
# ============================================================
@app.get("/health", tags=["Health"])
async def health_check():
    """
    健康检查端点
    
    返回服务健康状态和 provider 连通性。
    """
    from core.ai_client import get_ai_client
    
    try:
        ai_client = get_ai_client()
        provider_health = await ai_client.healthcheck_all()
        
        # 判断整体健康状态
        all_healthy = all(provider_health.values())
        
        return {
            "status": "healthy" if all_healthy else "degraded",
            "providers": provider_health,
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }
        )


@app.get("/metrics", tags=["Health"])
async def get_metrics():
    """
    获取 provider 统计指标
    
    返回所有 provider 的请求统计、错误率、健康检查状态等。
    """
    from core.ai_client import get_ai_client
    
    try:
        ai_client = get_ai_client()
        metrics = ai_client.get_metrics()
        
        return {
            "metrics": metrics,
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        logger.error(f"Metrics retrieval error: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }
        )


# ============================================================
# Middleware: Request ID 注入
# ============================================================
@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """
    请求 ID 中间件
    
    从请求 header 中读取或生成 request_id，并注入到日志上下文中。
    支持从上游透传 Trace-ID。
    """
    # 从 header 读取 ID：Trace-ID
    request_id = request.headers.get("Trace-ID")
    
    # 如果不存在，则生成新的 ID（沿用项目原有格式：req-YYYYMMDDHHMMSS-xxxxxxxx）
    if not request_id:
        request_id = f"req-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
    
    # 调用 log_manager.set_request_id() 写入 contextvar
    set_request_id(request_id)
    
    # 处理请求
    response = await call_next(request)
    
    # 在响应 header 中写回 Trace-ID
    response.headers["Trace-ID"] = request_id
    
    return response


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
        strict=True,  # 严格模式：不允许字符串自动转换为布尔值
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
    纯嵌套结构：只有 answer 和 debug_info 两个字段。
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
# 全局异常处理器
# ============================================================
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    处理请求验证错误（422）
    
    FastAPI 默认会处理 Pydantic 验证错误，但我们需要确保返回正确的状态码。
    """
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"detail": exc.errors()}
    )


@app.exception_handler(SecurityPolicyNotFound)
async def security_policy_not_found_handler(request: Request, exc: SecurityPolicyNotFound):
    """
    RBAC fail-closed：role 未配置 policy => 403
    """
    logger.warning(
        "Security policy not found (RBAC fail-closed)",
        extra={
            "error_stage": "SECURITY",
            "path": request.url.path,
            "role_id": getattr(exc, "role_id", None),
            "error_type": type(exc).__name__,
        },
    )
    error_response = ErrorResponse(
        status="error",
        error={
            "stage": "SECURITY",
            "code": "SECURITY_POLICY_NOT_FOUND",
            "message": str(exc),
        },
    )
    return JSONResponse(
        status_code=status.HTTP_403_FORBIDDEN,
        content=error_response.model_dump(),
    )


@app.exception_handler(SecurityConfigError)
async def security_config_error_handler(request: Request, exc: SecurityConfigError):
    """
    RBAC 配置加载/解析失败 => 500 配置错误（不要伪装成 403）
    """
    logger.opt(exception=exc).error(
        "Security config error",
        extra={
            "error_stage": "SECURITY",
            "path": request.url.path,
            "error_type": type(exc).__name__,
        },
    )
    error_response = ErrorResponse(
        status="error",
        error={
            "stage": "SECURITY",
            "code": "SECURITY_CONFIG_ERROR",
            "message": str(exc),
        },
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=error_response.model_dump(),
    )


@app.exception_handler(AIProviderInitError)
async def ai_provider_init_error_handler(request: Request, exc: AIProviderInitError):
    """
    LLM Provider 初始化失败（通常是代理/网络/配置问题）=> 503（服务暂不可用）
    """
    logger.opt(exception=exc).error(
        "LLM provider initialization failed",
        extra={
            "error_stage": "LLM",
            "path": request.url.path,
            "provider": getattr(exc, "provider_name", None),
            "error_type": type(exc).__name__,
        },
    )
    error_response = ErrorResponse(
        status="error",
        error={
            "stage": "LLM",
            "code": "LLM_PROVIDER_INIT_FAILED",
            "message": str(exc),
        },
    )
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content=error_response.model_dump(),
    )


@app.exception_handler(AppError)
async def app_error_handler(request: Request, exc: AppError):
    """
    统一 AppError 响应结构（不改变 status_code 语义，只增强 body）。
    """
    rid = get_request_id()
    error_obj = {
        "code": exc.code,
        "message": exc.message,
    }
    safe_details = sanitize_details(getattr(exc, "details", None))
    if safe_details:
        error_obj["details"] = safe_details
    return JSONResponse(
        status_code=getattr(exc, "status_code", status.HTTP_500_INTERNAL_SERVER_ERROR),
        content={
            "request_id": rid,
            "error_stage": getattr(exc, "error_stage", "UNKNOWN"),
            "error": error_obj,
        },
    )


@app.exception_handler(stage3_validation.PermissionDeniedError)
async def permission_denied_error_handler(
    request: Request, exc: stage3_validation.PermissionDeniedError
):
    """
    软错误：权限拒绝（按设计文档：HTTP 200）

    安全要求：对外响应必须脱敏，不返回具体 METRIC_* ID。
    详细信息仅写入服务端日志，便于排查 Stage2 的 Permission Shadow Check。
    """
    rid = get_request_id()

    # 服务端日志：记录完整 detail（可能包含被拦截指标的名称/域信息）
    logger.warning(
        "Permission denied (RBAC blocked query)",
        extra={
            "request_id": rid,
            "path": request.url.path,
            "error_stage": "STAGE_3_VALIDATION",
            "error_type": type(exc).__name__,
            "detail": str(exc),
        },
    )

    # 对外脱敏文案（不包含具体指标 ID）
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "request_id": rid,
            "status": "ERROR",
            "error": {
                "code": "PERMISSION_DENIED",
                "message": "您当前的角色没有权限访问查询中涉及的业务域数据（如销售域）。",
                "stage": "STAGE_3_VALIDATION",
            },
        },
    )


@app.exception_handler(stage3_validation.MissingMetricError)
async def missing_metric_error_handler(
    request: Request, exc: stage3_validation.MissingMetricError
):
    """
    软错误：缺少指标（按设计文档：HTTP 200）

    目标：/nl2sql/plan 不应因业务输入不满足而返回 5xx。
    """
    rid = get_request_id()

    logger.info(
        "Need clarification: missing metric in plan",
        extra={
            "request_id": rid,
            "path": request.url.path,
            "error_stage": "STAGE_3_VALIDATION",
            "error_type": type(exc).__name__,
            "detail": str(exc),
        },
    )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "request_id": rid,
            "status": "ERROR",
            "error": {
                "code": "NEED_CLARIFICATION",
                "message": "当前问题还不够明确：请说明您想看的具体指标或口径（例如 GMV、订单数、销售额等），以及需要的时间范围。",
                "stage": "STAGE_3_VALIDATION",
            },
        },
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    处理所有其他异常
    
    捕获未预期的异常并转换为标准化的错误响应。
    """
    logger.opt(exception=exc).error(
        "Unhandled exception occurred",
        extra={
            "error": str(exc),
            "error_type": type(exc).__name__,
            "path": request.url.path,
        },
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
async def root():
    """
    根端点
    
    Returns:
        Dict[str, str]: 服务信息
    """
    return {"status": "ok", "service": "NL2SQL Service"}


@app.get("/health")
async def health_check():
    """
    健康检查端点
    
    Returns:
        Dict[str, str]: 健康状态
    """
    return {"status": "ok"}


@app.post("/nl2sql/execute", response_model=Union[FinalAnswer, DebugResponse])
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

    print("🔥 I AM HERE! I RECEIVED THE REQUEST! 🔥")  # 用 print，别用 logger，防止 logger 配置问题

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
        
        # 获取当前请求 ID（由 middleware 或 Stage 1 设置）
        actual_request_id = query_desc.request_context.request_id
        
        # 处理调试模式
        if request.include_trace:
            # 调试模式：需要收集中间产物
            debug_info = await _execute_with_debug(query_desc, registry, request.question)
            
            # 返回调试响应（纯嵌套结构）
            return DebugResponse(
                answer=debug_info["final_answer"],
                debug_info=DebugInfo(
                    sub_queries=[sq.model_dump() for sq in query_desc.sub_queries],
                    plans=debug_info["plans"],
                    validated_plans=debug_info["validated_plans"],
                    sql_queries=debug_info["sql_queries"]
                )
            )
        
        # 正常模式：执行完整流水线
        # Stage 2-5: Pipeline Orchestration
        batch_results = await run_pipeline(
            query_desc=query_desc,
            registry=registry
        )
        
        # Stage 6: Answer Generation
        final_answer = await stage6_answer.generate_final_answer(
            batch_results=batch_results,
            original_question=request.question
        )
        
        logger.info(
            f"✓ 请求完成 | 状态: {final_answer.status.value} | 子查询: {len(batch_results)} | 答案长度: {len(final_answer.answer_text)}",
            extra={
                "request_id": actual_request_id,
                "status": final_answer.status.value,
                "batch_count": len(batch_results),
                "answer_length": len(final_answer.answer_text)
            }
        )
        
        return final_answer
    
    except AppError:
        # AppError 必须自然传播，交给 app_error_handler 输出统一结构
        raise
    except (stage3_validation.PermissionDeniedError, stage3_validation.MissingMetricError) as e:
        # 业务软错误：无堆栈，避免误报系统崩溃；交给全局 handler 返回 HTTP 200
        rid = get_request_id()
        logger.warning(
            "Request ended with business exception: {}",
            str(e),
            extra={
                "request_id": rid,
                "path": "/nl2sql/execute",
                "error_type": type(e).__name__,
            },
        )
        raise
    except Exception as e:
        logger.opt(exception=e).error(
            "NL2SQL request failed",
            extra={
                "error": str(e),
                "error_type": type(e).__name__,
            },
        )
        # 让特定异常自然传播，由全局异常处理器捕获（避免破坏错误结构/状态码）
        if isinstance(e, (SecurityPolicyNotFound, SecurityConfigError, AIProviderInitError, stage3_validation.PermissionDeniedError)):
            raise
        # 未知异常：包装成 AppError（不改变 status code=500），走统一结构
        raise AppError(
            code="INTERNAL_ERROR",
            message="Internal server error",
            error_stage="UNKNOWN",
            details={
                "error_type": type(e).__name__,
                "error_summary": str(e),
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
    api_start = time.perf_counter()
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
        stage1_start = time.perf_counter()
        query_desc = await stage1_decomposition.process_request(
            question=request.question,
            user_id=request.user_id,
            role_id=request.role_id,
            tenant_id=request.tenant_id
        )
        stage1_ms = int((time.perf_counter() - stage1_start) * 1000)
        
        # 获取当前请求 ID（由 middleware 或 Stage 1 设置）
        actual_request_id = query_desc.request_context.request_id
        
        logger.info(
            "Stage 1 completed",
            extra={
                "request_id": actual_request_id,
                "sub_query_count": len(query_desc.sub_queries),
                "stage1_ms": stage1_ms,
            }
        )
        # DEBUG：子查询明细（长文本/列表禁止在 INFO）
        logger.debug(
            "Stage 1 sub-queries (details)",
            extra={
                "request_id": actual_request_id,
                "sub_queries": [{"id": sq.id, "description": sq.description} for sq in query_desc.sub_queries],
            },
        )
        
        # 检查是否有子查询
        if not query_desc.sub_queries:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No sub-queries generated from the question"
            )
        
        # 简化逻辑：只处理第一个子查询
        first_sub_query = query_desc.sub_queries[0]
        
        logger.debug(
            "Processing first sub-query for plan generation",
            extra={
                "request_id": actual_request_id,
                "sub_query_id": first_sub_query.id,
                "sub_query_description": first_sub_query.description
            }
        )
        
        # Stage 2: Plan Generation
        stage2_start = time.perf_counter()
        plan = await stage2_plan_generation.process_subquery(
            sub_query=first_sub_query,
            context=query_desc.request_context,
            registry=registry
        )
        stage2_ms = int((time.perf_counter() - stage2_start) * 1000)
        
        logger.info(
            "Stage 2 completed",
            extra={
                "request_id": actual_request_id,
                "intent": plan.intent.value,
                "stage2_ms": stage2_ms,
            }
        )
        
        # Stage 3: Validation
        stage3_start = time.perf_counter()
        validated_plan = await stage3_validation.validate_and_normalize_plan(
            plan=plan,
            context=query_desc.request_context,
            registry=registry
        )
        stage3_ms = int((time.perf_counter() - stage3_start) * 1000)
        
        logger.info(
            "Plan generation completed successfully",
            extra={
                "request_id": actual_request_id,
                "intent": validated_plan.intent.value,
                "metrics_count": len(validated_plan.metrics),
                "dimensions_count": len(validated_plan.dimensions),
                "filters_count": len(validated_plan.filters),
                "stage3_ms": stage3_ms,
                "total_ms": int((time.perf_counter() - api_start) * 1000),
            }
        )
        # DEBUG：完整最终计划（INFO 严禁完整 JSON）
        logger.debug(
            "Plan generation completed (final_plan details)",
            extra={
                "request_id": actual_request_id,
                "final_plan": {
                    "intent": validated_plan.intent.value,
                    "metrics": [{"id": m.id, "compare_mode": m.compare_mode.value if m.compare_mode else None} for m in validated_plan.metrics],
                    "dimensions": [{"id": d.id, "time_grain": d.time_grain.value if d.time_grain else None} for d in validated_plan.dimensions],
                    "filters": [{"id": f.id, "op": f.op.value, "values": f.values} for f in validated_plan.filters],
                    "time_range": validated_plan.time_range.model_dump() if validated_plan.time_range else None,
                    "order_by": [{"id": o.id, "direction": o.direction.value} for o in validated_plan.order_by] if validated_plan.order_by else [],
                    "limit": validated_plan.limit,
                    "warnings": validated_plan.warnings if hasattr(validated_plan, "warnings") and validated_plan.warnings else [],
                },
            },
        )
        
        return validated_plan
    
    except RequestValidationError:
        # 让 RequestValidationError 自然传播，由异常处理器处理
        raise
    except HTTPException:
        # 让 HTTPException 自然传播，由 FastAPI 的异常处理器处理
        raise
    except AppError:
        # AppError 必须自然传播，交给 app_error_handler 输出统一结构
        raise
    except (stage3_validation.PermissionDeniedError, stage3_validation.MissingMetricError) as e:
        # 业务软错误：无堆栈，避免误报系统崩溃；交给全局 handler 返回 HTTP 200
        rid = get_request_id()
        logger.warning(
            "Request ended with business exception: {}",
            str(e),
            extra={
                "request_id": rid,
                "path": "/nl2sql/plan",
                "error_type": type(e).__name__,
            },
        )
        raise
    except Exception as e:
        logger.opt(exception=e).error(
            "Plan generation failed",
            extra={
                "error": str(e),
                "error_type": type(e).__name__,
            },
        )
        if isinstance(e, (SecurityPolicyNotFound, SecurityConfigError, AIProviderInitError, stage3_validation.PermissionDeniedError)):
            raise
        # 未知异常：包装成 AppError（不改变 status code=500），走统一结构
        raise AppError(
            code="INTERNAL_ERROR",
            message="Internal server error",
            error_stage="UNKNOWN",
            details={
                "error_type": type(e).__name__,
                "error_summary": str(e),
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
    # 获取请求 ID（由 middleware 设置，或从 request_context 获取）
    actual_request_id = request.request_context.request_id
    
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
    
    except AppError:
        # AppError 必须自然传播，交给 app_error_handler 输出统一结构
        raise
    except Exception as e:
        logger.opt(exception=e).error(
            "SQL generation failed",
            extra={
                "error": str(e),
                "error_type": type(e).__name__,
            },
        )
        if isinstance(e, (SecurityPolicyNotFound, SecurityConfigError, AIProviderInitError)):
            raise
        # 未知异常：包装成 AppError（不改变 status code=500），走统一结构
        raise AppError(
            code="INTERNAL_ERROR",
            message="Internal server error",
            error_stage="STAGE_4_SQL_GENERATION",
            details={
                "error_type": type(e).__name__,
                "error_summary": str(e),
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
    
    request_id = query_desc.request_context.request_id
    
    logger.info(
        "Starting debug mode execution",
        extra={
            "request_id": request_id,
            "sub_query_count": len(query_desc.sub_queries),
        }
    )
    
    # 为每个子查询执行 Stage 2-5 并收集中间产物
    for sub_query in query_desc.sub_queries:
        subquery_start = time.perf_counter()
        try:
            logger.debug(
                "Processing sub-query in debug mode",
                extra={
                    "request_id": request_id,
                    "sub_query_id": sub_query.id,
                    "sub_query_description": sub_query.description
                }
            )
            
            # Stage 2: Plan Generation
            stage2_start = time.perf_counter()
            logger.info(
                "Stage 2 started (debug mode)",
                extra={
                    "request_id": request_id,
                    "sub_query_id": sub_query.id,
                }
            )
            
            plan = await stage2_plan_generation.process_subquery(
                sub_query=sub_query,
                context=query_desc.request_context,
                registry=registry
            )
            stage2_ms = int((time.perf_counter() - stage2_start) * 1000)
            plans.append(plan.model_dump())
            
            logger.info(
                "Stage 2 completed (debug mode)",
                extra={
                    "request_id": request_id,
                    "sub_query_id": sub_query.id,
                    "intent": plan.intent.value,
                    "stage2_ms": stage2_ms,
                }
            )
            logger.debug(
                "Stage 2 plan details (debug mode)",
                extra={
                    "request_id": request_id,
                    "sub_query_id": sub_query.id,
                    "plan": {
                        "intent": plan.intent.value,
                        "metrics": [{"id": m.id, "compare_mode": m.compare_mode.value if m.compare_mode else None} for m in plan.metrics],
                        "dimensions": [{"id": d.id, "time_grain": d.time_grain.value if d.time_grain else None} for d in plan.dimensions],
                        "filters": [{"id": f.id, "op": f.op.value, "values": f.values} for f in plan.filters],
                    },
                },
            )
            
            # Stage 3: Validation
            stage3_start = time.perf_counter()
            logger.info(
                "Stage 3 started (debug mode)",
                extra={
                    "request_id": request_id,
                    "sub_query_id": sub_query.id,
                }
            )
            
            validated_plan = await stage3_validation.validate_and_normalize_plan(
                plan=plan,
                context=query_desc.request_context,
                registry=registry
            )
            stage3_ms = int((time.perf_counter() - stage3_start) * 1000)
            validated_plans.append(validated_plan.model_dump())
            
            logger.info(
                "Stage 3 completed (debug mode)",
                extra={
                    "request_id": request_id,
                    "sub_query_id": sub_query.id,
                    "intent": validated_plan.intent.value,
                    "metrics_count": len(validated_plan.metrics),
                    "dimensions_count": len(validated_plan.dimensions),
                    "filters_count": len(validated_plan.filters),
                    "stage3_ms": stage3_ms,
                }
            )
            logger.debug(
                "Stage 3 validated plan details (debug mode)",
                extra={
                    "request_id": request_id,
                    "sub_query_id": sub_query.id,
                    "validated_plan": {
                        "intent": validated_plan.intent.value,
                        "metrics": [{"id": m.id, "compare_mode": m.compare_mode.value if m.compare_mode else None} for m in validated_plan.metrics],
                        "dimensions": [{"id": d.id, "time_grain": d.time_grain.value if d.time_grain else None} for d in validated_plan.dimensions],
                        "filters": [{"id": f.id, "op": f.op.value, "values": f.values} for f in validated_plan.filters],
                        "time_range": validated_plan.time_range.model_dump() if validated_plan.time_range else None,
                        "warnings": validated_plan.warnings if hasattr(validated_plan, "warnings") and validated_plan.warnings else [],
                    },
                },
            )
            
            # Stage 4: SQL Generation
            stage4_start = time.perf_counter()
            logger.info(
                "Stage 4 started (debug mode)",
                extra={
                    "request_id": request_id,
                    "sub_query_id": sub_query.id,
                }
            )
            
            config = get_pipeline_config()
            db_type = config.db_type.value
            
            sql = await stage4_sql_gen.generate_sql(
                plan=validated_plan,
                context=query_desc.request_context,
                registry=registry,
                db_type=db_type
            )
            stage4_ms = int((time.perf_counter() - stage4_start) * 1000)
            sql_queries.append(sql)
            
            logger.info(
                "Stage 4 completed (debug mode)",
                extra={
                    "request_id": request_id,
                    "sub_query_id": sub_query.id,
                    "sql_length": len(sql),
                    "stage4_ms": stage4_ms,
                }
            )
            logger.debug(
                "Stage 4 SQL details (debug mode)",
                extra={
                    "request_id": request_id,
                    "sub_query_id": sub_query.id,
                    "sql": sql,
                },
            )
            
            # Stage 5: SQL Execution
            stage5_start = time.perf_counter()
            logger.info(
                "Stage 5 started (debug mode)",
                extra={
                    "request_id": request_id,
                    "sub_query_id": sub_query.id,
                }
            )
            
            result = await stage5_execution.execute_sql(
                sql=sql,
                context=query_desc.request_context,
                db_type=db_type
            )
            stage5_ms = int((time.perf_counter() - stage5_start) * 1000)
            
            logger.info(
                "Stage 5 completed (debug mode)",
                extra={
                    "request_id": request_id,
                    "sub_query_id": sub_query.id,
                    "status": result.status.value,
                    "row_count": len(result.data) if result.data else 0,
                    "stage5_ms": stage5_ms,
                }
            )
            
            # 添加到批量结果
            batch_results.append({
                "sub_query_id": sub_query.id,
                "sub_query_description": sub_query.description,
                "execution_result": result
            })
            
            subquery_ms = int((time.perf_counter() - subquery_start) * 1000)
            logger.info(
                "Sub-query completed successfully (debug mode)",
                extra={
                    "request_id": request_id,
                    "sub_query_id": sub_query.id,
                    "total_ms": subquery_ms,
                }
            )
        
        except Exception as e:
            # 如果某个阶段失败，创建错误结果
            subquery_ms = int((time.perf_counter() - subquery_start) * 1000)
            logger.opt(exception=e).error(
                "Sub-query failed in debug mode",
                extra={
                    "request_id": request_id,
                    "sub_query_id": sub_query.id,
                    "sub_query_description": sub_query.description,
                    "error_type": type(e).__name__,
                    "error": str(e),
                    "total_ms": subquery_ms,
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
    stage6_start = time.perf_counter()
    logger.info(
        "Stage 6 started (debug mode)",
        extra={
            "request_id": request_id,
        }
    )
    
    final_answer = await stage6_answer.generate_final_answer(
        batch_results=batch_results,
        original_question=original_question
    )
    
    stage6_ms = int((time.perf_counter() - stage6_start) * 1000)
    logger.info(
        "Stage 6 completed (debug mode)",
        extra={
            "request_id": request_id,
            "status": final_answer.status.value,
            "answer_length": len(final_answer.answer_text),
            "stage6_ms": stage6_ms,
        }
    )
    
    return {
        "final_answer": final_answer,
        "plans": plans,
        "validated_plans": validated_plans,
        "sql_queries": sql_queries
    }

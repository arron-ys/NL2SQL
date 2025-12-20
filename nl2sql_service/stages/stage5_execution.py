"""
Stage 5: SQL Execution (SQL 执行)

在受控环境中安全执行生成的 SQL 查询，并返回结构化数据。
对应详细设计文档 3.5 的定义。
"""
import base64
import os
import time
from datetime import datetime, date
from decimal import Decimal
from typing import Any, List, Dict, Optional
import json

from sqlalchemy import text
from sqlalchemy.exc import TimeoutError as SQLTimeoutError, ProgrammingError

from config.pipeline_config import get_pipeline_config
from core.db_connector import get_engine
from core.dialect_adapter import DialectAdapter
from schemas.request import RequestContext
from schemas.result import ExecutionResult, ExecutionStatus
from utils.log_manager import get_logger
from utils.log_preview_helper import preview_text, preview_json

logger = get_logger(__name__)


# ============================================================
# 异常定义
# ============================================================
class Stage5Error(Exception):
    """
    Stage 5 处理异常
    
    用于表示 SQL 执行阶段的错误，包括：
    - 数据库连接失败
    - SQL 执行超时
    - SQL 语法错误
    - 数据获取失败
    """
    pass


# ============================================================
# 辅助函数
# ============================================================
async def _execute_diagnostic_queries(
    conn: Any,
    context: RequestContext,
    sub_query_id: Optional[str],
    diag_ctx: dict,
    db_type: str
) -> None:
    """
    执行诊断 SQL 查询，用于定位 SUM 返回 NULL 的根因
    
    Args:
        conn: 数据库连接对象
        context: 请求上下文
        sub_query_id: 子查询 ID
        diag_ctx: 诊断上下文（包含 view_name, time_field, time_start, time_end, tenant_field, tenant_id）
        db_type: 数据库类型
    """
    view_name = diag_ctx.get("view_name")
    if not view_name:
        logger.warning("诊断上下文缺少 view_name，跳过诊断")
        return
    
    time_field = diag_ctx.get("time_field")
    time_start = diag_ctx.get("time_start")
    time_end = diag_ctx.get("time_end")
    tenant_field = diag_ctx.get("tenant_field", "tenant_id")
    tenant_id = diag_ctx.get("tenant_id")
    
    # 根据数据库类型选择引号
    quote_char = "`" if db_type.lower() == "mysql" else '"'
    
    # 构建诊断查询列表
    diagnostic_queries = []
    
    # 1. 总行数
    diagnostic_queries.append({
        "label": "总行数",
        "sql": f"SELECT COUNT(*) as cnt FROM {quote_char}{view_name}{quote_char}"
    })
    
    # 2. 时间范围（如果有时间字段）
    if time_field:
        diagnostic_queries.append({
            "label": "时间范围",
            "sql": f"SELECT MIN({quote_char}{time_field}{quote_char}) as min_date, MAX({quote_char}{time_field}{quote_char}) as max_date FROM {quote_char}{view_name}{quote_char}"
        })
    
    # 3. 时间过滤行数（如果有时间范围）
    if time_field and time_start and time_end:
        diagnostic_queries.append({
            "label": "时间过滤行数",
            "sql": f"SELECT COUNT(*) as cnt FROM {quote_char}{view_name}{quote_char} WHERE {quote_char}{time_field}{quote_char}>='{time_start}' AND {quote_char}{time_field}{quote_char}<='{time_end}'"
        })
    
    # 4. 租户过滤行数（如果有租户 ID）
    if tenant_id:
        diagnostic_queries.append({
            "label": "租户过滤行数",
            "sql": f"SELECT COUNT(*) as cnt FROM {quote_char}{view_name}{quote_char} WHERE {quote_char}{tenant_field}{quote_char}='{tenant_id}'"
        })
        
        # 5. 租户 NULL 行数
        diagnostic_queries.append({
            "label": "租户NULL行数",
            "sql": f"SELECT COUNT(*) as cnt FROM {quote_char}{view_name}{quote_char} WHERE {quote_char}{tenant_field}{quote_char} IS NULL"
        })
    
    # 6. 时间+租户组合过滤行数（如果两者都有）
    if time_field and time_start and time_end and tenant_id:
        diagnostic_queries.append({
            "label": "时间+租户过滤行数",
            "sql": f"SELECT COUNT(*) as cnt FROM {quote_char}{view_name}{quote_char} WHERE {quote_char}{time_field}{quote_char}>='{time_start}' AND {quote_char}{time_field}{quote_char}<='{time_end}' AND {quote_char}{tenant_field}{quote_char}='{tenant_id}'"
        })
    
    # 7. 租户列检查（检查列是否存在）
    if tenant_id:
        diagnostic_queries.append({
            "label": "租户列检查",
            "sql": f"SELECT {quote_char}{tenant_field}{quote_char} FROM {quote_char}{view_name}{quote_char} LIMIT 1"
        })
    
    # 执行所有诊断查询
    for diag_query in diagnostic_queries:
        try:
            diag_result = await conn.execute(text(diag_query["sql"]))
            diag_rows = diag_result.fetchall()
            
            # 转换为字典列表（兼容不同数据库返回格式）
            diag_data = []
            for row in diag_rows:
                if hasattr(row, '_mapping'):
                    diag_data.append(dict(row._mapping))
                elif hasattr(row, '_asdict'):
                    diag_data.append(row._asdict())
                else:
                    # 降级：尝试转换为列表
                    diag_data.append(list(row))
            
            # 使用 preview_json 格式化输出（max_lines=40, max_chars=1200）
            diag_preview = preview_json(diag_data, max_lines=40, max_chars=1200, label=diag_query["label"])
            
            logger.info(
                f"【诊断SQL】{diag_query['label']}",
                extra={
                    "request_id": context.request_id,
                    "sub_query_id": sub_query_id,
                    "diagnostic_label": diag_query["label"],
                    "diagnostic_sql": diag_query["sql"],
                    "diagnostic_result": diag_preview
                }
            )
        except Exception as e:
            logger.warning(
                f"诊断SQL执行失败: {diag_query['label']}",
                extra={
                    "request_id": context.request_id,
                    "sub_query_id": sub_query_id,
                    "diagnostic_label": diag_query["label"],
                    "diagnostic_sql": diag_query["sql"],
                    "error": str(e)
                }
            )


def _sanitize_row(row: Any) -> List[Any]:
    """
    清理和规范化数据库行数据
    
    将数据库特定的类型（如 Decimal, datetime）转换为 JSON 兼容的类型。
    
    Args:
        row: 数据库行数据（可以是 Row 对象、tuple 或 dict）
    
    Returns:
        List[Any]: 清理后的行数据列表
    """
    sanitized = []
    
    # 处理不同类型的行对象
    if hasattr(row, '_mapping'):
        # SQLAlchemy Row 对象（2.0+）
        row_dict = dict(row._mapping)
        for value in row_dict.values():
            sanitized.append(_sanitize_value(value))
    elif isinstance(row, dict):
        # 字典类型
        for value in row.values():
            sanitized.append(_sanitize_value(value))
    elif isinstance(row, (tuple, list)):
        # 元组或列表类型
        for value in row:
            sanitized.append(_sanitize_value(value))
    else:
        # 其他类型，尝试转换为列表
        try:
            row_list = list(row)
            for value in row_list:
                sanitized.append(_sanitize_value(value))
        except (TypeError, ValueError):
            logger.warning(
                "Unable to convert row to list, using as-is",
                extra={"row_type": type(row).__name__}
            )
            sanitized = [row]
    
    return sanitized


def _sanitize_value(value: Any) -> Any:
    """
    清理单个值，将数据库特定类型转换为 JSON 兼容类型
    
    Args:
        value: 原始值
    
    Returns:
        Any: 清理后的值
    """
    if value is None:
        return None
    
    # Decimal 类型转换为 float
    if isinstance(value, Decimal):
        return float(value)
    
    # datetime 类型转换为 ISO 8601 格式字符串
    if isinstance(value, datetime):
        return value.isoformat()
    
    # date 类型转换为 ISO 8601 格式字符串
    if isinstance(value, date):
        return value.isoformat()
    
    # bytes 类型转换为字符串（假设是 UTF-8 编码）
    if isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            logger.warning(
                "Failed to decode bytes as UTF-8, using base64 representation",
                extra={"value_type": "bytes"}
            )
            return base64.b64encode(value).decode('ascii')
    
    # 其他类型直接返回（int, str, float, bool 等已经是 JSON 兼容的）
    return value


# ============================================================
# 核心处理函数
# ============================================================
async def execute_sql(
    sql: str,
    context: RequestContext,
    db_type: str,
    *,
    sub_query_id: Optional[str] = None,
    diag_ctx: Optional[dict] = None
) -> ExecutionResult:
    """
    执行 SQL 查询并返回结构化结果
    
    在受控环境中安全执行 SQL，包括：
    - 会话级别的超时设置
    - 结果行数限制
    - 数据清理和规范化
    - 错误处理和日志记录
    
    Args:
        sql: 要执行的 SQL 查询字符串
        context: 请求上下文
        db_type: 数据库类型（"mysql" 或 "postgresql"）
    
    Returns:
        ExecutionResult: 执行结果，包含状态、数据和元数据
    
    Raises:
        Stage5Error: 当执行失败时抛出（但通常返回错误状态的 ExecutionResult）
    """
    # 记录开始时间
    start_time = time.time()
    
    # Step 1: Resource Acquisition
    try:
        # 获取数据库引擎
        engine = get_engine(context.tenant_id)
        
        # 实例化方言适配器
        dialect_adapter = DialectAdapter()
        
        # 获取配置
        config = get_pipeline_config()
        timeout_ms = config.execution_timeout_ms
        max_result_rows = config.max_result_rows
        
        logger.debug(
            "Resources acquired",
            extra={
                "timeout_ms": timeout_ms,
                "max_result_rows": max_result_rows
            }
        )
    except Exception as e:
        logger.error(
            "Failed to acquire resources",
            extra={"error": str(e)}
        )
        latency_ms = int((time.time() - start_time) * 1000)
        return ExecutionResult.create_error(
            error=f"Failed to acquire database resources: {str(e)}",
            latency_ms=latency_ms
        )
    
    # Step 2: Secure Execution Sandbox
    try:
        async with engine.connect() as conn:
            # Session Guardrails: 设置会话级别的超时和只读模式
            session_setup_sqls = dialect_adapter.get_session_setup_sql(
                timeout_ms=timeout_ms,
                db_type=db_type
            )
            
            # 执行会话设置 SQL
            for setup_sql in session_setup_sqls:
                try:
                    await conn.execute(text(setup_sql))
                    logger.debug(
                        "Session setup SQL executed",
                        extra={"setup_sql": setup_sql}
                    )
                except Exception as e:
                    logger.warning(
                        "Failed to execute session setup SQL",
                        extra={"setup_sql": setup_sql, "error": str(e)}
                    )
                    # 继续执行，不中断流程
            
            # ============================================================
            # 诊断 SQL：证据性定位（仅在 SQL_DIAGNOSTICS=1 时执行）
            # ============================================================
            if os.getenv("SQL_DIAGNOSTICS") == "1" and diag_ctx:
                try:
                    await _execute_diagnostic_queries(
                        conn=conn,
                        context=context,
                        sub_query_id=sub_query_id,
                        diag_ctx=diag_ctx,
                        db_type=db_type
                    )
                except Exception as e:
                    # 诊断失败不影响主流程
                    logger.warning(
                        "诊断SQL执行异常（不影响主流程）",
                        extra={
                            "request_id": context.request_id,
                            "sub_query_id": sub_query_id,
                            "error": str(e)
                        }
                    )
            
            # Query Execution: 执行主 SQL 查询
            logger.debug(
                "Executing main SQL query",
                extra={"sql_preview": sql[:200] if len(sql) > 200 else sql}
            )
            
            result = await conn.execute(text(sql))
            
            # 获取列名
            columns = list(result.keys())
            
            if not columns:
                logger.warning("Query returned no columns")
                latency_ms = int((time.time() - start_time) * 1000)
                return ExecutionResult.create_success(
                    columns=[],
                    rows=[],
                    is_truncated=False,
                    latency_ms=latency_ms,
                    row_count=0
                )
            
            # Hard Limit Fetch: 防御性地获取 max_result_rows + 1 行来检测截断
            fetch_size = max_result_rows + 1
            rows_raw = result.fetchmany(fetch_size)
            
            # 确定是否截断
            is_truncated = len(rows_raw) > max_result_rows
            
            # 如果截断，只保留前 max_result_rows 行
            if is_truncated:
                rows_raw = rows_raw[:max_result_rows]
                logger.warning(
                    "Query result truncated",
                    extra={
                        "max_result_rows": max_result_rows,
                        "fetched_rows": len(rows_raw) + 1
                    }
                )
            
            # Step 3: Data Sanitization & Normalization
            rows_sanitized: List[List[Any]] = []
            for row in rows_raw:
                sanitized_row = _sanitize_row(row)
                rows_sanitized.append(sanitized_row)
            
            # 计算执行耗时
            latency_ms = int((time.time() - start_time) * 1000)
            row_count = len(rows_sanitized)
            
            truncated_info = " | 已截断" if is_truncated else ""
            logger.info(
                f"Stage 5 完成 | SQL 执行 | 行数: {row_count}, 列数: {len(columns)}{truncated_info} | 耗时: {latency_ms}ms",
                extra={
                    "row_count": row_count,
                    "column_count": len(columns),
                    "is_truncated": is_truncated,
                    "latency_ms": latency_ms
                }
            )
            
            # ============================================================
            # 产出物日志：SQL 执行结果（人眼优先：单条日志输出“多行 JSON”）
            # ============================================================
            rows_head_count = min(5, len(rows_sanitized))
            rows_head = rows_sanitized[:rows_head_count] if rows_sanitized else []

            sub_query_id_part = f" sub_query_id={sub_query_id}" if sub_query_id else ""

            result_payload = {
                "sub_query_id": sub_query_id,
                "row_count": row_count,
                "column_count": len(columns),
                "is_truncated": is_truncated,
                "latency_ms": latency_ms,
                "columns": columns,
                "rows_head": rows_head,
            }

            # pretty JSON（多行、缩进、中文不转义）
            result_json_pretty = json.dumps(
                result_payload,
                ensure_ascii=False,
                indent=2,
                default=str
            )

            # 防刷屏：按“行数”截断（更适合人眼），必要时再按“字符数”兜底
            max_lines = 80
            lines = result_json_pretty.splitlines()
            if len(lines) > max_lines:
                lines = lines[:max_lines] + ["  ...(truncated)"]
                result_json_pretty = "\n".join(lines)

            max_chars = 4000
            if len(result_json_pretty) > max_chars:
                result_json_pretty = result_json_pretty[:max_chars] + "\n...(truncated)"

            logger.info(
                f"【STAGE5关键产物：SQL执行结果】 request_id={context.request_id}{sub_query_id_part}\n{result_json_pretty}"
            )
            
            # Step 4: Result Encapsulation
            return ExecutionResult.create_success(
                columns=columns,
                rows=rows_sanitized,
                is_truncated=is_truncated,
                latency_ms=latency_ms,
                row_count=row_count
            )
    
    except SQLTimeoutError as e:
        # SQL 执行超时
        latency_ms = int((time.time() - start_time) * 1000)
        error_msg = f"SQL execution timeout after {timeout_ms}ms: {str(e)}"
        logger.error(
            "SQL execution timeout",
            extra={
                "timeout_ms": timeout_ms,
                "error": str(e),
                "latency_ms": latency_ms
            }
        )
        return ExecutionResult.create_error(
            error=error_msg,
            latency_ms=latency_ms
        )
    
    except ProgrammingError as e:
        # SQL 语法错误或编程错误
        latency_ms = int((time.time() - start_time) * 1000)
        error_msg = f"SQL execution error: {str(e)}"
        logger.error(
            "SQL execution programming error",
            extra={
                "error": str(e),
                "error_code": getattr(e, 'code', None),
                "latency_ms": latency_ms
            }
        )
        return ExecutionResult.create_error(
            error=error_msg,
            latency_ms=latency_ms
        )
    
    except Exception as e:
        # 其他数据库异常
        latency_ms = int((time.time() - start_time) * 1000)
        error_msg = f"Database error: {str(e)}"
        logger.error(
            "Unexpected database error",
            extra={
                "error": str(e),
                "error_type": type(e).__name__,
                "latency_ms": latency_ms
            }
        )
        return ExecutionResult.create_error(
            error=error_msg,
            latency_ms=latency_ms
        )

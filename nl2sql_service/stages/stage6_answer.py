"""
Stage 6: Answer Generation (答案生成)

聚合多个子查询的结果，处理部分失败情况，并使用 LLM 生成最终的人类可读答案。
对应详细设计文档 3.6 的定义。
"""
from typing import Any, Dict, List, Optional, Tuple

from config.pipeline_config import get_pipeline_config
from core.ai_client import get_ai_client
from schemas.answer import FinalAnswer, FinalAnswerStatus, ResultDataItem
from schemas.result import ExecutionResult, ExecutionStatus
from utils.log_manager import get_logger
from utils.prompt_templates import PROMPT_CLARIFICATION, PROMPT_DATA_INSIGHT

logger = get_logger(__name__)


# ============================================================
# 异常定义
# ============================================================
class Stage6Error(Exception):
    """
    Stage 6 处理异常
    
    用于表示答案生成阶段的错误，包括：
    - LLM 调用失败
    - 数据格式错误
    - 结果聚合失败
    """
    pass


# ============================================================
# 类型定义
# ============================================================
# batch_results 中每个元素的类型
# 假设格式为: {"sub_query_id": str, "sub_query_description": str, "execution_result": ExecutionResult}
BatchResultItem = Dict[str, Any]


# ============================================================
# 辅助函数
# ============================================================
def _build_multi_table_markdown(
    success_items: List[BatchResultItem],
    failed_items: List[BatchResultItem],
    max_llm_rows: int
) -> str:
    """
    构建多表 Markdown 格式的查询结果
    
    将多个子查询的结果格式化为 Markdown 表格，每个子查询一个表格。
    如果结果行数超过 max_llm_rows，则截断。
    
    Args:
        success_items: 成功的子查询结果列表
        failed_items: 失败的子查询结果列表
        max_llm_rows: 每个表格显示的最大行数
    
    Returns:
        str: Markdown 格式的查询结果文本
    """
    markdown_parts = []
    
    # 处理成功的子查询
    for item in success_items:
        sub_query_id = item.get("sub_query_id", "unknown")
        sub_query_description = item.get("sub_query_description", "未知查询")
        execution_result: ExecutionResult = item.get("execution_result")
        
        if not execution_result or not execution_result.data:
            continue
        
        # 添加子查询标题
        markdown_parts.append(f"## {sub_query_description} (ID: {sub_query_id})")
        markdown_parts.append("")
        
        # 获取数据
        columns = execution_result.data.get("columns", [])
        rows = execution_result.data.get("rows", [])
        is_truncated = execution_result.data.get("is_truncated", False)
        
        if not columns or not rows:
            markdown_parts.append("*查询结果为空*")
            markdown_parts.append("")
            continue
        
        # 截断行数
        display_rows = rows[:max_llm_rows]
        actual_row_count = len(rows)
        
        # 构建表格头部
        header = "| " + " | ".join(str(col) for col in columns) + " |"
        separator = "| " + " | ".join("---" for _ in columns) + " |"
        
        markdown_parts.append(header)
        markdown_parts.append(separator)
        
        # 构建表格行
        for row in display_rows:
            # 确保行数据长度与列数匹配
            row_data = row[:len(columns)]
            # 填充缺失的列
            while len(row_data) < len(columns):
                row_data.append("")
            
            row_str = "| " + " | ".join(str(val) if val is not None else "" for val in row_data) + " |"
            markdown_parts.append(row_str)
        
        # 添加元数据信息
        if is_truncated or actual_row_count > max_llm_rows:
            markdown_parts.append("")
            if is_truncated:
                markdown_parts.append(f"*注意：结果已截断，仅显示前 {max_llm_rows} 行（共 {actual_row_count} 行）*")
            else:
                markdown_parts.append(f"*注意：仅显示前 {max_llm_rows} 行（共 {actual_row_count} 行）*")
        
        markdown_parts.append("")
    
    # 如果有失败的子查询，添加摘要
    if failed_items:
        markdown_parts.append("## 部分数据缺失")
        markdown_parts.append("")
        markdown_parts.append("以下子查询执行失败，无法获取数据：")
        markdown_parts.append("")
        
        for item in failed_items:
            sub_query_id = item.get("sub_query_id", "unknown")
            sub_query_description = item.get("sub_query_description", "未知查询")
            execution_result: ExecutionResult = item.get("execution_result")
            
            error_msg = "未知错误"
            if execution_result and execution_result.error:
                error_msg = execution_result.error
            
            markdown_parts.append(f"- **{sub_query_description}** (ID: {sub_query_id}): {error_msg}")
        
        markdown_parts.append("")
    
    return "\n".join(markdown_parts)


async def _generate_synthesized_insight(
    success_items: List[BatchResultItem],
    failed_items: List[BatchResultItem],
    original_question: str,
    max_llm_rows: int
) -> Tuple[str, List[ResultDataItem]]:
    """
    生成综合洞察（成功/部分成功路径）
    
    使用 LLM 基于多个子查询的结果生成综合答案。
    
    Args:
        success_items: 成功的子查询结果列表
        failed_items: 失败的子查询结果列表
        original_question: 用户原始问题
        max_llm_rows: 每个表格显示的最大行数
    
    Returns:
        Tuple[str, List[ResultDataItem]]: (答案文本, 数据项列表)
    
    Raises:
        Stage6Error: 当 LLM 调用失败时抛出
    """
    # 构建 Markdown 格式的查询结果
    query_result_markdown = _build_multi_table_markdown(
        success_items=success_items,
        failed_items=failed_items,
        max_llm_rows=max_llm_rows
    )
    
    # 计算总执行耗时和行数（仅统计成功的）
    total_latency_ms = 0
    total_row_count = 0
    has_truncated = False
    
    for item in success_items:
        execution_result: ExecutionResult = item.get("execution_result")
        if execution_result:
            total_latency_ms += execution_result.execution_meta.get("latency_ms", 0)
            total_row_count += execution_result.execution_meta.get("row_count", 0)
            if execution_result.data and execution_result.data.get("is_truncated", False):
                has_truncated = True
    
    # 格式化提示模板
    formatted_prompt = PROMPT_DATA_INSIGHT.format(
        original_question=original_question,
        query_result_data=query_result_markdown,
        execution_latency_ms=total_latency_ms,
        row_count=total_row_count,
        is_truncated="是" if has_truncated else "否"
    )
    
    # 调用 LLM
    try:
        ai_client = get_ai_client()
        messages = [
            {
                "role": "user",
                "content": formatted_prompt
            }
        ]
        
        answer_text = await ai_client.generate_answer(
            messages=messages,
            temperature=0.3  # 稍微提高温度以获得更自然的语言
        )
        
        logger.debug(
            "LLM generated synthesized insight",
            extra={
                "success_count": len(success_items),
                "failed_count": len(failed_items),
                "answer_length": len(answer_text)
            }
        )
    except Exception as e:
        logger.error(
            "Failed to call LLM for synthesized insight",
            extra={"error": str(e)}
        )
        raise Stage6Error(f"Failed to generate synthesized insight: {str(e)}") from e
    
    # 构建数据项列表
    data_list: List[ResultDataItem] = []
    
    # 添加成功的项
    for item in success_items:
        sub_query_id = item.get("sub_query_id", "unknown")
        sub_query_description = item.get("sub_query_description", "未知查询")
        execution_result: ExecutionResult = item.get("execution_result")
        
        data_item = ResultDataItem(
            sub_query_id=sub_query_id,
            title=sub_query_description,
            data=execution_result.data if execution_result else None,
            error=None
        )
        data_list.append(data_item)
    
    # 添加失败的项
    for item in failed_items:
        sub_query_id = item.get("sub_query_id", "unknown")
        sub_query_description = item.get("sub_query_description", "未知查询")
        execution_result: ExecutionResult = item.get("execution_result")
        
        error_msg = "未知错误"
        if execution_result and execution_result.error:
            error_msg = execution_result.error
        
        data_item = ResultDataItem(
            sub_query_id=sub_query_id,
            title=sub_query_description,
            data=None,
            error=error_msg
        )
        data_list.append(data_item)
    
    return answer_text, data_list


def _select_primary_error(failed_items: List[BatchResultItem]) -> Tuple[str, str]:
    """
    选择主要错误
    
    从失败的子查询中选择最重要的错误信息。
    优先级：PermissionDeniedError > TimeoutError > 其他错误
    
    Args:
        failed_items: 失败的子查询结果列表
    
    Returns:
        Tuple[str, str]: (错误类型, 错误消息)
    """
    if not failed_items:
        return "UNKNOWN", "未知错误"
    
    # 优先级排序
    priority_order = {
        "PermissionDeniedError": 1,
        "权限": 1,
        "permission": 1,
        "TimeoutError": 2,
        "timeout": 2,
        "超时": 2,
    }
    
    best_error = None
    best_priority = float('inf')
    
    for item in failed_items:
        execution_result: ExecutionResult = item.get("execution_result")
        if not execution_result or not execution_result.error:
            continue
        
        error_msg = execution_result.error.lower()
        
        # 确定优先级
        priority = float('inf')
        for key, prio in priority_order.items():
            if key.lower() in error_msg:
                priority = min(priority, prio)
                break
        
        if priority < best_priority:
            best_priority = priority
            best_error = execution_result.error
    
    if best_error:
        # 确定错误类型
        error_lower = best_error.lower()
        if any(key in error_lower for key in ["permission", "权限", "denied"]):
            error_type = "PERMISSION_DENIED"
        elif any(key in error_lower for key in ["timeout", "超时"]):
            error_type = "TIMEOUT"
        else:
            error_type = "OTHER"
        
        return error_type, best_error
    
    # 如果没有找到，返回第一个错误
    first_item = failed_items[0]
    execution_result: ExecutionResult = first_item.get("execution_result")
    if execution_result and execution_result.error:
        return "OTHER", execution_result.error
    
    return "UNKNOWN", "所有子查询执行失败，但未提供具体错误信息"


async def _handle_all_failed(
    failed_items: List[BatchResultItem],
    original_question: str
) -> Tuple[str, List[ResultDataItem]]:
    """
    处理全部失败的情况
    
    当所有子查询都失败时，根据错误类型生成相应的响应。
    
    Args:
        failed_items: 失败的子查询结果列表
        original_question: 用户原始问题
    
    Returns:
        Tuple[str, List[ResultDataItem]]: (答案文本, 数据项列表)
    
    Raises:
        Stage6Error: 当 LLM 调用失败时抛出
    """
    # 选择主要错误
    error_type, error_msg = _select_primary_error(failed_items)
    
    logger.info(
        "All sub-queries failed",
        extra={
            "error_type": error_type,
            "failed_count": len(failed_items)
        }
    )
    
    # 根据错误类型生成响应
    if error_type == "PERMISSION_DENIED":
        # 权限错误：使用澄清提示
        try:
            uncertain_info = f"权限不足：{error_msg}"
            formatted_prompt = PROMPT_CLARIFICATION.format(
                original_question=original_question,
                uncertain_information=uncertain_info
            )
            
            ai_client = get_ai_client()
            messages = [
                {
                    "role": "user",
                    "content": formatted_prompt
                }
            ]
            
            answer_text = await ai_client.generate_answer(
                messages=messages,
                temperature=0.3
            )
        except Exception as e:
            logger.error(
                "Failed to call LLM for clarification",
                extra={"error": str(e)}
            )
            # 回退到静态消息
            answer_text = f"抱歉，您没有权限访问相关数据。错误详情：{error_msg}"
    
    elif error_type == "TIMEOUT":
        # 超时错误：静态消息
        answer_text = (
            f"抱歉，查询执行超时。这可能是由于数据量过大或系统负载较高导致的。"
            f"请尝试缩小查询范围或稍后重试。错误详情：{error_msg}"
        )
    
    else:
        # 其他错误：静态消息
        answer_text = (
            f"抱歉，所有子查询执行失败，无法获取数据。"
            f"错误详情：{error_msg}"
        )
    
    # 构建数据项列表（全部为失败项）
    data_list: List[ResultDataItem] = []
    for item in failed_items:
        sub_query_id = item.get("sub_query_id", "unknown")
        sub_query_description = item.get("sub_query_description", "未知查询")
        execution_result: ExecutionResult = item.get("execution_result")
        
        error_msg_item = "未知错误"
        if execution_result and execution_result.error:
            error_msg_item = execution_result.error
        
        data_item = ResultDataItem(
            sub_query_id=sub_query_id,
            title=sub_query_description,
            data=None,
            error=error_msg_item
        )
        data_list.append(data_item)
    
    return answer_text, data_list


# ============================================================
# 核心处理函数
# ============================================================
async def generate_final_answer(
    batch_results: List[BatchResultItem],
    original_question: str
) -> FinalAnswer:
    """
    生成最终答案
    
    聚合多个子查询的结果，处理部分失败情况，并使用 LLM 生成最终的人类可读答案。
    
    Args:
        batch_results: 批量结果列表，每个元素包含：
            - sub_query_id: 子查询ID
            - sub_query_description: 子查询描述
            - execution_result: ExecutionResult 对象
        original_question: 用户原始问题
    
    Returns:
        FinalAnswer: 最终答案对象
    
    Raises:
        Stage6Error: 当处理失败时抛出
    """
    logger.info(
        "Starting Stage 6: Answer Generation",
        extra={
            "batch_count": len(batch_results),
            "question_length": len(original_question)
        }
    )
    
    # Step 1: Sorting & Routing
    # 分离成功和失败的子查询
    success_items: List[BatchResultItem] = []
    failed_items: List[BatchResultItem] = []
    
    for item in batch_results:
        execution_result: ExecutionResult = item.get("execution_result")
        
        if execution_result and execution_result.status == ExecutionStatus.SUCCESS:
            success_items.append(item)
        else:
            failed_items.append(item)
    
    logger.info(
        "Results sorted",
        extra={
            "success_count": len(success_items),
            "failed_count": len(failed_items)
        }
    )
    
    # Step 2: Synthesized Insight (Success/Partial Success Path)
    if success_items:
        try:
            # 获取配置
            config = get_pipeline_config()
            max_llm_rows = config.max_llm_rows
            
            # 生成综合洞察
            answer_text, data_list = await _generate_synthesized_insight(
                success_items=success_items,
                failed_items=failed_items,
                original_question=original_question,
                max_llm_rows=max_llm_rows
            )
            
            # 确定状态
            if failed_items:
                status = FinalAnswerStatus.PARTIAL_SUCCESS
            else:
                status = FinalAnswerStatus.SUCCESS
            
            logger.info(
                "Stage 6 completed successfully",
                extra={
                    "status": status.value,
                    "answer_length": len(answer_text),
                    "data_item_count": len(data_list)
                }
            )
            
            return FinalAnswer(
                answer_text=answer_text,
                data_list=data_list,
                status=status
            )
        
        except Exception as e:
            logger.error(
                "Failed to generate synthesized insight",
                extra={"error": str(e)}
            )
            raise Stage6Error(f"Failed to generate final answer: {str(e)}") from e
    
    # Step 3: Error Handling (All Failed Path)
    else:
        try:
            # 处理全部失败的情况
            answer_text, data_list = await _handle_all_failed(
                failed_items=failed_items,
                original_question=original_question
            )
            
            logger.info(
                "Stage 6 completed (all failed)",
                extra={
                    "answer_length": len(answer_text),
                    "data_item_count": len(data_list)
                }
            )
            
            return FinalAnswer(
                answer_text=answer_text,
                data_list=data_list,
                status=FinalAnswerStatus.ALL_FAILED
            )
        
        except Exception as e:
            logger.error(
                "Failed to handle all failed case",
                extra={"error": str(e)}
            )
            raise Stage6Error(f"Failed to generate final answer: {str(e)}") from e

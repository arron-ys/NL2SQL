"""
Stage 1: Query Decomposition (查询分解)

将原始自然语言请求拆解为多个原子子查询。
对应详细设计文档 3.1 的定义。
"""
import json
import uuid
from datetime import date, datetime
from typing import List, Dict, Any

from core.ai_client import get_ai_client
from schemas.request import (
    QueryRequestDescription,
    RequestContext,
    SubQueryItem
)
from utils.log_manager import get_logger, get_request_id, set_request_id
from utils.prompt_templates import PROMPT_SUBQUERY_DECOMPOSITION

logger = get_logger(__name__)


# ============================================================
# 异常定义
# ============================================================
class Stage1Error(Exception):
    """
    Stage 1 处理异常
    
    用于表示查询分解阶段的错误，包括：
    - LLM 输出解析失败
    - 输出格式不符合预期
    - 子查询列表为空
    """
    pass


# ============================================================
# 核心处理函数
# ============================================================
async def process_request(
    question: str,
    user_id: str,
    role_id: str,
    tenant_id: str
) -> QueryRequestDescription:
    """
    处理用户请求，将复杂查询拆解为多个原子子查询
    
    Args:
        question: 用户的自然语言查询问题
        user_id: 用户唯一标识符
        role_id: 用户角色 ID
        tenant_id: 租户 ID（可选）
    
    Returns:
        QueryRequestDescription: 包含请求上下文和子查询列表的完整描述
    
    Raises:
        Stage1Error: 当 LLM 输出解析失败或格式不符合预期时抛出
    """
    # Step 1: Build RequestContext
    # 从日志上下文获取 request_id（由 middleware 在 HTTP 请求入口处设置）
    request_id = get_request_id()
    
    # 如果获取到的是默认值 "system"，说明不是 HTTP 请求路径，需要生成新的 ID 作为兜底
    if request_id == "system":
        request_id = f"req-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
        set_request_id(request_id)
    
    # 获取当前日期，格式为 "YYYY-MM-DD"
    current_date = date.today()
    current_date_str = current_date.strftime("%Y-%m-%d")
    
    logger.info(
        "Starting Stage 1: Query Decomposition",
        extra={
            "user_id": user_id,
            "role_id": role_id,
            "tenant_id": tenant_id,
            "question_length": len(question)
        }
    )
    
    # 创建 RequestContext 对象
    request_context = RequestContext(
        user_id=user_id,
        role_id=role_id,
        tenant_id=tenant_id,
        request_id=request_id,
        current_date=current_date
    )
    
    # Step 2: Call LLM for Decomposition
    # 格式化提示模板
    formatted_prompt = PROMPT_SUBQUERY_DECOMPOSITION.format(
        current_date=current_date_str,
        question=question
    )
    
    # 构建消息列表
    messages = [
        {
            "role": "user",
            "content": formatted_prompt
        }
    ]
    
    # 调用 LLM
    try:
        ai_client = get_ai_client()
        parsed_response = await ai_client.generate_decomposition(
            messages=messages,
            temperature=0.0
        )
        
        logger.debug(
            "LLM response received",
            extra={"response_keys": list(parsed_response.keys())}
        )
    except Exception as e:
        logger.error(
            "LLM call failed",
            extra={"error": str(e)}
        )
        raise Stage1Error(f"Failed to call LLM for query decomposition: {str(e)}") from e
    
    # Step 3: Parse and Validate LLM Output
    # parsed_response 已经是解析后的 JSON 对象（由 ai_client.generate_decomposition 返回），无需再次解析
    
    # 验证响应结构
    if "sub_queries" not in parsed_response:
        logger.error(
            "LLM response missing 'sub_queries' field",
            extra={"response_keys": list(parsed_response.keys())}
        )
        raise Stage1Error("LLM response missing 'sub_queries' field")
    
    sub_queries_raw = parsed_response["sub_queries"]
    
    # 验证 sub_queries 是否为列表
    if not isinstance(sub_queries_raw, list):
        logger.error(
            "LLM response 'sub_queries' is not a list",
            extra={"type": type(sub_queries_raw).__name__}
        )
        raise Stage1Error(f"LLM response 'sub_queries' is not a list, got {type(sub_queries_raw).__name__}")
    
    # 验证列表不为空
    if len(sub_queries_raw) == 0:
        logger.error("LLM response 'sub_queries' list is empty")
        raise Stage1Error("LLM response 'sub_queries' list is empty")
    
    # 验证每个子查询项
    for idx, item in enumerate(sub_queries_raw):
        if not isinstance(item, dict):
            logger.error(
                f"Sub-query item at index {idx} is not a dictionary",
                extra={"type": type(item).__name__}
            )
            raise Stage1Error(f"Sub-query item at index {idx} is not a dictionary")
        
        if "description" not in item:
            logger.error(
                f"Sub-query item at index {idx} missing 'description' field"
            )
            raise Stage1Error(f"Sub-query item at index {idx} missing 'description' field")
        
        description = item.get("description", "")
        if not description or not isinstance(description, str) or not description.strip():
            logger.error(
                f"Sub-query item at index {idx} has empty or invalid 'description'",
                extra={"description": description}
            )
            raise Stage1Error(f"Sub-query item at index {idx} has empty or invalid 'description'")
    
    logger.info(
        f"Successfully parsed {len(sub_queries_raw)} sub-queries from LLM response"
    )
    
    # Step 4: Normalize Sub-Queries
    # 忽略 LLM 提供的 id，生成新的稳定 id
    normalized_sub_queries: List[SubQueryItem] = []
    
    for index, item in enumerate(sub_queries_raw):
        # 生成新的稳定 id，格式为 f"{request_id}-{index:03d}"
        stable_id = f"{request_id}-{index:03d}"
        
        # 获取 description（已由 LLM 解析相对时间为绝对时间）
        description = item["description"].strip()
        
        # 创建 SubQueryItem 对象
        sub_query_item = SubQueryItem(
            id=stable_id,
            description=description
        )
        
        normalized_sub_queries.append(sub_query_item)
        
        logger.debug(
            f"Normalized sub-query {index + 1}/{len(sub_queries_raw)}",
            extra={
                "sub_query_id": stable_id,
                "description_length": len(description)
            }
        )
    
    # Step 5: Assemble Final Output
    query_request_description = QueryRequestDescription(
        request_context=request_context,
        sub_queries=normalized_sub_queries
    )
    
    logger.info(
        "Stage 1 completed successfully",
        extra={
            "sub_query_count": len(normalized_sub_queries),
            "request_id": request_id
        }
    )
    
    return query_request_description

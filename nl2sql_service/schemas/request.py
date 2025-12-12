"""
Request Schema Definition

定义 Stage 1 的输出结构，即从原始自然语言请求到结构化查询请求的转换结果。

对应详细设计文档 3.1.2 的输入输出定义。
"""
from datetime import date
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class RequestContext(BaseModel):
    """
    请求上下文
    
    包含用户身份、租户信息、请求元数据等上下文信息。
    这些信息用于权限控制、日志追踪、业务逻辑判断等。
    """
    model_config = ConfigDict(extra='forbid')
    
    user_id: str = Field(
        ...,
        description="用户唯一标识符"
    )
    
    role_id: str = Field(
        ...,
        description="用户角色 ID，用于权限控制（对应 semantic_security.yaml）"
    )
    
    tenant_id: Optional[str] = Field(
        default=None,
        description="租户 ID，用于多租户场景的数据隔离"
    )
    
    request_id: str = Field(
        ...,
        description="请求唯一标识符，用于日志追踪和问题排查"
    )
    
    current_date: date = Field(
        ...,
        description="当前日期，用于时间范围计算（如 LAST_N 类型）"
    )
    
    # 可扩展字段
    session_id: Optional[str] = Field(
        default=None,
        description="会话 ID，用于多轮对话场景"
    )
    
    client_ip: Optional[str] = Field(
        default=None,
        description="客户端 IP 地址"
    )
    
    user_agent: Optional[str] = Field(
        default=None,
        description="用户代理信息"
    )


class SubQueryItem(BaseModel):
    """
    子查询项
    
    Stage 1 将复杂问题拆解为多个原子查询，每个原子查询对应一个 SubQueryItem。
    每个子查询将独立进入后续的 Stage 2-6 流程。
    """
    model_config = ConfigDict(extra='forbid')
    
    id: str = Field(
        ...,
        description="子查询唯一标识符，格式建议为 'sq_1', 'sq_2' 等"
    )
    
    description: str = Field(
        ...,
        description="子查询的自然语言描述，将作为 Stage 2 的输入"
    )


class QueryRequestDescription(BaseModel):
    """
    查询请求描述
    
    Stage 1 的完整输出结构，包含请求上下文和拆解后的子查询列表。
    
    这是整个流水线的入口数据结构，后续所有阶段都基于此结构进行处理。
    """
    model_config = ConfigDict(extra='forbid')
    
    request_context: RequestContext = Field(
        ...,
        description="请求上下文信息"
    )
    
    sub_queries: List[SubQueryItem] = Field(
        ...,
        min_length=1,
        description="子查询列表，至少包含一个子查询"
    )



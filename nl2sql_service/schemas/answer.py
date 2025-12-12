"""
Answer Schema Definition

定义 Stage 6 的输出结构，即最终答案生成的结果。

对应详细设计文档 3.6 的定义。
"""
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class ResultDataItem(BaseModel):
    """
    结果数据项
    
    表示单个子查询的执行结果数据，用于最终答案的数据列表。
    """
    model_config = ConfigDict(extra='forbid')
    
    sub_query_id: str = Field(
        ...,
        description="子查询唯一标识符"
    )
    
    title: str = Field(
        ...,
        description="子查询的标题/描述"
    )
    
    data: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "查询结果数据，仅当执行成功时有效。"
            "包含 'columns'（列名列表）、'rows'（行数据列表）、'is_truncated'（是否截断）"
        )
    )
    
    error: Optional[str] = Field(
        default=None,
        description="错误信息，仅当执行失败时有效"
    )


class FinalAnswerStatus(str, Enum):
    """
    最终答案状态枚举
    
    表示所有子查询的执行状态汇总。
    """
    SUCCESS = "SUCCESS"           # 所有子查询都成功
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"  # 部分子查询成功
    ALL_FAILED = "ALL_FAILED"     # 所有子查询都失败


class FinalAnswer(BaseModel):
    """
    最终答案
    
    Stage 6 的完整输出结构，包含生成的答案文本、数据列表和状态。
    """
    model_config = ConfigDict(extra='forbid')
    
    answer_text: str = Field(
        ...,
        description="生成的最终答案文本（自然语言）"
    )
    
    data_list: List[ResultDataItem] = Field(
        ...,
        description="所有子查询的结果数据列表"
    )
    
    status: FinalAnswerStatus = Field(
        ...,
        description="最终答案状态"
    )

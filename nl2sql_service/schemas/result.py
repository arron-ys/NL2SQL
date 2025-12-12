"""
Result Schema Definition

定义 Stage 5 的输出结构，即 SQL 执行后的结果数据。

对应详细设计文档 3.5.2 的输入输出定义。
"""
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class ExecutionStatus(str, Enum):
    """
    执行状态枚举
    
    表示 SQL 执行的最终状态。
    """
    SUCCESS = "SUCCESS"  # 执行成功
    ERROR = "ERROR"      # 执行失败


class ExecutionResult(BaseModel):
    """
    SQL 执行结果
    
    Stage 5 的完整输出结构，包含执行状态、数据、元数据和错误信息。
    
    数据结构说明：
    - status: 执行状态（成功/失败）
    - data: 查询结果数据（仅当 status=SUCCESS 时有效）
        - columns: 列名列表
        - rows: 行数据列表（每行是一个字典或列表）
        - is_truncated: 是否被截断（超过 max_result_rows 限制）
    - execution_meta: 执行元数据
        - latency_ms: 执行耗时（毫秒）
        - row_count: 实际返回行数
        - executed_at: 执行时间戳
    - error: 错误信息（仅当 status=ERROR 时有效）
    """
    model_config = ConfigDict(extra='forbid')
    
    status: ExecutionStatus = Field(
        ...,
        description="执行状态"
    )
    
    data: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "查询结果数据，仅当 status=SUCCESS 时有效。"
            "包含 'columns'（列名列表）、'rows'（行数据列表）、'is_truncated'（是否截断）"
        )
    )
    
    execution_meta: Dict[str, Any] = Field(
        ...,
        description=(
            "执行元数据，包含："
            "- latency_ms: 执行耗时（毫秒）"
            "- row_count: 实际返回行数"
            "- executed_at: 执行时间戳（ISO 8601 格式）"
        )
    )
    
    error: Optional[str] = Field(
        default=None,
        description="错误信息，仅当 status=ERROR 时有效"
    )
    
    @classmethod
    def create_success(
        cls,
        columns: List[str],
        rows: List[List[Any]],
        is_truncated: bool,
        latency_ms: int,
        row_count: int,
        executed_at: Optional[datetime] = None
    ) -> "ExecutionResult":
        """
        创建成功结果
        
        Args:
            columns: 列名列表
            rows: 行数据列表
            is_truncated: 是否被截断
            latency_ms: 执行耗时（毫秒）
            row_count: 实际返回行数
            executed_at: 执行时间戳，如果为 None 则使用当前时间
        
        Returns:
            ExecutionResult: 成功结果对象
        """
        if executed_at is None:
            executed_at = datetime.now()
        
        return cls(
            status=ExecutionStatus.SUCCESS,
            data={
                "columns": columns,
                "rows": rows,
                "is_truncated": is_truncated
            },
            execution_meta={
                "latency_ms": latency_ms,
                "row_count": row_count,
                "executed_at": executed_at.isoformat()
            },
            error=None
        )
    
    @classmethod
    def create_error(
        cls,
        error: str,
        latency_ms: int,
        executed_at: Optional[datetime] = None
    ) -> "ExecutionResult":
        """
        创建错误结果
        
        Args:
            error: 错误信息
            latency_ms: 执行耗时（毫秒）
            executed_at: 执行时间戳，如果为 None 则使用当前时间
        
        Returns:
            ExecutionResult: 错误结果对象
        """
        if executed_at is None:
            executed_at = datetime.now()
        
        return cls(
            status=ExecutionStatus.ERROR,
            data=None,
            execution_meta={
                "latency_ms": latency_ms,
                "row_count": 0,
                "executed_at": executed_at.isoformat()
            },
            error=error
        )



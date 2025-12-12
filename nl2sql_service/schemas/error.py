"""
Error Schema Definition

定义流水线执行过程中的错误结构。

对应详细设计文档 2.3 的定义。
"""
from pydantic import BaseModel, ConfigDict, Field


class PipelineError(BaseModel):
    """
    流水线错误
    
    表示单个子查询流水线执行过程中的错误。
    """
    model_config = ConfigDict(extra='forbid')
    
    stage: str = Field(
        ...,
        description="发生错误的阶段，例如 'STAGE_2_PLAN_GENERATION', 'STAGE_3_VALIDATION' 等"
    )
    
    code: str = Field(
        ...,
        description="错误代码，例如 'PERMISSION_DENIED', 'MISSING_METRIC', 'TIMEOUT' 等"
    )
    
    message: str = Field(
        ...,
        description="错误消息描述"
    )

"""
Schemas Package

核心数据交互协议定义，包含：
- plan: PLAN 模型（核心数据契约）
- request: 请求模型（Stage 1 输出）
- result: 结果模型（Stage 5 输出）
- answer: 答案模型（Stage 6 输出）
- error: 错误模型（流水线错误）
"""
from .answer import (
    FinalAnswer,
    FinalAnswerStatus,
    ResultDataItem,
)
from .error import PipelineError
from .plan import (
    CompareMode,
    DimensionItem,
    FilterItem,
    FilterOp,
    MetricItem,
    OrderDirection,
    OrderItem,
    PlanIntent,
    QueryPlan,
    TimeGrain,
    TimeRange,
    TimeRangeType,
)
from .request import (
    QueryRequestDescription,
    RequestContext,
    SubQueryItem,
)
from .result import (
    ExecutionResult,
    ExecutionStatus,
)

__all__ = [
    # Plan models
    "QueryPlan",
    "PlanIntent",
    "TimeGrain",
    "FilterOp",
    "CompareMode",
    "OrderDirection",
    "TimeRangeType",
    "MetricItem",
    "DimensionItem",
    "FilterItem",
    "TimeRange",
    "OrderItem",
    # Request models
    "QueryRequestDescription",
    "RequestContext",
    "SubQueryItem",
    # Result models
    "ExecutionResult",
    "ExecutionStatus",
    # Answer models
    "FinalAnswer",
    "FinalAnswerStatus",
    "ResultDataItem",
    # Error models
    "PipelineError",
]


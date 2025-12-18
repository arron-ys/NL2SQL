"""
PLAN Schema Definition

核心数据契约：定义从自然语言到 SQL 的中间态表达结构。
严格遵循 plan_spec.pdf 中的结构定义和约束。

关键约束：
1. 扁平化结构：不支持嵌套 sub-plans
2. 无 entities 字段：由后端根据 metrics/dimensions 自动推导
3. 严格类型：使用枚举固定值
4. 禁止额外字段：防止 LLM 生成非法字段
"""
from enum import Enum
from typing import Annotated, Any, List, Optional, Union

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator


# ============================================================
# 验证器：将 None 转换为空列表
# ============================================================
def none_to_empty_list(v: Union[List[Any], None]) -> List[Any]:
    """
    验证器：如果传入 None，自动转换为空列表
    
    用于处理前端可能传入 null 或未传列表字段的情况。
    """
    if v is None:
        return []
    return v


# ============================================================
# 枚举定义
# ============================================================
class PlanIntent(str, Enum):
    """
    查询意图枚举
    
    定义查询结果集的形状：
    - AGG: 聚合查询，返回一行或几行汇总数据（标量/字典）
    - TREND: 趋势查询，返回时间序列（X轴是时间，Y轴是值）
    - DETAIL: 明细查询，返回二维明细表
    """
    AGG = "AGG"
    TREND = "TREND"
    DETAIL = "DETAIL"


class TimeGrain(str, Enum):
    """时间粒度枚举"""
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    QUARTER = "QUARTER"
    YEAR = "YEAR"


class FilterOp(str, Enum):
    """过滤器操作符枚举"""
    EQ = "EQ"          # 等于
    NEQ = "NEQ"        # 不等于
    IN = "IN"          # 在列表中
    NOT_IN = "NOT_IN"  # 不在列表中
    GT = "GT"          # 大于
    LT = "LT"          # 小于
    GTE = "GTE"        # 大于等于
    LTE = "LTE"        # 小于等于
    BETWEEN = "BETWEEN"  # 区间
    LIKE = "LIKE"      # 模糊匹配


class CompareMode(str, Enum):
    """
    对比模式枚举
    
    用于指标的时间对比：
    - YOY: Year-over-Year (同比)
    - MOM: Month-over-Month (环比)
    - WOW: Week-over-Week (周环比)
    """
    YOY = "YOY"
    MOM = "MOM"
    WOW = "WOW"


class OrderDirection(str, Enum):
    """排序方向枚举"""
    ASC = "ASC"
    DESC = "DESC"


class TimeRangeType(str, Enum):
    """时间范围类型枚举"""
    LAST_N = "LAST_N"      # 最近 N 个时间单位
    ABSOLUTE = "ABSOLUTE"  # 绝对时间范围


# ============================================================
# 子模型定义
# ============================================================
class MetricItem(BaseModel):
    """
    指标项
    
    表示查询中要聚合的指标，支持时间对比模式。
    """
    model_config = ConfigDict(extra='forbid')
    
    id: str = Field(
        ...,
        description="指标 ID，必须以 METRIC_ 前缀开头（如 METRIC_GMV）"
    )
    compare_mode: Optional[CompareMode] = Field(
        default=None,
        description="时间对比模式，用于同比/环比计算"
    )


class DimensionItem(BaseModel):
    """
    维度项
    
    表示查询中的分组维度，支持时间粒度设置。
    """
    model_config = ConfigDict(extra='forbid')
    
    id: str = Field(
        ...,
        description="维度 ID，必须以 DIM_ 前缀开头（如 DIM_REGION）"
    )
    time_grain: Optional[TimeGrain] = Field(
        default=None,
        description="时间粒度，仅当维度为时间维度时使用"
    )


class FilterItem(BaseModel):
    """
    过滤器项
    
    表示查询中的筛选条件，支持多种操作符。
    """
    model_config = ConfigDict(extra='forbid')
    
    id: str = Field(
        ...,
        description="过滤对象 ID，可以是 DIM_ 或 METRIC_ 或 LF_ 前缀"
    )
    op: FilterOp = Field(
        ...,
        description="过滤操作符"
    )
    values: List[Any] = Field(
        ...,
        description="过滤值列表，根据操作符类型可能有不同格式"
    )


class TimeRange(BaseModel):
    """
    时间范围
    
    支持两种类型：
    1. LAST_N: 最近 N 个时间单位（需要 value 和 unit）
    2. ABSOLUTE: 绝对时间范围（需要 start 和 end）
    """
    model_config = ConfigDict(extra='forbid')
    
    type: TimeRangeType = Field(
        ...,
        description="时间范围类型"
    )
    value: Optional[int] = Field(
        default=None,
        description="LAST_N 类型时使用，表示最近 N 个时间单位"
    )
    unit: Optional[str] = Field(
        default=None,
        description="LAST_N 类型时使用，时间单位（如 'day', 'week', 'month'）"
    )
    start: Optional[str] = Field(
        default=None,
        description="ABSOLUTE 类型时使用，开始时间（ISO 8601 格式）"
    )
    end: Optional[str] = Field(
        default=None,
        description="ABSOLUTE 类型时使用，结束时间（ISO 8601 格式）"
    )

    @model_validator(mode='before')
    @classmethod
    def fix_nested_absolute_structure(cls, data: Any) -> Any:
        """
        兼容性修复：处理 LLM 偶尔产生的嵌套结构幻觉。
        
        LLM 有时会错误地将 ABSOLUTE 类型的 start/end 嵌套在 value 字段中：
        错误: {"type": "ABSOLUTE", "value": {"start": "...", "end": "..."}}
        修正: {"type": "ABSOLUTE", "start": "...", "end": "..."}
        """
        # 确保 data 是字典且类型为 ABSOLUTE
        if not isinstance(data, dict):
            return data
            
        if data.get('type') == 'ABSOLUTE':
            val = data.get('value')
            # 如果 value 是一个字典（说明 LLM 犯错了）
            if isinstance(val, dict):
                # 1. 将嵌套的 start/end 提取到顶层
                if 'start' in val:
                    data['start'] = val['start']
                if 'end' in val:
                    data['end'] = val['end']
                
                # 2. 删除错误的 value 字段（使用 pop 避免 KeyError）
                data.pop('value', None)
                
        return data


class OrderItem(BaseModel):
    """
    排序项
    
    表示查询结果的排序规则。
    """
    model_config = ConfigDict(extra='forbid')
    
    id: str = Field(
        ...,
        description="排序字段 ID，可以是 DIM_ 或 METRIC_ 前缀"
    )
    direction: OrderDirection = Field(
        ...,
        description="排序方向"
    )


# ============================================================
# 主模型：QueryPlan
# ============================================================
class QueryPlan(BaseModel):
    """
    查询计划（PLAN）
    
    核心数据契约，承载从自然语言到 SQL 的中间态表达。
    
    设计约束：
    1. 扁平化结构：不支持嵌套 sub-plans
    2. 无 entities 字段：由后端根据 metrics/dimensions 自动推导
    3. 声明式：只描述"要什么"，不描述"怎么取"
    4. 自包含：每个 PLAN 对象必须独立完整
    
    字段说明：
    - intent: 查询意图，决定结果集形状
    - metrics: 指标列表，默认为空列表
    - dimensions: 维度列表，默认为空列表
    - filters: 过滤器列表，默认为空列表
    - time_range: 时间范围，可选
    - order_by: 排序列表，默认为空列表
    - limit: 结果限制数量，可选
    - warnings: 警告信息列表，用于记录 LLM 生成时的异常情况
    """
    model_config = ConfigDict(
        extra='forbid',  # 禁止额外字段，防止 LLM 生成非法字段
        str_strip_whitespace=True,  # 自动去除字符串首尾空格
    )
    
    intent: PlanIntent = Field(
        ...,
        description="查询意图，决定结果集的形状（AGG/TREND/DETAIL）"
    )
    
    metrics: Annotated[
        List[MetricItem],
        BeforeValidator(none_to_empty_list),
        Field(
            default_factory=list,
            description="指标列表，每个指标可配置时间对比模式"
        )
    ]
    
    dimensions: Annotated[
        List[DimensionItem],
        BeforeValidator(none_to_empty_list),
        Field(
            default_factory=list,
            description="维度列表，用于分组聚合，时间维度可配置粒度"
        )
    ]
    
    filters: Annotated[
        List[FilterItem],
        BeforeValidator(none_to_empty_list),
        Field(
            default_factory=list,
            description="过滤器列表，用于筛选数据"
        )
    ]
    
    time_range: Optional[TimeRange] = Field(
        default=None,
        description="时间范围，用于限定查询的时间窗口"
    )
    
    order_by: Annotated[
        List[OrderItem],
        BeforeValidator(none_to_empty_list),
        Field(
            default_factory=list,
            description="排序规则列表，支持多字段排序"
        )
    ]
    
    limit: Optional[int] = Field(
        default=None,
        gt=0,
        description="结果限制数量，如果为 None 则使用配置中的默认值"
    )
    
    warnings: Annotated[
        List[str],
        BeforeValidator(none_to_empty_list),
        Field(
            default_factory=list,
            description="警告信息列表，用于记录 LLM 生成时的异常或不确定情况"
        )
    ]



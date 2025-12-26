from typing import Any, Dict, List, Optional
from enum import Enum
from pydantic import BaseModel, Field


class ReasonCode(str, Enum):
    """EX evaluation reason codes"""
    EXACT_MATCH = "EXACT_MATCH"
    COLUMN_MISMATCH = "COLUMN_MISMATCH"
    SHAPE_MISMATCH = "SHAPE_MISMATCH"
    VALUE_MISMATCH = "VALUE_MISMATCH"
    TRUNCATED_UNSCORABLE = "TRUNCATED_UNSCORABLE"
    NO_RESULT = "NO_RESULT"
    EXECUTION_ERROR = "EXECUTION_ERROR"
    NO_DATA = "NO_DATA"
    MULTI_SUBQUERY_WARNING = "MULTI_SUBQUERY_WARNING"


class GoldResult(BaseModel):
    """Gold result structure for dataset"""
    columns: List[str] = Field(..., description="Column names")
    rows: List[List[Any]] = Field(..., description="Row data")
    is_truncated: Optional[bool] = Field(default=False, description="Whether result is truncated")


class DatasetCase(BaseModel):
    """Single test case in dataset"""
    case_id: str = Field(..., description="Unique case identifier")
    question: str = Field(..., description="Natural language query")
    expected_outcome: str = Field(..., description="Expected outcome description")
    order_sensitive: bool = Field(..., description="Whether row order matters")
    gold_result: GoldResult = Field(..., description="Gold standard result")
    notes: Optional[str] = Field(default=None, description="Optional notes")


class EvaluationResult(BaseModel):
    """Per-case evaluation result"""
    case_id: str
    question: str
    match: bool
    reason: ReasonCode
    detail: Optional[str] = None
    is_unscorable: bool = False
    is_multi_subquery: bool = False
    pred_columns: Optional[List[str]] = None
    gold_columns: Optional[List[str]] = None
    pred_row_count: Optional[int] = None
    gold_row_count: Optional[int] = None


class SummaryReport(BaseModel):
    """Summary report for EX evaluation"""
    total_cases: int
    scorable_cases: int
    unscorable_cases: int
    exact_match_count: int
    ex_score: float
    multi_subquery_cases: int
    
    unscorable_breakdown: Dict[str, int] = Field(default_factory=dict)
    failure_breakdown: Dict[str, int] = Field(default_factory=dict)
    
    config: Dict[str, Any] = Field(default_factory=dict)

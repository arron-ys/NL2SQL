from typing import Dict, Any, Optional, List
from .canonicalize import canonicalize_table, stable_sort_rows
from .compare import compare_tables
from .schema import DatasetCase, EvaluationResult, SummaryReport, ReasonCode


def extract_pred_table(response: Dict[str, Any], include_trace: bool = False) -> Optional[Dict[str, Any]]:
    """
    Extract pred table from API response following guardrail 2.4.2.
    
    Args:
        response: API response dict
        include_trace: Whether response is in debug mode
        
    Returns:
        Pred table dict or None if extraction fails
    """
    try:
        # Handle include_trace modes
        if "answer" in response:
            # Debug mode: response.answer.data_list[0].data
            data_list = response["answer"]["data_list"]
        else:
            # Normal mode: response.data_list[0].data
            data_list = response["data_list"]
        
        # Check data_list is not empty
        if not data_list or len(data_list) == 0:
            return None
        
        # Extract first item
        first_item = data_list[0]
        
        # Check if data field exists and is not None
        if "data" not in first_item or first_item["data"] is None:
            return None
        
        return first_item["data"]
    
    except (KeyError, IndexError, TypeError):
        return None


def evaluate_case(
    case: DatasetCase,
    pred_response: Dict[str, Any],
    include_trace: bool = False
) -> EvaluationResult:
    """
    Evaluate a single case against prediction.
    
    Args:
        case: Dataset case with gold result
        pred_response: API response dict
        include_trace: Whether response is in debug mode
        
    Returns:
        EvaluationResult with match status and details
    """
    # Extract data_list length for multi_subquery tracking
    is_multi_subquery = False
    try:
        if "answer" in pred_response:
            data_list = pred_response["answer"]["data_list"]
        else:
            data_list = pred_response["data_list"]
        
        if len(data_list) > 1:
            is_multi_subquery = True
    except (KeyError, TypeError):
        pass
    
    # Extract pred table
    pred_table = extract_pred_table(pred_response, include_trace)
    
    # Handle extraction failures (scorable failures)
    if pred_table is None:
        # Determine specific reason
        try:
            if "answer" in pred_response:
                data_list = pred_response["answer"]["data_list"]
            else:
                data_list = pred_response["data_list"]
            
            if not data_list or len(data_list) == 0:
                return EvaluationResult(
                    case_id=case.case_id,
                    question=case.question,
                    match=False,
                    reason=ReasonCode.NO_RESULT,
                    detail="data_list is empty",
                    is_unscorable=False,
                    is_multi_subquery=is_multi_subquery
                )
            
            first_item = data_list[0]
            
            # Check for error field
            if "error" in first_item and first_item["error"] is not None:
                return EvaluationResult(
                    case_id=case.case_id,
                    question=case.question,
                    match=False,
                    reason=ReasonCode.EXECUTION_ERROR,
                    detail=str(first_item["error"]),
                    is_unscorable=False,
                    is_multi_subquery=is_multi_subquery
                )
            
            # data field is None
            return EvaluationResult(
                case_id=case.case_id,
                question=case.question,
                match=False,
                reason=ReasonCode.NO_DATA,
                detail="data field is None",
                is_unscorable=False,
                is_multi_subquery=is_multi_subquery
            )
        
        except Exception as e:
            return EvaluationResult(
                case_id=case.case_id,
                question=case.question,
                match=False,
                reason=ReasonCode.NO_DATA,
                detail=f"Failed to extract pred table: {str(e)}",
                is_unscorable=False,
                is_multi_subquery=is_multi_subquery
            )
    
    # Build gold table from case
    gold_table = {
        "columns": case.gold_result.columns,
        "rows": case.gold_result.rows,
        "is_truncated": case.gold_result.is_truncated or False
    }
    
    # Canonicalize both tables
    try:
        canonical_pred = canonicalize_table(pred_table)
        canonical_gold = canonicalize_table(gold_table)
    except Exception as e:
        return EvaluationResult(
            case_id=case.case_id,
            question=case.question,
            match=False,
            reason=ReasonCode.NO_DATA,
            detail=f"Canonicalization failed: {str(e)}",
            is_unscorable=False,
            is_multi_subquery=is_multi_subquery
        )
    
    # Apply stable sort if order_sensitive=False
    if not case.order_sensitive:
        canonical_pred = stable_sort_rows(canonical_pred)
        canonical_gold = stable_sort_rows(canonical_gold)
    
    # Compare tables
    comparison_result = compare_tables(canonical_pred, canonical_gold, case.order_sensitive)
    
    # Build evaluation result
    is_unscorable = (comparison_result["reason"] == "TRUNCATED_UNSCORABLE")
    
    return EvaluationResult(
        case_id=case.case_id,
        question=case.question,
        match=comparison_result["match"],
        reason=ReasonCode(comparison_result["reason"]),
        detail=comparison_result.get("detail"),
        is_unscorable=is_unscorable,
        is_multi_subquery=is_multi_subquery,
        pred_columns=canonical_pred["columns"],
        gold_columns=canonical_gold["columns"],
        pred_row_count=len(canonical_pred["rows"]),
        gold_row_count=len(canonical_gold["rows"])
    )


class EXEvaluator:
    """
    EX (Execution Accuracy) evaluator.
    
    Implements frozen caliber per ex_canonicalize_spec.md.
    """
    
    def __init__(self, tolerance: float = 1e-4):
        """
        Initialize evaluator.
        
        Args:
            tolerance: Numeric comparison tolerance (frozen at 1e-4)
        """
        self.tolerance = tolerance
        self.results: List[EvaluationResult] = []
    
    def evaluate_dataset(
        self,
        cases: List[DatasetCase],
        pred_responses: List[Dict[str, Any]],
        include_trace: bool = False
    ) -> SummaryReport:
        """
        Evaluate entire dataset.
        
        Args:
            cases: List of dataset cases
            pred_responses: List of API responses (same order as cases)
            include_trace: Whether responses are in debug mode
            
        Returns:
            SummaryReport with aggregated metrics
        """
        if len(cases) != len(pred_responses):
            raise ValueError(
                f"Mismatched lengths: {len(cases)} cases vs {len(pred_responses)} responses"
            )
        
        self.results = []
        
        for case, pred_response in zip(cases, pred_responses):
            result = evaluate_case(case, pred_response, include_trace)
            self.results.append(result)
        
        return self.generate_summary()
    
    def generate_summary(self) -> SummaryReport:
        """
        Generate summary report from evaluation results.
        
        Returns:
            SummaryReport with metrics and breakdowns
        """
        total_cases = len(self.results)
        unscorable_cases = sum(1 for r in self.results if r.is_unscorable)
        scorable_cases = total_cases - unscorable_cases
        exact_match_count = sum(1 for r in self.results if r.match and not r.is_unscorable)
        multi_subquery_cases = sum(1 for r in self.results if r.is_multi_subquery)
        
        # Calculate EX score
        ex_score = exact_match_count / scorable_cases if scorable_cases > 0 else 0.0
        
        # Build breakdowns
        unscorable_breakdown = {}
        failure_breakdown = {}
        
        for result in self.results:
            if result.is_unscorable:
                reason = result.reason.value
                unscorable_breakdown[reason] = unscorable_breakdown.get(reason, 0) + 1
            elif not result.match:
                reason = result.reason.value
                failure_breakdown[reason] = failure_breakdown.get(reason, 0) + 1
        
        return SummaryReport(
            total_cases=total_cases,
            scorable_cases=scorable_cases,
            unscorable_cases=unscorable_cases,
            exact_match_count=exact_match_count,
            ex_score=ex_score,
            multi_subquery_cases=multi_subquery_cases,
            unscorable_breakdown=unscorable_breakdown,
            failure_breakdown=failure_breakdown,
            config={
                "tolerance": self.tolerance,
                "null_marker": "<NULL>",
                "inf_markers": ["<INF>", "<-INF>"]
            }
        )

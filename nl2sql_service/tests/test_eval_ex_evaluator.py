"""
【简述】
验证 EX 评测的 evaluator 模块能正确提取 pred 表、评估单个用例、生成汇总报告。

【范围/不测什么】
- 不测试真实 API 调用；使用模拟响应数据。
- 不测试 canonicalize 和 compare 细节；假设这些模块已正确实现。

【用例概述】
- test_extract_pred_table_normal_mode:
  -- 验证正常模式（include_trace=False）提取 pred 表。
- test_extract_pred_table_debug_mode:
  -- 验证调试模式（include_trace=True）提取 pred 表。
- test_extract_pred_table_no_result:
  -- 验证 data_list 为空时返回 None。
- test_extract_pred_table_no_data:
  -- 验证 data 字段为 None 时返回 None。
- test_evaluate_case_exact_match:
  -- 验证完全匹配的用例返回 EXACT_MATCH。
- test_evaluate_case_execution_error:
  -- 验证执行错误返回 EXECUTION_ERROR（可评分失败）。
- test_evaluate_case_no_result:
  -- 验证 data_list 为空返回 NO_RESULT（可评分失败）。
- test_evaluate_case_no_data:
  -- 验证 data 为 None 返回 NO_DATA（可评分失败）。
- test_evaluate_case_truncated_unscorable:
  -- 验证截断结果返回 TRUNCATED_UNSCORABLE（不可评分）。
- test_evaluate_case_multi_subquery:
  -- 验证多子查询标记 is_multi_subquery=True。
- test_evaluator_generate_summary:
  -- 验证汇总报告计算 EX 分数和分解统计。
"""

import pytest

from evaluation.ex.evaluator import extract_pred_table, evaluate_case, EXEvaluator
from evaluation.ex.schema import DatasetCase, GoldResult, ReasonCode


@pytest.mark.unit
def test_extract_pred_table_normal_mode():
    """
    【测试目标】
    1. 验证正常模式（include_trace=False）提取 pred 表。

    【执行过程】
    1. 构造正常模式响应（response.data_list[0].data）。
    2. 调用 extract_pred_table 提取表。
    3. 检查返回的表结构。

    【预期结果】
    1. 返回非 None 的表字典。
    2. 表包含 columns、rows、is_truncated 字段。
    """
    response = {
        "data_list": [
            {
                "sub_query_id": "sq1",
                "title": "Test Query",
                "data": {
                    "columns": ["id", "name"],
                    "rows": [[1, "Alice"]],
                    "is_truncated": False
                },
                "error": None
            }
        ]
    }
    
    table = extract_pred_table(response, include_trace=False)
    
    assert table is not None
    assert table["columns"] == ["id", "name"]
    assert table["rows"] == [[1, "Alice"]]
    assert table["is_truncated"] is False


@pytest.mark.unit
def test_extract_pred_table_debug_mode():
    """
    【测试目标】
    1. 验证调试模式（include_trace=True）提取 pred 表。

    【执行过程】
    1. 构造调试模式响应（response.answer.data_list[0].data）。
    2. 调用 extract_pred_table 提取表。
    3. 检查返回的表结构。

    【预期结果】
    1. 返回非 None 的表字典。
    2. 表包含 columns、rows、is_truncated 字段。
    """
    response = {
        "answer": {
            "data_list": [
                {
                    "sub_query_id": "sq1",
                    "title": "Test Query",
                    "data": {
                        "columns": ["id", "name"],
                        "rows": [[1, "Alice"]],
                        "is_truncated": False
                    },
                    "error": None
                }
            ]
        },
        "debug_info": {}
    }
    
    table = extract_pred_table(response, include_trace=True)
    
    assert table is not None
    assert table["columns"] == ["id", "name"]


@pytest.mark.unit
def test_extract_pred_table_no_result():
    """
    【测试目标】
    1. 验证 data_list 为空时返回 None。

    【执行过程】
    1. 构造 data_list 为空的响应。
    2. 调用 extract_pred_table 提取表。
    3. 检查返回值为 None。

    【预期结果】
    1. 返回 None。
    """
    response = {
        "data_list": []
    }
    
    table = extract_pred_table(response, include_trace=False)
    
    assert table is None


@pytest.mark.unit
def test_extract_pred_table_no_data():
    """
    【测试目标】
    1. 验证 data 字段为 None 时返回 None。

    【执行过程】
    1. 构造 data 为 None 的响应。
    2. 调用 extract_pred_table 提取表。
    3. 检查返回值为 None。

    【预期结果】
    1. 返回 None。
    """
    response = {
        "data_list": [
            {
                "sub_query_id": "sq1",
                "title": "Test Query",
                "data": None,
                "error": "Execution failed"
            }
        ]
    }
    
    table = extract_pred_table(response, include_trace=False)
    
    assert table is None


@pytest.mark.unit
def test_evaluate_case_exact_match():
    """
    【测试目标】
    1. 验证完全匹配的用例返回 EXACT_MATCH。

    【执行过程】
    1. 构造 gold case 和匹配的 pred response。
    2. 调用 evaluate_case 评估。
    3. 检查返回结果的 match 和 reason 字段。

    【预期结果】
    1. match 为 True。
    2. reason 为 EXACT_MATCH。
    3. is_unscorable 为 False。
    """
    case = DatasetCase(
        case_id="case1",
        question="Test question",
        expected_outcome="Test outcome",
        order_sensitive=False,
        gold_result=GoldResult(
            columns=["id", "name"],
            rows=[[1, "Alice"], [2, "Bob"]]
        )
    )
    
    pred_response = {
        "data_list": [
            {
                "data": {
                    "columns": ["ID", "Name"],
                    "rows": [[1, "alice"], [2, "bob"]],
                    "is_truncated": False
                }
            }
        ]
    }
    
    result = evaluate_case(case, pred_response)
    
    assert result.match is True
    assert result.reason == ReasonCode.EXACT_MATCH
    assert result.is_unscorable is False


@pytest.mark.unit
def test_evaluate_case_execution_error():
    """
    【测试目标】
    1. 验证执行错误返回 EXECUTION_ERROR（可评分失败）。

    【执行过程】
    1. 构造包含 error 字段的 pred response。
    2. 调用 evaluate_case 评估。
    3. 检查返回结果的 reason 和 is_unscorable 字段。

    【预期结果】
    1. match 为 False。
    2. reason 为 EXECUTION_ERROR。
    3. is_unscorable 为 False（可评分失败）。
    """
    case = DatasetCase(
        case_id="case1",
        question="Test question",
        expected_outcome="Test outcome",
        order_sensitive=False,
        gold_result=GoldResult(columns=["id"], rows=[[1]])
    )
    
    pred_response = {
        "data_list": [
            {
                "data": None,
                "error": "Permission denied"
            }
        ]
    }
    
    result = evaluate_case(case, pred_response)
    
    assert result.match is False
    assert result.reason == ReasonCode.EXECUTION_ERROR
    assert result.is_unscorable is False
    assert "Permission denied" in result.detail


@pytest.mark.unit
def test_evaluate_case_no_result():
    """
    【测试目标】
    1. 验证 data_list 为空返回 NO_RESULT（可评分失败）。

    【执行过程】
    1. 构造 data_list 为空的 pred response。
    2. 调用 evaluate_case 评估。
    3. 检查返回结果的 reason 和 is_unscorable 字段。

    【预期结果】
    1. match 为 False。
    2. reason 为 NO_RESULT。
    3. is_unscorable 为 False（可评分失败）。
    """
    case = DatasetCase(
        case_id="case1",
        question="Test question",
        expected_outcome="Test outcome",
        order_sensitive=False,
        gold_result=GoldResult(columns=["id"], rows=[[1]])
    )
    
    pred_response = {
        "data_list": []
    }
    
    result = evaluate_case(case, pred_response)
    
    assert result.match is False
    assert result.reason == ReasonCode.NO_RESULT
    assert result.is_unscorable is False


@pytest.mark.unit
def test_evaluate_case_no_data():
    """
    【测试目标】
    1. 验证 data 为 None 返回 NO_DATA（可评分失败）。

    【执行过程】
    1. 构造 data 为 None 的 pred response。
    2. 调用 evaluate_case 评估。
    3. 检查返回结果的 reason 和 is_unscorable 字段。

    【预期结果】
    1. match 为 False。
    2. reason 为 NO_DATA。
    3. is_unscorable 为 False（可评分失败）。
    """
    case = DatasetCase(
        case_id="case1",
        question="Test question",
        expected_outcome="Test outcome",
        order_sensitive=False,
        gold_result=GoldResult(columns=["id"], rows=[[1]])
    )
    
    pred_response = {
        "data_list": [
            {
                "data": None,
                "error": None
            }
        ]
    }
    
    result = evaluate_case(case, pred_response)
    
    assert result.match is False
    assert result.reason == ReasonCode.NO_DATA
    assert result.is_unscorable is False


@pytest.mark.unit
def test_evaluate_case_truncated_unscorable():
    """
    【测试目标】
    1. 验证截断结果返回 TRUNCATED_UNSCORABLE（不可评分）。

    【执行过程】
    1. 构造 is_truncated=True 的 pred response。
    2. 调用 evaluate_case 评估。
    3. 检查返回结果的 reason 和 is_unscorable 字段。

    【预期结果】
    1. match 为 False。
    2. reason 为 TRUNCATED_UNSCORABLE。
    3. is_unscorable 为 True（不可评分）。
    """
    case = DatasetCase(
        case_id="case1",
        question="Test question",
        expected_outcome="Test outcome",
        order_sensitive=False,
        gold_result=GoldResult(columns=["id"], rows=[[1]])
    )
    
    pred_response = {
        "data_list": [
            {
                "data": {
                    "columns": ["id"],
                    "rows": [[1]],
                    "is_truncated": True
                }
            }
        ]
    }
    
    result = evaluate_case(case, pred_response)
    
    assert result.match is False
    assert result.reason == ReasonCode.TRUNCATED_UNSCORABLE
    assert result.is_unscorable is True


@pytest.mark.unit
def test_evaluate_case_multi_subquery():
    """
    【测试目标】
    1. 验证多子查询标记 is_multi_subquery=True。

    【执行过程】
    1. 构造 data_list 长度 > 1 的 pred response。
    2. 调用 evaluate_case 评估。
    3. 检查返回结果的 is_multi_subquery 字段。

    【预期结果】
    1. is_multi_subquery 为 True。
    2. 仍只评估 data_list[0]。
    """
    case = DatasetCase(
        case_id="case1",
        question="Test question",
        expected_outcome="Test outcome",
        order_sensitive=False,
        gold_result=GoldResult(columns=["id"], rows=[[1]])
    )
    
    pred_response = {
        "data_list": [
            {
                "data": {
                    "columns": ["id"],
                    "rows": [[1]],
                    "is_truncated": False
                }
            },
            {
                "data": {
                    "columns": ["name"],
                    "rows": [["Alice"]],
                    "is_truncated": False
                }
            }
        ]
    }
    
    result = evaluate_case(case, pred_response)
    
    assert result.is_multi_subquery is True
    assert result.match is True


@pytest.mark.unit
def test_evaluator_generate_summary():
    """
    【测试目标】
    1. 验证汇总报告计算 EX 分数和分解统计。

    【执行过程】
    1. 构造多个 dataset cases 和 pred responses。
    2. 调用 EXEvaluator.evaluate_dataset 评估。
    3. 检查返回的 SummaryReport 各字段。

    【预期结果】
    1. total_cases 正确。
    2. scorable_cases 正确（排除 UNSCORABLE）。
    3. ex_score 正确（exact_match / scorable）。
    4. unscorable_breakdown 和 failure_breakdown 正确。
    5. multi_subquery_cases 正确。
    """
    cases = [
        DatasetCase(
            case_id="case1",
            question="Q1",
            expected_outcome="O1",
            order_sensitive=False,
            gold_result=GoldResult(columns=["id"], rows=[[1]])
        ),
        DatasetCase(
            case_id="case2",
            question="Q2",
            expected_outcome="O2",
            order_sensitive=False,
            gold_result=GoldResult(columns=["id"], rows=[[2]])
        ),
        DatasetCase(
            case_id="case3",
            question="Q3",
            expected_outcome="O3",
            order_sensitive=False,
            gold_result=GoldResult(columns=["id"], rows=[[3]])
        ),
    ]
    
    pred_responses = [
        {
            "data_list": [
                {"data": {"columns": ["id"], "rows": [[1]], "is_truncated": False}}
            ]
        },
        {
            "data_list": [
                {"data": {"columns": ["id"], "rows": [[2]], "is_truncated": True}}
            ]
        },
        {
            "data_list": [
                {"data": None, "error": "Failed"}
            ]
        },
    ]
    
    evaluator = EXEvaluator()
    summary = evaluator.evaluate_dataset(cases, pred_responses)
    
    assert summary.total_cases == 3
    assert summary.unscorable_cases == 1
    assert summary.scorable_cases == 2
    assert summary.exact_match_count == 1
    assert summary.ex_score == 0.5
    assert summary.unscorable_breakdown["TRUNCATED_UNSCORABLE"] == 1
    assert summary.failure_breakdown["EXECUTION_ERROR"] == 1

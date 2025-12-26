"""
【简述】
验证 EX 评测的 compare 模块能按 ex_canonicalize_spec.md 规范正确比较表格与单元格。

【范围/不测什么】
- 不测试 canonicalize 逻辑；假设输入已是 canonical 形式。
- 不测试真实数据库或网络调用。

【用例概述】
- test_cell_compare_exact_match:
  -- 验证完全相同的单元格返回 True。
- test_cell_compare_numeric_tolerance:
  -- 验证数值在 1e-4 容差内返回 True。
- test_cell_compare_numeric_exceeds_tolerance:
  -- 验证数值超出 1e-4 容差返回 False。
- test_cell_compare_special_markers:
  -- 验证特殊标记（<NULL>/<INF>/<-INF>）精确匹配。
- test_cell_compare_text_mismatch:
  -- 验证文本不匹配返回 False。
- test_compare_tables_exact_match:
  -- 验证完全匹配的表返回 EXACT_MATCH。
- test_compare_tables_truncated:
  -- 验证截断表返回 TRUNCATED_UNSCORABLE。
- test_compare_tables_column_mismatch:
  -- 验证列不匹配返回 COLUMN_MISMATCH。
- test_compare_tables_shape_mismatch:
  -- 验证行数不匹配返回 SHAPE_MISMATCH。
- test_compare_tables_value_mismatch:
  -- 验证单元格值不匹配返回 VALUE_MISMATCH。
- test_compare_tables_empty_both:
  -- 验证双方都为空表返回 EXACT_MATCH。
"""

import pytest

from evaluation.ex.compare import cell_compare, compare_tables


@pytest.mark.unit
def test_cell_compare_exact_match():
    """
    【测试目标】
    1. 验证完全相同的单元格返回 True。

    【执行过程】
    1. 调用 cell_compare 比较相同字符串。
    2. 检查返回值是否为 True。

    【预期结果】
    1. "42.0" vs "42.0" → True
    2. "hello" vs "hello" → True
    3. "<NULL>" vs "<NULL>" → True
    """
    assert cell_compare("42.0", "42.0") is True
    assert cell_compare("hello", "hello") is True
    assert cell_compare("<NULL>", "<NULL>") is True


@pytest.mark.unit
def test_cell_compare_numeric_tolerance():
    """
    【测试目标】
    1. 验证数值在 1e-4 容差内返回 True。

    【执行过程】
    1. 调用 cell_compare 比较容差内的数值。
    2. 检查返回值是否为 True。

    【预期结果】
    1. "42.0" vs "42.00001" → True (差值 0.00001 < 1e-4)
    2. "0.00001" vs "0.00002" → True (差值 0.00001 < 1e-4)
    """
    assert cell_compare("42.0", "42.00001") is True
    assert cell_compare("0.00001", "0.00002") is True


@pytest.mark.unit
def test_cell_compare_numeric_exceeds_tolerance():
    """
    【测试目标】
    1. 验证数值超出 1e-4 容差返回 False。

    【执行过程】
    1. 调用 cell_compare 比较超出容差的数值。
    2. 检查返回值是否为 False。

    【预期结果】
    1. "42.0" vs "42.1" → False (差值 0.1 > 1e-4)
    2. "1000000.0" vs "1000000.1" → False (差值 0.1 > 1e-4)
    """
    assert cell_compare("42.0", "42.1") is False
    assert cell_compare("1000000.0", "1000000.1") is False


@pytest.mark.unit
def test_cell_compare_special_markers():
    """
    【测试目标】
    1. 验证特殊标记（<NULL>/<INF>/<-INF>）精确匹配。

    【执行过程】
    1. 调用 cell_compare 比较特殊标记。
    2. 检查不同标记返回 False，相同标记返回 True。

    【预期结果】
    1. "<NULL>" vs "0.0" → False
    2. "<INF>" vs "999999" → False
    3. "<NULL>" vs "<NULL>" → True
    """
    assert cell_compare("<NULL>", "0.0") is False
    assert cell_compare("<INF>", "999999") is False
    assert cell_compare("<NULL>", "<NULL>") is True


@pytest.mark.unit
def test_cell_compare_text_mismatch():
    """
    【测试目标】
    1. 验证文本不匹配返回 False。

    【执行过程】
    1. 调用 cell_compare 比较不同文本。
    2. 检查返回值是否为 False。

    【预期结果】
    1. "hello" vs "world" → False
    2. "alice" vs "bob" → False
    """
    assert cell_compare("hello", "world") is False
    assert cell_compare("alice", "bob") is False


@pytest.mark.unit
def test_compare_tables_exact_match():
    """
    【测试目标】
    1. 验证完全匹配的表返回 EXACT_MATCH。

    【执行过程】
    1. 调用 compare_tables 比较相同的表。
    2. 检查返回结果的 match 和 reason 字段。

    【预期结果】
    1. match 为 True。
    2. reason 为 "EXACT_MATCH"。
    """
    pred_table = {
        "columns": ["id", "name"],
        "rows": [["1.0", "alice"], ["2.0", "bob"]],
        "is_truncated": False
    }
    gold_table = {
        "columns": ["id", "name"],
        "rows": [["1.0", "alice"], ["2.0", "bob"]],
        "is_truncated": False
    }
    
    result = compare_tables(pred_table, gold_table)
    
    assert result["match"] is True
    assert result["reason"] == "EXACT_MATCH"


@pytest.mark.unit
def test_compare_tables_truncated():
    """
    【测试目标】
    1. 验证截断表返回 TRUNCATED_UNSCORABLE。

    【执行过程】
    1. 调用 compare_tables 比较包含截断标记的表。
    2. 检查返回结果的 match 和 reason 字段。

    【预期结果】
    1. match 为 False。
    2. reason 为 "TRUNCATED_UNSCORABLE"。
    3. detail 包含截断信息。
    """
    pred_table = {
        "columns": ["id"],
        "rows": [["1.0"]],
        "is_truncated": True
    }
    gold_table = {
        "columns": ["id"],
        "rows": [["1.0"]],
        "is_truncated": False
    }
    
    result = compare_tables(pred_table, gold_table)
    
    assert result["match"] is False
    assert result["reason"] == "TRUNCATED_UNSCORABLE"
    assert "pred_truncated=True" in result["detail"]


@pytest.mark.unit
def test_compare_tables_column_mismatch():
    """
    【测试目标】
    1. 验证列不匹配返回 COLUMN_MISMATCH。

    【执行过程】
    1. 调用 compare_tables 比较列不同的表。
    2. 检查返回结果的 match 和 reason 字段。

    【预期结果】
    1. match 为 False。
    2. reason 为 "COLUMN_MISMATCH"。
    3. detail 包含列信息。
    """
    pred_table = {
        "columns": ["id", "name"],
        "rows": [["1.0", "alice"]],
        "is_truncated": False
    }
    gold_table = {
        "columns": ["name", "id"],
        "rows": [["alice", "1.0"]],
        "is_truncated": False
    }
    
    result = compare_tables(pred_table, gold_table)
    
    assert result["match"] is False
    assert result["reason"] == "COLUMN_MISMATCH"
    assert "pred_columns" in result["detail"]


@pytest.mark.unit
def test_compare_tables_shape_mismatch():
    """
    【测试目标】
    1. 验证行数不匹配返回 SHAPE_MISMATCH。

    【执行过程】
    1. 调用 compare_tables 比较行数不同的表。
    2. 检查返回结果的 match 和 reason 字段。

    【预期结果】
    1. match 为 False。
    2. reason 为 "SHAPE_MISMATCH"。
    3. detail 包含形状信息。
    """
    pred_table = {
        "columns": ["id"],
        "rows": [["1.0"], ["2.0"]],
        "is_truncated": False
    }
    gold_table = {
        "columns": ["id"],
        "rows": [["1.0"]],
        "is_truncated": False
    }
    
    result = compare_tables(pred_table, gold_table)
    
    assert result["match"] is False
    assert result["reason"] == "SHAPE_MISMATCH"
    assert "pred_shape=(2, 1)" in result["detail"]
    assert "gold_shape=(1, 1)" in result["detail"]


@pytest.mark.unit
def test_compare_tables_value_mismatch():
    """
    【测试目标】
    1. 验证单元格值不匹配返回 VALUE_MISMATCH。

    【执行过程】
    1. 调用 compare_tables 比较单元格值不同的表。
    2. 检查返回结果的 match 和 reason 字段。

    【预期结果】
    1. match 为 False。
    2. reason 为 "VALUE_MISMATCH"。
    3. detail 包含行列位置和值信息。
    """
    pred_table = {
        "columns": ["id", "gmv"],
        "rows": [["1.0", "100.0"], ["2.0", "200.5"]],
        "is_truncated": False
    }
    gold_table = {
        "columns": ["id", "gmv"],
        "rows": [["1.0", "100.0"], ["2.0", "300.0"]],
        "is_truncated": False
    }
    
    result = compare_tables(pred_table, gold_table)
    
    assert result["match"] is False
    assert result["reason"] == "VALUE_MISMATCH"
    assert "row=1" in result["detail"]
    assert "col='gmv'" in result["detail"]
    assert "pred='200.5'" in result["detail"]
    assert "gold='300.0'" in result["detail"]


@pytest.mark.unit
def test_compare_tables_empty_both():
    """
    【测试目标】
    1. 验证双方都为空表返回 EXACT_MATCH。

    【执行过程】
    1. 调用 compare_tables 比较两个空表。
    2. 检查返回结果的 match 和 reason 字段。

    【预期结果】
    1. match 为 True。
    2. reason 为 "EXACT_MATCH"。
    """
    pred_table = {
        "columns": [],
        "rows": [],
        "is_truncated": False
    }
    gold_table = {
        "columns": [],
        "rows": [],
        "is_truncated": False
    }
    
    result = compare_tables(pred_table, gold_table)
    
    assert result["match"] is True
    assert result["reason"] == "EXACT_MATCH"

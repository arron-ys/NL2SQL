"""
【简述】
验证 EX 评测的 canonicalize 模块能按 ex_canonicalize_spec.md 规范正确转换各类型单元格值与列名。

【范围/不测什么】
- 不测试真实数据库或网络调用；仅测试纯函数转换逻辑。
- 不测试完整评测流程；仅测试 canonicalize 层。

【用例概述】
- test_canonicalize_column_basic:
  -- 验证列名 lower + strip 转换。
- test_canonicalize_cell_none_and_nan:
  -- 验证 None 和 NaN 统一转为 <NULL> 标记。
- test_canonicalize_cell_numeric_types:
  -- 验证 int/float/Decimal/numeric_string 转为 float 字符串。
- test_canonicalize_cell_infinity:
  -- 验证正负无穷转为 <INF>/<-INF> 标记。
- test_canonicalize_cell_boolean:
  -- 验证布尔值转为 "1"/"0"。
- test_canonicalize_cell_text:
  -- 验证文本 lower + strip，保留内部空格。
- test_canonicalize_cell_datetime:
  -- 验证 datetime/date 对象转 ISO8601 格式。
- test_canonicalize_cell_datetime_string:
  -- 验证 datetime 字符串解析并转 ISO8601。
- test_canonicalize_cell_dict_list:
  -- 验证 dict 稳定 JSON（sort_keys），list 保序 JSON。
- test_canonicalize_cell_bytes:
  -- 验证 bytes 解码后应用文本规则。
- test_validate_table_structure_valid:
  -- 验证合法表结构通过校验。
- test_validate_table_structure_invalid:
  -- 验证非法表结构抛出异常。
- test_canonicalize_table_full:
  -- 验证完整表转换（列名+单元格）。
- test_stable_sort_rows:
  -- 验证稳定排序按字典序排列行。
"""

import pytest
import math
from datetime import datetime, date
from decimal import Decimal

from evaluation.ex.canonicalize import (
    canonicalize_column,
    canonicalize_cell,
    validate_table_structure,
    canonicalize_table,
    stable_sort_rows,
)


@pytest.mark.unit
def test_canonicalize_column_basic():
    """
    【测试目标】
    1. 验证列名 lower + strip 转换规则。

    【执行过程】
    1. 调用 canonicalize_column 处理各种列名。
    2. 检查输出是否符合 lower + strip 规则。

    【预期结果】
    1. "GMV_Total" → "gmv_total"
    2. "  OrderID  " → "orderid"
    3. "REGION" → "region"
    """
    assert canonicalize_column("GMV_Total") == "gmv_total"
    assert canonicalize_column("  OrderID  ") == "orderid"
    assert canonicalize_column("REGION") == "region"


@pytest.mark.unit
def test_canonicalize_cell_none_and_nan():
    """
    【测试目标】
    1. 验证 None 和 NaN 统一转为 <NULL> 标记。

    【执行过程】
    1. 调用 canonicalize_cell 处理 None 和 float('nan')。
    2. 检查输出是否为 "<NULL>"。

    【预期结果】
    1. None → "<NULL>"
    2. float('nan') → "<NULL>"
    """
    assert canonicalize_cell(None) == "<NULL>"
    assert canonicalize_cell(float('nan')) == "<NULL>"


@pytest.mark.unit
def test_canonicalize_cell_numeric_types():
    """
    【测试目标】
    1. 验证 int/float/Decimal/numeric_string 转为 float 字符串。

    【执行过程】
    1. 调用 canonicalize_cell 处理各种数值类型。
    2. 检查输出是否为 float 字符串形式。

    【预期结果】
    1. 42 → "42.0"
    2. 3.14 → "3.14"
    3. Decimal("123.45") → "123.45"
    4. "456" → "456.0"
    5. "45.67" → "45.67"
    """
    assert canonicalize_cell(42) == "42.0"
    assert canonicalize_cell(3.14) == "3.14"
    assert canonicalize_cell(Decimal("123.45")) == "123.45"
    assert canonicalize_cell("456") == "456.0"
    assert canonicalize_cell("45.67") == "45.67"


@pytest.mark.unit
def test_canonicalize_cell_infinity():
    """
    【测试目标】
    1. 验证正负无穷转为 <INF>/<-INF> 标记。

    【执行过程】
    1. 调用 canonicalize_cell 处理 float('inf') 和 float('-inf')。
    2. 检查输出是否为对应标记。

    【预期结果】
    1. float('inf') → "<INF>"
    2. float('-inf') → "<-INF>"
    """
    assert canonicalize_cell(float('inf')) == "<INF>"
    assert canonicalize_cell(float('-inf')) == "<-INF>"


@pytest.mark.unit
def test_canonicalize_cell_boolean():
    """
    【测试目标】
    1. 验证布尔值转为 "1"/"0"。

    【执行过程】
    1. 调用 canonicalize_cell 处理 True 和 False。
    2. 检查输出是否为 "1" 或 "0"。

    【预期结果】
    1. True → "1"
    2. False → "0"
    """
    assert canonicalize_cell(True) == "1"
    assert canonicalize_cell(False) == "0"


@pytest.mark.unit
def test_canonicalize_cell_text():
    """
    【测试目标】
    1. 验证文本 lower + strip，保留内部空格。

    【执行过程】
    1. 调用 canonicalize_cell 处理各种文本。
    2. 检查输出是否符合 lower + strip 规则。

    【预期结果】
    1. "  Hello  " → "hello"
    2. "WORLD" → "world"
    3. "Hello  World" → "hello  world" (内部空格保留)
    4. "  " → "" (空白字符串 strip 后为空)
    """
    assert canonicalize_cell("  Hello  ") == "hello"
    assert canonicalize_cell("WORLD") == "world"
    assert canonicalize_cell("Hello  World") == "hello  world"
    assert canonicalize_cell("  ") == ""


@pytest.mark.unit
def test_canonicalize_cell_datetime():
    """
    【测试目标】
    1. 验证 datetime/date 对象转 ISO8601 格式。

    【执行过程】
    1. 调用 canonicalize_cell 处理 datetime 和 date 对象。
    2. 检查输出是否为 ISO8601 字符串。

    【预期结果】
    1. datetime(2024, 1, 15, 10, 30, 0) → "2024-01-15T10:30:00"
    2. date(2024, 1, 15) → "2024-01-15"
    """
    assert canonicalize_cell(datetime(2024, 1, 15, 10, 30, 0)) == "2024-01-15T10:30:00"
    assert canonicalize_cell(date(2024, 1, 15)) == "2024-01-15"


@pytest.mark.unit
def test_canonicalize_cell_datetime_string():
    """
    【测试目标】
    1. 验证 datetime 字符串解析并转 ISO8601。

    【执行过程】
    1. 调用 canonicalize_cell 处理各种 datetime 字符串格式。
    2. 检查输出是否为统一的 ISO8601 格式。

    【预期结果】
    1. "2024-01-15 10:30:00" → "2024-01-15T10:30:00"
    2. "2024-01-15" → "2024-01-15T00:00:00" (补零时间)
    """
    assert canonicalize_cell("2024-01-15 10:30:00") == "2024-01-15T10:30:00"
    assert canonicalize_cell("2024-01-15") == "2024-01-15T00:00:00"


@pytest.mark.unit
def test_canonicalize_cell_dict_list():
    """
    【测试目标】
    1. 验证 dict 稳定 JSON（sort_keys），list 保序 JSON。

    【执行过程】
    1. 调用 canonicalize_cell 处理 dict 和 list。
    2. 检查输出是否为稳定 JSON 字符串。

    【预期结果】
    1. {"b": 2, "a": 1} → '{"a":1,"b":2}' (键排序)
    2. [3, 1, 2] → '[3,1,2]' (保序)
    3. {"name": "张三"} → '{"name":"张三"}' (Unicode 保留)
    """
    assert canonicalize_cell({"b": 2, "a": 1}) == '{"a":1,"b":2}'
    assert canonicalize_cell([3, 1, 2]) == '[3,1,2]'
    assert canonicalize_cell({"name": "张三"}) == '{"name":"张三"}'


@pytest.mark.unit
def test_canonicalize_cell_bytes():
    """
    【测试目标】
    1. 验证 bytes 解码后应用文本规则。

    【执行过程】
    1. 调用 canonicalize_cell 处理 bytes。
    2. 检查输出是否为解码后的 lower + strip 文本。

    【预期结果】
    1. b"hello" → "hello"
    2. b"WORLD" → "world"
    """
    assert canonicalize_cell(b"hello") == "hello"
    assert canonicalize_cell(b"WORLD") == "world"


@pytest.mark.unit
def test_validate_table_structure_valid():
    """
    【测试目标】
    1. 验证合法表结构通过校验。

    【执行过程】
    1. 调用 validate_table_structure 处理合法表。
    2. 检查不抛出异常。

    【预期结果】
    1. 正常表结构不抛出异常。
    2. 空表（columns=[], rows=[]）不抛出异常。
    """
    valid_table = {
        "columns": ["col1", "col2"],
        "rows": [[1, 2], [3, 4]]
    }
    validate_table_structure(valid_table)
    
    empty_table = {
        "columns": [],
        "rows": []
    }
    validate_table_structure(empty_table)


@pytest.mark.unit
def test_validate_table_structure_invalid():
    """
    【测试目标】
    1. 验证非法表结构抛出 ValueError。

    【执行过程】
    1. 调用 validate_table_structure 处理各种非法表。
    2. 检查是否抛出 ValueError。

    【预期结果】
    1. 缺失 columns 字段抛出异常。
    2. 缺失 rows 字段抛出异常。
    3. 行列数不匹配抛出异常。
    """
    with pytest.raises(ValueError, match="missing 'columns'"):
        validate_table_structure({"rows": []})
    
    with pytest.raises(ValueError, match="missing 'rows'"):
        validate_table_structure({"columns": []})
    
    with pytest.raises(ValueError, match="expected 2"):
        validate_table_structure({
            "columns": ["col1", "col2"],
            "rows": [[1, 2, 3]]
        })


@pytest.mark.unit
def test_canonicalize_table_full():
    """
    【测试目标】
    1. 验证完整表转换（列名+单元格）。

    【执行过程】
    1. 调用 canonicalize_table 处理包含多种类型的表。
    2. 检查列名和单元格是否正确转换。

    【预期结果】
    1. 列名转为 lowercase。
    2. 单元格按类型规则转换。
    3. is_truncated 字段保留。
    """
    table = {
        "columns": ["ID", "Name", "GMV"],
        "rows": [
            [1, "Alice", 100.5],
            [2, "Bob", 200.0]
        ],
        "is_truncated": False
    }
    
    result = canonicalize_table(table)
    
    assert result["columns"] == ["id", "name", "gmv"]
    assert result["rows"] == [
        ["1.0", "alice", "100.5"],
        ["2.0", "bob", "200.0"]
    ]
    assert result["is_truncated"] is False


@pytest.mark.unit
def test_stable_sort_rows():
    """
    【测试目标】
    1. 验证稳定排序按字典序排列行。

    【执行过程】
    1. 调用 stable_sort_rows 处理乱序表。
    2. 检查行是否按字典序排序。

    【预期结果】
    1. 行按第一列排序，相同时按第二列排序。
    2. 空表不报错。
    """
    table = {
        "columns": ["col1", "col2"],
        "rows": [
            ["b", "2"],
            ["a", "1"],
            ["b", "1"]
        ]
    }
    
    result = stable_sort_rows(table)
    
    assert result["rows"] == [
        ["a", "1"],
        ["b", "1"],
        ["b", "2"]
    ]
    
    empty_table = {"columns": [], "rows": []}
    result_empty = stable_sort_rows(empty_table)
    assert result_empty["rows"] == []

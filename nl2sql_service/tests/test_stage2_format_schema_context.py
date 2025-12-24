"""
【简述】
验证 Stage2 Plan 生成中 _format_schema_context 函数的格式化行为，包括排序、字段输出、截断、时间标记和枚举透出逻辑。

【范围/不测什么】
- 不覆盖完整 Plan 生成流程；仅验证 _format_schema_context 的格式化逻辑。

【用例概述】
- test_format_schema_context_term_id_sorting:
  -- 验证 term_id 字母序排序稳定
- test_format_schema_context_aliases_empty_not_displayed:
  -- 验证 aliases 为空时不出现 Aliases 字段
- test_format_schema_context_description_truncate_50:
  -- 验证 desc 超过 50 被截断
- test_format_schema_context_is_time_dimension_marker:
  -- 验证 is_time_dimension=true 时出现 Is_Time: True
- test_format_schema_context_enum_values_threshold_50:
  -- 验证 enum_values 长度 50 与 51 的分界行为：<=50 输出 Values；>50 不输出 Values
"""

import pytest
from unittest.mock import MagicMock

from core.semantic_registry import SemanticRegistry


@pytest.mark.unit
def test_format_schema_context_term_id_sorting():
    """
    【测试目标】
    1. 验证 term_id 字母序排序稳定

    【执行过程】
    1. 创建包含乱序 term_ids 的列表
    2. 调用 _format_schema_context
    3. 检查输出中 term_ids 按字母序排列

    【预期结果】
    1. Metrics 和 Dimensions 两组内，term_ids 按字母序排列
    """
    from stages.stage2_plan_generation import _format_schema_context
    
    # 创建 mock registry
    mock_registry = MagicMock(spec=SemanticRegistry)
    
    # 乱序的 term_ids
    terms = ["METRIC_Z", "METRIC_A", "METRIC_M", "DIM_B", "DIM_Z", "DIM_A"]
    
    # Mock get_term 返回基本定义
    def mock_get_term(term_id):
        return {
            "id": term_id,
            "name": f"Name_{term_id}",
            "aliases": [],
            "description": ""
        }
    
    mock_registry.get_term.side_effect = mock_get_term
    
    # 调用函数
    result = _format_schema_context(terms, mock_registry)
    
    # 验证排序：提取所有 ID 行
    lines = result.split("\n")
    metric_ids = []
    dim_ids = []
    in_metrics = False
    in_dimensions = False
    
    for line in lines:
        if line == "[METRICS]":
            in_metrics = True
            in_dimensions = False
            continue
        if line == "[DIMENSIONS]":
            in_metrics = False
            in_dimensions = True
            continue
        if line.startswith("ID:") and in_metrics:
            metric_ids.append(line.split(" | ")[0].replace("ID: ", ""))
        if line.startswith("ID:") and in_dimensions:
            dim_ids.append(line.split(" | ")[0].replace("ID: ", ""))
    
    # 验证字母序
    assert metric_ids == sorted(metric_ids), "Metrics 应该按字母序排列"
    assert dim_ids == sorted(dim_ids), "Dimensions 应该按字母序排列"


@pytest.mark.unit
def test_format_schema_context_aliases_empty_not_displayed():
    """
    【测试目标】
    1. 验证 aliases 为空时不出现 Aliases 字段

    【执行过程】
    1. 创建 term_def，aliases 为空列表
    2. 调用 _format_schema_context
    3. 检查输出中不包含 "| Aliases:" 字段

    【预期结果】
    1. 输出中不包含 "| Aliases:" 字段（当 aliases 为空时）
    """
    from stages.stage2_plan_generation import _format_schema_context
    
    mock_registry = MagicMock(spec=SemanticRegistry)
    terms = ["METRIC_TEST"]
    
    mock_registry.get_term.return_value = {
        "id": "METRIC_TEST",
        "name": "Test Metric",
        "aliases": [],  # 空列表
        "description": "Test description"
    }
    
    result = _format_schema_context(terms, mock_registry)
    
    # 验证不包含 Aliases 字段
    assert "| Aliases:" not in result, "aliases 为空时不应输出 Aliases 字段"
    assert "ID: METRIC_TEST" in result
    assert "Name: Test Metric" in result


@pytest.mark.unit
def test_format_schema_context_description_truncate_50():
    """
    【测试目标】
    1. 验证 desc 超过 50 被截断

    【执行过程】
    1. 创建 term_def，description 长度为 60 字符
    2. 调用 _format_schema_context
    3. 检查输出中 description 被截断到 50 字符

    【预期结果】
    1. 输出中 description 长度 <= 50 字符（如果原长度 > 50）
    """
    from stages.stage2_plan_generation import _format_schema_context
    
    mock_registry = MagicMock(spec=SemanticRegistry)
    terms = ["METRIC_TEST"]
    
    # 创建 60 字符的 description
    long_desc = "A" * 60
    mock_registry.get_term.return_value = {
        "id": "METRIC_TEST",
        "name": "Test Metric",
        "aliases": [],
        "description": long_desc
    }
    
    result = _format_schema_context(terms, mock_registry)
    
    # 提取 Desc 部分
    desc_part = None
    for part in result.split(" | "):
        if part.startswith("Desc: "):
            desc_part = part.replace("Desc: ", "")
            break
    
    assert desc_part is not None, "应该包含 Desc 字段"
    assert len(desc_part) == 50, f"description 应该被截断到 50 字符，实际长度: {len(desc_part)}"
    assert desc_part == "A" * 50, "截断后的内容应该是前 50 个字符"


@pytest.mark.unit
def test_format_schema_context_is_time_dimension_marker():
    """
    【测试目标】
    1. 验证 is_time_dimension=true 时出现 Is_Time: True

    【执行过程】
    1. 创建 dimension term_def，is_time_dimension=True
    2. 调用 _format_schema_context
    3. 检查输出中包含 "| Is_Time: True"

    【预期结果】
    1. 输出中包含 "| Is_Time: True"（当 is_time_dimension=True 时）
    """
    from stages.stage2_plan_generation import _format_schema_context
    
    mock_registry = MagicMock(spec=SemanticRegistry)
    terms = ["DIM_ORDER_DATE"]
    
    mock_registry.get_term.return_value = {
        "id": "DIM_ORDER_DATE",
        "name": "订单日期",
        "aliases": [],
        "description": "订单日期维度",
        "is_time_dimension": True  # 关键字段
    }
    
    result = _format_schema_context(terms, mock_registry)
    
    # 验证包含 Is_Time: True
    assert "| Is_Time: True" in result, "is_time_dimension=True 时应输出 Is_Time: True"
    assert "ID: DIM_ORDER_DATE" in result


@pytest.mark.unit
def test_format_schema_context_enum_values_threshold_50():
    """
    【测试目标】
    1. 验证 enum_values 长度 50 与 51 的分界行为：<=50 输出 Values；>50 不输出 Values

    【执行过程】
    1. 创建两个 dimension term_def：
       - enum_values 长度为 50（应该输出）
       - enum_values 长度为 51（不应该输出）
    2. 分别调用 _format_schema_context
    3. 检查输出行为

    【预期结果】
    1. enum_values 长度为 50 时，输出包含 "| Values: [...]"
    2. enum_values 长度为 51 时，输出不包含 "| Values:" 字段
    """
    from stages.stage2_plan_generation import _format_schema_context
    
    mock_registry = MagicMock(spec=SemanticRegistry)
    
    # 测试 1: enum_values 长度为 50（应该输出）
    terms_50 = ["DIM_TEST_50"]
    enum_values_50 = [f"VALUE_{i}" for i in range(50)]
    enum_def_50 = {
        "id": "ENUM_TEST_50",
        "values": enum_values_50
    }
    
    def mock_get_term_50(term_id):
        if term_id == "DIM_TEST_50":
            return {
                "id": "DIM_TEST_50",
                "name": "Test Dimension 50",
                "aliases": [],
                "description": "",
                "enum_value_set_id": "ENUM_TEST_50"
            }
        elif term_id == "ENUM_TEST_50":
            return enum_def_50
        return None
    
    mock_registry.get_term.side_effect = mock_get_term_50
    result_50 = _format_schema_context(terms_50, mock_registry)
    
    # 验证包含 Values 字段
    assert "| Values: [" in result_50, "enum_values 长度为 50 时应输出 Values 字段"
    assert "VALUE_0" in result_50, "应该包含枚举值"
    assert "VALUE_49" in result_50, "应该包含最后一个枚举值"
    
    # 测试 2: enum_values 长度为 51（不应该输出）
    terms_51 = ["DIM_TEST_51"]
    enum_values_51 = [f"VALUE_{i}" for i in range(51)]
    enum_def_51 = {
        "id": "ENUM_TEST_51",
        "values": enum_values_51
    }
    
    def mock_get_term_51(term_id):
        if term_id == "DIM_TEST_51":
            return {
                "id": "DIM_TEST_51",
                "name": "Test Dimension 51",
                "aliases": [],
                "description": "",
                "enum_value_set_id": "ENUM_TEST_51"
            }
        elif term_id == "ENUM_TEST_51":
            return enum_def_51
        return None
    
    mock_registry.get_term.side_effect = mock_get_term_51
    result_51 = _format_schema_context(terms_51, mock_registry)
    
    # 验证不包含 Values 字段
    assert "| Values:" not in result_51, "enum_values 长度为 51 时不应输出 Values 字段"
    assert "ID: DIM_TEST_51" in result_51, "应该包含基本字段"


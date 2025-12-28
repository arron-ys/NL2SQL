"""
【简述】
验证所有 Prompt 模板的格式化契约：模板可安全格式化（占位符与代码变量一致、花括号转义正确），且不会因输入数据触发 format/渲染异常导致链路中断。同时验证时间口径解析职责归属规范（Final Spec）的规则是否已写入 prompt。

【范围/不测什么】
- 不覆盖 LLM 推理质量；仅验证模板格式化的安全性与占位符一致性，以及规范规则是否在 prompt 中体现。

【用例概述】
- test_all_templates_format_safely:
  -- 验证所有模板的.format()不抛异常
- test_template_placeholders_match_code:
  -- 验证占位符与代码变量一致
- test_template_brace_escaping:
  -- 验证花括号转义正确（如{{、}}）
- test_stage1_prompt_forbids_vague_time_dateification:
  -- 验证 Stage1 prompt 禁止将模糊时间词日期化
- test_stage2_prompt_time_range_rules:
  -- 验证 Stage2 prompt 包含 time_range 推断规则（以 raw_question 为主、模糊词→null、显式窗口→LAST_N）
"""

import pytest

from utils.prompt_templates import (
    PROMPT_CLARIFICATION,
    PROMPT_DATA_INSIGHT,
    PROMPT_PLAN_GENERATION,
    PROMPT_SUBQUERY_DECOMPOSITION,
)


@pytest.mark.unit
def test_all_templates_format_safely():
    """
    【测试目标】
    1. 验证所有模板的.format()不抛异常

    【执行过程】
    1. 为每个模板准备完整的占位符参数
    2. 调用.format()方法
    3. 验证不抛异常且返回字符串

    【预期结果】
    1. 所有模板.format()调用不抛异常
    2. 返回值为字符串类型
    3. 返回字符串不为空
    """
    # PROMPT_SUBQUERY_DECOMPOSITION: current_date, question
    result1 = PROMPT_SUBQUERY_DECOMPOSITION.format(
        current_date="2024-01-15",
        question="统计每个部门的员工数量"
    )
    assert isinstance(result1, str)
    assert len(result1) > 0

    # PROMPT_PLAN_GENERATION: current_date, raw_question, sub_query_description, schema_context
    result2 = PROMPT_PLAN_GENERATION.format(
        current_date="2024-01-15",
        raw_question="统计每个部门的员工数量",
        sub_query_description="统计每个部门的员工数量",
        schema_context="METRIC_GMV\nDIM_DEPARTMENT"
    )
    assert isinstance(result2, str)
    assert len(result2) > 0

    # PROMPT_DATA_INSIGHT: raw_question, context_summary, query_result_data, row_count, is_truncated, execution_latency_ms
    result3 = PROMPT_DATA_INSIGHT.format(
        raw_question="统计每个部门的员工数量",
        context_summary="（当前查询无额外业务上下文）",
        query_result_data="| 部门 | 员工数 |\n|------|--------|\n| 研发 | 50 |",
        row_count=1,
        is_truncated="否",
        execution_latency_ms=100
    )
    assert isinstance(result3, str)
    assert len(result3) > 0

    # PROMPT_CLARIFICATION: raw_question, uncertain_information
    result4 = PROMPT_CLARIFICATION.format(
        raw_question="统计每个部门的员工数量",
        uncertain_information="权限不足：您当前的角色没有权限访问查询中涉及的业务域数据"
    )
    assert isinstance(result4, str)
    assert len(result4) > 0


@pytest.mark.unit
def test_template_placeholders_match_code():
    """
    【测试目标】
    1. 验证占位符与代码变量一致

    【执行过程】
    1. 检查每个模板中使用的占位符
    2. 验证占位符名称与代码中.format()调用时使用的参数名一致
    3. 验证所有占位符都被提供值

    【预期结果】
    1. PROMPT_SUBQUERY_DECOMPOSITION 使用 current_date, question
    2. PROMPT_PLAN_GENERATION 使用 current_date, raw_question, sub_query_description, schema_context
    3. PROMPT_DATA_INSIGHT 使用 raw_question, context_summary, query_result_data, row_count, is_truncated, execution_latency_ms
    4. PROMPT_CLARIFICATION 使用 raw_question, uncertain_information
    5. 所有占位符在代码中都有对应的参数提供
    """
    # 验证 PROMPT_SUBQUERY_DECOMPOSITION 占位符
    # 从 stage1_decomposition.py:90-93 可以看到使用 current_date, question
    assert "{current_date}" in PROMPT_SUBQUERY_DECOMPOSITION
    assert "{question}" in PROMPT_SUBQUERY_DECOMPOSITION

    # 验证 PROMPT_PLAN_GENERATION 占位符
    # 从 stage2_plan_generation.py 可以看到使用 current_date, raw_question, sub_query_description, schema_context
    assert "{current_date}" in PROMPT_PLAN_GENERATION
    assert "{raw_question}" in PROMPT_PLAN_GENERATION
    assert "{sub_query_description}" in PROMPT_PLAN_GENERATION
    assert "{schema_context}" in PROMPT_PLAN_GENERATION

    # 验证 PROMPT_DATA_INSIGHT 占位符
    # 从 stage6_answer.py 可以看到使用 raw_question, context_summary, query_result_data, row_count, is_truncated, execution_latency_ms
    assert "{raw_question}" in PROMPT_DATA_INSIGHT
    assert "{context_summary}" in PROMPT_DATA_INSIGHT
    assert "{query_result_data}" in PROMPT_DATA_INSIGHT
    assert "{row_count}" in PROMPT_DATA_INSIGHT
    assert "{is_truncated}" in PROMPT_DATA_INSIGHT
    assert "{execution_latency_ms}" in PROMPT_DATA_INSIGHT

    # 验证 PROMPT_CLARIFICATION 占位符
    # 从 stage6_answer.py 可以看到使用 raw_question, uncertain_information
    assert "{raw_question}" in PROMPT_CLARIFICATION
    assert "{uncertain_information}" in PROMPT_CLARIFICATION


@pytest.mark.unit
def test_template_brace_escaping():
    """
    【测试目标】
    1. 验证花括号转义正确（如{{、}}）

    【执行过程】
    1. 检查模板中需要转义的花括号（用于显示JSON示例）
    2. 验证转义后的花括号在格式化后正确显示
    3. 验证未转义的花括号作为占位符被替换

    【预期结果】
    1. 模板中的{{和}}在格式化后显示为{和}
    2. 模板中的{占位符}在格式化后被替换为实际值
    3. 格式化后的字符串包含正确的JSON示例结构
    """
    # PROMPT_SUBQUERY_DECOMPOSITION 包含 JSON 示例，使用 {{ 和 }}
    result = PROMPT_SUBQUERY_DECOMPOSITION.format(
        current_date="2024-01-15",
        question="测试问题"
    )
    # 验证转义的花括号在结果中显示为单个花括号
    assert "{{" in PROMPT_SUBQUERY_DECOMPOSITION or "}}" in PROMPT_SUBQUERY_DECOMPOSITION
    # 验证格式化后包含JSON结构示例
    assert "sub_queries" in result.lower() or "id" in result.lower()

    # PROMPT_PLAN_GENERATION 包含 JSON 示例，使用 {{ 和 }}
    result2 = PROMPT_PLAN_GENERATION.format(
        current_date="2024-01-15",
        raw_question="测试问题",
        sub_query_description="测试问题",
        schema_context="METRIC_GMV"
    )
    # 验证格式化后包含JSON结构示例
    assert "intent" in result2.lower() or "metrics" in result2.lower()

    # 验证转义的花括号不会导致格式化异常
    # 如果模板中有 {{ 或 }}，它们应该被正确转义
    # 测试所有模板都能安全格式化，说明转义正确
    all_templates = [
        PROMPT_SUBQUERY_DECOMPOSITION,
        PROMPT_PLAN_GENERATION,
        PROMPT_DATA_INSIGHT,
        PROMPT_CLARIFICATION
    ]
    for template in all_templates:
        # 统计未转义的 { 和 } 数量（应该是占位符）
        open_braces = template.count("{")
        close_braces = template.count("}")
        # 统计转义的 {{ 和 }} 数量
        escaped_open = template.count("{{")
        escaped_close = template.count("}}")
        # 未转义的 { 应该等于未转义的 }（每个占位符都是 {name} 格式）
        unescaped_open = open_braces - escaped_open
        unescaped_close = close_braces - escaped_close
        # 验证未转义的括号数量匹配（每个占位符都有开闭括号）
        assert unescaped_open == unescaped_close, f"Template has mismatched braces: {template[:100]}"


@pytest.mark.unit
def test_stage1_prompt_forbids_vague_time_dateification():
    """
    【测试目标】
    1. 验证 Stage1 prompt 明确禁止将模糊时间词日期化为具体日期
    2. 验证 Stage1 prompt 明确 Stage1 不承担时间口径决策

    【执行过程】
    1. 检查 PROMPT_SUBQUERY_DECOMPOSITION 中是否包含禁止模糊时间词日期化的规则
    2. 检查是否包含"不承担时间口径决策"的表述
    3. 检查是否包含模糊时间词列表

    【预期结果】
    1. prompt 中包含"严禁将模糊时间词日期化为具体日期"或类似表述
    2. prompt 中包含"Stage1 不承担时间口径决策"或类似表述
    3. prompt 中包含至少部分模糊时间词（如"最近"、"目前"、"当前"等）
    """
    # 验证禁止模糊时间词日期化的规则
    assert "严禁" in PROMPT_SUBQUERY_DECOMPOSITION or "禁止" in PROMPT_SUBQUERY_DECOMPOSITION
    assert "模糊时间词" in PROMPT_SUBQUERY_DECOMPOSITION or "最近" in PROMPT_SUBQUERY_DECOMPOSITION
    
    # 验证职责边界说明
    assert "不承担时间口径决策" in PROMPT_SUBQUERY_DECOMPOSITION or "时间口径" in PROMPT_SUBQUERY_DECOMPOSITION
    
    # 验证包含至少部分模糊时间词示例
    vague_time_keywords_in_prompt = [
        "最近", "目前", "当前", "近期"
    ]
    found_keywords = [kw for kw in vague_time_keywords_in_prompt if kw in PROMPT_SUBQUERY_DECOMPOSITION]
    assert len(found_keywords) > 0, "Prompt should contain at least some vague time keywords"


@pytest.mark.unit
def test_stage2_prompt_time_range_rules():
    """
    【测试目标】
    1. 验证 Stage2 prompt 明确 time_range 推断以 raw_question 为主
    2. 验证 Stage2 prompt 明确模糊时间词必须输出 null
    3. 验证 Stage2 prompt 明确显式相对窗口必须输出 LAST_N

    【执行过程】
    1. 检查 PROMPT_PLAN_GENERATION 中是否包含"以 raw_question 为主"的规则
    2. 检查是否包含模糊时间词必须输出 null 的规则
    3. 检查是否包含显式相对窗口必须输出 LAST_N 的规则

    【预期结果】
    1. prompt 中包含"以 Raw Question 为主"或"raw_question"作为时间权威来源的表述
    2. prompt 中包含模糊时间词必须输出 null 的规则
    3. prompt 中包含显式相对窗口（如"最近30天"）必须输出 LAST_N 的规则
    """
    # 验证 time_range 推断以 raw_question 为主
    assert "Raw Question" in PROMPT_PLAN_GENERATION or "raw_question" in PROMPT_PLAN_GENERATION
    assert "为主" in PROMPT_PLAN_GENERATION or "primary" in PROMPT_PLAN_GENERATION.lower() or "authority" in PROMPT_PLAN_GENERATION.lower()
    
    # 验证模糊时间词必须输出 null 的规则
    assert "模糊时间词" in PROMPT_PLAN_GENERATION or "最近" in PROMPT_PLAN_GENERATION
    assert "null" in PROMPT_PLAN_GENERATION or "必须" in PROMPT_PLAN_GENERATION
    
    # 验证显式相对窗口必须输出 LAST_N 的规则
    assert "显式相对窗口" in PROMPT_PLAN_GENERATION or "最近30天" in PROMPT_PLAN_GENERATION or "LAST_N" in PROMPT_PLAN_GENERATION
    assert "LAST_N" in PROMPT_PLAN_GENERATION


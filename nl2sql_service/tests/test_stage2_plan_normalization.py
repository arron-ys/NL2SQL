"""
【简述】
验证 Stage2 Plan 生成中 _extract_all_ids_from_plan 函数对异常 order_by 值的容错性，以及 _normalize_plan_structure 和 _perform_anti_hallucination_check 对畸形 JSON 的清洗/修复逻辑。

【范围/不测什么】
- 不覆盖完整 Plan 生成流程；仅验证 ID 提取函数、结构归一化和反幻觉检查的异常处理逻辑。

【用例概述】
- test_extract_all_ids_from_plan_order_by_none_does_not_raise:
  -- 验证 order_by 为 None 时不抛异常
- test_extract_all_ids_from_plan_order_by_bad_type_warns_and_does_not_raise:
  -- 验证 order_by 为非法类型时记录警告且不抛异常
- test_missing_metrics_field_with_agg_intent:
  -- 验证字段缺失（intent=AGG但metrics缺失）时被归一化为空列表
- test_type_drift_limit_string:
  -- 验证类型漂移（limit="100"应为int）时Pydantic校验会失败或转换
- test_nested_time_range_malformed:
  -- 验证嵌套错位（time_range结构错误）时Pydantic校验会失败
- test_invalid_enum_intent:
  -- 验证非法枚举值（intent="INVALID"）时Pydantic校验会失败
- test_repairable_error_auto_fixed:
  -- 验证可修复错误自动修复（无效ID被移除）
- test_unrepairable_error_degraded_flag:
  -- 验证不可修复错误返回降级标志（通过warnings表达）
"""

import pytest
from unittest.mock import MagicMock, Mock

from schemas.plan import PlanIntent


@pytest.mark.unit
def test_extract_all_ids_from_plan_order_by_none_does_not_raise():
    """
    【测试目标】
    1. 验证 order_by 为 None 时 _extract_all_ids_from_plan 不抛异常

    【执行过程】
    1. 构造 plan_dict 包含 order_by=None
    2. 调用 _extract_all_ids_from_plan
    3. 检查返回值类型

    【预期结果】
    1. 返回 set 类型
    2. 不抛异常
    """
    # 注意：项目运行时通过 `main.py` 从 `nl2sql_service/` 目录作为工作目录导入 `stages.*`。
    # 这里沿用同样的导入路径，避免触发 `nl2sql_service.stages.*` 下的绝对导入问题（如 `from config...`）。
    from stages.stage2_plan_generation import _extract_all_ids_from_plan

    plan_dict = {"order_by": None}
    ids = _extract_all_ids_from_plan(plan_dict)
    assert isinstance(ids, set)


@pytest.mark.unit
def test_extract_all_ids_from_plan_order_by_bad_type_warns_and_does_not_raise(monkeypatch):
    """
    【测试目标】
    1. 验证 order_by 为非法类型时记录警告且不抛异常

    【执行过程】
    1. mock logger.warning
    2. 构造 plan_dict 包含 order_by="bad_type"
    3. 调用 _extract_all_ids_from_plan
    4. 检查返回值与 warning 调用

    【预期结果】
    1. 返回 set 类型
    2. logger.warning 被调用
    3. 不抛异常
    """
    from stages import stage2_plan_generation

    warn_mock = Mock()
    # 仅替换 warning 方法，避免影响其他 loguru 行为
    monkeypatch.setattr(stage2_plan_generation.logger, "warning", warn_mock)

    ids = stage2_plan_generation._extract_all_ids_from_plan({"order_by": "bad_type"})
    assert isinstance(ids, set)
    assert warn_mock.called


@pytest.mark.unit
def test_missing_metrics_field_with_agg_intent():
    """
    【测试目标】
    1. 验证字段缺失（intent=AGG但metrics缺失）时被归一化为空列表

    【执行过程】
    1. 构造 plan_dict 包含 intent="AGG" 但缺少 metrics 字段
    2. 调用 _normalize_plan_structure
    3. 检查 metrics 字段被设置为空列表

    【预期结果】
    1. metrics 字段被设置为 []
    2. 不抛异常
    """
    from stages.stage2_plan_generation import _normalize_plan_structure

    plan_dict = {"intent": "AGG"}  # 缺少 metrics 字段
    normalized = _normalize_plan_structure(plan_dict)
    assert normalized.get("metrics") == []
    assert normalized.get("intent") == "AGG"


@pytest.mark.unit
def test_type_drift_limit_string():
    """
    【测试目标】
    1. 验证类型漂移（limit="100"应为int）时Pydantic校验会失败或转换

    【执行过程】
    1. 构造 plan_dict 包含 limit="100"（字符串）
    2. 尝试通过 QueryPlan 模型验证
    3. 检查是否抛出 ValidationError 或自动转换

    【预期结果】
    1. Pydantic 校验失败（ValidationError）或自动转换为 int
    2. 如果转换，limit 应为 int 类型
    """
    from pydantic import ValidationError
    from schemas.plan import QueryPlan

    # 测试字符串 limit 是否会被拒绝或转换
    plan_dict = {
        "intent": "AGG",
        "metrics": [],
        "limit": "100"  # 字符串而非 int
    }
    
    # Pydantic 应该拒绝字符串类型的 limit（如果 limit 定义为 int）
    # 或者自动转换（如果配置了类型转换）
    try:
        plan = QueryPlan(**plan_dict)
        # 如果成功，验证 limit 是否为 int
        assert isinstance(plan.limit, int) or plan.limit is None
    except ValidationError:
        # 如果失败，说明类型校验生效
        pass


@pytest.mark.unit
def test_nested_time_range_malformed():
    """
    【测试目标】
    1. 验证嵌套错位（time_range结构错误）时Pydantic校验会失败

    【执行过程】
    1. 构造 plan_dict 包含错误的 time_range 结构（嵌套错位）
    2. 尝试通过 QueryPlan 模型验证
    3. 检查是否抛出 ValidationError

    【预期结果】
    1. Pydantic 校验失败（ValidationError）
    2. 错误信息包含 time_range 相关字段
    """
    from pydantic import ValidationError
    from schemas.plan import QueryPlan

    # 错误的 time_range 结构：将 start/end 嵌套在 value 中（应该是顶层字段）
    plan_dict = {
        "intent": "AGG",
        "metrics": [],
        "time_range": {
            "type": "ABSOLUTE",
            "value": {  # 错误：start/end 不应该在 value 中
                "start": "2024-01-01",
                "end": "2024-01-31"
            }
        }
    }
    
    # Pydantic 应该拒绝这种结构
    with pytest.raises(ValidationError):
        QueryPlan(**plan_dict)


@pytest.mark.unit
def test_invalid_enum_intent():
    """
    【测试目标】
    1. 验证非法枚举值（intent="INVALID"）时Pydantic校验会失败

    【执行过程】
    1. 构造 plan_dict 包含 intent="INVALID"
    2. 尝试通过 QueryPlan 模型验证
    3. 检查是否抛出 ValidationError

    【预期结果】
    1. Pydantic 校验失败（ValidationError）
    2. 错误信息包含 intent 字段和允许的值
    """
    from pydantic import ValidationError
    from schemas.plan import QueryPlan

    plan_dict = {
        "intent": "INVALID",  # 非法枚举值
        "metrics": []
    }
    
    # Pydantic 应该拒绝非法枚举值
    with pytest.raises(ValidationError) as exc_info:
        QueryPlan(**plan_dict)
    
    # 验证错误信息包含 intent
    error_str = str(exc_info.value)
    assert "intent" in error_str.lower() or "INVALID" in error_str


@pytest.mark.unit
def test_repairable_error_auto_fixed():
    """
    【测试目标】
    1. 验证可修复错误自动修复（无效ID被移除）

    【执行过程】
    1. 构造 plan_dict 包含无效的 metric ID（不在 registry 中）
    2. mock registry.get_term 返回 None（表示ID不存在）
    3. 调用 _perform_anti_hallucination_check
    4. 检查无效ID被移除，warnings 包含警告信息

    【预期结果】
    1. 无效的 metric ID 从 metrics 列表中移除
    2. warnings 列表包含 "Invalid ID" 警告
    3. cleaned_plan 中不再包含无效ID
    """
    from stages.stage2_plan_generation import _perform_anti_hallucination_check

    # 构造包含无效ID的 plan_dict
    plan_dict = {
        "intent": "AGG",
        "metrics": [
            {"id": "METRIC_VALID"},  # 有效ID
            {"id": "METRIC_INVALID"}  # 无效ID（不在registry中）
        ],
        "dimensions": []
    }
    
    # Mock registry
    mock_registry = MagicMock()
    mock_registry.get_term.side_effect = lambda term_id: {
        "id": term_id,
        "type": "metric"
    } if term_id == "METRIC_VALID" else None  # 无效ID返回None
    
    cleaned_plan, warnings = _perform_anti_hallucination_check(plan_dict, mock_registry)
    
    # 验证无效ID被移除
    metric_ids = [m.get("id") for m in cleaned_plan.get("metrics", [])]
    assert "METRIC_VALID" in metric_ids
    assert "METRIC_INVALID" not in metric_ids
    
    # 验证 warnings 包含警告
    assert any("Invalid ID" in w or "METRIC_INVALID" in w for w in warnings)


@pytest.mark.unit
def test_unrepairable_error_degraded_flag():
    """
    【测试目标】
    1. 验证不可修复错误返回降级标志（通过warnings表达）

    【执行过程】
    1. 构造 plan_dict 包含 intent=AGG 但所有 metrics 都是无效ID
    2. mock registry.get_term 对所有ID返回 None
    3. 调用 _perform_anti_hallucination_check
    4. 检查 cleaned_plan 的 metrics 为空，warnings 包含降级信息

    【预期结果】
    1. cleaned_plan 的 metrics 为空列表（所有无效ID被移除）
    2. warnings 列表包含多个 "Invalid ID" 警告
    3. 虽然结构合法，但内容被清空，表示发生了降级
    """
    from stages.stage2_plan_generation import _perform_anti_hallucination_check

    # 构造所有 metrics 都是无效ID的 plan_dict
    plan_dict = {
        "intent": "AGG",
        "metrics": [
            {"id": "METRIC_INVALID_1"},
            {"id": "METRIC_INVALID_2"}
        ],
        "dimensions": []
    }
    
    # Mock registry：所有ID都不存在
    mock_registry = MagicMock()
    mock_registry.get_term.return_value = None
    
    cleaned_plan, warnings = _perform_anti_hallucination_check(plan_dict, mock_registry)
    
    # 验证所有无效ID被移除
    assert len(cleaned_plan.get("metrics", [])) == 0
    
    # 验证 warnings 包含降级信息（多个无效ID警告）
    assert len(warnings) >= 2
    assert all("Invalid ID" in w or "hallucination" in w.lower() for w in warnings)
    
    # 验证 cleaned_plan 结构仍然合法（虽然内容被清空）
    assert "intent" in cleaned_plan
    assert "metrics" in cleaned_plan
    assert isinstance(cleaned_plan["metrics"], list)


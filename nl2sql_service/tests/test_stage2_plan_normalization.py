"""
【简述】
验证 Stage2 Plan 生成中 _extract_all_ids_from_plan 函数对异常 order_by 值的容错性。

【范围/不测什么】
- 不覆盖完整 Plan 生成流程；仅验证 ID 提取函数的异常处理逻辑。

【用例概述】
- test_extract_all_ids_from_plan_order_by_none_does_not_raise:
  -- 验证 order_by 为 None 时不抛异常
- test_extract_all_ids_from_plan_order_by_bad_type_warns_and_does_not_raise:
  -- 验证 order_by 为非法类型时记录警告且不抛异常
"""

import pytest
from unittest.mock import Mock


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


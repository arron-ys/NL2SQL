import pytest
from unittest.mock import Mock


@pytest.mark.unit
def test_extract_all_ids_from_plan_order_by_none_does_not_raise():
    # 注意：项目运行时通过 `main.py` 从 `nl2sql_service/` 目录作为工作目录导入 `stages.*`。
    # 这里沿用同样的导入路径，避免触发 `nl2sql_service.stages.*` 下的绝对导入问题（如 `from config...`）。
    from stages.stage2_plan_generation import _extract_all_ids_from_plan

    plan_dict = {"order_by": None}
    ids = _extract_all_ids_from_plan(plan_dict)
    assert isinstance(ids, set)


@pytest.mark.unit
def test_extract_all_ids_from_plan_order_by_bad_type_warns_and_does_not_raise(monkeypatch):
    from stages import stage2_plan_generation

    warn_mock = Mock()
    # 仅替换 warning 方法，避免影响其他 loguru 行为
    monkeypatch.setattr(stage2_plan_generation.logger, "warning", warn_mock)

    ids = stage2_plan_generation._extract_all_ids_from_plan({"order_by": "bad_type"})
    assert isinstance(ids, set)
    assert warn_mock.called


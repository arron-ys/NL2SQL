import pytest
from unittest.mock import Mock


@pytest.mark.unit
def test_extract_all_ids_from_plan_order_by_none_does_not_raise():
    from nl2sql_service.stages.stage2_plan_generation import _extract_all_ids_from_plan

    plan_dict = {"order_by": None}
    ids = _extract_all_ids_from_plan(plan_dict)
    assert isinstance(ids, set)


@pytest.mark.unit
def test_extract_all_ids_from_plan_order_by_bad_type_warns_and_does_not_raise(monkeypatch):
    from nl2sql_service.stages import stage2_plan_generation

    warn_mock = Mock()
    # 仅替换 warning 方法，避免影响其他 loguru 行为
    monkeypatch.setattr(stage2_plan_generation.logger, "warning", warn_mock)

    ids = stage2_plan_generation._extract_all_ids_from_plan({"order_by": "bad_type"})
    assert isinstance(ids, set)
    assert warn_mock.called


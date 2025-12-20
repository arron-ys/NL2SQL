"""
【简述】
验证 /nl2sql/plan 遇到权限拒绝时返回 200 状态码并生成脱敏的 PERMISSION_DENIED 错误体，不泄露 METRIC_* 内部 ID。

【范围/不测什么】
- 不覆盖真实权限校验；仅验证权限错误的软降级响应结构与响应脱敏。

【用例概述】
- test_plan_permission_denied_returns_200_and_is_sanitized:
  -- 验证权限拒绝时返回 200 且错误体不泄露 METRIC_* ID
"""

import re
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from main import app
from stages import stage3_validation
from schemas.plan import PlanIntent, QueryPlan


@pytest.mark.integration
def test_plan_permission_denied_returns_200_and_is_sanitized():
    """
    【测试目标】
    1. 验证权限拒绝时 /nl2sql/plan 返回 200 且错误体不泄露 METRIC_* ID

    【执行过程】
    1. mock registry 和 stage1/stage2
    2. mock stage3_validation 抛出 PermissionDeniedError 包含 METRIC_GMV
    3. 调用 POST /nl2sql/plan
    4. 验证响应状态、错误结构与 METRIC_* 泄露检查

    【预期结果】
    1. 返回 200 状态码
    2. status 为 "ERROR"，error.code 为 "PERMISSION_DENIED"
    3. error.message 包含 "没有权限"
    4. 响应文本不包含 "METRIC_" 或 METRIC_* 格式的内部 ID
    """
    client = TestClient(app)

    with patch("main.registry", new=MagicMock()):
        fake_query_desc = MagicMock()
        fake_query_desc.request_context = MagicMock(
            user_id="u1",
            role_id="ROLE_HR_HEAD",
            tenant_id="t1",
            request_id="test-trace-001",
            current_date=date(2024, 1, 15),
        )
        fake_query_desc.sub_queries = [MagicMock(id="sq_1", description="最近公司的销售业绩怎么样？")]

        with patch("main.stage1_decomposition.process_request", return_value=fake_query_desc):
            # Return a real, picklable QueryPlan to avoid Loguru/multiprocessing pickling issues
            fake_plan = QueryPlan(intent=PlanIntent.AGG, metrics=[], dimensions=[], filters=[], warnings=[])
            with patch("main.stage2_plan_generation.process_subquery", return_value=fake_plan):
                with patch(
                    "main.stage3_validation.validate_and_normalize_plan",
                    side_effect=stage3_validation.PermissionDeniedError(
                        "[PERMISSION_DENIED] Blocked metrics: ['GMV'] (Domain: SALES) METRIC_GMV"
                    ),
                ):
                    resp = client.post(
                        "/nl2sql/plan",
                        json={
                            "question": "最近公司的销售业绩怎么样？",
                            "user_id": "u1",
                            "role_id": "ROLE_HR_HEAD",
                            "tenant_id": "t1",
                            "include_trace": False,
                        },
                        headers={"Trace-ID": "test-trace-001"},
                    )

    assert resp.status_code == 200
    data = resp.json()

    assert data.get("request_id") == "test-trace-001"
    assert data.get("status") == "ERROR"
    assert data.get("error", {}).get("code") == "PERMISSION_DENIED"
    assert data.get("error", {}).get("stage") == "STAGE_3_VALIDATION"
    assert "没有权限" in data.get("error", {}).get("message", "")

    # Must not leak METRIC_* identifiers in body
    body_text = resp.text
    assert "METRIC_" not in body_text
    assert not re.search(r"METRIC_[A-Z0-9_]+", body_text)


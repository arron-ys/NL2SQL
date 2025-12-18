"""
/nl2sql/plan permission denied soft error contract (integration).

Requirements:
- No real network calls.
- Must return HTTP 200 with a sanitized PERMISSION_DENIED error body.
- Must not leak specific METRIC_* IDs in response message/body.
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
                        headers={"X-Trace-ID": "test-trace-001"},
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


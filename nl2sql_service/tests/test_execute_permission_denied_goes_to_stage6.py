"""
/nl2sql/execute permission denied should be handled by Stage6 (integration).

Goal:
- When pipeline returns a PERMISSION_DENIED error, /nl2sql/execute should still return HTTP 200
  and produce a natural language answer via Stage6 (LLM mocked).
- Must not leak METRIC_* IDs in response.
"""

import re
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from main import app
from schemas.result import ExecutionResult


@pytest.mark.integration
def test_execute_permission_denied_is_answered_by_stage6_and_sanitized():
    client = TestClient(app)

    with patch("main.registry", new=MagicMock()):
        fake_query_desc = MagicMock()
        fake_query_desc.request_context = MagicMock(
            user_id="u1",
            role_id="ROLE_HR_HEAD",
            tenant_id="t1",
            request_id="test-trace-003",
            current_date=date(2024, 1, 15),
        )
        fake_query_desc.sub_queries = [MagicMock(id="sq_1", description="最近公司的销售业绩怎么样？")]

        # Pipeline returns "all failed" with a permission denied error message
        batch_results = [
            {
                "sub_query_id": "sq_1",
                "sub_query_description": "最近公司的销售业绩怎么样？",
                "execution_result": ExecutionResult.create_error(
                    error="[STAGE_3_VALIDATION] PERMISSION_DENIED: 您当前的角色没有权限访问查询中涉及的业务域数据（Domain: SALES）。 METRIC_GMV",
                    latency_ms=0,
                ),
            }
        ]

        # Mock Stage6 LLM generation
        fake_ai = MagicMock()
        fake_ai.generate_answer = AsyncMock(
            return_value="抱歉，我无法查询销售域数据，因为您当前角色没有相关权限。您可以联系管理员或切换到有权限的角色。"
        )

        with patch("main.stage1_decomposition.process_request", return_value=fake_query_desc):
            with patch("main.run_pipeline", return_value=batch_results):
                with patch("stages.stage6_answer.get_ai_client", return_value=fake_ai):
                    resp = client.post(
                        "/nl2sql/execute",
                        json={
                            "question": "最近公司的销售业绩怎么样？",
                            "user_id": "u1",
                            "role_id": "ROLE_HR_HEAD",
                            "tenant_id": "t1",
                            "include_trace": False,
                        },
                        headers={"Trace-ID": "test-trace-003"},
                    )

    assert resp.status_code == 200
    data = resp.json()
    assert data.get("status") == "ALL_FAILED"
    assert "没有相关权限" in data.get("answer_text", "")

    # Must not leak METRIC_* in response
    assert "METRIC_" not in resp.text
    assert not re.search(r"METRIC_[A-Z0-9_]+", resp.text)


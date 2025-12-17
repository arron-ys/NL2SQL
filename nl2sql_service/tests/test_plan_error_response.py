"""
/nl2sql/plan error response contract tests.

Requirements:
- No real network calls.
- When Stage/Provider raises AppError (e.g. JinaEmbeddingError), response body MUST include:
  request_id, error_stage, error.code, error.message
- Must not leak secrets (api_key/authorization/bearer).
"""

import re
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from core.providers.jina_provider import JinaEmbeddingError
from main import app


@pytest.mark.integration
def test_plan_error_response_contains_code_stage_request_id_and_is_sanitized():
    client = TestClient(app)

    # Patch main.registry to bypass startup dependency and prevent real semantics/qdrant usage.
    with patch("main.registry", new=MagicMock()):
        # Stage 1: return a deterministic request_context with request_id
        fake_query_desc = MagicMock()
        fake_query_desc.request_context = MagicMock(
            user_id="u1",
            role_id="ROLE_TEST",
            tenant_id="t1",
            request_id="test-trace-001",
            current_date=date(2024, 1, 15),
        )
        fake_query_desc.sub_queries = [MagicMock(id="sq_1", description="统计每个部门的员工数量")]

        with patch("main.stage1_decomposition.process_request", return_value=fake_query_desc):
            # Stage 2: raise JinaEmbeddingError (provider/embedding failure)
            def _raise_embedding(*args, **kwargs):
                raise JinaEmbeddingError("All connection attempts failed")

            with patch("main.stage2_plan_generation.process_subquery", side_effect=_raise_embedding):
                resp = client.post(
                    "/nl2sql/plan",
                    json={
                        "question": "统计每个部门的员工数量",
                        "user_id": "u1",
                        "role_id": "ROLE_TEST",
                        "tenant_id": "t1",
                        "include_trace": False,
                    },
                    headers={"X-Trace-ID": "test-trace-001"},
                )

    assert resp.status_code == 500
    data = resp.json()
    assert isinstance(data.get("request_id"), str) and data["request_id"]
    assert data.get("error_stage") == "STAGE_2_PLAN_GENERATION"
    assert data.get("error", {}).get("code") == "EMBEDDING_UNAVAILABLE"
    assert "All connection attempts failed" in data.get("error", {}).get("message", "")

    body_text = resp.text.lower()
    assert "api_key" not in body_text
    assert "authorization" not in body_text
    assert "bearer" not in body_text
    assert not re.search(r"sk-[a-z0-9]{10,}", body_text)


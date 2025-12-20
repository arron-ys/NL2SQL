"""
【简述】
验证 /nl2sql/execute 错误响应的结构契约：包含 request_id、error_stage、error.code，并确保敏感信息（API key）被脱敏。

【范围/不测什么】
- 不覆盖真实数据库连接与 embedding 服务；仅验证错误响应结构与日志脱敏规则。

【用例概述】
- test_execute_error_response_contains_code_stage_request_id_and_is_sanitized:
  -- 验证 execute 端点错误响应包含必需字段且敏感信息被脱敏
"""

import re
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from core.providers.jina_provider import JinaEmbeddingError
from main import app


@pytest.mark.integration
def test_execute_error_response_contains_code_stage_request_id_and_is_sanitized():
    """
    【测试目标】
    1. 验证 /nl2sql/execute 错误响应包含必需字段且敏感信息被脱敏

    【执行过程】
    1. mock registry 和 stage1_decomposition
    2. mock run_pipeline 抛出 JinaEmbeddingError
    3. 调用 POST /nl2sql/execute
    4. 验证响应状态码、字段结构与敏感信息过滤

    【预期结果】
    1. 返回 500 状态码
    2. 响应包含 request_id、error_stage="STAGE_2_PLAN_GENERATION"、error.code="EMBEDDING_UNAVAILABLE"
    3. 响应文本不包含 api_key、authorization、bearer 等敏感关键字
    4. 响应文本不包含 sk-xxx 格式的 API key 模式
    """
    client = TestClient(app)

    with patch("main.registry", new=MagicMock()):
        fake_query_desc = MagicMock()
        fake_query_desc.request_context = MagicMock(
            user_id="u1",
            role_id="ROLE_TEST",
            tenant_id="t1",
            request_id="test-trace-002",
            current_date=date(2024, 1, 15),
        )
        fake_query_desc.sub_queries = [MagicMock(id="sq_1", description="统计每个部门的员工数量")]

        with patch("main.stage1_decomposition.process_request", return_value=fake_query_desc):
            # Orchestrator path: make run_pipeline raise embedding error (simulates Stage2 embedding failure)
            with patch("main.run_pipeline", side_effect=JinaEmbeddingError("All connection attempts failed")):
                resp = client.post(
                    "/nl2sql/execute",
                    json={
                        "question": "统计每个部门的员工数量",
                        "user_id": "u1",
                        "role_id": "ROLE_TEST",
                        "tenant_id": "t1",
                        "include_trace": False,
                    },
                    headers={"Trace-ID": "test-trace-002"},
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


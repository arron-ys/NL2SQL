"""
【简述】
验证 /nl2sql/execute 在遇到 PERMISSION_DENIED 错误时降级到 Stage6 生成自然语言答案，并确保响应不泄露 METRIC_* 内部 ID。

【范围/不测什么】
- 不覆盖真实权限校验与 LLM 推理；仅验证错误降级路径、Stage6 调用与响应脱敏。

【用例概述】
- test_execute_permission_denied_is_answered_by_stage6_and_sanitized:
  -- 验证权限拒绝时返回 200 且通过 Stage6 生成答案，不泄露 METRIC_* ID
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
    """
    【测试目标】
    1. 验证权限拒绝时 /nl2sql/execute 降级到 Stage6 生成答案且不泄露 METRIC_* ID

    【执行过程】
    1. mock stage1 返回查询描述
    2. mock run_pipeline 返回包含 PERMISSION_DENIED 错误的结果
    3. mock Stage6 generate_answer 返回友好提示
    4. 调用 POST /nl2sql/execute
    5. 验证响应状态、答案内容与 METRIC_* 泄露检查

    【预期结果】
    1. 返回 200 状态码
    2. status 为 "ALL_FAILED"
    3. answer_text 包含 "没有相关权限"
    4. 响应文本不包含 "METRIC_" 或 METRIC_* 格式的内部 ID
    """
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


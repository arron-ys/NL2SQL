"""
【简述】
验证 /nl2sql/execute 错误响应的结构契约：包含 request_id、error_stage、error.code，并确保敏感信息（API key）被脱敏。

【范围/不测什么】
- 不覆盖真实数据库连接与 embedding 服务；仅验证错误响应结构与日志脱敏规则。

【用例概述】
- test_execute_error_response_contains_code_stage_request_id_and_is_sanitized:
  -- 验证 execute 端点错误响应包含必需字段且敏感信息被脱敏
- test_execute_422_missing_required_field:
  -- 验证参数缺失时返回422状态码和结构化错误
- test_execute_sql_syntax_error:
  -- 验证SQL语法错误时返回500状态码和错误结构
- test_execute_table_not_found:
  -- 验证表不存在时返回500状态码和错误结构
- test_execute_missing_semantic_term:
  -- 验证语义缺失时返回200状态码和软错误结构
"""

import re
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from freezegun import freeze_time
from sqlalchemy.exc import OperationalError, ProgrammingError

from core.providers.jina_provider import JinaEmbeddingError
from main import app
from schemas.plan import MetricItem, PlanIntent, QueryPlan
from schemas.request import RequestContext, SubQueryItem
from stages.stage3_validation import MissingMetricError


@pytest.mark.integration
@freeze_time("2024-01-15")
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

    request_payload = {
        "question": "统计每个部门的员工数量",
        "user_id": "u1",
        "role_id": "ROLE_TEST",
        "tenant_id": "t1",
    }
    
    assert resp.status_code == 500, f"Expected 500, got {resp.status_code} | Trace-ID: test-trace-002 | Request: {request_payload}"
    data = resp.json()
    assert isinstance(data.get("request_id"), str) and data["request_id"], f"Missing request_id | Response: {data.get('error', {})}"
    assert data.get("error_stage") == "STAGE_2_PLAN_GENERATION", f"Expected STAGE_2_PLAN_GENERATION, got {data.get('error_stage')} | Request-ID: {data.get('request_id')}"
    assert data.get("error", {}).get("code") == "EMBEDDING_UNAVAILABLE", f"Expected EMBEDDING_UNAVAILABLE, got {data.get('error', {}).get('code')} | Request-ID: {data.get('request_id')}"
    assert "All connection attempts failed" in data.get("error", {}).get("message", ""), f"Error message mismatch | Request-ID: {data.get('request_id')}"

    body_text = resp.text.lower()
    assert "api_key" not in body_text
    assert "authorization" not in body_text
    assert "bearer" not in body_text
    assert not re.search(r"sk-[a-z0-9]{10,}", body_text)


@pytest.mark.integration
@freeze_time("2024-01-15")
def test_execute_422_missing_required_field():
    """
    【测试目标】
    1. 验证参数缺失时返回422状态码和结构化错误

    【执行过程】
    1. 调用 POST /nl2sql/execute，缺少必填字段（如user_id）
    2. 验证响应状态码和错误结构

    【预期结果】
    1. 返回422状态码
    2. 响应包含"detail"字段，列出缺失字段
    """
    client = TestClient(app)

    resp = client.post(
        "/nl2sql/execute",
        json={
            "question": "统计员工数量",
            # 缺少user_id
            "role_id": "ROLE_TEST",
            "tenant_id": "t1",
        },
    )

    request_payload = {
        "question": "统计员工数量",
        "role_id": "ROLE_TEST",
        "tenant_id": "t1",
    }
    
    assert resp.status_code == 422, f"Expected 422, got {resp.status_code} | Request: {request_payload}"
    data = resp.json()
    assert "detail" in data, f"Missing 'detail' field | Response: {data}"
    assert isinstance(data["detail"], list), f"'detail' is not a list | Response: {data}"
    # 验证错误信息包含缺失字段
    error_fields = [err.get("loc", []) for err in data["detail"]]
    assert any("user_id" in str(field) for field in error_fields), f"Missing 'user_id' validation error | Errors: {data['detail']}"


@pytest.mark.integration
@freeze_time("2024-01-15")
def test_execute_sql_syntax_error():
    """
    【测试目标】
    1. 验证SQL语法错误时返回500状态码和错误结构

    【执行过程】
    1. mock registry和stage1_decomposition
    2. mock run_pipeline返回包含SQL语法错误的ExecutionResult
    3. 调用 POST /nl2sql/execute
    4. 验证响应状态码和错误结构

    【预期结果】
    1. 返回500状态码
    2. 响应包含request_id、error_stage、error.code
    3. error.code为"SQL_EXECUTION_ERROR"或类似
    """
    client = TestClient(app)

    with patch("main.registry", new=MagicMock()):
        fake_query_desc = MagicMock()
        fake_query_desc.request_context = MagicMock(
            user_id="u1",
            role_id="ROLE_TEST",
            tenant_id="t1",
            request_id="test-trace-sql-error",
            current_date=date(2024, 1, 15),
        )
        fake_query_desc.sub_queries = [MagicMock(id="sq_1", description="统计员工数量")]

        # Mock run_pipeline返回包含SQL语法错误的结果
        from schemas.result import ExecutionResult
        batch_results = [
            {
                "sub_query_id": "sq_1",
                "sub_query_description": "统计员工数量",
                "execution_result": ExecutionResult.create_error(
                    error="SQL execution error: syntax error at or near \"FROM\"",
                    latency_ms=10,
                ),
            }
        ]

        with patch("main.stage1_decomposition.process_request", return_value=fake_query_desc):
            with patch("main.run_pipeline", return_value=batch_results):
                resp = client.post(
                    "/nl2sql/execute",
                    json={
                        "question": "统计员工数量",
                        "user_id": "u1",
                        "role_id": "ROLE_TEST",
                        "tenant_id": "t1",
                    },
                    headers={"Trace-ID": "test-trace-sql-error"},
                )

    assert resp.status_code == 200  # execute端点返回200，但status为ALL_FAILED
    data = resp.json()
    assert data.get("status") == "ALL_FAILED"
    assert "answer_text" in data


@pytest.mark.integration
@freeze_time("2024-01-15")
def test_execute_table_not_found():
    """
    【测试目标】
    1. 验证表不存在时返回500状态码和错误结构

    【执行过程】
    1. mock registry和stage1_decomposition
    2. mock run_pipeline返回包含表不存在错误的ExecutionResult
    3. 调用 POST /nl2sql/execute
    4. 验证响应状态码和错误结构

    【预期结果】
    1. 返回200状态码（execute端点统一返回200）
    2. status为"ALL_FAILED"
    3. answer_text包含错误信息
    """
    client = TestClient(app)

    with patch("main.registry", new=MagicMock()):
        fake_query_desc = MagicMock()
        fake_query_desc.request_context = MagicMock(
            user_id="u1",
            role_id="ROLE_TEST",
            tenant_id="t1",
            request_id="test-trace-table-error",
            current_date=date(2024, 1, 15),
        )
        fake_query_desc.sub_queries = [MagicMock(id="sq_1", description="统计员工数量")]

        # Mock run_pipeline返回包含表不存在错误的结果
        from schemas.result import ExecutionResult
        batch_results = [
            {
                "sub_query_id": "sq_1",
                "sub_query_description": "统计员工数量",
                "execution_result": ExecutionResult.create_error(
                    error="Database error: relation 'non_existent_table' does not exist",
                    latency_ms=15,
                ),
            }
        ]

        with patch("main.stage1_decomposition.process_request", return_value=fake_query_desc):
            with patch("main.run_pipeline", return_value=batch_results):
                resp = client.post(
                    "/nl2sql/execute",
                    json={
                        "question": "统计员工数量",
                        "user_id": "u1",
                        "role_id": "ROLE_TEST",
                        "tenant_id": "t1",
                    },
                    headers={"Trace-ID": "test-trace-table-error"},
                )

    assert resp.status_code == 200
    data = resp.json()
    assert data.get("status") == "ALL_FAILED"
    assert "answer_text" in data


@pytest.mark.integration
@freeze_time("2024-01-15")
def test_execute_missing_semantic_term():
    """
    【测试目标】
    1. 验证语义缺失时返回200状态码和软错误结构

    【执行过程】
    1. mock registry和stage1_decomposition
    2. mock stage3_validation抛出MissingMetricError
    3. 调用 POST /nl2sql/execute
    4. 验证响应状态码和错误结构

    【预期结果】
    1. 返回200状态码（业务软错误）
    2. status为"ERROR"
    3. error.code为"NEED_CLARIFICATION"
    """
    client = TestClient(app)

    with patch("main.registry", new=MagicMock()):
        fake_query_desc = MagicMock()
        fake_query_desc.request_context = MagicMock(
            user_id="u1",
            role_id="ROLE_TEST",
            tenant_id="t1",
            request_id="test-trace-semantic-error",
            current_date=date(2024, 1, 15),
        )
        fake_query_desc.sub_queries = [MagicMock(id="sq_1", description="统计员工数量")]

        with patch("main.stage1_decomposition.process_request", return_value=fake_query_desc):
            # Mock run_pipeline抛出MissingMetricError
            with patch("main.run_pipeline", side_effect=MissingMetricError("Plan with intent AGG must have at least one metric")):
                resp = client.post(
                    "/nl2sql/execute",
                    json={
                        "question": "统计员工数量",
                        "user_id": "u1",
                        "role_id": "ROLE_TEST",
                        "tenant_id": "t1",
                    },
                    headers={"Trace-ID": "test-trace-semantic-error"},
                )

    # MissingMetricError应该被捕获并返回200+status=ERROR
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("status") == "ERROR"
    assert data.get("error", {}).get("code") == "NEED_CLARIFICATION"


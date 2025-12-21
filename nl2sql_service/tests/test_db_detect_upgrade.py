"""
【简述】
验证 DB 连接检测与 503 护栏功能：启动时 DB 探测、入口闸门、readiness 端点以及 ALL_FAILED 语义修正。

【范围/不测什么】
- 不覆盖真实数据库连接；仅验证全局状态更新、API 响应状态码与错误结构。
- 不覆盖 uvicorn --reload 兼容性（需手动验证）。

【用例概述】
- test_execute_rejects_when_db_not_ready:
  -- db_ready=false 时 POST /nl2sql/execute 直接返回 503，且 Stage1/AIClient/embedding 未被调用
- test_readiness_returns_200_when_db_ready:
  -- db_ready=true 时 GET /health/ready 返回 200，包含 db_ready=true
- test_readiness_returns_503_when_db_not_ready:
  -- db_ready=false 时 GET /health/ready 返回 503，包含 db_last_error 或 db_last_checked_ts
- test_all_failed_with_db_unavailable_returns_503:
  -- ALL_FAILED 且错误包含 [DB_UNAVAILABLE] 时，最终 HTTP 响应为 503（不是 200）
"""

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from main import app
from schemas.result import ExecutionResult, ExecutionStatus
from schemas.request import RequestContext, SubQueryItem


@pytest.mark.integration
def test_execute_rejects_when_db_not_ready():
    """
    【测试目标】
    1. 验证 db_ready=false 时 POST /nl2sql/execute 直接返回 503
    2. 验证 Stage1/AIClient/embedding 未被调用（避免浪费成本）

    【执行过程】
    1. Mock main.db_ready = False, main.db_last_error = "Connection refused"
    2. Mock registry 确保服务可启动
    3. 调用 POST /nl2sql/execute
    4. 使用 patch 监控 Stage1.process_request、AIClient.chat_json、Jina embedding 调用次数

    【预期结果】
    1. 响应状态码为 503
    2. 响应 JSON 包含 error_type="DB_UNAVAILABLE"
    3. Stage1.process_request 调用次数为 0
    4. AIClient.chat_json 调用次数为 0（通过监控相关方法）
    5. Jina embedding 相关调用次数为 0
    """
    client = TestClient(app)
    
    # Mock db_ready 为 False
    with patch("main.db_ready", False), \
         patch("main.db_last_error", "Connection refused"), \
         patch("main.db_last_checked_ts", 1234567890.0), \
         patch("main._db_state_lock.__aenter__", return_value=None), \
         patch("main._db_state_lock.__aexit__", return_value=None):
        
        # Mock registry 确保服务可启动
        with patch("main.registry", new=MagicMock()):
            # 监控 Stage1、AIClient 和 embedding 调用
            with patch("main.stage1_decomposition.process_request") as mock_stage1, \
                 patch("core.ai_client.get_ai_client") as mock_get_ai_client, \
                 patch("core.semantic_registry.SemanticRegistry.get_instance") as mock_registry_get:
                
                # Mock AIClient
                mock_ai_client = MagicMock()
                mock_ai_client.chat_json = AsyncMock()
                mock_ai_client.get_embeddings = AsyncMock()
                mock_get_ai_client.return_value = mock_ai_client
                
                # Mock registry
                mock_registry_instance = MagicMock()
                mock_registry_get.return_value = mock_registry_instance
                
                resp = client.post(
                    "/nl2sql/execute",
                    json={
                        "question": "统计每个部门的员工数量",
                        "user_id": "u1",
                        "role_id": "ROLE_TEST",
                        "tenant_id": "t1",
                        "include_trace": False,
                    },
                )
    
    # 验证响应
    assert resp.status_code == 503, f"Expected 503, got {resp.status_code}"
    data = resp.json()
    assert data.get("detail", {}).get("error_type") == "DB_UNAVAILABLE"
    
    # 验证 Stage1 未被调用
    assert mock_stage1.call_count == 0, "Stage1.process_request should not be called when db_ready=False"
    
    # 验证 AIClient 方法未被调用
    mock_get_ai_client.assert_not_called()


@pytest.mark.integration
def test_readiness_returns_200_when_db_ready():
    """
    【测试目标】
    1. 验证 db_ready=true 时 GET /health/ready 返回 200

    【执行过程】
    1. Mock main.db_ready = True
    2. Mock main.db_last_ok_ts 和 db_last_checked_ts
    3. 调用 GET /health/ready

    【预期结果】
    1. 响应状态码为 200
    2. 响应 JSON 包含 db_ready=true
    """
    client = TestClient(app)
    
    with patch("main.db_ready", True), \
         patch("main.db_last_error", None), \
         patch("main.db_last_ok_ts", 1234567890.0), \
         patch("main.db_last_checked_ts", 1234567890.0), \
         patch("main._db_state_lock.__aenter__", return_value=None), \
         patch("main._db_state_lock.__aexit__", return_value=None):
        
        resp = client.get("/health/ready")
    
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    data = resp.json()
    assert data.get("db_ready") is True


@pytest.mark.integration
def test_readiness_returns_503_when_db_not_ready():
    """
    【测试目标】
    1. 验证 db_ready=false 时 GET /health/ready 返回 503
    2. 验证响应包含 db_last_error 或 db_last_checked_ts

    【执行过程】
    1. Mock main.db_ready = False, main.db_last_error = "Connection refused"
    2. Mock main.db_last_checked_ts
    3. 调用 GET /health/ready

    【预期结果】
    1. 响应状态码为 503
    2. 响应 JSON 包含 db_ready=false
    3. 响应 JSON 包含 db_last_error 或 db_last_checked_ts
    """
    client = TestClient(app)
    
    with patch("main.db_ready", False), \
         patch("main.db_last_error", "Connection refused"), \
         patch("main.db_last_ok_ts", None), \
         patch("main.db_last_checked_ts", 1234567890.0), \
         patch("main._db_state_lock.__aenter__", return_value=None), \
         patch("main._db_state_lock.__aexit__", return_value=None):
        
        resp = client.get("/health/ready")
    
    assert resp.status_code == 503, f"Expected 503, got {resp.status_code}"
    data = resp.json()
    assert data.get("db_ready") is False
    assert data.get("db_last_error") == "Connection refused" or data.get("db_last_checked_ts") is not None


@pytest.mark.integration
def test_all_failed_with_db_unavailable_returns_503():
    """
    【测试目标】
    1. 验证 ALL_FAILED 且错误包含 [DB_UNAVAILABLE] 时，最终 HTTP 响应为 503（不是 200）

    【执行过程】
    1. Mock registry 和 stage1_decomposition
    2. Mock run_pipeline 返回包含 [DB_UNAVAILABLE] 错误的 batch_results
    3. Mock db_ready=True（模拟启动后 DB 中途掉线场景）
    4. 调用 POST /nl2sql/execute

    【预期结果】
    1. 最终 HTTP 响应状态码为 503（不是 200）
    2. 响应 JSON 包含 error_type="DB_UNAVAILABLE"
    """
    client = TestClient(app)
    
    # 创建包含 DB_UNAVAILABLE 错误的 ExecutionResult
    db_error_result = ExecutionResult.create_error(
        error="[DB_UNAVAILABLE] Database connection error: Connection refused",
        latency_ms=100
    )
    
    # Mock batch_results
    batch_results = [{
        "sub_query_id": "sq_1",
        "sub_query_description": "统计每个部门的员工数量",
        "execution_result": db_error_result
    }]
    
    # Mock registry
    with patch("main.registry", new=MagicMock()):
        # Mock db_ready=True（模拟启动后 DB 中途掉线）
        with patch("main.db_ready", True), \
             patch("main._db_state_lock.__aenter__", return_value=None), \
             patch("main._db_state_lock.__aexit__", return_value=None):
            
            # Mock stage1_decomposition
            fake_query_desc = MagicMock()
            fake_query_desc.request_context = MagicMock(
                user_id="u1",
                role_id="ROLE_TEST",
                tenant_id="t1",
                request_id="test-trace-003",
                current_date=date(2024, 1, 15),
            )
            fake_query_desc.sub_queries = [SubQueryItem(
                id="sq_1",
                description="统计每个部门的员工数量"
            )]
            
            with patch("main.stage1_decomposition.process_request", return_value=fake_query_desc):
                # Mock run_pipeline 返回包含 DB_UNAVAILABLE 错误的结果
                with patch("main.run_pipeline", return_value=batch_results):
                    resp = client.post(
                        "/nl2sql/execute",
                        json={
                            "question": "统计每个部门的员工数量",
                            "user_id": "u1",
                            "role_id": "ROLE_TEST",
                            "tenant_id": "t1",
                            "include_trace": False,
                        },
                    )
    
    # 验证响应
    assert resp.status_code == 503, f"Expected 503, got {resp.status_code}. Response: {resp.json()}"
    data = resp.json()
    assert data.get("detail", {}).get("error_type") == "DB_UNAVAILABLE"


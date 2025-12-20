"""
【简述】
验证 Request ID Middleware 的 Trace-ID 读取、响应头回写与日志 contextvar 传递正确性。

【范围/不测什么】
- 不覆盖真实 API 业务逻辑；仅验证中间件的 ID 生成、响应头注入与日志链路传递。

【用例概述】
- test_generate_ids_when_no_headers:
  -- 验证无 header 时自动生成 request_id 并回写到响应头
- test_echo_trace_id:
  -- 验证带 Trace-ID header 时响应头回写相同值
- test_422_still_has_headers:
  -- 验证 422 错误响应仍包含 Trace-ID
- test_422_with_trace_id_header:
  -- 验证 422 错误时回写用户提供的 Trace-ID
- test_logger_captures_request_id_no_header:
  -- 验证无 header 时日志捕获自动生成的 request_id
- test_logger_captures_request_id_with_trace_id:
  -- 验证带 Trace-ID header 时日志捕获正确的 request_id
- test_logger_contextvar_persistence:
  -- 验证同一请求上下文中多次日志调用都获取相同 request_id
"""

import io

import pytest
from fastapi.testclient import TestClient
from loguru import logger

from main import app
from utils.log_manager import get_logger

test_logger = get_logger(__name__)


# ============================================================
# Test Fixtures
# ============================================================

# client fixture 已统一到 conftest.py，这里不再重复定义


@pytest.fixture
def log_capture():
    """
    日志捕获 fixture
    
    创建一个临时的 loguru sink 来捕获日志，测试结束后自动清理。
    使用 StringIO 来捕获格式化的日志消息。
    每个测试都会创建新的 StringIO 实例，确保日志隔离。
    """
    # 创建新的 StringIO 实例用于捕获日志
    captured_logs = io.StringIO()
    
    # 添加临时 sink，格式包含 extra[request_id]
    handler_id = logger.add(
        captured_logs,
        format="[{extra[request_id]}] {message}",
        level="INFO",
        enqueue=False  # 同步模式，确保立即写入
    )
    
    yield captured_logs
    
    # 清理：移除临时 sink
    logger.remove(handler_id)


# ============================================================
# 接口契约测试
# ============================================================

@pytest.mark.unit
def test_generate_ids_when_no_headers(client):
    """
    【测试目标】
    1. 验证无 header 时自动生成 request_id 并回写到响应头

    【执行过程】
    1. 调用 GET / 不带任何 header
    2. 检查响应头中的 Trace-ID

    【预期结果】
    1. 响应状态码为 200
    2. 响应头包含 Trace-ID
    3. Trace-ID 以 "req-" 开头
    """
    response = client.get("/")
    
    assert response.status_code == 200
    
    # 断言响应头包含 Trace-ID
    assert "Trace-ID" in response.headers
    
    # 断言以 "req-" 开头
    trace_id = response.headers["Trace-ID"]
    assert trace_id.startswith("req-")


@pytest.mark.unit
def test_echo_trace_id(client):
    """
    【测试目标】
    1. 验证带 Trace-ID header 时响应头回写相同值

    【执行过程】
    1. 调用 GET / 带 Trace-ID="trace-test-001"
    2. 检查响应头中的 Trace-ID

    【预期结果】
    1. 响应状态码为 200
    2. 响应头 Trace-ID 值为 "trace-test-001"
    """
    response = client.get(
        "/",
        headers={"Trace-ID": "trace-test-001"}
    )
    
    assert response.status_code == 200
    
    # 断言响应头回写了相同的值
    assert response.headers["Trace-ID"] == "trace-test-001"






@pytest.mark.unit
def test_422_still_has_headers(client):
    """
    【测试目标】
    1. 验证 422 错误响应仍包含 Trace-ID

    【执行过程】
    1. 调用 POST /nl2sql/plan 发送空请求体
    2. 触发 422 验证错误
    3. 检查响应头

    【预期结果】
    1. 响应状态码为 422
    2. 响应头包含 Trace-ID
    """
    # 发送空的请求体，触发 422
    response = client.post(
        "/nl2sql/plan",
        json={}
    )
    
    # 断言状态码是 422
    assert response.status_code == 422
    
    # 断言响应头仍然包含 Trace-ID
    assert "Trace-ID" in response.headers


@pytest.mark.unit
def test_422_with_trace_id_header(client):
    """
    【测试目标】
    1. 验证 422 错误时回写用户提供的 Trace-ID

    【执行过程】
    1. 调用 POST /nl2sql/plan 发送空请求体，带 Trace-ID="trace-422-test"
    2. 触发 422 验证错误
    3. 检查响应头

    【预期结果】
    1. 响应状态码为 422
    2. 响应头 Trace-ID 值为 "trace-422-test"
    """
    response = client.post(
        "/nl2sql/plan",
        json={},
        headers={"Trace-ID": "trace-422-test"}
    )
    
    assert response.status_code == 422
    
    # 断言响应头回写了相同的值
    assert response.headers["Trace-ID"] == "trace-422-test"


# ============================================================
# 日志链路测试
# ============================================================

@pytest.mark.integration
def test_logger_captures_request_id_no_header(client, log_capture):
    """
    【测试目标】
    1. 验证无 header 时日志捕获自动生成的 request_id

    【执行过程】
    1. 添加临时测试路由并调用 logger.info
    2. 调用该路由不带 header
    3. 检查捕获的日志内容

    【预期结果】
    1. 响应状态码为 200
    2. 捕获的日志包含 "[req-" 前缀
    """
    # 添加临时测试路由
    @app.get("/__log_test")
    async def log_test_endpoint():
        test_logger.info("log_test")
        return {"status": "ok"}
    
    try:
        # 调用测试路由（不带 header）
        response = client.get("/__log_test")
        
        assert response.status_code == 200
        
        # 等待日志处理完成（TestClient 是同步的，但 loguru 可能异步处理）
        # 由于我们使用的是同步 sink，应该立即捕获到
        
        # 获取捕获的日志内容
        log_content = log_capture.getvalue()
        
        # 断言捕获的日志中包含 "[req-"
        assert "[req-" in log_content
        
    finally:
        # 清理：移除临时路由（通过重新创建 app 或使用路由移除方法）
        # 注意：FastAPI 不支持直接移除路由，但测试隔离可以通过 fixture 实现
        # 这里我们依赖测试隔离，实际项目中可以考虑使用 fixture 级别的 app 实例
        pass


@pytest.mark.integration
def test_logger_captures_request_id_with_trace_id(client, log_capture):
    """
    【测试目标】
    1. 验证带 Trace-ID header 时日志捕获正确的 request_id

    【执行过程】
    1. 添加临时测试路由并调用 logger.info
    2. 调用该路由带 Trace-ID="trace-test-001"
    3. 检查捕获的日志内容

    【预期结果】
    1. 响应状态码为 200
    2. 捕获的日志包含 "[trace-test-001]"
    """
    # 添加临时测试路由
    @app.get("/__log_test_trace")
    async def log_test_trace_endpoint():
        test_logger.info("log_test_trace")
        return {"status": "ok"}
    
    try:
        # 调用测试路由（带 Trace-ID header）
        response = client.get(
            "/__log_test_trace",
            headers={"Trace-ID": "trace-test-001"}
        )
        
        assert response.status_code == 200
        
        # 获取捕获的日志内容
        log_content = log_capture.getvalue()
        
        # 断言捕获的日志中包含 "[trace-test-001]"
        assert "[trace-test-001]" in log_content
        
    finally:
        # 清理：移除临时路由
        pass


@pytest.mark.integration
def test_logger_contextvar_persistence(client, log_capture):
    """
    【测试目标】
    1. 验证同一请求上下文中多次日志调用都获取相同 request_id

    【执行过程】
    1. 添加临时测试路由，内部调用 3 次 logger.info
    2. 调用该路由带 Trace-ID="trace-multiple-test"
    3. 检查捕获的日志内容

    【预期结果】
    1. 响应状态码为 200
    2. 捕获的日志至少包含 3 条带相同 trace_id 的记录
    """
    # 添加临时测试路由，内部多次调用 logger
    @app.get("/__log_test_multiple")
    async def log_test_multiple_endpoint():
        test_logger.info("log_test_1")
        test_logger.info("log_test_2")
        test_logger.info("log_test_3")
        return {"status": "ok"}
    
    try:
        # 调用测试路由（带 Trace-ID header）
        trace_id = "trace-multiple-test"
        response = client.get(
            "/__log_test_multiple",
            headers={"Trace-ID": trace_id}
        )
        
        assert response.status_code == 200
        
        # 获取捕获的日志内容
        log_content = log_capture.getvalue()
        
        # 断言捕获的日志都包含相同的 request_id
        # 应该包含 3 条日志，每条都带有相同的 trace_id
        assert log_content.count(f"[{trace_id}]") >= 3
        
    finally:
        # 清理：移除临时路由
        pass

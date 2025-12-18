"""
Request ID Middleware and Logger Test Suite

测试 Request ID Middleware 的正确性，包括：
- Header 读取：Trace-ID > 自动生成
- 响应头回写：无论成功还是 422，都要回写 Trace-ID
- 日志链路：验证 contextvar 在日志中正确传递 request_id
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

@pytest.fixture
def client():
    """创建 TestClient 实例"""
    return TestClient(app)


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

def test_generate_ids_when_no_headers(client):
    """
    测试：不带 header 时，自动生成 request_id 并回写到响应头
    
    断言：
    - response.headers 包含 Trace-ID
    - 以 "req-" 开头
    """
    response = client.get("/")
    
    assert response.status_code == 200
    
    # 断言响应头包含 Trace-ID
    assert "Trace-ID" in response.headers
    
    # 断言以 "req-" 开头
    trace_id = response.headers["Trace-ID"]
    assert trace_id.startswith("req-")


def test_echo_trace_id(client):
    """
    测试：带 Trace-ID header 时，回写相同的值
    
    断言：
    - response.headers["Trace-ID"] == "trace-test-001"
    """
    response = client.get(
        "/",
        headers={"Trace-ID": "trace-test-001"}
    )
    
    assert response.status_code == 200
    
    # 断言响应头回写了相同的值
    assert response.headers["Trace-ID"] == "trace-test-001"






def test_422_still_has_headers(client):
    """
    测试：422 错误时，响应头仍然包含 Trace-ID
    
    发送一个无效的请求体，触发 422 验证错误。
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


def test_422_with_trace_id_header(client):
    """
    测试：422 错误时，如果提供了 Trace-ID，应该回写相同的值
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

def test_logger_captures_request_id_no_header(client, log_capture):
    """
    测试：无 header 时，日志中能捕获到自动生成的 request_id
    
    1. 添加临时路由用于测试日志
    2. 调用该路由
    3. 断言捕获的日志包含 "[req-"
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


def test_logger_captures_request_id_with_trace_id(client, log_capture):
    """
    测试：带 Trace-ID header 时，日志中能捕获到正确的 request_id
    
    1. 添加临时路由用于测试日志
    2. 调用该路由，带 Trace-ID header
    3. 断言捕获的日志包含 "[trace-test-001]"
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


def test_logger_contextvar_persistence(client, log_capture):
    """
    测试：在同一个请求上下文中，多次日志调用都能获取到相同的 request_id
    
    验证 contextvar 在异步环境中正确传递。
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

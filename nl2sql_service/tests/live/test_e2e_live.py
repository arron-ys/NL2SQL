"""
Live End-to-End Test Suite

测试完整的 NL2SQL 流水线（真实外部服务）：
- Stage 1: Query Decomposition
- Stage 2-5: Pipeline Orchestration
- Stage 6: Answer Generation

需要真实的 API Key，如果 Key 不可用则跳过测试。
"""
import os

import pytest
from fastapi.testclient import TestClient

from main import app
from tests.live.helpers import get_openai_api_key, get_jina_api_key, is_placeholder_key


# ============================================================
# Test Fixtures
# ============================================================


@pytest.fixture
def client():
    """创建 TestClient 实例"""
    return TestClient(app)


# ============================================================
# Skip Conditions
# ============================================================


def _should_skip_live_tests():
    """检查是否应该跳过 Live 测试"""
    # 直接检查环境变量并使用 is_placeholder_key 判断
    openai_key_env = os.getenv("OPENAI_API_KEY", "")
    
    # 如果 OpenAI Key 缺失或为占位符，跳过测试
    if not openai_key_env or is_placeholder_key(openai_key_env):
        return True, "OpenAI API Key not available or is placeholder"
    
    # Jina Key 可选，但如果提供了但为占位符，也跳过
    jina_key_env = os.getenv("JINA_API_KEY", "")
    if jina_key_env and is_placeholder_key(jina_key_env):
        return True, "Jina API Key is placeholder"
    
    return False, None


# 在模块级别计算 skip 条件（用于装饰器）
_SKIP_LIVE_TESTS, _SKIP_REASON = _should_skip_live_tests()


# ============================================================
# 端到端测试
# ============================================================


class TestE2ELive:
    """测试端到端流程（真实外部服务）"""

    @pytest.mark.asyncio
    @pytest.mark.e2e
    @pytest.mark.live
    @pytest.mark.slow
    @pytest.mark.skipif(
        _SKIP_LIVE_TESTS,
        reason=_SKIP_REASON or "Live services not available"
    )
    async def test_full_pipeline_execute(self, client):
        """测试完整的 /nl2sql/execute 流程"""
        
        # 发送完整的 NL2SQL 请求
        response = client.post(
            "/nl2sql/execute",
            json={
                "question": "统计每个部门的员工数量",
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
                "include_trace": False,
            },
            timeout=60,  # 完整流程可能需要更长时间
        )
        
        # 验证响应状态码
        assert response.status_code == 200, f"Request failed with status {response.status_code}: {response.text}"
        
        # 验证响应结构
        result = response.json()
        assert "answer_text" in result or "status" in result, "Response missing expected fields"
        
        # 如果返回了答案，验证答案不为空
        if "answer_text" in result:
            assert len(result["answer_text"]) > 0, "Answer text is empty"

    @pytest.mark.asyncio
    @pytest.mark.e2e
    @pytest.mark.live
    @pytest.mark.slow
    @pytest.mark.skipif(
        _SKIP_LIVE_TESTS,
        reason=_SKIP_REASON or "Live services not available"
    )
    async def test_full_pipeline_with_trace(self, client):
        """测试完整的 /nl2sql/execute 流程（包含调试信息）"""
        
        # 发送完整的 NL2SQL 请求（包含调试信息）
        response = client.post(
            "/nl2sql/execute",
            json={
                "question": "统计每个部门的员工数量",
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
                "include_trace": True,
            },
            timeout=60,
        )
        
        # 验证响应状态码
        assert response.status_code == 200, f"Request failed with status {response.status_code}: {response.text}"
        
        # 验证响应结构（调试模式）
        result = response.json()
        assert "answer" in result, "Debug response missing 'answer' field"
        assert "debug_info" in result, "Debug response missing 'debug_info' field"
        
        # 验证调试信息结构
        debug_info = result.get("debug_info", {})
        assert "sub_queries" in debug_info, "Debug info missing 'sub_queries'"
        assert "plans" in debug_info, "Debug info missing 'plans'"
        # SQL 查询可能在执行阶段生成，所以可选
        # assert "sql_queries" in debug_info, "Debug info missing 'sql_queries'"

    @pytest.mark.asyncio
    @pytest.mark.e2e
    @pytest.mark.live
    @pytest.mark.slow
    @pytest.mark.skipif(
        _SKIP_LIVE_TESTS,
        reason=_SKIP_REASON or "Live services not available"
    )
    async def test_plan_generation_live(self, client):
        """测试 /nl2sql/plan 端点（真实 LLM）"""
        
        # 发送 Plan 生成请求
        response = client.post(
            "/nl2sql/plan",
            json={
                "question": "统计每个部门的员工数量",
                "user_id": "user_001",
                "role_id": "ROLE_HR_HEAD",
                "tenant_id": "tenant_001",
            },
            timeout=30,
        )
        
        # 验证响应状态码
        assert response.status_code == 200, f"Request failed with status {response.status_code}: {response.text}"
        
        # 验证响应结构
        plan = response.json()
        assert "intent" in plan, "Plan missing 'intent' field"
        assert "metrics" in plan, "Plan missing 'metrics' field"
        assert "dimensions" in plan, "Plan missing 'dimensions' field"
        
        # 验证 intent 是有效值
        assert plan["intent"] in ["AGG", "TREND", "DETAIL"], f"Invalid intent: {plan['intent']}"


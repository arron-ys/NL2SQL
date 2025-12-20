"""
【简述】
验证 JinaProvider 代理模式配置规则与 Stage2 向量检索故障的严格失败机制。

【范围/不测什么】
- 不覆盖真实 Jina API 调用；仅验证代理配置逻辑与向量检索失败时的异常传播。

【用例概述】
- test_jina_provider_explicit_mode_does_not_use_system_env_proxy:
  -- 验证 PROXY_MODE=explicit 时 JinaProvider 强制 trust_env=False
- test_stage2_vector_search_failure_is_strict_and_has_error_code:
  -- 验证 Stage2 向量检索失败时严格抛出异常且包含稳定 error_code
"""

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.providers.jina_provider import JinaProvider, JinaEmbeddingError
from schemas.request import RequestContext, SubQueryItem


@pytest.mark.unit
def test_jina_provider_explicit_mode_does_not_use_system_env_proxy(monkeypatch):
    """
    【测试目标】
    1. 验证 PROXY_MODE=explicit 且 JINA_PROXY 为空时 JinaProvider 强制 trust_env=False

    【执行过程】
    1. 设置 HTTP_PROXY 和 HTTPS_PROXY 系统环境变量
    2. 设置 PROXY_MODE=explicit, PROXY_STRICT=0
    3. 删除 JINA_PROXY 环境变量
    4. spy httpx.AsyncClient.__init__ 捕获初始化参数
    5. 初始化 JinaProvider

    【预期结果】
    1. trust_env 为 False
    2. kwargs 中不包含 proxy 参数
    """
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:7897")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:7897")
    monkeypatch.setenv("PROXY_MODE", "explicit")
    monkeypatch.setenv("PROXY_STRICT", "0")
    monkeypatch.delenv("JINA_PROXY", raising=False)

    captured = {}
    import httpx

    real_init = httpx.AsyncClient.__init__

    def _spy_init(self, *args, **kwargs):
        captured["kwargs"] = dict(kwargs)
        return real_init(self, *args, **kwargs)

    with patch("httpx.AsyncClient.__init__", new=_spy_init):
        provider = JinaProvider(api_key="fake-jina-key", base_url="https://api.jina.ai/v1")
        assert provider is not None

    assert captured["kwargs"].get("trust_env") is False
    assert "proxy" not in captured["kwargs"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stage2_vector_search_failure_is_strict_and_has_error_code():
    """
    【测试目标】
    1. 验证 Stage2 向量检索失败时严格抛出异常且包含稳定 error_code

    【执行过程】
    1. 准备 SubQueryItem 和 RequestContext
    2. mock registry 关键词索引可匹配但向量检索抛出 JinaEmbeddingError
    3. 调用 process_subquery
    4. 捕获 VectorSearchFailed 异常并检查属性

    【预期结果】
    1. 抛出 VectorSearchFailed 异常
    2. 异常 code 为 "EMBEDDING_UNAVAILABLE"
    3. 异常消息包含原始错误 "All connection attempts failed"
    """
    from stages.stage2_plan_generation import process_subquery, VectorSearchFailed

    sub_query = SubQueryItem(id="q1", description="统计每个部门的员工数量")
    context = RequestContext(
        user_id="u1",
        role_id="ROLE_TEST",
        request_id="req-123",
        current_date=date.today(),
    )

    # registry：关键词可匹配，但向量检索直接失败（模拟 embedding 不可用）
    mock_registry = MagicMock()
    mock_registry.get_allowed_ids.return_value = set()
    mock_registry.keyword_index = {"员工": ["METRIC_HEADCOUNT"], "部门": ["DIM_DEPARTMENT"]}
    mock_registry.search_similar_terms = AsyncMock(
        side_effect=JinaEmbeddingError("All connection attempts failed", details={"provider": "jina"})
    )
    mock_registry.get_term.return_value = {"id": "METRIC_HEADCOUNT", "name": "在职人数（Headcount）", "metric_type": "AGG"}

    mock_config = MagicMock()
    mock_config.vector_search_top_k = 20
    mock_config.similarity_threshold = 0.4
    mock_config.max_term_recall = 20

    with patch("stages.stage2_plan_generation.get_pipeline_config", return_value=mock_config):
        with pytest.raises(VectorSearchFailed) as exc:
            await process_subquery(sub_query, context, mock_registry)

    assert getattr(exc.value, "code", None) == "EMBEDDING_UNAVAILABLE"
    assert "All connection attempts failed" in str(exc.value)



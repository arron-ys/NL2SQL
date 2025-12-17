"""
Jina Proxy & Stage2 Vector Required Test Suite

覆盖：
1) PROXY_MODE=explicit 时，JinaProvider 默认 trust_env=False（不读取 HTTP_PROXY/HTTPS_PROXY）
2) Stage2 向量检索 REQUIRED：embedding/向量检索失败必须严格失败，并携带明确 error_code/message
"""

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.providers.jina_provider import JinaProvider, JinaEmbeddingError
from schemas.request import RequestContext, SubQueryItem


def test_jina_provider_explicit_mode_does_not_use_system_env_proxy(monkeypatch):
    """
    monkeypatch 设置 HTTP_PROXY/HTTPS_PROXY=127.0.0.1:7897，同时 PROXY_MODE=explicit 且 JINA_PROXY 为空：
    断言 JinaProvider 创建 httpx.AsyncClient 时 trust_env=False（不会读系统 env 代理）。
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


@pytest.mark.asyncio
async def test_stage2_vector_search_failure_is_strict_and_has_error_code():
    """
    向量检索 REQUIRED：embedding/向量检索失败必须直接失败（不允许降级继续生成 Plan）。
    断言抛出的异常具备稳定 code，且 message 含原始异常摘要。
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



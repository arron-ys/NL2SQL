"""
Jina Provider Module

实现 Jina AI 的提供商适配器（主要用于嵌入）。
"""
import os
import socket
from urllib.parse import urlparse
from typing import Any, AsyncIterator, Dict, List, Optional

import httpx

from core.errors import AppError
from utils.log_manager import get_logger
from .base import BaseAIProvider

logger = get_logger(__name__)


class JinaEmbeddingError(AppError):
    """
    Jina embedding 不可用错误（用于严格失败 + 可诊断）。
    """
    def __init__(self, message: str, *, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            code="EMBEDDING_UNAVAILABLE",
            message=message,
            error_stage="STAGE_2_PLAN_GENERATION",
            details=details or {},
            status_code=500,
        )


class JinaProvider(BaseAIProvider):
    """
    Jina AI 提供商实现
    
    目前仅支持嵌入功能，其他功能暂未实现。
    """
    
    def __init__(
        self,
        api_key: str,
        base_url: Optional[str] = None
    ):
        """
        初始化 Jina 提供商
        
        Args:
            api_key: Jina API Key
            base_url: Jina Base URL，如果为 None 则使用官方 API
        """
        self.api_key = api_key
        self.base_url = base_url or "https://api.jina.ai/v1"
        self.api_url = f"{self.base_url}/embeddings"
        
        # 设置超时时间（优先级：LLM_TIMEOUT > JINA_TIMEOUT > 默认值 30.0）
        timeout_str = os.getenv("LLM_TIMEOUT") or os.getenv("JINA_TIMEOUT", "30.0")
        timeout = float(timeout_str)

        def _is_proxy_reachable(proxy_url: str) -> bool:
            """
            快速探测代理是否可连通（仅 TCP 探测）。
            """
            try:
                parsed = urlparse(proxy_url)
                host = parsed.hostname
                port = parsed.port
                if not host or not port:
                    return True
                with socket.create_connection((host, port), timeout=0.5):
                    return True
            except Exception:
                return False

        # ============================================================
        # Proxy 统一控制（与 OpenAIProvider 对齐）
        #
        # PROXY_MODE:
        # - none:     禁用代理，trust_env=False
        # - explicit: 仅使用 JINA_PROXY，trust_env=False（默认）
        # - system:   允许读取 HTTP_PROXY/HTTPS_PROXY/ALL_PROXY，trust_env=True
        #
        # PROXY_STRICT:
        # - 1: 显式代理不可达 => 直接报错（提示 JINA_PROXY 与 proxy_url）
        # - 0: 显式代理不可达 => 降级直连，但仍 trust_env=False（避免系统 env 劫持）
        # ============================================================
        proxy_mode = (os.getenv("PROXY_MODE") or "explicit").strip().lower()
        if proxy_mode not in {"none", "explicit", "system"}:
            proxy_mode = "explicit"
        proxy_strict_raw = (os.getenv("PROXY_STRICT") or "").strip()
        proxy_strict = proxy_strict_raw in {"1", "true", "TRUE", "yes", "YES"}

        self._proxy_mode: str = proxy_mode
        self._proxy_strict: bool = proxy_strict
        self._trust_env: bool = proxy_mode == "system"
        self._proxy_source: str = "none"  # none|explicit|system
        self._proxy_url: Optional[str] = None
        self._proxy_downgraded: bool = False
        self._proxy_disabled_reason: Optional[str] = None

        explicit_proxy = os.getenv("JINA_PROXY")

        if proxy_mode == "none":
            self._proxy_source = "none"
            self._proxy_url = None
        elif proxy_mode == "explicit":
            self._proxy_source = "explicit" if explicit_proxy else "none"
            self._proxy_url = explicit_proxy
        else:  # system
            # system 模式下只信任系统 env proxy；不使用 JINA_PROXY（避免混用）
            self._proxy_source = "system"
            self._proxy_url = None

        # 显式代理可达性检查（与 OpenAIProvider 一致）
        if self._proxy_source == "explicit" and self._proxy_url:
            if not _is_proxy_reachable(self._proxy_url):
                self._proxy_disabled_reason = "unreachable"
                logger.warning(
                    "Jina proxy is configured but unreachable",
                    extra={
                        "provider": "jina",
                        "proxy_mode": self._proxy_mode,
                        "proxy_strict": self._proxy_strict,
                        "proxy_source": self._proxy_source,
                        "proxy_url": self._proxy_url,
                        "trust_env": self._trust_env,
                    },
                )
                if self._proxy_strict:
                    raise ConnectionError(
                        f"JINA_PROXY is set but unreachable: {self._proxy_url}. "
                        f"Start your proxy process or set JINA_PROXY to the correct URL/port. "
                        f"(PROXY_MODE={self._proxy_mode}, PROXY_STRICT=true)"
                    )
                # strict=false：降级直连，且必须 trust_env=False（避免被 HTTP_PROXY/HTTPS_PROXY 劫持）
                self._proxy_url = None
                self._proxy_source = "none"
                self._trust_env = False
                self._proxy_downgraded = True
                logger.warning(
                    "Jina proxy unreachable; downgraded to direct connection with trust_env=False",
                    extra={"provider": "jina"},
                )

        # 初始化异步 HTTP 客户端（显式设置 trust_env）
        client_kwargs: Dict[str, Any] = {"timeout": timeout, "trust_env": bool(self._trust_env)}
        if self._proxy_source == "explicit" and self._proxy_url:
            client_kwargs["proxy"] = self._proxy_url
        self._client = httpx.AsyncClient(**client_kwargs)
        
        logger.info(
            "JinaProvider initialized",
            extra={
                "base_url": self.base_url,
                "timeout": timeout,
                "proxy_mode": self._proxy_mode,
                "proxy_strict": self._proxy_strict,
                "trust_env": self._trust_env,
                "proxy_source": self._proxy_source,
                "proxy_url": self._proxy_url,
                "proxy_downgraded": self._proxy_downgraded,
                "proxy_disabled_reason": self._proxy_disabled_reason,
            }
        )
    
    async def embed(
        self,
        texts: List[str],
        model: str = "jina-embeddings-v3",
        **kwargs: Any
    ) -> List[List[float]]:
        """生成文本嵌入向量"""
        if not self.api_key:
            raise ValueError("JINA_API_KEY is required")
        
        try:
            # 调用 Jina 嵌入 API
            response = await self._client.post(
                self.api_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": model,
                    "input": texts,
                    **kwargs
                }
            )
            response.raise_for_status()
            data = response.json()
            
            # Jina API 返回格式: {"data": [{"embedding": [...]}, ...]}
            embeddings = [item["embedding"] for item in data["data"]]
            
            logger.debug(
                "Jina embedding generated",
                extra={
                    "model": model,
                    "text_count": len(texts),
                    "dimension": len(embeddings[0]) if embeddings else 0
                }
            )
            
            return embeddings
        
        except httpx.HTTPStatusError as e:
            logger.error(
                "Jina embedding HTTP error",
                extra={
                    "error": str(e),
                    "status_code": e.response.status_code,
                    "response": e.response.text[:200] if e.response else None
                }
            )
            raise JinaEmbeddingError(
                "Jina embedding HTTP error",
                details={
                    "provider": "jina",
                    "base_url": self.base_url,
                    "api_url": self.api_url,
                    "proxy_mode": getattr(self, "_proxy_mode", None),
                    "proxy_strict": getattr(self, "_proxy_strict", None),
                    "trust_env": getattr(self, "_trust_env", None),
                    "proxy_source": getattr(self, "_proxy_source", None),
                    "proxy_url": getattr(self, "_proxy_url", None),
                    "proxy_downgraded": getattr(self, "_proxy_downgraded", None),
                    "status_code": e.response.status_code if e.response else None,
                    "response_preview": e.response.text[:200] if e.response else None,
                    "error": str(e),
                },
            ) from e
        except Exception as e:
            logger.error(
                "Jina embedding failed",
                extra={
                    "error": str(e),
                    "model": model
                }
            )
            diag = {
                "provider": "jina",
                "base_url": self.base_url,
                "api_url": self.api_url,
                "proxy_mode": getattr(self, "_proxy_mode", None),
                "proxy_strict": getattr(self, "_proxy_strict", None),
                "trust_env": getattr(self, "_trust_env", None),
                "proxy_source": getattr(self, "_proxy_source", None),
                "proxy_url": getattr(self, "_proxy_url", None),
                "proxy_downgraded": getattr(self, "_proxy_downgraded", None),
                "explicit_proxy_configured": bool(os.getenv("JINA_PROXY")),
                "system_env_proxy_present": bool(os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("ALL_PROXY")),
                "error_type": type(e).__name__,
                "error": str(e),
                "model": model,
            }
            msg = (
                "Jina embedding failed "
                f"(provider=jina, base_url={self.base_url}, proxy_mode={diag['proxy_mode']}, "
                f"trust_env={diag['trust_env']}, proxy_source={diag['proxy_source']}, proxy_url={diag['proxy_url']}). "
                f"Underlying error: {type(e).__name__}: {str(e)}"
            )
            raise JinaEmbeddingError(msg, details=diag) from e
    
    async def chat(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        **kwargs: Any
    ) -> str:
        """执行聊天补全（返回文本）"""
        raise NotImplementedError("JinaProvider does not support chat yet.")
    
    async def chat_json(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """执行聊天补全（返回 JSON 对象）"""
        raise NotImplementedError("JinaProvider does not support chat_json yet.")
    
    async def stream_chat(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        **kwargs: Any
    ) -> AsyncIterator[str]:
        """流式聊天补全（用于 WebSocket 推送）"""
        raise NotImplementedError("JinaProvider does not support stream_chat yet.")
    
    async def chat_with_tools(
        self,
        messages: List[Dict[str, str]],
        tools: List[Dict[str, Any]],
        model: str,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """带工具调用的聊天补全（用于 Agentic 工作流）"""
        raise NotImplementedError("JinaProvider does not support chat_with_tools yet.")
    
    async def rerank(
        self,
        query: str,
        documents: List[str],
        model: str,
        top_n: Optional[int] = None,
        **kwargs: Any
    ) -> List[Dict[str, Any]]:
        """重排序文档（用于 RAG / 术语选择）"""
        raise NotImplementedError("JinaProvider does not support rerank yet.")
    
    async def close(self):
        """关闭 HTTP 客户端"""
        await self._client.aclose()

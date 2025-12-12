"""
Jina Provider Module

实现 Jina AI 的提供商适配器（主要用于嵌入）。
"""
from typing import Any, AsyncIterator, Dict, List, Optional

import httpx

from utils.log_manager import get_logger
from .base import BaseAIProvider

logger = get_logger(__name__)


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
        
        # 初始化异步 HTTP 客户端
        self._client = httpx.AsyncClient(timeout=30.0)
        
        logger.info(
            "JinaProvider initialized",
            extra={"base_url": self.base_url}
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
            raise
        except Exception as e:
            logger.error(
                "Jina embedding failed",
                extra={
                    "error": str(e),
                    "model": model
                }
            )
            raise
    
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

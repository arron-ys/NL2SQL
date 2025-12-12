"""
OpenAI Provider Module

实现 OpenAI 的 AI 提供商适配器。
"""
import json
from typing import Any, AsyncIterator, Dict, List, Optional

from openai import AsyncOpenAI
from openai.types.chat import ChatCompletionMessageParam

from utils.log_manager import get_logger
from .base import BaseAIProvider

logger = get_logger(__name__)


class OpenAIProvider(BaseAIProvider):
    """
    OpenAI 提供商实现
    
    封装 OpenAI AsyncOpenAI 客户端，实现 BaseAIProvider 接口。
    """
    
    def __init__(
        self,
        api_key: str,
        base_url: Optional[str] = None
    ):
        """
        初始化 OpenAI 提供商
        
        Args:
            api_key: OpenAI API Key
            base_url: OpenAI Base URL，如果为 None 则使用官方 API
        """
        self.api_key = api_key
        self.base_url = base_url
        
        # 初始化异步客户端
        client_kwargs = {
            "api_key": self.api_key,
        }
        if self.base_url:
            client_kwargs["base_url"] = self.base_url
        
        self.client = AsyncOpenAI(**client_kwargs)
        
        logger.info(
            "OpenAIProvider initialized",
            extra={"base_url": self.base_url or "default"}
        )
    
    async def chat(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        **kwargs: Any
    ) -> str:
        """执行聊天补全（返回文本）"""
        try:
            # 转换消息格式
            chat_messages: List[ChatCompletionMessageParam] = [
                {"role": msg["role"], "content": msg["content"]}
                for msg in messages
            ]
            
            # 构建请求参数
            request_params: Dict[str, Any] = {
                "model": model,
                "messages": chat_messages,
                "temperature": temperature,
            }
            
            if max_tokens is not None:
                request_params["max_tokens"] = max_tokens
            
            # 添加其他参数
            request_params.update(kwargs)
            
            # 调用 API
            response = await self.client.chat.completions.create(**request_params)
            
            # 提取内容
            content = response.choices[0].message.content
            if content is None:
                raise ValueError("Empty response from LLM")
            
            logger.debug(
                "OpenAI chat completion successful",
                extra={
                    "model": model,
                    "tokens_used": response.usage.total_tokens if response.usage else None
                }
            )
            
            return content
        
        except Exception as e:
            logger.error(
                "OpenAI chat completion failed",
                extra={
                    "error": str(e),
                    "model": model
                }
            )
            raise
    
    async def chat_json(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """执行聊天补全（返回 JSON 对象）"""
        try:
            # 转换消息格式
            chat_messages: List[ChatCompletionMessageParam] = [
                {"role": msg["role"], "content": msg["content"]}
                for msg in messages
            ]
            
            # 构建请求参数
            request_params: Dict[str, Any] = {
                "model": model,
                "messages": chat_messages,
                "temperature": temperature,
                "response_format": {"type": "json_object"},
            }
            
            if max_tokens is not None:
                request_params["max_tokens"] = max_tokens
            
            # 添加其他参数
            request_params.update(kwargs)
            
            # 调用 API
            response = await self.client.chat.completions.create(**request_params)
            
            # 提取内容
            content = response.choices[0].message.content
            if content is None:
                raise ValueError("Empty response from LLM")
            
            # 解析 JSON
            try:
                parsed_json = json.loads(content)
            except json.JSONDecodeError as e:
                logger.error(
                    "Failed to parse OpenAI response as JSON",
                    extra={"error": str(e), "response_preview": content[:200]}
                )
                raise ValueError(f"Failed to parse response as JSON: {str(e)}") from e
            
            logger.debug(
                "OpenAI chat JSON completion successful",
                extra={
                    "model": model,
                    "tokens_used": response.usage.total_tokens if response.usage else None
                }
            )
            
            return parsed_json
        
        except Exception as e:
            logger.error(
                "OpenAI chat JSON completion failed",
                extra={
                    "error": str(e),
                    "model": model
                }
            )
            raise
    
    async def embed(
        self,
        texts: List[str],
        model: str,
        **kwargs: Any
    ) -> List[List[float]]:
        """生成文本嵌入向量"""
        try:
            # 调用嵌入 API
            response = await self.client.embeddings.create(
                model=model,
                input=texts,
                **kwargs
            )
            
            # 提取嵌入向量列表
            embeddings = [item.embedding for item in response.data]
            
            logger.debug(
                "OpenAI embedding generated",
                extra={
                    "model": model,
                    "text_count": len(texts),
                    "dimension": len(embeddings[0]) if embeddings else 0
                }
            )
            
            return embeddings
        
        except Exception as e:
            logger.error(
                "OpenAI embedding failed",
                extra={
                    "error": str(e),
                    "model": model
                }
            )
            raise
    
    async def stream_chat(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        **kwargs: Any
    ) -> AsyncIterator[str]:
        """流式聊天补全（用于 WebSocket 推送）"""
        try:
            # 转换消息格式
            chat_messages: List[ChatCompletionMessageParam] = [
                {"role": msg["role"], "content": msg["content"]}
                for msg in messages
            ]
            
            # 构建请求参数
            request_params: Dict[str, Any] = {
                "model": model,
                "messages": chat_messages,
                "temperature": temperature,
                "stream": True,
            }
            
            if max_tokens is not None:
                request_params["max_tokens"] = max_tokens
            
            # 添加其他参数
            request_params.update(kwargs)
            
            # 调用流式 API
            stream = await self.client.chat.completions.create(**request_params)
            
            async for chunk in stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    yield chunk.choices[0].delta.content
        
        except Exception as e:
            logger.error(
                "OpenAI stream chat failed",
                extra={
                    "error": str(e),
                    "model": model
                }
            )
            raise
    
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
        try:
            # 转换消息格式
            chat_messages: List[ChatCompletionMessageParam] = [
                {"role": msg["role"], "content": msg["content"]}
                for msg in messages
            ]
            
            # 构建请求参数
            request_params: Dict[str, Any] = {
                "model": model,
                "messages": chat_messages,
                "tools": tools,
                "temperature": temperature,
            }
            
            if max_tokens is not None:
                request_params["max_tokens"] = max_tokens
            
            # 添加其他参数
            request_params.update(kwargs)
            
            # 调用 API
            response = await self.client.chat.completions.create(**request_params)
            
            # 提取响应
            choice = response.choices[0]
            result: Dict[str, Any] = {
                "content": choice.message.content,
                "tool_calls": []
            }
            
            # 提取工具调用
            if choice.message.tool_calls:
                for tool_call in choice.message.tool_calls:
                    result["tool_calls"].append({
                        "id": tool_call.id,
                        "type": tool_call.type,
                        "function": {
                            "name": tool_call.function.name,
                            "arguments": tool_call.function.arguments
                        }
                    })
            
            logger.debug(
                "OpenAI chat with tools successful",
                extra={
                    "model": model,
                    "tool_calls_count": len(result["tool_calls"]),
                    "tokens_used": response.usage.total_tokens if response.usage else None
                }
            )
            
            return result
        
        except Exception as e:
            logger.error(
                "OpenAI chat with tools failed",
                extra={
                    "error": str(e),
                    "model": model
                }
            )
            raise
    
    async def rerank(
        self,
        query: str,
        documents: List[str],
        model: str,
        top_n: Optional[int] = None,
        **kwargs: Any
    ) -> List[Dict[str, Any]]:
        """重排序文档（用于 RAG / 术语选择）"""
        # OpenAI 目前没有官方的 rerank API，可以通过其他方式实现
        # 这里先抛出 NotImplementedError，后续可以基于 embedding 相似度实现
        raise NotImplementedError(
            "OpenAIProvider does not support rerank yet. "
            "Consider using embedding-based similarity as an alternative."
        )

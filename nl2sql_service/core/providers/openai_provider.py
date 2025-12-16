"""
OpenAI Provider Module

实现 OpenAI 的 AI 提供商适配器。
"""
import json
import os
from typing import Any, AsyncIterator, Dict, List, Optional

import httpx
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
        base_url: Optional[str] = None,
        timeout: Optional[float] = None,
        proxy: Optional[str] = None
    ):
        """
        初始化 OpenAI 提供商
        
        Args:
            api_key: OpenAI API Key
            base_url: OpenAI Base URL，如果为 None 则使用官方 API
            timeout: 超时时间（秒），默认 60 秒
            proxy: 代理 URL（如 "http://proxy.example.com:8080"），如果为 None 则从环境变量读取
        """
        self.api_key = api_key
        self.base_url = base_url
        
        # 从环境变量读取代理配置（如果未提供）
        # 优先级：1. 显式传入的 proxy 参数
        #         2. OPENAI_PROXY 环境变量（推荐，专门用于 OpenAI）
        #         3. HTTP_PROXY/HTTPS_PROXY 环境变量（向后兼容，系统级代理）
        if proxy is None:
            proxy = os.getenv("OPENAI_PROXY") or os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("http_proxy") or os.getenv("https_proxy")
        
        # 设置超时时间（优先级：LLM_TIMEOUT > OPENAI_TIMEOUT > 默认值 60.0）
        if timeout is None:
            # 优先使用通用超时配置
            timeout_str = os.getenv("LLM_TIMEOUT") or os.getenv("OPENAI_TIMEOUT", "60.0")
            timeout = float(timeout_str)
        
        # 构建 HTTP 客户端配置
        # 总是创建自定义 HTTP 客户端，以便应用超时和代理设置
        http_client_kwargs = {}
        
        # 配置代理
        # httpx.AsyncClient 使用 'proxy' 参数（不是 'proxies'）
        # 如果 proxy 是字符串，直接传递；如果是字典，也直接传递
        if proxy:
            # httpx 支持字符串格式的代理 URL，会自动应用到 http 和 https
            http_client_kwargs["proxy"] = proxy
            logger.info(f"Using proxy for OpenAI API: {proxy}")
        else:
            logger.debug("No proxy configured for OpenAI API")
        
        # 配置超时（总是设置，确保有合理的超时时间）
        http_timeout = httpx.Timeout(
            connect=10.0,  # 连接超时 10 秒
            read=timeout,  # 读取超时
            write=10.0,    # 写入超时 10 秒
            pool=5.0       # 连接池超时 5 秒
        )
        http_client_kwargs["timeout"] = http_timeout
        
        # 创建自定义 HTTP 客户端（总是创建，以应用超时和代理设置）
        http_client = httpx.AsyncClient(**http_client_kwargs)
        
        # 初始化异步客户端
        client_kwargs = {
            "api_key": self.api_key,
            "http_client": http_client  # 总是使用自定义 HTTP 客户端（包含超时和代理配置）
        }
        if self.base_url:
            client_kwargs["base_url"] = self.base_url
        
        self.client = AsyncOpenAI(**client_kwargs)
        
        logger.info(
            "OpenAIProvider initialized",
            extra={
                "base_url": self.base_url or "default",
                "timeout": timeout,
                "has_proxy": bool(proxy)
            }
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
            # 构建详细的错误信息
            error_type = type(e).__name__
            error_msg = str(e)
            
            # 尝试获取更多错误信息（如果是 OpenAI 异常）
            extra_info = {
                "error": error_msg,
                "error_type": error_type,
                "model": model,
                "has_api_key": bool(self.api_key),
                "api_key_length": len(self.api_key) if self.api_key else 0,
                "base_url": self.base_url or "default"
            }
            
            # OpenAI SDK 的异常通常有这些属性
            if hasattr(e, "status_code"):
                extra_info["status_code"] = e.status_code
            if hasattr(e, "response"):
                try:
                    extra_info["response_text"] = str(e.response.text) if hasattr(e.response, "text") else str(e.response)
                except:
                    pass
            if hasattr(e, "body"):
                try:
                    extra_info["body"] = str(e.body) if e.body else None
                except:
                    pass
            if hasattr(e, "message"):
                extra_info["api_message"] = str(e.message)
            
            # 对于连接错误，获取底层异常信息
            if error_type == "APIConnectionError" and hasattr(e, "__cause__"):
                cause = e.__cause__
                if cause:
                    extra_info["underlying_error"] = str(cause)
                    extra_info["underlying_error_type"] = type(cause).__name__
                    # 如果是 httpx 异常，获取更多信息
                    if hasattr(cause, "request"):
                        try:
                            extra_info["request_url"] = str(cause.request.url) if cause.request else None
                        except:
                            pass
            
            # 输出详细的错误日志
            logger.error(
                f"OpenAI chat JSON completion failed: {error_type} - {error_msg}",
                extra=extra_info,
                exc_info=True
            )
            
            # 对于连接错误，提供诊断建议
            if error_type == "APIConnectionError":
                logger.error(
                    "Connection error detected. Possible causes:",
                    extra={
                        "diagnosis": {
                            "1": "Network connectivity issue - check internet connection",
                            "2": "Proxy required - set HTTP_PROXY or HTTPS_PROXY environment variable",
                            "3": "Firewall blocking - check firewall settings",
                            "4": "DNS resolution failed - check DNS settings",
                            "5": "Timeout too short - increase OPENAI_TIMEOUT environment variable"
                        }
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

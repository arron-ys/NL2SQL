"""
OpenAI Provider Module

实现 OpenAI 的 AI 提供商适配器。
"""
import json
import os
import socket
from urllib.parse import urlparse
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
        proxy: Optional[str] = None,
        provider_name: str = "openai",
    ):
        """
        初始化 OpenAI 提供商
        
        Args:
            api_key: OpenAI API Key
            base_url: OpenAI Base URL，如果为 None 则使用官方 API
            timeout: 超时时间（秒），默认 60 秒
            proxy: 代理 URL（如 "http://proxy.example.com:8080"），优先级高于环境变量
            provider_name: provider 名称（openai/deepseek/qwen），用于分流 proxy env
        """
        self.api_key = api_key
        self.base_url = base_url
        self.provider_name = provider_name
        self._proxy_mode: str = ""
        self._proxy_strict: bool = False
        self._trust_env: bool = False
        self._proxy_source: str = "none"  # none|explicit|system
        self._proxy_url: Optional[str] = None
        self._proxy_downgraded: bool = False
        self._proxy_disabled_reason: Optional[str] = None
        
        def _is_proxy_reachable(proxy_url: str) -> bool:
            """
            快速探测代理是否可连通（避免因本地代理未启动导致所有请求直接 500）。
            仅做 TCP 连接探测，不发 HTTP 请求。
            """
            try:
                parsed = urlparse(proxy_url)
                host = parsed.hostname
                port = parsed.port
                if not host or not port:
                    return True  # 无法解析时不做强判断，保持原行为
                # 本地代理最常见：127.0.0.1 / localhost
                with socket.create_connection((host, port), timeout=0.5):
                    return True
            except Exception:
                return False

        # ============================================================
        # Proxy 统一控制（彻底修复 env proxy 劫持：显式 trust_env=False）
        #
        # PROXY_MODE:
        # - none:     全部禁用代理，trust_env=False
        # - explicit: 仅使用 provider 专用 proxy（OPENAI_PROXY / DEEPSEEK_PROXY / QWEN_PROXY），trust_env=False
        # - system:   允许读取 HTTP_PROXY/HTTPS_PROXY/ALL_PROXY（trust_env=True）
        #
        # PROXY_STRICT:
        # - true:  显式 proxy 不可达 => 直接抛清晰错误
        # - false: 显式 proxy 不可达 => 降级直连，且必须 trust_env=False（避免再次被 env 劫持）
        #
        # 兼容旧变量：OPENAI_PROXY_STRICT（等价于 PROXY_STRICT）
        # ============================================================
        proxy_mode = (os.getenv("PROXY_MODE") or "explicit").strip().lower()
        if proxy_mode not in {"none", "explicit", "system"}:
            proxy_mode = "explicit"
        proxy_strict_raw = (os.getenv("PROXY_STRICT") or os.getenv("OPENAI_PROXY_STRICT") or "").strip()
        proxy_strict = proxy_strict_raw in {"1", "true", "TRUE", "yes", "YES"}

        self._proxy_mode = proxy_mode
        self._proxy_strict = proxy_strict
        self._trust_env = proxy_mode == "system"

        env_proxy_var = {
            "openai": "OPENAI_PROXY",
            "deepseek": "DEEPSEEK_PROXY",
            "qwen": "QWEN_PROXY",
        }.get(self.provider_name, "OPENAI_PROXY")

        explicit_proxy = proxy or os.getenv(env_proxy_var)

        if proxy_mode == "none":
            self._proxy_source = "none"
            self._proxy_url = None
        elif proxy_mode == "explicit":
            self._proxy_source = "explicit" if explicit_proxy else "none"
            self._proxy_url = explicit_proxy
        else:  # system
            # system 模式下，允许 env proxy；但如果显式提供了 provider 专用 proxy，仍按 explicit 处理
            self._proxy_source = "explicit" if explicit_proxy else "system"
            self._proxy_url = explicit_proxy

        # 兼容常见开发环境：.env 里配置了本地代理端口，但代理进程未启动
        # 处理策略：探测不可达则自动禁用代理（fail-open），避免服务直接不可用
        if self._proxy_source == "explicit" and self._proxy_url:
            if not _is_proxy_reachable(self._proxy_url):
                logger.warning(
                    "Proxy is configured but unreachable",
                    extra={
                        "provider": self.provider_name,
                        "proxy_mode": self._proxy_mode,
                        "proxy_source": self._proxy_source,
                        "proxy_url": self._proxy_url,
                        "proxy_strict": self._proxy_strict,
                    },
                )
                self._proxy_disabled_reason = "unreachable"
                if self._proxy_strict:
                    raise ConnectionError(
                        f"{env_proxy_var} is set but unreachable: {self._proxy_url}. "
                        f"Start your proxy process or set {env_proxy_var} to the correct port. "
                        f"(PROXY_MODE={self._proxy_mode}, PROXY_STRICT=true)"
                    )
                # strict=false：降级直连，并强制 trust_env=False，避免被系统 env proxy 劫持
                self._proxy_url = None
                self._proxy_source = "none"
                self._trust_env = False
                self._proxy_downgraded = True
                logger.warning(
                    "Proxy unreachable; downgraded to direct connection with trust_env=False",
                    extra={"provider": self.provider_name},
                )
        
        # 设置超时时间（优先级：LLM_TIMEOUT > OPENAI_TIMEOUT > 默认值 60.0）
        if timeout is None:
            # 优先使用通用超时配置
            timeout_str = os.getenv("LLM_TIMEOUT") or os.getenv("OPENAI_TIMEOUT", "60.0")
            timeout = float(timeout_str)
        
        # 构建 HTTP 客户端配置
        # 总是创建自定义 HTTP 客户端，以便应用超时和代理设置
        http_client_kwargs = {}
        
        # 配置 trust_env：默认必须禁用系统 env proxy（除非 PROXY_MODE=system）
        http_client_kwargs["trust_env"] = bool(self._trust_env)

        # 配置代理（只在 proxy_source=explicit 且 proxy_url 存在时传递）
        if self._proxy_source == "explicit" and self._proxy_url:
            http_client_kwargs["proxy"] = self._proxy_url
            logger.info(
                "Using explicit proxy for provider",
                extra={
                    "provider": self.provider_name,
                    "proxy_source": self._proxy_source,
                    "proxy_url": self._proxy_url,
                    "trust_env": self._trust_env,
                },
            )
        else:
            logger.info(
                "No explicit proxy for provider",
                extra={
                    "provider": self.provider_name,
                    "proxy_source": self._proxy_source,
                    "trust_env": self._trust_env,
                    "proxy_downgraded": self._proxy_downgraded,
                },
            )
        
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
                "provider": self.provider_name,
                "base_url": self.base_url or "default",
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
                "base_url": self.base_url or "default",
                "provider": self.provider_name,
                "proxy_mode": self._proxy_mode,
                "proxy_strict": self._proxy_strict,
                "trust_env": self._trust_env,
                "proxy_source": self._proxy_source,
                "proxy_url": self._proxy_url,
                "proxy_downgraded": self._proxy_downgraded,
                "proxy_disabled_reason": self._proxy_disabled_reason,
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
            # 注意：loguru 使用 str.format 渲染 message，error_msg 可能包含 JSON 花括号，
            # 直接拼接到 message 会触发 KeyError。这里用占位符传参，避免被误格式化。
            logger.error(
                "OpenAI chat JSON completion failed: {} - {}",
                error_type,
                error_msg,
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
                            "2": "Proxy required - set OPENAI_PROXY (preferred) or HTTP_PROXY/HTTPS_PROXY, and ensure proxy process is running",
                            "3": "Firewall blocking - check firewall settings",
                            "4": "DNS resolution failed - check DNS settings",
                            "5": "Timeout too short - increase OPENAI_TIMEOUT environment variable"
                        },
                        "proxy_state": {
                            "proxy_mode": self._proxy_mode,
                            "proxy_strict": self._proxy_strict,
                            "trust_env": self._trust_env,
                            "proxy_source": self._proxy_source,
                            "proxy_url": self._proxy_url,
                            "proxy_downgraded": self._proxy_downgraded,
                            "disabled_reason": self._proxy_disabled_reason,
                        },
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
